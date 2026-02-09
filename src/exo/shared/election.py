from typing import Final, Self

import anyio
from anyio import (
    CancelScope,
    Event,
    create_task_group,
    get_cancelled_exc_class,
)
from anyio.abc import TaskGroup
from loguru import logger

from exo.routing.connection_message import ConnectionMessage, ConnectionMessageType
from exo.shared.types.commands import ForwarderCommand
from exo.shared.types.common import NodeId, SessionId
from exo.utils.channels import Receiver, Sender
from exo.utils.pydantic_ext import CamelCaseModel

DEFAULT_ELECTION_TIMEOUT = 3.0

# Debounce window for connection-triggered elections.
# Absorbs transient WiFi glitches: a disconnect followed by a reconnect
# within this window is a no-op instead of a full re-election.
CONNECTION_DEBOUNCE_SECONDS: Final[float] = 10.0


class ElectionMessage(CamelCaseModel):
    clock: int
    seniority: int
    proposed_session: SessionId
    commands_seen: int

    # Could eventually include a list of neighbour nodes for centrality
    def __lt__(self, other: Self) -> bool:
        if self.clock != other.clock:
            return self.clock < other.clock
        if self.seniority != other.seniority:
            return self.seniority < other.seniority
        elif self.commands_seen != other.commands_seen:
            return self.commands_seen < other.commands_seen
        else:
            return (
                self.proposed_session.master_node_id
                < other.proposed_session.master_node_id
            )


class ElectionResult(CamelCaseModel):
    session_id: SessionId
    won_clock: int
    is_new_master: bool


class Election:
    def __init__(
        self,
        node_id: NodeId,
        *,
        election_message_receiver: Receiver[ElectionMessage],
        election_message_sender: Sender[ElectionMessage],
        election_result_sender: Sender[ElectionResult],
        connection_message_receiver: Receiver[ConnectionMessage],
        command_receiver: Receiver[ForwarderCommand],
        is_candidate: bool = True,
        seniority: int = 0,
    ):
        # If we aren't a candidate, simply don't increment seniority.
        # For reference: This node can be elected master if all nodes are not master candidates
        # Any master candidate will automatically win out over this node.
        self.seniority = seniority if is_candidate else -1
        self.clock = 0
        self.node_id = node_id
        self.commands_seen = 0
        # Every node spawns as master
        self.current_session: SessionId = SessionId(
            master_node_id=node_id, election_clock=0
        )

        # Senders/Receivers
        self._em_sender = election_message_sender
        self._em_receiver = election_message_receiver
        self._er_sender = election_result_sender
        self._cm_receiver = connection_message_receiver
        self._co_receiver = command_receiver

        # Campaign state
        self._candidates: list[ElectionMessage] = []
        self._campaign_cancel_scope: CancelScope | None = None
        self._campaign_done: Event | None = None
        self._tg: TaskGroup | None = None

        # Connection tracking: net-change filtering prevents elections when a
        # disconnect + reconnect of the same peer cancel each other out.
        self._connected_peers: set[NodeId] = set()

        # Loading suppression: when set, both connection-triggered and
        # election-message-triggered campaigns are deferred so that transient
        # WiFi drops during the long model tensor transfer do not kill a
        # loading worker.
        self._suppress_elections: bool = False
        self._suppress_resume_scope: CancelScope | None = None
        self._delayed_suppress_scope: CancelScope | None = None
        self._deferred_election_message: ElectionMessage | None = None

    async def run(self):
        logger.info("Starting Election")
        async with create_task_group() as tg:
            self._tg = tg
            tg.start_soon(self._election_receiver)
            tg.start_soon(self._connection_receiver)
            tg.start_soon(self._command_counter)

            # And start an election immediately, that instantly resolves
            candidates: list[ElectionMessage] = []
            logger.debug("Starting initial campaign")
            self._candidates = candidates
            await self._campaign(candidates, campaign_timeout=0.0)
            logger.debug("Initial campaign finished")

        # Cancel and wait for the last election to end
        if self._campaign_cancel_scope is not None:
            logger.debug("Cancelling campaign")
            self._campaign_cancel_scope.cancel()
        if self._campaign_done is not None:
            logger.debug("Waiting for campaign to finish")
            await self._campaign_done.wait()
        logger.debug("Campaign cancelled and finished")
        logger.info("Election finished")

    async def elect(self, em: ElectionMessage) -> None:
        logger.debug(f"Electing: {em}")
        is_new_master = em.proposed_session != self.current_session
        self.current_session = em.proposed_session
        logger.debug(f"Current session: {self.current_session}")
        await self._er_sender.send(
            ElectionResult(
                won_clock=em.clock,
                session_id=em.proposed_session,
                is_new_master=is_new_master,
            )
        )

    async def shutdown(self) -> None:
        if not self._tg:
            logger.warning(
                "Attempted to shutdown election service that was not running"
            )
            return
        self._tg.cancel_scope.cancel()

    def suppress_elections(self, duration_seconds: float = 5400.0) -> None:
        """Suppress all elections (connections and messages) during model loading.

        Both ``_connection_receiver`` and ``_election_receiver`` honour this
        flag.  Election messages that arrive while suppressed are deferred and
        replayed when ``resume_elections`` is called (or when the auto-resume
        timer fires after *duration_seconds*).

        Each call resets the auto-resume timer.
        """
        self._suppress_elections = True

        if self._suppress_resume_scope is not None:
            self._suppress_resume_scope.cancel()
            self._suppress_resume_scope = None

        if self._tg is not None:
            self._tg.start_soon(self._auto_resume, duration_seconds)

        logger.info(
            f"Election: all elections SUPPRESSED (auto-resume in {duration_seconds}s)"
        )

    def suppress_elections_after_delay(
        self, delay_seconds: float = 30.0, duration_seconds: float = 5400.0
    ) -> None:
        """Activate election suppression after a grace period.

        The delay allows late-joining nodes to trigger elections and join the
        cluster before suppression locks the topology for model loading.
        Calling again before the delay fires cancels the pending activation.
        """
        if self._delayed_suppress_scope is not None:
            self._delayed_suppress_scope.cancel()
            self._delayed_suppress_scope = None

        if self._tg is not None:
            self._tg.start_soon(
                self._delayed_suppress, delay_seconds, duration_seconds
            )
            logger.info(
                f"Election: suppression scheduled in {delay_seconds}s"
            )

    async def _delayed_suppress(
        self, delay_seconds: float, duration_seconds: float
    ) -> None:
        scope = CancelScope()
        self._delayed_suppress_scope = scope
        with scope:
            await anyio.sleep(delay_seconds)
        if not scope.cancel_called:
            self.suppress_elections(duration_seconds)

    def resume_elections(self) -> None:
        """Resume normal elections and replay the highest deferred message."""
        if not self._suppress_elections:
            return

        self._suppress_elections = False

        if self._suppress_resume_scope is not None:
            self._suppress_resume_scope.cancel()
            self._suppress_resume_scope = None

        deferred = self._deferred_election_message
        self._deferred_election_message = None

        if deferred is not None and self._tg is not None:
            logger.info(
                f"Election: replaying deferred message at clock {deferred.clock}"
            )
            candidates: list[ElectionMessage] = [deferred]
            self._candidates = candidates
            self._tg.start_soon(
                self._campaign, candidates, DEFAULT_ELECTION_TIMEOUT
            )

        logger.info("Election: elections RESUMED")

    async def _auto_resume(self, delay_seconds: float) -> None:
        """Background timer that calls ``resume_elections`` after *delay_seconds*."""
        scope = CancelScope()
        self._suppress_resume_scope = scope
        with scope:
            await anyio.sleep(delay_seconds)
        if not scope.cancel_called:
            logger.info("Election: auto-resume timer fired")
            self.resume_elections()

    def _apply_connection_message(self, message: ConnectionMessage) -> None:
        """Update the local connected-peers set from a single message."""
        if message.connection_type is ConnectionMessageType.Connected:
            self._connected_peers.add(message.node_id)
        elif message.connection_type is ConnectionMessageType.Disconnected:
            self._connected_peers.discard(message.node_id)

    async def _election_receiver(self) -> None:
        with self._em_receiver as election_messages:
            async for message in election_messages:
                logger.debug(f"Election message received: {message}")
                if message.proposed_session.master_node_id == self.node_id:
                    logger.debug("Dropping message from ourselves")
                    continue
                if message.clock > self.clock:
                    self.clock = message.clock
                    if self._suppress_elections:
                        logger.info(
                            f"Election message deferred (clock {message.clock}, loading)"
                        )
                        self._deferred_election_message = message
                        self.suppress_elections()
                        continue
                    assert self._tg is not None
                    candidates: list[ElectionMessage] = [message]
                    self._candidates = candidates
                    self._tg.start_soon(
                        self._campaign, candidates, DEFAULT_ELECTION_TIMEOUT
                    )
                    continue
                if message.clock < self.clock:
                    logger.debug(f"Dropping old message: {message}")
                    continue
                logger.debug(f"Election added candidate {message}")
                self._candidates.append(message)

    async def _connection_receiver(self) -> None:
        with self._cm_receiver as connection_messages:
            async for first in connection_messages:
                peers_before = frozenset(self._connected_peers)
                self._apply_connection_message(first)

                # Debounce: wait long enough for transient WiFi reconnects
                # to cancel out the preceding disconnects.
                await anyio.sleep(CONNECTION_DEBOUNCE_SECONDS)
                rest = connection_messages.collect()
                for message in rest:
                    self._apply_connection_message(message)

                peers_after = frozenset(self._connected_peers)

                if self._suppress_elections:
                    logger.info(
                        f"Election suppressed during model loading "
                        f"({len(peers_before)} -> {len(peers_after)} peers, "
                        f"{1 + len(rest)} messages absorbed)"
                    )
                    self.suppress_elections()
                    continue

                if peers_before == peers_after:
                    logger.debug(
                        f"Connection debounce: no net topology change "
                        f"({1 + len(rest)} messages cancelled out)"
                    )
                    continue

                logger.info(
                    f"Topology changed after debounce: "
                    f"{len(peers_before)} -> {len(peers_after)} peers"
                )
                self.clock += 1
                assert self._tg is not None
                candidates: list[ElectionMessage] = []
                self._candidates = candidates
                self._tg.start_soon(
                    self._campaign, candidates, DEFAULT_ELECTION_TIMEOUT
                )

    async def _command_counter(self) -> None:
        with self._co_receiver as commands:
            async for _command in commands:
                self.commands_seen += 1

    async def _campaign(
        self, candidates: list[ElectionMessage], campaign_timeout: float
    ) -> None:
        clock = self.clock

        # Kill the old campaign
        if self._campaign_cancel_scope:
            logger.info("Cancelling other campaign")
            self._campaign_cancel_scope.cancel()
        if self._campaign_done:
            logger.info("Waiting for other campaign to finish")
            await self._campaign_done.wait()

        done = Event()
        self._campaign_done = done
        scope = CancelScope()
        self._campaign_cancel_scope = scope

        try:
            with scope:
                logger.debug(f"Election {clock} started")

                status = self._election_status(clock)
                candidates.append(status)
                await self._em_sender.send(status)

                logger.debug(f"Sleeping for {campaign_timeout} seconds")
                await anyio.sleep(campaign_timeout)
                # minor hack - rebroadcast status in case anyone has missed it.
                await self._em_sender.send(status)
                logger.debug("Woke up from sleep")
                # add an anyio checkpoint - anyio.lowlevel.chekpoint() or checkpoint_if_cancelled() is preferred, but wasn't typechecking last I checked
                await anyio.sleep(0)

                # Election finished!
                elected = max(candidates)
                logger.debug(f"Election queue {candidates}")
                logger.debug(f"Elected: {elected}")
                if (
                    self.node_id == elected.proposed_session.master_node_id
                    and self.seniority >= 0
                ):
                    logger.debug(
                        f"Node is a candidate and seniority is {self.seniority}"
                    )
                    self.seniority = max(self.seniority, len(candidates))
                    logger.debug(f"New seniority: {self.seniority}")
                else:
                    logger.debug(
                        f"Node is not a candidate or seniority is not {self.seniority}"
                    )
                logger.debug(
                    f"Election finished, new SessionId({elected.proposed_session}) with queue {candidates}"
                )
                logger.debug("Sending election result")
                await self.elect(elected)
                logger.debug("Election result sent")
        except get_cancelled_exc_class():
            logger.debug(f"Election {clock} cancelled")
        finally:
            logger.debug(f"Election {clock} finally")
            if self._campaign_cancel_scope is scope:
                self._campaign_cancel_scope = None
            logger.debug("Setting done event")
            done.set()
            logger.debug("Done event set")

    def _election_status(self, clock: int | None = None) -> ElectionMessage:
        c = self.clock if clock is None else clock
        return ElectionMessage(
            proposed_session=(
                self.current_session
                if self.current_session.master_node_id == self.node_id
                else SessionId(master_node_id=self.node_id, election_clock=c)
            ),
            clock=c,
            seniority=self.seniority,
            commands_seen=self.commands_seen,
        )

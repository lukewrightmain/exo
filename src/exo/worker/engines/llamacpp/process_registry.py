"""
Process Registry for llama.cpp subprocess lifecycle management.

This module provides centralized tracking and cleanup of C++ subprocesses
(rpc-server, llama-server) to prevent orphan processes when Python exits
unexpectedly (crash, SIGKILL, OOM).

Key features:
1. Global registry tracks all active subprocesses
2. atexit handler ensures cleanup on normal Python exit
3. Pre-start cleanup kills stale processes from previous runs
4. Signal handlers for graceful shutdown on SIGTERM/SIGINT
"""

import atexit
import os
import signal
import subprocess
from subprocess import Popen
from typing import Set

from loguru import logger


# Global set of active subprocess handles
_active_processes: Set[Popen] = set()

# Flag to track if cleanup has already run
_cleanup_done: bool = False


def register_process(proc: Popen) -> None:
    """
    Register a subprocess for automatic cleanup.

    Call this immediately after spawning rpc-server or llama-server.
    The process will be terminated on Python exit.
    """
    _active_processes.add(proc)
    logger.debug(f"Registered process PID={proc.pid} for cleanup ({len(_active_processes)} total)")


def unregister_process(proc: Popen) -> None:
    """
    Unregister a subprocess from automatic cleanup.

    Call this when you've intentionally stopped a process.
    """
    _active_processes.discard(proc)
    logger.debug(f"Unregistered process PID={proc.pid} ({len(_active_processes)} remaining)")


def cleanup_all_processes() -> None:
    """
    Terminate all registered subprocesses.

    Called automatically on Python exit via atexit.
    Uses escalating force: SIGTERM -> wait 2s -> SIGKILL
    """
    global _cleanup_done

    if _cleanup_done:
        return
    _cleanup_done = True

    if not _active_processes:
        return

    logger.info(f"Cleaning up {len(_active_processes)} subprocess(es)...")

    for proc in list(_active_processes):
        try:
            if proc.poll() is None:  # Still running
                logger.info(f"Terminating process PID={proc.pid}")
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                    logger.info(f"Process PID={proc.pid} terminated gracefully")
                except subprocess.TimeoutExpired:
                    logger.warning(f"Process PID={proc.pid} didn't terminate, killing...")
                    proc.kill()
                    proc.wait(timeout=1)
                    logger.info(f"Process PID={proc.pid} killed")
        except Exception as e:
            logger.warning(f"Error cleaning up process PID={proc.pid}: {e}")

    _active_processes.clear()
    logger.info("Process cleanup complete")


def kill_stale_processes() -> None:
    """
    Kill any orphaned rpc-server/llama-server from previous runs.

    Call this BEFORE spawning new processes to ensure ports are free
    and prevent resource conflicts.

    This is especially important on Android where processes may survive
    Python crashes due to the Low Memory Killer behavior.
    """
    logger.info("Checking for stale llama.cpp processes...")

    killed = []

    # Kill rpc-server processes
    try:
        result = os.system("pkill -9 rpc-server 2>/dev/null")
        if result == 0:
            killed.append("rpc-server")
    except Exception:
        pass

    # Kill llama-server processes
    try:
        result = os.system("pkill -9 llama-server 2>/dev/null")
        if result == 0:
            killed.append("llama-server")
    except Exception:
        pass

    # Also kill any 'server' process that might be llama-server
    # (older builds used this name)
    try:
        # Be more specific to avoid killing unrelated servers
        os.system("pkill -9 -f 'llama.cpp.*server' 2>/dev/null")
    except Exception:
        pass

    if killed:
        logger.info(f"Killed stale processes: {', '.join(killed)}")
    else:
        logger.debug("No stale llama.cpp processes found")


def get_active_process_count() -> int:
    """Return the number of currently registered processes."""
    return len(_active_processes)


def get_active_pids() -> list[int]:
    """Return PIDs of all registered processes that are still running."""
    return [proc.pid for proc in _active_processes if proc.poll() is None]


# Register the cleanup function to run on normal Python exit
atexit.register(cleanup_all_processes)


def _signal_handler(signum: int, frame) -> None:
    """
    Handle SIGTERM/SIGINT by cleaning up processes before exit.

    This ensures subprocesses are terminated even when Python is killed
    by an external signal (e.g., from systemd or manual kill).
    """
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name}, cleaning up processes...")
    cleanup_all_processes()
    # Re-raise the signal to allow default handling (exit)
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


def install_signal_handlers() -> None:
    """
    Install signal handlers for graceful shutdown.

    Call this once at startup (e.g., in main.py) to ensure
    SIGTERM and SIGINT trigger process cleanup.
    """
    try:
        signal.signal(signal.SIGTERM, _signal_handler)
        logger.debug("Installed SIGTERM handler for process cleanup")
    except Exception as e:
        logger.warning(f"Could not install SIGTERM handler: {e}")

    # Note: SIGINT is usually handled by the Node class,
    # but we can install a backup handler
    # signal.signal(signal.SIGINT, _signal_handler)

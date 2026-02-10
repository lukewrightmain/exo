#!/usr/bin/env python3
"""
EXO Cluster Diagnostics CLI

Easy cluster diagnostics via command line:

    python -m exo.cli.diagnostics memory     # Memory report for all nodes
    python -m exo.cli.diagnostics processes  # Process report for all nodes
    python -m exo.cli.diagnostics kill-all   # Kill all llama.cpp processes

For SSH-based remote commands across a cluster, provide node IPs via
environment variable or config file:

    EXO_NODES="10.69.3.193,10.69.3.79,10.69.3.63" python -m exo.cli.diagnostics memory
"""

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional

from loguru import logger


# Default SSH options for Android/Termux connections
SSH_OPTIONS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "ConnectTimeout=5",
    "-o", "BatchMode=yes",
]


@dataclass
class NodeMemoryInfo:
    """Memory information for a cluster node."""
    ip: str
    total_mb: int
    available_mb: int
    used_percent: float
    status: str  # "OK", "WARNING", "CRITICAL", "UNREACHABLE"


@dataclass
class NodeProcessInfo:
    """Process information for a cluster node."""
    ip: str
    python_pids: list[int]
    rpc_server_pids: list[int]
    llama_server_pids: list[int]
    status: str  # "OK", "UNREACHABLE"


def get_node_ips() -> list[str]:
    """Get list of node IPs from environment or config."""
    # Check environment variable
    nodes_env = os.getenv("EXO_NODES", "")
    if nodes_env:
        return [ip.strip() for ip in nodes_env.split(",") if ip.strip()]

    # Check config file
    config_paths = [
        os.path.expanduser("~/.exo/nodes.txt"),
        "/etc/exo/nodes.txt",
    ]
    for path in config_paths:
        try:
            with open(path, "r") as f:
                return [line.strip() for line in f if line.strip() and not line.startswith("#")]
        except FileNotFoundError:
            continue

    # Default to localhost only
    return ["localhost"]


def run_ssh_command(ip: str, command: str, timeout: int = 10) -> tuple[bool, str]:
    """Run a command on a remote node via SSH."""
    if ip == "localhost" or ip == "127.0.0.1":
        # Run locally
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return True, result.stdout + result.stderr
        except subprocess.TimeoutExpired:
            return False, "Command timed out"
        except Exception as e:
            return False, str(e)
    else:
        # Run via SSH
        ssh_cmd = ["ssh"] + SSH_OPTIONS + [ip, command]
        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr or "SSH connection failed"
        except subprocess.TimeoutExpired:
            return False, "SSH connection timed out"
        except Exception as e:
            return False, str(e)


def get_memory_info(ip: str) -> NodeMemoryInfo:
    """Get memory information from a node."""
    # Try /proc/meminfo first (more accurate on Android)
    success, output = run_ssh_command(
        ip,
        "cat /proc/meminfo | grep -E '^(MemTotal|MemAvailable):'"
    )

    if success:
        try:
            lines = output.strip().split("\n")
            total_kb = 0
            available_kb = 0
            for line in lines:
                if line.startswith("MemTotal:"):
                    total_kb = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    available_kb = int(line.split()[1])

            total_mb = total_kb // 1024
            available_mb = available_kb // 1024
            used_percent = ((total_kb - available_kb) / total_kb * 100) if total_kb > 0 else 0

            if available_mb < 200:
                status = "CRITICAL"
            elif available_mb < 500:
                status = "WARNING"
            else:
                status = "OK"

            return NodeMemoryInfo(
                ip=ip,
                total_mb=total_mb,
                available_mb=available_mb,
                used_percent=used_percent,
                status=status,
            )
        except (ValueError, IndexError):
            pass

    return NodeMemoryInfo(
        ip=ip,
        total_mb=0,
        available_mb=0,
        used_percent=0,
        status="UNREACHABLE",
    )


def get_process_info(ip: str) -> NodeProcessInfo:
    """Get process information from a node."""
    # Get PIDs for each process type
    success, output = run_ssh_command(
        ip,
        "pgrep -f 'python.*exo' 2>/dev/null; echo '---'; "
        "pgrep rpc-server 2>/dev/null; echo '---'; "
        "pgrep -f llama-server 2>/dev/null || pgrep -f 'llama.cpp.*server' 2>/dev/null"
    )

    if not success:
        return NodeProcessInfo(
            ip=ip,
            python_pids=[],
            rpc_server_pids=[],
            llama_server_pids=[],
            status="UNREACHABLE",
        )

    try:
        sections = output.split("---")
        python_pids = [int(p) for p in sections[0].strip().split() if p.isdigit()]
        rpc_server_pids = [int(p) for p in sections[1].strip().split() if p.isdigit()] if len(sections) > 1 else []
        llama_pids_raw = sections[2].strip().split() if len(sections) > 2 else []
        llama_server_pids = [int(p) for p in llama_pids_raw if p.isdigit()]

        return NodeProcessInfo(
            ip=ip,
            python_pids=python_pids,
            rpc_server_pids=rpc_server_pids,
            llama_server_pids=llama_server_pids,
            status="OK",
        )
    except (ValueError, IndexError):
        return NodeProcessInfo(
            ip=ip,
            python_pids=[],
            rpc_server_pids=[],
            llama_server_pids=[],
            status="OK",
        )


def cmd_memory(args: argparse.Namespace) -> int:
    """Display memory report for all nodes."""
    nodes = get_node_ips()
    print(f"\n{'='*60}")
    print("EXO CLUSTER MEMORY REPORT")
    print(f"{'='*60}\n")

    total_available = 0
    total_capacity = 0
    node_count = 0

    for ip in nodes:
        info = get_memory_info(ip)
        if info.status == "UNREACHABLE":
            print(f"Node {ip}: UNREACHABLE")
        else:
            status_indicator = {
                "OK": "",
                "WARNING": " (WARNING - below 500MB)",
                "CRITICAL": " (CRITICAL - below 200MB)",
            }.get(info.status, "")

            print(
                f"Node {ip}: {info.available_mb / 1024:.1f}GB available "
                f"({info.used_percent:.0f}% used){status_indicator}"
            )
            total_available += info.available_mb
            total_capacity += info.total_mb
            node_count += 1

    print(f"\n{'-'*60}")
    print(f"Total cluster: {total_available / 1024:.1f}GB available across {node_count} nodes")
    print(f"Cluster capacity: {total_capacity / 1024:.1f}GB total")

    # Model fit analysis
    if args.model:
        model_sizes = {
            "qwen3-80b-q4": 49 * 1024,  # 49GB in MB
            "qwen3-80b-q3": 39 * 1024,  # 39GB in MB
            "qwen3-80b-q2": 32 * 1024,  # 32GB in MB
        }
        if args.model in model_sizes:
            required_mb = model_sizes[args.model]
            # Add 20% overhead
            required_with_overhead = int(required_mb * 1.2)
            fit = "YES" if total_available >= required_with_overhead else "NO"
            headroom = (total_available - required_with_overhead) / 1024
            print(f"\nModel {args.model} needs: {required_with_overhead / 1024:.1f}GB runtime")
            print(f"Fit: {fit} with {headroom:.1f}GB headroom")

    print()
    return 0


def cmd_processes(args: argparse.Namespace) -> int:
    """Display process report for all nodes."""
    nodes = get_node_ips()
    print(f"\n{'='*60}")
    print("EXO CLUSTER PROCESS REPORT")
    print(f"{'='*60}\n")

    for ip in nodes:
        info = get_process_info(ip)
        if info.status == "UNREACHABLE":
            print(f"Node {ip}: UNREACHABLE")
        else:
            python_str = ",".join(map(str, info.python_pids)) if info.python_pids else "NONE"
            rpc_str = ",".join(map(str, info.rpc_server_pids)) if info.rpc_server_pids else "NONE"
            llama_str = ",".join(map(str, info.llama_server_pids)) if info.llama_server_pids else "NONE"
            print(f"Node {ip}: python({python_str}) rpc-server({rpc_str}) llama-server({llama_str})")

    print()
    return 0


def cmd_kill_all(args: argparse.Namespace) -> int:
    """Kill all EXO-related processes on all nodes."""
    nodes = get_node_ips()
    print(f"\n{'='*60}")
    print("EXO CLUSTER KILL ALL")
    print(f"{'='*60}\n")

    if not args.force:
        confirm = input("This will kill python, rpc-server, and llama-server on all nodes. Continue? [y/N] ")
        if confirm.lower() != "y":
            print("Aborted.")
            return 1

    for ip in nodes:
        print(f"\nNode {ip}:")

        # Kill in order: llama-server, rpc-server, then python
        commands = [
            ("llama-server", "pkill -9 -f llama-server; pkill -9 -f 'llama.cpp.*server'"),
            ("rpc-server", "pkill -9 rpc-server"),
            ("python/exo", "pkill -9 -f 'python.*exo'"),
        ]

        for name, cmd in commands:
            success, output = run_ssh_command(ip, cmd)
            if success:
                print(f"  Killed {name}")
            else:
                print(f"  {name}: no processes or error")

    print("\nDone.")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Display combined status for all nodes."""
    nodes = get_node_ips()
    print(f"\n{'='*70}")
    print("EXO CLUSTER STATUS")
    print(f"{'='*70}\n")

    print(f"{'Node':<20} {'Memory':<20} {'Processes':<30}")
    print(f"{'-'*20} {'-'*20} {'-'*30}")

    for ip in nodes:
        mem_info = get_memory_info(ip)
        proc_info = get_process_info(ip)

        if mem_info.status == "UNREACHABLE":
            mem_str = "UNREACHABLE"
        else:
            mem_str = f"{mem_info.available_mb / 1024:.1f}GB free ({mem_info.status})"

        if proc_info.status == "UNREACHABLE":
            proc_str = "UNREACHABLE"
        else:
            procs = []
            if proc_info.python_pids:
                procs.append(f"py:{len(proc_info.python_pids)}")
            if proc_info.rpc_server_pids:
                procs.append(f"rpc:{len(proc_info.rpc_server_pids)}")
            if proc_info.llama_server_pids:
                procs.append(f"llama:{len(proc_info.llama_server_pids)}")
            proc_str = ", ".join(procs) if procs else "none"

        print(f"{ip:<20} {mem_str:<20} {proc_str:<30}")

    print()
    return 0


def main() -> int:
    """Main entry point for diagnostics CLI."""
    parser = argparse.ArgumentParser(
        prog="exo-diagnostics",
        description="EXO Cluster Diagnostics CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Memory command
    memory_parser = subparsers.add_parser("memory", help="Display memory report")
    memory_parser.add_argument(
        "--model",
        choices=["qwen3-80b-q4", "qwen3-80b-q3", "qwen3-80b-q2"],
        help="Check if specific model fits",
    )

    # Processes command
    subparsers.add_parser("processes", help="Display process report")

    # Kill-all command
    kill_parser = subparsers.add_parser("kill-all", help="Kill all EXO processes")
    kill_parser.add_argument("-f", "--force", action="store_true", help="Skip confirmation")

    # Status command
    subparsers.add_parser("status", help="Display combined status")

    args = parser.parse_args()

    if args.command == "memory":
        return cmd_memory(args)
    elif args.command == "processes":
        return cmd_processes(args)
    elif args.command == "kill-all":
        return cmd_kill_all(args)
    elif args.command == "status":
        return cmd_status(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

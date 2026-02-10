import asyncio
import os
import platform
from enum import Enum
from typing import Any, Callable, Coroutine

import anyio
from loguru import logger

from exo.shared.types.memory import Memory
from exo.shared.types.profiling import (
    MemoryPerformanceProfile,
    NodePerformanceProfile,
    SystemPerformanceProfile,
)

from .macmon import (
    MacMonError,
    Metrics,
)
from .macmon import (
    get_metrics_async as macmon_get_metrics_async,
)
from .system_info import (
    get_friendly_name,
    get_model_and_chip,
    get_network_interfaces,
)


class MemoryPressureLevel(Enum):
    """Memory pressure levels for health monitoring."""
    OK = "ok"
    WARNING = "warning"      # < 500MB available
    CRITICAL = "critical"    # < 200MB available


def get_android_available_memory() -> Memory:
    """
    Get available memory on Android using /proc/meminfo.

    This is more accurate than psutil on Android/Termux because:
    1. /proc/meminfo shows actual MemAvailable (includes reclaimable)
    2. psutil may report incorrect values on some Android versions
    3. We can apply Android-specific safety margins

    Returns:
        Memory object with available memory after safety margin
    """
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    # Format: "MemAvailable:    1234567 kB"
                    parts = line.split()
                    if len(parts) >= 2:
                        kb = int(parts[1])
                        available = Memory.from_kb(kb)
                        # Reserve 15% for Android Low Memory Killer
                        safe_bytes = int(available.in_bytes * 0.85)
                        return Memory.from_bytes(safe_bytes)
    except (OSError, ValueError, IndexError) as e:
        logger.debug(f"Failed to read /proc/meminfo: {e}")

    # Fall back to psutil if /proc/meminfo fails
    import psutil
    vm = psutil.virtual_memory()
    # Still apply safety margin
    safe_bytes = int(vm.available * 0.85)
    return Memory.from_bytes(safe_bytes)


def get_memory_pressure() -> MemoryPressureLevel:
    """
    Check current memory pressure level.

    Uses Android-specific memory detection when available.

    Returns:
        MemoryPressureLevel indicating system memory state
    """
    try:
        available = get_android_available_memory()
    except Exception:
        # Fall back to psutil
        import psutil
        vm = psutil.virtual_memory()
        available = Memory.from_bytes(vm.available)

    if available.in_mb < 200:
        return MemoryPressureLevel.CRITICAL
    elif available.in_mb < 500:
        return MemoryPressureLevel.WARNING
    return MemoryPressureLevel.OK


def is_android() -> bool:
    """Check if running on Android (Termux)."""
    # Check common Android indicators
    if os.path.exists("/data/data/com.termux"):
        return True
    if "ANDROID_ROOT" in os.environ:
        return True
    if "TERMUX_VERSION" in os.environ:
        return True
    # Check for Android-specific paths
    if os.path.exists("/system/build.prop"):
        return True
    return False


async def get_metrics_async() -> Metrics | None:
    """Return detailed Metrics on macOS or a minimal fallback elsewhere."""

    if platform.system().lower() == "darwin":
        return await macmon_get_metrics_async()


def get_memory_profile() -> MemoryPerformanceProfile:
    """
    Construct a MemoryPerformanceProfile.

    Uses Android-specific memory detection when running on Android/Termux
    for more accurate available memory reporting.
    """
    override_memory_env = os.getenv("OVERRIDE_MEMORY_MB")
    override_memory: int | None = (
        Memory.from_mb(int(override_memory_env)).in_bytes
        if override_memory_env
        else None
    )

    # On Android, use /proc/meminfo for more accurate readings
    if override_memory is None and is_android():
        try:
            android_available = get_android_available_memory()
            import psutil
            vm = psutil.virtual_memory()
            sm = psutil.swap_memory()
            return MemoryPerformanceProfile.from_bytes(
                ram_total=vm.total,
                ram_available=android_available.in_bytes,
                swap_total=sm.total,
                swap_available=sm.free,
            )
        except Exception as e:
            logger.debug(f"Android memory detection failed, using psutil: {e}")

    return MemoryPerformanceProfile.from_psutil(override_memory=override_memory)


async def start_polling_memory_metrics(
    callback: Callable[[MemoryPerformanceProfile], Coroutine[Any, Any, None]],
    *,
    poll_interval_s: float = 5.0,
) -> None:
    """Continuously poll and emit memory-only metrics.

    Parameters
    - callback: coroutine called with a fresh MemoryPerformanceProfile each tick
    - poll_interval_s: interval between polls (kept high to reduce gossip traffic)
    """
    while True:
        try:
            mem = get_memory_profile()
            await callback(mem)
        except MacMonError as e:
            logger.opt(exception=e).error("Memory Monitor encountered error")
        finally:
            await anyio.sleep(poll_interval_s)


async def start_polling_node_metrics(
    callback: Callable[[NodePerformanceProfile], Coroutine[Any, Any, None]],
):
    poll_interval_s = 10.0
    while True:
        try:
            metrics = await get_metrics_async()

            network_interfaces = get_network_interfaces()
            model_id, chip_id = await get_model_and_chip()
            friendly_name = await get_friendly_name()
            memory_profile = get_memory_profile()

            if metrics is not None:
                system_profile = SystemPerformanceProfile(
                    gpu_usage=metrics.gpu_usage[1],
                    temp=metrics.temp.gpu_temp_avg,
                    sys_power=metrics.sys_power,
                    pcpu_usage=metrics.pcpu_usage[1],
                    ecpu_usage=metrics.ecpu_usage[1],
                    ane_power=metrics.ane_power,
                )
            else:
                system_profile = SystemPerformanceProfile(
                    gpu_usage=0.0,
                    temp=0.0,
                    sys_power=0.0,
                    pcpu_usage=0.0,
                    ecpu_usage=0.0,
                    ane_power=0.0,
                )

            await callback(
                NodePerformanceProfile(
                    model_id=model_id,
                    chip_id=chip_id,
                    friendly_name=friendly_name,
                    network_interfaces=network_interfaces,
                    memory=memory_profile,
                    system=system_profile,
                )
            )

        except asyncio.TimeoutError:
            logger.warning(
                "[resource_monitor] Operation timed out after 30s, skipping this cycle."
            )
        except MacMonError as e:
            logger.opt(exception=e).error("Resource Monitor encountered error")
        finally:
            await anyio.sleep(poll_interval_s)

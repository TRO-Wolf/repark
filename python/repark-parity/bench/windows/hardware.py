"""Machine-profile snapshot for W-0 result files."""

from __future__ import annotations

import os
from pathlib import Path


def cpu_core_count() -> int:
    """Logical CPU count; at least 1."""
    counted = os.cpu_count()
    return counted if counted is not None and counted > 0 else 1


def read_text_or_none(path: Path) -> str | None:
    """Read a small sysfs/proc file; return None if it is absent."""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def hardware_fields() -> dict[str, str]:
    """Best-effort hardware snapshot for the results header."""
    model = "unknown"
    cpuinfo = read_text_or_none(Path("/proc/cpuinfo"))
    if cpuinfo is not None:
        for line in cpuinfo.splitlines():
            if line.lower().startswith("model name"):
                _, _, model = line.partition(":")
                model = "_".join(model.strip().split())
                break
    governor = read_text_or_none(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"))
    mem_kib = None
    meminfo = read_text_or_none(Path("/proc/meminfo"))
    if meminfo is not None:
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                mem_kib = line.split()[1]
                break
    ram_gib = "unknown"
    if mem_kib is not None:
        ram_gib = f"{int(mem_kib) / (1024 * 1024):.1f}"
    return {
        "cpu": model,
        "cores": str(cpu_core_count()),
        "governor": governor if governor is not None else "unknown",
        "ram_gib": ram_gib,
    }


def native_build_flavor() -> str:
    """Heuristic: the debug cdylib is hundreds of MiB; a release develop is smaller."""
    try:
        from repark import _native
    except ImportError:
        return "repark_not_importable"
    path = getattr(_native, "__file__", None)
    if not isinstance(path, str):
        return "unknown"
    try:
        size = Path(path).stat().st_size
    except OSError:
        return "unknown"
    native_debug_size_bytes = 200_000_000
    if size >= native_debug_size_bytes:
        return f"debug_or_unstripped size_bytes={size}"
    return f"release_or_stripped size_bytes={size}"

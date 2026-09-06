"""Time the PERF-APPROXPCT-1 sketch cells: wall, peak RSS, answer, load.

Usage: run_cells.py ROWS ATTEMPTS [--control]
One attempt per line: wall seconds, ru_maxrss peak, the answer, start/end load.
`--control` runs count(id) instead of the sketch. A fresh process per cell keeps
peak RSS attributable; the caller runs one process per baseline row.
"""

import os
import resource
import subprocess
import sys
import time
from pathlib import Path

from repark import ReparkSession, _native


def lane_root() -> Path:
    """The lane checkout this probe runs from."""
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True
    )
    return Path(out.stdout.strip())


def refuse_unless_release(lane: Path) -> None:
    """Refuse anything but the lane release module."""
    import repark

    path = Path(repark.__file__).resolve()
    assert lane in path.parents, path
    assert _native.__debug_assertions__ is False


def peak_mb() -> float:
    """Peak RSS of this process in MiB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def load1() -> float:
    """The 1-minute load average."""
    return os.getloadavg()[0]


def main() -> None:
    """Time every attempt of one baseline row on a range scan."""
    lane = lane_root()
    refuse_unless_release(lane)
    rows = int(sys.argv[1])
    attempts = int(sys.argv[2])
    control = "--control" in sys.argv[3:]
    engine = ReparkSession.builder.appName("approxpct-cells").getOrCreate()
    try:
        frame = engine.range(1, rows + 1)
        call = "count(id) AS p" if control else "percentile_approx(id, 0.5) AS p"
        print("attempt wall_s peak_mb answer load_start load_end", flush=True)
        for attempt in range(attempts):
            start_load = load1()
            started = time.monotonic()
            answer = frame.selectExpr(call).collect()[0][0]
            wall = time.monotonic() - started
            print(
                f"{attempt} {wall:.2f} {peak_mb():.1f} {answer} {start_load:.1f} {load1():.1f}",
                flush=True,
            )
    finally:
        engine.stop()


if __name__ == "__main__":
    main()

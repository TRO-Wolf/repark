"""ICE-TSNS-MERGE-WALL-1: record what the installed build stores in every matrix cell.

Run it against a build of main to refresh ``ice_tsns_merge_wall_1_main.json``:
``python _record_ice_tsns_merge_wall_1_main.py <commit>``. The fixture keeps the cells the pins
read (``doors.in_fixture``): every ``timestamp_ns`` door in every zone, and the control targets
in one zone. ``--whole`` records all 1926 cells, carries and every zone of controls included;
with ``--output <path>`` that is how a build is measured for comparison without touching the
fixture. ``--zone <zone>`` prints that zone's cells as JSON; the parent uses it to record the
three zones in parallel.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors

FIXTURE = Path(__file__).with_name("ice_tsns_merge_wall_1_main.json")


def record_zone(zone: str, whole: bool) -> dict[str, Any]:
    """Measure the cells of ``zone``, one fresh session and warehouse per door."""
    cells: dict[str, Any] = {}
    for target in doors.TARGETS:
        for door in doors.ALL_DOORS:
            if not (whole or doors.in_fixture(zone, target, door)):
                continue
            with tempfile.TemporaryDirectory(prefix="tsns-wall-") as warehouse:
                spark = doors.open_session(zone, Path(warehouse))
                try:
                    measured = doors.measure(spark, target, door)
                finally:
                    spark.stop()
            for part, cell in measured.items():
                cells[doors.cell_key(zone, target, door, part)] = cell
    return cells


def main() -> None:
    """Record one zone to stdout, or every zone in parallel into the fixture or ``--output``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit")
    parser.add_argument("--zone")
    parser.add_argument("--whole", action="store_true")
    parser.add_argument("--output", type=Path, default=FIXTURE)
    arguments = parser.parse_args()
    if arguments.zone:
        json.dump(record_zone(arguments.zone, arguments.whole), sys.stdout)
        return
    workers = [
        subprocess.Popen(
            [sys.executable, __file__, arguments.commit, "--zone", zone]
            + (["--whole"] if arguments.whole else []),
            stdout=subprocess.PIPE,
            text=True,
        )
        for zone in doors.ZONES
    ]
    cells: dict[str, Any] = {}
    for worker in workers:
        output, _ = worker.communicate()
        if worker.returncode != 0:
            raise SystemExit(worker.returncode)
        cells.update(json.loads(output))
    document = {
        "recorded_at_commit": arguments.commit,
        "moments": list(doors.MOMENTS),
        "cells": cells,
    }
    arguments.output.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
    print(len(cells), "cells")


if __name__ == "__main__":
    main()

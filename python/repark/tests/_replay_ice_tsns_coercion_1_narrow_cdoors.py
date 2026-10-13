"""Replay the c_doors attribution cells on this head against main's recorded outs.

Runs each case of the probe's ``c_doors-head-err-main-ok-attribution.json``
on this head in UTC and America/New_York, and compares the captured frame
with main's recorded out file. Any differing cell fails the run. Writes a
per-cell JSON with the equality flag and the two error classes.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import importlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors


def _normalised(cell: dict[str, Any]) -> dict[str, Any]:
    """Return the cell with its rows sorted, as the probe normaliser does."""
    cell = dict(cell)
    rows = cell.get("rows")
    if isinstance(rows, dict):
        columns = list(rows.values())
        zipped = zip(*columns, strict=False) if columns else []
        cell["rows"] = sorted(json.dumps(row, default=str) for row in zipped)
    return cell


def _cell_equal(head: dict[str, Any], main: dict[str, Any]) -> bool:
    """Return true when the two captured frames match up to row order."""
    return json.dumps(_normalised(head), sort_keys=True, default=str) == json.dumps(
        _normalised(main), sort_keys=True, default=str
    )


def main(probe_root: Path, out_path: Path) -> int:
    """Replay every attribution cell in both zones, return 1 on any differ."""
    sys.path.insert(0, str(probe_root / "w"))
    consumer = importlib.import_module("c_doors")
    attribution = probe_root / "out" / "c_doors-head-err-main-ok-attribution.json"
    keys = [row[0] for row in json.loads(attribution.read_text())]
    report: dict[str, Any] = {"cells": {}, "differ": []}
    for zone in ("UTC", "America/New_York"):
        label = zone.replace("/", "_")
        path = probe_root / "out" / f"c_doors-main-{label}.json"
        main_cases = json.loads(path.read_text())["cases"]
        with tempfile.TemporaryDirectory(prefix="narrow-cdoors-") as scratch:
            spark = doors.open_session(zone, Path(scratch))
            cases = consumer.build(spark)
            for key in keys:
                head = consumer.capture(cases[key])
                row = {
                    "equal": bool(_cell_equal(head, main_cases[key])),
                    "head_error": head.get("error_type"),
                    "main_error": main_cases[key].get("error_type"),
                }
                report["cells"][f"{key}|{label}"] = row
                if not row["equal"]:
                    report["differ"].append(f"{key}|{label}")
            spark.stop()
    report["ran"] = len(report["cells"])
    out_path.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    print(f"ran {report['ran']} differ {len(report['differ'])}")
    return 1 if report["differ"] else 0


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1]), Path(sys.argv[2])))

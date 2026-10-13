"""Replay the 117 read cells where fold 1 errored and main answered.

Runs each case of the probe's ``head-errors-main-answered.json`` on this head
in the recorded zone, ANSI mode and source column, and compares the outcome
with main's recorded out file through the probe's own row normaliser. Any
differing cell fails the run. Writes a per-cell JSON with the equality flag,
the two error classes and the gate deltas.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _ice_tsns_coercion_1_doors as doors


def _comparison(probe_dir: Path) -> Any:
    """Return the probe's row normaliser module."""
    location = probe_dir / "w" / "cmp.py"
    spec = importlib.util.spec_from_file_location("narrow_replay_cmp", str(location))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _groups(cells: dict[str, list[str]]) -> dict[tuple[str, str, str], list[tuple[str, str]]]:
    """Group ``suite:case`` cells by suite, zone and ANSI flag."""
    grouped: dict[tuple[str, str, str], list[tuple[str, str]]] = {}
    for key, members in cells.items():
        suite, case = key.split(":")
        for member in members:
            tag, column = member.split("|")
            zone, ansi = tag.rsplit("-ansi", 1)
            grouped.setdefault((suite, zone.replace("_", "/", 1), ansi), []).append((case, column))
    return grouped


def _replay_group(
    normaliser: Any,
    runner: Any,
    spark: Any,
    module: Any,
    wants: list[tuple[str, str]],
    main_cases: dict[str, Any],
) -> dict[str, Any]:
    """Replay one suite, zone and ANSI group in statement role."""
    rows: dict[str, Any] = {}
    by_id = {case["id"]: case for case in module.CASES}
    for case, column in wants:
        bound = dict(by_id[case], c=column)
        head = runner.run_case(spark, bound, "statement")
        main = main_cases[f"{case}|{column}|statement"]
        rows[f"{case}|{column}"] = {
            "equal": bool(normaliser.core(head) == normaliser.core(main)),
            "head_error": head.get("error_type"),
            "main_error": main.get("error_type"),
            "gate": head.get("gate"),
            "fallback": head.get("fallback"),
        }
    return rows


def main(probe_dir: Path, cells_path: Path, out_path: Path) -> int:
    """Replay every cell, write the per-cell report, return 1 on any differ."""
    sys.path.insert(0, str(probe_dir / "w"))
    runner = importlib.import_module("px")
    normaliser = _comparison(probe_dir)
    cells = json.loads(cells_path.read_text())
    report: dict[str, Any] = {"cells": {}, "differ": [], "setup_errors": {}}
    for (suite, zone, ansi), wants in sorted(_groups(cells).items()):
        tag = f"{zone.replace('/', '_')}-ansi{ansi}"
        path = probe_dir / "out" / f"{suite}-main-{tag}.json"
        main_cases = json.loads(path.read_text())["cases"]
        module = importlib.import_module(suite)
        with tempfile.TemporaryDirectory(prefix="narrow-replay-") as scratch:
            spark = doors.open_session(zone, Path(scratch))
            spark.conf.set("spark.sql.ansi.enabled", ansi)
            errors: list[Any] = []
            runner.extra_setup(spark, errors)
            if hasattr(module, "setup"):
                module.setup(spark, errors, Path(scratch))
            report["setup_errors"][f"{suite}|{tag}"] = [entry[0] for entry in errors]
            rows = _replay_group(normaliser, runner, spark, module, wants, main_cases)
            for key, row in rows.items():
                cell = f"{suite}:{key}|{tag}"
                report["cells"][cell] = row
                if not row["equal"]:
                    report["differ"].append(cell)
            spark.stop()
    report["ran"] = len(report["cells"])
    out_path.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    print(f"ran {report['ran']} differ {len(report['differ'])}")
    return 1 if report["differ"] else 0


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])))

"""Run every ICE-TSNS-TYPE-PLANNER-1 cell on the installed module and count by family.

``python _record_ice_tsns_type_planner_1.py --output <path> [--jobs N] [--family NAME]``
writes each cell's reason (empty when green) and whether its statement answered.
``--main <sha>`` writes the statements that errored, and the cells that were red, to
``ice_tsns_type_planner_1_main.json``; run it with main's module only.

pins: ice-tsns-type-planner-1/C-006
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_ice_tsns_type_planner_1 as fixture


def _run(index: int) -> tuple[str, str, dict[str, str], dict[str, bool]]:
    group = fixture.all_groups()[index]
    with tempfile.TemporaryDirectory(prefix="tsns-type-planner-") as scratch:
        try:
            results, answered = fixture.run_group(group, Path(scratch))
        except Exception as error:
            results = {f"{group.name}|group": f"{type(error).__name__}: {error}"[:300]}
            answered = {}
    return group.name, group.family, results, answered


def main() -> None:
    """Run the groups, print the family counts, write the record."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--family", default="")
    parser.add_argument("--main", default="")
    args = parser.parse_args()
    groups = fixture.all_groups()
    wanted = [
        index
        for index, group in enumerate(groups)
        if not args.family or group.family in args.family.split(",")
    ]
    context = multiprocessing.get_context("spawn")
    with context.Pool(args.jobs, maxtasksperchild=1) as pool:
        done = pool.map(_run, wanted, chunksize=1)
    record: dict[str, Any] = {"cells": {}, "answered": {}, "families": {}}
    for _name, family, results, answered in done:
        counts = record["families"].setdefault(family, {"green": 0, "red": 0, "skip": 0})
        for cell, reason in results.items():
            record["cells"][cell] = reason
            counts["skip" if reason == "skip" else "red" if reason else "green"] += 1
        record["answered"].update(answered)
    Path(args.output).write_text(
        json.dumps(record, indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    if args.main:
        errored = sorted(cell for cell, ok in record["answered"].items() if not ok)
        red = {
            cell: reason for cell, reason in record["cells"].items() if reason not in ("", "skip")
        }
        main_record = {
            "module": args.main,
            "cells": len(record["answered"]),
            "errored": errored,
            "red": dict(sorted(red.items())),
        }
        fixture.MAIN.write_text(json.dumps(main_record, indent=1) + "\n", encoding="utf-8")
    for family, counts in sorted(record["families"].items()):
        print(f"{family}: green {counts['green']} red {counts['red']} skip {counts['skip']}")


if __name__ == "__main__":
    main()

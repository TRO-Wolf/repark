from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from compare_make_dt_interval import same

PINNED_DOORS = ("sql", "selectExpr", "expr", "view", "fn", "printSchema", "ctas")
BLAST_STATEMENTS = {
    "blast|sql|ts-ts col": (
        "SELECT id, ts - TIMESTAMP'2023-12-31 00:00:00' AS r FROM mem WHERE id = 1"
    ),
    "blast|sql|sum(ts-ts)": (
        "SELECT sum(ts - TIMESTAMP'2023-12-31 00:00:00') AS r FROM mem WHERE id IN (1, 2)"
    ),
    "blast|sql|union": (
        "SELECT make_dt_interval(1) AS r UNION ALL SELECT make_dt_interval(0, 1) AS r"
    ),
}


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())["cells"]


def has_datetime(cell: dict[str, Any]) -> bool:
    return "datetime:" in json.dumps(cell.get("rows", []))


def expected(cell: dict[str, Any]) -> dict[str, Any]:
    if cell["kind"] == "error":
        return {"kind": "error", "cls": cell.get("cls", "")}
    if "tree" in cell:
        return {"kind": "ok", "text": cell["tree"].splitlines()[-1]}
    if "readback" in cell:
        return expected(cell["readback"])
    return {
        "kind": "ok",
        "type": cell["dtypes"][-1][1],
        "nullable": cell["nulls"][-1],
        "rows": cell["rows"],
    }


def pinnable(key: str, spark: dict[str, Any], head: dict[str, Any]) -> bool:
    door = key.split("|", 1)[0]
    if door not in PINNED_DOORS and key not in BLAST_STATEMENTS:
        return False
    cell = spark.get(key)
    if cell is None or has_datetime(cell):
        return False
    if door == "ctas":
        return cell["kind"] == "error" and head.get(key, {}).get("kind") == "error"
    return same(cell, head.get(key))


def build(args: argparse.Namespace) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    modes = ("false", "true")
    spark = {mode: load(args.dir / f"spark_{mode}.json") for mode in modes}
    main = {mode: load(args.dir / f"main_{mode}.json") for mode in modes}
    head = {mode: load(args.dir / f"head_{mode}.json") for mode in modes}
    cells: dict[str, dict[str, Any]] = {}
    counts = {"fixed": 0, "control": 0}
    for key in sorted(head["false"]):
        if not all(pinnable(key, spark[mode], head[mode]) for mode in modes):
            continue
        pin_key = key
        if key in BLAST_STATEMENTS:
            pin_key = f"blast|sql|{BLAST_STATEMENTS[key]}"
        cells[pin_key] = {mode: expected(spark[mode][key]) for mode in modes}
        on_main = all(same(spark[mode][key], main[mode].get(key)) for mode in modes)
        if key.startswith("ctas|"):
            on_main = main["false"].get(key, {}).get("kind") == "error"
        counts["control" if on_main else "fixed"] += 1
    return cells, counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    cells, counts = build(args)
    args.out.write_text(json.dumps(cells, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"cells": len(cells), **counts}))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
EVIDENCE = ROOT / "task" / "wo" / "write-path-envelope-matrix-1" / "evidence" / "raw"
OUT = ROOT / "python" / "repark" / "tests" / "parity" / "data" / "flat_matrix_expected.json"
SOURCES = (
    "spark-sql-select.jsonl",
    "spark-sql-delete.jsonl",
    "spark-sql-update.jsonl",
    "spark-sql-insert.jsonl",
    "spark-sql-ctas.jsonl",
    "spark-sql-merge.jsonl",
    "spark-df.jsonl",
)
PREDICATE_IDS = frozenset([f"p{number:02d}" for number in range(1, 27)] + ["p35", "p36"])
COMMITTED_KEYS = frozenset(
    [
        "spark-sql|select|v2-cow",
        "spark-sql|delete|v2-cow",
        "spark-sql|delete|v2-mor",
        "spark-sql|update|v2-cow",
        "spark-sql|update|v2-mor",
        "spark-sql|insert|v2-cow",
        "spark-sql|insert|v2-mor",
        "spark-sql|ctas|v2-cow",
        "spark-sql|ctas|v2-mor",
        "spark-sql|merge|v2-cow",
        "spark-sql|merge|v2-mor",
        "filter-str|select|v2-cow",
        "f-expr|select|v2-cow",
        "filter-str|insert|v2-cow",
        "f-expr|insert|v2-cow",
    ]
)
EXPECTED_CELLS = len(COMMITTED_KEYS) * len(PREDICATE_IDS)


def row_ids(rows: list | None) -> list[int] | None:
    if rows is None:
        return None
    return sorted(int(row[0]) for row in rows)


def cell_entry(record: dict) -> dict:
    return {
        "outcome": record["outcome"],
        "ids": record["ids"],
        "lost": record["lost"],
        "damaged": record["damaged"],
        "after_ids": row_ids(record["rows_after"]),
        "commits": commit_delta(record),
        "error": record["error"],
    }


def commit_delta(record: dict) -> int | None:
    before = record["snapshot_before"]
    after = record["snapshot_after"]
    if not before or not after:
        return None
    return int(after["count"]) - int(before["count"])


def collect() -> dict[str, dict]:
    cells: dict[str, dict] = {}
    for name in SOURCES:
        for line in (EVIDENCE / name).read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if record.get("record") != "cell":
                continue
            key = record["key"]
            prefix, _, pid = key.rpartition("|")
            if prefix not in COMMITTED_KEYS or pid not in PREDICATE_IDS:
                continue
            if key in cells:
                raise SystemExit(f"duplicate committed Spark cell {key} in {name}")
            cells[key] = cell_entry(record)
    return cells


def main() -> None:
    cells = collect()
    if len(cells) != EXPECTED_CELLS:
        raise SystemExit(f"expected {EXPECTED_CELLS} Class A cells, found {len(cells)}")
    document = {
        "source": "task/wo/write-path-envelope-matrix-1/evidence/raw (Spark 4.1.2 legs)",
        "generator": "python/repark/tests/parity/data/build_flat_matrix_expected.py",
        "cells": dict(sorted(cells.items())),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(document, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    print(f"wrote {len(cells)} Class A cells to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from compare_over_accept import verdict
from probe_over_accept import DML_CELLS, EXPRESSION_CELLS, STATEMENT_CELLS

PINNED_VERDICTS = {"EQ", "REFUSE~CLASS"}


def door_texts(cell: dict[str, Any]) -> dict[str, str]:
    item = cell.get("item")
    sql = cell.get("sql") or f"SELECT {item}"
    texts = {"sql": sql, "view": sql}
    if item is not None:
        texts["expr"] = item
        texts["selectExpr"] = item
    if cell.get("pred") is not None:
        texts["filter"] = cell["pred"]
    return texts


def outcome(result: dict[str, Any]) -> dict[str, Any]:
    if result["kind"] == "error":
        return {"kind": "error", "class": result["class"], "type": result["type"]}
    return {"kind": "ok", "fields": result["fields"], "rows": result["rows"]}


def class_base(result: dict[str, Any]) -> str | None:
    if result["kind"] != "error" or result["class"] is None:
        return None
    return result["class"].split(".")[0]


def entry(
    cell: dict[str, Any],
    door: str,
    text: str,
    mode: str,
    reports: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    spark = reports["spark"]["cells"][cell["id"]]["doors"].get(door)
    main = reports["main"]["cells"][cell["id"]]["doors"].get(door)
    head = reports["head"]["cells"][cell["id"]]["doors"].get(door)
    if spark is None or main is None or head is None:
        return None
    head_verdict = verdict(spark, head)
    if head_verdict not in PINNED_VERDICTS:
        return None
    spark_class = class_base(spark)
    record: dict[str, Any] = {
        "id": cell["id"],
        "door": door,
        "ansi": mode,
        "role": cell["role"],
        "text": text,
        "setup": cell.get("setup", []),
        "after": cell.get("after"),
        "spark": outcome(spark),
        "pin_class": spark_class if spark_class == class_base(head) else None,
        "fixed": verdict(spark, main) not in PINNED_VERDICTS
        or (spark_class is not None and spark_class == class_base(head) != class_base(main)),
    }
    if spark.get("after") is not None:
        record["spark_after"] = outcome(spark["after"])
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    entries: list[dict[str, Any]] = []
    for mode in ("true", "false"):
        reports = {
            "spark": json.loads((arguments.dir / f"spark-{mode}.json").read_text()),
            "main": json.loads((arguments.dir / f"main-{mode}.json").read_text()),
            "head": json.loads((arguments.dir / f"{arguments.head}-{mode}.json").read_text()),
        }
        for cell in EXPRESSION_CELLS:
            for door, text in door_texts(cell).items():
                record = entry(cell, door, text, mode, reports)
                if record is not None:
                    entries.append(record)
        for cell in STATEMENT_CELLS:
            record = entry(cell, "sql", cell["sql"], mode, reports)
            if record is not None:
                record["door"] = "statement"
                entries.append(record)
        for cell in DML_CELLS:
            record = entry(cell, "dml", cell["sql"], mode, reports)
            if record is not None:
                entries.append(record)
    arguments.out.write_text(json.dumps(entries, indent=1, sort_keys=True) + "\n")
    fixed = sum(1 for record in entries if record["fixed"])
    print(f"entries: {len(entries)} fixed: {fixed} neighbours: {len(entries) - fixed}")


if __name__ == "__main__":
    main()

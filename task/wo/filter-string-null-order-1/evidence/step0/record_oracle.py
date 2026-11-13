"""Write the facade pin's oracle from the step-0 legs.

Usage: record_oracle.py <spark.jsonl> <base.jsonl> <oracle.json>

A cell on a door Spark has answers Spark's rows. A native-door cell answers the base leg's rows,
because that door keeps its own dialect. A cell is grouped ``fixed`` when the base leg was a
WRONG-READ, ``equal`` when the base already matched Spark, and ``native`` for the native door.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load(path: str) -> dict[tuple[str, str], dict[str, Any]]:
    """Return a leg's records keyed by (door, cell)."""
    records = {}
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if "header" not in row:
                records[(row["door"], row["cell"])] = row
    return records


def group_of(spark: dict[str, Any] | None, base: dict[str, Any]) -> str | None:
    """Group one cell, or return None when it carries no recorded rows."""
    if isinstance(base["value"], str):
        return None
    if spark is None:
        return "native"
    if isinstance(spark["value"], str):
        return None
    return "equal" if spark["value"] == base["value"] else "fixed"


def main(spark_path: str, base_path: str, target_path: str) -> None:
    """Merge the legs into the oracle file."""
    spark = load(spark_path)
    cells = []
    for key, base in sorted(load(base_path).items()):
        group = group_of(spark.get(key), base)
        if group is None:
            continue
        truth = base if group == "native" else spark[key]
        cells.append(
            {
                "kind": base["kind"],
                "cell": base["cell"],
                "door": base["door"],
                "group": group,
                "text": base["text"],
                "expected": truth["value"],
                "source": "repark-base" if group == "native" else "spark-4.1.2",
            }
        )
    head = {
        "oracle": "PySpark 4.1.2 local[2]; native cells hold the base build's own answer",
        "recorder": "task/wo/filter-string-null-order-1/evidence/step0/record_oracle.py",
    }
    lines = [json.dumps(cell, sort_keys=True, separators=(",", ":")) for cell in cells]
    with Path(target_path).open("w", encoding="utf-8") as out:
        out.write("{" + json.dumps(head, sort_keys=True)[1:-1] + ',"cells":[\n')
        out.write(",\n".join(lines))
        out.write("\n]}\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])

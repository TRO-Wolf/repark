"""Render the step-0 legs as labelled markdown tables.

Usage: render.py <spark.jsonl> <base.jsonl> [<head.jsonl>] > table.md

Each RePark cell is labelled against Spark: EQUAL, WRONG-READ (rows differ, no error),
REFUSED-WHERE-SPARK-ANSWERS, or BASE-ONLY for a door Spark lacks (the native door), whose
answer is compared with the base leg. With a head leg, the table adds the head label and a
move summary.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def load(path: str) -> tuple[dict[str, Any], dict[tuple[str, str], Any]]:
    """Return a leg's header and its answers keyed by (door, cell)."""
    header: dict[str, Any] = {}
    answers: dict[tuple[str, str], Any] = {}
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if "header" in row:
                header = row["header"]
                continue
            answers[(row["door"], row["cell"])] = row["value"]
    return header, answers


def refused(value: Any) -> bool:
    """Tell whether an answer is a refusal."""
    return isinstance(value, str) and value.startswith("ERR:")


def label(
    spark: dict[tuple[str, str], Any],
    base: dict[tuple[str, str], Any],
    key: tuple[str, str],
    value: Any,
) -> str:
    """Label one RePark answer against Spark, or against the base where Spark has no such door."""
    if key not in spark:
        return "BASE-EQUAL" if value == base.get(key) else "BASE-MOVED"
    expected = spark[key]
    if refused(expected) and refused(value):
        return "REFUSED-BOTH"
    if refused(expected):
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if refused(value):
        return "REFUSED-WHERE-SPARK-ANSWERS"
    return "EQUAL" if value == expected else "WRONG-READ"


def main() -> None:
    """Print the summary, the per-door counts and every cell."""
    spark_header, spark = load(sys.argv[1])
    base_header, base = load(sys.argv[2])
    head = load(sys.argv[3])[1] if len(sys.argv) > 3 else None
    print("# Step 0 legs\n")
    print(f"- Spark: `{json.dumps(spark_header)}`")
    print(f"- Base: `{json.dumps(base_header)}`\n")
    counts: Counter[tuple[str, str, str]] = Counter()
    lines = []
    for key in sorted(base):
        base_label = label(spark, base, key, base[key])
        head_label = label(spark, base, key, head[key]) if head is not None else ""
        counts[(key[0], base_label, head_label)] += 1
        spark_value = json.dumps(spark.get(key, "n/a"))
        cells = [f"`{key[1]}`", key[0], spark_value, json.dumps(base[key]), base_label]
        if head is not None:
            cells += [json.dumps(head[key]), head_label]
        lines.append("| " + " | ".join(cells) + " |")
    columns = ["Door", "Base label"] + (["Head label"] if head is not None else []) + ["Cells"]
    print("| " + " | ".join(columns) + " |")
    print("|" + "---|" * len(columns))
    for (door, base_label, head_label), count in sorted(counts.items()):
        row = [door, base_label] + ([head_label] if head is not None else []) + [str(count)]
        print("| " + " | ".join(row) + " |")
    print("\n## Every cell\n")
    columns = ["Cell", "Door", "Spark", "Base", "Base label"] + (
        ["Head", "Head label"] if head is not None else []
    )
    print("| " + " | ".join(columns) + " |")
    print("|" + "---|" * len(columns))
    print("\n".join(lines))


if __name__ == "__main__":
    main()

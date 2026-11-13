from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load(path: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    cells = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        cell = json.loads(line)
        cells[(cell["group"], cell["op"], cell["shape"])] = cell
    return cells


def answer(cell: dict[str, Any] | None) -> str:
    if cell is None:
        return "n/a"
    if cell["kind"] == "refused":
        head = cell["text"].split(";")[0].replace("|", "/")
        if len(head) > 70:
            head = head[:70].rsplit(" ", 1)[0] + " ..."
        text = f"refused {cell['class']}: {head}"
    else:
        rows = cell["rows"]
        text = "rows" if rows == "elided" else "".join(str(row) for row in rows) or "[]"
    if "stored" in cell:
        after = cell["after"]
        text += f"; stored {cell['stored']}; snapshots {after['snapshots']}"
        if not after["exists"]:
            text += " (no table)"
    if "create" in cell and cell["create"]["kind"] == "refused":
        text += f"; create refused {cell['create']['class']}"
    return text.replace("\n", " ")


def verdict(spark: dict[str, Any] | None, door: dict[str, Any] | None) -> str:
    if door is None or spark is None:
        return "n/a"
    if door["kind"] == "refused":
        return "REFUSED"
    if spark["kind"] == "refused":
        return "ACCEPTED"
    if door.get("stored", door["rows"]) == spark.get("stored", spark["rows"]):
        return "EQUAL"
    return "WRONG"


def main() -> None:
    spark, door_a, door_b = (load(path) for path in sys.argv[1:4])
    title = sys.argv[4]
    keys = list(spark)
    for key in list(door_a) + list(door_b):
        if key not in keys:
            keys.append(key)
    lines = [
        f"# {title}",
        "",
        "| group | operator | shape | Spark 4.1.2 | Spark door | verdict | native door | verdict |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for key in keys:
        group, op, shape = key
        a, b, s = door_a.get(key), door_b.get(key), spark.get(key)
        label = op.replace("\n", "\\n").replace("|", "/")
        lines.append(
            f"| {group} | `{label}` | {shape} | {answer(s)} | {answer(a)} | {verdict(s, a)} | "
            f"{answer(b)} | {verdict(s, b)} |"
        )
    print("\n".join(lines))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

BUILDS = ("base", "1325", "head3")


def load(path: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    out = {}
    for line in path.open():
        record = json.loads(line)
        if record["record"] == "cell":
            out[(record["group"], record["cell"], record["door"])] = record
    return out


def answer(record: dict[str, Any] | None) -> str:
    if record is None:
        return "."
    if not record["ok"]:
        return "ERR " + record["error"].split(":")[0][:40]
    text = json.dumps(record["rows"], separators=(",", ":"))
    return text if len(text) <= 70 else text[:69] + "…"


def main() -> int:
    runs = Path(sys.argv[1])
    out = Path(sys.argv[2])
    spark = load(runs / "cells-spark.jsonl")
    legs = {b: load(runs / f"cells-{b}.jsonl") for b in BUILDS}
    keys = sorted(
        {k for leg in legs.values() for k in leg}, key=lambda k: (k[0], k[2] != "sql", k[1])
    )
    lines = [
        "| group | cell | door | Spark | base | 1325 | head3 |",
        "|---|---|---|---|---|---|---|",
    ]
    rows = []
    for group, cell, door in keys:
        truth = spark.get((group, cell, "sql"))
        mine = [answer(legs[b].get((group, cell, door))) for b in BUILDS]
        expected = answer(truth)
        shown = ["=" if m == expected else m for m in mine]
        row = {
            "group": group,
            "cell": cell,
            "door": door,
            "spark": expected,
            **dict(zip(BUILDS, mine, strict=True)),
        }
        rows.append(row)
        lines.append(
            f"| {group} | {cell} | {door} | {expected.replace('|', '/')} | "
            + " | ".join(s.replace("|", "/") for s in shown)
            + " |"
        )
    out.write_text("\n".join(lines) + "\n")
    with out.with_suffix(".jsonl").open("w") as sink:
        for row in rows:
            sink.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

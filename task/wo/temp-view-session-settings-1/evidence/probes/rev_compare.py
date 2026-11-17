import json
import sys
from collections import Counter
from pathlib import Path


def load(path: str) -> dict:
    cells = {}
    for line in Path(path).read_text().splitlines():
        record = json.loads(line)
        if record["cell"] != "provenance":
            cells[record["cell"]] = record
    return cells


def show(result: dict) -> str:
    if "error" in result:
        return "RAISE " + (result["error"] or result["text"][:60])
    text = ";".join(result.get("rows", []))
    extra = {k: v for k, v in result.items() if k not in ("rows", "types")}
    return text + ((" " + json.dumps(extra, sort_keys=True)) if extra else "")


def verdict(spark: dict, repark: dict) -> str:
    if "error" in spark and "error" in repark:
        return "BOTH_RAISE" if spark["error"] == repark["error"] else "BOTH_RAISE_OTHER"
    if "error" in spark:
        return "REPARK_ANSWERS"
    if "error" in repark:
        return "REPARK_RAISES"
    if show(spark) != show(repark):
        return "VALUE"
    if spark.get("types") != repark.get("types"):
        return "TYPE"
    return "EQUAL"


spark, repark = load(sys.argv[1]), load(sys.argv[2])
out = Path(sys.argv[3])
rows = []
tally = Counter()
for cell, record in spark.items():
    other = repark.get(cell)
    if other is None:
        continue
    v = verdict(record["result"], other["result"])
    tally[(record["kind"], v)] += 1
    rows.append(
        {
            "cell": cell,
            "kind": record["kind"],
            "verdict": v,
            "spark": show(record["result"]),
            "repark": show(other["result"]),
        }
    )
lines = [
    "# Revision cells: Spark 4.1.2 against main `6f80793b`",
    "",
    "## Counts by kind and verdict",
    "",
]
lines += ["| kind | verdict | cells |", "|---|---|---|"]
lines += ["| " + k + " | " + v + " | " + str(n) + " |" for (k, v), n in sorted(tally.items())]
lines += ["", "## Every cell", "", "| cell | verdict | Spark | RePark |", "|---|---|---|---|"]
for row in rows:
    cells = [row["cell"], row["verdict"], row["spark"][:90], row["repark"][:90]]
    lines.append(
        "| `" + cells[0] + "` | " + " | ".join(c.replace("|", "\\|") for c in cells[1:]) + " |"
    )
out.write_text("\n".join(lines) + "\n")
Path(str(out).replace(".md", ".jsonl")).write_text(
    "\n".join(json.dumps(r, sort_keys=True) for r in rows) + "\n"
)
print(sum(tally.values()), dict(Counter(v for (_, v) in tally.elements())))

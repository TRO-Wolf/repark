import json
import sys
from pathlib import Path

raw = Path(sys.argv[1])


def load(name: str) -> dict:
    rows = {}
    for line in (raw / f"{name}.jsonl").read_text().splitlines():
        r = json.loads(line)
        if r["key"] != "provenance":
            rows[r["key"]] = r
    return rows


def fmt(v: object) -> str:
    if isinstance(v, str) and v.startswith("ERR:"):
        parts = v.split(":")
        return "raises " + (parts[2] if parts[2] not in ("None", "") else parts[1])
    if isinstance(v, list) and v and isinstance(v[0], str):
        return "plan"
    return "`" + json.dumps(v) + "`"


tables = {a: (load(f"shapes-spark-{a}"), load(f"shapes-repark-{a}")) for a in ("false", "true")}
print("| key | shape | Spark off | main off | Spark on | main on |")
print("|---|---|---|---|---|---|")
for key, rec in tables["false"][1].items():
    cols = []
    for a in ("false", "true"):
        spark, mine = tables[a]
        cols += [fmt(spark.get(key, {}).get("result")), fmt(mine[key]["result"])]
    shape = rec["sql"].replace("|", "/")
    print(f"| `{key}` | `{shape}` | " + " | ".join(cols) + " |")

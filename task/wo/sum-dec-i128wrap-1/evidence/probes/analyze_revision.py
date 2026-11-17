import json
import sys
from collections import Counter
from pathlib import Path

raw = Path(sys.argv[1])
SKIP = ("provenance", "warehouse")


def load(path: Path) -> dict:
    return {r["key"]: r for r in map(json.loads, path.open())}


def condition(value: object) -> str | None:
    if isinstance(value, str) and value.startswith("ERR:"):
        return value.split(":")[2]
    return None


def is_err(value: object) -> bool:
    return isinstance(value, str) and value.startswith("ERR:")


def verdict(spark: object, repark: object) -> str:
    if is_err(spark) and is_err(repark):
        same = condition(spark).split(".")[0] == condition(repark).split(".")[0]
        return "BOTH_RAISE" if same else "BOTH_RAISE_OTHER"
    if is_err(spark):
        return "SPARK_RAISES"
    if is_err(repark):
        return "REPARK_RAISES"
    if spark == repark:
        return "EQUAL"
    if isinstance(spark, dict) and isinstance(repark, dict) and spark["rows"] == repark["rows"]:
        return "TYPE"
    return "VALUE"


def statement_verdict(spark: list, repark: list) -> str:
    pairs = [verdict(a, b) for a, b in zip(spark, repark, strict=True)]
    worst = [v for v in pairs if v != "EQUAL"]
    return worst[-1] if worst else "EQUAL"


def short(value: object) -> object:
    if isinstance(value, str) and len(value) > 220:
        return value[:220].rsplit(" ", 1)[0]
    return value


def group_of(key: str) -> str:
    if key.startswith(("rev/view", "rev/dfview", "rev/flip")):
        return "views"
    if key.startswith("rev/"):
        return "shapes"
    if key.startswith("ice/"):
        return "iceberg-readback" if key.endswith("/readback") else "iceberg"
    return "observations"


def cross_counts() -> Counter:
    found: Counter = Counter()
    for ansi in ("true", "false"):
        for r in map(json.loads, (raw / f"rev-cross-{ansi}.jsonl").open()):
            if r["key"] == "provenance":
                continue
            label = r["key"].rsplit("/", 1)[1]
            res = r["result"]
            found[(ansi, label, condition(res) or "answers")] += 1
    return found


verdicts = []
counts: Counter = Counter()
for ansi in ("true", "false"):
    spark = load(raw / f"rev-spark-{ansi}.jsonl")
    repark = load(raw / f"rev-repark-{ansi}.jsonl")
    for key, rec in spark.items():
        if key in SKIP or key.startswith(("conf/", "data/", "route/", "explain/")):
            continue
        if key.endswith(("/files", "/columns")):
            continue
        a = rec["result"]
        b = repark.get(key, {}).get("result")
        if key.startswith("ice/") and not key.endswith("/readback"):
            v = statement_verdict(a, b)
        else:
            v = verdict(a, b)
        counts[(ansi, group_of(key), v)] += 1
        row = {"key": key, "ansi": ansi, "verdict": v, "spark": short(a), "repark": short(b)}
        verdicts.append(row)
(raw / "rev-verdicts.jsonl").write_text("".join(json.dumps(v) + "\n" for v in verdicts))
for k in sorted(counts):
    print(*k, counts[k])
for k in sorted(cross_counts().items()):
    print("cross", *k[0], k[1])

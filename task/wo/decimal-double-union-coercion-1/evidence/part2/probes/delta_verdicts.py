import collections
import json
import sys
from pathlib import Path

raw, tag, head, base = sys.argv[1:5]
Record = dict[str, object]


def load(path: Path) -> dict[str, Record]:
    rows: dict[str, Record] = {}
    if path.exists():
        for line in path.open():
            record = json.loads(line)
            rows[record["key"]] = record
    return rows


def answer(record: Record | None) -> tuple:
    if record is None:
        return ("missing",)
    if "error" in record:
        return ("raise",)
    return (record.get("types"), sorted(map(tuple, record["rows"])))


def equal(spark: Record, record: Record | None) -> bool:
    if record is None:
        return False
    if "error" in spark:
        return "error" in record
    return answer(record) == answer(spark)


def verdict(spark: Record, before: Record | None, after: Record | None) -> str:
    was, now = equal(spark, before), equal(spark, after)
    if was and now:
        moved = (
            before is not None and after is not None and before.get("error") != after.get("error")
        )
        return "EQUAL->EQUAL(text-moved)" if moved else "EQUAL->EQUAL"
    if not was and now:
        return "WRONG->EQUAL"
    if was and after is not None and "error" in after and "error" not in spark:
        return "EQUAL->RAISE"
    if was:
        return "EQUAL->WRONG"
    if before is not None and after is not None and answer(before) == answer(after):
        if "error" in before and before["error"] != after["error"]:
            return "still-wrong(text-moved)"
        return "still-wrong(same)"
    return "still-wrong(moved)"


counts: collections.Counter = collections.Counter()
for ansi in ("true", "false"):
    spark = load(Path(raw) / f"{tag}-spark-{ansi}.jsonl")
    before = load(Path(raw) / f"{tag}-{base}-{ansi}.jsonl")
    after = load(Path(raw) / f"{tag}-{head}-{ansi}.jsonl")
    for key, record in before.items():
        if key == "provenance":
            continue
        parts = key.split("/")
        family = "/".join(parts[:2]) if key.startswith("nat/") else parts[0]
        if key.startswith("nat/"):
            v = "native-same" if after.get(key) == record else "native-DIFF"
        elif key in spark:
            v = verdict(spark[key], record, after.get(key))
        else:
            v = "no-spark"
        counts[(family, ansi, v)] += 1
for key, n in sorted(counts.items()):
    print(*key, n)

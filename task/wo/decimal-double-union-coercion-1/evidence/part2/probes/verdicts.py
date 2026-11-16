import collections
import json
import sys
from pathlib import Path

raw, head, base, out = sys.argv[1:5]
PROBES = ("build_cells", "fam", "bound", "identity", "f3", "p2_cells")


def load(path: Path) -> dict:
    rows = {}
    if not path.exists():
        return rows
    for line in path.open():
        record = json.loads(line)
        rows[record["key"]] = record
    return rows


def answer(record: dict | None) -> object:
    if record is None:
        return ("missing",)
    if "error" in record:
        return ("raise",)
    return (record.get("types"), sorted(map(tuple, record["rows"])))


def verdict(spark: dict, before: dict | None, after: dict | None) -> str:
    def equal(record: dict | None) -> bool:
        if record is None:
            return False
        if "error" in spark:
            return "error" in record
        return answer(record) == answer(spark)

    def raised(record: dict | None) -> bool:
        return record is not None and "error" in record and "error" not in spark

    was, now = equal(before), equal(after)
    if was and now:
        return "EQUAL->EQUAL"
    if not was and now:
        return "WRONG->EQUAL"
    if was and raised(after):
        return "EQUAL->RAISE"
    if was:
        return "EQUAL->WRONG"
    return "still-wrong"


counts = collections.Counter()
cells = []
for probe in PROBES:
    for ansi in ("true", "false"):
        spark = load(Path(raw) / f"{probe}-spark-{ansi}.jsonl")
        before = load(Path(raw) / f"{probe}-{base}-{ansi}.jsonl")
        after = load(Path(raw) / f"{probe}-{head}-{ansi}.jsonl")
        for key, record in spark.items():
            if key == "provenance" or key.startswith("explain/"):
                continue
            v = verdict(record, before.get(key), after.get(key))
            counts[(probe, ansi, v)] += 1
            if v != "EQUAL->EQUAL":
                cells.append(
                    {
                        "probe": probe,
                        "ansi": ansi,
                        "key": key,
                        "verdict": v,
                        "spark": answer(record),
                        base: answer(before.get(key)),
                        head: answer(after.get(key)),
                    }
                )
with Path(out).open("w") as fh:
    for cell in cells:
        fh.write(json.dumps(cell, default=str) + "\n")
for key, n in sorted(counts.items()):
    print(*key, n)

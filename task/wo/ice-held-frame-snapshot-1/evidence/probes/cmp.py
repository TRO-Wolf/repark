import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

raw, out = Path(sys.argv[1]), Path(sys.argv[2])
STORED = {"copy", "insel"}


def load(engine: str) -> dict[tuple[str, ...], Any]:
    cells: dict[tuple[str, ...], Any] = {}
    texts = [path.read_text() for path in sorted(raw.glob(engine + "-*.jsonl"))]
    texts += [
        gzip.decompress(path.read_bytes()).decode() for path in raw.glob(engine + ".jsonl.gz")
    ]
    for text in texts:
        for line in text.splitlines():
            record = json.loads(line)
            key = (record["group"], record["hold"], record["write"], record["action"])
            cells[(*key, record["ansi"])] = record["got"]
    return cells


def raised(got: Any) -> bool:
    return isinstance(got, str) and got.startswith("ERROR")


def verdict(spark: Any, other: Any) -> str:
    if raised(spark) and raised(other):
        return "EQUAL"
    if spark == other:
        return "EQUAL"
    if raised(other):
        return "RAISE"
    return "WRONG"


spark, main, base = load("spark"), load("main"), load("base")
rows = []
tally: dict[str, Counter[str]] = defaultdict(Counter)
for key in sorted(spark):
    if key[0] == "X":
        continue
    record = {"key": "|".join(key), "spark": spark[key]}
    for name, cells in (("main", main), ("base", base)):
        got = cells.get(key, "MISSING")
        record[name] = got
        record["v_" + name] = verdict(spark[key], got)
        tally[name][record["v_" + name]] += 1
    record["stored"] = key[3] in STORED and record["v_main"] == "WRONG"
    record["spark_raises"] = raised(spark[key])
    rows.append(record)
with out.open("w") as handle:
    for record in rows:
        handle.write(json.dumps(record) + "\n")
print("cells", len(rows))
for name, counts in tally.items():
    print(name, dict(counts))
by: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
for record in rows:
    group, hold, write, action, ansi = record["key"].split("|")
    by[(group, ansi)][record["v_main"]] += 1
    if record["v_main"] != record["v_base"]:
        print("MAIN_BASE_DIFF", record["key"])
for (group, ansi), counts in sorted(by.items()):
    print("group", group, "ansi", ansi, dict(counts))
modes = defaultdict(set)
for record in rows:
    group, hold, write, action, ansi = record["key"].split("|")
    modes[(group, hold, write, action)].add(record["v_main"] + record["v_base"])
print("mode_dependent", sum(1 for value in modes.values() if len(value) > 1))

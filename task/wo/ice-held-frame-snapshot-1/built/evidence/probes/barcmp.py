import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

evidence, raw, bar, out = (Path(arg) for arg in sys.argv[1:5])
labels = sys.argv[5:]
STORED = {"copy", "insel", "cor", "app", "ins_into", "sat", "self"}
SCHEMA = {"alter_add", "alter_drop", "alter_rename", "recreate", "wt_create"}
TEXT = {"table", "read", "sql", "sqlf"}
WRITE_TO = {"copy", "cor", "app", "self"}


def records(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def keyed(rows: list[dict[str, Any]]) -> dict[tuple[str, ...], Any]:
    return {(r["group"], r["hold"], r["write"], r["action"], r["ansi"]): r["got"] for r in rows}


def spark_cells() -> dict[tuple[str, ...], Any]:
    found: dict[tuple[str, ...], Any] = {}
    for name in ("spark.jsonl.gz", "rev-spark.jsonl.gz"):
        found.update(keyed(records(gzip.decompress((evidence / name).read_bytes()).decode())))
    for ansi in ("true", "false"):
        found.update(keyed(records((raw / ("D-spark-" + ansi + ".jsonl")).read_text())))
    return found


def label_cells(label: str) -> dict[tuple[str, ...], Any]:
    found: dict[tuple[str, ...], Any] = {}
    for path in sorted((bar / label).glob("*.jsonl")):
        found.update(keyed(records(path.read_text())))
    return found


def raised(got: Any) -> bool:
    return isinstance(got, str) and got.startswith("ERROR")


def verdict(spark: Any, other: Any) -> str:
    if other == "MISSING":
        return "MISSING"
    if raised(spark) and raised(other):
        return "EQUAL"
    if spark == other:
        return "EQUAL"
    if raised(other):
        return "RAISE"
    return "WRONG"


def unit(key: tuple[str, ...], spark: Any) -> str:
    group, hold, write, action, _ = key
    if group == "D":
        return "D"
    if group != "B":
        return "first"
    if write in SCHEMA and hold in TEXT and action in WRITE_TO:
        return "1b"
    if write == "alter_add" and isinstance(spark, str) and "COLUMNS_MISMATCH" in spark:
        return "1b"
    return "1a"


spark = spark_cells()
cells = {label: label_cells(label) for label in labels}
summary: dict[str, Any] = {}
rows: list[dict[str, Any]] = []
for key in sorted(spark):
    if key[0] == "X":
        continue
    record: dict[str, Any] = {
        "key": "|".join(key),
        "spark": spark[key],
        "unit": unit(key, spark[key]),
    }
    for label in labels:
        got = cells[label].get(key, "MISSING")
        record[label] = got
        record["v_" + label] = verdict(spark[key], got)
        record["stored_" + label] = key[3] in STORED and record["v_" + label] == "WRONG"
    rows.append(record)
with out.open("w") as handle:
    for record in rows:
        handle.write(json.dumps(record) + "\n")
for label in labels:
    total: Counter[str] = Counter()
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    by_unit: dict[str, Counter[str]] = defaultdict(Counter)
    stored = 0
    for record in rows:
        v = record["v_" + label]
        total[v] += 1
        by_group[record["key"].split("|")[0] + "|" + record["key"].split("|")[-1]][v] += 1
        by_unit[record["unit"]][v] += 1
        stored += record["stored_" + label]
    print(label, dict(total), "stored", stored)
    for group, counts in sorted(by_group.items()):
        print("  group", group, dict(counts))
    for name, counts in sorted(by_unit.items()):
        print("  unit", name, dict(counts))
if len(labels) == 2:
    first, second = labels
    moved: Counter[str] = Counter()
    lost = []
    for record in rows:
        a, b = record["v_" + first], record["v_" + second]
        moved[a + "->" + b] += 1
        if a == "EQUAL" and b != "EQUAL":
            lost.append(record)
    print("moved", dict(moved))
    print("EQUAL lost", len(lost))
    for record in lost:
        print(
            "  LOST",
            record["key"],
            "spark",
            record["spark"],
            first,
            record[first],
            second,
            record[second],
        )
    left = [r for r in rows if r["v_" + second] in ("WRONG", "RAISE")]
    print("still not EQUAL on", second, len(left))
    for record in left:
        print(
            "  LEFT",
            record["v_" + second],
            record["unit"],
            record["key"],
            "spark",
            record["spark"],
            second,
            record[second],
        )
for label in labels:
    for key, got in sorted(cells[label].items()):
        if key[0] == "X":
            print("X", label, "|".join(key), got)
for key, got in sorted(spark.items()):
    if key[0] == "X":
        print("X spark", "|".join(key), got)

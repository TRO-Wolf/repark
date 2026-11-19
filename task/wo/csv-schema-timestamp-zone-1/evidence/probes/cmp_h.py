import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

RAW = Path(sys.argv[1])
OUT = Path(sys.argv[2])
PARTS = ["Z0", "Z1", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7", "A", "T", "S", "W0", "W1", "W2", "W3"]
BUILDS = ["main", "head"]


def load(engine: str) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    cells: dict[str, dict[str, Any]] = {}
    creates: dict[str, str] = {}
    for part in PARTS:
        path = RAW / (engine + "-" + part + ".jsonl")
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if "create_error" in record:
                creates[record["tag"]] = record["create_error"]
            else:
                cells[record["tag"]] = record
    return cells, creates


def created_error(tag: str, creates: dict[str, str]) -> str | None:
    parts = tag.split("/")
    for cut in range(len(parts), 2, -1):
        prefix = "/".join(parts[:cut])
        for suffix in ("/create", ""):
            if prefix + suffix in creates:
                return creates[prefix + suffix]
    return None


def verdict(want: dict[str, Any] | None, got: dict[str, Any] | None) -> str:
    if want is None:
        return "NOSPARK"
    if got is None:
        return "RAISE"
    if "error" in want:
        return "EQUAL" if "error" in got else "WRONG"
    if "error" in got:
        return "RAISE"
    return "EQUAL" if want["rows"] == got["rows"] else "WRONG"


def show(cell: dict[str, Any] | None, error: str | None = None) -> str:
    if cell is None:
        return "RAISE at create: " + (error or "?")[:70]
    if "error" in cell:
        return "RAISE " + cell["error"][:70]
    return " ".join(cell["rows"])[:110]


def family(tag: str) -> dict[str, str]:
    parts = tag.split("/")
    group = parts[0]
    if group in ("Z", "W"):
        mode, direction, reader = parts[1], parts[2], parts[3]
        door = "/".join(parts[4:])
    else:
        mode, direction, reader = "", parts[1], parts[2]
        door = "/".join(parts[3:])
    base, _, body = door.partition("_")
    if group == "W":
        base, body = door, ""
    return {
        "group": group,
        "mode": mode,
        "direction": direction,
        "reader": reader,
        "door": base,
        "body": body or "ts",
    }


spark, spark_creates = load("spark")
builds = {build: load(build) for build in BUILDS}
tags = sorted(set(spark) | {tag for cells, _ in builds.values() for tag in cells})
rows = []
for tag in tags:
    want = spark.get(tag)
    record: dict[str, Any] = {
        "tag": tag,
        **family(tag),
        "spark": show(want, created_error(tag, spark_creates)),
    }
    if want is None and created_error(tag, spark_creates):
        record["spark_create"] = created_error(tag, spark_creates)
    for build, (cells, creates) in builds.items():
        got = cells.get(tag)
        record[build] = show(got, created_error(tag, creates))
        record["v_" + build] = verdict(want, got)
        if want is None and record.get("spark_create"):
            record["v_" + build] = "EQUAL" if got is None or "error" in got else "WRONG"
    main_cell, head_cell = builds["main"][0].get(tag), builds["head"][0].get(tag)
    record["moved"] = (main_cell or {}).get("rows", (main_cell or {}).get("error")) != (
        head_cell or {}
    ).get("rows", (head_cell or {}).get("error"))
    rows.append(record)
MATCH = {
    "Z": {"la_utc": ("utc_utc", "la_la"), "utc_la": ("la_la", "utc_utc")},
    "T": {"LTZ_NTZ": ("NTZ_NTZ", "LTZ_LTZ"), "NTZ_LTZ": ("LTZ_LTZ", "NTZ_NTZ")},
    "A": {"true_false": ("false_false", "true_true"), "false_true": ("true_true", "false_false")},
}
by_tag = {record["tag"]: record for record in rows}
TWINS = {"emu": "json_s", "csv_fix": "csv_s"}


def twins_of(record: dict[str, Any]) -> list[str]:
    pair = MATCH.get(record["group"], {}).get(record["direction"], ())
    old = "/" + record["direction"] + "/"
    return [record["tag"].replace(old, "/" + target + "/", 1) for target in pair]


for record in rows:
    if record["reader"] in TWINS:
        twin = record["tag"].replace(
            "/" + record["reader"] + "/", "/" + TWINS[record["reader"]] + "/", 1
        )
        if twin in spark:
            record["twin_vs_spark"] = verdict(spark[twin], builds["main"][0].get(record["tag"]))
    record["cross"] = record["group"] == "W" or record["direction"] in MATCH.get(
        record["group"], {}
    )
    if record["group"] in ("Z", "T", "A") and not record["cross"]:
        record["gap"] = "matching"
        continue
    refs = [by_tag.get(tag) for tag in twins_of(record)]
    if record["v_main"] in ("EQUAL", "NOSPARK"):
        record["gap"] = record["v_main"].lower()
    elif any(ref is not None and ref["v_main"] != "EQUAL" for ref in refs):
        record["gap"] = "direct"
    else:
        record["gap"] = "view"
    if record["moved"]:
        gained = record["v_head"] == "EQUAL"
        lost = record["v_main"] == "EQUAL"
        record["move"] = (
            "gained" if gained and not lost else ("lost" if lost and not gained else "changed")
        )
OUT.mkdir(parents=True, exist_ok=True)
with (OUT / "verdicts.jsonl").open("w") as handle:
    for record in rows:
        handle.write(json.dumps(record) + "\n")
summary: dict[str, Counter[str]] = defaultdict(Counter)
for record in rows:
    scope = record["group"] + ("|cross" if record["cross"] else "|matching")
    summary[scope + "|all|main"][record["v_main"]] += 1
    summary[scope + "|all|head"][record["v_head"]] += 1
    summary[scope + "|moved"][str(record["moved"])] += 1
    if record.get("move"):
        summary[scope + "|move|" + record["reader"]][record["move"]] += 1
    summary[scope + "|gap"][record.get("gap", "")] += 1
    for key in ("reader", "door", "body", "direction", "mode"):
        summary[scope + "|" + key + "=" + record[key]][record["v_main"]] += 1
        summary[scope + "|gap|" + key + "=" + record[key]][record.get("gap", "")] += 1
    if "twin_vs_spark" in record:
        summary[scope + "|twin_vs_spark|" + record["reader"]][record["twin_vs_spark"]] += 1
with (OUT / "summary.txt").open("w") as handle:
    for key in sorted(summary):
        handle.write(key + " " + json.dumps(dict(sorted(summary[key].items()))) + "\n")
print((OUT / "summary.txt").read_text())

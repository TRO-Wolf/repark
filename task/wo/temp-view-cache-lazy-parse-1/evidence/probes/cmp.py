import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

raw, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
BUILDS = ["main", "base"]
NAMES = ["n", "t", "h", "u", "x"]


def load(build: str) -> dict[str, dict[str, Any]]:
    cells: dict[str, dict[str, Any]] = {}
    for path in sorted(raw.glob(build + "-*.jsonl")):
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if record["tag"] != "DONE":
                cells[record["tag"]] = record
    return cells


def tables(value: Any) -> list[list[list[Any]]]:
    if isinstance(value, dict):
        return [t for key in sorted(value) for t in tables(value[key])]
    if isinstance(value, list) and value and all(isinstance(r, list) for r in value):
        if all(r and not isinstance(r[0], list) for r in value):
            return [value]
        return [t for item in value for t in tables(item)]
    return []


def columns(spark: Any, other: Any) -> list[str]:
    left, right = tables(spark), tables(other)
    if len(left) != len(right):
        return ["shape"]
    found: set[str] = set()
    for a, b in zip(left, right, strict=True):
        if len(a) != len(b):
            found.add("rows")
            continue
        width = max(len(r) for r in a + b)
        for index in range(width):
            col_a = sorted(json.dumps(r[index] if index < len(r) else None) for r in a)
            col_b = sorted(json.dumps(r[index] if index < len(r) else None) for r in b)
            if col_a != col_b:
                found.add(NAMES[min(index, 4)])
    if not found and spark != other:
        found.add("other")
    return sorted(found)


def verdict(spark: dict[str, Any], other: dict[str, Any] | None) -> tuple[str, list[str]]:
    if other is None:
        return "MISSING", []
    s_err = spark.get("err") or spark.get("build_err")
    o_err = other.get("err") or other.get("build_err")
    if s_err and o_err:
        return "EQUAL", ["both raise"]
    if s_err:
        return "WRONG", ["answers where Spark raises " + s_err]
    if o_err:
        return "RAISE", [o_err]
    if spark["v"] == other["v"]:
        return "EQUAL", []
    return "WRONG", columns(spark["v"], other["v"])


def axes(tag: str) -> dict[str, str]:
    parts = tag.split("/")
    if parts[0] in ("R", "Q"):
        return {"group": parts[0], "reader": parts[1], "door": parts[2]}
    keys = ["group", "mode", "dir", "reader", "door", "action"]
    return dict(zip(keys, parts, strict=True))


spark = load("spark")
others = {build: load(build) for build in BUILDS}
out_dir.mkdir(parents=True, exist_ok=True)
lines = []
summary: list[str] = []
tally: dict[str, Counter[str]] = {b: Counter() for b in BUILDS}
by_axis: dict[str, Counter[tuple[str, ...]]] = {b: Counter() for b in BUILDS}
moved: Counter[tuple[str, str]] = Counter()
for tag, record in spark.items():
    row: dict[str, Any] = {"tag": tag}
    for build in BUILDS:
        result, detail = verdict(record, others[build].get(tag))
        row[build] = result
        row[build + "_cols"] = detail
        a = axes(tag)
        tally[build][a["group"] + " " + result] += 1
        if result != "EQUAL":
            key = (a["group"], a["door"], a.get("action", "-"), a["reader"], a.get("mode", "-"))
            by_axis[build][(*key, result, ",".join(detail))] += 1
    if row["main"] != row["base"]:
        moved[(row["base"], row["main"])] += 1
    lines.append(json.dumps(row))
(out_dir / "verdicts.jsonl").write_text("\n".join(lines) + "\n")
summary.append(f"cells with a Spark record: {len(spark)}")
for build in BUILDS:
    summary.append(f"== {build}")
    summary.extend(f"  {key}: {count}" for key, count in sorted(tally[build].items()))
summary.append("== moved base -> main")
summary.extend(f"  {a} -> {b}: {count}" for (a, b), count in sorted(moved.items()))
for build in BUILDS:
    summary.append(f"== not EQUAL on {build} (group, door, action, reader, mode, verdict, columns)")
    summary.extend(f"  {' '.join(key)}: {count}" for key, count in sorted(by_axis[build].items()))
TEXT = {"json_s", "csv_s", "csv_i"}
grid: dict[tuple[str, str, str], Counter[str]] = {}
for line in lines:
    row = json.loads(line)
    a = axes(row["tag"])
    if a["group"] in ("R", "Q"):
        continue
    side = "text" if a["reader"] in TEXT else "ctrl"
    cell = grid.setdefault((a["group"], a["door"], a["action"]), Counter())
    cell[side + " n"] += 1
    for build in BUILDS:
        if row[build] != "EQUAL":
            cell[side + " " + build] += 1
summary.append(
    "== grid: group door action | text not-EQUAL main/base of n | controls main/base of n"
)
for (group, door, action), cell in sorted(grid.items()):
    text = f"{cell['text main']}/{cell['text base']} of {cell['text n']}"
    ctrl = f"{cell['ctrl main']}/{cell['ctrl base']} of {cell['ctrl n']}"
    summary.append(f"  {group} {door} {action} | {text} | {ctrl}")
(out_dir / "summary.txt").write_text("\n".join(summary) + "\n")
print("\n".join(summary[:40]))

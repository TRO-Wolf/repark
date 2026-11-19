import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

RAW = Path(sys.argv[1])
MID = Path(sys.argv[2])
BASE = Path(sys.argv[3])
OUT = Path(sys.argv[4])
ENGINES = ["spark", "main", "part1"]
EMU = {"emu_r": "json_s", "emu_c": "json_s", "emu_live": "json_s", "emu_mid": "json_s"}


def load(prefix: Path, engine: str) -> dict[str, dict[str, Any]]:
    cells: dict[str, dict[str, Any]] = {}
    for path in sorted(prefix.parent.glob(prefix.name + engine + "-*.jsonl")):
        for line in path.read_text().splitlines():
            record = json.loads(line)
            cells[record["tag"]] = record
    return cells


def failed(cell: dict[str, Any] | None) -> bool:
    return cell is None or "error" in cell or "create_error" in cell


def verdict(want: dict[str, Any] | None, got: dict[str, Any] | None) -> str:
    if want is None:
        return "NOSPARK"
    if failed(want):
        return "EQUAL" if failed(got) else "WRONG"
    if failed(got):
        return "RAISE"
    return "EQUAL" if want["cols"] == got["cols"] else "WRONG"


def columns(
    want: dict[str, Any] | None, got: dict[str, Any] | None, skip: tuple[str, ...] = ()
) -> dict[str, str]:
    if failed(want) or failed(got) or want is None or got is None:
        return {}
    names = [name for name in want["names"] if name in got["names"] and name not in skip]
    if len(want["cols"]) != len(got["cols"]):
        return dict.fromkeys(names, "ROWS")
    out = {}
    for name in names:
        left = [row[want["names"].index(name)] for row in want["cols"]]
        right = [row[got["names"].index(name)] for row in got["cols"]]
        out[name] = "EQUAL" if left == right else "WRONG"
    return out


def wrong(cols: dict[str, str]) -> list[str]:
    return sorted(name for name, state in cols.items() if state != "EQUAL")


def family(tag: str) -> dict[str, str]:
    parts = tag.split("/")
    if parts[0] == "Z":
        door, _, body = "/".join(parts[4:]).partition("_")
        return {
            "group": "M",
            "mode": parts[1],
            "direction": parts[2],
            "reader": parts[3],
            "door": door,
            "body": body or "ts",
        }
    return {"group": parts[0], "direction": parts[1], "reader": parts[2], "door": parts[3]}


def compact(cell: dict[str, Any] | None) -> Any:
    if cell is None:
        return None
    if failed(cell):
        return "RAISE " + str(cell.get("error", cell.get("create_error")))[:80]
    return cell["cols"]


def build(folder: Path, tags_from: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    engines = {engine: load(folder, engine) for engine in ENGINES}
    spark = engines["spark"]
    rows = []
    for tag in sorted(set(spark) | set(engines["main"])):
        if tag.startswith(("E/", "SS/")):
            continue
        record: dict[str, Any] = {"tag": tag, **family(tag)}
        want = spark.get(tag)
        for build_name in ("main", "part1"):
            got = engines[build_name].get(tag)
            record["v_" + build_name] = verdict(want, got)
            record["wrong_" + build_name] = wrong(columns(want, got))
        record["spark"] = compact(want)
        record["main"] = compact(engines["main"].get(tag))
        record["moved"] = compact(engines["main"].get(tag)) != compact(engines["part1"].get(tag))
        reader = record["reader"]
        if reader in EMU or reader == "emu":
            twin = tag.replace("/" + reader + "/", "/json_s/", 1)
            if twin in spark:
                got = engines["main"].get(tag)
                cols = columns(spark[twin], got, ("k",))
                record["twin"] = "EQUAL" if cols and not wrong(cols) else verdict(spark[twin], got)
                record["twin_wrong"] = wrong(cols)
        rows.append(record)
    tags_from.update({record["tag"]: record for record in rows})
    return rows


def show(title: str, counter: dict[Any, Counter[str]]) -> list[str]:
    lines = ["## " + title]
    for key in sorted(counter, key=str):
        lines.append(str(key) + " " + json.dumps(dict(sorted(counter[key].items()))))
    return lines


every: dict[str, dict[str, Any]] = {}
revision = build(RAW, every)
midnight = build(MID, every)
base = {}
for line in BASE.read_text().splitlines():
    record = json.loads(line)
    base[record["tag"]] = record
MATCH = {"la_utc": ("utc_utc", "la_la"), "utc_la": ("la_la", "utc_utc")}
for record in midnight:
    pair = MATCH.get(record["direction"])
    record["cross"] = pair is not None
    if not pair:
        record["gap"] = "matching"
        continue
    old_part = "/" + record["direction"] + "/"
    refs = [every.get(record["tag"].replace(old_part, "/" + target + "/", 1)) for target in pair]
    if record["v_main"] in ("EQUAL", "NOSPARK"):
        record["gap"] = record["v_main"].lower()
    elif any(ref is not None and ref["v_main"] != "EQUAL" for ref in refs):
        record["gap"] = "direct"
    else:
        record["gap"] = "view"
    old = base.get(record["tag"])
    record["base_gap"] = old.get("gap") if old else None
OUT.mkdir(parents=True, exist_ok=True)
with (OUT / "verdicts-r1.jsonl").open("w") as handle:
    for record in revision + midnight:
        handle.write(json.dumps(record) + "\n")
lines: list[str] = []
tally: dict[Any, Counter[str]] = defaultdict(Counter)
for record in midnight:
    if record["cross"]:
        tally[("M gap", record["mode"], record["direction"])][record["gap"]] += 1
        tally[("M gap by body", record["direction"], record["body"])][record["gap"]] += 1
        tally[("M base gap by body", record["direction"], record["body"])][
            str(record["base_gap"])
        ] += 1
        if record["gap"] != record["base_gap"]:
            tally[("M changed", record["direction"], record["body"], record["reader"])][
                str(record["base_gap"]) + "->" + record["gap"]
            ] += 1
        if record["gap"] == "view":
            tally[("M view by reader",)][record["reader"]] += 1
        if record["reader"] == "emu":
            tally[("M emu twin", record["direction"])][record.get("twin", "-")] += 1
    else:
        tally[("M matching", record["reader"])][record["v_main"]] += 1
    tally[("M moved main vs part1", record["reader"])][str(record["moved"])] += 1
lines += show("midnight recount (rp.py Z passes, midnight data)", tally)
tally = defaultdict(Counter)
for record in revision:
    group, reader, door = record["group"], record["reader"], record["door"]
    key = (group, reader, door)
    tally[key]["main " + record["v_main"]] += 1
    for name in record["wrong_main"]:
        tally[key]["main col " + name] += 1
    if "twin" in record:
        tally[key]["twin " + record["twin"]] += 1
        for name in record["twin_wrong"]:
            tally[key]["twin col " + name] += 1
    if record["moved"]:
        tally[key]["moved"] += 1
        tally[key]["part1 " + record["v_part1"]] += 1
lines += show("revision cells by group, reader, door", tally)
spark_e, main_e = load(RAW, "spark"), load(RAW, "main")
for tag in sorted(t for t in spark_e if t.startswith(("E/", "SS/"))):
    want, got = spark_e[tag], main_e.get(tag, {})
    keep = ("accepted", "lines", "value", "error")
    lines.append(
        tag
        + " spark="
        + json.dumps({k: want[k] for k in keep if k in want})
        + " main="
        + json.dumps({k: got[k] for k in keep if k in got})
    )
(OUT / "summary-r1.txt").write_text("\n".join(lines) + "\n")
print((OUT / "summary-r1.txt").read_text())

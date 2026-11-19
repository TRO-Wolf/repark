import json
import sys
from collections import Counter
from pathlib import Path

out_dir, raw, fresh = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
BUILDS = ("pre", "part1", "head")
PART1_DOORS = {"sql", "tsql", "vss", "vds", "ren", "tsqlf", "vdsf"}
PART2_DOORS = {"df", "dff", "vsd"}
HELD_DOORS = {"held", "heldf", "tdf", "tdff"}
PART1_WRITES = {"w-ctas-sql", "w-insert-sql", "w-writeto-tsql"}
PART1_NEST = {"nest-sql-sql", "nest-sql-df"}
PART1_CACHE = {"sql_then_set", "lazy_then_set", "catalog_then_set", "set_between_zone"}
PART1_CACHE |= {"set_between_off_on", "set_then_cache"}
DIRECT_GAPS = ("rename/view", "alter_view_as", "cache_table_as_select")


def load(path: Path) -> dict:
    found = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record.get("cell") != "provenance" and "result" in record:
            found[record["cell"]] = record
    return found


def show(result: dict | None) -> str:
    if result is None:
        return "OK"
    if "error" in result:
        return "RAISE " + (result["error"] or result.get("text", "")[:60])
    extra = {k: v for k, v in result.items() if k not in ("rows", "types")}
    text = json.dumps([result.get("rows", []), result.get("types", [])])
    return text + ((" " + json.dumps(extra, sort_keys=True)) if extra else "")


def verdict(spark: dict | None, repark: dict | None) -> str:
    if spark is None and repark is None:
        return "EQUAL"
    if spark is None or repark is None:
        return "CREATE_DIFF"
    se, re_ = "error" in spark, "error" in repark
    if se and re_:
        same = spark["error"] and spark["error"] == repark["error"]
        return "BOTH_RAISE" if same else "BOTH_RAISE_OTHER"
    if se:
        return "REPARK_ANSWERS"
    if re_:
        return "REPARK_RAISES"
    if (
        spark.get("rows") != repark.get("rows")
        or show(spark).split(" ", 1)[1:] != show(repark).split(" ", 1)[1:]
    ):
        return "VALUE"
    if spark.get("types") != repark.get("types") and "types" in spark and "types" in repark:
        return "TYPE"
    return "EQUAL"


def scope_run(record: dict) -> str:
    phase, door = record.get("phase", ""), record.get("door", "")
    if phase in ("direct", "conf") or door == "direct" or record["cell"].startswith("conf/"):
        return "direct"
    if phase == "write":
        return "part1" if door in PART1_WRITES else "part2"
    if phase == "nest":
        return "part1" if door in PART1_NEST else "part2"
    if door == "cat":
        return "catalog"
    if door in HELD_DOORS:
        return "held"
    if door in PART1_DOORS:
        return "part1"
    if door in PART2_DOORS:
        return "part2"
    if phase in ("create", "rename"):
        return "step"
    return "other:" + door


def scope_rev(cell: str) -> str:
    parts = cell.split("/")
    family = parts[0]
    if family == "direct" or cell.startswith(DIRECT_GAPS):
        return "direct"
    if family == "cache":
        if parts[1] in PART1_CACHE:
            return "part1"
        return "held" if parts[1] == "held_then_set" else "part2"
    if family in ("derive", "door", "held_write"):
        return "part2"
    if family == "rename":
        return "part1"
    if family == "defcat":
        return {"sql": "part1", "df": "part2"}.get(parts[1], "direct")
    if "sql" in parts:
        return "part1"
    if "df" in parts:
        return "part2"
    return "other:" + family


def scope_rc(cell: str) -> str:
    parts = cell.split("/")
    if parts[0] in ("direct", "rename"):
        return "direct"
    if parts[0] == "cache":
        return "part1" if parts[1] in PART1_CACHE else "part2"
    if parts[0] in ("derive", "door", "held_write"):
        return "part2"
    return "part1" if "sql" in parts else "part2"


def same_setting(record: dict) -> bool:
    created, read = record.get("created"), record.get("read")
    return created is not None and created == read


def sweep_records(path: Path) -> dict:
    found = json.loads(path.read_text())
    return {
        mode + ":" + key: {"cell": mode + ":" + key, "result": value}
        for mode, cells in found.items()
        for key, value in cells.items()
    }


rows = []
sets = [
    ("run", "true", scope_run),
    ("run-unset", "unset", scope_run),
    ("rev", "rev", scope_rev),
    ("rc", "rc", scope_rc),
    ("sweep", "sweep", None),
]
for kind, tag, scope in sets:
    if kind == "sweep":
        spark = sweep_records(fresh / "sweep-spark.json")
        builds = {b: sweep_records(fresh / ("sweep-" + b + ".json")) for b in BUILDS}
    elif kind.startswith("run"):
        spark = load(fresh / ("spark-" + tag + ".jsonl"))
        builds = {b: load(fresh / (b + "-" + tag + ".jsonl")) for b in BUILDS}
    else:
        spark = load(fresh / (tag + "-spark.jsonl"))
        builds = {b: load(fresh / (tag + "-" + b + ".jsonl")) for b in BUILDS}
    for cell, record in spark.items():
        if any(cell not in builds[b] for b in BUILDS):
            continue
        if (
            kind.startswith("run")
            and record.get("phase") in ("create", "rename", None)
            and not cell.startswith(("unset/", "conf/", "now/", "reader/", "nest/read"))
        ):
            continue
        if kind == "run-unset" and not cell.startswith(("unset/", "conf/")):
            continue
        if kind == "sweep":
            sc = "sweep"
        elif kind.startswith("run"):
            sc = scope(record)
        else:
            sc = scope(cell)
        if kind == "run-unset" and sc != "direct":
            sc = {"sql": "part1", "df": "part2", "held": "held"}.get(record.get("door", ""), sc)
        row = {
            "set": kind,
            "cell": cell,
            "scope": sc,
            "matching": kind.startswith("run") and same_setting(record),
            "spark_answer": show(record["result"])[:120],
        }
        for b in BUILDS:
            row[b] = verdict(record["result"], builds[b][cell]["result"])
            row[b + "_answer"] = show(builds[b][cell]["result"])[:120]
        rows.append(row)

out_dir.mkdir(parents=True, exist_ok=True)
(out_dir / "bar2-cells.jsonl").write_text(
    "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows)
)


def ok(v: str) -> bool:
    return v in ("EQUAL", "BOTH_RAISE")


def move(r: dict, base: str) -> str:
    if r[base] == r["head"] and r[base + "_answer"] == r["head_answer"]:
        return ""
    if ok(r[base]) and not ok(r["head"]):
        return "EQUAL->" + ("RAISE" if r["head"] == "REPARK_RAISES" else "WRONG")
    if not ok(r[base]) and ok(r["head"]):
        return "WRONG->EQUAL"
    if ok(r[base]) and ok(r["head"]):
        return "EQUAL->EQUAL(changed)"
    return "WRONG->WRONG(changed)"


lines = ["# Bar, part 2 (written by bar2.py)", "", "## Counts per verdict", ""]
lines += ["| set | scope | verdict | pre | part1 | head |", "|---|---|---|---|---|---|"]
tally = Counter()
for r in rows:
    for b in BUILDS:
        tally[(r["set"], r["scope"], r[b], b)] += 1
keys = sorted({(s, sc, v) for (s, sc, v, _) in tally})
for s, sc, v in keys:
    lines.append(
        f"| {s} | {sc} | {v} | " + " | ".join(str(tally[(s, sc, v, b)]) for b in BUILDS) + " |"
    )
for base in ("part1", "pre"):
    moves = [(move(r, base), r) for r in rows if move(r, base)]
    mc = Counter((m, r["scope"]) for m, r in moves)
    lines += ["", f"## Moves against {base}", "", "| move | scope | cells |", "|---|---|---|"]
    lines += [f"| {m} | {sc} | {n} |" for (m, sc), n in sorted(mc.items())]
    lines += ["", f"## Every move against {base}", ""]
    lines += [
        f"| move | scope | set | cell | Spark | {base} | head |",
        "|---|---|---|---|---|---|---|",
    ]
    for m, r in sorted(moves, key=lambda p: (p[0], p[1]["scope"], p[1]["cell"])):
        shown = [r["spark_answer"][:60], r[base + "_answer"][:60], r["head_answer"][:60]]
        lines.append(
            f"| {m} | {r['scope']} | {r['set']} | `{r['cell']}` | "
            + " | ".join(c.replace("|", "\\|") for c in shown)
            + " |"
        )
    print(base, "moves", Counter(m for m, _ in moves))
    print(
        base,
        "bad",
        [(m, r["scope"], r["cell"]) for m, r in moves if m in ("EQUAL->WRONG", "EQUAL->RAISE")][
            :80
        ],
    )
    print(
        base, "outside", Counter((m, r["scope"]) for m, r in moves if r["scope"] not in ("part2",))
    )
    print(base, "matching moves", [r["cell"] for m, r in moves if r["matching"]][:20])
left = [r for r in rows if r["scope"] == "part2" and not ok(r["head"])]
lines += ["", "## Every non-EQUAL cell left in part 2's scope", ""]
lines += ["| set | cell | head verdict | Spark | part1 | head |", "|---|---|---|---|---|---|"]
for r in sorted(left, key=lambda r: (r["set"], r["cell"])):
    shown = [r["spark_answer"][:60], r["part1_answer"][:60], r["head_answer"][:60]]
    lines.append(
        f"| {r['set']} | `{r['cell']}` | {r['head']} | "
        + " | ".join(c.replace("|", "\\|") for c in shown)
        + " |"
    )
(out_dir / "bar2-summary.md").write_text("\n".join(lines) + "\n")
print("rows", len(rows), "part2 left", len(left))

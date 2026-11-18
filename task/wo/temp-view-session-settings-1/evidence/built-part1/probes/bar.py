import json
import sys
from collections import Counter
from pathlib import Path

out_dir = Path(sys.argv[1])
raw = Path(sys.argv[2])
fresh = Path(sys.argv[3])

PART1_DOORS = {"sql", "tsql", "vss", "vds", "ren", "tsqlf", "vdsf"}
PART2_DOORS = {"df", "dff", "vsd"}
HELD_DOORS = {"held", "heldf", "tdf", "tdff"}
PART1_WRITES = {"w-ctas-sql", "w-insert-sql", "w-writeto-tsql"}
PART2_WRITES = {"w-ctas-df", "w-parquet-held", "w-writeto-held"}
PART1_NEST = {"nest-sql-sql", "nest-sql-df"}
PART1_CACHE = {"sql_then_set", "lazy_then_set", "catalog_then_set", "set_between_zone"}
PART1_CACHE |= {"set_between_off_on", "set_then_cache"}
DIRECT_GAPS = ("rename/view", "alter_view_as", "cache_table_as_select")
WRONG = {"VALUE", "TYPE", "REPARK_ANSWERS", "REPARK_RAISES", "CREATE_DIFF"}


def load(path: Path) -> dict:
    found = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record.get("cell") != "provenance" and "result" in record:
            found[record["cell"]] = record
    return found


def show(result: dict) -> str:
    if result is None:
        return "OK"
    if "error" in result:
        return "RAISE " + (result["error"] or result["text"][:60])
    extra = {k: v for k, v in result.items() if k not in ("rows", "types")}
    text = json.dumps([result.get("rows", []), result.get("types", [])])
    return text + ((" " + json.dumps(extra, sort_keys=True)) if extra else "")


def verdict(spark: dict, repark: dict) -> str:
    if spark is None and repark is None:
        return "EQUAL"
    if spark is None or repark is None:
        return "CREATE_DIFF"
    se, re_ = "error" in spark, "error" in repark
    if se and re_:
        return (
            "BOTH_RAISE"
            if spark["error"] and spark["error"] == repark["error"]
            else "BOTH_RAISE_OTHER"
        )
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
    if parts[0] == "direct" or parts[0] == "rename":
        return "direct"
    if parts[0] == "cache":
        return "part1" if parts[1] in PART1_CACHE else "part2"
    if parts[0] in ("derive", "door", "held_write"):
        return "part2"
    return "part1" if "sql" in parts else "part2"


def same_setting(record: dict) -> bool:
    created, read = record.get("created"), record.get("read")
    return created is not None and created == read


rows = []
sets = [
    ("run", load(raw / "spark-true.jsonl"), "true", scope_run),
    ("run-unset", load(raw / "spark-unset.jsonl"), "unset", scope_run),
    ("rev", load(fresh / "rev-spark.jsonl"), "rev", scope_rev),
    ("rc", load(fresh / "rc-spark.jsonl"), "rc", scope_rc),
]
for kind, spark, tag, scope in sets:
    if tag in ("true", "unset"):
        main, head = (
            load(fresh / ("main-" + tag + ".jsonl")),
            load(fresh / ("head-" + tag + ".jsonl")),
        )
    else:
        main, head = load(fresh / (tag + "-main.jsonl")), load(fresh / (tag + "-head.jsonl"))
    for cell, record in spark.items():
        if cell not in main or cell not in head:
            continue
        if (
            kind.startswith("run")
            and record.get("phase") in ("create", "rename", None)
            and not cell.startswith(("unset/", "conf/", "now/", "reader/", "nest/read"))
        ):
            continue
        if kind == "run-unset" and not (cell.startswith("unset/") or cell.startswith("conf/")):
            continue
        sc = scope(record) if kind.startswith("run") else scope(cell)
        if kind == "run-unset" and sc not in ("direct",):
            sc = {"sql": "part1", "df": "part2", "held": "held"}.get(record.get("door", ""), sc)
        mv, hv = (
            verdict(record["result"], main[cell]["result"]),
            verdict(record["result"], head[cell]["result"]),
        )
        rows.append(
            {
                "set": kind,
                "cell": cell,
                "scope": sc,
                "matching": kind.startswith("run") and same_setting(record),
                "main": mv,
                "head": hv,
                "spark_answer": show(record["result"])[:120],
                "main_answer": show(main[cell]["result"])[:120],
                "head_answer": show(head[cell]["result"])[:120],
            }
        )

out_dir.mkdir(parents=True, exist_ok=True)
(out_dir / "bar-cells.jsonl").write_text(
    "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows)
)


def ok(v: str) -> bool:
    return v in ("EQUAL", "BOTH_RAISE")


def move(r: dict) -> str:
    if r["main"] == r["head"] and r["main_answer"] == r["head_answer"]:
        return ""
    if ok(r["main"]) and not ok(r["head"]):
        return "EQUAL->" + ("RAISE" if r["head"] == "REPARK_RAISES" else "WRONG")
    if not ok(r["main"]) and ok(r["head"]):
        return "WRONG->EQUAL"
    if ok(r["main"]) and ok(r["head"]):
        return "EQUAL->EQUAL(changed)"
    return "WRONG->WRONG(changed)"


lines = ["# Bar (written by bar.py)", "", "## Counts per verdict", ""]
lines += ["| set | scope | build | verdict | cells |", "|---|---|---|---|---|"]
tally = Counter()
for r in rows:
    tally[(r["set"], r["scope"], "main", r["main"])] += 1
    tally[(r["set"], r["scope"], "head", r["head"])] += 1
for (s, sc, b, v), n in sorted(tally.items()):
    lines.append(f"| {s} | {sc} | {b} | {v} | {n} |")
moves = [(move(r), r) for r in rows if move(r)]
mc = Counter((m, r["scope"]) for m, r in moves)
lines += ["", "## Moves by kind and scope", "", "| move | scope | cells |", "|---|---|---|"]
lines += [f"| {m} | {sc} | {n} |" for (m, sc), n in sorted(mc.items())]
lines += [
    "",
    "## Every move",
    "",
    "| move | scope | cell | Spark | main | head |",
    "|---|---|---|---|---|---|",
]
for m, r in sorted(moves, key=lambda p: (p[0], p[1]["scope"], p[1]["cell"])):
    cells = [r["spark_answer"][:60], r["main_answer"][:60], r["head_answer"][:60]]
    lines.append(
        f"| {m} | {r['scope']} | `{r['cell']}` | "
        + " | ".join(c.replace("|", "\\|") for c in cells)
        + " |"
    )


def directs(cells: dict) -> dict:
    found = {}
    for record in cells.values():
        if record.get("phase") == "direct":
            found[(record["family"], record["body"], record["read"])] = show(record["result"])
    return found


spark_direct = directs(load(raw / "spark-true.jsonl"))
head_direct = directs(load(fresh / "head-true.jsonl"))
spark_run = load(raw / "spark-true.jsonl")


def agree(a: str, b: str) -> bool:
    return a == b or (a.startswith("RAISE") and b.startswith("RAISE"))


def gap_class(r: dict) -> str:
    if r["set"] == "run":
        record = spark_run[r["cell"]]
        keys = [
            (record.get("family"), record.get("body"), record.get(k)) for k in ("created", "read")
        ]
        if any(
            k in spark_direct and not agree(spark_direct[k], head_direct.get(k, "")) for k in keys
        ):
            return "direct gap"
        if r["matching"]:
            return "direct gap (settings match)"
        if r["head"] == "BOTH_RAISE_OTHER":
            return "direct gap (condition text)"
        return "view gap left"
    if r["head"] == "BOTH_RAISE_OTHER":
        return "direct gap (condition text)"
    return "view gap left"


left = [r for r in rows if r["scope"] == "part1" and not ok(r["head"])]
for r in left:
    r["class"] = gap_class(r)
lc = Counter(r["class"] for r in left)
lines += ["", "## Non-EQUAL part 1 cells by class", "", "| class | cells |", "|---|---|"]
lines += [f"| {k} | {n} |" for k, n in sorted(lc.items())]
lines += [
    "",
    "## Every non-EQUAL cell left in part 1's scope",
    "",
    "| cell | class | head verdict | Spark | head |",
    "|---|---|---|---|---|",
]
for r in sorted(left, key=lambda r: (r["class"], r["cell"])):
    lines.append(
        f"| `{r['cell']}` | {r['class']} | {r['head']} | "
        + r["spark_answer"][:60].replace("|", "\\|")
        + " | "
        + r["head_answer"][:60].replace("|", "\\|")
        + " |"
    )
(out_dir / "bar-summary.md").write_text("\n".join(lines) + "\n")
print("rows", len(rows), "moves", Counter(m for m, _ in moves))
print(
    "bad",
    [
        (m, r["scope"], r["cell"])
        for m, r in moves
        if m.startswith("EQUAL->") and m != "EQUAL->EQUAL(changed)"
    ][:60],
)
print("outside", Counter((m, r["scope"]) for m, r in moves if r["scope"] != "part1"))
print("matching moves", [r["cell"] for m, r in moves if r["matching"]][:20])
print("part1 left", len(left), lc)
print("view gaps left", [r["cell"] for r in left if r["class"] == "view gap left"])

import gzip
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

EVID = Path(sys.argv[1])
BAR = Path(sys.argv[2])
FABLE = Path(sys.argv[3])
F8 = Path(sys.argv[4])
OUT = Path(sys.argv[5])
NAMES = ["n", "t", "h", "u", "x"]
TEXT = {"json_s", "csv_s", "csv_i"}
HELD = {"cache", "persist_mo", "persist_disk", "recache", "recache_after", "after_set"}


def lines(path: Path) -> list[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as handle:
        return handle.read().splitlines()


def records(paths: list[Path]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in paths:
        for line in lines(path):
            record = json.loads(line)
            if record["tag"] == "DONE":
                continue
            if "got" in record:
                got = record["got"]
                if isinstance(got, str) and got.startswith("ERROR"):
                    record = {"tag": record["tag"], "err": got}
                else:
                    record = {"tag": record["tag"], "v": got}
            out[record["tag"]] = record
    return out


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


def family(tag: str) -> str:
    p = tag.split("/")
    group = p[0]
    if group == "R":
        door = p[2]
        text = p[1] in TEXT
        if door in ("explain_first",) and text:
            return "req:F1"
        if door == "own_then_cache" and text:
            return "req:own_then_cache"
        if door == "child_before" and text:
            return "req:F5"
        return "R"
    if group == "C":
        door, action = p[4], p[5]
        if door in ("cache_view", "cache_view_sql") and p[3] in TEXT:
            return "req:F1"
        if door in HELD and action == "join":
            return "fc:join"
        return "C"
    if group == "L":
        src, how = p[4], p[5]
        if src == "cache" and how == "create":
            return "req:F8"
        if src == "cache" and how == "insert" and p[3] in TEXT:
            return "req:F1"
        return "L"
    if group == "M":
        shape = p[2]
        if shape == "cache_siblings" and p[3] in TEXT:
            return "req:F5"
        if shape == "selfjoin":
            return "fc:join"
        return "M"
    return group


def main() -> None:
    spark = records([EVID / "spark.jsonl.gz", EVID / "m-spark.jsonl.gz"])
    spark.update(records([FABLE / "child-spark.jsonl"]))
    spark.update(records([F8 / "f8-spark.jsonl"]))
    base = records([EVID / "main.jsonl.gz", EVID / "m-main.jsonl.gz"])
    base.update(records([FABLE / "child-main.jsonl"]))
    base.update(records([F8 / "f8-main.jsonl"]))
    head = records([*sorted(BAR.glob("head-*.jsonl")), BAR / "m-head.jsonl"])
    head.update(records([BAR / "child-head.jsonl"]))
    head.update(records([BAR / "f8-head.jsonl"]))
    tally: Counter[tuple[str, str, str]] = Counter()
    moves: Counter[tuple[str, str, str]] = Counter()
    lost: list[str] = []
    rows = []
    for tag, record in sorted(spark.items()):
        fam = family(tag)
        if fam.startswith("c_") or tag.startswith("c_"):
            fam = "fable"
        m, _mc = verdict(record, base.get(tag))
        h, hc = verdict(record, head.get(tag))
        tally[(fam, "main", m)] += 1
        tally[(fam, "head", h)] += 1
        if m != h:
            moves[(fam, m, h)] += 1
        if m == "EQUAL" and h != "EQUAL":
            lost.append(f"{tag} {h} {hc}")
        rows.append({"tag": tag, "family": fam, "main": m, "head": h, "head_cols": hc})
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "verdicts.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    report = [f"cells with a Spark record: {len(spark)}"]
    for key, count in sorted(tally.items()):
        report.append(f"  {key[0]} {key[1]} {key[2]}: {count}")
    report.append("== moves main -> head")
    report.extend(f"  {a} {b} -> {c}: {n}" for (a, b, c), n in sorted(moves.items()))
    report.append(f"== EQUAL lost: {len(lost)}")
    report.extend("  " + item for item in lost)
    (OUT / "summary.txt").write_text("\n".join(report) + "\n")
    print("\n".join(report))


main()

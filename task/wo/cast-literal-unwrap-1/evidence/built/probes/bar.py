import importlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parent
sys.path.insert(0, str(B / "probes"))
cells = importlib.import_module("cells")

RAW = Path(sys.argv[1])
CRITIC = Path(sys.argv[2])
MAIN = B / "out-main"
HEAD = B / sys.argv[3]
OUT = Path(sys.argv[4])
KINDS = {k.name: k for k in cells.KINDS}
INT_MAX = {"TINYINT": 127, "SMALLINT": 32767, "INT": 2147483647, "BIGINT": 9223372036854775807}


def load(path: Path) -> dict:
    rows = {}
    if not path.exists():
        return rows
    for line in path.read_text().splitlines():
        r = json.loads(line)
        k = r.get("key")
        if (
            k in (None, "provenance", "elapsed")
            or k.endswith("/setup")
            or r.get("pos") == "explain"
        ):
            continue
        rows[k] = r
    return rows


def raised(r: dict) -> bool:
    return any(
        isinstance(r.get(f), str) and r[f].startswith(("ERR", "SETUP-ERR"))
        for f in ("statement", "result")
    )


def answer(r: dict) -> str:
    return json.dumps([r.get("result"), r.get("stored")], sort_keys=True)


def verdict(spark: dict, mine: dict) -> str:
    a, b = raised(spark), raised(mine)
    if a and b:
        return "EQUAL"
    if a:
        return "MISSED-RAISE"
    if b:
        return "REFUSE"
    return "EQUAL" if answer(spark) == answer(mine) else "WRONG"


def digits(t: str) -> tuple:
    inner = t[t.index("(") + 1 : -1].split(",")
    return int(inner[0]), int(inner[1])


def overflow_capable(kind: str) -> bool:
    k = KINDS.get(kind)
    if k is None:
        return False
    src, dst = k.col_type, k.target
    if src in INT_MAX and dst in INT_MAX:
        return INT_MAX[dst] < INT_MAX[src]
    if src.startswith("DECIMAL") and dst in INT_MAX:
        p, s = digits(src)
        return 10 ** (p - s) - 1 > INT_MAX[dst]
    if src.startswith("DECIMAL") and dst.startswith("DECIMAL"):
        p, s = digits(src)
        p2, s2 = digits(dst)
        return (p2 - s2 <= p - s) if s2 < s else (p2 - s2 < p - s)
    return False


def top_of_type(kind: str, key: str, sql: str | None) -> bool:
    if key.startswith("q4/"):
        return key.rsplit("/", 1)[-1] in ("gt_top", "top_lt", "lt_bottom", "eq_top_plus")
    k = KINDS.get(kind)
    if k is None or k.target not in INT_MAX:
        return False
    top, bottom = INT_MAX[k.target], -INT_MAX[k.target] - 1
    parts = key.split("/")
    if "where" in parts and len(parts) >= 6 and parts[-3].startswith("c"):
        const, op, side = k.consts[int(parts[-3][1:])], parts[-2], parts[-1]
        try:
            value = int(const)
        except ValueError:
            return False
        if side == "l":
            op = {"gt": "lt", "lt": "gt", "ge": "le", "le": "ge"}.get(op, op)
        return (
            (value == top and op == "gt")
            or (value == bottom and op == "lt")
            or (value == top + 1 and op == "eq")
        )
    return parts[-1] == "col_gt" and k.pylit == top


CRITIC_KINDS = {"b": "bigint_int", "i": "int_smallint"}
HAND = {"n11": {"result": ["1", "6", "7", "8"]}, "n12": {"result": ["1"]}}


def kind_of(key: str, rec: dict) -> str:
    if rec.get("kind"):
        return rec["kind"]
    if key.startswith("q4/"):
        return {"tinyint": "dec_tinyint_overflow", "smallint": "int_smallint", "int": "bigint_int"}[
            key.split("/")[1]
        ]
    if key[:1] in CRITIC_KINDS and key[1:].isdigit():
        return CRITIC_KINDS[key[:1]]
    return key.split("/")[0]


def excluded(key: str) -> bool:
    return key in ("x01", "s26/explain_parquet_view") or (
        key.startswith("date_ts/") and "/df/" in key
    )


SETS = []
for ansi in ("false", "true"):
    SETS.append(("cells", ansi, load(RAW / f"spark-{ansi}.jsonl"), f"repark-{ansi}.jsonl", None))
    SETS.append(
        (
            "shapes",
            ansi,
            load(RAW / f"shapes-spark-{ansi}.jsonl"),
            f"shapes-repark-{ansi}.jsonl",
            None,
        )
    )
    for p in ("reads", "dml", "shapes"):
        SETS.append(
            (
                f"critic-{p}",
                ansi,
                load(CRITIC / f"{p}-spark-{ansi}.jsonl"),
                f"critic-{p}-repark-{ansi}.jsonl",
                None,
            )
        )
native_spark = {
    k.replace("/view/", "/native_view/").replace("/parquet/", "/native_parquet/"): v
    for k, v in load(RAW / "spark-true.jsonl").items()
}
SETS.append(("native", "native", native_spark, "native.jsonl", None))
critic_reads = {
    r["sql"].replace("vdec", "nvdec"): r
    for r in load(CRITIC / "reads-spark-true.jsonl").values()
    if "sql" in r
}
critic_native_spark = {}
for key, r in load(MAIN / "critic-native.jsonl").items():
    if r.get("sql") in critic_reads:
        critic_native_spark[key] = critic_reads[r["sql"]]
SETS.append(("critic-native", "native", critic_native_spark, "critic-native.jsonl", None))

rows = []
for name, ansi, spark, fname, _ in SETS:
    main, head = load(MAIN / fname), load(HEAD / fname)
    for key, s in spark.items():
        m, h = main.get(key), head.get(key)
        if m is None or h is None:
            continue
        rec = h
        kind = kind_of(key, rec)
        if name == "critic-native" and key in HAND:
            s = HAND[key]
        vm, vh = verdict(s, m), verdict(s, h)
        cls = ""
        if vh != "EQUAL" and excluded(key):
            cls = "EXCLUDED (step 0's datetime-literal artifact or plan text)"
        elif vh != "EQUAL":
            q4 = top_of_type(kind, key, rec.get("sql"))
            q2 = ansi == "false" and overflow_capable(kind)
            if vm == vh == "REFUSE":
                cls = "REFUSE->REFUSE (unchanged refusal)"
            elif vh == "REFUSE" and q4:
                cls = f"{vm}->REFUSE (Q4)"
            elif vh == "REFUSE" and q2:
                cls = f"{vm}->REFUSE (Q2)"
            elif vm == vh == "REFUSE":
                cls = "REFUSE->REFUSE (kernel, out of unit)"
            else:
                cls = f"{vm}->{vh} UNEXPLAINED"
        rows.append(
            {
                "set": name,
                "ansi": ansi,
                "key": key,
                "kind": kind,
                "main": vm,
                "head": vh,
                "class": cls,
                "spark": s.get("result"),
                "main_result": m.get("result"),
                "head_result": h.get("result"),
                "spark_stmt": s.get("statement"),
                "head_stmt": h.get("statement"),
            }
        )

OUT.mkdir(parents=True, exist_ok=True)
with (OUT / "bar-cells.jsonl").open("w") as fh:
    for r in rows:
        fh.write(json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n")
trans = defaultdict(Counter)
for r in rows:
    trans[(r["set"], r["ansi"])][(r["main"], r["head"])] += 1
lines = [
    "| set | ANSI | cells | main EQUAL | head EQUAL | WRONG->EQUAL | MISSED->EQUAL "
    "| EQUAL->REFUSE | WRONG->REFUSE | MISSED->REFUSE | REFUSE->REFUSE | REFUSE->EQUAL "
    "| EQUAL->WRONG | other |",
    "|" + "---|" * 15,
]
for (name, ansi), c in sorted(trans.items()):
    total = sum(c.values())
    me = sum(v for (a, _), v in c.items() if a == "EQUAL")
    he = sum(v for (_, b), v in c.items() if b == "EQUAL")
    named = [
        ("WRONG", "EQUAL"),
        ("MISSED-RAISE", "EQUAL"),
        ("EQUAL", "REFUSE"),
        ("WRONG", "REFUSE"),
        ("MISSED-RAISE", "REFUSE"),
        ("REFUSE", "REFUSE"),
        ("REFUSE", "EQUAL"),
        ("EQUAL", "WRONG"),
    ]
    other = total - sum(c[p] for p in named) - c[("EQUAL", "EQUAL")]
    lines.append(
        f"| {name} | {ansi} | {total} | {me} | {he} | "
        + " | ".join(str(c[p]) for p in named)
        + f" | {other} |"
    )
classes = Counter(r["class"] for r in rows if r["class"])
lines.append("")
lines.append("| class | cells |")
lines.append("|---|---|")
for k, v in sorted(classes.items()):
    lines.append(f"| {k} | {v} |")
(OUT / "bar-summary.md").write_text("\n".join(lines) + "\n")
print("\n".join(lines))

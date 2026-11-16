import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import cells

raw = Path(sys.argv[1])
out = Path(sys.argv[2])
FAMILY = {k.name: k.family for k in cells.KINDS}


def load(name: str) -> dict:
    path = raw / f"{name}.jsonl"
    rows = {}
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if r["key"] in ("provenance", "elapsed") or r["key"].endswith("/setup"):
            continue
        if r.get("pos") == "explain":
            continue
        rows[r["key"]] = r
    return rows


def raised(v: object) -> bool:
    return isinstance(v, str) and v.startswith(("ERR:", "SETUP-ERR:"))


def verdict(spark: dict, mine: dict) -> str:
    a, b = spark.get("result"), mine.get("result")
    a_stmt, b_stmt = spark.get("statement"), mine.get("statement")
    if a_stmt is not None or b_stmt is not None:
        a_fail, b_fail = raised(a_stmt), raised(b_stmt)
        if a_fail and b_fail:
            return "both_raise"
        if a_fail:
            return "missed_raise"
        if b_fail:
            return "over_raise"
        return "agree" if a == b else "wrong_write"
    if raised(a) and raised(b):
        return "both_raise"
    if raised(a):
        return "missed_raise"
    if raised(b):
        return "over_raise"
    return "agree" if a == b else "wrong_answer"


def native_key(key: str) -> str:
    return key.replace("/native_view/", "/view/").replace("/native_parquet/", "/parquet/")


records = []
for ansi in ("false", "true"):
    spark = load(f"spark-{ansi}")
    mine = load(f"repark-{ansi}")
    for key in sorted(set(spark) | set(mine)):
        s, m = spark.get(key), mine.get(key)
        if s is None or m is None:
            v = "unpaired"
            s = s or {}
            m = m or {}
        else:
            v = verdict(s, m)
        base = m or s
        records.append(
            {
                "ansi": ansi,
                "engine": "repark",
                "key": key,
                "kind": base.get("kind"),
                "family": FAMILY.get(base.get("kind")),
                "storage": base.get("storage"),
                "door": base.get("door"),
                "pos": base.get("pos"),
                "sql": base.get("sql"),
                "spark": s.get("result"),
                "repark": m.get("result"),
                "spark_stmt": s.get("statement"),
                "repark_stmt": m.get("statement"),
                "verdict": v,
            }
        )
    native = load("native")
    for key, m in sorted(native.items()):
        s = spark.get(native_key(key))
        if s is None:
            continue
        records.append(
            {
                "ansi": ansi,
                "engine": "native",
                "key": key,
                "kind": m.get("kind"),
                "family": FAMILY.get(m.get("kind")),
                "storage": m.get("storage"),
                "door": "native",
                "pos": m.get("pos"),
                "sql": m.get("sql"),
                "spark": s.get("result"),
                "repark": m.get("result"),
                "verdict": verdict(s, m),
            }
        )
SLIM = ("key", "ansi", "engine", "verdict")
with (out / "verdicts.jsonl").open("w") as fh:
    for r in records:
        fh.write(json.dumps({k: r[k] for k in SLIM}, sort_keys=True) + "\n")

BAD = ("wrong_answer", "wrong_write", "missed_raise", "over_raise", "unpaired")


def table(field: str, engine: str) -> list:
    counts = defaultdict(Counter)
    for r in records:
        if r["engine"] != engine:
            continue
        counts[(r[field], r["ansi"])][r["verdict"]] += 1
    lines = [
        f"| {field} | ANSI | cells | agree | both raise | wrong answer | wrong write "
        "| missed raise | over-raise |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for (value, ansi), c in sorted(counts.items(), key=lambda x: (str(x[0][0]), x[0][1])):
        total = sum(c.values())
        lines.append(
            f"| {value} | {ansi} | {total} | {c['agree']} | {c['both_raise']} | "
            f"{c['wrong_answer']} | {c['wrong_write']} | {c['missed_raise']} | {c['over_raise']} |"
        )
    return lines


report = []
for engine in ("repark", "native"):
    for field in ("kind", "family", "storage", "door", "pos"):
        if engine == "native" and field in ("door", "family"):
            continue
        report.append(f"### {engine}: by {field}\n")
        report.extend(table(field, engine))
        report.append("")
totals = Counter((r["engine"], r["ansi"], r["verdict"]) for r in records)
report.append("### totals\n")
for k, v in sorted(totals.items()):
    report.append(f"- {k[0]} ANSI {k[1]} {k[2]}: {v}")
report.append("\n### wrong writes and DML cells\n")
for r in records:
    if r["storage"] in ("ice_cow", "ice_mor"):
        report.append(
            f"- `{r['key']}` ANSI {r['ansi']}: **{r['verdict']}**; Spark "
            f"{r['spark_stmt'] if raised(r['spark_stmt']) else r['spark']}; RePark "
            f"{r['repark_stmt'] if raised(r['repark_stmt']) else r['repark']}"
        )
(out / "summary.md").write_text("\n".join(report) + "\n")
print("\n".join(f"{k}: {v}" for k, v in sorted(totals.items())))


def held(engine: str, ansi: str) -> dict:
    path = raw / f"held-{engine}-{ansi}.jsonl"
    if not path.exists():
        return {}
    rows = {}
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if r["key"] != "provenance":
            rows[r["key"]] = r["result"]
    return rows


def fmt(v: object) -> str:
    if raised(v):
        return "raises " + str(v).split(":")[1]
    return "`" + json.dumps(v).replace("|", "/") + "`"


lines = [
    "### kernel check: the cast compared with a column holding the constant (no unwrap possible)\n",
    "| kind | ANSI | held cells | RePark held = Spark literal | Spark held = Spark literal |",
    "|---|---|---|---|---|",
]
for ansi in ("false", "true"):
    spark_lit = load(f"spark-{ansi}")
    rh, sh = held("repark", ansi), held("spark", ansi)
    per = defaultdict(lambda: [0, 0, 0])
    for key, value in rh.items():
        kind = key.split("/")[0]
        lit = spark_lit.get(key, {}).get("result")
        per[kind][0] += 1
        same = value == lit or (raised(value) and raised(lit))
        per[kind][1] += int(same)
        sv = sh.get(key)
        per[kind][2] += int(sv == lit or (raised(sv) and raised(lit)))
    for kind, (n, r_ok, s_ok) in sorted(per.items()):
        lines.append(f"| {kind} | {ansi} | {n} | {r_ok} | {s_ok if sh else 'not run'} |")
lines.append(
    "\n### headline cells (temp view, `CAST(c AS T) = <first constant>`, constant on the right)\n"
)
lines.append("| kind | SQL | Spark ANSI off | RePark ANSI off | Spark ANSI on | RePark ANSI on |")
lines.append("|---|---|---|---|---|---|")
by_key = {(r["engine"], r["ansi"], r["key"]): r for r in records}
for kind in cells.KINDS:
    key = f"{kind.name}/view/where/c0/eq/r"
    f = by_key.get(("repark", "false", key))
    t = by_key.get(("repark", "true", key))
    if f is None or t is None:
        continue
    pred = cells.predicate(kind, "eq", kind.consts[0], "r")
    lines.append(
        f"| {kind.name} | `{pred}` | {fmt(f['spark'])} | {fmt(f['repark'])} | "
        f"{fmt(t['spark'])} | {fmt(t['repark'])} |"
    )
with (out / "summary.md").open("a") as fh:
    fh.write("\n" + "\n".join(lines) + "\n")


ROWS = {k.name: len(k.values) for k in cells.KINDS}


def changed(kind: str, stmt: str, rec: dict) -> str:
    status, rows = rec.get("statement"), rec.get("result")
    if raised(status):
        return "raises " + str(status).split(":")[1]
    if not isinstance(rows, list):
        return str(rows)[:40]
    if stmt.startswith("delete"):
        kept = {int(x.split(":")[0]) for x in rows}
        return str(sorted(set(range(1, ROWS[kind] + 1)) - kept))
    return str(sorted(int(x.split(":")[0]) for x in rows if not x.endswith(":o")))


def dml_verdict(cols: list) -> str:
    s_raise = cols[0].startswith("raises") or cols[2].startswith("raises")
    r_raise = cols[1].startswith("raises") or cols[3].startswith("raises")
    if s_raise and r_raise:
        return "both raise"
    if s_raise:
        return "**missed raise**"
    if r_raise:
        return "**over-raise**"
    return "agree" if cols[0] == cols[1] and cols[2] == cols[3] else "**wrong write**"


dml = ["### DML: the ids each statement deleted or retagged\n"]
for ansi in ("false", "true"):
    spark, mine = load(f"spark-{ansi}"), load(f"repark-{ansi}")
    dml.append(f"\nANSI {ansi}.\n")
    dml.append("| kind | statement | Spark CoW | RePark CoW | Spark MoR | RePark MoR | verdict |")
    dml.append("|---|---|---|---|---|---|---|")
    pairs = sorted({(k.split("/")[0], k.split("/")[3]) for k in spark if "/dml/" in k})
    for kind, stmt in pairs:
        cols = []
        for mode in ("cow", "mor"):
            key = f"{kind}/dml/{mode}/{stmt}"
            cols.append(changed(kind, stmt, spark.get(key, {})))
            cols.append(changed(kind, stmt, mine.get(key, {})))
        dml.append(f"| {kind} | {stmt} | " + " | ".join(cols) + f" | {dml_verdict(cols)} |")
with (out / "summary.md").open("a") as fh:
    fh.write("\n" + "\n".join(dml) + "\n")

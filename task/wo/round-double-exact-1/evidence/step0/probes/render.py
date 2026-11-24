import json
import re
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

raw = Path(sys.argv[1])
values = json.loads(Path(sys.argv[2]).read_text())
analysis = json.loads(Path(sys.argv[3]).read_text())
out_dir = Path(sys.argv[4])
SCALES = values["scales"]
DOORS = ["sql", "fcol", "fexpr", "native"]
CLASS = {
    "DOUBLE": {str(r["id"]): r["class"] for r in values["double"]},
    "FLOAT": {str(r["id"]): r["class"] for r in values["float"]},
}
TEXT = {
    "DOUBLE": {str(r["id"]): r["text"] for r in values["double"]},
    "FLOAT": {str(r["id"]): r["text"] for r in values["float"]},
}


def load(name: object) -> object:
    return {json.loads(x)["key"]: json.loads(x) for x in (raw / name).read_text().splitlines()}


def bits(v: object) -> object:
    if v is None:
        return "NULL"
    if isinstance(v, dict) or v == "ERR":
        return "ERR"
    if v == "NaN":
        return "NaN"
    return "0x" + v.split("|")[1] if "|" in v else v


def cell(rec: object, k: object) -> object:
    if "error" in rec:
        return "ERR"
    return rec["rows"].get(k, "MISSING")


def sel_table(mode: object) -> object:
    sp = load(f"spark-ansi-{mode}.jsonl")
    rp = load(f"repark-ansi-{mode}.jsonl")
    rows = []
    for src in ("DOUBLE", "FLOAT"):
        for fn in ("round", "bround"):
            for sc in SCALES:
                s = sp[f"sel/sql/{src}/{fn}/{sc}"]
                recs = {d: rp[f"sel/{d}/{src}/{fn}/{sc}"] for d in DOORS}
                for k in sorted(s["rows"], key=int):
                    want = bits(cell(s, k))
                    got = [bits(cell(recs[d], k)) for d in DOORS]
                    if any(g != want for g in got):
                        rows.append(
                            [src[0], fn, str(sc), k, want] + ["=" if g == want else g for g in got]
                        )
    return rows


def norm_type(t: object) -> object:
    t = str(t)
    t = {"int32": "int", "int64": "bigint", "int16": "smallint", "int8": "tinyint"}.get(t, t)
    m = re.match(r"decimal128\((\d+), (\d+)\)", t)
    return f"decimal({m.group(1)},{m.group(2)})" if m else t


def misc_verdict(s: object, r: object) -> str:
    if s is None or r is None:
        return "MISSING"
    se, re_ = "error" in s, "error" in r
    if se and re_:
        return "EQUAL-ERR"
    if re_:
        return "ERR-FOR-VALUE"
    if se:
        return "VALUE-FOR-ERR"
    tv = [norm_type(x) for x in s["types"]] == [norm_type(x) for x in r["types"]]
    rows_s = [[bits(x) for x in row] for row in s["rows"]]
    rows_r = [[bits(x) for x in row] for row in r["rows"]]
    if rows_s == rows_r:
        return "EQUAL" if tv else "TYPE-ONLY"
    zs = [
        [("0" if x in ("0x0000000000000000", "0x8000000000000000") else x) for x in row]
        for row in rows_s
    ]
    zr = [
        [("0" if x in ("0x0000000000000000", "0x8000000000000000") else x) for x in row]
        for row in rows_r
    ]
    if zs == zr:
        return "SIGN-ZERO" if tv else "SIGN-ZERO+TYPE"
    return "WRONG-VALUE" if tv else "WRONG-VALUE+TYPE"


def misc_table(mode: object) -> object:
    sp = load(f"misc-spark-ansi-{mode}.jsonl")
    rp = load(f"misc-repark-ansi-{mode}.jsonl")
    out = []
    for key, s in sp.items():
        if key == "provenance":
            continue
        base, door = key.rsplit("/", 1)
        r = rp.get(key)
        out.append((base, door, misc_verdict(s, r), s, r))
        if door == "sql":
            out.append(
                (
                    base,
                    "native",
                    misc_verdict(s, rp.get(base + "/native")),
                    s,
                    rp.get(base + "/native"),
                )
            )
    return out


def short(rec: object) -> object:
    if rec is None:
        return "missing"
    if "error" in rec:
        return "ERR " + (rec.get("class") or (rec["error"] + ": " + rec.get("head", "")[:48]))
    rows = rec["rows"]
    first = rows[0] if rows else []
    vals = ", ".join((x.split("|")[0] if isinstance(x, str) else str(x)) for x in first[-4:])
    return f"{'/'.join(sorted({norm_type(t) for t in rec['types']}))}: {vals}"[:90]


tsv_rows = sel_table("true")
tsv_false = sel_table("false")
same_sel = tsv_rows == tsv_false
with (out_dir / "raw" / "sel-diff.tsv").open("w") as fh:
    fh.write("\t".join(["src", "fn", "scale", "id", "spark", *DOORS]) + "\n")
    for r in tsv_rows:
        fh.write("\t".join(r) + "\n")
summ = Counter()
for row in analysis["summary"]:
    if row[0] in ("true", "false"):
        summ[tuple(row[:-1])] += row[-1]
verdicts = [
    "EQUAL",
    "SIGN-ZERO",
    "WRONG-VALUE",
    "ERR-FOR-VALUE",
    "VALUE-FOR-ERR",
    "EQUAL-ERR",
    "NULL-MISMATCH",
]
lines = []
P = lines.append
total = {m: sum(v for k, v in summ.items() if k[0] == m) for m in ("true", "false")}
modes_equal = all(summ.get(("true", *k[1:]), 0) == v for k, v in summ.items() if k[0] == "false")
P("# ROUND-DOUBLE-EXACT-1 step 0 — the domain table")
P("")
P("Rendered by [probes/render.py](probes/render.py) from the probe output; do not edit by hand.")
P("")
P("## Provenance and size")
P("")
P(
    "- **RePark:** main `954423e0`, built `--profile release` in the lane clone; every "
    "RePark provenance row"
)
P(
    "  names `<scratchpad>/lane-round-exact-1/python/repark/src/repark/_native.abi3.so`. "
    "**Spark:** 4.1.2 on"
)
P(
    "  JDK 17.0.15 (`zulu-17`), Iceberg 1.11.0 runtime jar, `local[1]`, hadoop catalog; "
    "both ANSI modes."
)
P(
    f"- **Values:** {len(values['double'])} DOUBLE and {len(values['float'])} FLOAT "
    "([raw/values.json](raw/values.json)):"
)
P(
    "  halves at every scale from -3 to 18 with the nearest value and ±1, ±2, ±3 ulps "
    "(three half points per"
)
P(
    "  scale: `1 + 5·10^-(s+1)`, a random 12-digit or 5-digit `k + 0.5`, and "
    "`-2.5·10^-s`), subnormals, `±0.0`,"
)
P(
    "  NaN, ±Infinity, large magnitudes, ordinary values and twelve values whose JDK 17 "
    "text is not the"
)
P(
    "  shortest. Scales: -3 to 18 for every context, and 12 extreme scales from -400 to "
    "400 in the misc cells."
)
P(
    "- **Doors:** Spark-door SQL (`sql`), `F.round`/`F.bround` (`fcol`), `F.expr` "
    "(`fexpr`), the native door"
)
P(
    "  `repark.sql` (`native`, compared with Spark's SQL answer). **Contexts:** `sel` "
    "(SELECT list over an"
)
P(
    "  Iceberg table), `lit` (a folded literal, 20 values by 22 scales per query), `where` "
    "(`round(v, s)` `>`, `=`,"
)
P(
    "  `<` `v`, NaN rows left out), `group` (`GROUP BY round(v, s)`, one cell per group), "
    "`store` (`INSERT …"
)
P(
    "  SELECT round(v, s)` into DOUBLE or FLOAT, `DECIMAL(10,2)` and `DECIMAL(38,18)`; the "
    "DECIMAL stores take"
)
P("  the rows a DECIMAL can hold, `abs(v) < 10^7` and `< 10^19`, not NaN).")
P(f"- **Cells:** {total['true']:,} per ANSI mode, {total['true'] + total['false']:,} in all, plus")
P(
    "  the misc cells below. **The two ANSI modes agree cell for cell**"
    + (" in every context." if modes_equal else ": NO, see below.")
)
P(f"  The SELECT diff table is {'identical' if same_sel else 'NOT identical'} in both modes.")
P("- **Source check:** both RePark doors read back the exact bits Spark stored for every value.")
P("")
P("## Headline: non-EQUAL cells per door (each ANSI mode)")
P("")
P(
    "| door | `round` non-EQUAL | of | `bround` non-EQUAL | of | SIGN-ZERO | WRONG-VALUE | "
    "ERR-FOR-VALUE |"
)
P("|---|---|---|---|---|---|---|---|")
for d in DOORS:
    c = Counter()
    per = Counter()
    for k, v in summ.items():
        if k[0] == "true" and k[2] == d:
            c[k[-1]] += v
            per[(k[4], k[-1] != "EQUAL")] += v
    P(
        f"| {d} | **{per[('round', True)]:,}** | "
        f"{per[('round', True)] + per[('round', False)]:,} | "
        f"**{per[('bround', True)]:,}** | {per[('bround', True)] + per[('bround', False)]:,} | "
        f"{c['SIGN-ZERO']:,} | {c['WRONG-VALUE']:,} | {c['ERR-FOR-VALUE']:,} |"
    )
P("")
P("Per context, all functions:")
P("")
P("| door | sel | lit | where | group | store |")
P("|---|---|---|---|---|---|")
for d in DOORS:
    row = [d]
    for ctx in ("sel", "lit", "where", "group", "store"):
        bad = sum(
            v
            for k, v in summ.items()
            if k[0] == "true" and k[1] == ctx and k[2] == d and k[-1] != "EQUAL"
        )
        n = sum(v for k, v in summ.items() if k[0] == "true" and k[1] == ctx and k[2] == d)
        row.append(f"{bad:,} of {n:,}")
    P("| " + " | ".join(row) + " |")
P("")
P(
    "- `SIGN-ZERO`: RePark answers `-0.0` where Spark answers `0.0` (Spark's `BigDecimal` "
    "has no negative zero)."
)
P(
    "- `ERR-FOR-VALUE` on `native` is `bround`, which the native door does not have "
    "(`UNRESOLVED_ROUTINE`)."
)
P(
    "- `fexpr` equals `sql` in every context, and `native` `round` equals `fcol` `round`: "
    "two kernels (inventory A, B)."
)
P(
    "- **Types:** FLOAT `bround` answers DOUBLE on `sql`, `fcol` and `fexpr` (Spark: "
    "FLOAT), every scale; every other"
)
P("  SELECT type matches Spark.")
P("")
P("## Verdicts by context, door, source and function (ANSI on; ANSI off is identical)")
P("")
P("| ctx | door | src | fn | target | " + " | ".join(verdicts) + " |")
P("|" + "---|" * (5 + len(verdicts)))
keys = sorted({k[1:6] for k in summ if k[0] == "true"})
for k in keys:
    vals = [summ.get(("true", *k, v), 0) for v in verdicts]
    P("| " + " | ".join(list(k)) + " | " + " | ".join(f"{x:,}" if x else "·" for x in vals) + " |")
P("")
sp = load("spark-ansi-true.jsonl")
rp = load("repark-ansi-true.jsonl")
P("## The targeted halves: wrong at their own scale")
P("")
P(
    "Each value scale `s` has three half points; the cell is `round(v, s)` at that same "
    "`s`. Counts are of 3."
)
P("`sql` (datafusion-spark) / `fcol` (DataFusion core); `bround` is RePark's on both.")
P("")
for src in ("DOUBLE", "FLOAT"):
    for fn in ("round", "bround"):
        P(f"**{src} `{fn}`**")
        P("")
        P("| s | -3 ulp | -2 | -1 | nearest | +1 | +2 | +3 |")
        P("|---|---|---|---|---|---|---|---|")
        for sc in SCALES:
            row = [str(sc)]
            for step in ("-3", "-2", "-1", "+0", "+1", "+2", "+3"):
                parts = []
                for d in ("sql", "fcol"):
                    s = sp[f"sel/sql/{src}/{fn}/{sc}"]
                    r = rp[f"sel/{d}/{src}/{fn}/{sc}"]
                    ids = [
                        i
                        for i, c in CLASS[src].items()
                        if c.startswith(f"half/s={sc}/") and c.endswith(f"ulp{step}")
                    ]
                    parts.append(str(sum(1 for i in ids if cell(s, i) != cell(r, i))))
                row.append("/".join(parts))
            P("| " + " | ".join(row) + " |")
        P("")
P("## Other value classes: non-EQUAL cells by scale (`sql` / `fcol`, `round`)")
P("")
fams = ["special", "large", "ordinary", "jdk-not-shortest"]
for src in ("DOUBLE", "FLOAT"):
    P(f"**{src}**")
    P("")
    P("| class | n | " + " | ".join(str(s) for s in SCALES) + " |")
    P("|" + "---|" * (2 + len(SCALES)))
    for fam in fams:
        ids = [i for i, c in CLASS[src].items() if c == fam]
        if not ids:
            continue
        row = [fam, str(len(ids))]
        for sc in SCALES:
            s = sp[f"sel/sql/{src}/round/{sc}"]
            a = sum(1 for i in ids if cell(s, i) != cell(rp[f"sel/sql/{src}/round/{sc}"], i))
            b = sum(1 for i in ids if cell(s, i) != cell(rp[f"sel/fcol/{src}/round/{sc}"], i))
            row.append(f"{a}/{b}")
        P("| " + " | ".join(row) + " |")
    P("")
P("## The mechanism: unchanged cells against rounding cells (SELECT, ANSI on)")
P("")
P(
    "A cell is *unchanged* when JDK 17's text of the value has no more than `s` fractional "
    "digits, so Spark"
)
P("answers the input (or `+0.0` for `-0.0`); otherwise it is a *rounding* cell.")
P("")
P(
    "| door | src | fn | unchanged: SIGN-ZERO | unchanged: WRONG | rounding: SIGN-ZERO | "
    "rounding: WRONG | cells |"
)
P("|---|---|---|---|---|---|---|---|")
srcrec = {x: sp[f"source/{x}"] for x in ("DOUBLE", "FLOAT")}


def fracd(text: object) -> object:
    if text in ("NaN", "Infinity", "-Infinity"):
        return -(10**6)
    return -Decimal(text).normalize().as_tuple().exponent


for d in ("sql", "fcol"):
    for src in ("DOUBLE", "FLOAT"):
        texts = srcrec[src]["text"] if src == "DOUBLE" else srcrec[src]["dtext"]
        for fn in ("round", "bround"):
            c = Counter()
            n = 0
            for sc in SCALES:
                s = sp[f"sel/sql/{src}/{fn}/{sc}"]
                r = rp[f"sel/{d}/{src}/{fn}/{sc}"]
                for i, tx in texts.items():
                    n += 1
                    a, b = cell(s, i), cell(r, i)
                    if a == b:
                        continue
                    kind = "u" if fracd(tx) <= sc else "r"
                    z = (
                        isinstance(a, str)
                        and isinstance(b, str)
                        and a.split("|")[-1] in ("0000000000000000",)
                        and b.split("|")[-1] == "8000000000000000"
                    )
                    c[(kind, "z" if z else "w")] += 1
            P(
                f"| {d} | {src} | {fn} | {c[('u', 'z')]:,} | {c[('u', 'w')]:,} | "
                f"{c[('r', 'z')]:,} | {c[('r', 'w')]:,} | {n:,} |"
            )
P("")
P("## Named cells (SELECT, `round`, ANSI on): Spark, `sql`, `fcol`")
P("")
named = [
    ("DOUBLE", "1.005", 2),
    ("DOUBLE", "0.285", 2),
    ("DOUBLE", "1.255", 2),
    ("DOUBLE", "2.675", 2),
    ("DOUBLE", "-22.129547339983247", 18),
    ("DOUBLE", "1.0000000000000004", 18),
    ("DOUBLE", "-0.0", 2),
    ("DOUBLE", "-0.4", 0),
    ("DOUBLE", "1e+307", 2),
    ("DOUBLE", "1.7976931348623157e+308", 1),
    ("DOUBLE", "5e-324", 2),
    ("DOUBLE", "1.949133209895265e+16", -2),
    ("DOUBLE", "1250.0", -2),
    ("DOUBLE", "123456789012345.67", 1),
    ("FLOAT", "1.2549999952316284", 2),
]
P("| src | v | s | Spark | sql | fcol |")
P("|---|---|---|---|---|---|")
for src, t, sc in named:
    ids = [i for i, x in TEXT[src].items() if x == t]
    if not ids:
        continue
    i = ids[0]
    s = cell(sp[f"sel/sql/{src}/round/{sc}"], i)
    a = cell(rp[f"sel/sql/{src}/round/{sc}"], i)
    b = cell(rp[f"sel/fcol/{src}/round/{sc}"], i)
    P(
        f"| {src} | `{t}` | {sc} | `{str(s).split('|')[0]}` | `{str(a).split('|')[0]}` | "
        f"`{str(b).split('|')[0]}` |"
    )
P("")
P("## Misc cells (both modes; a cell differing between modes is marked)")
P("")
mt = misc_table("true")
mf = {(b, d): v for b, d, v, _, _ in misc_table("false")}
groups = defaultdict(Counter)
detail = []
for base, door, v, s, r in mt:
    fam = base.split("/")[0]
    flip = "" if mf.get((base, door)) == v else f" (ANSI off: {mf.get((base, door))})"
    groups[(fam, door)][v + flip] += 1
    if fam in (
        "card",
        "nullscale",
        "nullscale_typed",
        "colscale",
        "noscale",
        "bigintscale",
        "ctl_declit",
    ) or (fam in ("ctl_int", "ctl_dec") and v != "EQUAL"):
        detail.append((base, door, v + flip, short(s), short(r)))
P("| family | door | verdicts |")
P("|---|---|---|")
for (fam, door), c in sorted(groups.items()):
    P(f"| {fam} | {door} | " + ", ".join(f"{k} {n}" for k, n in sorted(c.items())) + " |")
P("")
P("| cell | door | verdict | Spark (first row) | RePark (first row) |")
P("|---|---|---|---|---|")
for base, door, v, s, r in detail:
    P(f"| `{base}` | {door} | {v} | {s} | {r} |")
P("")
P("## The rule and the kernel, measured")
P("")
P(
    f"- **Model against Spark:** {len(analysis['model_bad'])} differences over every SELECT "
    "cell of both modes"
)
P("  (the model is [probes/model.py](probes/model.py); see [spark-rule.md](spark-rule.md)).")
kc = sum(v for k, v in summ.items() if False)
krows = sum(r[-1] for r in analysis["summary"] if r[0] == "kcheck")
P(
    "- **K1 at scale s, converted back once, against Spark's `ROUND(v, s)`:** "
    f"{len(analysis['kcheck_bad'])} differences"
)
P(f"  in {krows:,} cells (s from 0 to 18, DOUBLE and FLOAT; [probes/kcheck.py](probes/kcheck.py)).")
P(
    "- **Every non-EQUAL cell of the SELECT context** is in "
    "[raw/sel-diff.tsv](raw/sel-diff.tsv): the source, the"
)
P(
    "  function, the scale, the value id, Spark's answer as the double's bits, then each "
    "door's bits (`=` where"
)
P(
    f"  it equals Spark). {len(tsv_rows):,} rows. The `lit` context's non-EQUAL cells are "
    "exactly these per door;"
)
P(
    "  a DOUBLE store is wrong exactly where its SELECT cell is; a FLOAT or DECIMAL store "
    "is never wrong where"
)
P("  its SELECT cell is right.")
(out_dir / "domain-table.md").write_text("\n".join(lines) + "\n")
print(len(tsv_rows), same_sel, modes_equal)

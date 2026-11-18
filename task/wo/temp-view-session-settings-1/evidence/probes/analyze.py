import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from cells import DOOR_ORDER

raw, out = sys.argv[1:3]
WRONG = {"VALUE", "TYPE", "REPARK_ANSWERS", "REPARK_RAISES", "CREATE_DIFF"}


def load(name: str) -> dict:
    rows = [json.loads(line) for line in Path(raw + "/" + name).read_text().splitlines()]
    return {r["cell"]: r for r in rows if r["cell"] != "provenance"}


def canon(res: object) -> str:
    if res is None:
        return "OK"
    if "error" in res:
        return "RAISE " + (res["error"] or res["text"][:60])
    return json.dumps([res["rows"], res["types"]])


def verdict(sp: object, rp: object) -> str:
    if sp is None and rp is None:
        return "EQUAL"
    if sp is None or rp is None:
        return "CREATE_DIFF"
    se, re_ = "error" in sp, "error" in rp
    if se and re_:
        return "BOTH_RAISE" if sp["error"] and sp["error"] == rp["error"] else "BOTH_RAISE_OTHER"
    if se:
        return "REPARK_ANSWERS"
    if re_:
        return "REPARK_RAISES"
    if sp["rows"] == rp["rows"]:
        return "EQUAL" if sp["types"] == rp["types"] else "TYPE"
    return "VALUE"


def directs(cells: dict) -> dict:
    found = {}
    for r in cells.values():
        if r.get("phase") == "direct":
            found[(r["family"], r["body"], r["read"])] = canon(r["result"])
    return found


def follows(r: dict, d: dict) -> str:
    if r.get("phase") not in ("read", "write") or "created" not in r:
        return ""
    c = d.get((r["family"], r["body"], r["created"]))
    w = d.get((r["family"], r["body"], r["read"]))
    if c is None or w is None:
        return "?"
    v = canon(r["result"])
    if c == w:
        return "same"
    if v == c:
        return "creation"
    if v == w:
        return "reader"
    return "neither"


def agrees(a: object, b: object) -> bool:
    return a == b or (
        isinstance(a, str)
        and isinstance(b, str)
        and a.startswith("RAISE")
        and b.startswith("RAISE")
    )


def pair(spark: dict, build: dict, label: str, keep: object) -> list:
    ds, dr = directs(spark), directs(build)
    out_rows = []
    for cell, sr in spark.items():
        if cell not in build or not keep(sr):
            continue
        rr = build[cell]
        v = verdict(sr["result"], rr["result"])
        direct_agree = ""
        if sr.get("family") in ("ansi", "tz", "cs", "tpp", "dec", "tst", "ns") and "body" in sr:
            keys = {
                (sr["family"], sr["body"], sr.get("created")),
                (sr["family"], sr["body"], sr.get("read")),
            }
            agree = all(agrees(ds.get(k), dr.get(k)) for k in keys if k[2] is not None and k in ds)
            direct_agree = "yes" if agree else "no"
        out_rows.append(
            {
                "cell": cell,
                "build": label,
                "family": sr.get("family", ""),
                "phase": sr.get("phase", ""),
                "door": sr.get("door", ""),
                "pos": sr.get("pos", ""),
                "body": sr.get("body", ""),
                "created": sr.get("created", ""),
                "read": sr.get("read", ""),
                "verdict": v,
                "direct_agree": direct_agree,
                "spark": canon(sr["result"]),
                "repark": canon(rr["result"]),
                "spark_follows": follows(sr, ds),
                "repark_follows": follows(rr, dr),
            }
        )
    return out_rows


spark_true, main_true, head_avg = (
    load("spark-true.jsonl"),
    load("main-true.jsonl"),
    load("head-avg-true.jsonl"),
)
spark_unset, main_unset = load("spark-unset.jsonl"), load("main-unset.jsonl")
rows = pair(spark_true, main_true, "main", lambda r: True)
rows += pair(spark_true, head_avg, "head-1355", lambda r: r.get("body") == "avg")
rows += pair(spark_unset, main_unset, "main", lambda r: r.get("phase") in ("unset", "conf"))
for r in rows:
    if r["phase"] == "unset" or r["cell"].startswith("conf/"):
        r["family"] = "unset"
Path(out + "/verdicts.jsonl").write_text(
    "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows)
)

lines = ["# Summary (written by analyze.py)", ""]
measured = [
    r for r in rows if r["phase"] in ("read", "write", "nest", "reader", "now", "unset", "direct")
]
lines.append(
    "Cells paired: "
    + str(len(rows))
    + "; measured reads, writes and directs: "
    + str(len(measured))
    + "."
)
lines.append("")
lines.append("## Verdicts by build, family and phase")
lines.append("")
lines.append(
    "| build | family | phase | cells | EQUAL | BOTH_RAISE | BOTH_RAISE_OTHER | wrong | wrong, "
    "direct agrees |"
)
lines.append("|---|---|---|---|---|---|---|---|---|")
groups: dict = defaultdict(list)
for r in measured:
    groups[(r["build"], r["family"], r["phase"])].append(r)
for key in sorted(groups):
    g = groups[key]
    c = Counter(r["verdict"] for r in g)
    wrong = [r for r in g if r["verdict"] in WRONG]
    view = [r for r in wrong if r["direct_agree"] != "no"]
    lines.append(
        "| "
        + " | ".join(key)
        + " | "
        + " | ".join(
            str(x)
            for x in (
                len(g),
                c["EQUAL"],
                c["BOTH_RAISE"],
                c["BOTH_RAISE_OTHER"],
                len(wrong),
                len(view),
            )
        )
        + " |"
    )
lines.append("")
lines.append("## Wrong reads by door and direction (main; avg from #1355's package), direct agrees")
lines.append("")
lines.append(
    "| family | door | created → read | cells | wrong | Spark follows (creation/reader/same) | "
    "RePark follows (creation/reader/same/neither) |"
)
lines.append("|---|---|---|---|---|---|---|")
reads: dict = defaultdict(list)
for r in rows:
    if r["phase"] != "read" or (r["body"] == "avg" and r["build"] == "main"):
        continue
    reads[(r["family"], r["door"], r["created"] + " → " + r["read"])].append(r)
for key in sorted(reads):
    g = reads[key]
    if all(r["created"] == r["read"] for r in g):
        continue
    wrong = [r for r in g if r["verdict"] in WRONG and r["direct_agree"] != "no"]
    sf, rf = Counter(r["spark_follows"] for r in g), Counter(r["repark_follows"] for r in g)
    lines.append(
        "| "
        + " | ".join(key)
        + " | "
        + str(len(g))
        + " | "
        + str(len(wrong))
        + " | "
        + "/".join(str(sf[k]) for k in ("creation", "reader", "same"))
        + " | "
        + "/".join(str(rf[k]) for k in ("creation", "reader", "same", "neither"))
        + " |"
    )
lines.append("")
lines.append("## Follows grid (plain position): body by door; letters Spark/RePark")
lines.append("")
lines.append(
    "C creation, R reader, = not discriminating (the two directs agree), N neither, x missing. "
    "Two letters per created → read direction."
)
lines.append("")
for fam in ("ansi", "tz", "cs", "tpp", "dec", "tst", "ns"):
    fam_rows = [
        r
        for r in rows
        if r["family"] == fam
        and r["phase"] == "read"
        and r["pos"] == "plain"
        and r["created"] != r["read"]
        and not (r["body"] == "avg" and r["build"] == "main")
    ]
    if not fam_rows:
        continue
    doors = sorted({r["door"] for r in fam_rows}, key=DOOR_ORDER.index)
    bodies = sorted({r["body"] for r in fam_rows})
    lines.append("**" + fam + "**")
    lines.append("")
    lines.append("| body | " + " | ".join(doors) + " |")
    lines.append("|---|" + "---|" * len(doors))
    letter = {"creation": "C", "reader": "R", "same": "=", "neither": "N", "?": "x", "": "x"}
    for b in bodies:
        cellsx = []
        for d in doors:
            pick = sorted(
                (r for r in fam_rows if r["body"] == b and r["door"] == d),
                key=lambda r: r["created"],
            )
            cellsx.append(
                " ".join(
                    letter[r["spark_follows"]]
                    + "/"
                    + letter[r["repark_follows"]]
                    + ("!" if r["verdict"] in WRONG else "")
                    for r in pick
                )
                or "-"
            )
        lines.append("| " + b + " | " + " | ".join(cellsx) + " |")
    lines.append("")
lines.append("`!` marks a wrong cell (RePark differs from Spark).")
lines.append("")
for phase in ("write", "nest", "reader", "now", "unset"):
    pr = [r for r in rows if r["phase"] == phase]
    if not pr:
        continue
    lines.append("## Every " + phase + " cell")
    lines.append("")
    lines.append("| build | cell | verdict | Spark | RePark |")
    lines.append("|---|---|---|---|---|")
    for r in pr:
        lines.append(
            "| "
            + r["build"]
            + " | `"
            + r["cell"]
            + "` | "
            + r["verdict"]
            + " | "
            + r["spark"].replace("|", "/")
            + " | "
            + r["repark"].replace("|", "/")
            + " |"
        )
    lines.append("")
Path(out + "/summary.md").write_text("\n".join(lines) + "\n")
print("rows " + str(len(rows)))

RELATION_LEVEL = {"cs_twin", "cs_lower"}
SILENT = {"VALUE", "TYPE", "REPARK_ANSWERS"}


def in_scope(r: dict) -> bool:
    if "/create" in r["cell"]:
        return False
    if r["body"] == "avg":
        return r["build"] == "head-1355"
    return r["build"] == "main" and r["body"] not in RELATION_LEVEL


head = ["# Headline counts (written by analyze.py)", ""]
head.append(
    "Scope: cells where the created and read settings differ; `avg` from #1355's package, "
    "every other body from main;"
)
head.append(
    "the twin-column bodies `cs_twin` and `cs_lower` are left out (Spark refuses the relation "
    "itself under"
)
head.append(
    "`caseSensitive=false` at every door, a held frame included). A view gap is a wrong cell "
    "whose two direct"
)
head.append(
    "queries (the body under the created value and under the read value) agree with Spark "
    "(both raising counts as"
)
head.append("agreeing, whatever the error text).")
head.append("")
head.append(
    "| family | phase | door | cells | EQUAL or same raise | view gap, silent | view gap, loud "
    "| direct gap |"
)
head.append("|---|---|---|---|---|---|---|---|")
tally: dict = defaultdict(lambda: [0, 0, 0, 0, 0])
for r in rows:
    if r["phase"] not in ("read", "write", "nest", "reader") or not in_scope(r):
        continue
    if r["phase"] in ("read", "write") and r["created"] == r["read"]:
        continue
    t = tally[(r["family"], r["phase"], r["door"])]
    t[0] += 1
    if r["verdict"] in ("EQUAL", "BOTH_RAISE", "BOTH_RAISE_OTHER"):
        t[1] += 1
    elif r["direct_agree"] == "no":
        t[4] += 1
    elif r["verdict"] in SILENT:
        t[2] += 1
    else:
        t[3] += 1
totals = [0, 0, 0, 0, 0]
for key in sorted(
    tally, key=lambda k: (k[0], k[1], DOOR_ORDER.index(k[2]) if k[2] in DOOR_ORDER else 99, k[2])
):
    t = tally[key]
    totals = [a + b for a, b in zip(totals, t, strict=True)]
    head.append("| " + " | ".join(key) + " | " + " | ".join(str(x) for x in t) + " |")
head.append("| **all** | | | " + " | ".join("**" + str(x) + "**" for x in totals) + " |")
head.append("")
Path(out + "/headline.md").write_text("\n".join(head) + "\n")
print("headline " + " ".join(str(x) for x in totals))

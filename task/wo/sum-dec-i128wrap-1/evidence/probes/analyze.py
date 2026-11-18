import json
import re
import sys
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path

import cells

getcontext().prec = 120

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
COND = re.compile(r"\[([A-Z][A-Z_]+(?:\.[A-Z_]+)?)\]")
LETTER = {
    "EQUAL": "=",
    "TYPE": "T",
    "FORMAT": "F",
    "VALUE": "V",
    "REPARK_RAISES": "R",
    "SPARK_RAISES": "S",
    "BOTH_RAISE": "B",
    "BOTH_RAISE_OTHER": "b",
    "READ_ERR": "E",
}
TAG_LETTER = {"wrap_38": "w", "wrap_39": "W", "exact_39": "X", "exact_fits": "k"}
ORDER = list(LETTER)


def load(name: str) -> dict:
    path = src / name
    if not path.exists():
        return {}
    return {r["key"]: r for r in map(json.loads, path.open())}


def num(x: object) -> object:
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return x


def is_err(v: object) -> bool:
    return isinstance(v, str) and v.startswith(("ERR", "SETUP"))


def cond(v: str) -> str:
    m = COND.search(v)
    if m:
        return m.group(1).split(".")[0]
    parts = v.split(":")
    return parts[2] if len(parts) > 2 and parts[2] != "None" else parts[1] if len(parts) > 1 else v


def renorm(res: object) -> object:
    if not isinstance(res, dict) or "types" not in res:
        return res
    scales = []
    for ty in res["types"]:
        scales.append(int(ty[:-1].split(",")[1]) if ty.startswith("decimal(") else None)
    rows = []
    for row in res["rows"]:
        fixed = []
        for x, sc in zip(row, scales, strict=False):
            v = num(x)
            if sc is not None and isinstance(v, Decimal) and v.as_tuple().exponent < -sc:
                q = v.quantize(Decimal(1).scaleb(-sc))
                fixed.append(format(q, "f") if q == v else x)
            else:
                fixed.append(x)
        rows.append(fixed)
    return dict(res, rows=rows)


def rows_equal_numeric(a: list, b: list) -> bool:
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b, strict=True):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb, strict=True):
            if num(x) != num(y):
                return False
    return True


def table_of(pos: str) -> str:
    if pos.endswith("_s4"):
        return "s4"
    if pos.endswith("_p2"):
        return "p2"
    if "big" in pos:
        return "big"
    return "s"


def prec_scale(ty: str) -> tuple | None:
    if not ty.startswith("decimal("):
        return None
    p, s = ty[8:-1].split(",")
    return int(p), int(s)


def tag_value(x: str, ty: str, exact: int | None) -> str | None:
    ps = prec_scale(ty)
    v = num(x)
    if ps is None or not isinstance(v, Decimal):
        return None
    p, s = ps
    u = int(v.scaleb(s))
    past = abs(u) >= 10**p
    if exact is None:
        return "past_precision" if past else "in_precision"
    if u == exact:
        return "exact_39" if past else "exact_fits"
    if (u - exact) % (2**128) == 0:
        return "wrap_39" if past else "wrap_38"
    return "other_39" if past else "other"


def exact_for(rec: dict, group: object) -> int | None:
    col = rec.get("col")
    if group is None or not isinstance(col, str) or not col.startswith("c"):
        return None
    pos = rec.get("pos") or ""
    if pos.startswith(("win", "try_win", "linear", "plus1", "as_string")) or "window" in pos:
        return None
    p = int(col[1:].split("_")[0])
    try:
        return cells.exact_sum(table_of(pos), int(group), p)
    except KeyError:
        return None


def classify(sp: dict, rp: dict) -> str:
    a, b = sp.get("result"), rp.get("result")
    if sp.get("door") == "iceberg":
        sa, sb = sp.get("statement"), rp.get("statement")
        if is_err(sa) and is_err(sb):
            return "BOTH_RAISE" if cond(sa) == cond(sb) else "BOTH_RAISE_OTHER"
        if is_err(sa) != is_err(sb):
            return "REPARK_RAISES" if is_err(sb) else "SPARK_RAISES"
        if is_err(a) or is_err(b):
            return "READ_ERR"
        if sp.get("schema") != rp.get("schema"):
            return "TYPE" if rows_equal_numeric(a["rows"], b["rows"]) else "VALUE"
        if a["rows"] != b["rows"]:
            return "TYPE" if rows_equal_numeric(a["rows"], b["rows"]) else "VALUE"
        return "EQUAL"
    if is_err(a) and is_err(b):
        return "BOTH_RAISE" if cond(a) == cond(b) else "BOTH_RAISE_OTHER"
    if is_err(a):
        return "SPARK_RAISES"
    if is_err(b):
        return "REPARK_RAISES"
    if not isinstance(a, dict) or not isinstance(b, dict):
        return "EQUAL" if a == b else "VALUE"
    if a["types"] == b["types"] and a["rows"] == b["rows"]:
        return "EQUAL"
    if rows_equal_numeric(a["rows"], b["rows"]):
        return "TYPE" if a["types"] != b["types"] else "FORMAT"
    return "VALUE"


def value_tags(sp: dict, rp: dict, group: object) -> list:
    a, b = sp.get("result"), rp.get("result")
    if not isinstance(b, dict):
        return []
    exact = exact_for(rp, group)
    types = b.get("types") or []
    tags = []
    a_rows = a["rows"] if isinstance(a, dict) else None
    for i, row in enumerate(b["rows"]):
        other = a_rows[i] if a_rows is not None and i < len(a_rows) else None
        for j, x in enumerate(row):
            if j >= len(types) or not types[j].startswith("decimal("):
                continue
            if other is not None and j < len(other) and num(other[j]) == num(x):
                continue
            ex = exact
            if rp.get("door") == "iceberg" or rp.get("pos") == "grouped_all":
                ex = None
                if row and str(row[0]).lstrip("-").isdigit() and isinstance(rp.get("col"), str):
                    p = int(rp["col"][1:].split("_")[0])
                    table = "big" if "big" in (rp.get("pos") or "") else "s"
                    try:
                        ex = cells.exact_sum(table, int(row[0]), p)
                    except KeyError:
                        ex = None
            t = tag_value(x, types[j], ex)
            if t:
                tags.append(t)
    return tags


def wrapped_set(p: int) -> set:
    out = set()
    tables = [("s", g) for g in cells.SHAPES] + [("big", g) for g in cells.BIG_SHAPES]
    for table, g in tables:
        if table == "big" and p != 34:
            continue
        ex = cells.exact_sum(table, g, p)
        if ex is None:
            continue
        w = (ex + 2**127) % 2**128 - 2**127
        if w != ex:
            out.add(w)
    return out


def file_tags(rp: dict) -> list:
    files = rp.get("files")
    if not isinstance(files, list) or not isinstance(rp.get("col"), str):
        return []
    out = []
    p = int(rp["col"][1:].split("_")[0])
    wraps = wrapped_set(p)
    failed = is_err(rp.get("statement"))
    for f in files:
        ty = f["type"].replace("decimal128(", "decimal(").replace(" ", "")
        ps = prec_scale(ty)
        if ps is None:
            continue
        for v in f["values"]:
            d = num(v)
            if not isinstance(d, Decimal):
                continue
            u = int(d.scaleb(ps[1]))
            kind = None
            if u in wraps:
                kind = "file_wrap_39" if abs(u) >= 10 ** ps[0] else "file_wrap_38"
            elif abs(u) >= 10 ** ps[0]:
                kind = "file_past_precision"
            if kind:
                out.append(kind + ("_orphan" if failed else ""))
    return out


verdicts = []
summary = defaultdict(Counter)
grid = defaultdict(dict)
tagcount = defaultdict(Counter)
runs = [
    ("true", "spark-true.jsonl", "repark-true.jsonl", "spark_door"),
    ("false", "spark-false.jsonl", "repark-false.jsonl", "spark_door"),
    ("true", "spark-true.jsonl", "native.jsonl", "native"),
    ("false", "spark-false.jsonl", "native.jsonl", "native"),
    ("true", "spark-mp-true.jsonl", "repark-mp-true.jsonl", "spark_door"),
    ("false", "spark-mp-false.jsonl", "repark-mp-false.jsonl", "spark_door"),
]
for ansi, sname, rname, label in runs:
    sp_all, rp_all = load(sname), load(rname)
    for key, sp in sp_all.items():
        if key == "provenance" or key.startswith(("data/", "setup/", "conf/", "explain/")):
            continue
        rp = rp_all.get(key)
        if rp is None:
            continue
        sp = dict(sp, result=renorm(sp.get("result")))
        rp = dict(rp, result=renorm(rp.get("result")))
        v = classify(sp, rp)
        door = rp.get("door") if label == "native" else sp.get("door")
        group = sp.get("group")
        rec = {
            "key": key,
            "ansi": ansi,
            "run": label,
            "family": sp.get("family"),
            "door": door,
            "pos": sp.get("pos"),
            "col": sp.get("col"),
            "group": group,
            "verdict": v,
        }
        if v == "EQUAL" and sp.get("door") != "iceberg":
            pass
        elif isinstance(sp.get("result"), dict):
            rec["spark"] = sp["result"]
        elif is_err(sp.get("result")):
            rec["spark"] = sp["result"][:220]
        if v == "EQUAL" and sp.get("door") != "iceberg":
            pass
        elif isinstance(rp.get("result"), dict):
            rec["repark"] = rp["result"]
        elif is_err(rp.get("result")):
            rec["repark"] = rp["result"][:220]
        if sp.get("door") == "iceberg":
            for f in ("statement", "schema", "files"):
                rec[f"spark_{f}"] = sp.get(f)
                rec[f"repark_{f}"] = rp.get(f)
            rec["file_tags"] = sorted(set(file_tags(rp)))
        tags = []
        if v in ("VALUE", "SPARK_RAISES") or (sp.get("door") == "iceberg" and v != "EQUAL"):
            tags = value_tags(sp, rp, group)
        rec["tags"] = sorted(set(tags))
        verdicts.append(rec)
        summary[(label, ansi, rec["family"], door, rec["pos"])][v] += 1
        for t in set(tags):
            tagcount[(label, ansi, rec["family"], door, rec["pos"])][t] += 1
        letter = LETTER[v]
        for t in ("wrap_38", "wrap_39", "exact_39", "exact_fits"):
            if t in tags and v in ("VALUE", "SPARK_RAISES"):
                letter = TAG_LETTER[t]
                break
        grid[(label, ansi, rec["family"], door, rec["pos"])][(group, rec["col"])] = letter
with (dst / "verdicts.jsonl").open("w") as fh:
    for r in verdicts:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
lines = ["| run | ANSI | family | door | position | " + " | ".join(ORDER) + " | tags |"]
lines.append("|" + "---|" * (6 + len(ORDER)))
tot = defaultdict(Counter)
for k, c in sorted(summary.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
    tags = ", ".join(f"{t} {n}" for t, n in sorted(tagcount[k].items()))
    lines.append(
        "| "
        + " | ".join(str(x) for x in k)
        + " | "
        + " | ".join(str(c.get(o, 0)) for o in ORDER)
        + f" | {tags} |"
    )
    tot[(k[0], k[1])].update(c)
lines.append("")
for (label, ansi), c in sorted(tot.items()):
    lines.append(
        f"Totals {label} ANSI {ansi}: "
        + ", ".join(f"{o} {c.get(o, 0)}" for o in ORDER)
        + f", cells {sum(c.values())}"
    )
(dst / "summary.md").write_text("\n".join(lines) + "\n")
glines = [
    "Letters: `=` equal, `T` type, `F` format, `V` value, `R` RePark raises,",
    "`S` Spark raises and RePark answers, `B` both raise (same condition),",
    "`b` both raise (different condition), `E` read error; for `V`/`S`, the RePark value:",
    "`w` wrapped and inside the type, `W` wrapped and past it, `X` exact and past the type,",
    "`k` exact and inside it (Spark overflowed on its own path).",
    "",
]
for k in sorted(grid, key=lambda kk: tuple(str(x) for x in kk)):
    cellmap = grid[k]
    groups = sorted({g for g, _ in cellmap}, key=lambda x: (x is None, x if x is not None else 0))
    cols = []
    for _, c in cellmap:
        if c not in cols:
            cols.append(c)
    if len(cellmap) < 2:
        continue
    glines.append("### " + " / ".join(str(x) for x in k))
    glines.append("")
    glines.append("| group | " + " | ".join(cols) + " |")
    glines.append("|" + "---|" * (len(cols) + 1))
    for g in groups:
        name = (
            (
                cells.SHAPE_NAMES.get(g)
                or cells.P2_NAMES.get(g)
                or cells.BIG_NAMES.get(g)
                or cells.K_NAMES.get(g)
                or "all"
            )
            if g is not None
            else "all"
        )
        glines.append(
            f"| {g} {name} | " + " | ".join(cellmap.get((g, c), " ") for c in cols) + " |"
        )
    glines.append("")
(dst / "grids.md").write_text("\n".join(glines) + "\n")
for (label, ansi), c in sorted(tot.items()):
    print(label, ansi, sum(c.values()), dict(c))

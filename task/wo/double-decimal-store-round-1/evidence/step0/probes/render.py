import json
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

from analyze import cell_rows, short, verdict
from cells import EXTRA_DOORS, READ_DOORS, SOURCES, TARGETS, TEXTS, WRITE_DOORS

raw = Path(sys.argv[1])
MAIN = READ_DOORS + WRITE_DOORS
KINDS = ["EQUAL", "EQUAL-ERR", "WRONG-VALUE", "ERR-FOR-NULL", "NULL-FOR-ERR", "MISSING-VALUE"]


def load(name: str) -> dict:
    path = raw / name
    if not path.exists():
        return {}
    return {json.loads(x)["key"]: json.loads(x) for x in path.read_text().splitlines()}


def kind(value: str) -> str:
    if value.startswith("ERR"):
        return "ERR"
    if value == "NULL":
        return value
    return str(Decimal(value).normalize())


def pair(prefix: str, ansi: str) -> tuple:
    return load(f"{prefix}spark-ansi-{ansi}.jsonl"), load(f"{prefix}repark-ansi-{ansi}.jsonl")


def summary() -> None:
    print("| ANSI | door | source | " + " | ".join(KINDS) + " |")
    print("|---|---|---|" + "---|" * len(KINDS))
    for ansi in ("true", "false"):
        for prefix, doors in (("", MAIN), ("extra-", EXTRA_DOORS)):
            sp, rp = pair(prefix, ansi)
            for door in doors:
                for src in SOURCES:
                    c = Counter()
                    for p, sc in TARGETS:
                        key = f"{door}/{src}/{p},{sc}"
                        srows, rrows = cell_rows(sp.get(key)), cell_rows(rp.get(key))
                        for i in range(len(TEXTS)):
                            c[verdict(srows.get(str(i)), rrows.get(str(i)))] += 1
                    cells = " | ".join(str(c.get(k, 0)) for k in KINDS)
                    print(f"| {ansi} | `{door}` | {src} | {cells} |")


def matrix(ansi: str, src: str) -> None:
    sp, rp = pair("", ansi)
    ex_sp, ex_rp = pair("extra-", ansi)
    sp.update(ex_sp)
    rp.update(ex_rp)
    heads = " | ".join(f"({p},{s})" for p, s in TARGETS)
    print(f"| value | {heads} |")
    print("|---|" + "---|" * len(TARGETS))
    for i, text in enumerate(TEXTS):
        cols = []
        for p, sc in TARGETS:
            base = f"sel_cast_col/{src}/{p},{sc}"
            s_val = short(cell_rows(sp.get(base)).get(str(i)))
            r_val = short(cell_rows(rp.get(base)).get(str(i)))
            v = verdict(cell_rows(sp.get(base)).get(str(i)), cell_rows(rp.get(base)).get(str(i)))
            cell = s_val if v in ("EQUAL", "EQUAL-ERR") else f"{s_val} / **{r_val}**"
            odd = []
            for door in MAIN + EXTRA_DOORS:
                key = f"{door}/{src}/{p},{sc}"
                d_val = short(cell_rows(rp.get(key)).get(str(i)))
                if key in rp and kind(d_val) != kind(r_val):
                    odd.append(f"{door}: {d_val}")
            if odd:
                cell += " (" + "; ".join(odd) + ")"
            cols.append(cell)
        print(f"| `{text}` | " + " | ".join(cols) + " |")


def selects() -> None:
    print("| ANSI | cell | Spark | RePark |")
    print("|---|---|---|---|")
    for ansi in ("true", "false"):
        sp, rp = pair("selects-", ansi)
        for key, rec in sp.items():
            if not key.startswith("select/"):
                continue
            r = rp.get(key, {})
            sv = rec.get("values") or [rec.get("class") or rec.get("head", "")[:60]]
            rv = r.get("values") or [r.get("class") or r.get("head", "")[:60]]
            print(
                f"| {ansi} | `{key[7:]}` | {', '.join(map(str, sv))} | {', '.join(map(str, rv))} |"
            )
        sp, rp = pair("", ansi)
        for key, rec in sp.items():
            if key.startswith("card/"):
                r = rp.get(key, {})
                sv = rec.get("rows") or rec.get("write")
                rv = r.get("rows") or r.get("write")
                print(f"| {ansi} | `{key}` | {sv} | {rv} |")


if __name__ == "__main__":
    part = sys.argv[2]
    if part == "summary":
        summary()
    elif part == "selects":
        selects()
    else:
        matrix(sys.argv[3], part)

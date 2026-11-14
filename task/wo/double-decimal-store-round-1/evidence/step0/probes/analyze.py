import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

from cells import READ_DOORS, SOURCES, TARGETS, TEXTS, WRITE_DOORS

raw = Path(sys.argv[1]) if len(sys.argv) > 1 else Path()
DOORS = READ_DOORS + WRITE_DOORS


def load(engine: str, ansi: str) -> dict:
    out = {}
    for line in (raw / f"{engine}-ansi-{ansi}.jsonl").read_text().splitlines():
        rec = json.loads(line)
        out[rec["key"]] = rec
    return out


def short(v: object) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, dict):
        cls = v.get("class") or ""
        if cls:
            return f"ERR {cls.split('.')[0]}"
        head = v.get("head", "")
        if "Overflowing on" in head:
            return "ERR arrow-overflow"
        if "too large to store" in head:
            return "ERR arrow-precision"
        if "NUMERIC_VALUE_OUT_OF_RANGE" in head:
            return "ERR NUMERIC_VALUE_OUT_OF_RANGE"
        if "CAST_OVERFLOW" in head:
            return "ERR CAST_OVERFLOW"
        return "ERR " + head[:40]
    if isinstance(v, str) and v.startswith("dec:"):
        return v[4:]
    return str(v)


def verdict(sv: object, rv: object) -> str:
    s, r = short(sv), short(rv)
    sd = not s.startswith("ERR") and s != "NULL"
    rd = not r.startswith("ERR") and r != "NULL"
    if sd and rd:
        return "EQUAL" if Decimal(s) == Decimal(r) else "WRONG-VALUE"
    if s == "NULL" and r == "NULL":
        return "EQUAL"
    if s.startswith("ERR") and r.startswith("ERR"):
        return "EQUAL-ERR"
    if s == "NULL" and r.startswith("ERR"):
        return "ERR-FOR-NULL"
    if s.startswith("ERR") and r == "NULL":
        return "NULL-FOR-ERR"
    if sd:
        return "MISSING-VALUE"
    return "VALUE-FOR-" + ("NULL" if s == "NULL" else "ERR")


def cell_rows(rec: dict | None) -> dict:
    if rec is None:
        return {}
    if "setup" in rec:
        return {str(i): rec["setup"] for i in range(len(TEXTS))}
    return rec.get("rows", {})


def main() -> None:
    summary = []
    detail = defaultdict(dict)
    for ansi in ("true", "false"):
        sp, rp = load("spark", ansi), load("repark", ansi)
        for door in DOORS:
            for src in SOURCES:
                counts = Counter()
                for p, sc in TARGETS:
                    key = f"{door}/{src}/{p},{sc}"
                    srows, rrows = cell_rows(sp.get(key)), cell_rows(rp.get(key))
                    for i in range(len(TEXTS)):
                        sv, rv = srows.get(str(i)), rrows.get(str(i))
                        v = verdict(sv, rv)
                        counts[v] += 1
                        detail[(ansi, src, i, p, sc)][door] = (short(sv), short(rv), v)
                summary.append((ansi, door, src, counts))
    out = {"summary": [], "detail": []}
    for ansi, door, src, counts in summary:
        out["summary"].append({"ansi": ansi, "door": door, "src": src, **counts})
    for (ansi, src, i, p, sc), doors in sorted(detail.items()):
        out["detail"].append(
            {"ansi": ansi, "src": src, "value": TEXTS[i], "target": f"({p},{sc})", "doors": doors}
        )
    for ansi in ("true", "false"):
        sp, rp = load("spark", ansi), load("repark", ansi)
        for k in sp:
            if k.startswith(("card/", "source/")):
                out.setdefault("extra", []).append(
                    {
                        "ansi": ansi,
                        "key": k,
                        "spark": sp[k].get("rows"),
                        "repark": rp.get(k, {}).get("rows"),
                    }
                )
    Path(sys.argv[2]).write_text(json.dumps(out, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()

import json
import sys
from pathlib import Path

raw = sys.argv[1]


def load(name: str) -> dict:
    return {
        r["key"]: r
        for r in (json.loads(x) for x in (Path(raw) / name).read_text().splitlines())
        if r["key"] != "provenance"
    }


def rows_of(rec: dict, field: str) -> str:
    got = rec.get(field)
    if isinstance(got, dict) and "error" in got:
        return "raises " + (got.get("class") or got.get("error"))
    vals = rec.get("rows")
    t = got.get("type") if isinstance(got, dict) else None
    return f"{t}: " + ", ".join(f"{i}={v}" for i, v in vals) if vals is not None else "?"


def write_status(rec: dict) -> str:
    w = rec.get("write")
    if w == "ok":
        return "ok"
    return "raises " + (w.get("class") or w.get("error") or "") + ": " + (w.get("head") or "")[:90]


print("| cell | target | ANSI | Spark writes; Spark reads | RePark writes; Spark reads | verdict |")
print("|---|---|---|---|---|---|")
for mode in ("true", "false"):
    sw, rw = load(f"write-spark-ansi-{mode}.jsonl"), load(f"write-repark-ansi-{mode}.jsonl")
    sr, rr = load(f"readback-spark-ansi-{mode}.jsonl"), load(f"readback-repark-ansi-{mode}.jsonl")
    for key in sw:
        s_state = rows_of(sr[key], "spark_read")
        r_state = rows_of(rr[key], "spark_read")
        sws, rws = write_status(sw[key]), write_status(rw[key])
        if sws != "ok" and rws != "ok":
            v = "BOTH-RAISE"
        elif rws != "ok":
            v = "REPARK-RAISES"
        elif sws != "ok":
            v = "REPARK-ACCEPTS"
        elif s_state == r_state:
            v = "EQUAL"
        else:
            v = "WRONG-WRITE"
        target = sw[key]["target"]
        print(f"| `{key}` | {target} | {mode} | {sws}; {s_state} | {rws}; {r_state} | {v} |")

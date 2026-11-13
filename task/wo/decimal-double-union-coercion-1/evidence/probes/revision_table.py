import json
import sys
from pathlib import Path

from analyze import verdict

raw = Path(sys.argv[1])


def load(name: str) -> dict:
    out = {}
    for line in (raw / name).read_text().splitlines():
        r = json.loads(line)
        out[r["key"]] = r
    return out


def shown(rec: dict) -> str:
    if "error" in rec:
        return "raises " + str(rec.get("class") or rec.get("head") or rec.get("error"))[:110]
    return f"`{rec['type']}` " + ", ".join(str(v).split("|")[0] for v in rec["values"])


def state(read: dict, write: dict) -> str:
    w = write.get("write")
    prefix = "" if w == "ok" else "write raises " + str(w.get("class") or w.get("head"))[:90] + "; "
    got = read.get("spark_read", {})
    if "error" in got:
        return prefix + "read raises"
    t = got.get("type")
    return prefix + f"`{t}` " + "; ".join("=".join(r) for r in read.get("rows", []))


def write_verdict(sw: dict, rw: dict, sr: dict, rr: dict) -> str:
    s_ok, r_ok = sw.get("write") == "ok", rw.get("write") == "ok"
    if not s_ok and not r_ok:
        return "BOTH-RAISE"
    if not r_ok:
        return "REPARK-RAISES"
    if not s_ok:
        return "REPARK-ACCEPTS"
    same = sr.get("rows") == rr.get("rows") and sr["spark_read"].get("type") == rr[
        "spark_read"
    ].get("type")
    return "EQUAL" if same else "WRONG-WRITE"


print("| cell | ANSI | Spark 4.1.2 | RePark | verdict |")
print("|---|---|---|---|---|")
for mode in ("true", "false"):
    sp, rp = load(f"revision-spark-ansi-{mode}.jsonl"), load(f"revision-repark-ansi-{mode}.jsonl")
    for key, s in sp.items():
        if key.startswith("select/"):
            print(
                f"| `{key[7:]}` | {mode} | {shown(s)} | {shown(rp[key])} | {verdict(s, rp[key])} |"
            )
print()
print("| cell | ANSI | Spark writes; Spark reads | RePark writes; Spark reads | verdict |")
print("|---|---|---|---|---|")
for mode in ("true", "false"):
    sw, rw = load(f"revision-spark-ansi-{mode}.jsonl"), load(f"revision-repark-ansi-{mode}.jsonl")
    sr = load(f"revision-readback-spark-ansi-{mode}.jsonl")
    rr = load(f"revision-readback-repark-ansi-{mode}.jsonl")
    for key, s in sw.items():
        if "table" not in s:
            continue
        v = write_verdict(s, rw[key], sr[key], rr[key])
        print(f"| `{key}` | {mode} | {state(sr[key], s)} | {state(rr[key], rw[key])} | {v} |")

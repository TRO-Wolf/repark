import json
import re
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

raw = sys.argv[1]
DEC_RE = re.compile(r"dec:(-?[0-9.]+(?:E[-+]?\d+)?)")


def load(name: str) -> dict:
    path = Path(raw) / name
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        r = json.loads(line)
        out[r["key"]] = r
    return out


def norm_value(v: str | None) -> str:
    if v is None:
        return "null"
    v = DEC_RE.sub(lambda m: repr(float(Decimal(m.group(1)))), v)
    v = re.sub(r"\|[0-9a-f]{16}", "", v)
    v = v.replace("inf", "Infinity") if "Infinity" not in v else v
    return v


def verdict(sp: dict, rp: dict) -> str:
    se, re_ = "error" in sp, "error" in rp
    if se and re_:
        return "both-raise"
    if se:
        return "spark-raises-repark-answers"
    if re_:
        head = (rp.get("head") or "") + str(rp.get("class"))
        tag = (
            "overflow"
            if ("Overflow" in head or "overflow" in head or "simplify_expressions" in head)
            else "other"
        )
        return f"repark-raises-{tag}"
    if sp["type"] == rp["type"] and sp["values"] == rp["values"]:
        return "equal"
    if [norm_value(x) for x in sp["values"]] == [norm_value(x) for x in rp["values"]]:
        return "type-only"
    return "wrong-value"


def cell_root(key: str) -> str:
    parts = key.split("/")
    if parts[0] == "extra" or parts[0].startswith("dfcol"):
        return "dd"
    left = parts[1]
    return "dd" if left.startswith("dec") else "control"


def main() -> dict:
    out = {}
    for mode in ("true", "false"):
        sp = load(f"select-spark-ansi-{mode}.jsonl")
        rp = load(f"select-repark-ansi-{mode}.jsonl")
        for k, s in sp.items():
            if k in ("provenance", "elapsed") or k not in rp:
                continue
            out[(k, mode, "sql")] = (verdict(s, rp[k]), s, rp[k])
        fsp = load(f"frames-spark-ansi-{mode}.jsonl")
        frp = load(f"frames-repark-ansi-{mode}.jsonl")
        for k, s in fsp.items():
            if k == "provenance" or k not in frp:
                continue
            out[(k, mode, "df")] = (verdict(s, frp[k]), s, frp[k])
    return out


if __name__ == "__main__":
    res = main()
    by = defaultdict(Counter)
    for (k, mode, _door), (v, _, _) in res.items():
        construct = k.split("/")[0]
        by[(construct, cell_root(k), mode)][v] += 1
    rows = []
    for key in sorted(by):
        rows.append({"construct": key[0], "root": key[1], "ansi": key[2], **by[key]})
    (Path(sys.argv[2]) / "summary-counts.json").write_text(
        json.dumps(rows, indent=1, sort_keys=True)
    )
    flat = []
    for (k, mode, door), (v, s, r) in sorted(res.items()):
        flat.append(
            {
                "key": k,
                "ansi": mode,
                "door": door,
                "root": cell_root(k),
                "verdict": v,
                "spark_type": s.get("type"),
                "repark_type": r.get("type"),
                "spark_values": s.get("values"),
                "repark_values": r.get("values"),
                "spark_error": s.get("class") or s.get("error"),
                "repark_error": (r.get("head") or "")[:160] if "error" in r else None,
            }
        )
    with (Path(sys.argv[2]) / "verdicts.jsonl").open("w") as fh:
        for f in flat:
            fh.write(json.dumps(f, sort_keys=True) + "\n")
    tot = Counter((f["root"], f["verdict"]) for f in flat)
    for k in sorted(tot):
        print(k, tot[k])

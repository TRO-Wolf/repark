import json
import sys
from collections import Counter
from pathlib import Path

raw, result = Path(sys.argv[1]), Path(sys.argv[2])
WRONG = {"VALUE", "TYPE", "REPARK_ANSWERS", "REPARK_RAISES", "CREATE_DIFF"}
PART1 = {
    "sql",
    "tsql",
    "vss",
    "vds",
    "ren",
    "tsqlf",
    "vdsf",
    "w-ctas-sql",
    "w-insert-sql",
    "w-writeto-tsql",
    "nest-sql-sql",
    "nest-sql-df",
}
head = {}
for line in (result / "bar-cells.jsonl").read_text().splitlines():
    r = json.loads(line)
    head[(r["set"], r["cell"])] = r
step0 = [json.loads(line) for line in (raw / "verdicts.jsonl").read_text().splitlines()]
target = []
for v in step0:
    if v["family"] in ("cs",) and v["body"] in ("cs_twin", "cs_lower"):
        continue
    if v["verdict"] not in WRONG or v["direct_agree"] != "yes":
        continue
    if v["created"] == v["read"] or v["door"] not in PART1:
        continue
    if v["phase"] == "reader" and v["door"] != "sql":
        continue
    target.append(v)
rev = [json.loads(line) for line in (raw / "rev-cells.jsonl").read_text().splitlines()]
rev_target = [
    r
    for r in rev
    if r["verdict"] in WRONG
    and r["kind"] not in ("direct", "direct_gap", "same")
    and (
        "/sql/" in r["cell"]
        or r["cell"].startswith(
            ("rename/table", "cache/sql", "cache/lazy", "cache/catalog", "cache/set_between")
        )
    )
]
out = Counter()
missed = []
for v in target:
    key = ("run-unset" if v["family"] == "unset" else "run", v["cell"])
    h = head.get(key)
    state = (
        "missing" if h is None else ("EQUAL" if h["head"] in ("EQUAL", "BOTH_RAISE") else h["head"])
    )
    out[("run", state)] += 1
    if state != "EQUAL":
        missed.append((v["cell"], state))
for r in rev_target:
    h = head.get(("rev", r["cell"]))
    state = (
        "missing" if h is None else ("EQUAL" if h["head"] in ("EQUAL", "BOTH_RAISE") else h["head"])
    )
    out[("rev", state)] += 1
    if state != "EQUAL":
        missed.append((r["cell"], state))
print(len(target), len(rev_target), dict(out))
for m in missed:
    print("MISSED", m)

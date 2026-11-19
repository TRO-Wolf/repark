import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

new, old = Path(sys.argv[1]), Path(sys.argv[2])
TAGS = ["frames", "attack", "attack2", "attack3", "case", "fable", "readers", "joins", "smart"]
HEAD, D1, PART1, PRE = (
    "pkg-view2-9d71e030",
    "pkg-view2-513208ce",
    "pkg-view1-c54f5fc7",
    "pkg-avg-27d6149f",
)


def load(path: Path) -> dict[str, dict[str, Any]]:
    found = {}
    if path.exists():
        for line in path.read_text().splitlines():
            record = json.loads(line)
            key = record.pop("tag", None)
            if key is not None:
                found[key] = record
    return found


def verdict(want: dict[str, Any] | None, got: dict[str, Any] | None) -> str | None:
    if want is None or got is None:
        return None
    if "error" in want or "error" in got:
        return "EQUAL" if want.get("error") == got.get("error") else "DIFF"
    return "EQUAL" if want == got else "DIFF"


total: Counter = Counter()
for tag in TAGS:
    head = load(new / f"f-{tag}-{HEAD}.jsonl")
    d1 = load(old / f"f-{tag}-{D1}.jsonl")
    part1 = load(new / f"f-{tag}-{PART1}.jsonl")
    pre = load(new / f"f-{tag}-{PRE}.jsonl")
    spark = load(new / f"f-{tag}-spark.jsonl")
    moved = [key for key in head if key in d1 and head[key] != d1[key]]
    vs_part1: Counter = Counter()
    worse = []
    for key in head:
        if key not in spark or key not in part1:
            continue
        before, after = verdict(spark[key], part1[key]), verdict(spark[key], head[key])
        vs_part1[f"{before}->{after}"] += 1
        if before == "EQUAL" and after != "EQUAL":
            worse.append(key)
    vs_pre = Counter(
        f"{verdict(spark[k], pre[k])}->{verdict(spark[k], head[k])}"
        for k in head
        if k in spark and k in pre
    )
    print(tag, "cells", len(head), "| moved vs 513208ce", len(moved), moved[:6])
    print("   vs part1", dict(vs_part1), "| EQUAL->not", worse[:6])
    print("   vs pre", dict(vs_pre))
    total["cells"] += len(head)
    total["moved"] += len(moved)
    total["worse"] += len(worse)
print(dict(total))

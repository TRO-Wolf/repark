import json
import sys
from collections import defaultdict
from pathlib import Path

cells: defaultdict[tuple[str, str, str], list[tuple[int, str]]] = defaultdict(list)
with Path(sys.argv[1]).open() as fh:
    for line in fh:
        r = json.loads(line)
        a = r["answer"]
        if isinstance(a, list):
            a = a[0][-1]
        elif a.startswith("ERR"):
            if "ARITHMETIC_OVERFLOW" in a:
                a = "ARITHMETIC_OVERFLOW"
            elif "past its declared precision" in a:
                a = "PAST_PRECISION"
            else:
                a = a[:90]
        cells[(r["query"], r["file"], r["ansi"])].append((r["tp"], a))
for key in sorted(cells):
    shown = " ".join(f"tp{tp}={a}" for tp, a in cells[key])
    print(f"{key[0]:18} {key[1]:13} ansi={key[2]:5} {shown}")

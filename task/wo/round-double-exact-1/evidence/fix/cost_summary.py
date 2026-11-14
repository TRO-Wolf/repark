import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

samples_path, round_path = sys.argv[1:3]
walls = defaultdict(dict)
for line in Path(samples_path).read_text().splitlines():
    r = json.loads(line)
    walls[(r["case"], r["run"])][r["build"]] = r["wall"]
rows = defaultdict(dict)
for line in Path(round_path).read_text().splitlines():
    r = json.loads(line)
    for name, value in r["times"].items():
        rows[(name, r["run"])][r["label"]] = value
print("| row | runs | base median (s) | head median (s) | head/base | base-aa/base |")
print("|---|---|---|---|---|---|")


def line(name: str, table: dict) -> None:
    runs = sorted(run for (case, run) in table if case == name)
    picks = [
        table[(name, run)] for run in runs if {"base", "head", "base-aa"} <= set(table[(name, run)])
    ]
    if not picks:
        return
    ratio = statistics.median(p["head"] / p["base"] for p in picks)
    control = statistics.median(p["base-aa"] / p["base"] for p in picks)
    base = statistics.median(p["base"] for p in picks)
    head = statistics.median(p["head"] for p in picks)
    print(f"| {name} | {len(picks)} | {base:.4f} | {head:.4f} | **{ratio:.3f}** | {control:.3f} |")


for name in ("c012p6", "c012rest"):
    line(name, walls)
for name in sorted({case for case, _ in rows}):
    line(name, rows)

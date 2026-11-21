import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ORDER = [
    "fill count",
    "then collect",
    "count then collect, total",
    "collect first, total",
    "view registration",
    "view first count",
    "view total",
    "selective child first count",
    "second child count",
]
BUILDS = ("main", "head", "main-again")


def read_lines(path: str) -> list[dict]:
    with Path(path).open() as handle:
        return [json.loads(line) for line in handle]


def step_samples(path: str) -> dict[tuple[str, str], list[float]]:
    steps: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in read_lines(path):
        ms, c, b = r["ms"], r["case"], r["build"]
        if c == "count_collect":
            steps[("fill count", b)].append(ms["count"] - ms["build"])
            steps[("then collect", b)].append(ms["collect"] - ms["count"])
            steps[("count then collect, total", b)].append(ms["collect"])
        elif c == "collect_first":
            steps[("collect first, total", b)].append(ms["count"])
        elif c == "view_count":
            steps[("view registration", b)].append(ms["register"])
            steps[("view first count", b)].append(ms["count"] - ms["register"])
            steps[("view total", b)].append(ms["count"])
        elif c == "selective_child":
            steps[("selective child first count", b)].append(ms["child"] - ms["build"])
        elif c == "second_child":
            steps[("second child count", b)].append(ms["second"] - ms["first"])
    return steps


def row(name: str, n: int, meds: tuple[float, float, float], spans: tuple[str, str]) -> str:
    m, h, a = meds
    ratio = h / ((m + a) / 2)
    cells = [name, str(n), f"{m:,.1f}", f"{h:,.1f}", f"{a:,.1f}", f"{ratio:.3f}", *spans]
    return "| " + " | ".join(cells) + " |"


steps = step_samples(sys.argv[1])
print("| row | n | main ms | head ms | main again ms | ratio | head range | main range |")
for k in ORDER:
    m, h, a = (steps[(k, b)] for b in BUILDS)
    meds = (statistics.median(m), statistics.median(h), statistics.median(a))
    spans = (f"{min(h):,.0f}-{max(h):,.0f}", f"{min(m + a):,.0f}-{max(m + a):,.0f}")
    print(row(k, len(h), meds, spans))
walls: dict[tuple[str, str], list[float]] = defaultdict(list)
for r in read_lines(sys.argv[2]):
    walls[(r["case"], r["build"])].append(r["wall"])
for case in ("c012p6", "c012rest"):
    meds = tuple(statistics.median(walls[(case, b)]) for b in BUILDS)
    print(row(f"C-012 `{case}` (s)", len(walls[(case, "head")]), meds, ("", "")))

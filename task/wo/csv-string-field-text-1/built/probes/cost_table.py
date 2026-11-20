import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

out = Path(sys.argv[1])
samples = [json.loads(line) for line in (out / "samples.jsonl").read_text().splitlines()]
counts: dict[tuple[str, str, int], dict[str, float]] = {}
for path in out.glob("perf-*.txt"):
    label, *case_parts, round_no = path.stem.split("-")[1:]
    values: dict[str, float] = {}
    for line in path.read_text().splitlines():
        fields = line.split(",")
        if len(fields) > 2 and fields[2] in ("instructions:u", "cycles:u") and fields[0].isdigit():
            values[fields[2]] = float(fields[0]) / 10
    counts[(label, "-".join(case_parts), int(round_no))] = values
medians: dict[tuple[str, str], list[float]] = defaultdict(list)
answers: dict[tuple[str, str], set[str]] = defaultdict(set)
for sample in samples:
    key = (sample["label"], sample["case"])
    medians[key].append(sample["median"] * 1000)
    answers[key].add(sample["answer"])
lines = [
    "| case | base ms | head ms | head / base | rounds | base spread | instr base | instr head "
    "| instr ratio | cycles ratio | answers equal |",
    "|---|---|---|---|---|---|---|---|---|---|---|",
]
for case in ("k0_count", "k0_hour", "is_count", "is_sum", "is_none", "is_infer"):
    base, head = medians.get(("base", case), []), medians.get(("head", case), [])
    if not base or not head:
        continue

    def mean_count(label: str, event: str, case: str = case) -> float:
        found = [
            v[event]
            for (lb, cs, _), v in counts.items()
            if lb == label and cs == case and event in v
        ]
        return statistics.median(found) if found else float("nan")

    ib, ih = mean_count("base", "instructions:u"), mean_count("head", "instructions:u")
    cb, ch = mean_count("base", "cycles:u"), mean_count("head", "cycles:u")
    mb, mh = statistics.median(base), statistics.median(head)
    spread = f"{min(base):.2f}-{max(base):.2f}"
    same = answers[("base", case)] == answers[("head", case)]
    lines.append(
        f"| `{case}` | {mb:.2f} | {mh:.2f} | **{mh / mb:.3f}x** | {len(base)}/{len(head)} | "
        f"{spread} | {ib / 1e6:,.1f} M | {ih / 1e6:,.1f} M | {ih / ib:.4f} | {ch / cb:.3f} | "
        f"{same} |"
    )
print("\n".join(lines))
for key in sorted(answers):
    print(key, sorted(answers[key]))

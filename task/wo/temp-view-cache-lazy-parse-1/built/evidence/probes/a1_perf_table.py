import statistics
import sys
from pathlib import Path

d = Path(sys.argv[1])
pk = sys.argv[2].split()
cases = sys.argv[3].split()
for case in cases:
    base = None
    for p in pk:
        vals = {}
        for f in sorted(d.glob(f"perf-{p}-{case}-*.txt")):
            for ln in f.read_text().splitlines():
                parts = ln.split(",")
                if len(parts) > 3 and parts[0].replace(".", "").isdigit():
                    vals.setdefault(parts[2], []).append(float(parts[0]))
        if not vals:
            continue
        med = {k: statistics.median(v) for k, v in vals.items()}
        if base is None:
            base = med
        n = len(next(iter(vals.values())))
        cells = []
        for k, v in med.items():
            name = (
                k.replace(":u", "")
                .replace("L1-icache-load-misses", "icache-miss")
                .replace("iTLB-load-misses", "itlb-miss")
            )
            cells.append(f"{name} {v / 1e6:9.2f}M {v / base[k]:.3f}x")
        print(f"{case:11s} {p:20s} n={n} " + " | ".join(cells))

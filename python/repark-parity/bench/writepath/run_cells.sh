set -euo pipefail
lane="${REPARK_LANE:-$(cd "$(dirname "$0")/../../../.." && pwd)}"
label="$1"
out="$lane/scratch/cells_${label}.jsonl"
: > "$out"
for pass in 1 2 3; do
  for cell in ctas ctas_partitioned8 df_write_parquet_zstd; do
    "$lane/.venv/bin/python" "$lane/python/repark-parity/bench/writepath/probe_cell.py" \
      "$lane/scratch/synth_1000000.parquet" "$cell" 8 5 >> "$out"
  done
done
python3 - "$out" <<'PY'
import json, sys
from collections import defaultdict
rows = defaultdict(list)
for line in open(sys.argv[1]):
    r = json.loads(line)
    rows[r["cell"]].append(r)
for cell, rs in rows.items():
    best = min(rs, key=lambda r: r["min_ms"])
    print(
        cell,
        "min", min(r["min_ms"] for r in rs),
        "best_median", min(r["median_ms"] for r in rs),
        "medians", [r["median_ms"] for r in rs],
        "files", best["files"],
        "load", [r["load1_start"] for r in rs],
    )
PY

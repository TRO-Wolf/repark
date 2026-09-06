# Approximate-percentile baseline — `percentile_approx` sketch (PERF-APPROXPCT-1)

Scope: the Spark `percentile_approx` / `approx_percentile` UDAF after
PERF-APPROXPCT-1 replaced the whole-group `Vec<ScalarValue>` kernel with
Spark 4.1's Greenwald-Khanna `QuantileSummaries` (PERF-ANALYSIS-1 §2 row
for the unbounded-state aggregate; slate item: bound the memory and honor
the accuracy knob). Before: one group held every input value boxed, so a
1e7-row group peaked at 2.5 GB and the accuracy argument was silently
ignored. After: inserts buffer in a 50 k head, flush into a compressed
sample set (threshold 10000, `relativeError = 1/accuracy`), merge before
query on multi-partition scans; per-group state is kilobytes and the
accuracy knob moves the answers exactly as Spark's does (the committed
matrix pins both against live 4.1.2 goldens). The `count(id)` leg beside
every cell is the control: it runs the same scan with no sketch state, so
peak-minus-floor is the aggregate-attributable cost.

Machine/profile (2026-09-05): this box (64 threads, 125 GB RAM, shared
lane box), release module (`__debug_assertions__ False`), fresh
subprocess per cell, 1-minute load recorded beside each run. Loads:
before ~25–30, after ~11 (shared box; the wall comparison favors neither
side by more than the noise — the gap is 20×). Every cell is a single
run, not a median; the 1e6 triplet is cold + two warm attempts in one
process.

| cell | before (base `bc7c76cc`) | after (tip `d476556e`) | control `count(id)` |
|---|---:|---:|---:|
| 1e6, attempt 0 (cold), wall / peak | 1.28 s / 475.7 MB | 0.03 s / 378.5 MB | — |
| 1e6, attempt 1 (warm), wall / peak | 0.21 s / 644.9 MB | 0.02 s / 476.6 MB | — |
| 1e6, attempt 2 (warm), wall / peak | 0.13 s / 697.7 MB | 0.02 s / 488.6 MB | — |
| 1e6, answer (median of 1..1e6) | 500000 | 500001 / 499911 / 499971 | — |
| 1e7, fresh subprocess, wall / peak | 2.95 s / 2507.8 MB | 0.15 s / 650.0 MB | 0.03 s / 188.2 MB |
| 1e7, answer (true median 5000000.5) | 5000000 | 4999593 (err 407, budget 50000) | — |
| 1e7 aggregate-attributable (peak − floor) | 2320 MB | 462 MB | — |

Notes. The warm-1e6 wall bar was "within 1.5× of before" (0.13–0.21 s);
after runs 0.02 s, ~7–10× faster, because the sketch never materializes
the group. The 1e7 residual (462 MB over the floor) is not sketch state:
state is kilobytes, pinned by `million_row_state_stays_small` (< 2 MB
serialized at 1 M rows) and by `inserts_compress_eagerly_before_any_query`
(sampled < 100 k after 200 k inserts with no query call); the residual
scales sublinearly (190 MB at 1e6 → 462 MB at 1e7) and reads as transient
Arrow batches plus allocator retention in a 0.15 s run — inferred, not
measured. Error budget: default accuracy 100 → eps 0.005, so ±50000 at
1e7; the measured 407 is two orders of magnitude inside. Accuracy-knob
cells (default/10/2 on int, double, decimal, grouped, and window-frame
paths) live in the unit ledger §4 with live-Spark goldens, not here.

Reproduce (from the repo root, release module):

```
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release
.venv/bin/python /tmp/bench_approx.py 1000000 3   # cold + two warm attempts
.venv/bin/python /tmp/bench_approx.py 10000000 1  # fresh subprocess
```

(`/tmp/bench_approx.py` is the scratch harness: `range(1, N+1)` →
`percentile_approx(id, 0.5)`, wall plus `ru_maxrss`, printed per attempt.
`/tmp/bench_count.py` is the same shape with `count(id)`.)

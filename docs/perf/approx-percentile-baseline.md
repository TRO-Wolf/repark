# Approximate-percentile baseline — `percentile_approx` sketch (PERF-APPROXPCT-1)

Scope: the Spark `percentile_approx` / `approx_percentile` UDAF after
PERF-APPROXPCT-1 replaced the whole-group `Vec<ScalarValue>` kernel with
Spark 4.1's Greenwald-Khanna `QuantileSummaries` (PERF-ANALYSIS-1 §2 row
for the unbounded-state aggregate; slate item: bound the memory and honor
the accuracy knob). Before: one group held every input value boxed, so a
1e7-row group peaked at 2.5 GB and the accuracy argument was silently
ignored. After: inserts buffer in a 50 k head, flush into a compressed
sample set (threshold 10000, `relativeError = 1/accuracy`), merge before
query on multi-partition scans; per-group state is 952656 B at default
accuracy (39693 samples after 1e6 inserts; 4776 B at acc 100, 72 B at acc 2 —
O((1/eps)·log(eps N)), "kilobytes" only at accuracy ≤ 100) and the accuracy
knob moves the answers as Spark's does on single-partition inputs (the committed
matrix pins both against live 4.1.2 goldens; multi-partition merges are
deterministic within the GK bound — `FN-APPROXPCT-ORDER-1`). The `count(id)` leg
beside every cell is the control: it runs the same scan with no sketch state, so
peak-minus-floor is the aggregate-attributable cost.

Machine/profile (2026-09-06 re-derivation): this box (64 threads, 125 GB RAM,
shared lane box), release module (`__debug_assertions__ False`), fresh
subprocess per cell, 1-minute load recorded beside each run. Loads:
before ~25–30 (recorded 2026-09-05), after ~6–9. Every cell is a single
run, not a median; the 1e6 triplet is cold + two warm attempts in one
process.

| cell | before (base `bc7c76cc`, recorded) | after (round 2 `001eee7d`) | control `count(id)` |
|---|---|---:|---:|
| 1e6, attempt 0 (cold), wall / peak | 1.28 s / 475.7 MB | 0.03 s / 398.4 MB | — |
| 1e6, attempt 1 (warm), wall / peak | 0.21 s / 644.9 MB | 0.03 s / 478.5 MB | — |
| 1e6, attempt 2 (warm), wall / peak | 0.13 s / 697.7 MB | 0.03 s / 526.6 MB | — |
| 1e6, answer (median of 1..1e6) | 500000 | 499971 / 499971 / 499971 | — |
| 1e7, fresh subprocess, wall / peak | 2.95 s / 2507.8 MB | 0.14 s / 752.9 MB | 0.02 s / 188.6 MB |
| 1e7, answer (true median 5000000.5) | 5000000 | 4999593 (err 407, budget 50000) | — |
| 1e7 aggregate-attributable (peak − floor) | 2320 MB | 564 MB | — |

Notes. The committed warm-1e6 wall bar is 1.0 s (round 2;
`test_million_row_wall_stays_within_bar`); after runs 0.03 s against the old
kernel's 0.13–0.21 s. The 1e7 residual (564 MB over the floor) is not sketch
state: state is 0.95 MB at default accuracy, pinned by `state_size_follows_one_over_eps`
(952656/4776/72 B at acc 10000/100/2 after 1e6 inserts) and bounded mid-insert by
`inserts_compress_eagerly_before_any_query`; the residual scales sublinearly and
reads as transient Arrow batches plus allocator retention in a 0.14 s run —
inferred, not measured. Error budget: default accuracy 10000 → eps 0.0001, so
±100 rank at 1e6 (±50000 at 1e7); the measured 29 (1e6) and 407 (1e7) sit inside.
Accuracy-knob cells (default/10/2 on int, double, decimal, grouped, and
window-frame paths) live in the unit ledger §4 with live-Spark goldens, not here.

Recorded, not reproduced: the before column was measured 2026-09-05 with the
throwaway `/tmp/bench_approx.py` (ledger §3) and stands as recorded — re-running
it needs a second release build of the pre-unit tree, which round 2 did not do.
Only the after column (tracked harness below) and the committed pins re-derive.

Reproduce (from the repo root, release module):

```
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release
CELL=python/repark-parity/bench/approxpct/run_cells.py
.venv/bin/python $CELL 1000000 3   # cold + two warm attempts
.venv/bin/python $CELL 10000000 1  # fresh subprocess
.venv/bin/python $CELL 10000000 1 --control
```

# Cost, slice 1 round 2 (2026-10-12)

Head `4da261fc` (untyped-NULL rewrite committed) vs baseline `/tmp/xcoerce0/.venv`.
Method: `task/wo/ice-tsns-coercion-1/interleaved_cost.py`, 5 pairs, each pair one
locked command (`flock` on the build lock, `taskset -c 48-63`), baseline set then
head set, 15 repeats of the slice-0 `insert` / `merge` / `merge_mor` statements.
Load decayed across pairs (9.33 → 5.43), which the interleaving absorbs.

| Statement | Baseline set medians (ms) | Head set medians (ms) | Ratio of medians | Pair range |
|---|---|---|---|---|
| insert | 20.63, 21.07, 26.66, 20.86, 20.63 | 20.60, 20.47, 21.06, 20.71, 20.71 | 0.9928 | 0.7899 – 1.0039 |
| merge | 65.18, 66.76, 84.71, 67.61, 66.18 | 65.58, 65.58, 68.64, 65.21, 64.95 | 0.9823 | 0.8103 – 1.0061 |
| merge_mor | 72.09, 72.38, 82.27, 73.91, 71.88 | 71.94, 72.09, 78.26, 70.55, 71.39 | 0.9939 | 0.9513 – 0.9979 |

Raw sets: [interleaved-sets-round2.json](interleaved-sets-round2.json). Reduced:
[interleaved-cost-round2.json](interleaved-cost-round2.json). The head set-1 counter
bracket moved the gate counter 0 → 2 with the fallback at 0: the timed statements
open the gate and never fall back.

Verdict: every interleaved ratio at or below 1.05. No halt. No product change.

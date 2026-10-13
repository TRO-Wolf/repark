# Cost, slice 1 fold 1 (2026-10-12)

Head `d2f2a3db` (per-node ask committed; the release module below adds the same
code) vs baseline `/tmp/xcoerce0/.venv`, both release profile. Method:
`task/wo/ice-tsns-coercion-1/interleaved_cost.py`, 5 pairs, each pair one locked
command (`flock` on the build lock, `taskset -c 48-63`, lock verified free before
each set), baseline set then head set, 15 repeats of the slice-0 `insert` /
`merge` / `merge_mor` statements.

| Statement | Ratio of medians | Pair range |
|---|---|---|
| insert | 1.0067 | 0.9947 – 1.0138 |
| merge | 1.0063 | 0.9875 – 1.0231 |
| merge_mor | 0.9929 | 0.9748 – 1.0104 |

Raw sets: [interleaved-sets-fold1.json](interleaved-sets-fold1.json). Reduced:
[interleaved-cost-fold1.json](interleaved-cost-fold1.json).

Verdict: every median ratio at or below 1.05. No halt. No product change.

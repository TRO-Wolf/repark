# task/wo/sum-dec-i128wrap-1/evidence/built/delta2/cost/ — delta 2's cost samples and table

- `samples.jsonl` — every sample's wall time and exit code: 135 samples, all exit 0 (the C-012 replay and the K rows'
  per-sample lines).
- `raw-sum.jsonl` — one line per K sample (seconds, repetitions, answer, the package's `_native` path with its scratch
  directory written as `<scratchpad>`): 105 lines.
- `table.md` — `../../cost_table.py` over the two files as the script prints it. Its `bar` column applies 1.05x to every
  row; the corrected bars are in [../../cost.md](../../cost.md), "Delta 2" (`k1_sum_d38_off` and `k1_sum_d38_on` held to 1.12x).

Up: [../map.md](../map.md).

# task/wo/sum-dec-i128wrap-1/evidence/built/delta3/cost/ — delta 3's cost samples

- `samples.jsonl` — every completed sample (exit 0; a sample killed when the builder paused is dropped).
- `raw-sum.jsonl` — one line per K sample (the scratch directory written as `<scratchpad>`).
- `table.md` — `../../cost_table.py` over the two files: the final table, 5 runs per row except `k1_sum_d38_on` (10)
  and `c012p6` (6 samples per build). Its `bar` column is corrected by hand: the K1 `d38` rows are held to 1.12x, the other held rows to 1.05x, and `k3_sum_d10` and `k5` are reported.

Up: [../map.md](../map.md).

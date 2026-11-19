# task/wo/sum-dec-i128wrap-1/evidence/built/cost/ — the cost runner and its samples

- `run.sh` — the runner (`S`, `B`, `HEAD` and `REPLAY` come from the environment): five alternating runs of main,
  head and main again per case, each sample under the exclusive measure lock; `FIRST`, `RUNS`, `GROUPS_` and
  `SUM_CASES` choose the runs and cases (the K3 rows ran 6 to 10 separately).
- `sum_data.py` — writes the `k1` (10 M rows) and `k2` (1 M rows) Parquet data with main's package.
- `sum_cost.py` — one case, one build: the session (ANSI on, or off for `*_off`), a warm-up, then the timed
  repetitions, appended to `raw-sum.jsonl`.
- `samples.jsonl` — one line per sample (wall time, exit code); `raw-sum.jsonl` — one line per K sample (seconds,
  repetitions, answer, the package's `_native` path).
- `table.md` — `../cost_table.py` over the two files.

Up: [../map.md](../map.md).

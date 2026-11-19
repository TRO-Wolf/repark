# STRING-HOUR-MINUTE-SECOND-1 — the cost (seat A, head `58b2725e`)

Head `pkg-shms-58b2725e` against the base `pkg-sum-845766fd`, the package built from the merge base's product tree
(main `2900efde`; `git diff --name-only 2900efde 81ae984e -- crates python/repark/src` is empty, so main's later
commits are records only).

## Method

- **Cases.** K1 `sum(hour(c))`, `c STRING`; K1x `sum(hour(CAST(c AS TIMESTAMP)))`; K2 `sum(hour(ts))`, `ts TIMESTAMP`;
  K5 `sum(minute(c))`: two million rows of one Iceberg v2 table (step 0's `cost.py` data, `yyyy-MM-dd HH:mm:ss`
  texts 37 s apart). K2pq, the twin row: `sum(hour(ts))` over the same rows as a Parquet file. C-012:
  `S/ddu-work/cost-sum-d3/run.sh`'s `c012` group (`replay_work.py`, `c012p6` and `c012rest`), process wall.
- **Samples.** `S/ddu-work/cost-shms/run.sh`: RUNS rounds, each round main, head, main again per case, one sample at
  a time under
  `flock -x measure.lock flock link.lock flock opus-cargo.lock flock opus2-cargo.lock nice -n 10 taskset -c 32-47`,
  the clock started inside the locks. A K sample is one process that builds the table, then per case runs the query
  once warm and ten times timed, and records the median of the ten; the row's figure is the median over the rounds.
  `evidence/built/cost_summary.py` gives the table.
- **The gate** (owner 2026-11-19, about 14:00, adopting Fable §2.2): C-012 a hard 1.05x; a row under 100 ms over its
  bar after ten runs is a flag, attributed by a twin row (K2 and K2pq: kernels the change does not touch); attributed
  to the change it is a miss, to the build it is recorded; no row passes above 1.15x without the owner. K1's bar is
  1.05x of main's K1x (ruling 4), which runs seat A's plan.

## Results

Five rounds (RUNS=5), 15:04 UTC end; no row over its bar, so no tenth round and no flag to attribute (the
instruction-count method the orchestrator allowed at about 14:50 was not needed).

| row | measure | main | head | main again | head / main | control band (main again / main) | bar | verdict |
|---|---|---|---|---|---|---|---|---|
| K1 `hour(c)` | median of 10, s | 0.0457 (answers `0`) | 0.0746 (answers 22,993,024) | 0.0454 | 1.632 | 0.993 | 1.05x of main's K1x | **0.962** of K1x (0.920 of K1x main again): pass |
| K1x `hour(CAST(c AS TIMESTAMP))` | median of 10, s | 0.0775 | 0.0771 | 0.0811 | 0.995 | 1.046 | reported | seat A's plan, unchanged |
| K2 `hour(ts)` | median of 10, s | 0.0247 | 0.0249 | 0.0241 | **1.009** | 0.978 | 1.05x | pass |
| K2pq `hour(ts)`, Parquet (twin) | median of 10, s | 0.0428 | 0.0436 | 0.0429 | 1.019 | 1.003 | reported | the build's noise |
| K5 `minute(c)` | median of 10, s | 0.0487 (answers `0`) | 0.0854 (answers 58,999,254) | 0.0490 | 1.754 | 1.007 | reported | the parse, as K1 |
| C-012 `c012p6` | process wall, s | 40.30 | 40.04 | 40.20 | **0.993** | 0.997 | 1.05x hard | pass |
| C-012 `c012rest` | process wall, s | 43.50 | 42.89 | 43.17 | **0.986** | 0.992 | 1.05x hard | pass |

- **K1** costs what K1x costs: the head's `hour(c)` is the plan `hour(to_timestamp(c))`, and its median sits inside
  K1x's own spread (K1x main 0.0764–0.0845 s over the five rounds, head K1 0.0646–0.0842 s). Against today's K1
  (1.632x) the difference is the parse main skipped by reading a midnight (ruling 4).
- **K5** is K1's case for `minute`: 1.754x of today's wrong `0`, at K1x's cost.
- Every answer is the same on every sample of a build; the head's K1 equals `sum(hour(ts))`.
- Raw: `S/ddu-work/cost-shms/samples.jsonl` (process walls), `raw-k.jsonl` (per-case medians), `run.sh`,
  `shms_cost.py`.

Up: [map.md](map.md).

# task/wo/sum-dec-i128wrap-1/evidence/built/delta2/det/ — check 3, one answer in every configuration

- `det.py` — one configuration, both ANSI modes: writes the pin file's two three-file tables
  (`write_spread_tables`, with columns at precision 10, 18, 19, 20 and 38), optionally turns file work stealing off and
  sets `datafusion.execution.target_partitions` before the tables are read, then runs every grouped and ungrouped
  `sum` and `try_sum` 20 times (and grouped `avg`, reported), recording the distinct answers and whether each is the
  exact total. Run with the package and the pin file's directory on `PYTHONPATH`, under `taskset` at 2, 4, 8 and 10
  cores with stealing on and off, and at 10 cores with `target_partitions` 2, 4, 8 and 16.
- `head.jsonl` — the head `pkg-sum-fcb0c888`: 24 configurations × 126 queries.
- After the run, `det.py` was split into functions for `ruff` (the same queries, labels and exactness test).

Up: [../map.md](../map.md).

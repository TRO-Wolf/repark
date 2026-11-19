# task/wo/sum-dec-i128wrap-1/evidence/built/delta1/ — delta 1: the grouped final merge checks its total once

Delta 1 of SUM-DEC-I128WRAP-1 (owner, 2026-11-18: "Go with Fable's leans on A, B and C."). The scripts that sized the
change before any product file moved, and measured it after; the results are in [../bar.md](../bar.md) and
[../cost.md](../cost.md). Comment-free Python, run from this directory; `cells` and `lib` come from
[../../probes/](../../probes/map.md).

- `files.py` — writes the fixed Parquet files both engines read: `s` (one file), `s4` (four), `p2` (two) from the step
  0's fixtures, and `fq`, Fable's probe table (group 11 holding `M`, `1`, `-1` in three files, group 12 in a fourth),
  with each file's row count and hash in `files.json`.
- `spread.py` — one engine and one ANSI mode: every `s4` and `p2` cell of the step 0 (named `mp/…`, the RePark plan is
  the same in both passes), the `DISTINCT` cells, the DataFrame `repartition(4)` cells and Fable's three queries, each
  run `reps` times in one session, with the count of each distinct answer. Spark runs `local[4]` with
  `spark.sql.shuffle.partitions` 4, either Spark's default split packing (`default`) or one split per file (`split`,
  `openCostInBytes` 128 MiB); `passback` keeps the pass-back groups and the condition groups.
- `spread_table.py` — prints the runs, the queries with more than one answer and every class (b) query's spread.
- `predict.py` — the change's model for the cells whose partials the file layout fixes (`s4` and `p2` grouped `sum`
  and `try_sum`): each file's partial checked after every row, the merged total once; prints the predicted moves
  against Spark's default and multi-partition passes.
- `sizing.py` — each RePark build's answers on every run against Spark's `local[4]` answers, and the model's.
- [spread/](spread/map.md) — the raw spreads, before (`pkg-sum-a8016e75`, main's `pkg-view1-c54f5fc7`, Spark) and after
  (the delta's head).
- [raw/](raw/map.md) — the bar's re-run on the head package `pkg-sum-6d730107` and main's verdicts.
- [compare/](compare/map.md) — the head against main, the build's base and the build's head; main against the base.

Up: [../map.md](../map.md).

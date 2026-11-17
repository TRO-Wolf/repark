# task/wo/avg-decimal-round-1/evidence/probes/ — the step 0 probes (comment-free Python)

Run from this directory; `cells` and `lib` are imported by the runners. Commands and provenance:
[../map.md](../map.md).

- `cells.py` — the data and every cell: the `avg` table `t` (13 decimal columns, precisions 10, 28, 34 and 38, plus
  `INT`, `BIGINT` and `DOUBLE` controls) as unscaled patterns per group (14 ordinary groups, 5 near the 38-digit
  limit), the division table `v` (10 decimal columns, integer columns of four widths), the `avg` SQL forms, the
  division literals, typed operands and controls, the `h / 2` scopes, and the two confs. `spark_avg_expect` is the
  half-up value the `HAVING avg(x) = …` cell compares with.
- `lib.py` — the session builder for both engines (Iceberg hadoop catalog `lc`, the session zone pinned, the ANSI flag, a conf), the
  answer normaliser and the error text.
- `run_door.py` — one engine (`spark` or `repark`), one ANSI mode, one conf (`base`, `apl_false`, `pmp_false`):
  Parquet tables `t` (one file), `t4` (four files) and `v`; every SQL cell, the DataFrame cells, `describe` and
  `summary`, the `h / 2` scopes (temp view, session view over an Iceberg table, derived, CTE, flat) and the Iceberg
  CTAS, `INSERT … SELECT`, `UPDATE` and `MERGE` cells with the stored rows and schema read back. A conf run keeps a
  subset (the `avg` grouped, ungrouped and sliding forms, every division cell, two DataFrame forms).
- `run_native.py` — the native door `repark.sql()` over `VALUES` views of the same rows: a sample of the `avg` and
  division cells.
- `explain_paths.py` — RePark only: `EXPLAIN` of the `avg`, `avg(DISTINCT …)`, 3-row window, `/`, `*` and `%`
  cells on the Spark door and two on the native door, the DataFrame `col / 3` schema, and two `avg(DISTINCT …)`
  answers.
- `run_extra.py` — after the main pass, ANSI on, both engines: `avg(DISTINCT …)` beside `count` and as a window,
  `*` and `+` with `3Y`, `3S`, `3L` literals, and `try_add`, `try_multiply`, `try_divide` over a decimal.
- `analyze.py` — pairs each RePark (or native) cell with Spark's in the same mode and conf, renormalises a Spark
  `decimal(38,38)` value that PySpark prints with a 39th zero digit, classifies (`EQUAL`, `TYPE`, `VALUE`,
  `REPARK_RAISES`, `SPARK_RAISES`, `BOTH_RAISE`), tags each differing value (`truncated_one_unit`,
  `longer_quotient`, `fewer_digits`, …), writes `verdicts.jsonl` and `summary.md`.
- `grids.py` — writes `grids.md`: the group-by-column and form-by-column letter grids per mode, conf and door.
- `compact.py` — copies the raw outputs into [../raw/](../raw/map.md) without the SQL text (rebuilt from the key by
  `cells.py`) and the timings, with scratch paths replaced by a placeholder.

Up: [../map.md](../map.md).

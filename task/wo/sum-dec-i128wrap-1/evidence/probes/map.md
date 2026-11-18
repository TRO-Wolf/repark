# task/wo/sum-dec-i128wrap-1/evidence/probes/ — the step 0 probes (comment-free Python)

Run from this directory; `cells` and `lib` are imported by the runners. Commands and provenance:
[../map.md](../map.md).

- `cells.py` — the fixtures (`s`/`s4`: twelve decimal columns, fourteen shapes; `p2`: four two-file shapes; `big`:
  three 10^4-row shapes at p = 34; `k`: the `INT`, `BIGINT`, `DOUBLE` controls), the exact sum of each shape, every
  SQL cell (`sum_sql_cells`: 1,837), the DataFrame form names and the eight `EXPLAIN` cells.
- `lib.py` — the session builder for both engines (Iceberg hadoop catalog `lc`, the session zone pinned, the ANSI
  flag), the answer normaliser and the error text; the avg unit's `lib.py` with its app name changed.
- `run_door.py` — one engine and one ANSI mode: the Parquet tables, every SQL cell, the eight plans, the twelve
  DataFrame forms, and the Iceberg CTAS, `INSERT … SELECT` and `MERGE` (copy-on-write, merge-on-read) cells with the
  stored rows, the schema and every data file read with `pyarrow`.
- `run_mp.py` — the `s4` and `p2` cells again with `spark.sql.files.openCostInBytes` at 128 MiB, so Spark reads one
  split per file; records the partition counts.
- `run_native.py` — the native door `repark.sql()` over `VALUES` views: a sample of the `s` cells and every control.
- `analyze.py` — pairs each RePark (or native) cell with Spark's in the same mode, classifies (`EQUAL`, `TYPE`,
  `FORMAT`, `VALUE`, `REPARK_RAISES`, `SPARK_RAISES`, `BOTH_RAISE`, `BOTH_RAISE_OTHER`, `READ_ERR`), tags RePark's
  value against the exact sum (wrapped or exact, inside or past the type) and the stored files (committed or
  orphan); writes `verdicts.jsonl`, `summary.md` and `grids.md`.
- `tables.py` — the domain table's counts from `verdicts.jsonl`.
- `run_revision.py` — the revision (after the critic), one engine and one ANSI mode: table `s` as one Parquet file,
  the missed shapes (grouping sets and totals, `FILTER`, `CASE`, `* 1`, the `DOUBLE` cast, `HAVING < 0`, the mixed
  `DISTINCT`, `UNION ALL`), the DataFrame `pivot`, `rollup`, `cube`, `groupingSets`, `call_function` and ungrouped
  `F.try_sum`, the views across an ANSI flip, the observations, RePark's EXPLAIN VERBOSE of seven window forms, and
  the Iceberg statements under each metrics mode and at 100 and 101 columns, with the read-back and the files.
- `crossread.py` — Spark reads every Iceberg table in RePark's revision warehouses (four queries per table).
- `trim_revision.py` — trims the revision's raw files in place: Spark's quoted SQL fragment (` == SQL …`) is cut from
  each error, and the last, possibly cut, word from every other error and from every EXPLAIN line.
- `analyze_revision.py` — pairs the revision's cells, writes `rev-verdicts.jsonl` and prints the counts of the
  domain table §12.
- `compact.py` — copies the raw outputs into [../raw/](../raw/map.md) without SQL text or timings, with scratch paths
  replaced by `<scratchpad>` (the avg unit's, with one more pattern for the paths in truncated plans).

Up: [../map.md](../map.md).

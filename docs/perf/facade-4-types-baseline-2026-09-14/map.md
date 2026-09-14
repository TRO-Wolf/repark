# map — docs/perf/facade-4-types-baseline-2026-09-14

## Purpose

The runnable harness behind
[facade-4-types-baseline-2026-09-14.md](../facade-4-types-baseline-2026-09-14.md)
(FACADE-4 step 0). One session, one process, no JVM. Both scripts run under
`systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0` with
`OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8` against a release native module;
the runner waits for an idle box (no cargo/rustc/maturin) before each cell and
reports medians of 5 reps after one warmup.

## Contents

- [run_baseline.py](run_baseline.py) — the four baseline cells, verdict NO WALL: (a)
  `repark_type_to_arrow` per-call µs on the F1 schema set plus a spy count of
  calls per `createDataFrame` / `collect` / `to_arrow` / `show` / `df.schema`
  of a 1e5 frame; (b) `struct_type_from_arrow` round-trip; (c) DDL parse
  (`fromDDL`/`_parse_datatype_string`) and DDL write (`simpleString`/`toDDL`)
  walls; (d) cProfile cumulative share of the conversion set inside
  `df.schema`, `createDataFrame(pandas)` and `spark.read.csv(inferSchema)` at
  1e5 rows. Prints JSON to stdout:
  `OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0 .venv/bin/python docs/perf/facade-4-types-baseline-2026-09-14/run_baseline.py`
- [census_probe.py](census_probe.py) — the three-table agreement census:
  calls the facade `types.py` conversions, the `_csv_smart` rungs, and the
  reader lattice + Rust `spark_ddl_type_name`/`arrow_type_key` surfaces
  (including `DESCRIBE TABLE` vs `session.table(...).dtypes` on the same
  Iceberg CTAS bed) and prints every answer as JSON. Feeds the census table in
  the ledger.

## Pointers

- Up: [../map.md](../map.md)
- Baseline doc: [../facade-4-types-baseline-2026-09-14.md](../facade-4-types-baseline-2026-09-14.md)
- Ledger: [../../../task/ledgers/staging/facade-4-ledger.md](../../../task/ledgers/staging/facade-4-ledger.md)

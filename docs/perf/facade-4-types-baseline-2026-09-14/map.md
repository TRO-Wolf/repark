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
  of a 1e5 frame across all six conversion entry points
  (`repark_type_to_arrow`, `struct_type_from_arrow`, `_arrow_type_to_repark`,
  `_parse_datatype_string`, `_sql_type_to_arrow`, `DataType.fromDDL`);
  (b) `struct_type_from_arrow` round-trip; (c) DDL parse
  (`fromDDL`/`_parse_datatype_string`) and DDL write (`simpleString`/`toDDL`)
  walls; (d) cProfile cumulative share of the conversion set inside
  `df.schema`, `createDataFrame(pandas)` and `spark.read.csv(inferSchema)` at
  1e5 rows. Prints JSON to stdout; `--spy-only` re-runs just the cell (a)
  call-count leg:
  `OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0 .venv/bin/python docs/perf/facade-4-types-baseline-2026-09-14/run_baseline.py`
- [census_probe.py](census_probe.py) — the three-table agreement census
  (verdict: 18 agree rows, 24 measured disagreements D1–D24): calls the facade
  `types.py` conversions, the `_csv_smart` rungs (under both LTZ and NTZ
  sessions), and the reader lattice + Rust
  `spark_ddl_type_name`/`arrow_type_key` surfaces (including `DESCRIBE TABLE`
  vs `session.table(...).dtypes` on the same Iceberg CTAS bed) and prints
  every answer as JSON. Feeds the census table in the ledger.
- [census_answers.json](census_answers.json) — the committed probe output from
  the remediated run (all four unsigned widths, `pa.null()`, the offset
  timestamp literal, and `csv_smart_rungs_ntz` measured after the NTZ flip);
  the ledger's census table is distilled from it.
- [mutation_probe.py](mutation_probe.py) — the golden mutation proof: loads
  `test_facade_4_ddl_round_trip.py` and `facade_4_type_goldens.json`, applies
  each critic counterexample in process (fromJson forcing
  `containsNull`/`valueContainsNull` `True`; inbound `int8`/`int16` →
  `IntegerType`; preserving Arrow list/map item nullability; dropping inner
  struct field nullability), and prints whether each rebuild differs from the
  committed bytes plus which case ids moved. All four go red on this tree;
  restore goes green. Same memory-scope invocation as the runner.

## Pointers

- Up: [../map.md](../map.md)
- Baseline doc: [../facade-4-types-baseline-2026-09-14.md](../facade-4-types-baseline-2026-09-14.md)
- Ledger: [../../../task/ledgers/staging/facade-4-ledger.md](../../../task/ledgers/staging/facade-4-ledger.md)

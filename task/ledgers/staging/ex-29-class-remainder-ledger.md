# Unit ledger — EX-29 · v1.1 example backfill, the class-surface remainder (29 names)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-29 merges, or when the owner closes the
slate row.

**Unit:** EX-29 · **Date:** 2026-09-11 · **Model:** swe-2-high · **Branch:** `docs/ex-29-class-remainder` · **Base:** `a10062b8` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-29 lane card (29 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/{catalog,column,dataframe,io,session}/`,
`docs/examples/backlog.txt`, the `BACKLOG_BASELINE` constant in
`scripts/check_example_coverage.py`, `docs/spark-sql-iceberg-parity.md` §7,
`python/repark/tests/test_examples_dataframe_d.py`, `test_examples_column_a.py`,
`test_examples_window_catalog.py`, `test_examples_io_session.py`, lockstep `map.md` files, and
this ledger with its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other
`scripts/` line, `.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the 29 non-`F.*` backlog names of the lane card, re-derived at the base
`a10062b8` (all 29 present on `docs/examples/backlog.txt`; C-001 records the exact list). The
oracle is live PySpark 4.1.2 (ANSI on, UTC session zone, zulu-17 JVM, Iceberg
`iceberg-spark-runtime-4.1_2.13:1.11.0` hadoop catalog for the WriterV2 cell): every asserted
behaviour was re-measured there first, then matched on repark. Every stayed name already
carries a §7 row from its filing batch (EX-15…EX-22, EX-26); this unit re-measures each row's
repark half, extends the two §7 rows the new measurements touch (EX-DF-4's string-column arm,
EX-IO-7's missing-dependency note), and fills the pin gaps the earlier batches left — the
`get_database` / `list_databases` snake twins and two un-pinned arms (`describe` on a string
column, `colRegex` multi-match). No example file ships: no roster name has an arm where the
engines agree that a prior batch had not already taught. The six `Column.*` engine-plumbing
names are measured absent from `pyspark.sql.Column` and reported for an owner ruling (D-4).
No product file is touched.

**Roster (29):** `Catalog.getDatabase`, `Catalog.get_database`, `Catalog.listDatabases`,
`Catalog.list_databases`, `Column.for_select`, `Column.join_sql_part`,
`Column.spark_display_part`, `Column.spark_wrap_display_part`, `Column.sql_expr_part`,
`Column.sql_expr_without_alias`, `DataFrame.colRegex`, `DataFrame.col_regex`,
`DataFrame.createGlobalTempView`, `DataFrame.createOrReplaceGlobalTempView`,
`DataFrame.create_global_temp_view`, `DataFrame.describe`, `DataFrame.exceptAll`,
`DataFrame.except_all`, `DataFrame.groupingSets`, `DataFrame.grouping_sets`,
`DataFrame.intersectAll`, `DataFrame.intersect_all`, `DataFrame.toJSON`,
`DataFrameReader.excel`, `DataFrameReader.sheet_names`, `DataFrameStatFunctions.freqItems`,
`DataFrameWriterV2.overwrite`, `SparkSession.excel_sheet_names`, `SparkSession.read_excel`.

**Stay rows (23 names, all pre-existing, re-measured this unit):**

| Names | Row | Pin |
|---|---|---|
| `Catalog.getDatabase`, `Catalog.get_database` | EX-CAT-1 | `test_get_database_default_fields` |
| `Catalog.listDatabases`, `Catalog.list_databases` | EX-CAT-2 | `test_list_databases_fields_none` |
| `DataFrame.colRegex`, `DataFrame.col_regex` | EX-DF-1 | `test_colregex_spelling_divergence` |
| `DataFrame.createGlobalTempView`, `DataFrame.createOrReplaceGlobalTempView`, `DataFrame.create_global_temp_view` | EX-DF-2 | `test_global_temp_view_divergence` |
| `DataFrame.describe` | EX-DF-4 | `test_describe_row_order_divergence`, `test_describe_string_column_refuses` |
| `DataFrame.exceptAll`, `DataFrame.except_all` | EX-DF-3 | `test_except_all_divergence` |
| `DataFrame.groupingSets`, `DataFrame.grouping_sets` | EX-DF-8 | `test_grouping_sets_divergence` |
| `DataFrame.intersectAll`, `DataFrame.intersect_all` | EX-DF-7 | `test_intersect_all_divergence` |
| `DataFrame.toJSON` | EX-DF-17 | `test_tojson_refuses` |
| `DataFrameReader.excel`, `DataFrameReader.sheet_names`, `SparkSession.read_excel`, `SparkSession.excel_sheet_names` | EX-IO-7 | `test_excel_reader_refuses`, `test_excel_sheet_names_refuses` |
| `DataFrameStatFunctions.freqItems` | EX-DF-19 | `test_stat_freq_items_refuses` |
| `DataFrameWriterV2.overwrite` | EX-W2-1 | `test_writerv2_overwrite_condition_refuses` |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The 29-name roster above is exactly the lane card's list, every name re-derived from `docs/examples/backlog.txt` at the base `a10062b8`; 0 names are covered (none has an agreeing arm a prior batch had not already taught), 23 stay on the backlog with the §7 rows in the stay table, and the 6 `Column.*` plumbing names stay pending the owner ruling they raise. | The backlog grep at dispatch (all 29 present), the stay table, and the oracle table (29 rows, one per roster name). | **OPEN** |
| C-002 | Every stayed name's §7 row re-measured accurate on live PySpark 4.1.2 (ANSI on, UTC, 2026-09-11) and on repark in this clone: refusals still refuse with the recorded classes and texts, divergent cells still diverge as recorded; EX-DF-4 gains the string-column `AnalysisException` arm and EX-IO-7 gains the `openpyxl`-absent-from-the-locked-env note as the only row edits. | The oracle table below and the two amended rows. | **OPEN** |
| C-003 | Every stayed name carries a live pin exercising both spellings where twins exist: `test_get_database_default_fields` and `test_list_databases_fields_none` gain the `get_database` / `list_databases` legs, and `test_examples_dataframe_d.py` gains `test_describe_string_column_refuses` (EX-DF-4 string arm) and `test_colregex_multi_match_first_match` (EX-DF-1 multi-match arm); the four pin files run green. | The pytest run over the four pin files. | **OPEN** |
| C-004 | The six `Column.*` helpers are measured absent from `pyspark.sql.Column` on the oracle (`hasattr` False ×6) and present on repark's `Column` as bound engine-plumbing methods; no example pretends they are API and the inventory is not edited; the names stay on the backlog and the measurements are reported in the hand-back as inventory-narrowing candidates. | The oracle table's six rows and the hand-back `questions`. | **OPEN** |
| C-005 | `docs/examples/backlog.txt` and `BACKLOG_BASELINE` are unchanged (128): zero names left the backlog; the gate's static half and its `--require-execute` leg both exit 0 (`782 covered; 128 backlog; 2 exceptions; 207 examples` on this tree). | The gate's own counts line at the base `a10062b8` (782/128/207, the unit's before-line) and on the shipped tree, plus the red-first provocation below. | **OPEN** |
| C-006 | Red-first provocations run and pasted: (1) a stayed roster name deleted from `backlog.txt` reds the static gate with the `has no example COVERS row` finding naming it; (2) a wrong expected value injected into a shipped example reds the `--require-execute` leg naming the script and both values. Both restore to 0. | The provocation evidence below. | **OPEN** |

`LOGIC_SCORE` = **0/6 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Pending — filled after the gate runs.

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-11)

Pending — filled after the one-JVM measurement run.

## Gates (2026-09-11, on this tree)

Pending.

## Cost

The Devin (SWE-2) leg started 2026-09-11: read the contract, the EX-28 ledger, the coverage
gate, the §7 stay rows and the four pin files; measured live Spark 4.1.2 cells and repark
probes; extended two pins, two §7 rows, the maps, and this ledger.

## Disk

Pickup: `df -h` free space recorded in the session log. The oracle probe lives under the
gitignored `scratch/ex29/` (removable at close). The lane venv carries a debug native
(`repark._native.__debug_assertions__` True). The Iceberg runtime jar resolves from the
in-lane `.ivy2` cache (untracked).

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-29 moves no inventory, backlog, baseline, or wire; `.github/` is closed to this
unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-29-class-remainder
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Pending — filled at close.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/backlog.txt]
    - id: AT-2
      status: ATTACKED
      evidence: Pending — filled at close.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: Pending — filled at close.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: The gate and the pins are read-only over source files; no shared mutable engine state beyond the per-test session fixture.
    - id: AT-5
      status: N/A
      justification: No new execution surface; the gate's child drops AWS_* and PYTHONPATH and touches no network or cloud service.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the unit re-measures names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: Pending — filled at close.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: Pending — filled at close.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Pending — filled at close.
      artifacts: [scripts/map.md, python/repark/tests/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_d.py](../../../python/repark/tests/test_examples_dataframe_d.py), [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7
- Sibling: [ex-28-scalar-remainder-ledger.md](ex-28-scalar-remainder-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-29-class-remainder
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-29-class-remainder
  artifacts_verified:
    ledger: PENDING
    coverage_attestation: PENDING
    findings_ledger: PENDING
    shipped_flag_register: PASS (count 0)
  done_gate: PENDING
  status_update: v1.1 example backfill, class-surface remainder — in flight
  verdict: PENDING
  rejection_route: N/A
```

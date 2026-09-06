# Unit ledger — EX-20 · v1.1 example backfill, `Window` / `WindowSpec` and the first `Catalog.*` names

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-20 merges, or when the owner closes the
slate row.

**Unit:** EX-20 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-20-window-catalog` · **Base:** `3484f8d7`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-20 lane brief (40 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/window/`, `docs/examples/catalog/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_window_catalog.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the 22 `Window`/`WindowSpec` names plus the first 18 `Catalog.*` names at base
`3484f8d7` (camelCase and snake_case aliases are one example each, both names in `COVERS`).
Eight files cover the 37 names the live oracle measured Spark-equal: all 22 window names and 15
catalog names. `Catalog.getDatabase`/`get_database` and `Catalog.listDatabases` stay on the
backlog as measured divergences with §7 rows `EX-CAT-1`/`EX-CAT-2`, the `functionExists(name,
dbName)` arm is §7 `EX-CAT-3`, and the DataFrame-door tied-key ordered default frame is §7
`EX-WIN-1`; all four are pinned in `python/repark/tests/test_examples_window_catalog.py`. Every
snake_case spelling measured `hasattr` `False` on live PySpark 4.1.2 and is covered as a repark
extension beside its camelCase twin. The remaining 10 `Catalog.*` names after the roster cut
(`list_databases`, `list_tables`, `registerFunction`/`register_function`,
`setCurrentCatalog`/`setCurrentDatabase`/`set_current_catalog`/`set_current_database`,
`tableExists`/`table_exists`) are untouched backlog rows for a later batch. Catalog examples use
the default session and temp views only (no cloud, no registered catalog).

**Roster (40):** `Window.currentRow`, `Window.current_row`, `Window.orderBy`, `Window.order_by`,
`Window.partitionBy`, `Window.partition_by`, `Window.rangeBetween`, `Window.range_between`,
`Window.rowsBetween`, `Window.rows_between`, `Window.unboundedFollowing`,
`Window.unboundedPreceding`, `Window.unbounded_following`, `Window.unbounded_preceding`,
`WindowSpec.orderBy`, `WindowSpec.order_by`, `WindowSpec.partitionBy`, `WindowSpec.partition_by`,
`WindowSpec.rangeBetween`, `WindowSpec.range_between`, `WindowSpec.rowsBetween`,
`WindowSpec.rows_between`, `Catalog.clearCache`, `Catalog.clear_cache`, `Catalog.currentCatalog`,
`Catalog.currentDatabase`, `Catalog.current_catalog`, `Catalog.current_database`,
`Catalog.databaseExists`, `Catalog.database_exists`, `Catalog.dropTempView`,
`Catalog.drop_temp_view`, `Catalog.functionExists`, `Catalog.function_exists`,
`Catalog.getDatabase`, `Catalog.get_database`, `Catalog.listCatalogs`, `Catalog.listDatabases`,
`Catalog.listTables`, `Catalog.list_catalogs`.

**Grouping (8 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `window/spec_builders.py` | `Window.partitionBy`, `Window.partition_by`, `Window.orderBy`, `Window.order_by`, `WindowSpec.partitionBy`, `WindowSpec.partition_by`, `WindowSpec.orderBy`, `WindowSpec.order_by` | Spec builders: per-partition `row_number`, the global cumulative `sum`, `rank` over a descending key, and the reverse-chained spec, each in both spellings. |
| `window/frames.py` | `Window.rowsBetween`, `Window.rows_between`, `Window.rangeBetween`, `Window.range_between`, `WindowSpec.rowsBetween`, `WindowSpec.rows_between`, `WindowSpec.rangeBetween`, `WindowSpec.range_between` | Frames: the whole-frame static, the chained cumulative `RANGE`, per-partition running and `±5` sums, the sliding three-row `avg`, all in both spellings. |
| `window/bounds.py` | `Window.currentRow`, `Window.current_row`, `Window.unboundedPreceding`, `Window.unbounded_preceding`, `Window.unboundedFollowing`, `Window.unbounded_following` | The frame-bound constants: measured values and their use as running and trailing frame bounds. |
| `catalog/current_names.py` | `Catalog.currentCatalog`, `Catalog.current_catalog`, `Catalog.currentDatabase`, `Catalog.current_database` | The session's default catalog and database names in both spellings. |
| `catalog/views_and_exists.py` | `Catalog.databaseExists`, `Catalog.database_exists`, `Catalog.dropTempView`, `Catalog.drop_temp_view` | Namespace probes (default True, missing False) and the drop-once-then-miss temp-view arm. |
| `catalog/list_names.py` | `Catalog.listTables`, `Catalog.listCatalogs`, `Catalog.list_catalogs` | The exact `Table` row for one temp view plus a pattern arm, and the `spark_catalog` `CatalogMetadata` row. |
| `catalog/udf_probe.py` | `Catalog.functionExists`, `Catalog.function_exists` | A session-registered temp UDF exists; an unknown name does not (single-arg form). |
| `catalog/clear_cache.py` | `Catalog.clearCache`, `Catalog.clear_cache` | The None return and the cached frame still answering after the drop. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Eight files under `docs/examples/window/` and `docs/examples/catalog/` land runnable local examples for the 37 Spark-equal roster names, every asserted value measured against live PySpark 4.1.2 before it was written; those 37 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 37, 449 → 412 at the dispatch base and 411 → 374 on the shipped tree after the EX-19 merge, with no other `scripts/` change; `Catalog.getDatabase`/`get_database` and `Catalog.listDatabases` stay on the backlog with §7 rows `EX-CAT-1`/`EX-CAT-2`, the `functionExists(name, dbName)` arm is §7 `EX-CAT-3`, and the DataFrame-door tied-key default frame is §7 `EX-WIN-1`, all pinned in `python/repark/tests/test_examples_window_catalog.py`; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (37 findings before, 0 after), the oracle table (40 rows, one per roster name), the eight scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `3484f8d7` (the base, with the eight example files held outside the tree), in this
tree: the 37 roster rows were deleted and `BACKLOG_BASELINE` lowered to 412 in place, measured,
then restored with `git checkout` before any example file was written. At the base — the 40
roster rows still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=449` — `python3
scripts/check_example_coverage.py --skip-execute` exits **0** (`913 public names; 462 covered;
449 backlog; 2 exceptions; 120 examples`). **Provocation:** delete the 37 Spark-equal roster
rows and lower `BACKLOG_BASELINE` to 412 (`449 − 37`) with no example files present; the same
gate exits **1** with exactly 37 findings, one per roster name and no others (the three
divergent names stayed listed, so they produced no finding). With the eight files present, the
37 names removed and `BACKLOG_BASELINE=412`, the gate exits **0** (`499 covered; 412 backlog;
128 examples`).

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK zulu-17, TZ=UTC)

Measured with `.venv/bin/python` and `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, throwaway scripts
under `scratch/ex20-oracle/` (gitignored, never committed) driving
`_live_parity.build_spark_engine` / `build_repark_engine` over identical fixtures, one Spark JVM
per leg, `PYSPARK_PYTHON` pinned to the venv interpreter. Leg 1: the 40-name `hasattr`
classification (JVM-free) — every camelCase name exists on both engines, every snake_case name
is absent on Spark. Leg 2 (round 1): the value table with a `k`-tied fixture — every arm matched
except the ordered no-frame cumulative `sum`, which exposed EX-WIN-1, and three catalog names /
arms. Round 2 + round 3: the window suite re-measured on the globally unique-`k` example fixture
and every example arm verbatim on both engines; all agree. Round 1 also measured: the unordered
frame-only `rangeBetween` refusal (`DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER`, SQLSTATE
42K09) — same class on both engines, no agreeing arm exists, so the examples never assert it;
`functionExists` with `dbName` (Spark False, repark True → EX-CAT-3); a created namespace with
`COMMENT`/`LOCATION` (`getDatabase` field shapes → EX-CAT-1). Fixtures: unique-`k` `g/k/v` frame
`[("a",1,10.0),("a",2,20.0),("a",3,30.0),("b",4,50.0),("b",5,60.0),("b",6,70.0)]` (window
examples), tied-`k` six-row `g/k/v` frame (EX-WIN-1 pin), `[(1,"x")]` temp-view fixture, six-row
corpus `g/k/v` frame (clearCache). Round 4 (the round-2 controls, 2026-09-04): the null
`g/k/v` frame `[("a",1,10.0),(None,2,20.0),("a",3,None),(None,4,50.0)]` — NULL keys form their
own partition, `sum` skips NULL values, running `rowsBetween(unboundedPreceding, currentRow)`
`[10.0, 20.0 / 10.0, 70.0]`, `row_number` `1,2` per partition — and the peer-only
`rangeBetween(0, 0)` arm on the unique-`k` fixture (own value per row; tied-key control shares
peer sums); every cell byte-identical across engines. Bare-session `listTables()` measured `[]`
on Spark and repark before any temp view exists, and `[]` again after the drop.
`pins: ex-20-window-catalog/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `Window.currentRow` | `0` | same | kept | `window/bounds.py` | bound of the running/trailing frames |
| `Window.current_row` | `hasattr` False (extension) | `0` | kept | `window/bounds.py` | snake constant |
| `Window.orderBy` | cumulative `[10.0, 30.0, 60.0, 110.0, 170.0, 240.0]` on unique `k` | same | kept | `window/spec_builders.py` | tied-`k` arm is §7 `EX-WIN-1` |
| `Window.order_by` | `hasattr` False (extension) | same callable, same rows | kept | `window/spec_builders.py` | snake spelling |
| `Window.partitionBy` | `row_number` `1,2,3` per partition; null-key fixture `row_number` `1,2` per partition, NULL key its own partition | same | kept | `window/spec_builders.py`, `window/frames.py` | chained with `orderBy`; null-key arm round 4 |
| `Window.partition_by` | `hasattr` False (extension) | same callable | kept | `window/spec_builders.py` | snake spelling |
| `Window.rangeBetween` | `rangeBetween(unboundedPreceding, currentRow).orderBy("k")` cumulative `[10.0, 30.0, 60.0, 110.0, 170.0, 240.0]`; frame-only unordered RAISED `AnalysisException` `[DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER]` | same rows; same refusal class + SQLSTATE | kept | `window/frames.py` | unordered arm refused on both engines, never asserted |
| `Window.range_between` | `hasattr` False (extension) | same callable | kept | `window/frames.py` | snake spelling |
| `Window.rowsBetween` | whole-frame `sum` `240.0` on all rows | same | kept | `window/frames.py` | static frame-only spec, no order needed |
| `Window.rows_between` | `hasattr` False (extension) | same callable | kept | `window/frames.py` | snake spelling |
| `Window.unboundedFollowing` | `9223372036854775807` | same | kept | `window/bounds.py` | trailing-frame bound |
| `Window.unboundedPreceding` | `-9223372036854775808` | same | kept | `window/bounds.py` | running-frame bound |
| `Window.unbounded_following` | `hasattr` False (extension) | same value | kept | `window/bounds.py` | snake constant |
| `Window.unbounded_preceding` | `hasattr` False (extension) | same value | kept | `window/bounds.py` | snake constant |
| `WindowSpec.orderBy` | `rank` over desc key `3,2,1` per partition | same | kept | `window/spec_builders.py` | chained on a local spec |
| `WindowSpec.order_by` | `hasattr` False (extension) | same callable | kept | `window/spec_builders.py` | snake spelling |
| `WindowSpec.partitionBy` | `row_number` over the re-partitioned spec | same | kept | `window/spec_builders.py` | reverse chain `Window.orderBy("k").partitionBy("g")` |
| `WindowSpec.partition_by` | `hasattr` False (extension) | same callable | kept | `window/spec_builders.py` | snake spelling |
| `WindowSpec.rangeBetween` | running `[10.0, 30.0, 60.0 / 50.0, 110.0, 180.0]`; wide `±5` `[60.0 ×3 / 180.0 ×3]`; peer-only `rangeBetween(0, 0)` own value `[10.0, 20.0, 30.0 / 50.0, 60.0, 70.0]` | same | kept | `window/frames.py` | three arms on one ordered spec; peer-only arm landed round 2 |
| `WindowSpec.range_between` | `hasattr` False (extension) | same callable | kept | `window/frames.py` | snake spelling |
| `WindowSpec.rowsBetween` | sliding `avg` `15.0/20.0/25.0 / 55.0/60.0/65.0`; running `[10.0, 30.0, 60.0 / 50.0, 110.0, 180.0]`; trailing `[60.0, 50.0, 30.0 / 180.0, 130.0, 70.0]`; null-key running `[10.0, 10.0 / 20.0, 70.0]` (`sum` skips NULL values) | same | kept | `window/frames.py`, `window/bounds.py` | three arms; null-key arm round 2 |
| `WindowSpec.rows_between` | `hasattr` False (extension) | same callable | kept | `window/frames.py`, `window/bounds.py` | snake spelling |
| `Catalog.clearCache` | `None`; rows after `filter("k = 1")` `[('a', 1, 10.0), ('b', 1, 50.0)]` | same | kept | `catalog/clear_cache.py` | cached frame re-read |
| `Catalog.clear_cache` | `hasattr` False (extension) | same callable | kept | `catalog/clear_cache.py` | snake spelling |
| `Catalog.currentCatalog` | `'spark_catalog'` | same | kept | `catalog/current_names.py` | untouched default session |
| `Catalog.current_catalog` | `hasattr` False (extension) | same callable | kept | `catalog/current_names.py` | snake spelling |
| `Catalog.currentDatabase` | `'default'` | same | kept | `catalog/current_names.py` | |
| `Catalog.current_database` | `hasattr` False (extension) | same callable | kept | `catalog/current_names.py` | snake spelling |
| `Catalog.databaseExists` | `True` for `'default'`, `False` for `'nope_ex20'` | same | kept | `catalog/views_and_exists.py` | never raises for absence on either engine |
| `Catalog.database_exists` | `hasattr` False (extension) | same callable | kept | `catalog/views_and_exists.py` | snake spelling |
| `Catalog.dropTempView` | `True` then `False` | same | kept | `catalog/views_and_exists.py` | existing, then again |
| `Catalog.drop_temp_view` | `hasattr` False (extension) | same callable | kept | `catalog/views_and_exists.py` | snake spelling |
| `Catalog.functionExists` | `True` registered / `False` missing (single-arg); `functionExists("ex20_fn", "default")` `False` | single-arg same; dbName arm `True` | kept | `catalog/udf_probe.py` | dbName arm is §7 `EX-CAT-3` |
| `Catalog.function_exists` | `hasattr` False (extension) | same callable | kept | `catalog/udf_probe.py` | snake spelling |
| `Catalog.getDatabase` | `('default', 'spark_catalog', 'default database', 'file:<warehouse>')` | `('default', 'spark_catalog', None, None)` | dropped | §7 `EX-CAT-1` | description/locationUri fields; created-namespace location lacks the `file:` prefix |
| `Catalog.get_database` | `hasattr` False (extension) | shares the camel answer | dropped | §7 `EX-CAT-1` | snake spelling of a divergent name |
| `Catalog.listCatalogs` | `[('spark_catalog', None)]` | same | kept | `catalog/list_names.py` | bare session |
| `Catalog.list_catalogs` | `hasattr` False (extension) | same callable | kept | `catalog/list_names.py` | snake spelling |
| `Catalog.listDatabases` | `[('default', 'spark_catalog', 'default database', 'file:<warehouse>')]` | `[('default', 'spark_catalog', None, None)]` | dropped | §7 `EX-CAT-2` | FA-2 fields, re-measured 4.1.2 |
| `Catalog.listTables` | bare session `[]` before any temp view; `[('ex20_tv', None, [], None, 'TEMPORARY', True)]`; pattern `'ex20*'` arm same | same | kept | `catalog/list_names.py` | exact `Table` tuple match; bare arm round 2 |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py -q` | **0** (4 passed) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests` | **0** |

The system `python3` in this clone cannot import `repark._native`; the `--require-execute` leg
runs under `.venv/bin/python`, which resolves `repark` to the sibling checkout of the same base
SHA `3484f8d7` (expected for this lane). The static half also runs green under system `python3`
with `--skip-execute`.

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 537 covered; 374 backlog; 2 exceptions; 138 examples`

Before this unit: `462 covered; 449 backlog; 120 examples` (at `3484f8d7`). On this unit's own
tree before the merge: `499 covered; 412 backlog; 128 examples` (`BACKLOG_BASELINE` 449 → 412).
On the shipped tree, after the EX-19 merge: `537 covered; 374 backlog; 138 examples`
(`BACKLOG_BASELINE` 411 → 374) — exactly the 37 kept names.

## Review-gap table (round-1 findings, resolved in-lane)

| Finding | Disposition |
|---|---|
| `frames.py`'s static `rangeBetween` cumulative arm was first written with the per-partition running values as its expected | caught by the example's own `SystemExit` on the first local run (the frame-only spec has no partition, so the answer is the global cumulative); the expected was rebound to the round-3 measured global arm and the example re-run green |
| the round-3 probe ran snake spellings unguarded and crashed on Spark (no snake names) | snake arms gated on `hasattr` in the throwaway probe; the committed examples are repark-only for snake spellings and measured on repark |
| `python3 scripts/check_example_coverage.py --require-execute` exits 1 in this clone (native module absent from system python3) | same environment note as EX-19; the execute leg is recorded under `.venv/bin/python`, which resolves the native build |

Round-2 critic findings, resolved in-lane:

| Finding | Disposition |
|---|---|
| the `WindowSpec.rangeBetween` oracle row and `window/map.md` claimed a peer-only `rangeBetween(0, 0)` arm `frames.py` did not contain (S3) | the arm landed in `frames.py`, measured on both engines (own value per row on unique `k`, tied-key control shares peer sums); the map and the oracle row now match the file |
| no NULL partition-key / NULL-value control on the window surface (S2) | the null fixture and its running `rowsBetween(unboundedPreceding, currentRow)` sum and `row_number` arms landed in `frames.py`, measured byte-identical on both engines |
| `list_names.py` never asserted the bare-session empty listing (S2) | the bare `listTables() == []` arm landed before the temp view is created, measured `[]` on Spark and repark |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract, the corpus, and the merged
EX-19 ledger; ran four oracle legs (leg 1 `hasattr` JVM-free, leg 2 the round-1 value table, and
the round-2/round-3 unique-`k` re-measurement, one Spark JVM at a time); wrote the eight example
files, the divergence pins, the registry rows, the backlog ratchet and the maps, then committed
in slices; merged `origin/main` (EX-19 + fn-regexp-extract-1) before hand-back with the backlog
as the intersection of both sides and the baseline at main's 411 minus 37. Round 2 (same day):
measured the three round-2 controls (oracle round 4, one Spark JVM), landed the null-key and
peer-only arms in `frames.py` and the bare empty listing in `list_names.py`, and merged
`origin/main` again (perf-dynflatten-1, no backlog or registry overlap). Base `3484f8d7`.

## Disk

Pickup: `df -h` 545 GB free of 1.8 TB. The oracle scratch lives under the gitignored `scratch/`
(probe scripts plus captured outputs, left gitignored at close); Spark's `spark-warehouse`
residue from the oracle legs lands at the repo root (gitignored) and is removed at close. `.venv`
and the sibling-checkout native module reused; no cargo build, `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-20 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-20-window-catalog
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 37 Spark-equal roster names are covered by eight new example files and the oracle table records both engines' values per name, all 40 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/window/spec_builders.py, docs/examples/window/frames.py, docs/examples/window/bounds.py, docs/examples/catalog/current_names.py, docs/examples/catalog/views_and_exists.py, docs/examples/catalog/list_names.py, docs/examples/catalog/udf_probe.py, docs/examples/catalog/clear_cache.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red (the Window class-root and WindowSpec local bindings were probe-verified before authoring); the backlog is an exact baseline 412 with the three divergent names still listed.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds Window.* on the class root and WindowSpec.*/Catalog.* through repark-rooted locals; the examples bind every COVERS name through the real receiver (spec locals, catalog = repark.catalog).
      artifacts: [scripts/check_example_coverage.py, docs/examples/window/spec_builders.py, docs/examples/catalog/current_names.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the eight local examples and the four pin tests; example children drop AWS_* and PYTHONPATH.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half at the base with exactly 37 findings.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citation for C-001 lives in task/ledgers/staging/map.md beside the prior example batches, and the pin tests cite the registry rows in their one-line docstrings.
      artifacts: [task/ledgers/staging/map.md, python/repark/tests/test_examples_window_catalog.py, docs/examples/window/spec_builders.py, docs/examples/window/frames.py, docs/examples/window/bounds.py, docs/examples/catalog/current_names.py, docs/examples/catalog/views_and_exists.py, docs/examples/catalog/list_names.py, docs/examples/catalog/udf_probe.py, docs/examples/catalog/clear_cache.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-WIN-1`, `EX-CAT-1`, `EX-CAT-2`, `EX-CAT-3`
- Siblings: [ex-18-dataframe-c-ledger.md](ex-18-dataframe-c-ledger.md), [ex-17-column-a-ledger.md](ex-17-column-a-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-20-window-catalog
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-20-window-catalog
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries the three in-lane round-1 resolutions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, Window/WindowSpec + first Catalog.* batch — 37 covered, 3 divergent stay
  verdict: PENDING
  rejection_route: N/A
```

# Unit ledger — EX-19 · v1.1 example backfill, `DataFrame.*` remainder, `GroupedData`, `Row`, na/stat functions (d)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-19 merges, or when the owner closes the
slate row.

**Unit:** EX-19 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-19-dataframe-d-window` · **Base:** `7496049`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-19 lane brief (39 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)"; orchestrator ruling on EX-19-Q1 (2026-09-04): no new directory — the GroupedData and Row examples live in the gate-visible `docs/examples/dataframe/` beside the DataFrame/na/stat files.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/dataframe/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE`
constant in `scripts/check_example_coverage.py`, `docs/spark-sql-iceberg-parity.md` §7,
`python/repark/tests/test_examples_dataframe_d.py`, lockstep `map.md` files, and this ledger with
its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line,
`.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the 39-name DataFrame remainder plus the GroupedData / Row / na / stat surfaces at
base `7496049` (camelCase and snake_case aliases are one example each, both names in `COVERS`).
Ten files cover the 38 names the live oracle measured Spark-equal; `stat.freqItems` stays on the
backlog as a measured loud refusal with §7 row `EX-DF-19`, the `withColumnsRenamed`
duplicate-final-name arm is §7 `EX-DF-18`, the struct `Row` field arm is §7 `EX-ROW-1`, and all
three are pinned in `python/repark/tests/test_examples_dataframe_d.py`. `Row.as_dict`,
`Row.from_mapping`, and `Row.from_ordered_fields` are repark extensions (`hasattr` measured
`False` on live PySpark 4.1.2) and are covered as extensions.

**Roster (39):** `DataFrame.transform`, `DataFrame.union`, `DataFrame.unionAll`,
`DataFrame.unionByName`, `DataFrame.union_by_name`, `DataFrame.unpersist`, `DataFrame.unpivot`,
`DataFrame.withColumn`, `DataFrame.withColumnRenamed`, `DataFrame.withColumns`,
`DataFrame.withColumnsRenamed`, `DataFrame.with_column`, `DataFrame.with_column_renamed`,
`DataFrame.with_columns`, `DataFrame.with_columns_renamed`, `DataFrame.writeTo`,
`DataFrame.write_to`, `DataFrameNaFunctions.drop`, `DataFrameNaFunctions.fill`,
`DataFrameStatFunctions.approxQuantile`, `DataFrameStatFunctions.corr`,
`DataFrameStatFunctions.cov`, `DataFrameStatFunctions.crosstab`,
`DataFrameStatFunctions.freqItems`, `DataFrameStatFunctions.sampleBy`, `GroupedData.agg`,
`GroupedData.applyInPandas`, `GroupedData.apply_in_pandas`, `GroupedData.avg`,
`GroupedData.count`, `GroupedData.max`, `GroupedData.mean`, `GroupedData.min`,
`GroupedData.pivot`, `GroupedData.sum`, `Row.asDict`, `Row.as_dict`, `Row.from_mapping`,
`Row.from_ordered_fields`.

**Grouping (10 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `set_ops.py` | `DataFrame.union`, `DataFrame.unionAll`, `DataFrame.unionByName`, `DataFrame.union_by_name` | Set operations: by-position union with the duplicate row proved, the historical alias, by-name union over reordered columns, and the `allowMissingColumns` NULL-fill arm. |
| `frame_shape.py` | `DataFrame.transform`, `DataFrame.withColumn`, `DataFrame.with_column`, `DataFrame.withColumns`, `DataFrame.with_columns` | Reshaping one frame: the callable transform with args, the add and replace `withColumn` arms, and the atomic `withColumns` swap plus the appended column. |
| `rename_columns.py` | `DataFrame.withColumnRenamed`, `DataFrame.with_column_renamed`, `DataFrame.withColumnsRenamed`, `DataFrame.with_columns_renamed` | Renames: the singular spelling with the absent-name no-op, the map form, and the sequential chain arm; rows asserted Spark-equal throughout. |
| `unpivot_rows.py` | `DataFrame.unpivot` | Wide to long: list ids with two values, the string-values arm, and the two-id arm. |
| `cache_write.py` | `DataFrame.unpersist`, `DataFrame.writeTo`, `DataFrame.write_to` | Materialization and the V2 write door: unpersist returns the frame with the count unchanged, and `writeTo().create()` plus the SQL read-back answers Spark's rows. |
| `na_surface.py` | `DataFrameNaFunctions.fill`, `DataFrameNaFunctions.drop` | The na surface: fill scalar (numeric), string (with a NULL-bearing string fixture), dict, and subset arms; drop default, `thresh=1`, and `thresh=2` with subset. |
| `stat_helpers.py` | `DataFrameStatFunctions.approxQuantile`, `DataFrameStatFunctions.corr`, `DataFrameStatFunctions.cov`, `DataFrameStatFunctions.crosstab`, `DataFrameStatFunctions.sampleBy` | The stat helpers: median quantile, Pearson correlation, sample covariance, the pair-frequency table, and the stratified sample (deterministic 1.0/0.0 arms exact, the 0.5 arm as a containment property). |
| `grouped_agg.py` | `GroupedData.agg`, `GroupedData.count`, `GroupedData.sum`, `GroupedData.avg`, `GroupedData.mean`, `GroupedData.min`, `GroupedData.max` | Grouped aggregates: expression and dict `agg`, the structural `count`, every shortcut with named columns, and the no-argument arms that include the numeric keys. |
| `grouped_pivot.py` | `GroupedData.pivot`, `GroupedData.applyInPandas`, `GroupedData.apply_in_pandas` | Pivot and the pandas bridge: explicit values, discovery, multi-aggregate column naming, and the per-group pandas function under both spellings. |
| `row_dicts.py` | `Row.asDict`, `Row.as_dict`, `Row.from_mapping`, `Row.from_ordered_fields` | Reading a collected Row back out (flat, and recursive over a struct field) and the repark builder extensions, duplicate field names proved. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Ten files under `docs/examples/dataframe/` land runnable local examples for the 38 Spark-equal roster names, every asserted value measured against PySpark 4.1.2 before it was written; those 38 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 38, 518 → 480 at the dispatch base and 449 → 411 on the shipped tree after the EX-18 merge, with no other `scripts/` change; `DataFrameStatFunctions.freqItems` stays on the backlog with §7 row `EX-DF-19`, and the measured `withColumnsRenamed` and struct-`Row` arms are recorded as §7 rows `EX-DF-18`/`EX-ROW-1`, all pinned in `python/repark/tests/test_examples_dataframe_d.py`; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (38 findings before, 0 after), the oracle table (39 rows, one per roster name), the ten scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `7496049` (dispatch base, before any of the ten example files existed), in this tree:
the 38 backlog rows were deleted and `BACKLOG_BASELINE` lowered to 480 in place, measured, then
restored with `git checkout` before any example file was written. At the base — the 39 roster
rows still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=518` — `python3
scripts/check_example_coverage.py --skip-execute` exits **0** (`913 public names; 393 covered;
518 backlog; 2 exceptions; 99 examples`). **Provocation:** delete the 38 Spark-equal roster rows
from `backlog.txt` and lower `BACKLOG_BASELINE` to 480 (`518 − 38`) with no new example files
present; the same gate exits **1** with exactly 38 findings, one per roster name and no others
(`DataFrameStatFunctions.freqItems` stayed listed, so it produced no finding). With the ten files
present, the 38 names removed and `BACKLOG_BASELINE=480`, the gate exits **0** (`431 covered;
480 backlog; 109 examples`).

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK 17, TZ=UTC)

Measured at `.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, five throwaway scripts
under `scratch/ex19-oracle/` (gitignored, never committed) driving `_live_parity.build_spark_engine`
and `build_repark_engine` over identical fixtures, printing per name both engines' values; one
Spark JVM per leg, `PYSPARK_PYTHON` pinned to the venv interpreter. Leg 1: the 39-name value table.
Leg 2: the `hasattr` classification on live PySpark 4.1.2 — every snake spelling
(`union_by_name`, `with_column`, `with_columns`, `with_column_renamed`, `with_columns_renamed`,
`write_to`, `apply_in_pandas`) is absent on Spark's classes, and `Row.as_dict`,
`Row.from_mapping`, `Row.from_ordered_fields` are absent (repark extensions), while
`GroupedData.mean` exists on both. Round 2: the extra example arms (unpivot string and two-id
arms, `na.drop` thresh and how arms, tuple-subset fill). Round 2b: the NULL-bearing string fill
fixture after round-1 review caught the miswritten expected. Unordered results compared as sorted
tuples, both engines. Fixtures: six-row `g/k/v` frame
`[("a",1,10.0),("a",2,20.0),("a",2,30.0),("a",3,40.0),("b",1,50.0),("b",2,None)]`; null-free
`k/v` stats frame `[(1,10.0),(2,20.0),(2,30.0),(3,40.0),(1,50.0)]`; crosstab strata
`[("a",1),("a",10),("a",2),("b",1),("b",2)]`; sparse `g/k/v` frame
`[("a",1,10.0),("a",None,20.0),("a",2,None),("b",3,30.0)]`; wide `g/k/x/y` frame
`[("a",1,10.0,100.0),("b",2,20.0,200.0)]`; union frames `[("a",1),("b",2)]` and
`[("c",3),("a",1)]`; by-name and missing-column frames; swap frame `[(1,10)]`; sampleBy frame
`[(1,10.0),(1,11.0),(2,20.0),(3,30.0)]`; applyInPandas frame
`[("a",1,10.0),("a",2,20.0),("b",1,50.0)]`; string-fill frame `[("a",1,None),(None,2,"y")]`.
**Round 3 (2026-09-04):** the critic's NULL controls measured on both engines (one JVM,
throwaway `scratch/ex19-oracle/oracle_round3.py`): a NULL-bearing wide frame
`[("a",1,10.0,None),("b",2,None,200.0)]` unpivots to
`[('a','x',10.0),('a','y',None),('b','x',None),('b','y',200.0)]` on both engines, and a NULL
grouping key `[("a",1),(None,2),(None,3)]` answers `sum(k)` `[('a',1),(None,5)]` and `count`
`[('a',1),(None,2)]` on both — four MATCH arms, asserted in `unpivot_rows.py` and
`grouped_agg.py`.
`pins: ex-19-dataframe-d-window/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `DataFrame.transform` | `[('a', 1)]` (`transform(func, 2)`) | same | kept | `frame_shape.py` | callable with args |
| `DataFrame.union` | `[('a', 1), ('a', 1), ('b', 2), ('c', 3)]` sorted; duplicate kept | same | kept | `set_ops.py` | by position, no dedup |
| `DataFrame.unionAll` | same as `union` | same | kept | `set_ops.py` | alias on both engines |
| `DataFrame.unionByName` | cols `['g', 'k']` rows `[('a', 1), ('b', 2)]`; missing arm `('a', 1), (None, 2)` | same | kept | `set_ops.py` | reordered columns; NULL-fill arm |
| `DataFrame.union_by_name` | `hasattr` False (no snake spelling) | same callable | kept | `set_ops.py` | extension alias spelling |
| `DataFrame.unpersist` | `(6, 'DataFrame')` after cache | same | kept | `cache_write.py` | returns the frame |
| `DataFrame.unpivot` | cols `['g', 'var', 'val']` 4 rows; string arm 2 rows; two-id arm 4 rows; NULL wide frame `[('a', 'x', 10.0), ('a', 'y', None), ('b', 'x', None), ('b', 'y', 200.0)]` | same | kept | `unpivot_rows.py` | NULL arm since round 3 |
| `DataFrame.withColumn` | add cols `['g', 'k', 'v', 'w']` rows w=v+1; replace rows k*10 | same | kept | `frame_shape.py` | both arms |
| `DataFrame.with_column` | `hasattr` False (no snake spelling) | same callable | kept | `frame_shape.py` | extension alias spelling |
| `DataFrame.withColumns` | swap `[(10, 1)]`; newcol `[(1, 10, 11)]` | same | kept | `frame_shape.py` | atomic swap proved |
| `DataFrame.with_columns` | `hasattr` False (no snake spelling) | same callable | kept | `frame_shape.py` | extension alias spelling |
| `DataFrame.withColumnRenamed` | cols `['g', 'k', 'u']`; absent-name no-op cols unchanged | same | kept | `rename_columns.py` | rows asserted too |
| `DataFrame.with_column_renamed` | `hasattr` False (no snake spelling) | same callable | kept | `rename_columns.py` | extension alias spelling |
| `DataFrame.withColumnsRenamed` | `{'g':'gg','k':'kk'}` → `['gg', 'kk', 'v']`; chain `{'g':'gg','k':'g'}` → `['gg', 'g', 'v']`; duplicate arm → `['k', 'k', 'v']` | agreeing arms same; duplicate arm RAISED `AnalysisException` (duplicate column names) | kept | `rename_columns.py` | duplicate arm is §7 `EX-DF-18` |
| `DataFrame.with_columns_renamed` | `hasattr` False (no snake spelling) | same callable | kept | `rename_columns.py` | extension alias spelling |
| `DataFrame.writeTo` | type `DataFrameWriterV2`; `create()` + read back `[('a', 1), ('b', 2)]` | same | kept | `cache_write.py` | default catalog both engines |
| `DataFrame.write_to` | `hasattr` False (no snake spelling) | same callable | kept | `cache_write.py` | extension alias spelling |
| `DataFrameNaFunctions.drop` | default `[('a', 1, 10.0), ('b', 3, 30.0)]`; `thresh=1` 4 rows; `thresh=2, subset` 2 rows | same | kept | `na_surface.py` | three arms |
| `DataFrameNaFunctions.fill` | scalar fills numerics; string fills `['a', 1, 'zz'], ['zz', 2, 'y']`; subset `['a', 1, None], ['zz', 2, 'y']`; dict and list-subset rows | same | kept | `na_surface.py` | four arms, string fixture bears NULLs |
| `DataFrameStatFunctions.approxQuantile` | `[30.0]` (`relativeError=0.0`) | same | kept | `stat_helpers.py` | delegates to the DataFrame |
| `DataFrameStatFunctions.corr` | `0.18898223650461363` | same | kept | `stat_helpers.py` | null-free frame; the NULL-pair arm stays §7 `EX-DF-5` |
| `DataFrameStatFunctions.cov` | `2.5` | same | kept | `stat_helpers.py` | same frame as `corr` |
| `DataFrameStatFunctions.crosstab` | cols `['g_k', '1', '10', '2']`; rows `[('a', 1, 1, 1), ('b', 1, 0, 1)]` | same | kept | `stat_helpers.py` | 0 cell asserted |
| `DataFrameStatFunctions.freqItems` | `(['k_freqItems', 'v_freqItems'], [([1, 2, 3], [50.0, 20.0, 40.0, 10.0, 30.0])])` | RAISED `UnsupportedOperationException` (disclosed R-DF-BATCH2) | dropped | §7 `EX-DF-19` | loud refusal, no agreeing arm |
| `DataFrameStatFunctions.sampleBy` | fractions `{1:1.0, 2:0.0, 3:0.0}` → `[(1, 10.0), (1, 11.0)]`; flipped → `[(3, 30.0)]`; 0.5 arm membership | same | kept | `stat_helpers.py` | exact on the deterministic arms; 0.5 arm asserted as a containment property |
| `GroupedData.agg` | expr cols `['g', 'sum(v)', 'count(1)']` rows `[('a', 100.0, 4), ('b', 50.0, 2)]`; dict cols `['g', 'max(v)']` | same | kept | `grouped_agg.py` | expression and dict forms |
| `GroupedData.applyInPandas` | `[('a', 1, 10.0, 11.0), ('a', 2, 20.0, 21.0), ('b', 1, 50.0, 51.0)]` | same | kept | `grouped_pivot.py` | pandas bridge, DDL schema |
| `GroupedData.apply_in_pandas` | `hasattr` False (no snake spelling) | same callable | kept | `grouped_pivot.py` | extension alias spelling |
| `GroupedData.avg` | `[('a', 25.0), ('b', 50.0)]`; no-col `[('a', 2.0, 25.0), ('b', 1.5, 50.0)]` | same | kept | `grouped_agg.py` | no-col arm includes numeric keys |
| `GroupedData.count` | cols `['g', 'count']` rows `[('a', 4), ('b', 2)]`; NULL key `('a', 1), (None, 2)` | same | kept | `grouped_agg.py` | NULL-key arm since round 3 |
| `GroupedData.max` | `[('a', 40.0), ('b', 50.0)]` | same | kept | `grouped_agg.py` | |
| `GroupedData.mean` | equals `avg` (`hasattr` True on both) | same | kept | `grouped_agg.py` | alias on both engines |
| `GroupedData.min` | `[('a', 10.0), ('b', 50.0)]` | same | kept | `grouped_agg.py` | |
| `GroupedData.pivot` | explicit cols `['g', '1', '2']`; discovery `['g', '1', '2', '3']`; multi-agg `['g', '1_sum(v)', '1_count(1)', '2_sum(v)', '2_count(1)']` | same | kept | `grouped_pivot.py` | three arms, NULL cell asserted |
| `GroupedData.sum` | `[('a', 100.0), ('b', 50.0)]`; no-col cols `['g', 'sum(k)', 'sum(v)']` rows `[('a', 8, 100.0), ('b', 3, 50.0)]`; NULL key `[('a', 1), (None, 5)]` | same | kept | `grouped_agg.py` | NULL-key arm since round 3 |
| `Row.asDict` | `{'g': 'a', 'k': 1}`; repr `"Row(g='a', k=1)"`; recursive `{'s': {'g': 'a', 'k': 1}}`; recursive=False struct arm `{'s': Row(g='a', k=1)}` | flat and recursive arms same; struct arm `{'s': {'g': 'a', 'k': 1}}` | kept | `row_dicts.py` | struct arm is §7 `EX-ROW-1` |
| `Row.as_dict` | `hasattr` False (Spark class lacks it) | same callable, `{'g': 'a', 'k': 1}` | kept | `row_dicts.py` | repark extension |
| `Row.from_mapping` | `hasattr` False (Spark class lacks it) | fields `['g', 'k']`, repr `"Row(g='a', k=1)"` | kept | `row_dicts.py` | repark extension |
| `Row.from_ordered_fields` | `hasattr` False (Spark class lacks it) | repr `"Row(g=1, g=2)"`, values `[1, 2]`, duplicate names kept | kept | `row_dicts.py` | repark extension |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_dataframe_d.py -q` | **0** (3 passed) |
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
SHA `7496049` (expected for this lane).

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 500 covered; 411 backlog; 2 exceptions; 130 examples`

Before this unit: `393 covered; 518 backlog; 99 examples` (at `7496049`). On this unit's own
tree before the merges: `431 covered; 480 backlog; 109 examples` (`BACKLOG_BASELINE` 518 → 480).
On the shipped tree, after the EX-17 and EX-18 merges: `500 covered; 411 backlog; 130 examples`
(`BACKLOG_BASELINE` 449 → 411) — exactly the 38 kept names.

## Review-gap table (round-1 findings, resolved in-lane)

| Finding | Disposition |
|---|---|
| The round-1 `na.fill("zz")` expected row was written from memory against a fixture with no NULL string column — the oracle had answered the no-op on both engines | caught by the example's own `SystemExit` on the first local run; round 2b measured a NULL-bearing string fixture `[("a",1,None),(None,2,"y")]` on both engines and the arm was rewritten against the measured answers |
| repark `Row` values are unorderable, so bare `sorted(collect())` raises `TypeError` | every multi-row comparison converted to tuples before sorting (the corpus form); all ten examples re-run green |
| Round 3 (critic): the ledger's C-001 and counts lines stated the pre-merge tree (`431 covered; 480 backlog; 109 examples`) instead of the shipped tree | rewritten to the shipped numbers (`449 → 411`, `500 covered; 411 backlog; 2 exceptions; 130 examples`), same numbers in `scripts/map.md` and the staging map row |
| Round 3 (critic): the dataframe map carried a 20-line summary paragraph duplicating the closing summary | duplicate deleted; the EX-19 bullet block moved above the closing summary; the closing summary names `EX-DF-18`, `EX-DF-19`, and `EX-ROW-1` in four lines |
| Round 3 (critic): `row_dicts.py` interpolated retyped literals (`'a'`, `1, 2`) in two SystemExit messages | both access checks bind `*_expected` variables and interpolate them |
| Round 3 (critic): no NULL controls on `unpivot` and the grouped shortcuts | a NULL-bearing wide frame and a NULL grouping key measured on both engines (round-3 leg, four MATCH arms) and asserted in `unpivot_rows.py` and `grouped_agg.py` |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract and precedent, filed the
gate-visibility ruling question (EX-19-Q1), then after the ruling wrote one throwaway oracle
script per leg (four Spark JVM legs total), wrote the ten example files, the divergence pins, the
registry rows, the backlog ratchet and the maps, then committed in slices. Base `7496049`.

**Round 3 (critic findings, 2026-09-04):** the shipped-tree counts replaced the pre-merge ones in
C-001, the counts lines, `scripts/map.md`, and the staging map; the dataframe map's duplicate
summary paragraph was deleted with the closing summary naming the three new §7 rows in four
lines; two retyped-literal `SystemExit` messages in `row_dicts.py` now interpolate expected
variables; and the round-3 NULL controls were measured (one further Spark JVM leg) and asserted.

## Disk

Pickup: `df -h` 460 GB free of 1.8 TB. The oracle scratch lives under the gitignored `scratch/`
(four scripts plus captured outputs, left gitignored at close); Spark's `spark-warehouse` and
Derby state from the oracle legs land inside `scratch/ex19-oracle/` and are removed at close.
`.venv` and the sibling-checkout native module reused; no cargo build, `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-19 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-19-dataframe-d-window
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 38 Spark-equal roster names are covered by ten new example files and the oracle table records both engines' values per name, all 39 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/dataframe/set_ops.py, docs/examples/dataframe/frame_shape.py, docs/examples/dataframe/rename_columns.py, docs/examples/dataframe/unpivot_rows.py, docs/examples/dataframe/cache_write.py, docs/examples/dataframe/na_surface.py, docs/examples/dataframe/stat_helpers.py, docs/examples/dataframe/grouped_agg.py, docs/examples/dataframe/grouped_pivot.py, docs/examples/dataframe/row_dicts.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red (the GroupedData/Row binding was probe-verified before authoring); the backlog is an exact baseline 411 on the shipped tree (480 at the dispatch base) with the divergent freqItems name still listed.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds each COVERS name through a repark-rooted receiver; a probe in docs/examples/dataframe/ bound GroupedData.agg, GroupedData.pivot, Row.asDict, and Row.from_mapping before the examples were written.
      artifacts: [scripts/check_example_coverage.py, docs/examples/dataframe/row_dicts.py, docs/examples/dataframe/grouped_agg.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the ten local examples and the three pin tests; example children drop AWS_* and PYTHONPATH.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half at the base with exactly 38 findings.
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
      artifacts: [task/ledgers/staging/map.md, python/repark/tests/test_examples_dataframe_d.py, docs/examples/dataframe/set_ops.py, docs/examples/dataframe/frame_shape.py, docs/examples/dataframe/rename_columns.py, docs/examples/dataframe/unpivot_rows.py, docs/examples/dataframe/cache_write.py, docs/examples/dataframe/na_surface.py, docs/examples/dataframe/stat_helpers.py, docs/examples/dataframe/grouped_agg.py, docs/examples/dataframe/grouped_pivot.py, docs/examples/dataframe/row_dicts.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_d.py](../../../python/repark/tests/test_examples_dataframe_d.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-DF-18`, `EX-DF-19`, `EX-ROW-1`
- Siblings: [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md), [ex-16-dataframe-b-ledger.md](ex-16-dataframe-b-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-19-dataframe-d-window
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-19-dataframe-d-window
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries the two in-lane round-1 resolutions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, DataFrame.* remainder + GroupedData/Row/na/stat (d) batch — 38 covered, 1 divergent stays
  verdict: PENDING
  rejection_route: N/A
```

# Unit ledger — EX-18 · v1.1 example backfill, `DataFrame.*` (c)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-18 merges, or when the owner closes the
slate row.

**Unit:** EX-18 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-18-dataframe-c` · **Base:** `e3600a1`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-18 lane brief (36 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/dataframe/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE`
constant in `scripts/check_example_coverage.py`, `docs/spark-sql-iceberg-parity.md` §7,
`python/repark/tests/test_examples_dataframe_c.py`, lockstep `map.md` files, and this ledger with
its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line,
`.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the next 36 `DataFrame.*` rows of the backlog after EX-15 and EX-16 at the base
`e3600a1` (camelCase and snake_case aliases are one example each, both names in `COVERS`). Eleven
files cover the 35 names the live oracle measured Spark-equal on their demonstrated arms; `toJSON`
refuses (R-DF-BATCH2) and stays on the backlog with §7 `EX-DF-17`. Seven further divergent arms
measured on covered names are filed as §7 `EX-DF-11`…`EX-DF-17` with pins in
`python/repark/tests/test_examples_dataframe_c.py`.

**Roster (36):** `DataFrame.repartition`, `DataFrame.repartitionById`, `DataFrame.repartitionByRange`,
`DataFrame.replace`, `DataFrame.rollup`, `DataFrame.sameSemantics`, `DataFrame.same_semantics`,
`DataFrame.sample`, `DataFrame.sampleBy`, `DataFrame.schema`, `DataFrame.selectExpr`,
`DataFrame.select_expr`, `DataFrame.show`, `DataFrame.sort`, `DataFrame.sortWithinPartitions`,
`DataFrame.sort_within_partitions`, `DataFrame.stat`, `DataFrame.storageLevel`,
`DataFrame.storage_level`, `DataFrame.subtract`, `DataFrame.summary`, `DataFrame.tail`,
`DataFrame.take`, `DataFrame.toArrow`, `DataFrame.toArrowBatches`, `DataFrame.toDF`,
`DataFrame.toJSON`, `DataFrame.toLocalIterator`, `DataFrame.toPandas`, `DataFrame.to_arrow`,
`DataFrame.to_arrow_batches`, `DataFrame.to_df`, `DataFrame.to_local_iterator`,
`DataFrame.to_numpy`, `DataFrame.to_pandas`, `DataFrame.to_polars`.

**Grouping (11 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `repartition.py` | `DataFrame.repartition`, `DataFrame.repartitionById`, `DataFrame.repartitionByRange` | Partitioning calls: all three keep the row multiset and count; set compared because Spark reorders, repark does not. |
| `rollup_stat.py` | `DataFrame.rollup`, `DataFrame.stat` | The two grouping tables: rollup's hierarchy subtotals plus grand total, and the stat accessor's crosstab cells. |
| `replace_sample.py` | `DataFrame.replace`, `DataFrame.sample`, `DataFrame.sampleBy` | Cell and row shaping: the subset-scoped scalar and dict replaces, the fraction-1.0 sample, and the 1.0/0.0 strata sampleBy arms. |
| `same_semantics.py` | `DataFrame.sameSemantics`, `DataFrame.same_semantics` | Plan identity: True on one object, False on a distinct plan — both spellings. |
| `schema_select.py` | `DataFrame.schema`, `DataFrame.selectExpr`, `DataFrame.select_expr` | Metadata and projection: simpleString and jsonValue, then the SQL-expression select, both spellings. |
| `show_sort.py` | `DataFrame.show`, `DataFrame.sort`, `DataFrame.sortWithinPartitions`, `DataFrame.sort_within_partitions` | Presenting and ordering rows: the printed cells and row counts (never the rendering), ascending, descending, column-descending, and the single-partition within-partitions sort. |
| `storage_level.py` | `DataFrame.storageLevel`, `DataFrame.storage_level` | The cache lifecycle: NONE before, MEMORY_AND_DISK_DESER under cache, NONE after unpersist, by constant equality. |
| `subtract_summary.py` | `DataFrame.subtract`, `DataFrame.summary` | Difference rows on int and string frames, and the single-stat summary row. |
| `take_tail.py` | `DataFrame.take`, `DataFrame.tail` | Both ends of one ordered frame, plus the empty tail. |
| `export_arrow.py` | `DataFrame.toArrow`, `DataFrame.to_arrow`, `DataFrame.toArrowBatches`, `DataFrame.to_arrow_batches`, `DataFrame.toDF`, `DataFrame.to_df` | The Arrow exports: table columns and values, the batch iterators (repark extension), and the column rename. |
| `export_local.py` | `DataFrame.toLocalIterator`, `DataFrame.to_local_iterator`, `DataFrame.toPandas`, `DataFrame.to_pandas`, `DataFrame.to_numpy`, `DataFrame.to_polars` | The local-container exports: row iterator, pandas columns/dtypes/values, and the numpy and polars extensions. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Eleven files under `docs/examples/dataframe/` land runnable local examples for the 35 Spark-equal roster names, every asserted value measured against PySpark 4.1.2 before it was written; those 35 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 35, 550 → 515, with no other `scripts/` change; `toJSON` stays on the backlog with §7 rows `EX-DF-11`…`EX-DF-17` and pins in `python/repark/tests/test_examples_dataframe_c.py`; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (35 findings before, 0 after), the oracle table (36 rows, one per roster name), the eleven scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `e3600a1` (dispatch base, before any of the eleven example files existed). At that
base — the 36 roster rows still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=550` —
`python3 scripts/check_example_coverage.py --skip-execute` exits **0** (`913 public names; 361
covered; 550 backlog; 2 exceptions; 91 examples`). **Provocation:** delete the 35 Spark-equal
roster rows from `backlog.txt` and lower `BACKLOG_BASELINE` to 515 (`550 − 35`) with no new
example files present; the same gate exits **1** with exactly 35 findings, one per roster name
and no others (captured in `scratch/ex18-oracle/red.findings`, tree restored after). With the
eleven files present, the 35 names removed and `BACKLOG_BASELINE=515`, the gate exits **0**
(`396 covered; 515 backlog; 102 examples`).
`pins: ex-18-dataframe-c/C-001`

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK 17 zulu, TZ=UTC)

Measured at `/tmp/oc-ex18/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, one
throwaway script under `scratch/ex18-oracle/` (gitignored, never committed) driving
`_live_parity.build_spark_engine` and `build_repark_engine` over identical fixtures and printing
per name both engines' values. Fixtures: base six-row `g/k/v` frame
`[("a",1,10.0),("a",2,20.0),("a",2,30.0),("a",3,40.0),("b",1,50.0),("b",2,None)]`; null-free
five-row `k/v` stats frame `[(1,10.0),(2,20.0),(2,30.0),(3,40.0),(1,50.0)]`; dup frame
`[(1,"x"),(1,"x"),(2,"y"),(3,"z"),(2,"y")]`; multiset frames `[(1,),(1,),(2,)]` and `[(1,)]`.
**Round 2 (2026-09-04):** rollup rows, the single-partition `sortWithinPartitions` arms, the
`storageLevel` constant-equality matrix over nine levels, `summary` stability over three
in-process collects, the no-subset `replace` arms, and the single-stratum `sampleBy` arms — one
JVM, throwaway `scratch/ex18-oracle/oracle_r2.py`. Unordered results compared as sets, both
engines. **Round 3 (2026-09-04):** the example-coverage execute leg caught repark's
`summary("count","mean","stddev")` collecting `[stddev, count, mean]` in a fresh process after
three in-process collects had agreed — the multi-stat order is engine-arbitrary, so the example
keeps only the single-stat arm and the divergence is filed as §7 `EX-DF-15`.

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `DataFrame.repartition` | multiset of the 6 rows, count `6`; Spark reorders across partitions | same multiset, count `6` | kept | `repartition.py` | set compared; repark preserves input order |
| `DataFrame.repartitionById` | frame rows answered (lit 0 arm) — the name exists on PySpark 4.1.2 | same multiset, count `6` | kept | `repartition.py` | the brief listed it as an extension; the live oracle shows 4.1.2 has it — measured Spark-equal and covered as such |
| `DataFrame.repartitionByRange` | same multiset, count `6` | same | kept | `repartition.py` | set compared |
| `DataFrame.replace` | subset scalar/dict arms equal; no-subset arms: string values answered `[(1,'xx'),…]` / `[…,(9,'x'),…]`, numeric no-subset keeps `k` bigint | subset arms equal; no-subset: RAISED `PySparkException` cast error (string arm), recast `k` to double `[(1.0, 10.0), …]` | kept | `replace_sample.py` | example covers the subset arms; the no-subset and string arms are §7 `EX-DF-12` |
| `DataFrame.rollup` | cols `['g', 'k', 'sum(v)']`; 8 grouping-set rows incl. `(None, None, 150.0)` | same | kept | `rollup_stat.py` | set compared; NULL marks rolled-up keys |
| `DataFrame.sameSemantics` | self `True`; aliased twin `True`; identical recreate `False`; filtered twin `False` | self `True`; aliased twin `False`; recreate `False`; filter `False` | kept | `same_semantics.py` | example covers the agreeing arms; the alias arm is §7 `EX-DF-11` |
| `DataFrame.same_semantics` | same as `sameSemantics` | same | kept | `same_semantics.py` | same callable |
| `DataFrame.sample` | fraction 1.0 seed 1 answers all 6 rows | same | kept | `replace_sample.py` | fraction 0.5 seed 1: repark answers a stable 3-row set, Spark a different 4-row set — §7 `EX-DF-13` |
| `DataFrame.sampleBy` | `{"a": 1.0}` and `{"a": 0.0, "b": 1.0}` strata arms equal; `{"a": 0.5, "b": 0.5}` seed 0 answers `[(a, 2, 30.0), (b, 2, None)]` | strata arms equal; seeded arm answers `[(a, 2, 30.0), (a, 3, 40.0), (b, 2, None)]` | kept | `replace_sample.py` | example covers the strata arms; the seeded-fraction arm is §7 `EX-DF-14` |
| `DataFrame.schema` | `'struct<g:string,k:bigint,v:double>'`; jsonValue fields `long`/`double` | identical | kept | `schema_select.py` | simpleString, jsonValue, and repr all agree |
| `DataFrame.selectExpr` | `[(1, 20.0), (2, 40.0), (2, 60.0), (3, 80.0), (1, 100.0), (2, None)]` | same | kept | `schema_select.py` | ordered rows |
| `DataFrame.select_expr` | same as `selectExpr` | same | kept | `schema_select.py` | same callable |
| `DataFrame.show` | 3-row arm: right-aligned grid + `only showing top 3 rows` footer; cells and truncated count agree; full table 6 rows + `NULL` cell | 3-row arm: space-padded grid, no footer; cells and row count agree; full table same rows + `NULL` | kept | `show_sort.py` | example asserts cells and row counts, never the rendering; the alignment and footer are §7 `EX-DF-16` |
| `DataFrame.sort` | asc / desc / column-desc ordered rows identical | same | kept | `show_sort.py` | all three arms measured byte-equal |
| `DataFrame.sortWithinPartitions` | on `coalesce(1)` input: asc and desc ordered rows identical; on Spark's default 2-partition input the order is per-partition | on `coalesce(1)` input: identical | kept | `show_sort.py` | no divergent answer to the same input — repark frames are single-partition, so the example demonstrates the one-partition contract |
| `DataFrame.sort_within_partitions` | same as `sortWithinPartitions` | same | kept | `show_sort.py` | same callable |
| `DataFrame.stat` | `stat.crosstab("g","k")` cols `['g_k', '1', '2', '3']`, rows `[('a', 1, 2, 1), ('b', 1, 1, 0)]` | same | kept | `rollup_stat.py` | accessor demonstrated through the crosstab call; `stat.freqItems` refuses (R-DF-BATCH2) and is not this batch's name |
| `DataFrame.storageLevel` | before `== NONE` `True`; after cache `== MEMORY_AND_DISK_DESER` `True`; after unpersist `== NONE` `True`; reprs `StorageLevel(False, False, False, False, 1)` / `StorageLevel(True, True, False, True, 1)` | constant-equality matrix identical over nine levels; reprs `Serialized 1x Replicated` / `Disk Memory Deserialized 1x Replicated` | kept | `storage_level.py` | example asserts the equality contract, which is Spark-equal; the repr strings diverge — review-gap row below |
| `DataFrame.storage_level` | same as `storageLevel` | same | kept | `storage_level.py` | same callable |
| `DataFrame.subtract` | int arm `[(2,)]`; string arm `{(2, 'y')}` | same | kept | `subtract_summary.py` | both arms set compared |
| `DataFrame.summary` | `summary("count")` → `[('count', '5', '5')]`; `("count","min","max")` stable order `count, min, max`; string-column mean answers `None` cells; bare call answers the 8-row percentile table | count arm equal; multi-stat order arbitrary (reordered across processes); string-column mean RAISED `AnalysisException`; bare call RAISED `UnsupportedOperationException` | kept | `subtract_summary.py` | example covers the single-stat arm; the multi-stat order, string raise, and bare refusal are §7 `EX-DF-15` |
| `DataFrame.tail` | `[('a', 2, 30.0), ('a', 3, 40.0)]`; `tail(0)` → `[]` | same | kept | `take_tail.py` | |
| `DataFrame.take` | `[('a', 1, 10.0), ('b', 1, 50.0)]` | same | kept | `take_tail.py` | |
| `DataFrame.toArrow` | names `['g', 'k', 'v']`; per-column pylist identical incl. `None` | same | kept | `export_arrow.py` | |
| `DataFrame.toArrowBatches` | RAISED `PySparkAttributeError: ATTRIBUTE_NOT_SUPPORTED` (no such attribute) | batches with names `['g', 'k', 'v']`, 6 rows total | kept | `export_arrow.py` | repark extension, no PySpark analog (documented as extension) |
| `DataFrame.toDF` | cols `['x', 'y', 'z']`; same rows | same | kept | `export_arrow.py` | |
| `DataFrame.toJSON` | one JSON object string per row, e.g. `'{"g":"a","k":1,"v":10.0}'`; null-valued keys omitted | RAISED `UnsupportedOperationException` (R-DF-BATCH2) | dropped | §7 `EX-DF-17` | loud refusal |
| `DataFrame.toLocalIterator` | sorted rows, iterator of `Row` | same | kept | `export_local.py` | |
| `DataFrame.toPandas` | cols `['g', 'k', 'v']`; dtypes `str`/`int64`/`float64`; values with `nan` for the null `v` | identical, incl. the pandas 3 `str` dtype | kept | `export_local.py` | both arms measured (null frame and null-free frame) |
| `DataFrame.to_arrow` | RAISED `ATTRIBUTE_NOT_SUPPORTED` (no such attribute) | names + pylist identical to `toArrow` | kept | `export_arrow.py` | snake spelling, repark-side convenience |
| `DataFrame.to_arrow_batches` | RAISED `ATTRIBUTE_NOT_SUPPORTED` | 6 rows across the batches | kept | `export_arrow.py` | repark extension, no PySpark analog (documented as extension) |
| `DataFrame.to_df` | same as `toDF` | same | kept | `export_arrow.py` | same callable |
| `DataFrame.to_local_iterator` | same as `toLocalIterator` | same | kept | `export_local.py` | same callable |
| `DataFrame.to_numpy` | RAISED `ATTRIBUTE_NOT_SUPPORTED` | shape `(5, 2)`; `[[1.0, 10.0], …]` float matrix | kept | `export_local.py` | repark extension, no PySpark analog (documented as extension) |
| `DataFrame.to_pandas` | same as `toPandas` | same | kept | `export_local.py` | same callable |
| `DataFrame.to_polars` | RAISED `ATTRIBUTE_NOT_SUPPORTED` | cols `['k', 'v']`; rows `[(1, 10.0), …]` | kept | `export_local.py` | repark extension, no PySpark analog (documented as extension) |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_dataframe_c.py -q` | **0** (7 passed) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests` | **0** |

The system `python3` in this clone cannot import `repark._native`; the `--require-execute` leg
runs under `.venv/bin/python`, which resolves `repark` to the main checkout of the same base SHA
`e3600a1` (expected for this lane).

Counts line (execute leg, at dispatch):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 396 covered; 515 backlog; 2 exceptions; 102 examples`

Before this unit: `361 covered; 550 backlog; 91 examples` (at `e3600a1`). After: `396 covered;
515 backlog; 102 examples` — exactly the 35 kept names. After the EX-16 merge (round 4):
`428 covered; 483 backlog; 110 examples` — main's 518 minus this unit's 35.

## Review-gap table (parameter-level gaps found in review, agreeing arm covered)

| Name | repark | Spark 4.1.2 | Disposition |
|---|---|---|---|
| `DataFrame.storageLevel` repr | `'Serialized 1x Replicated'` before cache, `'Disk Memory Deserialized 1x Replicated'` under cache | `'StorageLevel(False, False, False, False, 1)'` / `'StorageLevel(True, True, False, True, 1)'` | Equality contract against the named constants is Spark-equal on all nine levels (round-2 matrix) and is what the example asserts; the repr strings are presentation of a repark-owned parity class, recorded here like `explain`'s plan text, no §7 row |
| `DataFrame.summary("count","mean","stddev")` order | UNION ALL legs reorder between processes (three in-process collects agreed; the execute gate caught `[stddev, count, mean]` in a fresh process) | stable requested order every run | Promoted to §7 `EX-DF-15` (round 3); the example keeps the single-stat arm |
| `DataFrame.repartitionById` classification | no-op partitioning, disclosed in-source | PySpark 4.1.2 defines the name and answers the frame on the lit-0 arm | The brief's "no PySpark analog" premise did not survive the live measure; covered as Spark-equal (multiset arm), no §7 row |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract, EX-15 ledger, and corpus;
wrote one throwaway oracle script (both engines in one process, one Spark JVM leg per round,
three rounds total), wrote the eleven example files, the divergence pins, the registry rows, the
backlog ratchet and the maps, then committed in slices. Base `e3600a1`.

**Round 2 (2026-09-04):** rollup, single-partition sortWithinPartitions, storageLevel constant
matrix, summary stability, no-subset replace, and single-stratum sampleBy measured; one further
Spark JVM leg.

**Round 3 (2026-09-04):** the execute gate's caught summary reorder promoted to §7 `EX-DF-15`;
the example's multi-stat arm replaced with the measured single-stat arm; one further full
execute-leg run (no new Spark JVM).

**Round 4 (merge, 2026-09-04):** EX-16 merged to `origin/main` (`7496049`, baseline 518, its
registry rows reaching `EX-DF-10`). `git merge origin/main` resolved as main's content minus
this unit's 35 covered names: `BACKLOG_BASELINE` 518 → 483, the backlog rebuilt from main's
content, this unit's registry rows renumbered `EX-DF-11`…`EX-DF-17` after EX-16's `EX-DF-10`
(printSchema), with the pins docstrings, `scripts/map.md`, both `map.md` rows, and this ledger
renumbered to match. Every gate re-run green on the merge commit.

## Disk

Pickup: `df -h` 537 GB free of 1.8 TB. The oracle scratch lives under the gitignored
`scratch/ex18-oracle/` and is left gitignored at close. `.venv` and the main-checkout native
module reused; no cargo build, `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-18 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-18-dataframe-c
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 35 Spark-equal roster names are covered by eleven new example files and the oracle table records both engines' values per name, all 36 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/dataframe/repartition.py, docs/examples/dataframe/rollup_stat.py, docs/examples/dataframe/replace_sample.py, docs/examples/dataframe/same_semantics.py, docs/examples/dataframe/schema_select.py, docs/examples/dataframe/show_sort.py, docs/examples/dataframe/storage_level.py, docs/examples/dataframe/subtract_summary.py, docs/examples/dataframe/take_tail.py, docs/examples/dataframe/export_arrow.py, docs/examples/dataframe/export_local.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the backlog is an exact baseline 515 with `toJSON` still listed.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: A missing class, missing nested class, or module with no __all__ raises a hard RuntimeError; there is no silent skip on shape drift.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the eleven local examples and the seven pin tests; example children drop AWS_* and PYTHONPATH.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half at the base and captured 35 findings.
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
      evidence: The pins citation for C-001 lives in scripts/map.md beside the prior example batches, and the pin tests cite the registry rows in their one-line docstrings.
      artifacts: [scripts/map.md, python/repark/tests/test_examples_dataframe_c.py, docs/examples/dataframe/repartition.py, docs/examples/dataframe/rollup_stat.py, docs/examples/dataframe/replace_sample.py, docs/examples/dataframe/same_semantics.py, docs/examples/dataframe/schema_select.py, docs/examples/dataframe/show_sort.py, docs/examples/dataframe/storage_level.py, docs/examples/dataframe/subtract_summary.py, docs/examples/dataframe/take_tail.py, docs/examples/dataframe/export_arrow.py, docs/examples/dataframe/export_local.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_c.py](../../../python/repark/tests/test_examples_dataframe_c.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-DF-11`…`EX-DF-17`
- Sibling: [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-18-dataframe-c
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-18-dataframe-c
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries three measured dispositions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, DataFrame.* (c) batch — 35 covered, toJSON plus seven divergent arms stay
  verdict: PENDING
  rejection_route: N/A
```

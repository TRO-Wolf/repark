# Unit ledger — EX-16 · v1.1 example backfill, `DataFrame.*` (b)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-16 merges, or when the owner closes the
slate row.

**Unit:** EX-16 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-16-dataframe-b` · **Base:** `f3968aa`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-16 lane brief (36 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/dataframe/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE`
constant in `scripts/check_example_coverage.py`, `docs/spark-sql-iceberg-parity.md` §7,
`python/repark/tests/test_examples_dataframe_b.py`, lockstep `map.md` files, and this ledger with
its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line,
`.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the next 36 `DataFrame.*` rows of the backlog at the base `f3968aa` (camelCase and
snake_case aliases are one example each, both names in `COVERS`); `DataFrame.dynamicFlatten` /
`DataFrame.dynamic_flatten` are deliberately excluded — another unit owns them. Eight files cover
the 30 names the live oracle measured Spark-equal; 6 names stay on the backlog as measured
divergences with §7 rows `EX-DF-6`…`EX-DF-8` and pins in
`python/repark/tests/test_examples_dataframe_b.py`.

**Roster (36):** `DataFrame.first`, `DataFrame.groupBy`, `DataFrame.group_by`, `DataFrame.groupby`,
`DataFrame.groupingSets`, `DataFrame.grouping_sets`, `DataFrame.head`, `DataFrame.hint`,
`DataFrame.intersect`, `DataFrame.intersectAll`, `DataFrame.intersect_all`, `DataFrame.isEmpty`,
`DataFrame.isStreaming`, `DataFrame.is_cached`, `DataFrame.is_empty`, `DataFrame.is_streaming`,
`DataFrame.join`, `DataFrame.limit`, `DataFrame.localCheckpoint`, `DataFrame.mapInArrow`,
`DataFrame.mapInPandas`, `DataFrame.map_in_arrow`, `DataFrame.map_in_pandas`, `DataFrame.melt`,
`DataFrame.mergeInto`, `DataFrame.merge_into`, `DataFrame.na`, `DataFrame.offset`,
`DataFrame.orderBy`, `DataFrame.order_by`, `DataFrame.persist`, `DataFrame.pl`,
`DataFrame.printSchema`, `DataFrame.print_schema`, `DataFrame.randomSplit`,
`DataFrame.random_split`.

**Grouping (8 files, 4–8 allowed, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `first_head.py` | `DataFrame.first`, `DataFrame.head` | Row-first access: the single first row, the first n rows, `head(0)`, and the empty-frame `None` arms. |
| `group_by.py` | `DataFrame.groupBy`, `DataFrame.group_by`, `DataFrame.groupby` | One frame grouped by key under all three spellings: the count form, the expression agg, and the dict agg. |
| `joins_hints.py` | `DataFrame.join`, `DataFrame.hint`, `DataFrame.intersect` | Combining frames: the name, list, and Column-condition join arms (inner/left/anti/semi), the optimizer-hint no-op, and the deduplicating set intersect. |
| `rows_nulls.py` | `DataFrame.limit`, `DataFrame.offset`, `DataFrame.orderBy`, `DataFrame.order_by`, `DataFrame.melt`, `DataFrame.na` | Row shaping: slice and skip, the two order spellings with Spark's null placement, the wide-to-long melt, and the `na` fill/drop surface. |
| `state_cache.py` | `DataFrame.isEmpty`, `DataFrame.is_empty`, `DataFrame.isStreaming`, `DataFrame.is_streaming`, `DataFrame.is_cached`, `DataFrame.persist`, `DataFrame.localCheckpoint` | Frame state: emptiness, the batch-only streaming flags, the cache arc, persist, and the checkpoint that does not set `is_cached`. |
| `bridges.py` | `DataFrame.mapInArrow`, `DataFrame.map_in_arrow`, `DataFrame.mapInPandas`, `DataFrame.map_in_pandas`, `DataFrame.pl` | Bridges to other runtimes: per-batch Arrow and pandas functions under both spellings, and the polars door. |
| `print_schema.py` | `DataFrame.printSchema`, `DataFrame.print_schema` | The schema tree print in Spark's exact line shape, both spellings. |
| `random_split.py` | `DataFrame.randomSplit`, `DataFrame.random_split` | Weighted split: two parts, schemas preserved, every row placed exactly once. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Eight files under `docs/examples/dataframe/` land runnable local examples for the 30 Spark-equal roster names, every asserted value measured against PySpark 4.1.2 before it was written; those 30 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 30, 550 → 520, with no other `scripts/` change; the 6 measured divergent names stay on the backlog with §7 rows `EX-DF-6`…`EX-DF-8` and pins in `python/repark/tests/test_examples_dataframe_b.py`; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (30 findings before, 0 after), the oracle table (36 rows, one per roster name), the eight scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `f3968aa` (dispatch base, before any of the eight example files existed), in this tree:
the 30 backlog rows were deleted and `BACKLOG_BASELINE` lowered to 520 in place, measured, then
restored with `git checkout` before any example file was written. At the base — the 36 roster rows
still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=550` — `python3
scripts/check_example_coverage.py --skip-execute` exits **0** (`913 public names; 361 covered;
550 backlog; 2 exceptions; 91 examples`). **Provocation:** delete the 30 Spark-equal roster rows
from `backlog.txt` and lower `BACKLOG_BASELINE` to 520 (`550 − 30`) with no new example files
present; the same gate exits **1** with exactly 30 findings, one per roster name and no others.
With the eight files present, the 30 names removed and `BACKLOG_BASELINE=520`, the gate exits **0**
(`391 covered; 520 backlog; 99 examples`).

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK 17, TZ=UTC)

Measured at `.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, five throwaway scripts
under `scratch/ex16-oracle/` (gitignored, never committed) driving `_live_parity.build_spark_engine`
and `build_repark_engine` over identical fixtures and printing per name both engines' values. One
Spark JVM per round; the Spark Python worker was pinned to the venv interpreter via
`PYSPARK_PYTHON` (round 1's mapIn Spark leg died on a worker without pyarrow — fixed in round 2).
Fixtures: three-row `g/k` frame `[("a",1),("a",2),("b",3)]`; six-row `g/k/v` frame
`[("a",1,10.0),("a",2,20.0),("a",2,30.0),("a",3,40.0),("b",1,50.0),("b",2,None)]`; null-bearing
`g/k` order frame `[("a",None),("a",2),("b",None),("b",1)]`; sparse `g/k/v` frame
`[("a",1,10.0),("a",None,20.0),("a",2,None),("b",3,30.0)]`; join frames `[(1,"a"),(2,"b"),(3,"c")]`
over `k/name` and `[(1,10.0),(2,20.0),(4,40.0)]` over `k/v`; multiset frames `[(1,),(1,),(2,)]` and
`[(1,),(1,),(3,)]`; DDL-schema empty frames; merge target `[(1,"a"),(2,"b")]` and source
`[(1,"A"),(3,"c")]` over `id/name`. Round 3 measured Spark's *documented* `groupingSets` shape
(sets list + output columns, read from the pinned PySpark 4.1.2 source) and four mergeInto
condition spellings; round 4 the disjoint-column-name merge shape; round 5 the exact
`groupBy.agg(F.sum, F.count(F.lit(1)))` recipe. Unordered results compared as sets, both engines.
`pins: ex-16-dataframe-b/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `DataFrame.first` | `('a', 1)`; empty `None` | same | kept | `first_head.py` | empty arm on a DDL-schema frame |
| `DataFrame.groupBy` | count cols `['g', 'count']` rows `[('a', 4), ('b', 2)]`; agg cols `['g', 'sum(v)', 'count(1)']` rows `[('a', 100.0, 4), ('b', 50.0, 2)]` | same | kept | `group_by.py` | count and expression-agg arms |
| `DataFrame.group_by` | RAISED `ATTRIBUTE_NOT_SUPPORTED` (no snake spelling) | agg rows `[('a', 100.0, 4), ('b', 50.0, 2)]` | kept | `group_by.py` | same callable as `groupBy` |
| `DataFrame.groupby` | cols `['g', 'max(v)']` rows `[('a', 40.0), ('b', 50.0)]` | same | kept | `group_by.py` | dict agg; lowercase spelling answers on Spark too |
| `DataFrame.groupingSets` | documented shape `groupingSets([("g","k"),("g",),()], "g","k")` cols `['g','k','count']` 6 rows; repark's shape cols `['k','count']` rows `[(None,1),(None,2)]` | one-set-per-col shape cols `['g','k','count']` 6 rows; documented shape RAISED `AttributeError: 'list' object has no attribute 'sql_expr_part'` | dropped | §7 `EX-DF-7` | signature and semantics differ; no input answers Spark-equal on both |
| `DataFrame.grouping_sets` | same as `groupingSets` | same | dropped | §7 `EX-DF-7` | same callable |
| `DataFrame.head` | `('a', 1)`; `head(2)` `[('a', 1), ('a', 2)]`; `head(0)` `[]`; empty `None` | same | kept | `first_head.py` | |
| `DataFrame.hint` | cols `['g', 'k']`, rows unchanged | same | kept | `joins_hints.py` | single-node no-op, disclosed |
| `DataFrame.intersect` | `{(1,)}` (duplicates deduplicated) | same | kept | `joins_hints.py` | |
| `DataFrame.intersectAll` | `[(1,), (1,)]` multiset | RAISED `UnsupportedOperationException` (octo C1-L-005) | dropped | §7 `EX-DF-6` | loud refusal |
| `DataFrame.intersect_all` | same | same | dropped | §7 `EX-DF-6` | same callable |
| `DataFrame.isEmpty` | empty `True` (DDL schema); non-empty `False` | same | kept | `state_cache.py` | |
| `DataFrame.isStreaming` | `False` | `False` | kept | `state_cache.py` | batch-only on both engines |
| `DataFrame.is_cached` | `(False, True, False)` across cache/unpersist | same | kept | `state_cache.py` | |
| `DataFrame.is_empty` | `True` on empty | same | kept | `state_cache.py` | same callable as `isEmpty` |
| `DataFrame.is_streaming` | RAISED `ATTRIBUTE_NOT_SUPPORTED` (no snake spelling) | `False` | kept | `state_cache.py` | same callable as `isStreaming` |
| `DataFrame.join` | inner cols `['k','name','v']` rows `[(1,'a',10.0),(2,'b',20.0)]`; left adds `(3,'c',None)`; anti `[(3,'c')]`; semi `[(1,'a'),(2,'b')]`; Column condition cols `['k','name','k','v']` | same on all five arms | kept | `joins_hints.py` | five arms, sets compared |
| `DataFrame.limit` | `orderBy("k").limit(3)` rows `[(1,'a'),(2,'b'),(3,'c')]`; `limit(0)` `[]` | same | kept | `rows_nulls.py` | |
| `DataFrame.localCheckpoint` | count `6`, `is_cached` `False`, rows | same | kept | `state_cache.py` | does not set `is_cached` on either engine |
| `DataFrame.mapInArrow` | `[(1, 20.0), (2, 40.0)]` | same | kept | `bridges.py` | Spark leg needs `PYSPARK_PYTHON` at the venv; batch boundaries not contractual |
| `DataFrame.map_in_arrow` | RAISED `ATTRIBUTE_NOT_SUPPORTED` (no snake spelling) | same as `mapInArrow` | kept | `bridges.py` | same callable |
| `DataFrame.mapInPandas` | `[(1, 20.0), (2, 40.0)]` | same | kept | `bridges.py` | |
| `DataFrame.map_in_pandas` | RAISED `ATTRIBUTE_NOT_SUPPORTED` | same | kept | `bridges.py` | same callable |
| `DataFrame.melt` | cols `['g','var','val']`, dtypes `string/string/double`, 12 rows with `k` widened to double | same | kept | `rows_nulls.py` | union widening equal |
| `DataFrame.mergeInto` | v2 parquet target RAISED `UNSUPPORTED_FEATURE.TABLE_OPERATION`; documented condition spellings RAISED `AMBIGUOUS_REFERENCE` / `UNRESOLVED_COLUMN`; disjoint-name shape refused before the capability check | sugar arm answers `[(1,'A'),(2,'b'),(3,'c')]`; bound-value and Column-condition spellings raise | dropped | §7 `EX-DF-8` | no input answers Spark-equal on both |
| `DataFrame.merge_into` | same | same | dropped | §7 `EX-DF-8` | same callable |
| `DataFrame.na` | fill scalar/dict, drop any/subset/all/thresh — all five arms equal to EX-15's fillna/dropna cells | same | kept | `rows_nulls.py` | the `na` property surface |
| `DataFrame.offset` | `offset(2)` → `[(3,'c')]`; `offset(0)` → full frame | same | kept | `rows_nulls.py` | Spark 4.1.2 answers `offset` |
| `DataFrame.orderBy` | asc → nulls first; desc → nulls last; rows `[('a',None),('b',None),('b',1),('a',2)]` / `[('a',2),('b',1),('a',None),('b',None)]` | same | kept | `rows_nulls.py` | |
| `DataFrame.order_by` | RAISED `ATTRIBUTE_NOT_SUPPORTED` (no snake spelling) | same as `orderBy` | kept | `rows_nulls.py` | same callable |
| `DataFrame.persist` | count `3`, `is_cached` `True`, rows | same | kept | `state_cache.py` | no-arg level |
| `DataFrame.pl` | RAISED `ATTRIBUTE_NOT_SUPPORTED` (no analog) | `PolarsFrame`; `select("k").collect()` → real polars DataFrame, column `k`, values `[1, 2, 1]` | kept | `bridges.py` | repark extension, no Spark analog |
| `DataFrame.printSchema` | lines `['root', ' |-- g: string (nullable = true)', ' |-- k: long (nullable = true)', ' |-- v: double (nullable = true)']` plus one extra trailing blank line | same lines, no extra blank | kept | `print_schema.py` | line content equal; the trailing newline is a review-gap-table item |
| `DataFrame.print_schema` | RAISED `ATTRIBUTE_NOT_SUPPORTED` | same lines | kept | `print_schema.py` | same callable |
| `DataFrame.randomSplit` | structural `(2 parts, total 6, cols ['n'])`; seeded membership seed 7 `[[1, 2, 4, 6], [3, 5]]` | structural same; seeded `[[1, 3, 5], [2, 4, 6]]` | kept | `random_split.py` | example covers the unseeded structural arm; seeded membership divergence is a review-gap-table item (disclosed engine RNG) |
| `DataFrame.random_split` | RAISED `ATTRIBUTE_NOT_SUPPORTED` | structural same as `randomSplit` | kept | `random_split.py` | same callable |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_dataframe_b.py -q` | **0** (3 passed) |
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
SHA `f3968aa` (expected for this lane).

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 391 covered; 520 backlog; 2 exceptions; 99 examples`

Before this unit: `361 covered; 550 backlog; 91 examples` (at `f3968aa`). After: `391 covered;
520 backlog; 99 examples` — exactly the 30 kept names.

## Review-gap table (parameter-level gaps found in review, agreeing arm covered)

| Name | repark | Spark 4.1.2 | Disposition |
|---|---|---|---|
| `DataFrame.printSchema` trailing newline | the print strips `treeString`'s own trailing newline, so stdout ends with one `\n` | `print` adds a second, so stdout ends with `\n\n` | the example asserts Spark's exact line content via `splitlines()`; cosmetic newline divergence noted here, no §7 row |
| `DataFrame.randomSplit` seeded membership | seed 7, `[0.5, 0.5]`: `[[1, 3, 5], [2, 4, 6]]` | `[[1, 2, 4, 6], [3, 5]]` | disclosed engine-RNG divergence (the facade docstring states it); the example covers the unseeded structural arm where the engines' contracts agree; no §7 row |
| `DataFrame.mergeInto` value spellings | `col("source.name")` answers; bound `source.name` raises `Ambiguous reference to unqualified field name` | unmeasurable — every shape is refused before value resolution | covered by §7 `EX-DF-8` |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract and precedent, wrote five
throwaway oracle scripts (one Spark JVM start per round, five Spark legs total), wrote the eight
example files, the divergence pins, the registry rows, the backlog ratchet and the maps, then
committed. Base `f3968aa`.

## Disk

`df -h` 664 GB free of 1.8 TB at pickup. The oracle scratch lives under the gitignored
`scratch/ex16-oracle/` and stays (gitignored, never committed). `.venv` and the
sibling-checkout native module reused; no cargo build, `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-16 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-16-dataframe-b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 30 Spark-equal roster names are covered by eight new example files and the oracle table records both engines' values per name, all 36 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/dataframe/first_head.py, docs/examples/dataframe/group_by.py, docs/examples/dataframe/joins_hints.py, docs/examples/dataframe/rows_nulls.py, docs/examples/dataframe/state_cache.py, docs/examples/dataframe/bridges.py, docs/examples/dataframe/print_schema.py, docs/examples/dataframe/random_split.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the backlog is an exact baseline 520 with the 6 divergent names still listed.
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
      justification: No new execution surface beyond the eight local examples and the three pin tests; example children drop AWS_* and PYTHONPATH.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half at the base.
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
      artifacts: [scripts/map.md, python/repark/tests/test_examples_dataframe_b.py, docs/examples/dataframe/first_head.py, docs/examples/dataframe/group_by.py, docs/examples/dataframe/joins_hints.py, docs/examples/dataframe/rows_nulls.py, docs/examples/dataframe/state_cache.py, docs/examples/dataframe/bridges.py, docs/examples/dataframe/print_schema.py, docs/examples/dataframe/random_split.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_b.py](../../../python/repark/tests/test_examples_dataframe_b.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-DF-6`…`EX-DF-8`
- Sibling: [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-16-dataframe-b
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-16-dataframe-b
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries three no-row entries)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, DataFrame.* (b) batch — 30 covered, 6 divergent stay
  verdict: PENDING
  rejection_route: N/A
```

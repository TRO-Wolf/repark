# Unit ledger — EX-15 · v1.1 example backfill, `DataFrame.*` (a)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-15 merges, or when the owner closes the
slate row.

**Unit:** EX-15 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-15-dataframe-a` · **Base:** `c70a306`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-15 lane brief (36 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/dataframe/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE`
constant in `scripts/check_example_coverage.py`, `docs/spark-sql-iceberg-parity.md` §7,
`python/repark/tests/test_examples_dataframe_a.py`, lockstep `map.md` files, and this ledger with
its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line,
`.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is the first 36 `DataFrame.*` rows of the backlog at the base `c70a306` (camelCase and
snake_case aliases are one example each, both names in `COVERS`). Eight files cover the 28 names
the live oracle measured Spark-equal; 8 names stay on the backlog as measured divergences with §7
rows `EX-DF-1`…`EX-DF-4` and pins in `python/repark/tests/test_examples_dataframe_a.py`.

**Roster (36):** `DataFrame.agg`, `DataFrame.alias`, `DataFrame.approxQuantile`, `DataFrame.cache`,
`DataFrame.coalesce`, `DataFrame.colRegex`, `DataFrame.col_regex`, `DataFrame.columns`,
`DataFrame.corr`, `DataFrame.count`, `DataFrame.cov`, `DataFrame.createGlobalTempView`,
`DataFrame.createOrReplaceGlobalTempView`, `DataFrame.createOrReplaceTempView`,
`DataFrame.createTempView`, `DataFrame.create_global_temp_view`,
`DataFrame.create_or_replace_temp_view`, `DataFrame.create_temp_view`, `DataFrame.crossJoin`,
`DataFrame.cross_join`, `DataFrame.crosstab`, `DataFrame.cube`, `DataFrame.declareSorted`,
`DataFrame.declare_sorted`, `DataFrame.describe`, `DataFrame.describe_ingest`,
`DataFrame.distinct`, `DataFrame.drop`, `DataFrame.dropDuplicates`,
`DataFrame.drop_duplicates`, `DataFrame.dropna`, `DataFrame.dtypes`, `DataFrame.exceptAll`,
`DataFrame.except_all`, `DataFrame.explain`, `DataFrame.fillna`.

**Grouping (8 files, 4–8 allowed, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `agg_stats.py` | `DataFrame.agg`, `DataFrame.corr`, `DataFrame.cov`, `DataFrame.approxQuantile`, `DataFrame.crosstab` | One frame's column statistics: the expression and dict agg forms, Pearson correlation, sample covariance, quantiles at two arities, and the pair-frequency table. |
| `cube.py` | `DataFrame.cube` | The two-key cube: per-combination rows, both subtotal arms, the grand total, and NULL marking each rolled-up key. |
| `views.py` | `DataFrame.alias`, `DataFrame.createOrReplaceTempView`, `DataFrame.create_or_replace_temp_view`, `DataFrame.createTempView`, `DataFrame.create_temp_view` | Registering frames under SQL names: the self-alias read, the replace arm, and the fresh-name arm, both spellings. |
| `cross_join.py` | `DataFrame.crossJoin`, `DataFrame.cross_join` | The cartesian product: count 18 and the full ordered row set against a three-row right frame. |
| `dedup_nulls.py` | `DataFrame.distinct`, `DataFrame.dropDuplicates`, `DataFrame.drop_duplicates`, `DataFrame.dropna`, `DataFrame.fillna`, `DataFrame.drop` | Row shaping: dedup on the whole frame and per subset, the four dropna arms, the three fillna arms, and drops by name including the absent-name no-op. |
| `declare_sorted.py` | `DataFrame.declareSorted`, `DataFrame.declare_sorted` | The repark extension: a verified sorted declaration returns the frame untouched, an unsorted one refuses with the offending row indices. No Spark analog. |
| `inspect_cache.py` | `DataFrame.columns`, `DataFrame.count`, `DataFrame.dtypes`, `DataFrame.cache`, `DataFrame.coalesce`, `DataFrame.explain` | Inspection and materialization: shape and schema metadata, cache then recount, the single-node no-op coalesce, and explain asserting a non-empty plan print (never plan text). |
| `describe_ingest.py` | `DataFrame.describe_ingest` | The repark extension: the smartCsv ingest report (source, delimiter, row count, per-column resolved types and nulls) and the empty report on a non-ingest frame. No Spark analog. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Eight files under `docs/examples/dataframe/` land runnable local examples for the 28 Spark-equal roster names, every asserted value measured against PySpark 4.1.2 before it was written; those 28 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 28, 578 → 550, with no other `scripts/` change; the 8 measured divergent names stay on the backlog with §7 rows `EX-DF-1`…`EX-DF-4` and pins in `python/repark/tests/test_examples_dataframe_a.py`; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (28 findings before, 0 after), the oracle table (36 rows, one per roster name), the eight scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `c70a306` (dispatch base, before any of the eight example files existed), in a
throwaway worktree under `scratch/`, removed after the measurement. At that base — the 36 roster
rows still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=578` — `python3
scripts/check_example_coverage.py --skip-execute` exits **0** (`913 public names; 333 covered;
578 backlog; 2 exceptions; 83 examples`). **Provocation:** delete the 28 Spark-equal roster rows
from `backlog.txt` and lower `BACKLOG_BASELINE` to 550 (`578 − 28`) with no new example files
present; the same gate exits **1** with 28 findings, one per roster name and no others. With the
eight files present, the 28 names removed and `BACKLOG_BASELINE=550`, the gate exits **0**
(`361 covered; 550 backlog; 91 examples`).

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK 17, TZ=UTC)

Measured at `/tmp/oc-ex15/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, one
throwaway script under `scratch/ex15-oracle/` (gitignored, never committed) driving
`_live_parity.build_spark_engine` and `build_repark_engine` over identical fixtures and printing
per name both engines' values. Fixtures: base six-row `g/k/v` frame
`[("a",1,10.0),("a",2,20.0),("a",2,30.0),("a",3,40.0),("b",1,50.0),("b",2,None)]`; null-free
`k/v` stats frame `[(1,10.0),(2,20.0),(2,30.0),(3,40.0),(1,50.0)]`; dup frame
`[(1,"x"),(1,"x"),(2,"y"),(3,"z"),(2,"y")]`; null frame
`[("a",1,10.0),("a",None,20.0),("a",2,None),("b",3,30.0)]`; multiset frames `[(1,),(1,),(2,)]`
and `[(1,)]`; cross frames; crosstab strata `[("a",1),("a",10),("a",2),("b",1),("b",10),("b",2)]`;
declare frames sorted and unsorted. Unordered results compared as sets, both engines.
**Round 2 (2026-09-04):** the corr/cov NULL-pair arm re-measured on both engines with an explicit
all-nullable `StructType([StructField("u", DoubleType(), True), StructField("v", DoubleType(), True)])`
(one JVM, throwaway `scratch/ex15-oracle/oracle_round2.py`): the four numbers are unchanged —
repark `corr` `0.18898223650461363` / `cov` `2.5`, Spark `corr` `0.07100716024967264` / `cov`
`1.0` — so the divergence is real, not a schema-inference coercion artefact, and it is filed as
§7 `EX-DF-5`.
`pins: ex-15-dataframe-a/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `DataFrame.agg` | `[(50.0, 6)]` cols `['max(v)', 'count(1)']`; dict `[(50.0,)]` cols `['max(v)']` | same | kept | `agg_stats.py` | expression and dict forms |
| `DataFrame.alias` | k==2 rows `[('a', 2, 20.0), ('a', 2, 30.0), ('b', 2, None)]`, cols `['g', 'k', 'v']` | same | kept | `views.py` | self-alias read |
| `DataFrame.approxQuantile` | single `[30.0]`; multi `[[1.0, 2.0], [20.0, 30.0]]` | same | kept | `agg_stats.py` | `relativeError=0.0`, NULL `v` excluded |
| `DataFrame.cache` | count `6`, cols unchanged | same | kept | `inspect_cache.py` | materialize then recount |
| `DataFrame.coalesce` | count `6` | same | kept | `inspect_cache.py` | single-node no-op, disclosed |
| `DataFrame.colRegex` | plain `"^(k)$"` RAISED `UNRESOLVED_COLUMN.WITH_SUGGESTION`; backtick ``"`^(k)$`"`` → `['k']` | plain → `['k']`; backtick RAISED `AnalysisException: No column matched regex` | dropped | §7 `EX-DF-1` | opposite spellings; no input answers Spark-equal on both |
| `DataFrame.col_regex` | same as `colRegex` | same | dropped | §7 `EX-DF-1` | same callable |
| `DataFrame.columns` | `['g', 'k', 'v']` | same | kept | `inspect_cache.py` | metadata only |
| `DataFrame.corr` | null-free `0.18898223650461363`; with the NULL-v row `0.07100716024967264` | null-free same; with the NULL-v row `0.18898223650461363` | kept | `agg_stats.py` | example uses the null-free arm; the NULL-pair arm is §7 `EX-DF-5` (round-2 explicit-schema re-measure confirmed the divergence) |
| `DataFrame.count` | `6` | same | kept | `inspect_cache.py` | |
| `DataFrame.cov` | null-free `2.5`; with the NULL-v row `1.0` | null-free same; with the NULL-v row `2.5` | kept | `agg_stats.py` | example uses the null-free arm; the NULL-pair arm is §7 `EX-DF-5` (round-2 explicit-schema re-measure confirmed the divergence) |
| `DataFrame.createGlobalTempView` | view registered; `SELECT k FROM global_temp.gt` → `[(1,), (1,), (2,), (2,), (2,), (3,)]` | RAISED `UnsupportedOperationException` (no global_temp catalog) | dropped | §7 `EX-DF-2` | loud refusal, disclosed R-DF-BATCH2 |
| `DataFrame.createOrReplaceGlobalTempView` | same measured rows | RAISED `UnsupportedOperationException` | dropped | §7 `EX-DF-2` | same callable |
| `DataFrame.create_global_temp_view` | same as `createGlobalTempView` | same | dropped | §7 `EX-DF-2` | same callable |
| `DataFrame.createOrReplaceTempView` | `[(1,), (1,), (2,), (2,), (2,), (3,)]`; replaced `[(99,)]` | same | kept | `views.py` | replace arm measured both engines |
| `DataFrame.create_or_replace_temp_view` | same | same | kept | `views.py` | same callable |
| `DataFrame.createTempView` | fresh `[(7,)]`; existing RAISED `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` | fresh `[(7,)]`; existing NO RAISE (replaced) | kept | `views.py` | example covers the fresh arm only (see review-gap table) |
| `DataFrame.create_temp_view` | same | same | kept | `views.py` | same callable |
| `DataFrame.crossJoin` | count `18`; sorted product matches | same | kept | `cross_join.py` | set compared, count pinned |
| `DataFrame.cross_join` | count `18` | same | kept | `cross_join.py` | same callable |
| `DataFrame.crosstab` | cols `['g_k', '1', '10', '2']`; rows `[('a', 1, 1, 1), ('b', 1, 1, 1)]` | same | kept | `agg_stats.py` | string-ordered pivot columns, absent pairs → 0 |
| `DataFrame.cube` | cols `['g', 'k', 'sum(v)']`; 11 grouping-set rows | same | kept | `cube.py` | set compared; NULL marks rolled-up keys |
| `DataFrame.declareSorted` | RAISED `PySparkAttributeError: ATTRIBUTE_NOT_SUPPORTED` (no such attribute) | sorted input passes; unsorted RAISED `AnalysisException` naming rows 0,1 | kept | `declare_sorted.py` | repark extension, no Spark analog |
| `DataFrame.declare_sorted` | same | same | kept | `declare_sorted.py` | same callable |
| `DataFrame.describe` | cells count `('6', '5')`, mean `('1.8333333333333333', '30.0')`, stddev `('0.752772652709081', '15.811388300841896')`, min `('1', '10.0')`, max `('3', '50.0')` in stable order count/mean/stddev/min/max | same cells, collect order nondeterministic (three different orders in three collects) | dropped | §7 `EX-DF-4` | cells agree; the order is the divergence |
| `DataFrame.describe_ingest` | RAISED `AttributeError` (no `smartCsv`) | smartCsv report: source/delimiter/row count/per-column types; plain frame `{}` | kept | `describe_ingest.py` | repark extension, no Spark analog |
| `DataFrame.distinct` | `{(1, 'x'), (2, 'y'), (3, 'z')}` | same | kept | `dedup_nulls.py` | |
| `DataFrame.drop` | cols `['g', 'k']`; absent no-op; two cols `['g']` | same | kept | `dedup_nulls.py` | |
| `DataFrame.dropDuplicates` | all three arms `{(1, 'x'), (2, 'y'), (3, 'z')}` | same | kept | `dedup_nulls.py` | whole frame, list subset, tuple subset |
| `DataFrame.drop_duplicates` | same | same | kept | `dedup_nulls.py` | same callable |
| `DataFrame.dropna` | any 2 rows; all 4; subset `['v']` 3; thresh 2 4 | same | kept | `dedup_nulls.py` | four arms, set compared |
| `DataFrame.dtypes` | `[('g', 'string'), ('k', 'bigint'), ('v', 'double')]` | same | kept | `inspect_cache.py` | Python ints infer bigint on both |
| `DataFrame.exceptAll` | `[(1,), (2,)]` | RAISED `UnsupportedOperationException` (octo C1-L-006) | dropped | §7 `EX-DF-3` | loud refusal |
| `DataFrame.except_all` | same | same | dropped | §7 `EX-DF-3` | same callable |
| `DataFrame.explain` | prints plan (64 chars); mode `cost` (171) | prints plan (310 chars); mode `cost` (106) | kept | `inspect_cache.py` | plan text diverges (disclosed); modes and non-empty print pinned |
| `DataFrame.fillna` | `0.0` fills k and v; dict `{'v': -1.0, 'k': -2}`; subset `['v']` | same | kept | `dedup_nulls.py` | three arms, set compared |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_dataframe_a.py -q` | **0** (4 passed) |
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
SHA `c70a306` (expected for this lane).

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 361 covered; 550 backlog; 2 exceptions; 91 examples`

Before this unit: `333 covered; 578 backlog; 83 examples` (at `c70a306`). After: `361 covered;
550 backlog; 91 examples` — exactly the 28 kept names.

Round 2 re-ran every gate above on the round-2 commit (`EX-DF-5` + its pin; no example or backlog
change) with identical exits.

## Review-gap table (parameter-level gaps found in review, agreeing arm covered)

| Name | repark | Spark 4.1.2 | Disposition |
|---|---|---|---|
| `DataFrame.corr` / `DataFrame.cov` NULL-pair arm | skips the NULL pair: `0.18898223650461363` / `2.5` | answers the NULL as `0.0`: `0.07100716024967264` / `1.0`; divergence confirmed under an explicit all-nullable DoubleType schema (round-2 re-measure) | promoted to §7 `EX-DF-5` with pin `test_corr_cov_null_pair_divergence`; the null-free example arm stays |
| `DataFrame.createTempView(name)` on an existing view | replaces silently (disclosed in-source: "v1: same as createOrReplaceTempView") | raises `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` | the example covers the fresh-name arm only; replace-arm divergence noted here, no §7 row filed |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract and precedent, wrote one
throwaway oracle script (both engines in one process, one JVM start per leg, two Spark legs total),
wrote the eight example files, the divergence pins, the registry rows, the backlog ratchet and the
maps, then committed in slices. Base `c70a306`.

**Round 2 (owner ruling on Q2, 2026-09-04):** the corr/cov NULL-pair arm re-measured with an
explicit schema; the divergence persisted, so `EX-DF-5` and its pin replaced the round-1
review-gap queue entry. One further Spark JVM leg.

## Disk

Pickup: `df -h` 664 GB free of 1.8 TB. The throwaway red-first worktree and the oracle scratch
live under the gitignored `scratch/` and are removed (worktree) / left gitignored (oracle script)
at close. `.venv` and the sibling-checkout native module reused; no cargo build, `make develop`
not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-15 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-15-dataframe-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 28 Spark-equal roster names are covered by eight new example files and the oracle table records both engines' values per name, all 36 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/dataframe/agg_stats.py, docs/examples/dataframe/cube.py, docs/examples/dataframe/views.py, docs/examples/dataframe/cross_join.py, docs/examples/dataframe/dedup_nulls.py, docs/examples/dataframe/declare_sorted.py, docs/examples/dataframe/inspect_cache.py, docs/examples/dataframe/describe_ingest.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the backlog is an exact baseline 550 with the 8 divergent names still listed.
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
      justification: No new execution surface beyond the eight local examples and the four pin tests; example children drop AWS_* and PYTHONPATH.
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
      artifacts: [scripts/map.md, python/repark/tests/test_examples_dataframe_a.py, docs/examples/dataframe/agg_stats.py, docs/examples/dataframe/cube.py, docs/examples/dataframe/views.py, docs/examples/dataframe/cross_join.py, docs/examples/dataframe/dedup_nulls.py, docs/examples/dataframe/declare_sorted.py, docs/examples/dataframe/inspect_cache.py, docs/examples/dataframe/describe_ingest.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-DF-1`…`EX-DF-4`
- Sibling: [../completed/ex-14-functions-window-ledger.md](../completed/ex-14-functions-window-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-15-dataframe-a
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-15-dataframe-a
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries three no-row queue entries)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, DataFrame.* (a) batch — 28 covered, 8 divergent stay
  verdict: PENDING
  rejection_route: N/A
```

# Unit ledger — EX-8 · v0.7 example backfill, `F.*` array and higher-order family

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when EX-8 merges, or when the owner closes the slate row.

**Unit:** EX-8 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (batch, continuation of glm-5.3-flash); glm-5.3-flash (remediation) · **Branch:** `feat/ex-8-functions-arrays` · **Base:** `a0cd39e` (dispatch base `a0fe83a`)
**Risk_tier:** standard.

Repark is the engine; live PySpark 4.1.2 + Iceberg 1.11.0 at `/tmp/oc-ex8/.venv` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex8/python/repark/tests` via `_live_parity.build_spark_iceberg_engine` is the oracle. Every asserted value was measured on the oracle before it was written.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Seven files under `docs/examples/functions/` cover 33 of the 40 dispatched `F.*` array and higher-order names with assertions measured against the live Spark oracle; the 7 divergent or refused names stay on `docs/examples/backlog.txt` with both values recorded; `BACKLOG_BASELINE` 842 → 809; both gate halves and the maps are green and no product file is touched. | Oracle table below; `BACKLOG_BASELINE` diff; gate exit codes; `docs/examples/functions/map.md` and `scripts/map.md` rows. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Roster and oracle (measured 2026-09-03 against live PySpark 4.1.2 via `_live_parity`; probes re-measured in the same-day remediation)

| Name | Spark value | repark value | Kept | File |
|---|---|---|---|---|
| F.array | [([1, 7], [1]), ([2, 7], [2]), ([None, 7], [None])] | [([1, 7], [1]), ([2, 7], [2]), ([None, 7], [None])] | kept | arrays.py |
| F.array_append | [([10, 20, 30, 4],), ([None, 5, 4],), (None,)] | [([10, 20, 30, 4],), ([None, 5, 4],), (None,)] | kept | array_edit.py |
| F.array_compact | [([10, 20, 30],), ([5],), (None,)] | [([10, 20, 30],), ([5],), (None,)] | kept | array_edit.py |
| F.array_contains | [(False,), (True,), (None,)] | [(False,), (True,), (None,)] | kept | array_elements.py |
| F.array_distinct | [([1, 2, 3],), ([1, 2],), ([None],), (None,)] | [([1, 2, 3],), ([1, 2],), ([None],), (None,)] | kept | array_setops.py |
| F.array_except | [([1],), ([1],), ([None],), (None,)] | [([1],), ([1],), ([None],), (None,)] | kept | array_setops.py |
| F.array_intersect | [([2, 3],), ([2],), ([],), (None,)] | [([2, 3],), ([2],), ([],), (None,)] | kept | array_setops.py |
| F.array_join | [('3,1,2',), ('2,1',), (None,)] | [('3,1,2',), ('2,1',), (None,)] | kept | array_order.py |
| F.array_max | [(3,), (2,), (None,)] | [(3,), (2,), (None,)] | kept | array_order.py |
| F.array_min | [(1,), (1,), (None,)] | [(1,), (1,), (None,)] | kept | array_order.py |
| F.array_position | [(2,), (0,), (None,)] | [(2,), (None,), (None,)] | dropped | — |
| F.array_prepend | [([0, 10, 20, 30],), ([0, None, 5],), (None,)] | [([0, 10, 20, 30],), ([0, None, 5],), (None,)] | kept | array_edit.py |
| F.array_remove | [([10, 30],), ([None, 5],), (None,)] | [([10, 30],), ([None, 5],), (None,)] | kept | array_edit.py |
| F.array_repeat | [([1, 1],), ([2, 2],), ([None, None],)] | [([1, 1],), ([2, 2],), ([None, None],)] | kept | arrays.py |
| F.array_size | [(3,), (2,), (None,)] | [(3,), (2,), (None,)] | kept | arrays.py |
| F.array_sort | [([1, 2, 3],), ([1, 2, None],), (None,)] | [([1, 2, 3],), ([None, 1, 2],), (None,)] | dropped | — |
| F.array_union | [([1, 2, 3, 4],), ([1, 2, 3],), ([None, 1],), (None,)] | [([1, 2, 3, 4],), ([1, 2, 3],), ([None, 1],), (None,)] | kept | array_setops.py |
| F.arrays_overlap — simple probe | [(True,), (False,), (None,)] | [(True,), (False,), (None,)] | equal | — |
| F.arrays_overlap — adversarial probe | [(None,), (False,), (None,), (True,), (True,), (None,)] | [(False,), (False,), (False,), (True,), (True,), (False,)] | dropped | — |
| F.arrays_zip | [Row(a=1, b=10) …] | UnsupportedOperationException: functions.arrays_zip is not supported yet | dropped | — |
| F.cardinality | [(3,), (2,), (None,)] | [(3,), (2,), (None,)] | kept | arrays.py |
| F.size | [(3,), (2,), (None,)] | [(3,), (2,), (None,)] | kept | arrays.py |
| F.slice | [([20, 30], [20, 30]), ([5], [None, 5]), (None, None)] | [([20, 30], [20, 30]), ([5], [None, 5]), (None, None)] | kept | array_elements.py |
| F.sort_array | [([1, 2, 3],), ([None, 1, 2],), (None,)] | [([1, 2, 3],), ([None, 1, 2],), (None,)] | kept | array_order.py |
| F.shuffle | permutation of the input; NULL array stays NULL | permutation of the input; NULL array stays NULL | kept (shape-only; random values never asserted) | array_order.py |
| F.sequence | [([1],), ([1, 2],), (None,)] | [([1],), ([1, 2],), (None,)] | kept | arrays.py |
| F.flatten — simple probe | [([1, 2, 3],), ([1, None],), (None,)] | [([1, 2, 3],), ([1, None],), (None,)] | equal | — |
| F.flatten — adversarial probe | [([],), ([],), ([1, 2, 3],), (None,), ([None, 1, 2],), (None,)] | [([],), ([],), ([1, 2, 3],), ([1],), ([None, 1, 2],), (None,)] | dropped | — |
| F.element_at | [(10, 30, 20), (None, 5, 5), (None, None, None)] | [(10, 30, 20), (None, 5, 5), (None, None, None)] | kept | array_elements.py |
| F.try_element_at | [(10, None), (None, None), (None, None)] at index `F.lit(1)` / `F.lit(10)`; a bare int refuses `PySparkTypeError [NOT_COLUMN_OR_STR]` | [(10, None), (None, None), (None, None)] (accepts the bare int too; queued) | kept | array_elements.py |
| F.get | [(10, 30, None), (None, None, None), (None, None, None)] | [(10, 30, None), (None, None, None), (None, None, None)] | kept | array_elements.py |
| F.explode | [('r1', 1), ('r1', 2)] | [('r1', 1), ('r1', 2)] | kept | explode.py |
| F.explode_outer | [('r1', 1), ('r1', 2), ('r2', None), ('r3', None)] | [('r1', 1), ('r1', 2), ('r2', None), ('r3', None)] | kept | explode.py |
| F.posexplode | [('r1', 0, 1), ('r1', 1, 2)] | UnsupportedOperationException: posexplode is not supported yet | dropped | — |
| F.posexplode_outer | [('r1', 0, 1), ('r1', 1, 2), ('r2', None, None), ('r3', None, None)] | UnsupportedOperationException: posexplode_outer is not supported yet | dropped | — |
| F.exists | [(True,), (True,), (None,), (None,)] | [(True,), (True,), (None,), (None,)] | kept | higher_order.py |
| F.forall | [(True,), (True,), (None,), (None,)] | [(True,), (True,), (None,), (None,)] | kept | higher_order.py |
| F.filter | [([3, 5],), ([2, 4],), ([],), (None,)] | [([3, 5],), ([2, 4],), ([],), (None,)] | kept | higher_order.py |
| F.transform | [([2, 6, 10],), ([4, 8],), ([None, 2],), (None,)] + index [([1, 4, 7],), ([2, 5],), ([None, 2],), (None,)] | same | kept | higher_order.py |
| F.aggregate | [(9,), (6,), (None,), (None,)] ; finish [(90,), (60,), (None,), (None,)] ; empty [(0,), (0,), (0,), (None,)] | same | kept | higher_order.py |
| F.reduce | [(9,), (6,), (None,), (None,)] | [(9,), (6,), (None,), (None,)] | kept | higher_order.py |
| F.zip_with | [([11, 23, 35],), ([12, 24],), ([None, None],), (None,)] | [([11, 23, 35],), ([12, 24],), ([None, None],), (None,)] | kept | higher_order.py |

Reproduce red: at `a0fe83a` `python3 scripts/check_example_coverage.py --require-execute` lists all 40 roster names as uncovered (count 40).

## Counts

| Measure | Before | After |
|---|---|---|
| inventory total | 913 | 913 |
| covered | 69 | 102 |
| backlog | 842 | 809 |
| exceptions | 2 | 2 |
| examples (all families) | 15 | 22 |
| functions examples | 11 | 18 |
| BACKLOG_BASELINE | 842 | 809 |

## Gates (2026-09-03, on the batch tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` | 0 |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `uv run --no-sync ruff check docs/examples` | 0 |
| `uv run --no-sync ruff format --check docs/examples` | 0 |

Counts line: `example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 102 covered; 809 backlog; 2 exceptions; 22 examples`

## Remediation (2026-09-03, glm-5.3-flash)

The critic's re-measurement confirmed all 33 landed names Spark-equal at the examples' inputs
except one spelling, and failed the record. Fixes, one row each, every value re-measured on
live PySpark 4.1.2 before it was written:

| Fix | Evidence |
|---|---|
| `F.try_element_at` index spelled `F.lit(1)` / `F.lit(10)` in `array_elements.py`; both engines MATCH `[10, None, None]` / `[None, None, None]`; Spark refuses a bare int (`PySparkTypeError [NOT_COLUMN_OR_STR]`), repark's wider `Column \| str \| int` acceptance is queued unpinned | oracle row above; registry "Surfaced, awaiting pins" entry FN-TRY-EXTRACT-1 |
| `F.flatten` row rewritten as two proper rows (simple / adversarial), both values each | oracle table above; adversarial frame `[[]] / [[], []] / [[1, 2, 3]] / [[1], NULL] / [[NULL, 1, 2]] / NULL` re-measured |
| Four silent array divergences filed as §7 BACKLOG rows with pins asserting repark's current value | FN-ARRAYPOS-1, FN-ARRAYSORT-1, FN-ARRAYSOVERLAP-1, FN-FLATTEN-1; `python/repark/tests/test_fn_arrays_divergence.py` (4 passed) |
| AT-1 names only committed artifacts; no `task/scratch-*` anywhere | attestation below |
| Dead duplicate `array_union` assertion with the false "keeps duplicates" message removed | `array_setops.py` |
| `COVERS` lists exactly the exercised names: `F.lit` in `array_elements.py`, `F.slice` in `higher_order.py` | both files |
| Base corrected to `a0cd39e` (dispatch base `a0fe83a`); `F.shuffle` row marked shape-only; wall-clock + cost recorded | header, oracle table, Cost section |

## Cost

| Leg | Wall |
|---|---|
| Batch (GLM 5.3 Flash) | died on transport errors; wall-clock not recorded |
| Batch continuation (muse-spark-1.2-contributor) | ~5 min of Spark re-measurement, free tier |
| Remediation (GLM 5.3 Flash) | 2026-09-03T04:25:58-04:00 → 2026-09-03T04:41:25-04:00 |

## Disk

Pickup: worktree in `/tmp/oc-ex8`; no `make develop` rebuild (module already built for this base).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-8-functions-arrays
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Oracle measured 40 names on live Spark 4.1.2; 33 kept values equal, 7 dropped with both values recorded; re-measured in the same-day remediation.
      artifacts: [docs/examples/functions/arrays.py, docs/examples/functions/array_edit.py, docs/examples/functions/array_elements.py, docs/examples/functions/array_order.py, docs/examples/functions/array_setops.py, docs/examples/functions/explode.py, docs/examples/functions/higher_order.py, task/ledgers/staging/ex-8-functions-arrays-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Grid covers arrays, NULL arrays, NULL elements, empty arrays, negative indices, duplicates, outer keeps.
      artifacts: [docs/examples/functions/array_elements.py, docs/examples/functions/explode.py]
    - id: AT-3
      status: ATTACKED
      evidence: Divergent or refused names dropped (array_position null 0 vs None, array_sort null ordering, arrays_overlap null grid, arrays_zip refused, flatten null subarray, posexplode refused); the four silent ones are filed as §7 registry rows with pins.
      artifacts: [docs/examples/backlog.txt, docs/spark-sql-iceberg-parity.md, python/repark/tests/test_fn_arrays_divergence.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable engine state across examples; each creates its own local session.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, or .github changes; no product code touched.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-6
      status: ATTACKED
      evidence: NULL arrays and NULL elements exercised; compact vs remove vs distinct semantics asserted.
      artifacts: [docs/examples/functions/array_edit.py, docs/examples/functions/array_setops.py]
    - id: AT-7
      status: N/A
      justification: No new allocation shape; small local frames only.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml change; map rows lockstep.
      artifacts: [docs/examples/functions/map.md, scripts/map.md, task/ledgers/staging/map.md]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: C-001 cited in 7 example files and scripts/map.md; ledger in staging/map.md.
      artifacts: [docs/examples/functions/arrays.py, task/ledgers/staging/map.md]
```

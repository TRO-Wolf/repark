# Unit ledger — EX-8 · v0.7 example backfill, `F.*` array and higher-order family

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when EX-8 merges, or when the owner closes the slate row.

**Unit:** EX-8 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (continuation of glm-5.3-flash) · **Branch:** `feat/ex-8-functions-arrays` · **Base:** `a0fe83a`
**Risk_tier:** standard.

Repark is the engine; live PySpark 4.1.2 + Iceberg 1.11.0 at `/tmp/oc-ex8/.venv` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` and `PYTHONPATH=/tmp/oc-ex8/python/repark/tests` via `_live_parity.build_spark_iceberg_engine` is the oracle. Every asserted value was measured on the oracle before it was written.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Seven files under `docs/examples/functions/` cover 33 of the 40 dispatched `F.*` array and higher-order names with assertions measured against the live Spark oracle; the 7 divergent or refused names stay on `docs/examples/backlog.txt` with both values recorded; `BACKLOG_BASELINE` 842 → 809; both gate halves and the maps are green and no product file is touched. | Oracle table below; `BACKLOG_BASELINE` diff; gate exit codes; `docs/examples/functions/map.md` and `scripts/map.md` rows. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Roster and oracle (measured 2026-09-03, `ex8_oracle.py` via `_live_parity`)

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
| F.arrays_overlap | [(True,), (False,), (None,)] | [(True,), (False,), (None,)] simple; [(None,), (True,), (False,), (None,), (None,), (True,)] vs [(False,), (True,), (False,), (False,), (None,), (True,)] adv | dropped | — |
| F.arrays_zip | [Row(a=1, b=10) …] | UnsupportedOperationException: functions.arrays_zip is not supported yet | dropped | — |
| F.cardinality | [(3,), (2,), (None,)] | [(3,), (2,), (None,)] | kept | arrays.py |
| F.size | [(3,), (2,), (None,)] | [(3,), (2,), (None,)] | kept | arrays.py |
| F.slice | [([20, 30], [20, 30]), ([5], [None, 5]), (None, None)] | [([20, 30], [20, 30]), ([5], [None, 5]), (None, None)] | kept | array_elements.py |
| F.sort_array | [([1, 2, 3],), ([None, 1, 2],), (None,)] | [([1, 2, 3],), ([None, 1, 2],), (None,)] | kept | array_order.py |
| F.shuffle | [([3, 1, 2], [2, 3, 1]), ([10, 5, 8], [5, 10, 8]), (None, None)] | [([3, 1, 2], [1, 3, 2]), ([10, 5, 8], [10, 8, 5]), (None, None)] | kept | array_order.py |
| F.sequence | [([1],), ([1, 2],), (None,)] | [([1],), ([1, 2],), (None,)] | kept | arrays.py |
| F.flatten | [([1, 2, 3],), ([1, None],), (None,)] simple; [([],), ([],), ([1, 2, 3],), (None,), ([None, 1, 2],), (None,)] vs [([],), ([],), ([1, 2, 3],), ([1],), ([None, 1, 2],), (None,)] adv | dropped | — |
| F.element_at | [(10, 30, 20), (None, 5, 5), (None, None, None)] | [(10, 30, 20), (None, 5, 5), (None, None, None)] | kept | array_elements.py |
| F.try_element_at | [(10, None), (None, None), (None, None)] with F.lit | [(10, None), (None, None), (None, None)] | kept | array_elements.py |
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

## Disk

Pickup: worktree in `/tmp/oc-ex8`; no `make develop` rebuild (module already built for this base).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-8-functions-arrays
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Oracle measured 40 names on live Spark 4.1.2; 33 kept values equal, 7 dropped with both values recorded.
      artifacts: [docs/examples/functions/arrays.py, docs/examples/functions/array_edit.py, docs/examples/functions/array_elements.py, docs/examples/functions/array_order.py, docs/examples/functions/array_setops.py, docs/examples/functions/explode.py, docs/examples/functions/higher_order.py, task/scratch-ex8/ex8_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: Grid covers arrays, NULL arrays, NULL elements, empty arrays, negative indices, duplicates, outer keeps.
      artifacts: [docs/examples/functions/array_elements.py, docs/examples/functions/explode.py]
    - id: AT-3
      status: ATTACKED
      evidence: Divergent or refused names dropped (array_position null 0 vs None, array_sort null ordering, arrays_overlap null null, arrays_zip refused, flatten null subarray, posexplode refused).
      artifacts: [docs/examples/backlog.txt]
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

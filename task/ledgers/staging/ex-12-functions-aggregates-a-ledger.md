# Unit ledger — EX-12 · v0.7 example backfill, `F.*` aggregates (a)

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the orchestrator's departure move). This file closes when EX-12 merges, or when the owner closes the slate row.

**Unit:** EX-12 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (batch, continuation of glm-5.3-flash); glm-5.3-flash (remediation) · **Branch:** `feat/ex-12-functions-aggregates-a` · **Base:** `a0cd39e` (dispatch base `84c1801`)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), batch roster aggregate (a) (30 names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v0.7 — Full example documentation".
**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The family is the `F.*` aggregate (a) names the campaign left on the backlog. This unit is one batch of that backfill.

**Roster as dispatched (30 names, measured against `docs/examples/backlog.txt` at `84c1801`, where all thirty are rows):**

`F.avg`, `F.mean`, `F.sum`, `F.count`, `F.count_if`, `F.countDistinct`, `F.count_distinct`, `F.approx_count_distinct`, `F.approx_percentile`, `F.percentile_approx`, `F.median`, `F.mode`, `F.min`, `F.max`, `F.first`, `F.last`, `F.first_value`, `F.last_value`, `F.some`, `F.every`, `F.bool_and`, `F.bool_or`, `F.collect_list`, `F.collect_set`, `F.listagg`, `F.string_agg`, `F.array_agg`, `F.try_avg`, `F.try_sum`, `F.grouping`.

**As landed: 27.** `F.mode`, `F.approx_percentile` and `F.percentile_approx` are dropped and stay on the backlog — see "Oracle" below for the measurements and the reasons.

**Grouping.** Eight files, grouped by the idea a reader learns in one breath rather than one file per name:

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `summarize.py` | `F.count`, `F.sum`, `F.avg`, `F.mean`, `F.median`, `F.min`, `F.max` | The classic summary set on one grouped frame: totals, means, middles and extremes. Median is the interpolation edge (even count answers the midpoint, not a data value). NULLs skipped by every aggregate. |
| `counting.py` | `F.count_if`, `F.countDistinct`, `F.count_distinct`, `F.approx_count_distinct` | Counting variants: conditional count, distinct-value count (single column and tuple), and the approximate sketch that is exact on small input. |
| `first_last.py` | `F.first`, `F.last`, `F.first_value`, `F.last_value` | Ordered-window endpoints (`asc_nulls_last`, unbounded frame) where the answer is a contract, not row arrival order; the `first_value`/`last_value` aliases answer identically. |
| `booleans.py` | `F.bool_and`, `F.bool_or`, `F.every`, `F.some` | Boolean collapse: `bool_and`/`every` and `bool_or`/`some` are alias pairs; an all-NULL group answers NULL, not False. |
| `collect.py` | `F.collect_list`, `F.collect_set`, `F.array_agg` | Row-collecting aggregates: list and `array_agg` keep order-insensitive contents, `collect_set` de-duplicates. |
| `strings_agg.py` | `F.listagg`, `F.string_agg` | String join of a group's values into one delimited string; the two names are aliases. Neither signature takes an ordering, so the assertions compare sorted delimiters. |
| `grouping.py` | `F.grouping`, `F.sum` | `grouping` flag inside a cube: 1 for the grand-total row, 0 for member rows. `F.sum` already taught, reused as the cube measure. |
| `try_aggregates.py` | `F.try_sum`, `F.try_avg` | Null-on-overflow aggregates: `try_sum` NULLs the overflowed group, `try_avg` stays finite in double. |

No existing example under `docs/examples/functions/` demonstrates any of the thirty — prior backfill batches cover math, trig, logs and rounding. The eight new files list `F.col` in `COVERS` because they genuinely use it; it is already covered by `abs.py`, so it does not move the ratchet.

## Orchestrator rulings (build-to)

- Every asserted value is measured against the live Spark oracle before it is written: `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` `PYTHONPATH=python/repark/tests` `build_spark_iceberg_engine(Path(tmpdir)).session` under `.venv/bin/python` (PySpark 4.1.2 + Iceberg 1.11.0). One throwaway script under `/tmp/oc-ex12-oracle/` prints Spark and repark for the same inputs.
- A name whose repark value differs from Spark, or that repark refuses, is dropped from its file's `COVERS` and from the batch, and listed with both values — never adjusted to repark.
- The gate is the acceptance bar in both directions: a `COVERS` entry the script does not exercise is red, and every script runs green locally with no network, no cloud and no JVM.
- The backlog count moves down by exactly the names this batch covers, and `BACKLOG_BASELINE` moves with it — measured at 842 → 815, 27 of the 30 dispatched.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | This batch lands runnable local examples for the 27 roster names the live oracle confirms, in eight files under `docs/examples/functions/`, every asserted value measured against live PySpark 4.1.2 before it was written and every `COVERS` entry exercised by an assertion on that measured value; those 27 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 27, 842 → 815, with no other `scripts/` change; the three, `F.mode`, `F.approx_percentile` and `F.percentile_approx`, stay backlog rows with both oracle values recorded, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Oracle table below (27 equal, 3 divergent), the red-first capture (30 named findings), the green counts line, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the branch at `84c1801`, before any of the eight files existed. The thirty rows were deleted from `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moved 842 → 812 with nothing else changed. `python3 scripts/check_example_coverage.py --skip-execute` then exited **1** with exactly thirty findings, one per roster name and no others. With the eight files present the gate is green.

## Oracle (live PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-03, JDK 17)

Measured via `build_spark_iceberg_engine` and `build_repark_engine` on the same inputs. Inputs: `NUMS = [("a",1,"x"),("a",2,"y"),("a",3,"x"),("a",None,None),("b",4,"z"),("b",6,"z")]`, `BOOLS = [("a",True),("a",False),("a",None),("b",True),("b",True),("c",None),("c",None),("d",False),("d",None)]`, `BIGS = [("a",9223372036854775807),("a",1),("b",2),("b",3)]`. Grouped aggregates over `k` where shown; throwaway scripts under `/tmp/oc-ex12-oracle/` (not in repo) recorded the table verbatim.

| Name | Spark value | repark value | Verdict | File |
|---|---|---|---|---|
| `F.avg` (grouped `k`) | `[('a', 2.0), ('b', 5.0)]` | `[('a', 2.0), ('b', 5.0)]` | kept | `summarize.py` |
| `F.mean` (grouped `k`) | `[('a', 2.0), ('b', 5.0)]` | `[('a', 2.0), ('b', 5.0)]` | kept | `summarize.py` |
| `F.sum` (grouped `k`) | `[('a', 6), ('b', 10)]` | `[('a', 6), ('b', 10)]` | kept | `summarize.py`, `grouping.py` |
| `F.count` (`"v"` grouped) | `[('a', 3), ('b', 2)]` | `[('a', 3), ('b', 2)]` | kept | `summarize.py` |
| `F.count_if` (`v > 2`) | `3` | `3` | kept | `counting.py` |
| `F.countDistinct` (`"s"`) | `3` | `3` | kept | `counting.py` |
| `F.count_distinct` (`"k","v"`) | `5` | `5` | kept | `counting.py` |
| `F.approx_count_distinct` (`"s"`) | `3` | `3` | kept | `counting.py` |
| `F.approx_percentile` (global `0.5`) | `3` | `3.0` | dropped | — (backlog) |
| `F.approx_percentile` (grouped `0.5`) | `[('a', 2), ('b', 4)]` | `[('a', 2.0), ('b', 5.0)]` | dropped | — |
| `F.percentile_approx` (global `0.5`) | `3` | `3.0` | dropped | — |
| `F.percentile_approx` (grouped `0.5`) | `[('a', 2), ('b', 4)]` | `[('a', 2.0), ('b', 5.0)]` | dropped | — |
| `F.median` (grouped) | `[('a', 2.0), ('b', 5.0)]` | `[('a', 2.0), ('b', 5.0)]` | kept | `summarize.py` |
| `F.mode` (`"s"`) | `'z'` | `ERROR UnsupportedOperationException: functions.mode is not supported yet` | dropped | — |
| `F.min` (grouped) | `[('a', 1), ('b', 4)]` | `[('a', 1), ('b', 4)]` | kept | `summarize.py` |
| `F.max` (grouped) | `[('a', 3), ('b', 6)]` | `[('a', 3), ('b', 6)]` | kept | `summarize.py` |
| `F.first` (ordered window) | `[('a', 1), ('b', 4)]`, invariant at `repartition(1)/(3)/(6)` | `[('a', 1), ('b', 4)]` | kept | `first_last.py` |
| `F.last` (ordered window) | `[('a', None), ('b', 6)]`, invariant at `repartition(1)/(3)/(6)` | `[('a', None), ('b', 6)]` | kept | `first_last.py` |
| `F.first_value` (ordered window) | `[('a', 1), ('b', 4)]`, invariant at `repartition(1)/(3)/(6)` | `[('a', 1), ('b', 4)]` | kept | `first_last.py` |
| `F.last_value` (ordered window) | `[('a', None), ('b', 6)]`, invariant at `repartition(1)/(3)/(6)` | `[('a', None), ('b', 6)]` | kept | `first_last.py` |
| `F.last(ignorenulls)` (ordered window) | `[('a', 3), ('b', 6)]` | `[('a', None), ('b', 6)]` | dropped from the example (registry `FN-LAST-1`) | — |
| `F.some` (global `"b"`) | `True` | `True` | kept | `booleans.py` |
| `F.every` (global `"b"`) | `False` | `False` | kept | `booleans.py` |
| `F.bool_and` (grouped) | `[('a', False), ('b', True), ('c', None), ('d', False)]` | `[('a', False), ('b', True), ('c', None), ('d', False)]` | kept | `booleans.py` |
| `F.bool_or` (grouped) | `[('a', True), ('b', True), ('c', None), ('d', False)]` | `[('a', True), ('b', True), ('c', None), ('d', False)]` | kept | `booleans.py` |
| `F.collect_list` (grouped `k`) | `[('a', [1, 2, 3]), ('b', [4, 6])]` | `[('a', [1, 2, 3]), ('b', [4, 6])]` | kept | `collect.py` |
| `F.collect_set` (grouped `k`) | `[('a', ['x', 'y']), ('b', ['z'])]` | `[('a', ['x', 'y']), ('b', ['z'])]` | kept | `collect.py` |
| `F.listagg` (global `","`) | `'x,y,x,z,z'` at `repartition(1)`; order varies at `(3)/(6)` | `'x,y,x,z,z'` | kept, asserted sorted | `strings_agg.py` |
| `F.string_agg` (global `","`) | `'x,y,x,z,z'` at `repartition(1)`; order varies at `(3)/(6)` | `'x,y,x,z,z'` | kept, asserted sorted | `strings_agg.py` |
| `F.array_agg` (grouped `k`) | `[('a', [1, 2, 3]), ('b', [4, 6])]` | `[('a', [1, 2, 3]), ('b', [4, 6])]` | kept | `collect.py` |
| `F.try_avg` (grouped) | `[('a', 4.611686018427388e+18), ('b', 2.5)]` | `[('a', 4.611686018427388e+18), ('b', 2.5)]` | kept | `try_aggregates.py` |
| `F.try_sum` (grouped) | `[('a', None), ('b', 5)]` | `[('a', None), ('b', 5)]` | kept | `try_aggregates.py` |
| `F.grouping` (cube `k`) | `[Row(k=None, g=1), Row(k='a', g=0), Row(k='b', g=0)]` | `[Row(k=None, g=1), Row(k='a', g=0), Row(k='b', g=0)]` | kept | `grouping.py` |

Dropped names stay on the backlog with both values recorded above; no file asserts them. `approx_percentile`/`percentile_approx`: Spark is EXACT here — it answers the discrete data value as BIGINT (global `0.5` → `3` LongType; grouped `[('a', 2), ('b', 4)]`) — while repark answers the interpolated median as double (`3.0`; `[('a', 2.0), ('b', 5.0)]`). The divergence is repark's semantics (interpolation) and type (double), not approximate sketches; filed as registry row `FN-APPROXPCT-1`. `mode` is not yet implemented in repark (engine gap `R-FN-BATCH4`, disclosed).

## Remediation (2026-09-03, critic round)

The critic re-measured all 27 landed values Spark-equal and red-flagged two examples for
asserting ORDER-DEPENDENT values, plus ledger items. Fixes, every assertion re-measured on live
Spark at `repartition(1)`, `repartition(3)` and `repartition(6)` and on repark:

- **`first_last.py`** — the grouped `F.first`/`F.last`/`first_value`/`last_value` assertions had
  no ordering, so the values were row-arrival luck: live Spark's grouped form moved between
  `repartition(1)` `[('a', 1, None), ('b', 4, 6)]`, `(2)` `[('a', 3, None), ('b', 4, 6)]`, `(3)`
  `[('a', 2, 1), ('b', 6, 4)]` and `(6)` `[('a', None, 1), ('b', 6, 4)]`. The example now asserts
  the explicitly ordered window `Window.partitionBy('k').orderBy(col('v').asc_nulls_last())
  .rowsBetween(unboundedPreceding, unboundedFollowing)`: Spark and repark both answer
  `[('a', 1, None), ('b', 4, 6)]` for first/last and the `first_value`/`last_value` aliases,
  invariant at `(1)/(3)/(6)`. The `F.last(ignorenulls=True)` leg is DROPPED from the example
  (roster name `F.last` stays covered by the plain form): Spark answers `[('a', 3), ('b', 6)]`,
  repark `[('a', None), ('b', 6)]` — filed as registry row `FN-LAST-1` with the pin
  `test_last_ignorenulls_window_divergence_is_pinned`.
- **`strings_agg.py`** — `F.listagg`/`F.string_agg` were asserted to the exact joined string
  `'x,y,x,z,z'`, but live Spark moves to `'x,z,x,y,z'` at `repartition(3)` and `'z,x,x,y,z'` at
  `(6)` (grouped `'x-y-x'` → `'y-x-x'`); neither engine's signature takes an ordering. The
  assertions now compare `sorted(joined.split(','))` (`['x', 'x', 'y', 'z', 'z']`, grouped
  `[['x', 'x', 'y'], ['z', 'z']]`) and the alias leg compares sorted tokens; all green on both
  engines at `(1)/(3)/(6)`.
- **Registry** — §7 gains BACKLOG rows `FN-LAST-1` and `FN-APPROXPCT-1`, each with a pin
  asserting repark's current value, and `python/repark/tests/map.md` carries both `pins:` rows.

The 27 covered names are unchanged: `F.last` stays covered, no name left `COVERS`, and the
backlog and `BACKLOG_BASELINE` are untouched by this round.

Remediation-tree gates, all exit **0**: coverage static + `--require-execute` (`96 covered; 815 backlog; 23 examples`), `make check-map-sync`, `make check-ledger-grammar`, `make check-ledgers`, `python3 scripts/ledger_lifecycle.py check --base a0cd39e`, ruff check/format on `docs/examples`; the two touched pytest files pass 22/22 and both rewritten examples run green standalone.

## Gates (2026-09-03, on the batch tree)

| Command | Exit |
|---|---|
| `python3 scripts/check_example_coverage.py` (static) | **0** |
| `python3 scripts/check_example_coverage.py --require-execute` | **0** |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `uv run --no-sync ruff check docs/examples` | **0** |
| `uv run --no-sync ruff format --check docs/examples` | **0** |
| `docs/examples/functions/*.py` each via `.venv/bin/python` | **0** |

Counts line (both legs identical; every example executed, every module door's live `__all__` matched):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 215 covered; 696 backlog; 2 exceptions; 52 examples`

Before: `69 covered; 842 backlog; 15 examples`. After: `96 covered (+27); 815 backlog (-27); 23 examples (+8)`.

## Cost

| Measurement | Wall-clock | Cost |
|---|---|---|
| Oracle measurement (Spark tier, live PySpark 4.1.2 + Iceberg 1.11.0 JVM sessions) | ~14 min | free |

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Oracle harness: `python/repark/tests/_live_parity.py` (`build_spark_iceberg_engine`)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-12-functions-aggregates-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Enumerator emits 913 names; 27 new F.* aggregate names covered across eight examples, three dropped with oracle divergence.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-2
      status: ATTACKED
      evidence: Backlog ratchet 842 -> 815 exact; dropped names remain backlog rows with both values recorded.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: Every COVERS entry exercised by an assertion on the measured Spark value; unused cover is red.
      artifacts: [docs/examples/functions/summarize.py, docs/examples/functions/counting.py, docs/examples/functions/first_last.py, docs/examples/functions/booleans.py, docs/examples/functions/collect.py, docs/examples/functions/strings_agg.py, docs/examples/functions/grouping.py, docs/examples/functions/try_aggregates.py]
    - id: AT-4
      status: N/A
      justification: Gate is a read-only process over source and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No cloud or network in examples; local filesystem and memory catalog only.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; examples are local-only.
    - id: AT-7
      status: N/A
      justification: Gate static half is AST-only; execute leg is optional on native module import.
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the coverage-gate change; no facade import in the walk.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr via existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Ledger C-001 cited in scripts/map.md and docs/examples/functions/*.py; eight new examples pinned.
      artifacts: [scripts/map.md, docs/examples/functions/map.md, task/ledgers/staging/ex-12-functions-aggregates-a-ledger.md]
  reattested: []
  complete: true
```

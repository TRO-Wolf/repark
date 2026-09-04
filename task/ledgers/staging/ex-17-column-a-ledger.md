# Unit ledger — EX-17 · v1.1 example backfill, `Column.*` (a)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-17 merges, or when the owner closes the
slate row.

**Unit:** EX-17 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-17-column-a` · **Base:** `e3600a1`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-17 lane brief (40 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/column/`, `docs/examples/backlog.txt`, the `BACKLOG_BASELINE`
constant in `scripts/check_example_coverage.py`, `docs/spark-sql-iceberg-parity.md` §7,
`python/repark/tests/test_examples_column_a.py`, lockstep `map.md` files, and this ledger with
its `staging/map.md` row. Closed: `crates/`, `python/repark/src/`, every other `scripts/` line,
`.github/`, `STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The roster is every `Column.*` row of the backlog at the base `e3600a1` (40 names; camelCase and
snake_case aliases are one example each, both names in `COVERS`). Nine files cover the 32 names
the live oracle measured Spark-equal or repark-extension; 8 names stay on the backlog as engine
plumbing / accessor namespaces with no PySpark analog, one line each below. The two bare-name
arms the oracle measured divergent (the `F.col`-receiver `cast` select name and the unaliased
`getField` select name) are §7 rows `EX-COL-1`/`EX-COL-2` with pins in
`python/repark/tests/test_examples_column_a.py`; the examples keep the arms where the engines
agree. No product file is touched.

**Roster (40):** `Column.alias`, `Column.asc`, `Column.asc_nulls_first`, `Column.asc_nulls_last`,
`Column.between`, `Column.bitwiseAND`, `Column.bitwiseOR`, `Column.bitwiseXOR`, `Column.cast`,
`Column.contains`, `Column.desc`, `Column.desc_nulls_first`, `Column.desc_nulls_last`,
`Column.dt`, `Column.endswith`, `Column.eqNullSafe`, `Column.for_select`, `Column.getField`,
`Column.getItem`, `Column.ilike`, `Column.isNotNull`, `Column.isNull`, `Column.is_not_null`,
`Column.is_null`, `Column.join_sql_part`, `Column.like`, `Column.otherwise`, `Column.over`,
`Column.rlike`, `Column.round`, `Column.spark_display_part`, `Column.spark_wrap_display_part`,
`Column.sql_expr_part`, `Column.sql_expr_without_alias`, `Column.startswith`, `Column.str`,
`Column.substr`, `Column.transform`, `Column.try_cast`, `Column.when`.

**Left on the backlog (8, not documented as if they were Spark API):**

| Name | What it is |
|---|---|
| `Column.dt` | Polars-style datetime accessor namespace (`col.dt.year()`); no PySpark analog |
| `Column.str` | Polars-style string accessor namespace (`col.str.to_uppercase()`); no PySpark analog |
| `Column.for_select` | engine plumbing: aliases the native expression to the Spark projection name at the `DataFrame.select` boundary |
| `Column.join_sql_part` | engine plumbing: join-ON SQL fragment with origin-qualified tokens (H1) |
| `Column.spark_display_part` | engine plumbing: PySpark-style display fragment for aggregate output-name building |
| `Column.spark_wrap_display_part` | engine plumbing: child fragment when embedded inside an outer expression display |
| `Column.sql_expr_part` | engine plumbing: SQL fragment for embedding the column into generated SQL |
| `Column.sql_expr_without_alias` | engine plumbing: SQL fragment with a trailing `AS name` stripped (generator rewrites) |

**Grouping (9 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `naming.py` | `Column.alias`, `Column.transform` | Renaming a projection and reshaping a column through a Column→Column function. |
| `predicates.py` | `Column.between`, `Column.eqNullSafe`, `Column.isNull`, `Column.is_null`, `Column.isNotNull`, `Column.is_not_null` | Range test, `<=>` null-safe equality, and the null checks in both spellings. |
| `strings.py` | `Column.contains`, `Column.startswith`, `Column.endswith`, `Column.like`, `Column.ilike`, `Column.rlike`, `Column.substr` | The seven string predicates and the 1-based slice (int and Column arguments, 0 start ≡ 1). |
| `bitwise_cast.py` | `Column.bitwiseAND`, `Column.bitwiseOR`, `Column.bitwiseXOR`, `Column.cast`, `Column.try_cast` | Integer bit ops and type conversion, including the NULL-on-failure try arm. |
| `when_chains.py` | `Column.when`, `Column.otherwise` | The chained CASE ladder closed by an ELSE arm. |
| `order_markers.py` | `Column.asc`, `Column.asc_nulls_first`, `Column.asc_nulls_last`, `Column.desc`, `Column.desc_nulls_first`, `Column.desc_nulls_last` | The six sort markers with nulls placed explicitly. |
| `window_over.py` | `Column.over` | The window application: `row_number` over an ordered spec and `sum` over a partition frame. |
| `accessors.py` | `Column.getItem`, `Column.getField` | Array element by 0-based position and struct field by name (aliased read). |
| `round_ext.py` | `Column.round` | The repark extension (HALF_UP, delegates to `F.round`); PySpark's `Column` has no `round`. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Nine files under `docs/examples/column/` land runnable local examples for the 32 Spark-equal or repark-extension roster names, every asserted value measured against live PySpark 4.1.2 before it was written; those 32 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 32, 550 → 518, with no other `scripts/` change; the 8 plumbing/namespace names stay on the backlog with the one-line table above; the two measured divergent bare-name arms are §7 rows `EX-COL-1`/`EX-COL-2` with pins in `python/repark/tests/test_examples_column_a.py` while the covered arms are the ones the engines agree on; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (32 findings before, 0 after), the oracle table (40 rows, one per roster name), the nine scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `e3600a1` (dispatch base, before any of the nine example files existed), in a
throwaway worktree under `scratch/`, removed after the measurement. At that base — the 40 roster
rows still in `docs/examples/backlog.txt`, `BACKLOG_BASELINE=550` — the gate's static half exits
**0** (`913 public names; 361 covered; 550 backlog; 2 exceptions; 91 examples` at base counts).
**Provocation:** delete the 32 coverable roster rows from `backlog.txt` and lower
`BACKLOG_BASELINE` to 518 (`550 − 32`) with no new example files present; the same gate exits
**1** with exactly 32 findings, one per roster name from `Column.alias` through `Column.when`
and no others. With the nine files present, the 32 rows removed and `BACKLOG_BASELINE=518`, the
gate exits **0** (`393 covered; 518 backlog; 100 examples`).

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK 17, TZ=UTC)

Measured at `/tmp/oc-ex17/.venv/bin/python` with `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, one
throwaway script per round under `scratch/ex17-oracle/` (gitignored, never committed) driving
`_live_parity.build_spark_engine` and `build_repark_engine` over identical fixtures and printing
per name both engines' values. Fixtures: six-row `g/k/v` frame
`[("a",1,10.0),("a",2,20.0),("a",2,30.0),("a",3,40.0),("b",1,50.0),("b",2,None)]`; string frame
`[("apple",),("mango",),("cherry",),("apple pie",)]`; null frame
`[(None,20),(20,20),(30,None),(None,None)]`; sort frame `[(2.0,),(None,),(1.0,)]`; bitwise frame
`[(5,),(6,),(12,)]`; clean cast frame `[("7",),("42",)]`; dirty try-cast frame
`[("7",),("x",),("42",)]`; array frame `[([1,2,3],),([4,5],)]`; struct frame `r<a string,
b double>` over `("x",2.0)` / `("y",3.0)`. Unordered results compared as sets or sorted lists,
both engines; the six sort arms compared in collect order.
**Round 2:** the bare-projection names isolated — `df.v.cast("double")` answers `v` while
`F.col("v").cast("double")` answers `datafusion.public.__repark_cdf_<plan-id>.v` (divergence is
receiver-specific), and `getField` answers `r['a']` on both receivers where Spark answers `r.a`.
**Round 4 (round 3 discarded — helper defect measured two frames per probe):** every example
statement re-measured in its final form, one frame per probe, sort arms in true order.
**Round 5:** the receiver isolation re-confirmed on a fresh JVM leg (`df["v"]` and `df.v` arms
answer `v` on both engines, the `F.col` arm diverges).
**Round 6b (round 6 discarded — the same helper defect):** the string fixture swapped
`Banana` → `mango` so no expected-value fragment trips the typos gate, and all seven string
predicates plus the three `substr` arms re-measured on the new fixture; values above are the
round-6b readings. `pins: ex-17-column-a/C-001`

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `Column.alias` | columns `['kk', 'g']`; rows `[(1,'a'),(1,'b'),(2,'a'),(2,'a'),(2,'b'),(3,'a')]` | same | kept | `naming.py` | rename read on a frame-bound column |
| `Column.asc` | `[(None,), (1.0,), (2.0,)]` | same | kept | `order_markers.py` | asc default is nulls first, both engines |
| `Column.asc_nulls_first` | `[(None,), (1.0,), (2.0,)]` | same | kept | `order_markers.py` | |
| `Column.asc_nulls_last` | `[(1.0,), (2.0,), (None,)]` | same | kept | `order_markers.py` | |
| `Column.between` | `{('a',2,20.0),('a',2,30.0),('a',3,40.0),('b',2,None)}` | same | kept | `predicates.py` | inclusive both ends |
| `Column.bitwiseAND` | columns `['(m & 3)']`; rows `[(0,),(1,),(2,)]` | same | kept | `bitwise_cast.py` | scalar argument accepted on both |
| `Column.bitwiseOR` | columns `['(m | 1)']`; rows `[(5,),(7,),(13,)]` | same | kept | `bitwise_cast.py` | |
| `Column.bitwiseXOR` | columns `['(m ^ 3)']`; rows `[(5,),(6,),(15,)]` | same | kept | `bitwise_cast.py` | |
| `Column.cast` | df-bound bare answers `['v']` / `['k']` with cast values; `F.col` bare answers `['v']` | df-bound bare same; `F.col` bare answers `['datafusion.public.__repark_cdf_<plan-id>.v']` | kept | `bitwise_cast.py` | example keeps the df-bound and aliased arms; the `F.col`-receiver bare arm is §7 `EX-COL-1` |
| `Column.contains` | `{('mango',)}` | same | kept | `strings.py` | case-sensitive; scalar and Column args both measured |
| `Column.desc` | `[(2.0,), (1.0,), (None,)]` | same | kept | `order_markers.py` | desc default is nulls last, both engines |
| `Column.desc_nulls_first` | `[(None,), (2.0,), (1.0,)]` | same | kept | `order_markers.py` | |
| `Column.desc_nulls_last` | `[(2.0,), (1.0,), (None,)]` | same | kept | `order_markers.py` | |
| `Column.dt` | not measured | `DatetimeNameSpace` accessor | dropped | — | Polars-style namespace, no PySpark analog; backlog row |
| `Column.endswith` | `{('apple',), ('apple pie',)}` | same | kept | `strings.py` | mango arm: no match for the suffix, set compared |
| `Column.eqNullSafe` | columns `['(n <=> m)']`; rows `[(False,),(False,),(True,),(True,)]` | same | kept | `predicates.py` | NULL <=> NULL is True on both |
| `Column.for_select` | not measured | engine plumbing | dropped | — | select-boundary plumbing; backlog row |
| `Column.getField` | bare select answers `['r.a']`; aliased answers `['a']` with values `x`/`y` | bare select answers `["r['a']"]`; aliased same as Spark | kept | `accessors.py` | example keeps the aliased read; the bare arm is §7 `EX-COL-2` |
| `Column.getItem` | columns `['arr[1]']`; rows `{(2,),(5,)}` | same | kept | `accessors.py` | 0-based array element, name matches Spark |
| `Column.ilike` | `{('apple',), ('apple pie',)}` | same | kept | `strings.py` | case-insensitive LIKE || `Column.isNotNull` | `{(20, 20), (30, None)}` | same | kept | `predicates.py` | |
| `Column.is_not_null` | RAISED `TypeError: 'Column' object is not callable` (no snake spelling) | `{(20, 20), (30, None)}` | kept | `predicates.py` | same callable as `isNotNull`; Spark arm is the camel spelling |
| `Column.isNull` | `{(None, 20), (None, None)}` | same | kept | `predicates.py` | |
| `Column.is_null` | RAISED `TypeError: 'Column' object is not callable` (no snake spelling) | `{(None, 20), (None, None)}` | kept | `predicates.py` | same callable as `isNull` |
| `Column.join_sql_part` | not measured | engine plumbing | dropped | — | join-ON rewrite fragment; backlog row |
| `Column.like` | `{('apple',), ('apple pie',)}` | same | kept | `strings.py` | |
| `Column.otherwise` | columns `['k', 'w']`; ladder rows with ELSE `other` | same | kept | `when_chains.py` | closes the ladder built from `F.when` |
| `Column.over` | rank rows `{('a',1,1),('a',2,2),('a',2,3),('a',3,4),('b',1,1),('b',2,2)}`; partition sum `100.0`/`50.0` | same | kept | `window_over.py` | tie order not asserted; set compared |
| `Column.rlike` | `{('apple',), ('apple pie',)}` | same | kept | `strings.py` | |
| `Column.round` | RAISED `TypeError: 'Column' object is not callable` (`hasattr(Column, 'round')` is False) | columns `['round(v, 1)']`; `2.5` → `3.0` (HALF_UP) | kept | `round_ext.py` | repark extension, no Spark analog; delegates to `F.round`, whose `2.5 → 3.0` arm measured Spark-equal |
| `Column.spark_display_part` | not measured | engine plumbing | dropped | — | display-name building; backlog row |
| `Column.spark_wrap_display_part` | not measured | engine plumbing | dropped | — | nested display collapse; backlog row |
| `Column.sql_expr_part` | not measured | engine plumbing | dropped | — | SQL embedding fragment; backlog row |
| `Column.sql_expr_without_alias` | not measured | engine plumbing | dropped | — | alias-stripped SQL fragment; backlog row |
| `Column.startswith` | `{('apple',), ('apple pie',)}` | same | kept | `strings.py` | |
| `Column.str` | not measured | `StringNameSpace` accessor | dropped | — | Polars-style namespace, no PySpark analog; backlog row |
| `Column.substr` | columns `['substr(s, 1, 3)']`; rows `app`/`man`/`che`; 0-start `ap`/`ma`/`ch`; Column-arg `an`/`he`/`pp` | same | kept | `strings.py` | three arms measured: `(1,3)`, `(0,2)`, `(lit(2), lit(2))` |
| `Column.transform` | columns `['upper(s)']`; rows `APPLE`/`APPLE PIE`/`CHERRY`/`MANGO` | same | kept | `naming.py` | Spark 4.1.2 `Column.transform` exists; values and name equal |
| `Column.try_cast` | columns `['s']`; rows `{(7,),(42,),(None,)}` | same | kept | `bitwise_cast.py` | bad input → NULL on both |
| `Column.when` | ladder rows `(1,'one')`×2, `(2,'two')`×3, `(3,'other')` | same | kept | `when_chains.py` | chained arm on a when-column; the ladder start is `F.when` |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_column_a.py -q` | **0** (2 passed) |
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
SHA `e3600a1` (expected for this lane).

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 393 covered; 518 backlog; 2 exceptions; 100 examples`

Before this unit: `361 covered; 550 backlog; 91 examples` (at `e3600a1`). After: `393 covered;
518 backlog; 100 examples` — exactly the 32 kept names.

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract and the EX-15 precedent, wrote
the throwaway oracle rounds (both engines in one process, one JVM start per leg), wrote the nine
example files, the two divergence pins, the registry rows, the backlog ratchet and the maps, then
committed in slices. Base `e3600a1`.

**Rounds:** 1 full sweep over the 40-name roster; 2 bare-projection-name isolation; 3 discarded
(helper defect: two frames per probe tripped Spark's attribute check); 4 every example statement
re-measured in its final form; 5 the receiver-isolation re-confirmed on a fresh JVM leg
(`df["v"]` and `df.v` arms answer `v` on both engines, the `F.col` arm diverges); 6 discarded
(same helper defect); 6b the string fixture swapped `Banana` → `mango` (typos-gate fragments)
with the seven string predicates and three `substr` arms re-measured. Six Spark JVM legs total;
no Spark JVM leg was shared with a sibling unit (bind retries were not needed).

## Disk

Pickup: `df -h` 541 GB free of 1.8 TB after cleanup. The red-first worktree under `scratch/` was
removed after the measurement; the oracle scratch scripts live under the gitignored
`scratch/ex17-oracle/` and are left there (never committed). `.venv` and the sibling-checkout
native module reused; no cargo build, `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-17 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-17-column-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 32 coverable roster names are covered by nine new example files and the oracle table records both engines' values per name, all 40 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/column/naming.py, docs/examples/column/predicates.py, docs/examples/column/strings.py, docs/examples/column/bitwise_cast.py, docs/examples/column/when_chains.py, docs/examples/column/order_markers.py, docs/examples/column/window_over.py, docs/examples/column/accessors.py, docs/examples/column/round_ext.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red; the backlog is an exact baseline 518 with the 8 plumbing/namespace names still listed; every COVERS entry binds on a Column receiver (a repark-rooted local), not on an F.* or Window.* twin.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: A missing module docstring, empty or repeated COVERS list, or an unused cover raises a hard finding; the gate reds on shape drift instead of skipping.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the nine local examples and the two pin tests; example children drop AWS_* and PYTHONPATH and run with a 120 s timeout.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half at the base and produced exactly the 32 roster findings.
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
      evidence: The pins citation for C-001 lives in python/repark/tests/map.md beside the EX-15 batch row, and the pin tests cite the registry rows in their one-line docstrings.
      artifacts: [python/repark/tests/map.md, python/repark/tests/test_examples_column_a.py, docs/examples/column/map.md, docs/spark-sql-iceberg-parity.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_column_a.py](../../../python/repark/tests/test_examples_column_a.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-COL-1`, `EX-COL-2`
- Sibling: [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-17-column-a
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-17-column-a
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (no review-gap rows; both divergences filed as §7 with pins)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, Column.* (a) batch — 32 covered, 8 plumbing rows stay, two bare-name arms filed
  verdict: PENDING
  rejection_route: N/A
```

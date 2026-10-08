# Unit ledger — CROSS-JOIN-CONDITION-1 · `df.join(other, condition, "cross")` applies its condition

**Date:** 2026-10-08 · **Branch:** `fix/cross-join-condition-1` · **Base:** `40fc916f` (`main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Card:** [cross-join-condition-1-card-2026-10-08.md](../../roadmap/mid-term/cross-join-condition-1-card-2026-10-08.md).

**Retires:** this ledger moves to `../completed/` when the unit's product PR merges.

**Why now.** The STAMP-2-R5P6-2 hand-back (PR #997) measured that a cross join with a
condition ignores the condition: 12 rows where live Spark 4.1.2 answers 2. The cause is on
main: the H1 door emits `CROSS JOIN` with no `ON` and drops the exact condition for
`engine_how == "cross"`, and the #997 native route reproduces the SQL route, so the
differential pin stays green on the wrong answer.

**Not in this unit:** `Cargo.toml`, `.github/`, `STATUS.md`, any Rust change (routing only),
shared names under `"cross"` answering as inner-USING (ruling 3 fallback, kept refusal).

## PROPOSITION LEDGER — CROSS-JOIN-CONDITION-1 — 2026-10-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A Column condition on a cross join answers the inner join's rows and columns on both routes: the equality cell 2 rows, the inequality cell 9 rows, the false cell 0 rows, the aliased select `[(2, x), (3, y)]`, the join-then-filter cell 1 row. | `test_cross_join_condition_1.py` (each cell on the native route and with `_join_exact_plan` forced to miss, rows and columns literal, plus equality with the same condition under `"inner"`). | PROVEN | Spark's answers are the brief's live Spark 4.1.2 column; no interpreter on this machine holds PySpark, so they were not re-run here. Green on the fix, red on main (12 rows each). |
| C-002 | `crossJoin(other)` and `join(other, None, "cross")` are unmoved: 12 rows, the same plan as main. | The Cartesian pin in `test_cross_join_condition_1.py`; `cj.py` before/after. | PROVEN | 12 rows before and after on both spellings; the `None` path still emits `CROSS JOIN` with no `ON` and the same exact record. Red under M3. |
| C-003 | Two self-join shapes (a frame with itself, two derivations of one frame) answer exactly as the same condition does with `"inner"` on this branch. | The two self-join pins in `test_cross_join_condition_1.py`. | PROVEN | Both answer rows on this branch (the frame with itself: the 3 diagonal rows); the pins compare the full outcome, rows or refusal. |
| C-004 | Shared names under `"cross"` (`"id"`, `["id"]`) and a list of Columns keep main's refusals byte-identical. | The two refusal pins in `test_cross_join_condition_1.py`; `test_columns.py::test_join_rejects_unsupported_how` stays green. | PROVEN | `ValueError unsupported join type "cross" (...)` and `PySparkTypeError ... expects a column name ...` as on main. The names-to-inner-USING route is future work (ruling 3 fallback: it needs its own routing lines past the net-zero ceiling and would flip the pinned refusal). |
| C-005 | Each pin fails for its reason: three mutations, each red, each reverted. | §Mutations. | PROVEN | M1 reds the SQL legs, M2 the native legs, M3 the unmoved cell. |
| C-006 | No test outside the new file changes its result against main. | The neighbour run §Gates. | PROVEN | The same 3 pre-existing `test_fa_5_duplicate_csv.py` failures (missing native `rename_duplicate_tolerant`, unrelated to joins) before and after; +20 new passes, zero flips. |
| C-007 | The card reads closed with the date, every touched directory's `map.md` moves in the same change, and the parity registry carries no cross-join row to update. | Diff of the card, `task/roadmap/mid-term/map.md`, the three `map.md` rows; `check_docs_links.py`. | PROVEN | `grep cross docs/spark-sql-iceberg-parity.md` names only the ANSI `cross_door` suite and branch cross-read legs, neither a cross-join registry row. |

## The fix

One routing line in `DataFrame._join_on_condition_h1`
(`python/repark/src/repark/spark/dataframe/core.py`): when `engine_how` is `"cross"` and the
condition is present, `engine_how` becomes `"inner"`. The SQL text, the exact-condition
record and the native call are then the inner join's, with no second implementation. The
`"cross"` key of the `how_sql` map is deleted with it: a cross now reaches the map only
with a `None` condition, where the map is unused. Net zero lines in `core.py` (3461, the
ceiling unchanged, so the script baseline, the CAP-1 mirror and its dated row are untouched).
No row logic in Python: the only other H1 callers are `crossJoin` (always `None`) and the
`None` early return through it, so `None` still emits `CROSS JOIN` with no `ON`.

Measured on `cj.py` (`a = [(1,a),(2,b),(3,c)]`, `b = [(2,x),(3,y),(4,z),(9,q)]`):

| Cell | Spark 4.1.2 (brief) | Main | Head |
|---|---|---|---|
| `a.join(b, a.id == b.k, "cross")` | 2 rows | 12 rows | 2 rows |
| `a.join(b, a.id < b.k, "cross")` | 9 rows | 12 rows | 9 rows |
| `a.join(b, F.lit(False), "cross")` | 0 rows | 12 rows | 0 rows |
| aliased select `l.id, r.t` | `(2,x), (3,y)` | 12 rows | `(2,x), (3,y)` |
| `...where(a.id > 2)` | `(3,c,3,y)` | 4 rows | `(3,c,3,y)` |
| `join(b, None, "cross")`, `crossJoin(b)` | 12 rows | 12 rows | 12 rows |
| `join(b2, "id", "cross")`, `["id"]` | inner-USING rows | `ValueError` refusal | `ValueError` refusal (C-004) |
| `join(b, [cond, cond], "cross")` | 2 rows | `PySparkTypeError` refusal | `PySparkTypeError` refusal (C-004) |

## Mutations

Each is one edit to `core.py`, run against
`pytest python/repark/tests/test_cross_join_condition_1.py` (20 cells), then reverted with
`git checkout --`:

| Id | Edit | Red |
|---|---|---|
| M1 (brief: SQL route) | the `ON` branch of the SQL text emits `CROSS JOIN` instead | 7: every `[sql]` condition cell, plus `[native]` where the exact plan misses and falls back to the SQL text (inequality, false) |
| M2 (brief: native route) | the exact record becomes `("cross", None)` while the SQL text keeps `ON` | 5: every `[native]` condition cell answers 12 rows; every `[sql]` cell stays green |
| M3 (brief: None-equivalent) | the remap drops the `condition is not None` guard | 2: the unmoved Cartesian cell on both routes (`UnboundLocalError: on_sql`) |
| M2-first (own, stayed green) | the exact record keeps `engine_how` but drops only the condition (`exact_on = None`) | 0 of 20: the native attempt misses and the route switch falls back to the SQL text, which keeps `ON`. Kept as evidence that the switch fails closed; M2 supersedes it. |

## Gates

On the unit's tree, 2026-10-08; exit codes in the hand-back:

- `test_cross_join_condition_1.py`: 20 passed.
- Neighbours `pytest python/repark/tests -k "join or cross or using" -n 8`: 3 failed
  (the pre-existing `rename_duplicate_tolerant` trio, identical on main), 578 passed
  (558 + 20 new), 16 skipped, 8 xfailed.
- `uvx ruff@0.15.22 check .`, `format --check .`, `check_lib_py.sh`,
  `check_python_conventions.sh`, `check_docstring_presence.sh`, `sync_map_md.py --check`,
  `check_map_md.sh --base origin/main`, `check_docs_links.py`, `check_ledger_grammar.py`,
  `ledger_lifecycle.py check`, the CAP-1 mirror test: all exit 0.
- No Rust changed, so no `cargo` gate and no `make develop` (`.venv` imports `repark`).

Out of scope, observed while measuring:

- R-1. Shared names under `"cross"` answer Spark's inner-USING rows on live Spark but keep
  main's `ValueError` refusal here (ruling 3 fallback, C-004). Routing them is a second
  remap past the net-zero `core.py` ceiling plus the pinned refusal it would flip.
- R-2. A list of Columns is refused for every join type on main and here (ruling 4, C-004).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cross-join-condition-1
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-007 walked against behavior — every cj.py cell on head against the brief's Spark column, the refusal texts byte-compared with main, the card and map diffs read.
      artifacts: [task/ledgers/staging/cross-join-condition-1-ledger.md, python/repark/tests/test_cross_join_condition_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Equality, inequality, false, aliased, join-then-filter, None, shared name, name list, Column list, and two self-join shapes run as cells; the None/empty-list/Column/garbage partition of the join door is unchanged outside the remapped branch.
      artifacts: [python/repark/tests/test_cross_join_condition_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Both kept refusals pin class and text; M3 shows routing without the None guard fails loud (UnboundLocalError) rather than answering wrong rows, and the None path is pinned unmoved.
      artifacts: [python/repark/tests/test_cross_join_condition_1.py]
    - id: AT-4
      status: N/A
      justification: Routing-only change; no new state, no ordering or concurrency surface. Scratch-view registration and the try/finally drop are untouched.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no parsed input beyond the existing condition preparer, no path handling.
    - id: AT-6
      status: ATTACKED
      evidence: The unmoved Cartesian cells, the byte-identical refusals, and the neighbour run (same 3 pre-existing failures, zero flips) hold compatibility; the #997 differential pin stays green.
      artifacts: [python/repark/tests/test_cross_join_condition_1.py, task/ledgers/staging/cross-join-condition-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: No new plan pass; a cross with a condition now plans exactly as the same inner join. Not a system-breaking surface.
    - id: AT-8
      status: ATTACKED
      evidence: The facade signature is unchanged; refusal classes and texts are pinned byte-identical; the pre-existing test_join_rejects_unsupported_how pin stays green; the native call receives the inner shape it already serves.
      artifacts: [python/repark/tests/test_cross_join_condition_1.py]
    - id: AT-9
      status: N/A
      justification: No new error text, log, or metric; the only texts on the touched paths are main's, pinned.
    - id: AT-10
      status: ATTACKED
      evidence: Spark literals on both routes (stronger than the #997 differential); M1/M2/M3 each red the legs they target; branch liveness: the remap arm changes output exactly for cross-with-condition (M1/M2), and its None guard is load-bearing (M3).
      artifacts: [python/repark/tests/test_cross_join_condition_1.py, task/ledgers/staging/cross-join-condition-1-ledger.md]
```

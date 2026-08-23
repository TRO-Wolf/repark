# Unit ledger — Z-1 / G3-E8 PR-1: A1-identity + first IN spelling

**Unit:** G3-E8 phase 1, PR-1 · **Date:** 2026-08-13 · **Lane:** overnight Z-1 ·
**Worktree:** `/tmp/grok-z1` · **Branch:** `grok/z1-g3e8-pr1` ·
**Base (frozen):** `9b2dce3c73af402e8705923135d7de014da5501f` (`#72`)

**Charter:** `planning/grok/BRIEF-z1-g3e8-pr1.md` + `G3E8-FIX-DESIGN.md` §3.2/§5/§6 +
BRIEF-y2 addendum 2026-08-13. Design authority is the memo; addendum rulings win.

**SEPMO:** HIGH · octo + C4 · `cycles=4` · `early_stop=true` · `claims_critic=true`.

**A8 fence:** NEW `write/predicate_dml.rs` + tests; minimum `write/merge/mod.rs` factor
(`commit_overwrite` / `commit_row_delta_kind` + `pub(super)` scan helpers); Spark
`normalize.rs` / `router.rs` / `spark_ast.rs`; ANSI `router.rs` / `guards.rs`;
`cross_door.rs` ROW 9 restated; Python corpus + record driver; maps + this ledger.
CLOSED: rest of `write/**`, QueryPlanner, SET gating, detector hoist, dbt, fork.

---

## 0. Citation verification (memo §9 vs this base)

Re-read at `9b2dce3`. Memo line numbers from `#65` still hold in spirit; executing-parse
sites on this tree:

| Claim | This tree |
|---|---|
| Spark executing-parse valve | `spark_ast.rs` `execute_passthrough` still calls `refuse_dml_subquery_predicate_in_statement` |
| Spark router early valve | `router.rs` `execute_delete` / `execute_update` still call `refuse_dml_subquery_predicate` |
| ANSI valve | `repark-sql/src/router.rs` `Statement::Delete \| Update` |
| Detector | any `Query` under `WHERE` — `normalize.rs` / `guards.rs` |
| Needle | `subquery predicates are silently mis-executed` — **unchanged** |
| MERGE identity | `(_file, _pos)` streaming target, `write.merge.mode` |
| Fork empty=all | still the DF DML fail-open this path never calls |
| SET D-4 | still ungated |
| ROW 9 | `cross_door_g3e8_refusals_render_identically` — restated, not deleted |

No memo citation was stale enough to change the PR-1 design.

---

## 1. What landed

1. **Capability (general, internal):** `repark_iceberg::write::predicate_dml::execute_predicate_dml`
   pins the current snapshot, registers the MERGE streaming target (data + `_file` + `_pos`),
   runs `SELECT _file, _pos FROM scratch AS <alias> WHERE <original predicate>`, then:
   - **COW** (`write.delete.mode` ≠ `merge-on-read`): rewrite affected files dropping those
     identities; `commit_overwrite` with `write.delete.isolation-level` (default serializable).
   - **MoR** (`write.delete.mode = merge-on-read`, V2): `commit_row_delta_kind` with
     `RowDeltaKind::Delete` (no UPDATE/MERGE-only validations).
   Never calls `execute_merge`. Never reads `write.merge.mode`.

2. **Product hole:** valve allow-list opens **only** uncorrelated
   `DELETE … WHERE col IN (SELECT col FROM <table> …)`. Fail-closed: any other subquery
   `WHERE` still refuses with the **unchanged** needle. UPDATE IN stays refused.

3. **Attachment (Q6=A):** Spark `execute_passthrough` (executing parse) + router early valve
   (skip refuse so order vs BUG-001 stays). ANSI Delete/Update arm calls
   `execute_predicate_dml` (never `delegate` for the allowed spelling).

---

## 2. Per-spelling proof table (memo §5)

| # | Spelling | This PR | Proof |
|---|---|---|---|
| 1 | uncorrelated `DELETE … col IN (SELECT col …)` | **FIXED** | Rust identity pins; Spark `g3e8_delete_in_subquery_deletes_exactly_the_matching_row` + FROM-less + quoted/temp-view; ANSI `dml_subquery_in_delete_executes_and_deletes_exactly_the_match`; Python `delete_in_subquery` flipped `split` → `content` vs recorded Spark `{1,3}` |
| 2 | `IN (SELECT max(col) …)` | still refused | family refuse pins |
| 3 | `NOT IN` (no NULL) | still refused | family + ROW 9 |
| 4 | `NOT IN` + NULL | still refused | corpus `*_with_null_key` |
| 5–16 | EXISTS / NOT EXISTS / correlated IN / ANY/ALL / nested / mixed AND/OR / scalars / quoted residual / CTE | still refused or loud-today | existing pins; ROW 9 restated over NOT IN / EXISTS |
| 17 | UPDATE twins of IN | still refused | `g3e8_update_subquery_family_all_refuse`; corpus `update_in_subquery` |
| 19 | SET subquery, no WHERE subquery | untouched (D-4) | existing pin |

**Required identity fixtures (Q8=A)** — in `predicate_dml_tests.rs`:

| Pin | What it would miss |
|---|---|
| `identity_delete_duplicate_rows_deletes_every_copy` | all-column MERGE identity → cardinality |
| `identity_delete_null_column_row_is_still_deleted` | all-column 3VL (`NULL = NULL` unknown) |
| `identity_delete_null_key_is_unknown_and_survives` | `NULL IN (…)` is unknown |
| `identity_delete_empty_match_commits_nothing` | empty filter = delete-all |
| `identity_delete_full_match_empties_the_table` | partial rewrite |
| `identity_delete_honors_write_delete_mode_not_merge_mode` | MoR delete + COW merge.mode still writes position deletes; COW delete + MoR merge.mode does not |

---

## 3. Allow-list (fail-closed)

Accepted only when **all** of:

- `Statement::Delete` (not Update)
- no USING / RETURNING / OUTPUT / LIMIT / ORDER BY / multi-table
- `WHERE` is exactly `InSubquery { negated: false }`
- LHS is a column (`Ident` / `CompoundIdentifier`)
- subquery: no WITH / ORDER BY / LIMIT / DISTINCT / GROUP BY / HAVING / joins / derived FROM
- projection is a single column (not `max` / `*`)
- no nested `Query`
- no compound ident that is not the subquery's own table name or alias (correlation)

Unhandled ⇒ valve, never DataFusion DML.

---

## 4. Q10 — upstream DataFusion completeness (opportunistic, non-blocking)

Suggested issue text (not filed this unit):

> `extract_dml_filters` (`physical_planner.rs`) walks only `Filter` / `TableScan.filters`.
> After optimize, `IN` / `NOT IN` / `EXISTS` become `LeftSemi` / `LeftAnti` / `LeftMark`.
> The Join arm continues traversal and extracts nothing; there is no completeness signal.
> An empty filter list is Iceberg's spelling of *no WHERE* (delete/update every row).
> Please return `Result` / `is_complete` so engines can refuse instead of fail-open.

---

## 5. Gates

- `cargo test -p repark-iceberg --lib predicate_dml` — 8/8
- `cargo test -p repark-spark --lib g3e8` — 14/14
- `cargo test -p repark-sql --lib dml_subquery` + `mor_valve` — green
- `cargo test -p repark-sql --test cross_door cross_door_g3e8` — green
- `cargo test -p repark-iceberg --lib write::merge::occ_tests` — 11/11 (MERGE identity-diff)
- `make verify` / `make preflight` — recorded at PR time

---

## 6. Registry handoff (Z-5 owns the registry file — do **not** edit it here)

**Do not delete G3-E8.** Surface is still mostly refused.

### G3-E8 (BACKLOG — update, do not delete)

- **repark** — `DELETE`/`UPDATE` with a subquery `WHERE` are still **refused** (needle
  `subquery predicates are silently mis-executed`) **except** uncorrelated
  `DELETE … WHERE col IN (SELECT col FROM …)`, which now executes on both doors via the
  A1-identity path (`execute_predicate_dml`) and matches Spark. UPDATE IN, NOT IN, EXISTS,
  scalars, nested, mixed AND/OR remain refused. SET-assignment / INSERT / MERGE source
  still unaffected.
- **Apache Spark** — unchanged (runs all of them).
- **Pin** — `test_dml_subquery_parity.py::test_dml_subquery_row[delete_in_subquery]` (now
  **content**); residual splits unchanged; ROW 9 restated over NOT IN / EXISTS / UPDATE IN.
- **Rationale** — DEFECT, partial fix. Delete the row only when the claimed surface is
  actually re-enabled (memo §6).

### G3-E8-NULL

- **Keep.** `NOT IN` + NULL still refused; trap still recorded.

### Classification

| Item | Status |
|---|---|
| IN-DELETE (uncorrelated) | **FIXED** |
| UPDATE IN | still refused |
| NOT IN / EXISTS / scalars / rest of §5 | still refused |
| Registry G3-E8 | **BACKLOG** (footnote IN-DELETE); Z-5 pastes |
| Registry G3-E8-NULL | **keep** |
| dbt delete+insert → honest DELETE | **not yet** (Q12=A: wait for IN + NOT IN incl. NULL + EXISTS) |

---

## 7. JVM lock

Record/probe waits for `/tmp/grok-z2-probe-released`, then FIFO
`/tmp/grok-jvm-record.lock` (`MARKER=z1-g3e8`). Events in `Z1-COMPLETE.md`.
The Spark golden for `delete_in_subquery` was already recorded 2026-08-11; this PR
flips the pin to that recorded table. Re-record under the lock confirms the half.

---

## 8. Octo (C4, cycles=4, early_stop)

Spawn unavailable (parent session). Sequential hat-switch, no worktree isolation.

| Cycle | Half A OPEN ≥floor | Half B | Verify |
|---|---|---|---|
| 1 | C1-L-001 S1: selection-only allow-list skipped the valve while USING/RETURNING still reached DF DML | remediating: skip/intercept only via `try_allowed_delete_in` (full statement) | targeted g3e8 + identity green |
| 2 | CLEAN ≥floor (USING+IN pinned refused; 1-part IN stays valved) | skipped (early_stop) | — |

**OCTO-CONVERGED** (early stop after cycle 2 CLEAN). Claims: IN-DELETE FIXED is proven; G3-E8-NULL kept; registry not edited; `%ae` checked at commit.

## 9. Residuals / next PRs

- NOT IN (+ NULL trap) together
- EXISTS ± correlation
- UPDATE IN after DELETE of the same predicate
- QueryPlanner (PR-N, Q1 runner-up)
- Detector hoist (Q14=A: not this FIX)
- Upstream DF filing (Q10: text above, non-blocking)

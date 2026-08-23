# Unit ledger — V-1 / G3-E8 PR-3: EXISTS ± correlation

**Unit:** G3-E8 phase 1, PR-3 · **Date:** 2026-08-13 · **Lane:** overnight V-1 ·
**Worktree:** `/tmp/grok-v1` · **Branch:** `grok/v1-g3e8-pr3` ·
**Base (frozen):** `8d325d4f47f46154bd954dc515d717434517fca5` (`#85`)

**Charter:** `BRIEF-v1-g3e8-pr3.md` + `BRIEF-overnight-conductor-7.md` A1–A8 +
`G3E8-FIX-DESIGN.md` + BRIEF-y2 addendum (Q1=A1-identity, Q12=A). Repo contracts win.

**SEPMO:** HIGH · octo + C4 · `cycles=4` · `early_stop=true` · `claims_critic=true`.
Spawn unavailable — sequential hat-switch, no Grok isolation worktree.

**A7:** `spark_ast.rs` is comment refresh only (L70–73 spelling set). Attach already
spelling-generic.

---

## 0. Proposition ledger (scope audit)

| ID | Proposition | Verdict | Evidence |
|---|---|---|---|
| C-001 | Enable `DELETE … WHERE [NOT] EXISTS (SELECT …)` through `try_allowed_delete_in` → `execute_predicate_dml` | PROVEN | `predicate_dml.rs` `is_allowed_exists_selection`; USING/RETURNING stay `None` |
| C-002 | Uncorrelated form is all-or-nothing via the executed identity SELECT (never a match-none/all shortcut) | PROVEN | `identity_select_exists_matches_spark_412_row_sets` + `execute_predicate_dml` still formats `WHERE {selection}` |
| C-003 | Correlated form is per-row semi/anti-join; FQN target refs rewrite to the scratch alias | PROVEN | `allow_list_rewrites_target_fqn_to_scratch_alias`; correlated cases in the same SELECT pin |
| C-004 | A4: ship only if that SELECT remaining set matches live Spark 4.1.2 on every fixture | PROVEN | live probe 2026-08-13T16:35 + identity SELECT 16/16 + corpus content rows |
| C-005 | Fixtures: empty, none/all/some, NULL keys both sides, duplicates both sides, MoR vs COW, NOT variant | PROVEN | Python ROWS + Rust identity SELECT + `identity_delete_exists_honors_write_delete_mode_*` |
| C-006 | USING/RETURNING/nested/scalar/mixed AND/OR/CTE/UPDATE stay intercepted, needle unchanged | PROVEN | allow-list refuse + ROW 9 restated + UPDATE family untouched |
| C-007 | `cross_door.rs` ROW 9 restated: EXISTS moves to executed column | PROVEN | `cross_door_g3e8_exists_delete_executes_identically` |
| C-008 | `spark_ast.rs` logic hunks = 0 | PROVEN | comment-only at the L70–73 spelling set |
| C-009 | A3 / Q12=A: IN + NOT IN (incl. NULL trap) + EXISTS + NOT EXISTS execute both doors | PROVEN | content rows + both-door execute pins. **dbt-upgrade gate MET.** |
| C-010 | Out of scope stays out: correlated IN, ANY/ALL, UPDATE, mixed, CTE, nested, QueryPlanner, detector hoist, registry file | PROVEN | splits remain; no `docs/spark-sql-iceberg-parity.md` in the diff |

---

## 0b. Fixtures + live Spark 4.1.2 transcripts

Recorded 2026-08-13 under `/tmp/grok-jvm-record.lock` (`MARKER=v1-exists-record`).
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1`, Iceberg GAV
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, `local[2]`, ANSI on.

Standalone probe (`/tmp/grok-v1-probe-exists.py`, not committed) then official
`_record_dml_subquery_goldens.py` on the then-10-row corpus (0 mismatch).

| Fixture | Spark remaining (id, name) |
|---|---|
| exists correlated some | `(1,a), (3,c)` |
| not exists correlated some | `(2,b)` |
| exists uncorrelated nonempty | `[]` |
| exists uncorrelated empty | `(1,a), (2,b), (3,c)` |
| not exists uncorrelated nonempty | `(1,a), (2,b), (3,c)` |
| not exists uncorrelated empty | `[]` |
| exists correlated none (keys=99) | `(1,a), (2,b), (3,c)` |
| exists correlated all | `[]` |
| exists correlated empty | `(1,a), (2,b), (3,c)` |
| not exists correlated none | `[]` |
| not exists correlated all | `(1,a), (2,b), (3,c)` |
| not exists correlated empty | `[]` |
| exists correlated NULL keys both sides | `(NULL,n), (1,a)` |
| not exists correlated NULL keys | `(2,b)` |
| exists correlated duplicates | `(2,b)` |
| not exists correlated duplicates | `(1,a), (1,a)` |

Existing 10 corpus goldens re-derived: **0 mismatch**.

MoR vs COW is write-mode honesty (same remaining row-set); pinned in Rust
(`identity_delete_exists_honors_write_delete_mode_not_merge_mode`).

---

## 1. What landed

1. **Allow-list:** `try_allowed_delete_in` accepts `[NOT] EXISTS (SELECT … FROM <plain table>
   [WHERE …])` whose compound refs are only the subquery source or the DELETE target.
   Uncorrelated (no outer ref) and correlated (outer ref = target FQN or alias) both ship.
2. **No new write path.** `execute_predicate_dml` still runs
   `SELECT _file, _pos FROM scratch AS alias WHERE <predicate>`. EXISTS 2VL / anti-join is
   whatever DataFusion's SELECT plans.
3. **FQN rewrite:** `ice.sales.tgt.id` inside the predicate becomes `tgt.id` so the identity
   SELECT correlates against the scratch, not a second scan of the user table. Not a
   row-set shortcut.
4. **Door wiring:** already generic. Spark `tests/dml.rs` + `tests/normalize.rs` restated as
   companion pins (no other V-lane owns them).
5. **Valve restated** over correlated IN / nested / scalar / mixed AND/OR / UPDATE.

---

## 2. Per-spelling proof table (memo §5)

| # | Spelling | This PR | Proof |
|---|---|---|---|
| 1 | uncorrelated IN | already FIXED (PR-1) | unchanged |
| 3–4 | NOT IN + NULL trap | already FIXED (PR-2) | unchanged |
| 5 | EXISTS correlated | **FIXED** | identity SELECT + DELETE + both doors + Python content vs `{1,3}` |
| 6 | EXISTS uncorrelated empty | **FIXED** | identity SELECT + DELETE + Python `delete_exists_uncorrelated_empty` |
| 7 | NOT EXISTS correlated | **FIXED** | identity SELECT + DELETE + Python content vs `{2}` |
| 8 | NOT EXISTS uncorrelated nonempty | **FIXED** | identity SELECT + Python `delete_not_exists_uncorrelated` |
| — | EXISTS uncorrelated nonempty (delete-all) | **FIXED** | identity SELECT + Python `delete_exists_uncorrelated` |
| — | NOT EXISTS uncorrelated empty (delete-all) | **FIXED** | identity SELECT + Python `delete_not_exists_uncorrelated_empty` |
| — | none / all / empty / NULL keys / dups | **FIXED** | identity SELECT + Python content rows |
| — | MoR vs COW | **FIXED** (Rust) | `identity_delete_exists_honors_write_delete_mode_*` |
| 9 | correlated IN | still refused | corpus `delete_correlated_in_subquery` split; ROW 9 |
| 10–16 | ANY/ALL / nested / mixed / scalars / CTE | still refused | family + ROW 9 |
| 17 | UPDATE twins | still refused | `g3e8_update_subquery_family_all_refuse` |

---

## 3. A4 identity-SELECT evidence

`identity_select_exists_matches_spark_412_row_sets` runs the **same** SELECT the identity
path executes (`SELECT id, name FROM tgt WHERE [NOT] EXISTS (…)`) and subtracts that
delete-set from the seed. Remaining matches the live Spark table above for all 16
fixtures. `execute_predicate_dml` does not special-case empty/all.

---

## 4. Allow-list (fail-closed)

Accepted when **all** of:

- `Statement::Delete` (not Update)
- no USING / RETURNING / OUTPUT / LIMIT / ORDER BY / multi-table
- three-part Iceberg target
- `WHERE` is exactly `InSubquery` (PR-1/2) **or** `Exists { negated: _ }`
- EXISTS subquery: simple SELECT, one plain FROM, no WITH/ORDER/LIMIT/DISTINCT/GROUP/HAVING/joins/nested Query
- compound refs are the source relation **or** the DELETE target (FQN / alias / last ident)

Unhandled ⇒ valve, never DataFusion DML. Needle unchanged:
`subquery predicates are silently mis-executed`.

---

## 5. Gates

- `make verify` — exit 0 (2026-08-13, in-worktree)
- `make preflight` — recorded at PR time
- live record expanded ROWS — exit 0; **24 rows, 0 mismatch**
- `spark_ast.rs` — comment-only (3 insertions / 2 deletions, all `//`)

---

## 6. Registry handoff (V-5 owns the registry file — do **not** edit it here)

**Do not delete G3-E8.** Surface is still partially refused (UPDATE, correlated IN, scalars,
nested, mixed AND/OR, CTE, ANY/ALL).

### G3-E8 (BACKLOG — update, do not delete)

- **repark** — `DELETE`/`UPDATE` with a subquery `WHERE` are still **refused** (needle
  `subquery predicates are silently mis-executed`) **except** uncorrelated
  `DELETE … WHERE col IN (SELECT col FROM …)`,
  `DELETE … WHERE col NOT IN (SELECT col FROM …)` (including ANY-NULL-in-subquery
  matches nothing), and `DELETE … WHERE [NOT] EXISTS (SELECT …)` both uncorrelated
  (all-or-nothing) and correlated (per-row semi/anti-join, including NULL join keys
  and duplicate rows). Those execute on both doors via the A1-identity path.
  UPDATE IN/NOT IN/EXISTS, correlated IN, scalars, nested, mixed AND/OR remain refused.
  SET-assignment / INSERT / MERGE source still unaffected.
- **Apache Spark** — unchanged (runs all of them).
- **Pin** — `python/repark/tests/test_dml_subquery_parity.py::test_dml_subquery_row[delete_exists_correlated]`
  and `…[delete_not_exists_correlated]` plus the uncorrelated / none / all / empty /
  NULL-key / duplicate EXISTS content rows; residual splits
  `delete_correlated_in_subquery`, `update_in_subquery`,
  `update_not_in_subquery_with_null_key`; ROW 9 restated over correlated IN / UPDATE IN /
  nested / scalar.
- **Rationale** — DEFECT, partial fix. Delete the row only when the claimed surface is
  actually re-enabled (memo §6).

### G3-E8-NULL

- **Keep the row.** DELETE NOT IN + NULL already matches Spark (PR-2). UPDATE NOT IN +
  NULL stays refused.

### Classification

| Item | Status |
|---|---|
| IN-DELETE (uncorrelated) | FIXED (PR-1) |
| NOT IN-DELETE + NULL trap | FIXED (PR-2) |
| EXISTS / NOT EXISTS ± correlation | **FIXED** (this PR) |
| UPDATE IN / UPDATE NOT IN / UPDATE EXISTS | still refused |
| correlated IN / scalars / rest of §5 | still refused |
| Registry G3-E8 | **BACKLOG** (footnote IN+NOT IN+EXISTS); V-5 pastes |
| Registry G3-E8-NULL | **keep**; repark DELETE half already matches |
| dbt delete+insert → honest DELETE | **gate MET** — IN + NOT IN (incl. NULL trap) + EXISTS + NOT EXISTS execute both doors on this tree. Tomorrow's `dbt-repark` upgrade charter may proceed. |

---

## 7. JVM lock

| Event | Time | Detail |
|---|---|---|
| ACQUIRE | 2026-08-13T16:34:22-04:00 | `/tmp/grok-jvm-record.lock` `MARKER=v1-exists-record` (first create) |
| refresh | 16:35:14 / 16:35:27 | marker rewritten with the sync/probe pid |
| stale-rm | none | lock was absent at start; no other-lane lock overwritten |
| probe | 16:35:27–16:35:51 | `/tmp/grok-v1-probe-exists.py` exit 0; 16 fixtures |
| record | 16:35:51–16:36:11 | `_record_dml_subquery_goldens.py` exit 0; 10 rows, 0 mismatch |
| RELEASE-ON-EXIT | 16:36:11 | trap `rm` of own lock only |
| sentinel | 16:36:11 | `/tmp/grok-v1-first-released` written after first successful release |

No lock file of another lane was removed.

---

## 8. Octo (C4, cycles=4, early_stop)

Spawn unavailable. Sequential hat-switch. Recorded after Actor build.

**Context break executed; attacking artifacts, not memory.**

| Cycle | Half A OPEN ≥floor | Half B | Verify |
|---|---|---|---|
| 1 | 0 (C1/C2/C3/C4 CLEAN) | skipped (empty) | `make verify` 0 |
| 2–4 | not run | | early_stop |

**OCTO-CONVERGED** (early_stop after cycle 1 CLEAN ≥floor + verify green).
Scratch: `/tmp/critic-octo-repark-2026-08-13/`.

Also re-record under lock: `MARKER=v1-exists-rerecord` 2026-08-13T16:54:27 — 24 rows, 0 mismatch; RELEASE-ON-EXIT.

---

## 9. Companion files outside the printed ownership list

`crates/repark-spark/src/tests/dml.rs` and `tests/normalize.rs` restated so existing
G3-E8 refuse pins do not go red on the newly executed EXISTS family. No other V-lane
owns those files tonight. Valve needle unchanged.

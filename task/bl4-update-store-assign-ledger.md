# Unit ledger — BL-4 UPDATE SET ANSI store-assignment gate

**Unit:** BL-4 · conductor-14 Track T3 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Tree:** conductor-14 T3 existing checkout · **Branch:** `grok/bl4-update-store-assign` ·
**Base (FROZEN):** `a5d2d98` (TA performance-parity charter)

**Charter:** conductor-14 A5/A6/A8. Registry/STATUS/BL-4 flip is orchestrator
after SQM — this ledger does **not** edit `docs/spark-sql-iceberg-parity.md`
or `STATUS.md`.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Do **not** type-check inside `update_assignment_lookup` (string maps, no Arrow types). | PROVEN — lookup untouched |
| C-002 | Expose `ansi_store_assignable` + `normalize_for_assignment` as `pub(super)` from `insert.rs`. Never duplicate the matrix. | PROVEN — same functions, `pub(super)` |
| C-003 | `validate_insert_store_assignment` stays insert-specific unless a path-label covers both without forking the matrix. | PROVEN — `refuse_unless_ansi_store_assignable(path, …)` shared; INSERT/UPDATE SET labels |
| C-004 | Gate at the UPDATE plan/stream seam (analogue of `insert_stream_checked`): after assignment projections are planned, before batches write. | PROVEN — `update_stream_checked` at COW rewrite stream + MoR `matched_work_mor` |
| C-005 | Do **not** gate match-discovery (no assignment types). | PROVEN — L1242 `affected_files` still `stream_sql` |
| C-006 | Illegal SET pairs fail with `not ANSI-store-assignable`, not an incidental CASE coercion error. Probe assignment expressions themselves before the CASE rewrite must type-check. | PROVEN — `update_assignment_probe_sql` + gate before `ctx.sql(rewrite_sql)` |
| C-007 | T3 fence: insert.rs + UPDATE plan/stream seam only. No new `mod` decls at the top of merge/mod.rs. Commit region closed. `mod.rs` ≤ 2700. | PROVEN — diff names; measured mod.rs 2621 |
| C-008 | Spark door: UPDATE twins of the INSERT trio + widening + atomic→string. Reuse `NEEDLE`. | PROVEN — `test_merge_store_assign.py` |
| C-009 | Native door: at least one refuse + one pass of the same shapes. Native MERGE is live. | PROVEN — `merge_update_boolean_to_int_refuses` + `merge_update_numeric_widening_still_updates` in `repark-sql/src/tests.rs` |
| C-010 | `map.md` lockstep + tests in the same commit as the code. | PROVEN — listed in §2 |

---

## 0. Blast

Shared executor (`repark-iceberg::write::merge`). The INSERT path already
gates planned types through `Cast.canANSIStoreAssign` (Arrow). UPDATE `SET`
went through a rewrite `CASE (clause_id) WHEN i THEN (assignment) ELSE t.col
END`; DataFusion CASE-arm unification refused bool→int at **plan** time, so
a post-plan zip against the write schema never ran and the error class
diverged from INSERT (registry BL-4).

T5 owns commit-error seams and `mod abort;` — not touched.

## 1. Implementation

- `ansi_store_assignable` / `normalize_for_assignment` are `pub(super)`.
- `refuse_unless_ansi_store_assignable(path, column, src, dst)` is the one
  refusal constructor (`INSERT` vs `UPDATE SET` around the same needle).
- `update_assignment_probe_sql` builds
  `SELECT (expr) AS aN, … FROM source JOIN target ON on` for every UPDATE
  assignment (synthetic aliases so two clauses can SET the same column).
- `validate_update_store_assignment` plans that probe (no execute; not a
  PERF-19 logical target pass) and runs the matrix against write-schema types.
- `update_stream_checked` runs the probe **then** plans/streams the CASE
  rewrite. Call sites: COW rewrite stream; MoR `matched_work_mor`.
- After the gate, production THEN arms wrap `arrow_cast((expr), '<Arrow type>')`
  so CASE unifies on legal pairs CASE cannot coerce (bool→string). Test-only
  `rewrite_column` stays uncast so existing SQL-shape pins stay green.
  CAST is **after** the gate: wrapping first would make bool→int look like
  Int32→Int32 and silently write `1`.

Honest cut: the COW rewrite stream only runs when match-discovery found
affected files. An illegal `SET` with **zero** matching rows therefore does
not fire the COW gate (MoR still probes whenever a WHEN MATCHED clause
exists). Spark-door pins insert a matching key so UPDATE fires. Not a silent
wrong write — no SET is applied when nothing matches.

## 2. Files

- `crates/repark-iceberg/src/write/merge/insert.rs`
- `crates/repark-iceberg/src/write/merge/mod.rs` (one-line call-site swaps)
- `python/repark/tests/test_merge_store_assign.py`
- `crates/repark-sql/src/tests.rs`
- map.md: `crates/repark-iceberg/src/write/merge/map.md`,
  `python/repark/tests/map.md`, `crates/repark-sql/src/map.md`,
  `task/map.md`
- this ledger

## 3. Tests (red-then-green)

Red: UPDATE bool→int already failed at CASE (`Failed to coerce then (Boolean)
and else (Int32)`) before this unit; the probe makes that path emit
`not ANSI-store-assignable`. After the probe-only patch, Spark
`test_atomic_to_string_still_updates` was red with the CASE bool/Utf8
coercion error — that forced the post-gate `arrow_cast` wrap. Green:
10/10 `test_merge_store_assign.py`; native
`merge_update_boolean_to_int_refuses` +
`merge_update_numeric_widening_still_updates`.

Spark facade (`NEEDLE = r"not ANSI-store-assignable"`):

- `test_boolean_to_int_update_refuses`
- `test_timestamp_to_bigint_update_refuses`
- `test_string_to_bigint_update_refuses`
- `test_numeric_widening_still_updates`
- `test_atomic_to_string_still_updates`

Native ANSI door (time-boxed minimum: one refuse + one pass; timestamp CTAS
on this door can hit the A11 ns write-path residual, so it is not pinned
here):

- `merge_update_boolean_to_int_refuses`
- `merge_update_numeric_widening_still_updates`

Existing `spark_ansi_store_assign_matrix` table is unchanged. New unit pin
`update_set_refusal_uses_the_shared_needle_and_path_label`.

## 4. Gates

| Gate | Result |
|---|---|
| `make verify` | **exit 0** |
| `make py-test-facade` | **exit 0** — 3219 passed, 71 skipped |
| `make audit` | **exit 0** |
| `make workflows-lint` | **exit 0** |
| `make preflight` roster | verify + facade + audit + workflow lint all 0 |

Hooks: `scripts/check_map_md.sh` on the staged set. Hygiene two-pass before push.

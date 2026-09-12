# Unit ledger — AP-2 step 1 (apply) · `CALL apply_partitioning()`

**Unit:** AP-2 step 1 (apply) · **Date:** 2026-09-11 · **Branch:** `feat/ap-2-apply` · **Base:** `origin/main`
**Model:** grok-4.6
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** AP-1 prints a plan and a `plan_id`. This step applies that plan: `CALL
<catalog>.system.apply_partitioning(table => …, plan_id => … [, dry_run => …])` with dry-run
the default (P-4), one commit per step, and P-5 refusals. The guide section is round 2.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** the `docs/guide/maintenance-policy.md` AP-2 section (round 2), the
`adaptive_partitioning` hook (AP-3), any dependency file, `STATUS.md`,
`briefs/next-sequence.md`.

**D-5 (SQL-only).** AP-1 added no Python session method — `plan_partitioning` is a `CALL`
only. This step stays SQL-only and does not invent `apply_partitioning` on the Python
session. Confirmed by grep over `python/repark/**`: no `plan_partitioning` / `apply_partitioning`
session method.

**D-2 lookup target.** D-1 has no `target_file_size_bytes`. `plan_id` is
`hash(snapshot_id, candidate_label)` and does not include the target, so apply re-plans
through `collect_plan_rows` at the current snapshot with target `1` only to build the
candidate set. Single-field and `unpartitioned` labels (the pins) are independent of
target. Pair candidates follow plan's top-3 at that lookup target; a pair id from a
different target may refuse and the caller re-runs `plan_partitioning`.

**Partial apply.** A step that fails stops the chain and the error names the step number
and the statement. Earlier steps stay committed — this is not a transaction.

## PROPOSITION LEDGER — AP-2 step 1 — 2026-09-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `apply_signature`: `CALL apply_partitioning(table, plan_id [, dry_run])`; `dry_run` defaults true; any other named argument refuses naming the accepted set. | Unknown-key pin names `bogus_key` plus `dry_run`/`plan_id`/`table`; missing `plan_id` names `plan_id`; default dry-run pin (C-004) proves the default. | **PROVEN** | `apply_unknown_argument_names_the_accepted_set`; `apply_missing_plan_id_is_required`; `apply_default_dry_run_commits_nothing` |
| C-002 | `apply_plan_id_rederived`: the procedure re-plans at the current snapshot and matches `plan_id`; no match refuses naming the table, the id, and that the snapshot moved or the id is not from this table. | Unknown id pin; stale-id pin after a real apply. | **PROVEN** | `apply_unknown_plan_id_refuses_naming_table_and_id` (names `sales.miss`, `deadbeefdeadbeef`, `snapshot`, `not from this table`); `apply_stale_plan_id_after_commit_refuses` (same id after `dry_run => false` refuses naming table, id, snapshot) |
| C-003 | `apply_chain_and_frame`: one commit per step in order (each DDL, then rewrite_data_files, rewrite_manifests, expire_snapshots); unpartitioned has no DDL step; frame is `step` Int32 plus `procedure`/`arguments`/`status`/`result`/`plan_id` Utf8; a failure names the step number and statement (earlier steps stay committed). | Dry-run false pin walks the chain; unpartitioned pin has no ALTER; schema asserted on every collected frame. Failure path is implemented (`apply_partitioning failed at step N (statement): … earlier steps stay committed`) — no fixture forces a mid-chain error in this step. | **PROVEN** | `apply_dry_run_false_commits_the_chain` (ALTER first, then the three CALLs in order, snapshot moves, 18 rows); `apply_unpartitioned_candidate_has_no_ddl_step`; `apply_rows` schema assertion |
| C-004 | `apply_p5_and_dry_run`: extra branches refuse; sort order unchanged after a real apply; multi-spec table rewritten to one current spec with row-correct data; `dry_run => false` commits; default dry-run commits nothing (snapshot unchanged). | One pin each. | **PROVEN** | `apply_branch_besides_main_refuses` (names `feat`/`branch`/`main`); `apply_preserves_sort_order`; `apply_multi_spec_rewrites_to_one_current_spec` (one live data-file spec, 18 rows); `apply_dry_run_false_commits_the_chain`; `apply_default_dry_run_commits_nothing` |
| C-005 | `apply_sql_only`: AP-1 is SQL-only, so apply stays SQL-only; no public name beyond `apply_partitioning`. | Grep `python/repark` for a session method; none added. | **PROVEN** | AP-1 has no `plan_partitioning` in `python/repark/**`; this step adds none. Door is `CALL catalog.system.apply_partitioning`. |
| C-006 | `apply_red_first`: every pin failed on the base tree before the procedure existed. | Red output pasted below. | **PROVEN** | 10 failed, 0 passed, NotImplemented text pasted in Red first. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

All 10 pins were written before the procedure was dispatched and run against the base tree.
They fail because there is nothing to run — the honest red:

```text
test result: FAILED. 0 passed; 10 failed; 0 ignored; 0 measured; 926 filtered out; finished in 0.39s
apply_partitioning runs: NotImplemented("CALL system.apply_partitioning is not supported. Supported procedures: expire_snapshots, plan_partitioning, register_table, rewrite_data_files, rewrite_manifests, remove_orphan_files, rewrite_position_delete_files, rollback_to_snapshot, run_maintenance.")
```

Two compile fixes landed in test code only before that red: hold `batch.schema()` in a binding
so the column-name `&str`s live long enough, and rename the local `rows` binding so it does
not shadow `common::rows`. No production file was edited to make the red pass except adding
the procedure the pins name.

## Gates

- `cargo test -p repark-spark apply_partitioning`: green (10 passed; 0 failed).
- `cargo test -p repark-spark --lib`: green (932 passed; 4 ignored).
- `cargo test -p repark-spark`: green (crate including integration tests).
- `make verify`: green (fmt, clippy -D warnings, panic-ban, file-size, dag, maps, ledgers, grammar, rust-check, workspace tests).
- Comment fence (`git diff --cached` grep for added `//`/`#` lines): prints nothing.
- D-5 facade pytest: not run — AP-1 is SQL-only, no Python seat.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause was re-derived from the pins rather than read off the card — the 10 pins failed red on the base tree (0 passed, exact NotImplemented text pasted above) and pass green after.
      artifacts: [crates/repark-spark/src/tests/apply_partitioning.rs, crates/repark-spark/src/call/apply_partitioning.rs, crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Happy-path apply of days(ts) end to end (ALTER then three CALL procedures, snapshot moves, 18 rows round-trip), default dry-run commits nothing, unpartitioned candidate skips DDL, multi-spec table unifies live files onto one spec, sort order survives, stale and unknown plan ids refuse.
      artifacts: [crates/repark-spark/src/tests/apply_partitioning.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Unknown named argument, missing plan_id, unknown plan_id, stale plan_id after commit, and extra-branch refusal all pinned loud with the table/id/key/branch named.
      artifacts: [crates/repark-spark/src/tests/apply_partitioning.rs]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state, no concurrency: one CALL re-plans, then either returns the dry-run frame or runs steps sequentially against one table in an isolated memory catalog.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network. All writes are local memory-catalog Iceberg commits through the existing ALTER and CALL bodies.
    - id: AT-6
      status: ATTACKED
      evidence: P-4 is the integrity claim — apply requires a plan_id re-derived at the current snapshot, so a moved snapshot or a foreign id refuses rather than rewriting a table the caller did not just plan. Pinned by the unknown-id and stale-id tests. Mid-chain failure is not a transaction; the error text says earlier steps stay committed.
      artifacts: [crates/repark-spark/src/call/apply_partitioning.rs, crates/repark-spark/src/tests/apply_partitioning.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The work scales with the plan's candidate count plus one commit per step; the ten-test battery (6-file fixtures) finished in 0.70s after compile.
      artifacts: [crates/repark-spark/src/tests/apply_partitioning.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Apply does not duplicate rewrite/expire/manifest logic — it calls the existing execute_* entries with a table-only CallArgs, the same shape run_maintenance_apply uses. DDL is catalog-qualified from the plan row's ADD PARTITION FIELD text rather than parsed as SQL by a second parser.
      artifacts: [crates/repark-spark/src/call/apply_partitioning.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Unknown keys name the accepted set; missing plan_id names plan_id; no-match names the table, the id, and both reasons a match is missing; step failure names the step number and the statement.
      artifacts: [crates/repark-spark/src/call/apply_partitioning.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The pins are the reproduce record — 10 of 10 failed red before the procedure was dispatched and pass after, with the red output pasted verbatim above.
      artifacts: [task/ledgers/staging/ap-2-ledger.md, crates/repark-spark/src/tests/apply_partitioning.rs]
  complete: true
```

# Unit ledger — MB-2c · replay and reconcile (halted at DM-6)

**Date:** 2026-10-07 · **Branch:** `feat/mb-2c-replay-reconcile` · **Base:** `origin/main`
`1338385a` (the harness, #983) · **Model:** claude-opus-5-5 (opus-worker build lane) ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Scope.** The [MB-2c order](../../wo/microbatch/mb-2c-replay-reconcile.md) under the
[design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §2 Q8, §3.4 (with the 2026-10-07
ScopeToken amendment), §4 MBE-11 and MBE-14, §5, §6 and §9 DM-6, and the
[North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) §2, §5 and §8.
Files touched so far: `crates/repark-iceberg/src/write/{sink_offsets_tests.rs,
sink_offsets_fence_tests.rs, map.md}` and this ledger. `sink_offsets.rs` and `crash_tests.rs`
are not edited: the unit halted at step 0.

## Halt checks — 2026-10-07

- **Order halt rule 1 is met.** The sketch's Q8 needs branch A green (DM-6) or the
  `F-APPEND-PIN-BASE-1` pin merged before MB-2c builds the fence. DM-6 measured branch A not
  green (C-001). The fork pin is `076d5f98` (`Cargo.toml:167`); `FastAppendAction` and
  `MergeAppendAction` there carry no `validate_from_snapshot` or `validate_no_concurrent_data`,
  so `F-APPEND-PIN-BASE-1` has not merged. Steps 1–5 are not started.
- Order halt rules 2–4 are not reached.

## PROPOSITION LEDGER — MB-2c — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | DM-6, sketch Q8 branch A, on fork pin `076d5f98`. (a) The stamped append transaction as MB-2a builds it (`merge_append` with the stamp summary, then `stamp_transaction`'s property update) re-bases past a base moved by a concurrent append and lands one `append` snapshot. (b) Adding an empty `overwrite_files()` with `validate_from_snapshot(H)` and `validate_no_conflicting_data()` fails the raced commit at validation with `DataInvalid` and lands nothing. (c) The same fence refuses the quiet commit, with no race, with `PreconditionFailed` (an empty snapshot with no properties). (d) With `allow_empty_commit()` added, the quiet commit never lands: each snapshot producer asserts `main` at the transaction's base, the first moves it, and the second fails `CatalogCommitConflicts` through every retry. So no form of branch A holds "exactly one snapshot, operation `append`" on a quiet commit. | Four measurements over the memory catalog, each asserting the error kind, the snapshots added and the live ids. | **PROVEN** | `write/sink_offsets_fence_tests.rs`: `dm6_the_stamped_append_rebases_past_a_moved_base` (Ok, one `append`, ids `[1, 2, 3]`), `dm6_branch_a_fails_a_moved_base_at_validation` (`DataInvalid`, `Found conflicting files that can contain records matching true`, nothing added, ids `[1, 3]`), `dm6_branch_a_refuses_every_commit_without_a_race` (`PreconditionFailed`, nothing added), `dm6_branch_a_with_empty_commits_allowed_never_commits` (`CatalogCommitConflicts`, `Requirement failed: Branch or tag main's snapshot has changed`, nothing added). Source: the empty-commit guard `fork:crates/iceberg/src/transaction/snapshot.rs:1058`–`:1068`; validation before re-apply `fork:…/transaction/mod.rs` `do_commit`. pins: mb-2c/C-001 |
| C-002 | The append door is fenced to the base `H` the epoch check read (sketch Q8, step 3), so harness pins 2 and 3 (i) can turn green. | Branch A green or the `F-APPEND-PIN-BASE-1` pin merged. | **OPEN** | Halted on C-001: neither precondition holds. The closing question is Q1 below. |
| C-003 | The epoch check in `claim` (sketch §3.4, step 1): `AlreadyCommitted` under this run, `Fenced` under another, `GenerationMismatch` for another generation. | Pins 2 and 3 green on the reloaded-sink claim (harness D-1), and the mutation that drops the check turns pin 2 red. | **OPEN** | Not started: the brief halts at step 0. Closing question Q2 below. |
| C-004 | The C-008 walk in `commit_stamp_only`'s unknown branch (`resolve_unknown_outcome`, step 2): found by `engine.operation-id`, else `(query-id, epoch)`; absent gives `RecoveryRequired(CommitOutcomeUnknown)` with the durable record; never a re-submit or a replace. | Pin 4 green, and the mutation that retries a replace turns it red. | **OPEN** | Not started: the brief halts at step 0. Closing question Q2 below. |

## Dated decision rows

- **D-1 (2026-10-07). DM-6 measured with a direct transaction, not through an arm.** The
  measurement builds the arm's transaction shape in the test (`merge_append` with the stamp
  summary, then `ClaimedStamp::stamp_transaction`) and adds branch A's action after it, the
  only place MB-2c could add it (`SiteStamp::transaction` calls `stamp_transaction` after the
  arm applies its append). The race is a plain append committed after the transaction's table
  was loaded; the memory catalog's location check and the fork's re-base then behave as under
  the harness's `FaultCatalog::Race`. The measurements stay as pins: they record why the fence
  waits for the fork, and they turn red when a fork bump changes any of the four answers.
  - The question: how DM-6 is measured so the answer is the branch-A decision itself.
  - Flink: n/a (a measurement, not a behaviour).
  - Spark: n/a.
  - The NS default: measure before ruling, on the exact transaction shape the fence would ride.

## Owner and ruling questions (the halt)

- **Q1 (OWNER, under OQ-4).** DM-6 failed (C-001), so the OQ-4 ruling of 2026-10-06 ("filed
  only if DM-6 fails") now applies: file `F-APPEND-PIN-BASE-1` (sketch §7) and land its RP-N
  pin before MB-2c builds the fence. The measurement adds one requirement to §7's ask: the
  validation must live on the append action itself, because a second snapshot-producing
  action in the same transaction cannot commit (C-001 (d)). Lean: file it as §7 states.
- **Q2 (RULING).** May MB-2c land steps 1 and 2 now (the epoch check and the C-008 walk),
  un-ignoring pin 4 only, with pins 2 and 3 kept `#[ignore]` until the fork pin merges? Pin 4
  does not touch the append fence. Pins 2 and 3 (i) both need it: pin 2's stale re-delivery
  goes through the append arm against the empty pre-commit sink. Lean: yes, as a split, with the
  crash gate read as 3 passed / 2 ignored for the interim and the 5-passed gate kept for the
  closing slice.

## Gates — 2026-10-07

Run on the step-0 tree; the results are in the hand-back.

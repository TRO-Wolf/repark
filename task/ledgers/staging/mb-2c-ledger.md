# Unit ledger — MB-2c · replay and reconcile (split: steps 1–2 landed, the fence waits on F-APPEND-PIN-BASE-1)

**Date:** 2026-10-07 · **Branch:** `feat/mb-2c-replay-reconcile` · **Base:** `origin/main`
`1338385a` (the harness, #983) · **Model:** claude-opus-5-5 (opus-worker build lane) ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Scope.** The [MB-2c order](../../wo/microbatch/mb-2c-replay-reconcile.md) under the
[design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §2 Q8, §3.4 (with the 2026-10-07
ScopeToken amendment), §4 MBE-11 and MBE-14, §5, §6 and §9 DM-6, and the
[North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) §2, §5 and §8.
Round 2 (2026-10-07) runs under the orchestrator's rulings Q1 and Q2 below. Files touched:
`crates/repark-iceberg/src/write/{sink_offsets.rs, sink_offsets_tests.rs,
sink_offsets_epoch_tests.rs, sink_offsets_probe_tests.rs, sink_offsets_fence_tests.rs, map.md}`,
`crates/repark-iceberg/src/microbatch/{crash_tests.rs, map.md}` (the `#[ignore]` lines only, and
the map row the lockstep gate needs, D-5), the fork card
[f-append-pin-base-1-2026-10-07.md](../../roadmap/mid-term/f-append-pin-base-1-2026-10-07.md),
its `mid-term/map.md` row, the [packet](../../wo/microbatch/packet.md) §4 row, and this ledger.

## Halt checks — 2026-10-07

- **Order halt rule 1 is met.** The sketch's Q8 needs branch A green (DM-6) or the
  `F-APPEND-PIN-BASE-1` pin merged before MB-2c builds the fence. DM-6 measured branch A not
  green (C-001). The fork pin is `076d5f98` (`Cargo.toml:167`); `FastAppendAction` and
  `MergeAppendAction` there carry no `validate_from_snapshot` or `validate_no_concurrent_data`,
  so `F-APPEND-PIN-BASE-1` has not merged. Round 1 stopped before step 1.
- **Round 2 (2026-10-07), ruling Q2: the split.** Steps 1 and 2 land, and pin 4 loses its
  `#[ignore]`. Step 3 (the fence) and pins 2 and 3 wait for the fork card (C-002, C-006).
  Halt rule 1 still governs step 3 alone.
- **Order halt rule 2** does not fire. Pins 2 and 3 stay red for a reason outside
  `sink_offsets.rs`, but ruling Q2 keeps them ignored, so no pin is moved (C-005).
- **Order halt rules 3 and 4** are not reached. No row-level dedup is built. The MB-2a cells keep
  their answers: every pin in `write::sink_offsets` stays green, including the
  `SinkCommittedTwice` pins (D-2).

## PROPOSITION LEDGER — MB-2c — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | DM-6, sketch Q8 branch A, on fork pin `076d5f98`. (a) The stamped append transaction as MB-2a builds it (`merge_append` with the stamp summary, then `stamp_transaction`'s property update) re-bases past a base moved by a concurrent append and lands one `append` snapshot. (b) Adding an empty `overwrite_files()` with `validate_from_snapshot(H)` and `validate_no_conflicting_data()` fails the raced commit at validation with `DataInvalid` and lands nothing. (c) The same fence refuses the quiet commit, with no race, with `PreconditionFailed` (an empty snapshot with no properties). (d) With `allow_empty_commit()` added, the quiet commit never lands: each snapshot producer asserts `main` at the transaction's base, the first moves it, and the second fails `CatalogCommitConflicts` through every retry. So no form of branch A holds "exactly one snapshot, operation `append`" on a quiet commit. | Four measurements over the memory catalog, each asserting the error kind, the snapshots added and the live ids. | **PROVEN** | `write/sink_offsets_fence_tests.rs`: `dm6_the_stamped_append_rebases_past_a_moved_base` (Ok, one `append`, ids `[1, 2, 3]`), `dm6_branch_a_fails_a_moved_base_at_validation` (`DataInvalid`, `Found conflicting files that can contain records matching true`, nothing added, ids `[1, 3]`), `dm6_branch_a_refuses_every_commit_without_a_race` (`PreconditionFailed`, nothing added), `dm6_branch_a_with_empty_commits_allowed_never_commits` (`CatalogCommitConflicts`, `Requirement failed: Branch or tag main's snapshot has changed`, nothing added). Source: the empty-commit guard `fork:crates/iceberg/src/transaction/snapshot.rs:1058`–`:1068`; validation before re-apply `fork:…/transaction/mod.rs` `do_commit`. pins: mb-2c/C-001 |
| C-002 | Step 3, the fence: the append door is pinned to the base `H` the epoch check read (sketch Q8), so a stale or raced append fails instead of re-basing. | Branch A green or the `F-APPEND-PIN-BASE-1` pin merged and repinned. | **OPEN** | DM-6 measured branch A not green (C-001), so step 3 waits on the fork card [F-APPEND-PIN-BASE-1](../../roadmap/mid-term/f-append-pin-base-1-2026-10-07.md), filed 2026-10-07 (ruling Q1, D-6). It is the owner's fork work. The closing question: has the card's race pin merged at a pin RePark has taken? |
| C-003 | Step 1, the epoch check in `claim`, on the table the commit starts from (sketch §3.4). (a) A durable record of another generation refuses `GenerationMismatch { resumed, stamped }`. (b) An epoch at or below the durable epoch refuses `AlreadyCommitted` under this run. (c) The same refuses `Fenced { winner }` under another run. The winner is the run whose stamp on `main` carries the claimed epoch, when that run is not the claimant; otherwise it is the sink's current owner, the durable record's run (fold 1, D-9). (d) A higher epoch claims. (e) The check runs after the `SinkCommittedTwice` guard and after the caller's check. (f) It also runs on an unscoped `commit_stamp_only`. (g) A refused check (`AlreadyCommitted`, `Fenced` or `GenerationMismatch`) marks the scope refused: every later claim in that scope returns the same refusal, on any view, and adds no snapshot or rows on the append arm (fold 1, K3, D-8). | Pins on each outcome through `claim`, the append arm and the stamp-only door. Mutations M1 and M4 through M7 turn them red. | **PROVEN** | `write/sink_offsets_epoch_tests.rs`: `a_claim_of_an_epoch_this_run_committed_is_already_committed_and_adds_nothing` (b, g, and the older-epoch case of (b)), `a_claim_of_an_epoch_another_run_committed_is_fenced_by_the_winner` (c, d, and the stamp-only door), `a_claim_under_another_generation_is_a_generation_mismatch` (a, the first-run claim), `fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner` (c: the verifier's shape, run 0x11 commits epoch 1 and 0x22 epoch 2; 0x33's claim of 1 names 0x11, 0x11's replay of 1 names 0x22, a claim of 0 with no stamp at that epoch names 0x22), `an_unscoped_stamp_only_commit_runs_the_epoch_check` (f), `a_refused_epoch_check_refuses_every_later_claim_in_the_scope` (g: the fresh view, the stale pre-commit view and the stamp-only door all refuse `AlreadyCommitted`), `a_stale_view_after_a_refused_claim_does_not_commit_the_epoch_twice` (g: the verifier's `[1, 2, 2]` shape now refuses, one epoch-0 stamp, ids `[1, 2]`). (e): `sink_offsets_tests.rs::a_second_stamped_commit_in_one_batch_refuses` and `sink_offsets_scope_tests.rs::a_second_sink_write_in_one_batch_names_the_loss_and_the_fix` stay green. Code: `sink_offsets.rs` `epoch_check`, called from `BatchScope::claim_checked` and `commit_stamp_only`. pins: mb-2c/C-003 |
| C-004 | Step 2, the C-008 walk, `resolve_unknown_outcome`, on `commit_stamp_only`'s unknown branch. (a) After the fork's own reconcile fails, it reloads the sink once. (b) It searches the lineage above the stamp's base for this attempt. The whole stamped record is the key, and `engine.operation-id` is a fast path to the same snapshot (restated in fold 1: a landed attempt always carries the identical record, so no case is identified by the operation-id alone, and the verifier's MV3, which disables the operation-id match, survives by design). (c) A found snapshot is returned, and the scope is marked committed. (d) Absent, it refuses `RecoveryRequired(CommitOutcomeUnknown)` carrying the durable record of the reload. (e) A same-epoch stamp of another run is not taken as this attempt. (f) A stamp at or below the base is not this attempt. (g) Nothing is re-submitted and no replace is committed. (h) When the reload's resume point itself refuses (a rolled-back attempt: the property names the attempt's epoch, the summary on `main` an older one), the refusal stays `CommitOutcomeUnknown` at the attempt's epoch with its operation-id, and carries that refusal's durable record (fold 1, D-10). | Harness pin 4 green, plus six walk pins. Mutations M2, M3, M8, MV1 and MV2 turn them red. | **PROVEN** | `microbatch/crash_tests.rs::test_microbatch_unknown_outcome_reconciles_1` is green with its `#[ignore]` deleted: landed resolves to the head with one `update_table`, and unlanded refuses with epoch 0 durable (a, c, d, g). `write/sink_offsets_probe_tests.rs`: `an_unlanded_unknown_outcome_walks_to_the_durable_record_without_a_resubmit` (d, g: one request seen, no snapshot added, scope `NotCommitted`), `a_same_epoch_stamp_of_another_run_is_not_the_landed_attempt` (e: durable is the racer's record), `the_walk_finds_a_landed_stamp_above_the_base_by_operation_id_then_by_record` (b, f). Fold 1: `write/sink_offsets_walk_tests.rs` (the `#[path]` child of the probe module): `a_landed_unknown_outcome_marks_the_scope_committed_at_the_resolved_snapshot` (c: the fork's reconcile reload fails once under `commit.status-check.num-retries=0`, the walk resolves the landed head, and `guard.outcome() == Committed { snapshot: <resolved> }`, one request), `a_failed_reload_in_the_walk_refuses_unknown_with_no_durable_record` (D-3: every reload fails, the refusal is `CommitOutcomeUnknown` at epoch 1 with its operation-id and `durable: None`, one request, no snapshot added), `a_rolled_back_attempt_stays_unknown_at_its_epoch_with_the_durable_record` (h: the verifier's rollback with the landed copy on branch `side`; epoch 1, the operation-id, and durable epoch 0). pins: mb-2c/C-004 |
| C-005 | The interim crash gate (ruling Q2): `cargo test -p repark-iceberg --lib microbatch::crash_tests` reads 3 passed, 2 ignored. Pins 2 and 3 keep `#[ignore = "red until F-APPEND-PIN-BASE-1 + MB-2c fence: <scenario>"]`, and their bodies are unchanged. With the epoch check in, each fails at the append re-base: pin 2 at `crash_tests.rs:666` (the stale re-delivery commits) and pin 3 (i) at `:735` (run B's append commits over run A). Pin 3's copy-on-write variant (ii), run alone, is green. | The gate run, the `--ignored` run, and the variant (ii) probe. | **PROVEN** | Gate: `3 passed; 0 failed; 2 ignored`. `-- --ignored`: `the stale re-delivery of epoch 0 must not commit a second time` (`:666`), and `Append: run B's commit of epoch 1 must fail at its base once run A has committed epoch 1` (`:735`). Variant (ii) alone: a temporary edit dropping the `Door::Append` call, reverted before commit, gave `1 passed`. `git diff 97379aff -- crates/repark-iceberg/src/microbatch/crash_tests.rs` touches only the three `#[ignore]` lines. pins: mb-2c/C-005 |
| C-006 | The closing slice. (i) Step 3, the fence (C-002). (ii) Delete the `#[ignore]` lines on pins 2 and 3. (iii) The crash gate reads 5 passed. (iv) The DM-6 measurements (C-001) are re-read on the new fork pin. | `F-APPEND-PIN-BASE-1` merged and repinned (RP-N), then the 5-passed gate. | **OPEN** | Waits on the fork card [F-APPEND-PIN-BASE-1](../../roadmap/mid-term/f-append-pin-base-1-2026-10-07.md), the owner's fork work. The closing question is C-002's. |

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

- **D-2 (2026-10-07). The epoch check runs after `SinkCommittedTwice`, and generation before
  epoch.** Within one batch, a second write's table already shows the batch's own epoch. If the
  epoch check ran first, that write would read `AlreadyCommitted` and lose MB-2a's named
  refusal (order halt rule 4), so the claimed-twice guard goes first. A record of another
  generation is never advanced (CC-9), whatever its epoch, so the generation check goes first
  inside the epoch check.
  - The question: the order of the three refusals and the MB-2a guard.
  - Flink: the Kafka sink's transactional producer is fenced by its producer epoch
    (`ProducerFencedException`), and a fenced producer never commits.
  - Spark: n/a. Spark's `commits/` log has no generation.
  - The NS default: NS-5 (fencing by generation) and CC-9 first, and the ruled MB-2a cells
    unchanged.
- **D-3 (2026-10-07). The walk matches the whole stamped record, not only `(query-id, epoch)`.**
  The sketch's fallback key is `(query-id, epoch)`. A same-epoch stamp from another run would
  match that key, but it is a racer's commit, not this attempt, and taking it would report a
  commit that never landed (C-004 (e)). Matching the whole record is strictly narrower. When
  the reload itself fails, the refusal carries `durable: None`, because neither record can be
  asserted.
  - The question: what the fallback match keys on.
  - Flink: two-phase-commit sinks re-commit only the transactions recorded in their own
    checkpoint state, never another writer's.
  - Spark: n/a. Spark's batch commit has no unknown-outcome walk.
  - The NS default: NS-6 (fail loud). The match is never wider than this attempt.
- **D-4 (2026-10-07). An unscoped `commit_stamp_only` runs the epoch check.** It writes a stamp
  like any claim does. Skipping the check there would let a stale caller re-stamp an epoch.
- **D-5 (2026-10-07). `microbatch/map.md` is edited.** The brief lists `crash_tests.rs` (the
  ignore lines only) but not its directory's map. `check_map_md.sh --base origin/main` needs
  the map changed with any `.rs` change in that directory, and the crash-tests row stated
  "three red until MB-2c" and "Pin 4 (red)". Only that row changes.
- **D-6 (2026-10-07). Ruling Q1: `F-APPEND-PIN-BASE-1` is filed as a docs card.** It is created
  from the sketch's §7 with C-001 (d)'s constraint, its pins and its mutation, and given rows in
  `mid-term/map.md` and the packet's §4. Implementing it is the owner's fork work, outside this
  lane.
- **D-7 (2026-10-07). Ruling Q2: the split.** This slice lands steps 1 and 2 and pin 4. The
  closing slice (C-006) lands step 3 and pins 2 and 3 once the card merges.
- **D-8 (2026-10-07). Fold 1, K3: a durable refusal latches on the scope.** The verifier showed
  a refused claim followed by a commit from the pre-commit view under the same token landing
  epoch 0 twice (ids `[1, 2, 2]`). Once a scope has seen its epoch durable, the entry keeps the
  refusal and every later claim returns it, before the caller's check. Only the three durable
  refusals latch; a catalog or recovery error on the read does not, because it proves nothing
  about the epoch. This closes the stale-view path inside one scope before the fork fence lands.
- **D-9 (2026-10-07). Fold 1: `Fenced` names the claimed epoch's committer.** The verifier showed
  epoch 1's committer told it had lost epoch 1 to the newest run. The winner is now the run whose
  stamp on `main` carries the claimed epoch. When that run is the claimant itself (a run replaying
  an epoch it committed after another run took the sink over), or no stamp at that epoch is on
  `main` (expired, or a gap), the winner is the current owner, the durable record's run, so a run
  is never named as its own winner. Whether the refusal is `AlreadyCommitted` or `Fenced` is
  unchanged: it still compares the durable record's run.
  - The question: which run `Fenced` names (MBE-11 "names the winner run").
  - Flink: a fenced producer's error names the epoch that fenced it, not the transaction it lost.
  - Spark: n/a. Spark's `commits/` log does not record a run per batch.
  - The NS default: NS-5 (the error names the fencing run) and NS-6 (fail loud with the fix).
- **D-10 (2026-10-07). Fold 1: an unknown outcome stays unknown when the reload's resume point
  refuses.** After a rollback of the landed attempt, `read_resume_point` on the reload refuses
  `OffsetMismatch` at the older summary epoch, and round 2 returned that, dropping the attempt's
  epoch and operation-id. The walk now keeps `CommitOutcomeUnknown` at the attempt's epoch with
  its operation-id and attaches the refusal's durable record. Errors that are not a recovery
  refusal (a catalog error, an unsupported format) still propagate unchanged.
- **D-11 (2026-10-07). Fold 1: an epoch gap is admitted.** The epoch check refuses only an epoch
  at or below the durable one, so durable 0 and a claim of 5 claims. MB-3's driver assigns
  contiguous epochs, as Spark's does. The sink check stays offset-based, because windows are
  planned from offsets and a gap loses no rows. The code is unchanged.

## Mutations — 2026-10-07

Each mutation was a temporary edit to `sink_offsets.rs`, run under
`cargo test -p repark-iceberg --lib -- write::sink_offsets microbatch::crash_tests`, and
restored before commit.

| id | Mutation | Red pins |
|---|---|---|
| M1 | Drop the epoch check (`epoch_check` returns `Ok` at entry). Ruled. | All five `epoch::` pins. |
| M2 | Retry on unknown: re-submit the stamp transaction on the reloaded base instead of walking. Ruled ("retry a replace"). | `test_microbatch_unknown_outcome_reconciles_1`, `an_unlanded_unknown_outcome_walks_to_the_durable_record_without_a_resubmit`, `a_same_epoch_stamp_of_another_run_is_not_the_landed_attempt`. |
| M3 | The walk's fallback matches `(query-id, epoch)` only (D-3). Mine. | `a_same_epoch_stamp_of_another_run_is_not_the_landed_attempt`. |
| M4 | The epoch check refuses only an equal epoch (`<` becomes `!=`). Mine. | `a_claim_of_an_epoch_this_run_committed_is_already_committed_and_adds_nothing`, `a_claim_of_an_epoch_another_run_committed_is_fenced_by_the_winner`. |
| MV1 | The verifier's: the walk no longer calls `mark_committed` on a landed match. Survived the round-2 suite. | `a_landed_unknown_outcome_marks_the_scope_committed_at_the_resolved_snapshot`. |
| MV2 | The verifier's: a failed reload in the walk falls back to the caller's stale table. Survived the round-2 suite. | `a_failed_reload_in_the_walk_refuses_unknown_with_no_durable_record`. |
| M8 | Fold 1: the walk propagates the resume point's own refusal again (`?`), so a rollback reads `OffsetMismatch` at epoch 0. | `a_rolled_back_attempt_stays_unknown_at_its_epoch_with_the_durable_record`. |
| M5 | Fold 1, K3: a durable refusal is not latched on the scope entry (the claim stays open). | `a_refused_epoch_check_refuses_every_later_claim_in_the_scope`, `a_stale_view_after_a_refused_claim_does_not_commit_the_epoch_twice`. |
| M6 | Fold 1: `Fenced` names the durable record's run again (round 2's answer). | `fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner`. |
| M7 | Fold 1: the claimed epoch's committer is named even when it is the claimant. | `fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner`. |

## Owner and ruling questions

- **Q1 (OWNER, under OQ-4): RULED 2026-10-07, file it.** Filed as the docs card (D-6, C-002).
- **Q2 (RULING): RULED 2026-10-07, yes, as a split.** Steps 1 and 2 and pin 4 are landed (C-003,
  C-004, C-005). The rest is in C-006.

## Gates — 2026-10-07

The round-2 gate results are in the hand-back.

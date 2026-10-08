# Unit ledger — MB-2c · replay and reconcile (split: steps 1–2 landed; the closing slice lands the fence as a RePark-side stopgap)

**Date:** 2026-10-07 · **Branch:** `feat/mb-2c-replay-reconcile` · **Base:** `origin/main`
`1338385a` (the harness, #983) · **Model:** claude-opus-5-5 (opus-worker build lane) ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Scope.** The [MB-2c order](../../wo/microbatch/mb-2c-replay-reconcile.md) under the
[design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §2 Q8, §3.4 (with the 2026-10-07
ScopeToken amendment), §4 MBE-11 and MBE-14, §5, §6 and §9 DM-6, and the
[North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) §2, §5 and §8.
Round 2 (2026-10-07) runs under the orchestrator's rulings Q1 and Q2 below. Fold 1 (2026-10-07)
works the verifier's S2 and S3 findings under rulings K1 to K5. Fold 2 (2026-10-07) closes the
re-verify's two new findings under J1 and J2. Files touched:
`crates/repark-iceberg/src/write/{sink_offsets.rs, sink_offsets_tests.rs,
sink_offsets_epoch_tests.rs, sink_offsets_probe_tests.rs, sink_offsets_walk_tests.rs,
sink_offsets_fence_tests.rs, map.md}`,
`crates/repark-iceberg/src/microbatch/{crash_tests.rs, map.md}` (the `#[ignore]` lines only, and
the map row the lockstep gate needs, D-5), the fork card
[f-append-pin-base-1-2026-10-07.md](../../roadmap/mid-term/f-append-pin-base-1-2026-10-07.md),
its `mid-term/map.md` row, the [packet](../../wo/microbatch/packet.md) §4 row, and this ledger.
Fold 1 K2 (2026-10-07, ruling Q3) adds, on the local branch `wip/mb-2c-k2-mbe15`:
`crates/repark-iceberg/src/write/{merge/snapshot_commit.rs, predicate_dml.rs,
sink_offsets_isolation_tests.rs, merge/map.md}`, `crates/repark-iceberg/src/microbatch/error.rs`,
and the MB-2a pin `sink_offsets_probe_tests.rs::three_racing_appends_still_stamp_exactly_once_on_every_arm`
with its [ledger](mb-2a-ledger.md) C-019 (D-14, D-15).

**Closing slice (2026-10-07, branch `feat/mb-2c-append-fence`, cut from `origin/main`).** The
owner ruled at about 20:55 EDT that `F-APPEND-PIN-BASE-1` is not built in the fork now and that
the RePark-side catalog wrapper may stand in (Q4). The slice makes fold 1's emulation product
code: C-008 to C-011, D-17 to D-21, and the mutations MF1 to MF8. Files:
`crates/repark-iceberg/src/write/{sink_offsets/append_fence.rs, sink_offsets/map.md,
sink_offsets_append_fence_tests.rs, sink_offsets.rs, sink_offsets_fence_tests.rs, sink_offsets_probe_tests.rs, write_options.rs,
map.md}`, `crates/repark-iceberg/src/microbatch/{crash_tests.rs, map.md}` (the two `#[ignore]`
lines and the map row), the fork card and this ledger. `write_options.rs` changes by one line
(D-17).

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
| C-002 | Step 3, the fence: the append door is pinned to the base `H` the epoch check read (sketch Q8), so a stale or raced append fails instead of re-basing. | Branch A green, or the `F-APPEND-PIN-BASE-1` pin merged and repinned, or (owner ruling, 2026-10-07 ~20:55 EDT, Q4) the RePark-side catalog wrapper. | **PROVEN** (by the stopgap; the fork form is C-011) | DM-6 measured branch A not green (C-001), and the fork card was filed (ruling Q1, D-6). The owner then allowed the RePark-side wrapper, and the closing slice built it: C-008 is the rule and its pins, C-010 the harness. pins: mb-2c/C-002 |
| C-003 | Step 1, the epoch check in `claim`, on the table the commit starts from (sketch §3.4). (a) A durable record of another generation refuses `GenerationMismatch { resumed, stamped }`. (b) An epoch at or below the durable epoch refuses `AlreadyCommitted` under this run. (c) The same refuses `Fenced { winner }` under another run. The winner is the run whose stamp on `main` carries the claimed epoch, when that run is not the claimant; otherwise it is the sink's current owner, the durable record's run (fold 1, D-9). (d) A higher epoch claims. (e) The check runs after the `SinkCommittedTwice` guard and after the caller's check. (f) It also runs on an unscoped `commit_stamp_only`. (g) A refused check (`AlreadyCommitted`, `Fenced` or `GenerationMismatch`) marks the scope refused: every later claim in that scope returns the same refusal, on any view, and adds no snapshot or rows on the append arm (fold 1, K3, D-8). | Pins on each outcome through `claim`, the append arm and the stamp-only door. Mutations M1, M4 through M7 and RM3 turn them red. | **PROVEN** | `write/sink_offsets_epoch_tests.rs`: `a_claim_of_an_epoch_this_run_committed_is_already_committed_and_adds_nothing` (b, g, and the older-epoch case of (b)), `a_claim_of_an_epoch_another_run_committed_is_fenced_by_the_winner` (c, d, and the stamp-only door), `a_claim_under_another_generation_is_a_generation_mismatch` (a, the first-run claim), `fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner` (c: the verifier's shape, run 0x11 commits epoch 1 and 0x22 epoch 2; 0x33's claim of 1 names 0x11, 0x11's replay of 1 names 0x22, a claim of 0 with no stamp at that epoch names 0x22), `an_unscoped_stamp_only_commit_runs_the_epoch_check` (f), `a_refused_epoch_check_refuses_every_later_claim_in_the_scope` (g: the fresh view, the stale pre-commit view and the stamp-only door all refuse `AlreadyCommitted`), `a_stale_view_after_a_refused_claim_does_not_commit_the_epoch_twice` (g: the verifier's `[1, 2, 2]` shape now refuses, one epoch-0 stamp, ids `[1, 2]`). Fold 2 (J1) pins the latch for the other two durable refusals on the same shape (one shared body, `stale_view_after_a_refused_claim`): `a_stale_view_after_a_fenced_claim_does_not_commit_the_epoch_again` (g: run A's stamp is at epoch 0, run B claims epoch 0 on the fresh view and is `Fenced` naming run A, then B's append from the pre-stamp view under the same token refuses `Fenced`, adds no snapshot, one epoch-0 stamp, ids `[1, 2]`) and `a_stale_view_after_a_generation_mismatch_does_not_commit_under_the_old_generation` (g: run B under generation 2 claims epoch 1 and refuses `GenerationMismatch { resumed: 2, stamped: 1 }`, then the stale-view append refuses the same and adds no snapshot). The re-verify's M3 (RM3) turns both red. (e): `sink_offsets_tests.rs::a_second_stamped_commit_in_one_batch_refuses` and `sink_offsets_scope_tests.rs::a_second_sink_write_in_one_batch_names_the_loss_and_the_fix` stay green. Code: `sink_offsets.rs` `epoch_check`, called from `BatchScope::claim_checked` and `commit_stamp_only`. pins: mb-2c/C-003 |
| C-004 | Step 2, the C-008 walk, `resolve_unknown_outcome`, on `commit_stamp_only`'s unknown branch. (a) After the fork's own reconcile fails, it reloads the sink once. (b) It searches the lineage above the stamp's base for this attempt. The whole stamped record is the key, and `engine.operation-id` is a fast path to the same snapshot (restated in fold 1: a landed attempt always carries the identical record, so no case is identified by the operation-id alone, and the verifier's MV3, which disables the operation-id match, survives by design). (c) A found snapshot is returned, and the scope is marked committed. (d) Absent, it refuses `RecoveryRequired(CommitOutcomeUnknown)` carrying the durable record of the reload. (e) A same-epoch stamp of another run is not taken as this attempt. (f) A stamp at or below the base is not this attempt. (g) Nothing is re-submitted and no replace is committed. (h) When the reload's resume point itself refuses (a rolled-back attempt: the property names the attempt's epoch, the summary on `main` an older one), the refusal stays `CommitOutcomeUnknown` at the attempt's epoch with its operation-id, and carries that refusal's durable record (fold 1, D-10) and that refusal's own reason in `resume_refusal` (fold 2, J2, D-16), so the operator reads that resuming will then refuse, with the mismatch's summary and property epochs. A walk whose resume point reads clean carries `resume_refusal: None`. | Harness pin 4 green, plus six walk pins. Mutations M2, M3, M8, M9, MV1 and MV2 turn them red. | **PROVEN** | `microbatch/crash_tests.rs::test_microbatch_unknown_outcome_reconciles_1` is green with its `#[ignore]` deleted: landed resolves to the head with one `update_table`, and unlanded refuses with epoch 0 durable (a, c, d, g). `write/sink_offsets_probe_tests.rs`: `an_unlanded_unknown_outcome_walks_to_the_durable_record_without_a_resubmit` (d, g: one request seen, no snapshot added, scope `NotCommitted`), `a_same_epoch_stamp_of_another_run_is_not_the_landed_attempt` (e: durable is the racer's record), `the_walk_finds_a_landed_stamp_above_the_base_by_operation_id_then_by_record` (b, f). Fold 1: `write/sink_offsets_walk_tests.rs` (the `#[path]` child of the probe module): `a_landed_unknown_outcome_marks_the_scope_committed_at_the_resolved_snapshot` (c: the fork's reconcile reload fails once under `commit.status-check.num-retries=0`, the walk resolves the landed head, and `guard.outcome() == Committed { snapshot: <resolved> }`, one request), `a_failed_reload_in_the_walk_refuses_unknown_with_no_durable_record` (D-3: every reload fails, the refusal is `CommitOutcomeUnknown` at epoch 1 with its operation-id and `durable: None`, one request, no snapshot added), `a_rolled_back_attempt_stays_unknown_carrying_the_durable_record_and_the_mismatch` (h: the verifier's rollback with the landed copy on branch `side`; epoch 1, the operation-id, durable epoch 0, and, since fold 2, `resume_refusal: OffsetMismatch { summary_epoch: 0, property_epoch: 1 }`). `microbatch/error.rs::recovery_reasons_render` renders it: `commit outcome unknown (operation id: op-2); resuming will then refuse: summary epoch 0 disagrees with property epoch 1`. pins: mb-2c/C-004 |
| C-005 | The interim crash gate (ruling Q2): `cargo test -p repark-iceberg --lib microbatch::crash_tests` reads 3 passed, 2 ignored. Pins 2 and 3 keep `#[ignore = "red until F-APPEND-PIN-BASE-1 + MB-2c fence: <scenario>"]`, and their bodies are unchanged. With the epoch check in, each fails at the append re-base: pin 2 at `crash_tests.rs:666` (the stale re-delivery commits) and pin 3 (i) at `:735` (run B's append commits over run A). Pin 3's copy-on-write variant (ii), run alone, is green. | The gate run, the `--ignored` run, and the variant (ii) probe. | **PROVEN** | Gate: `3 passed; 0 failed; 2 ignored`. `-- --ignored`: `the stale re-delivery of epoch 0 must not commit a second time` (`:666`), and `Append: run B's commit of epoch 1 must fail at its base once run A has committed epoch 1` (`:735`). Variant (ii) alone: a temporary edit dropping the `Door::Append` call, reverted before commit, gave `1 passed`. `git diff 97379aff -- crates/repark-iceberg/src/microbatch/crash_tests.rs` touches only the three `#[ignore]` lines. pins: mb-2c/C-005 |
| C-006 | The closing slice. (i) Step 3, the fence (C-002). (ii) Delete the `#[ignore]` lines on pins 2 and 3. (iii) The crash gate reads 5 passed. (iv) The DM-6 measurements (C-001) are re-read. (v) Every MB-2a pin stays green under the fence. | As first filed: `F-APPEND-PIN-BASE-1` merged and repinned (RP-N), then the 5-passed gate. Under the owner ruling of 2026-10-07 (Q4): the RePark-side wrapper, then the 5-passed gate. | **PROVEN** (by the stopgap; the fork form is C-011) | (i) C-008. (ii), (iii) and (v) C-010. (iv) C-010: re-read at the unchanged pin `076d5f98`, none of the four changes, and they are re-read again at RP-N (C-011). The record before the ruling: Waits on the fork card [F-APPEND-PIN-BASE-1](../../roadmap/mid-term/f-append-pin-base-1-2026-10-07.md), the owner's fork work. The closing question is C-002's. Fold 1 measured the revised card's rule ahead of the fork (D-12). A temporary test-only catalog shim refused a stamped append or stamp-only `update_table` when `main` above the pinned base carried this query's stamp, or when the base was not on `main`. With `--include-ignored`, pins 2 and 3 were green, so the crash gate read `5 passed`. The four MB-2a pins the first draft turned red were green: `a_concurrent_unrelated_append_still_commits_both_halves_exactly_once`, `a_racing_stamp_of_another_query_keeps_both_records`, `a_foreign_writer_inside_the_scope_commits_as_on_main_and_leaves_the_claim` and `three_racing_appends_still_stamp_exactly_once_on_every_arm`. The full lib read `940 passed; 0 failed`. The shim was reverted, so this is evidence for the card, not the fence. pins: mb-2c/C-006 |
| C-007 | MBE-15 (ruling Q3). A stamped claim at either MERGE site (`merge/snapshot_commit.rs`, copy-on-write `commit_overwrite_on_ref` and merge-on-read `commit_row_delta_kind_on_ref`) whose `CommitScope` isolation is not `serializable` refuses `MergeIsolationRefused`. The refusal names the isolation property of the operation: `write.merge.isolation-level` for MERGE, `write.update.isolation-level` for identity UPDATE, `write.delete.isolation-level` for identity DELETE. It comes before the epoch check and before the scope is claimed, so the scope stays open and nothing latches. An unstamped commit, or one carrying the token of another table's scope, commits as before. Under `serializable`, a stamped MERGE claims and commits. | The verifier's shape, `two_drivers_one_sink(Door::CopyOnWrite)` under `write.merge.isolation-level=snapshot`: it refuses before commit and epoch 1 lands once. Stamped UPDATE and DELETE refuse under snapshot isolation. A stamped MERGE under `serializable` commits. Mutation: `claim_isolated` ignores the isolation level, and the pins turn red. | **PROVEN** (on `wip/mb-2c-k2-mbe15`, pending Q5) | `write/sink_offsets_isolation_tests.rs` (the `#[path]` child of the probe module): `a_stamped_merge_under_snapshot_isolation_refuses_before_commit_and_epoch_one_lands_once` (the verifier's race on `ProbeCatalog`'s stamped racer: B's insert-only MERGE refuses naming `write.merge.isolation-level`, the probe saw no `update_table`, the scope is `NotCommitted`; run A then commits; one epoch-1 stamp, ids `[1, 2, 3]`, and the offset property names run A), `a_stamped_update_or_delete_under_snapshot_isolation_refuses_on_both_arms` (copy-on-write and merge-on-read UPDATE and DELETE each refuse naming their own property, no snapshot added, ids unchanged), `a_stamped_merge_under_serializable_isolation_commits_its_stamp`, `an_unstamped_merge_under_snapshot_isolation_commits_as_before` (no token, and a token whose scope is another table: both commit, unstamped). The hazard, measured under mutation MZ1: B's MERGE re-bases past run A and returns `Ok`, two epoch-1 stamps, ids `[1, 2, 3, 3]`. `microbatch/error.rs::merge_isolation_refusal_names_serializable` renders `stamped write into silver.events needs write.update.isolation-level=serializable`. Code: `sink_offsets.rs` `SiteStamp::claim_isolated`; `merge/snapshot_commit.rs` `CommitScope::isolation_property` and `governed_by`. Commit `4db3c6ef`, and the lean in D-15. pins: mb-2c/C-007 |
| C-008 | The append fence, on the RePark side (owner ruling, Q4). A stamped commit on the append arm (`commit_append_with_summary`) and on the stamp-only door (`commit_stamp_only`) commits through `AppendFence`, a catalog wrapper. At `update_table` it reads the refreshed base and refuses when: (a) a snapshot on `main` above `ClaimedStamp::base` carries this query's `repark.cdc.query-id`; (b) the base is no longer an ancestor of `main`; (c) with a `None` base, any snapshot on `main` carries the pair. (d) A newer snapshot without the pair never refuses. (e) The refusal is an `iceberg::Error`, kind `DataInvalid`, not retryable, so the commit makes no further attempt. (f) Its message names the base, the newer snapshot (the head, for (b)) and the key. (g) It maps to the typed outcome of the epoch check re-run on the refreshed table (`AlreadyCommitted`, `Fenced`, `GenerationMismatch`, or a recovery refusal); when that check passes, to `Fenced` naming the concurrent stamp's run if it is another run, and otherwise to `Catalog` carrying the message (D-18). (h) A durable refusal latches on the scope (D-19). (i) The fork re-bases before every `update_table`, so the wrapper sees what an action's `validate` would. | Pins on each of (a) to (h) through both doors; the fork read at the pin for (i). Mutations MF1 to MF8. | **PROVEN** | `write/sink_offsets_append_fence_tests.rs`: `a_stamped_append_fails_at_its_base_when_its_own_query_stamped_above_it` (a, e, g, h: `Fenced` naming the racer's run, one `update_table` seen by the probe, two loads, the scope `NotCommitted`, a later claim on the stale and on the reloaded table both return the same `Fenced`, one stamped snapshot, ids `[1, 70]`, the resume point is the racer's record), `a_stamp_only_commit_fails_at_its_base_when_its_own_query_stamped_above_it` (the same on the stamp-only door), `an_empty_base_treats_every_snapshot_on_main_as_concurrent` (c, f, g: from the empty view, epoch 0 again is `AlreadyCommitted`; epoch 1 is `Catalog("append fence: query … epoch 1 pinned base snapshot none; newer snapshot <id> on main carries repark.cdc.query-id=…")`; no snapshot added), `a_base_that_left_main_fails_the_commit_on_both_doors` (b, f: after `rollback_to`, both doors refuse `Catalog("… pinned base snapshot <base>, which is no longer an ancestor of main (head <head>); nothing can be proven about repark.cdc.query-id=… above it")`, no snapshot added, ids `[1]`). (d): C-010's four MB-2a pins, and `sink_offsets_fence_tests.rs::dm6_the_catalog_fence_rebases_past_an_unrelated_append`. (i): `fork:crates/iceberg/src/transaction/mod.rs` at `076d5f98`, `do_commit` (`:506` on): each attempt runs `catalog.load_table`, swaps a stale base for the refreshed table, validates, re-applies every action, then makes the one `catalog.update_table` call, with `TableCommit::base_table` set to that refreshed table; `commit` reaches `update_table` only through `do_commit`, and the unknown-outcome reconcile only loads. Code: `sink_offsets/append_fence.rs` (`AppendFence`, `breach`, `typed`, `refusal_of`), `sink_offsets.rs` (`SiteStamp::fenced`, `SiteStamp::commit_append`, `latch_refusal`, `commit_stamp_only`). pins: mb-2c/C-008 |
| C-009 | The wrapper touches nothing else. (a) Every other `Catalog` method, the provided ones included, forwards to the inner catalog. (b) An unstamped commit is not wrapped: `SiteStamp::fenced` returns the caller's own `Arc`, so the transaction sees the same catalog object as before the slice, and an unstamped append re-bases past this query's own stamp as on `main`. (c) The two MERGE sites never install it. | A pointer-identity pin and a behaviour pin for (b); a forwarding pin for (a); the call sites for (c). | **PROVEN** | `write/sink_offsets_append_fence_tests.rs`: `an_unstamped_append_commits_through_the_callers_own_catalog` (`Arc::ptr_eq` on the unstamped site's catalog; with this query's stamped racer landing inside the commit, the unstamped append commits, two `update_table` requests, two loads, an unstamped head, ids `[1, 2, 70]`; a claimed site returns another `Arc`), `the_fence_forwards_every_other_catalog_call` (`name` is the memory catalog's `memory`, not the trait default; `properties`, `list_views`, `list_namespaces`, `namespace_exists`, `list_tables`, `table_exists`, and `load_table` counted once on the inner catalog with equal metadata). (c): `AppendFence::install` has two callers, `SiteStamp::fenced` and `commit_stamp_only`, and `SiteStamp::commit_append` has one, `write_options.rs::commit_append_with_summary`. The existing unscoped pins stay green (`unscoped_arms_commit_without_any_stamp`, `a_branch_commit_leaves_the_claim_for_main`). pins: mb-2c/C-009 |
| C-010 | The gates of the closing slice. (a) `cargo test -p repark-iceberg --lib microbatch::crash_tests` reads 5 passed, 0 ignored, and the diff of `crash_tests.rs` is the two deleted `#[ignore]` lines. (b) The four MB-2a tolerance pins the card names stay green. (c) The four DM-6 measurements (C-001) are re-read under the fence and none changes; two more are added under the wrapper. (d) The whole lib is green. | The gate runs. | **PROVEN** | (a) `5 passed; 0 failed; 0 ignored`; `git diff origin/main -- crates/repark-iceberg/src/microbatch/crash_tests.rs` is two deleted lines. (b) Green: `a_concurrent_unrelated_append_still_commits_both_halves_exactly_once`, `a_racing_stamp_of_another_query_keeps_both_records`, `a_foreign_writer_inside_the_scope_commits_as_on_main_and_leaves_the_claim`, `three_racing_appends_still_stamp_exactly_once_on_every_arm`; MF1 turns all four red. (c) D-20: the four build their transaction in the test and commit through the bare catalog, so the wrapper is not on their path, and they still record what the fork does alone at `076d5f98`. Added: `dm6_the_catalog_fence_lands_one_append_on_a_quiet_commit` (Ok, one `append`, ids `[1, 2]`: the case branch A failed) and `dm6_the_catalog_fence_rebases_past_an_unrelated_append` (Ok, one `append`, ids `[1, 2, 3]`). (d) `cargo test -p repark-iceberg --lib`: `954 passed; 0 failed; 0 ignored` (940 before the slice's 8 new pins and the 2 un-ignored, with the tests `origin/main` gained since fold 1). pins: mb-2c/C-010 |
| C-011 | Retirement. The wrapper is a stopgap: when the fork lands `F-APPEND-PIN-BASE-1` and RP-N repins RePark, it is deleted and the two doors set the fork's validation on the append action. | The fork card's race pin merged at a pin RePark has taken, then the deletion below with every C-008 and C-010 pin still green. | **OPEN** | The closing question: has `F-APPEND-PIN-BASE-1` merged in the fork at a pin RePark has taken? Until then the wrapper stands. **Delete:** the file `write/sink_offsets/append_fence.rs` and its directory map; in `sink_offsets.rs` the `mod append_fence;` line, the `use append_fence::AppendFence;` line, `SiteStamp::fenced`, and the `AppendFence::install` line in `commit_stamp_only`; in `sink_offsets_append_fence_tests.rs` the pin `the_fence_forwards_every_other_catalog_call` and the two `fenced` assertions of `an_unstamped_append_commits_through_the_callers_own_catalog`; in `sink_offsets_fence_tests.rs` the `Fence::Catalog` arm (its two measurements become measurements of the fork's validation). **Replace with:** `SiteStamp::commit_append` and `commit_stamp_only` call `validate_from_snapshot(ClaimedStamp::base)` (omitted for a `None` base) and `validate_no_concurrent_snapshot_with_summary("repark.cdc.query-id", <query>)` on the `merge_append`, and map the fork's non-retryable validation error to the typed refusal; `typed` and `latch_refusal` move to that mapping, which needs the refreshed table or the newer snapshot's id from the fork's error. **Keep green, unchanged:** the five harness pins, the four MB-2a pins, and the race, empty-base and base-off-`main` pins of C-008 (their message text follows the fork's error, and their two-loads assertions are re-measured). The same list is in `write/map.md` under "Retirement", and the fork card carries the dated line. |

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
- **D-12 (2026-10-07). Fold 1, K1: the fork card is revised, and its rule is measured ahead of
  the fork.** The verifier showed that the first draft's rule turns four MB-2a pins red, because
  it fails on any newer `Append`, `Overwrite` or `Delete`. It also left the empty-base and
  non-ancestor cases open, and its line 35 overstated the case against a single action. The card
  now asks for `validate_no_concurrent_snapshot_with_summary(key, value)`: only a newer snapshot
  on the ref that carries this query's `repark.cdc.query-id` fails the commit. A missing
  from-snapshot uses the transaction's start, so a `None` start treats every snapshot as
  concurrent; that is pin 2. A from-snapshot off the ref fails non-retryably. A′ (one add-only
  `overwrite_files`) is recorded as measured and rejected. The copy-on-write claim is qualified
  to `serializable` isolation. The rule was emulated as a temporary catalog shim, with the
  results in C-006, and the shim is not committed. Its patch is outside the repo, with the
  hand-back.
- **D-13 (2026-10-07). Fold 1, K2 halts on the brief's condition.** MBE-15 needs the claim to know
  it is on a MERGE arm. `SiteStamp` has no such input, and the shared MERGE, UPDATE and DELETE
  sites cannot be told apart inside `sink_offsets.rs`. The brief's rule is to halt on this item
  only if the arm files would have to change. The smallest edit changes the two `SiteStamp::claim`
  calls in `merge/snapshot_commit.rs` (`:170` and `:381`) to pass the `CommitScope`'s resolved
  `IsolationLevel` (a `SiteStamp::claim_isolated`). Those sites also serve stamped predicate
  UPDATE and DELETE, which take their isolation from the caller's `CommitScope`, so the ruling
  also decides whether those refuse. The edit waits for that ruling, and C-007 tracks the row.
  **Ruled 2026-10-07 (Q3): yes to both,** with the refusal naming the operation's property
  (D-14, C-007).
- **D-14 (2026-10-07). Fold 1, K2: the isolation property travels in `CommitScope`.** The ruling
  allowed two lines in an arm file, at the two `SiteStamp::claim` calls in
  `merge/snapshot_commit.rs`, for an S1-class duplicate epoch, and the two lines are as ruled.
  The ruling also says the refusal names the operation's property, and `IsolationLevel` alone
  cannot do that. Copy-on-write UPDATE and DELETE both reach `commit_overwrite_on_ref` with a
  scope built in `predicate_dml.rs`, and merge-on-read UPDATE shares `RowDeltaKind::Merge` with
  MERGE. So, beyond the two lines: `CommitScope` gains `isolation_property` (`scoped` and
  `unscoped` set `write.merge.isolation-level`) and a `governed_by` setter
  (`snapshot_commit.rs`, about 10 lines); the two scope constructors in `predicate_dml.rs` add
  `.governed_by(WRITE_DELETE_ISOLATION_LEVEL)` and `.governed_by(WRITE_UPDATE_ISOLATION_LEVEL)`
  (one line each); and `MicroBatchError::MergeIsolationRefused` gains `property`, rendering
  `stamped write into <sink> needs <property>=serializable` (`microbatch/error.rs`). The
  narrower alternative, one refusal text naming all three properties, keeps `predicate_dml.rs`
  untouched and is one revert away. Q5 asks for this to be ratified with the pin change below.
- **D-15 (2026-10-07). Fold 1, K2: order halt rule 4 fires on an MB-2a cell.** MB-2a's C-019
  pin `three_racing_appends_still_stamp_exactly_once_on_every_arm` runs every arm stamped under
  `write.merge.isolation-level=snapshot` and `write.delete.isolation-level=snapshot`, so its
  copy-on-write insert, copy-on-write rewrite and merge-on-read arms re-base past three racing
  appends. That is the shape MBE-15 refuses. With the ruled edit those three arms refuse
  `MergeIsolationRefused`, which changes the cell's answer, and the order halts on that (rule
  4). Under `serializable` with the arms' `AlwaysTrue` conflict filter, a racing append always
  conflicts, so the cell cannot keep its answer by flipping the isolation property alone.
  Measured with a temporary pin before deciding anything: under `serializable` and a conflict
  filter that misses the racers' rows (`id < 50`; the racers write 97, 98 and 99), all three
  MERGE arms give four attempts, one stamped snapshot, every racer row and `Committed`. The lean
  is applied as the second commit on `wip/mb-2c-k2-mbe15`. `run_arm`'s copy-on-write and
  merge-on-read arms take that filter. The three other `run_arm` callers are pinned and race
  nothing, so the filter changes none of their answers. The racing pin's two properties become
  `serializable`. The append and stamp-only arms ignore both properties. The unit branch stays
  at `a566deb8` until Q5 is ruled.
  - The question: what MB-2a's three-racer cell asserts once a stamped MERGE needs `serializable`.
  - Flink: the Iceberg Flink sink commits appends only. A MERGE-shaped commit has no Flink
    answer.
  - Spark: Iceberg's Spark MERGE under `serializable` validates conflicting data against the
    MERGE's own conflict filter, so concurrent appends outside that filter commit beside it.
  - The NS default: NS-6 (fail loud). The refusal stands under `snapshot`, and the cell's
    exactly-once answer is kept where Iceberg's own validation admits the race.
- **D-16 (2026-10-07). Fold 2, J2: the rolled-back attempt's refusal carries the resume point's
  refusal.** The re-verify showed the walk keeping the durable record but dropping the
  `OffsetMismatch` it read (summary epoch 0, property epoch 1), so an operator reading "outcome
  unknown for epoch 1, durable epoch 0" was not told that resuming would refuse too.
  `RecoveryReason::CommitOutcomeUnknown` gains `resume_refusal: Option<Box<RecoveryReason>>`,
  set to the resume point's own reason whenever `read_resume_point` on the reload refuses
  `RecoveryRequired`, and `None` when it reads clean or the reload fails. The display appends
  `; resuming will then refuse: <reason>`. The whole reason is nested rather than a bare property
  epoch, because the same arm also carries `StampedSnapshotExpired` and `StampNotInLineage`, and
  an `OffsetMismatch` can name no property epoch at all.
- **D-11 (2026-10-07). Fold 1: an epoch gap is admitted.** The epoch check refuses only an epoch
  at or below the durable one, so durable 0 and a claim of 5 claims. MB-3's driver assigns
  contiguous epochs, as Spark's does. The sink check stays offset-based, because windows are
  planned from offsets and a gap loses no rows. The code is unchanged.

- **D-17 (2026-10-07). Closing slice: `write_options.rs` changes by one line.** The brief's
  file list names the sink-offset files and a new sibling. The append arm's commit call is in
  `write_options.rs::commit_append_with_summary`, and the wrapper cannot be on that arm unless
  that call goes through it. The one line `tx.commit(catalog.as_ref()).await` becomes
  `stamp.commit_append(tx, catalog).await?`; the rest of the function is untouched, and the
  logic is in `sink_offsets.rs`. The wrapper module is a `#[path]` child of `sink_offsets.rs`,
  so `write/mod.rs` is not edited. It is reported in the hand-back as a question.
- **D-18 (2026-10-07). Closing slice: the typed outcome of a fence refusal.** The brief maps the
  refusal to the outcome the epoch check uses. The wrapper re-runs the epoch check on the
  refreshed table, which answers `AlreadyCommitted`, `Fenced` or `GenerationMismatch` whenever
  the concurrent stamp is at or above the claimed epoch (harness pins 2 and 3). Two cases pass
  that check and still break the rule: a concurrent stamp of this query at a lower epoch, and a
  base that left `main`. For the first, when the stamp's run is another run, the outcome is
  `Fenced` naming it. Otherwise, and for the second, it is `Catalog` carrying the fence's
  message, because neither `AlreadyCommitted` nor `Fenced` is true there and a new error
  variant is outside this slice's files.
  - The question: what a fence refusal reads as when the epoch check on the refreshed table passes.
  - Flink: a fenced transactional producer fails with the fencing error whatever it was writing.
  - Spark: n/a. Spark's batch commit has no pinned base.
  - The NS default: NS-6 (fail loud, name the cause), and NS-5 (name the fencing run when there
    is one). No refusal is retried.
- **D-19 (2026-10-07). Closing slice: a durable fence refusal latches on the scope.** When the
  wrapper refuses, the scope entry is still claimed, so a second write in the batch would read
  `SinkCommittedTwice` ("epoch already stamped the sink"), which is false: this batch stamped
  nothing. A durable refusal (`AlreadyCommitted`, `Fenced`, `GenerationMismatch`) clears the
  claim and sets the entry refused, as fold 1's K3 does for a refused epoch check (D-8), so
  every later claim returns the same refusal. A `Catalog` refusal does not latch, as before.
- **D-20 (2026-10-07). Closing slice: the DM-6 measurements are re-read and none changes.** The
  four measurements build the arm's transaction in the test and commit through the bare
  catalog (D-1). The wrapper is installed by the two doors, not by the transaction, so it is
  not on their path. They are kept unchanged as the record of what the fork does alone at
  `076d5f98`, which is why the wrapper exists, and they are the first thing re-read at RP-N.
  Two measurements are added on the same shape with the wrapper as the committing catalog.
- **D-21 (2026-10-07). Closing slice: the wrapper reads `TableCommit::base_table`.** Q4 costed
  the wrapper at one extra catalog load per attempt. The fork passes the refreshed base it
  re-applied on in `TableCommit::base_table` (`do_commit`), and the accessor is public, so the
  wrapper checks that table and adds no load. It is the same table an action's `validate`
  would be given. A commit without one (no path at this pin) falls back to one load through
  the inner catalog. A racer landing after the fork's load fails the write on the commit's
  own `main` requirement, retryably, and the next attempt's re-base shows it to the wrapper.

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
| M8 | Fold 1: the walk propagates the resume point's own refusal again (`?`), so a rollback reads `OffsetMismatch` at epoch 0. | `a_rolled_back_attempt_stays_unknown_carrying_the_durable_record_and_the_mismatch` (renamed in fold 2). |
| M9 | Fold 2, J2: the walk drops the resume point's refusal again (`resume_refusal: None` on the refusing arm). | `a_rolled_back_attempt_stays_unknown_carrying_the_durable_record_and_the_mismatch`. |
| M5 | Fold 1, K3: a durable refusal is not latched on the scope entry (the claim stays open). | `a_refused_epoch_check_refuses_every_later_claim_in_the_scope`, `a_stale_view_after_a_refused_claim_does_not_commit_the_epoch_twice`. |
| RM3 | Fold 2, J1: the re-verify's M3, `durable_refusal` latches only `AlreadyCommitted` (the `Fenced` and `GenerationMismatch` arms dropped). Survived the fold-1 suite (67 passed). | `a_stale_view_after_a_fenced_claim_does_not_commit_the_epoch_again`, `a_stale_view_after_a_generation_mismatch_does_not_commit_under_the_old_generation` (67 passed, 2 failed). |
| M6 | Fold 1: `Fenced` names the durable record's run again (round 2's answer). | `fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner`. |
| M7 | Fold 1: the claimed epoch's committer is named even when it is the claimant. | `fenced_names_the_committer_of_the_claimed_epoch_and_otherwise_the_owner`. |
| MZ1 | Fold 1, K2: `claim_isolated` ignores the isolation level (the closure matches `Serializable`). Ruled. | `a_stamped_merge_under_snapshot_isolation_refuses_before_commit_and_epoch_one_lands_once`, `a_stamped_update_or_delete_under_snapshot_isolation_refuses_on_both_arms`. |
| MZ2 | Fold 1, K2: the refusal always names `write.merge.isolation-level`. Mine. | `a_stamped_update_or_delete_under_snapshot_isolation_refuses_on_both_arms`. |
| MZ3 | Fold 1, K2: the isolation check runs whenever a token is carried, before the scope lookup. Mine. | `an_unstamped_merge_under_snapshot_isolation_commits_as_before`. |

### Mutations — the closing slice, 2026-10-07

Each was a temporary edit to `sink_offsets/append_fence.rs` or `sink_offsets.rs`, run under
`cargo test -p repark-iceberg --lib` (954 pins), and restored before commit.

| id | Mutation | Red pins |
|---|---|---|
| MF1 | Drop the query-id filter: every snapshot above the base refuses. Ruled. | The four MB-2a tolerance pins (`a_concurrent_unrelated_append_still_commits_both_halves_exactly_once`, `a_racing_stamp_of_another_query_keeps_both_records`, `a_foreign_writer_inside_the_scope_commits_as_on_main_and_leaves_the_claim`, `three_racing_appends_still_stamp_exactly_once_on_every_arm`), `dm6_the_catalog_fence_rebases_past_an_unrelated_append`, and `a_base_that_left_main_fails_the_commit_on_both_doors` (it names a newer snapshot, not the lost base). 948 passed, 6 failed. |
| MF2 | Drop the ancestor check: a base that is not reached is not a breach. Ruled. | `a_base_that_left_main_fails_the_commit_on_both_doors` (both doors commit). 953 passed, 1 failed. |
| MF3 | A `None` base means no concurrency. Ruled. | `test_microbatch_duplicate_delivery_skips_1` (harness pin 2), `an_empty_base_treats_every_snapshot_on_main_as_concurrent`. 952 passed, 2 failed. |
| MF4 | The refusal is retryable. Ruled. | `a_stamped_append_fails_at_its_base_when_its_own_query_stamped_above_it`, `a_stamp_only_commit_fails_at_its_base_when_its_own_query_stamped_above_it`: the outcome is still the typed refusal, and the load count catches the re-bases (more than two loads). 952 passed, 2 failed. |
| MF5 | The stamp-only door commits through the caller's catalog (no wrapper there). Mine. | `a_stamp_only_commit_fails_at_its_base_when_its_own_query_stamped_above_it`, `an_empty_base_treats_every_snapshot_on_main_as_concurrent`, `a_base_that_left_main_fails_the_commit_on_both_doors`. 951 passed, 3 failed. |
| MF6 | A durable fence refusal is not latched on the scope (D-19). Mine. | The two race pins (the later claim reads `SinkCommittedTwice`). 952 passed, 2 failed. |
| MF7 | The commit sites drop the typed refusal (`refusal_of` returns nothing), so the fence reads as a bare catalog error. Mine. | The two race pins, `an_empty_base_treats_every_snapshot_on_main_as_concurrent`, `a_base_that_left_main_fails_the_commit_on_both_doors`. 950 passed, 4 failed. |
| MF8 | The walk does not stop at the base, so this query's stamps at or below the base count as concurrent. Mine. | 18 pins, among them harness pins 1, 3, 4 and 5, `commit_stamp_only_commits_one_append_snapshot_with_both_halves` and `resume_reads_the_last_offset_from_one_loaded_table`: every second epoch refuses. 936 passed, 18 failed. |

## Owner and ruling questions

- **Q1 (OWNER, under OQ-4): RULED 2026-10-07, file it.** Filed as the docs card (D-6, C-002).
- **Q2 (RULING): RULED 2026-10-07, yes, as a split.** Steps 1 and 2 and pin 4 are landed (C-003,
  C-004, C-005). The rest is in C-006.
- **Q3 (RULING, fold 1): RULED 2026-10-07, yes to both, as leaned.** Built on
  `wip/mb-2c-k2-mbe15` (C-007, D-14).
- **Q3, as asked:** MBE-15 at the MERGE call sites. The question is whether
  `merge/snapshot_commit.rs` may pass its resolved isolation into the claim, and whether a stamped
  predicate UPDATE or DELETE under `snapshot` isolation refuses too (D-13, C-007). The lean is
  yes to both, with one refusal naming the isolation property of the operation.
- **Q4 (OWNER, fold 1, open): could the fence be RePark's own?** The C-006 emulation is a
  catalog wrapper on RePark's side, and it closed pins 2 and 3 with every MB-2a pin green. The
  wrapper's check reads the catalog at `update_table`. The fork's re-base has already happened
  by then, and the commit's own `main` requirement covers the gap between that read and the
  write. The question is whether such a wrapper may stand in for `F-APPEND-PIN-BASE-1`. The
  lean is no without the owner, because the card stays the ruled path (Q1). The wrapper costs
  one extra catalog load per attempt and has not been reviewed as product code. **Forwarded to the owner, 2026-10-07; not built.**
  **RULED by the owner, 2026-10-07 ~20:55 EDT: yes, as a stopgap.** `F-APPEND-PIN-BASE-1` is not
  built in the fork now. The closing slice builds the wrapper (C-008 to C-010), with no extra
  load (D-21), and the fork card becomes its retirement (C-011).
- **Q5 (RULING, fold 1, K2, open): may MBE-15 change MB-2a's three-racer cell, and may the
  property travel in `CommitScope`?** The ruled refusal turns
  `three_racing_appends_still_stamp_exactly_once_on_every_arm` red on its three MERGE arms
  (order halt rule 4, D-15). Naming the property for each operation needs `predicate_dml.rs`
  and `CommitScope` beyond the two ruled lines (D-14). The lean is yes to both: the racing pin
  runs under `serializable` with a conflict filter below the racers, so its answer holds, and
  the snapshot-isolation shape is pinned as the refusal (C-007). If ratified, the unit branch
  fast-forwards to `wip/mb-2c-k2-mbe15`.

## Gates — 2026-10-07

The round-2 gate results are in the hand-back.

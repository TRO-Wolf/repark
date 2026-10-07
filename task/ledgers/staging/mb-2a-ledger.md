# Unit ledger — MB-2a · summary stamping, the offset property and resume-from-sink

**Date:** 2026-10-07 · **Branch:** `feat/mb-2a-sink-offsets` · **Base:** `origin/main`
`6a48443c` (MB-1, #979) · **Model:** claude-opus-5-5 (opus-worker build lane) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Scope.** The [MB-2a order](../../wo/microbatch/mb-2a-sink-offsets.md) under the
[design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §2 Q6, Q9, Q10, §3.4, §4 MBE-13 and
MBE-14, §5 pin 1, §6 and §9 DM-5, and the
[North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) §2, §4, §5 (with the
2026-10-06 amendment) and §8. Files: `crates/repark-iceberg/src/write/{sink_offsets.rs,
sink_offsets_tests.rs, mod.rs, map.md}`, the three named commit arms
(`write_options.rs` `commit_append_with_summary`, `merge/snapshot_commit.rs`
`commit_overwrite_on_ref` and `commit_row_delta_kind_on_ref`), and
`crates/repark-iceberg/src/microbatch/{crash_tests.rs, mod.rs, map.md}` for harness pin 1.
`merge/mod.rs` is not edited.

## Halt checks — 2026-10-07

- **Q6 present and consistent with MB-0.** Sketch §2 Q6 names the six `repark.cdc.*` summary
  keys, the two mirrored Spark keys on the `toTable` door, the property key
  `repark.cdc.offsets.<query-id>` and the offset encodings. MB0-W7 records
  `spark.sql.streaming.queryId` and `spark.sql.streaming.epochId` on an `append` snapshot of the
  `toTable` door, and MB0-W4 records neither key on the `foreachBatch` door. No cell disagrees
  (halt rule 1 not met).
- **F-COMMIT proof merged.** Fork #366 (`267370b7`) is inside the pin `076d5f98`:
  `crates/iceberg/src/transaction/offset_property_retry_tests.rs` carries the combined-commit,
  retry and same-key race pins for both `row_delta` and `overwrite_files`. The RePark-side
  concurrent-append pin (C-008) re-proves the retry half on the append arm too (halt rule 2 not
  met).

## PROPOSITION LEDGER — MB-2a — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | DM-5: the fork at `076d5f98` accepts an empty `merge_append` carrying only snapshot properties, in one transaction with `update_table_properties`, as one new metadata version holding one `append` snapshot with no added files, the summary key and the property; a second stamp-only commit chains on the first. | The DM-5 pin in `sink_offsets_tests.rs`. | **PROVEN** | 1 pin green (`dm5_empty_merge_append_commits_one_stamp_only_append_snapshot`), on the committed and the reloaded table. An empty batch therefore stamps (sketch §3.4, R-18). pins: mb-2a/C-001 |
| C-002 | `BatchScope` holds at most one stamp per sink (`enter` twice refuses `SinkBusy`, MBE-13); `claim` returns `None` for a table with no scope and the stamp with base `H = current_snapshot_id` inside one; a second claim in one scope refuses `SinkCommittedTwice` (MBE-13); dropping the guard clears the entry. | Scope pins in `sink_offsets_tests.rs`. | **PROVEN** | 1 pin green (`scope_holds_one_stamp_per_sink_and_claims_once`): `SinkBusy` names the sink uuid, the claim carries the stamp and `H`, the second claim refuses `SinkCommittedTwice { epoch: 0 }`, the dropped guard frees the sink. The live arm refuses the second stamped commit too (`a_second_stamped_commit_in_one_batch_refuses`: the error's cause downcasts to `SinkCommittedTwice`, one stamped snapshot, the rows of the first commit only). pins: mb-2a/C-002 |
| C-003 | Every arm the sketch names stamps the six `repark.cdc.*` keys (plus the two Spark keys on the `Table` door only) and the property in the **same** commit — one new metadata version, one new snapshot: the append arm (`commit_append_with_summary`), the MERGE copy-on-write arm (`commit_overwrite_on_ref`, both its insert-only and its delete-and-add overwrite) and the merge-on-read arm (`commit_row_delta_kind_on_ref`); the stamps read back equal to the claimed record through `SinkRecord::from_summary` and `from_property`. | Arm pins in `sink_offsets_tests.rs`. | **PROVEN** | 3 pins green (`append_arm_stamps_summary_and_property_in_one_commit_on_both_doors`, `copy_on_write_arm_stamps_both_overwrite_shapes`, `merge_on_read_arm_stamps_the_row_delta`): each asserts `metadata_log + 1`, the summary record and the property record equal to the claimed stamp on the committed and the reloaded table, the Spark keys present on the `Table` door only, and the live rows. A caller's own summary extra rides beside the stamp. The insert-only copy-on-write commit records operation `append` and the delete-and-add one `overwrite` (fork behaviour, measured; D-4). pins: mb-2a/C-003 |
| C-004 | With no scope entered, the three arms commit exactly as before: no `repark.cdc.*` or Spark streaming key, no `repark.cdc.offsets.` property; the existing MERGE and write-options pins keep their answers. | The unscoped arm pin plus the full `repark-iceberg` lib suite. | **PROVEN** | 1 pin green (`unscoped_arms_commit_without_any_stamp`: the append, the copy-on-write and the merge-on-read arms with no scope leave no `repark.cdc.*` or Spark streaming key and no `repark.cdc.offsets.` property, and resume reads `None`); the branch pin (`a_branch_commit_leaves_the_claim_for_main`) shows a non-main commit inside a scope stays unstamped and the claim waits for main (FL-1). The full lib suite stays green (889 passed at the step-4 head, 873 before the unit): no MERGE or write-options pin changed its answer (halt rule 4 not met). pins: mb-2a/C-004 |
| C-005 | `ClaimedStamp::record_commit` records the committed snapshot id as the scope's outcome (`ScopeOutcome::Committed`), and a committed head that does not carry the stamp yields `RecoveryRequired(UnstampedSinkCommit)` (MBE-14). | Outcome pins in `sink_offsets_tests.rs`. | **PROVEN** | 2 pins green: the arm pins read `ScopeOutcome::Committed { snapshot }` equal to the new head; `record_commit_refuses_a_head_without_the_stamp` gets `RecoveryRequired { reason: UnstampedSinkCommit { snapshot: head }, durable: None }`. pins: mb-2a/C-005 |
| C-006 | `commit_stamp_only` commits one stamp-only `append` snapshot carrying the summary keys and the property in one metadata version, and records the scope outcome. | The stamp-only pin. | **PROVEN** | 1 pin green (`commit_stamp_only_commits_one_append_snapshot_with_both_halves`): inside a scope it adds one `append` snapshot (`metadata_log + 1`) with both halves and no rows, records the outcome, and a second call refuses `SinkCommittedTwice`; outside a scope it stamps the given record directly and resume reads it back. pins: mb-2a/C-006 |
| C-007 | `read_resume_point` returns the last committed record from one loaded `Table` with no catalog call of its own: the newest stamped snapshot of the query in the current ancestry, equal to the property; `None` on a sink with neither. | Resume pins in `sink_offsets_tests.rs`. | **PROVEN** | 1 pin green (`resume_reads_the_last_offset_from_one_loaded_table`): an empty sink reads `None`; after stamped epochs 0, 1, 2, an unstamped append and another query's stamp on top, one `load_table` and `read_resume_point` return epoch 2's record, and the other query reads its own. `read_resume_point` is synchronous and takes no catalog, so it cannot read twice; the race pin (C-008) counts exactly one catalog load for the resume. pins: mb-2a/C-007 |
| C-008 | A concurrent unrelated append landing between the stamped commit's load and its catalog update forces a retry, and the retried commit still carries both halves exactly once (one stamped snapshot, the property once, the racer's file live). | The racing-catalog pin. | **PROVEN** | 1 pin green (`a_concurrent_unrelated_append_still_commits_both_halves_exactly_once`): a test catalog commits a racer's `fast_append` inside the stamped commit's first `update_table`, so the append arm retries (2 update calls). The committed and the reloaded table each hold `base + 2` snapshots, exactly one stamped snapshot (the head), the property once and equal to the summary, the racer's unstamped snapshot as the head's parent on top of the original base, and live rows `[1, 2, 99]`; the scope outcome names the head. Resume afterwards costs one counted `load_table`. pins: mb-2a/C-008 |
| C-009 | The property-versus-summary guard (Q10, R-14): a summary record and a property that disagree refuse `RecoveryRequired(OffsetMismatch)` with the summary as the durable record; a property with no stamped snapshot retained refuses `RecoveryRequired(StampedSnapshotExpired)`; a stamp of another query is passed over; a format above 1 refuses `UnsupportedOffsetFormat`. | Guard pins in `sink_offsets_tests.rs`. | **PROVEN** | 3 pins green: `resume_refuses_when_summary_and_property_disagree` (a property drifted to epoch 1 over a summary at epoch 0 gives `OffsetMismatch { summary_epoch: Some(0), property_epoch: Some(1) }` with the summary as `durable`; the property removed gives `property_epoch: None`), `resume_refuses_a_property_whose_stamped_snapshot_expired` (a real `expire_snapshots` of the stamped snapshot gives `StampedSnapshotExpired` with the property as `durable`), `resume_refuses_a_newer_offset_format` (`UnsupportedOffsetFormat { found: "2", supported: 1 }`). Another query's stamp is passed over in the C-007 pin. pins: mb-2a/C-009 |
| C-010 | Harness pin 1 `test_microbatch_kill_after_commit_resumes_1` (sketch §5) is green: epoch 0 commits through the stamped append arm, every in-memory value drops, a reload and `read_resume_point` resume epoch 1 after a Bronze append; the sink equals Bronze with each id once, epochs 0 and 1 each appear once in the summary history, and the property equals the newest stamped summary. | `cargo test -p repark-iceberg --lib microbatch::crash_tests`. | **PROVEN** | `cargo test -p repark-iceberg --lib microbatch::crash_tests`: 1 passed. Real Parquet on both sides: the window is read through `provider_for_plan` and DataFusion, the sink scanned through the fork's reader. Kill point (c) is the end of `run_epoch`, where the planner, the scope guard and every `Table` drop; kill point (a) plans and reads epoch 1 and drops it before staging. The assertions are the sketch's: sink ids `[1, 2, 3]` equal Bronze's, stamped epochs `["0", "1"]` once each, the property equals the head's summary record, and the last offset names Bronze's head; a third trigger plans no window and commits nothing (R-18). pins: mb-2a/C-010 |
| C-011 | Mutations: dropping the property write turns the resume pin red; two further mutants are red on their owning pins. | Three mutation runs, restored by diff. | **PROVEN** | 3 mutants, each red, each restored from a backup and confirmed by `git diff --quiet`. M1 (the brief's): `stamp_transaction` returns the transaction without the property update — 9 of 17 red, among them the resume pin `resume_reads_the_last_offset_from_one_loaded_table` and harness pin 1. M2: `claim` stops marking the entry claimed — 3 red (`scope_holds_one_stamp_per_sink_and_claims_once`, `a_second_stamped_commit_in_one_batch_refuses`, `commit_stamp_only_commits_one_append_snapshot_with_both_halves`). M3: `read_resume_point` returns the summary record without comparing the property — 1 red (`resume_refuses_when_summary_and_property_disagree`). pins: mb-2a/C-011 |
| C-012 | Every gate in the brief is green and every touched map is current. | The gate list. | **PROVEN** | 12/12 brief gates exit 0 (the Gates section). pins: mb-2a/C-012 |


### Fold 1 — the verifier's FAIL on #981, under the orchestrator's 2026-10-07 rulings V1…V6

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-013 | V1 (S1, North Star §8 case 3): a scope is `(sink TableUuid, ScopeToken)`. `BatchScope::enter` mints an unguessable token (UUID v4) on the guard; a named arm claims only when its `summary_extra` carries `repark.cdc.scope-token` equal to the active token. A commit with no token, a different token or a malformed one never claims and commits as on main: a foreign, unscoped writer inside an active scope leaves no `repark.cdc.*` key and no property change, the batch commit then lands its rows **and** the stamp, and resume returns the batch epoch with the rows present. At most one scope per sink; `SinkBusy` unchanged. | The verifier's foreign-writer shape, ported; the wrong-token pin; the session-config seam pin; the scope pin. | **PROVEN** | 4 pins green: `a_foreign_writer_inside_the_scope_commits_as_on_main_and_leaves_the_claim` (the foreign append carries the summary key set of a plain append, no stamp, the properties unchanged and no stamped snapshot; the batch append then stamps the head, resume reads epoch 5, live ids `[1, 2, 3, 777]`, one stamped snapshot), `a_writer_with_a_wrong_token_does_not_claim` (a forged token and a non-uuid value each commit unstamped and leave `NotCommitted`; the batch's own token then claims; the token never lands in a summary; `Debug` prints `ScopeToken(..)`), `the_session_snapshot_property_carries_the_token_to_the_arm` (`spark.sql.iceberg.snapshot-property.repark.cdc.scope-token` resolved through `resolve_write_for_session` stamps the append), and `scope_holds_one_stamp_per_sink_and_claims_once` (a stranger token claims `None` and does not consume the claim). Mutation MV-T (claim ignores the token: `SiteStamp::claim` mints a token when none is carried and `claim_checked` drops the token filter): 3 red, the foreign-writer pin among them, plus the wrong-token and scope pins. pins: mb-2a/C-013 |
| C-015 | V3 (S2): a caller-supplied `repark.cdc.*` or `spark.sql.streaming.*` extra can never displace the stamp on a stamped commit: the claim refuses `Catalog` naming the key (prefix folded to lowercase, the reserved token key exempt) before it marks the entry claimed, and nothing is committed; `SiteStamp::extras` appends the stamp after the caller's extras as a second layer. An unscoped commit keeps such a key verbatim, as on main. | The live refusal pin (the verifier's `verify_caller_extras_cannot_override_the_stamp`, ported and tightened to the refusal) and the ordering unit pin. | **PROVEN** | 2 pins green: `caller_extras_cannot_displace_the_stamp` (`repark.cdc.epoch=99`, `spark.sql.streaming.epochId=99`, `Repark.CDC.Epoch=99` and `repark.cdc.offsets.other` each refuse with the key in the message, no snapshot lands, `NotCommitted`; the batch's own commit then stamps; an unscoped commit keeps `repark.cdc.note`), `site_stamp_extras_put_the_stamp_last_and_drop_the_token` (folded last-write-wins, the extras give the stamp's record and Spark epoch `7`, keep `run_id`, and drop the token). Mutations: MV6 as the verifier wrote it (caller extras after the stamp): the ordering pin red; the refusal dropped alone: the live pin red; both together: 2 red. pins: mb-2a/C-015 |
| C-017 | V5 (S3): `commit_stamp_only` with a stamp that differs from the active scope's refuses `Catalog` **before** it marks the entry claimed, so the scope's own stamp-only commit still claims. | The mismatched stamp-only pin. | **PROVEN** | 1 pin green (`a_mismatched_stamp_only_commit_leaves_the_claim`): epoch 4 against the epoch-3 scope refuses "does not match", then epoch 3's stamp-only commit claims, `outcome` names its snapshot, one stamped snapshot. Mutation MV-C (mark claimed before the comparison): that pin red (1 of 21). pins: mb-2a/C-017 |

VERDICT: 15 clauses, 15 PROVEN, 0 OPEN, 0 REJECTED.

## Dated decision rows

- **DM-5 (2026-10-07, measured).** Question: does the fork accept an empty `merge_append` as a
  stamp-only snapshot. Answer: yes. `SnapshotProducer` refuses an empty commit only when the
  snapshot properties are empty too (`fork:crates/iceberg/src/transaction/snapshot.rs:1058`–`:1063`),
  and the stamp is never empty. The commit is one metadata version, operation `append`, no
  `added-data-files`, scan plans no task. So the sketch's `commit_stamp_only` is buildable on
  the existing action, with no fork ask.
- **D-1 (2026-10-07).** `ClaimedStamp.base` is `Option<SnapshotId>`, not the sketch's
  `SnapshotId`: `H = current_snapshot_id()` is absent on an empty sink, which is exactly harness
  pin 1's starting state. MB-2c's fence reads `None` as "no base to pin".
- **D-2 (2026-10-07).** `ClaimedStamp::summary_entries` returns `Result`, following MB-1 fold 1,
  which made `SinkRecord::summary_entries` and `property` fallible.
- **D-3 (2026-10-07).** The three arms call one `pub(crate)` adapter, `SiteStamp`
  (`claim` → `extras` → `transaction` → `record`), which maps a `MicroBatchError` to
  `DataFusionError::External`, so a caller downcasts the cause and the arms keep their
  DataFusion error type. Each arm claims after its early empty return, so an empty MERGE consumes
  no claim and the driver's stamp-only commit (R-18) still can. Unscoped, `extras` borrows the
  caller's slice (no copy on the ordinary write path).
- **D-4 (2026-10-07, measured).** The copy-on-write arm with nothing to delete commits operation
  `append`; with deletes, `overwrite`. The sketch's "overwrite" arm is this one arm, both shapes
  stamped. `INSERT OVERWRITE` and replace-partitions commits (`commit_replace_write_with_summary`,
  `commit_overwrite_replace_all_with_summary`, `commit_replace_partitions_with_summary`) are not
  named by the sketch and are not stamped: `complete` mode refuses in 1.7 (Q3), so no streaming
  door reaches them, and a `foreachBatch` body that uses one leaves the head unstamped, which
  MB-2c/MB-3 report as `UnstampedSinkCommit`.
- **D-5 (2026-10-07).** `commit_stamp_only` claims the active scope when one exists (a second
  claim refuses, and a stamp that differs from the scope's refuses `Catalog`), and stamps the
  given record directly when none does. A `CommitStateUnknown` becomes
  `RecoveryRequired(CommitOutcomeUnknown { operation_id })` with `durable: None`; resolving it is
  MB-2c's walk.
- **D-6 (2026-10-07).** Iceberg and DataFusion error texts pass through
  `repark_common::redaction::mask_value_credentials` before they enter `MicroBatchError::Catalog`
  (sketch §0 row 6). The scope registry recovers a poisoned mutex (`PoisonError::into_inner`):
  every critical section is one map read or write and cannot leave the map torn.
- **FL-1 (2026-10-07).** Question: does a stamped scope stamp a commit to a non-main branch of
  the sink? Flink: the Iceberg Flink sink commits to its configured branch and stamps that
  branch's snapshot. Spark: the Iceberg Spark write commits to the write's branch when one is set
  (`toBranch`), stamping there. North Star default: the sketch's resume reads the current
  snapshot's ancestry, which is `main`, so only a `main` commit (`branch` absent or `main`)
  claims; a branch commit inside a scope stays unstamped and leaves the claim for `main`. Acted
  on (pin in C-004). The engines differ from the default; filed as owner question OQ-2a-1, not a
  halt (no ruled row changes and no Spark workload returns different rows: 1.7 exposes no branch
  option on the streaming writer).
- **D-7 (2026-10-07).** `read_resume_point`'s mismatch errors carry `epoch` and `durable` from the
  authority: the summary record when one exists (R-14), else the property's record (the stamped
  snapshot expired, but the property was written in the same commit, so it is durable). The
  ancestry walk is bounded by the snapshot count and stops at the first missing parent.
- **D-8 (2026-10-07).** Harness pins 2–4 are not added here. The brief adds them only if the
  [harness order](../../wo/microbatch/harness.md) says MB-2a carries them, and it does not: it
  assigns all five scenarios to the harness slice (as corrected by sketch §6 to
  `microbatch/crash_tests.rs`). This unit creates the file with pin 1 only, declared
  `#[cfg(test)] mod crash_tests;` in `microbatch/mod.rs`.

- **D-9 (2026-10-07, orchestrator ruling V1, a dated amendment to sketch §3.4).** The sketch's
  process-wide map keyed by sink uuid alone let any writer's commit on the sink claim the batch
  stamp (the verifier's S1). The scope is now `(sink TableUuid, ScopeToken)`. The token rides
  the arms' existing `summary_extra` under the reserved key `repark.cdc.scope-token`, so no arm
  signature changed and `merge/mod.rs` stays unedited: MERGE resolves the same session snapshot
  properties through `resolve_empty_session_write`. `SiteStamp::extras` strips the key on every
  commit of the three arms, claimed or not, so a token never lands in a summary that another
  reader could replay; a commit that carries a token key therefore differs from main by that one
  summary entry, which only a scoped session ever sets. `commit_stamp_only` has no options, so
  it takes `Option<&ScopeToken>`; without a matching token it stamps the given record directly
  (D-5's unscoped path). `record_commit` records the outcome only on a claimed entry.
- **D-10 (2026-10-07, MB-3 seam, ruling V1).** The driver installs the guard's token in the
  batch's session config (sketch §3.5's config extension) as
  `spark.sql.iceberg.snapshot-property.repark.cdc.scope-token=<token>`, and removes it when the
  guard drops. Arms the sketch does not name (D-4's replace and replace-partitions commits)
  would copy that key verbatim into their summary; MB-3 owns keeping the token off those paths
  (strip it in `merged_snapshot_extra`, or scope the config to the body's sink commits).

## Gates — 2026-10-07

All exit 0, in brief order, on the step-6 tree: `cargo test -p repark-iceberg --lib` (890 passed,
0 failed: 873 before the unit plus 16 `sink_offsets` pins and harness pin 1),
`cargo test -p repark-iceberg --lib microbatch::crash_tests` (1 passed), `make rust-clippy`,
`cargo fmt --check`, `make rust-panic-ban`, `python3 scripts/check_rust_file_size.py`,
`./scripts/check_lib_rs.sh`, `python3 scripts/sync_map_md.py --check`,
`bash scripts/check_map_md.sh --base origin/main`, `python3 scripts/check_docs_links.py`,
`python3 scripts/check_ledger_grammar.py`, and the comment-ban probe (`hits=0`).
`sink_offsets.rs` lands at 336 lines and `sink_offsets_tests.rs` at 947, both under the default
ceiling; `write_options.rs` and `merge/snapshot_commit.rs` grow by 5 and 6 lines (net) inside the
named functions only; `merge/mod.rs` is untouched.

## Owner questions (none halts)

- **OQ-2a-1 (FL-1).** Should a stamped scope stamp a commit to a non-`main` branch of the sink,
  as both engines' Iceberg sinks do for their configured branch, with resume reading that
  branch? The default acted on stamps `main` only, because the sketch's resume reads the current
  snapshot's ancestry and 1.7 exposes no branch option on the streaming writer.

```
COVERAGE_ATTESTATION:
  pr_unit: mb-2a
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-012 walked against the order's steps and the sketch's §3.4 signatures, Q6, Q9, Q10, MBE-13, MBE-14, §5 pin 1 and DM-5; each carries its pin citation. Deviations from §3.4 are dated (D-1 base is Option, D-2 summary_entries returns Result, D-3 the crate-private SiteStamp adapter).
      artifacts: [task/ledgers/staging/mb-2a-ledger.md, crates/repark-iceberg/src/write/sink_offsets.rs, crates/repark-iceberg/src/write/sink_offsets_tests.rs, crates/repark-iceberg/src/microbatch/crash_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Negative pins cover the busy sink, the second claim on the live arm and on the stamp-only path, a head without the stamp, a branch commit inside a scope, a property drifted ahead of the summary, a removed property, an expired stamped snapshot, a newer offset format, another query's stamp in the ancestry, and an empty sink.
      artifacts: [crates/repark-iceberg/src/write/sink_offsets_tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is a typed MicroBatchError variant from MB-1's enum (SinkBusy, SinkCommittedTwice, RecoveryRequired with OffsetMismatch, StampedSnapshotExpired, UnstampedSinkCommit, CommitOutcomeUnknown, UnsupportedOffsetFormat); the arms surface it as DataFusionError::External and the pin downcasts the cause rather than matching text.
      artifacts: [crates/repark-iceberg/src/write/sink_offsets.rs, crates/repark-iceberg/src/write/sink_offsets_tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The scope registry is one process-wide mutex; every critical section is a single map read or write, no guard is held across an await (claim and record run before and after the commit future, never inside it), and a poisoned lock is recovered because no section can leave the map torn. Tests key the registry by fresh table uuids, so parallel tests never share an entry. The retry path is proven under a racing catalog (C-008).
      artifacts: [crates/repark-iceberg/src/write/sink_offsets.rs, crates/repark-iceberg/src/write/sink_offsets_tests.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The stamp carries uuids, epochs, generations, snapshot ids, positions and the Bronze table identifier only; no path, location or credential field exists in SinkRecord. Iceberg and DataFusion error texts pass through mask_value_credentials before they enter MicroBatchError::Catalog (D-6).
      artifacts: [crates/repark-iceberg/src/write/sink_offsets.rs, crates/repark-iceberg/src/microbatch/offset.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The change is additive behind the scope; unscoped arms commit as before (C-004) and the full repark-iceberg lib suite stays green (873 pre-existing pins unchanged beside 17 new). The MB-0 W7 and W4 cells agree with the stamped key sets on the two doors.
      artifacts: [crates/repark-iceberg/src/write/sink_offsets_tests.rs, python/repark-parity/tests/live_spark/mb0_streaming_oracle.json]
    - id: AT-7
      status: ATTACKED
      evidence: One catalog commit per batch is kept: the property rides the data commit's transaction, never a second commit. Unscoped, the arms add one uncontended map lookup and borrow the caller's extras without a copy (Cow::Borrowed, pinned).
      artifacts: [crates/repark-iceberg/src/write/sink_offsets.rs, crates/repark-iceberg/src/write/sink_offsets_tests.rs]
    - id: AT-8
      status: ATTACKED
      evidence: 12/12 gates green; new files under the default ceiling (336 and 947 lines, the tests in a #[path] sibling); write/map.md, write/merge/map.md and microbatch/map.md updated in the same commits as their files.
      artifacts: [task/ledgers/staging/mb-2a-ledger.md, crates/repark-iceberg/src/write/map.md, crates/repark-iceberg/src/write/merge/map.md, crates/repark-iceberg/src/microbatch/map.md]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; every outcome is a typed value (ScopeOutcome) or a typed error.
    - id: AT-10
      status: ATTACKED
      evidence: M1 (property write dropped) turns the resume pin and harness pin 1 red among 9; M2 (claim not marked) turns its 3 owning pins red; M3 (no summary-property comparison) turns the guard pin red. Each restored from a backup and confirmed clean by git diff --quiet.
      artifacts: [task/ledgers/staging/mb-2a-ledger.md]
```

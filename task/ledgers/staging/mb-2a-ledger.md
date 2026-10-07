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
| C-002 | `BatchScope` holds at most one stamp per sink (`enter` twice refuses `SinkBusy`, MBE-13); `claim` returns `None` for a table with no scope and the stamp with base `H = current_snapshot_id` inside one; a second claim in one scope refuses `SinkCommittedTwice` (MBE-13); dropping the guard clears the entry. | Scope pins in `sink_offsets_tests.rs`. | **OPEN** (step 2) | — |
| C-003 | Every arm the sketch names stamps the six `repark.cdc.*` keys (plus the two Spark keys on the `Table` door only) and the property in the **same** commit — one new metadata version, one new snapshot: the append arm (`commit_append_with_summary`), the MERGE copy-on-write arm (`commit_overwrite_on_ref`, both its insert-only and its delete-and-add overwrite) and the merge-on-read arm (`commit_row_delta_kind_on_ref`); the stamps read back equal to the claimed record through `SinkRecord::from_summary` and `from_property`. | Arm pins in `sink_offsets_tests.rs`. | **OPEN** (step 2) | — |
| C-004 | With no scope entered, the three arms commit exactly as before: no `repark.cdc.*` or Spark streaming key, no `repark.cdc.offsets.` property; the existing MERGE and write-options pins keep their answers. | The unscoped arm pin plus the full `repark-iceberg` lib suite. | **OPEN** (step 2) | — |
| C-005 | `ClaimedStamp::record_commit` records the committed snapshot id as the scope's outcome (`ScopeOutcome::Committed`), and a committed head that does not carry the stamp yields `RecoveryRequired(UnstampedSinkCommit)` (MBE-14). | Outcome pins in `sink_offsets_tests.rs`. | **OPEN** (step 2) | — |
| C-006 | `commit_stamp_only` commits one stamp-only `append` snapshot carrying the summary keys and the property in one metadata version, and records the scope outcome. | The stamp-only pin. | **OPEN** (step 2) | — |
| C-007 | `read_resume_point` returns the last committed record from one loaded `Table` with no catalog call of its own: the newest stamped snapshot of the query in the current ancestry, equal to the property; `None` on a sink with neither. | Resume pins in `sink_offsets_tests.rs`. | **OPEN** (step 3) | — |
| C-008 | A concurrent unrelated append landing between the stamped commit's load and its catalog update forces a retry, and the retried commit still carries both halves exactly once (one stamped snapshot, the property once, the racer's file live). | The racing-catalog pin. | **OPEN** (step 4) | — |
| C-009 | The property-versus-summary guard (Q10, R-14): a summary record and a property that disagree refuse `RecoveryRequired(OffsetMismatch)` with the summary as the durable record; a property with no stamped snapshot retained refuses `RecoveryRequired(StampedSnapshotExpired)`; a stamp of another query is passed over; a format above 1 refuses `UnsupportedOffsetFormat`. | Guard pins in `sink_offsets_tests.rs`. | **OPEN** (step 4) | — |
| C-010 | Harness pin 1 `test_microbatch_kill_after_commit_resumes_1` (sketch §5) is green: epoch 0 commits through the stamped append arm, every in-memory value drops, a reload and `read_resume_point` resume epoch 1 after a Bronze append; the sink equals Bronze with each id once, epochs 0 and 1 each appear once in the summary history, and the property equals the newest stamped summary. | `cargo test -p repark-iceberg --lib microbatch::crash_tests`. | **OPEN** (step 5) | — |
| C-011 | Mutations: dropping the property write turns the resume pin red; two further mutants are red on their owning pins. | Three mutation runs, restored by diff. | **OPEN** (step 6) | — |
| C-012 | Every gate in the brief is green and every touched map is current. | The gate list. | **OPEN** (step 6) | — |

## Dated decision rows

- **DM-5 (2026-10-07, measured).** Question: does the fork accept an empty `merge_append` as a
  stamp-only snapshot. Answer: yes. `SnapshotProducer` refuses an empty commit only when the
  snapshot properties are empty too (`fork:crates/iceberg/src/transaction/snapshot.rs:1058`–`:1063`),
  and the stamp is never empty. The commit is one metadata version, operation `append`, no
  `added-data-files`, scan plans no task. So the sketch's `commit_stamp_only` is buildable on
  the existing action, with no fork ask.

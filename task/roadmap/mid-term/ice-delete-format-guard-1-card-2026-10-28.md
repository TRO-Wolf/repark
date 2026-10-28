# ICE-DELETE-FORMAT-GUARD-1: a merge-on-read DELETE writes position deletes the next read cannot consume (RePark card)

**Filed:** 2026-10-28. **Filed by:** the audit clerk round (`claude-haiku-5-5`), from the independent audit ([iceberg-v2-label-independent-audit-2026-10-28.md](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md)) and the orchestrator's reproduction of 2026-10-28.

**Status:** dispatched 2026-10-28 on an Opus 5.5 builder lane, the same lane as ICE-POST-PUBLISH-OUTCOME-1. Open until the merge. Registry row: [ICE-DELETE-FORMAT-GUARD-1](../../../docs/spark-sql-iceberg-parity.md).

**Status update, 2026-10-28:** built as PR #1206, awaiting verification. Fix round 1 at `a8ab9364` (the pre-passthrough guards resolve the DML target as the planner does and fail closed); the verifier's findings and the pins are in the ledger.

**Status update, 2026-10-28:** fix round 2 at `1c6e730b` (the branch path classifies the statement from its tokenised head and fails closed; the checks run before a session WAP branch is created; the tag path reads the tokenised head; a missing namespace keeps the planner's not-found text; 21 pins in the guard file). The round-2 verifier's findings and the pins are in the ledger; the card awaits the next verification.

**Status update, 2026-10-28:** fix round 3 at `f23e70c3` (a `MERGE` under a session WAP branch is classified and checked before the branch is created; the fork-route arm and the redirected branch DML use the fork's exact mode parse; 25 tests in the guard file). The round-3 verifier's findings and the pins are in the ledger; the card awaits the next verification.

**Status update, 2026-10-28:** verification round 4 PASS; merges on the owner's word. The round-4 Opus 5.5 verifier passed `78827c24` with one S3 (R4-V1, the WAP branch sentence in the registry row, C-020 and the release notes), fixed in the round-4 records; the late refusal under a WAP branch is carded as WAP-LATE-REFUSAL-BRANCH-1.

**Grade:** product unit, Opus 5.5 builder lane.

**Severity:** Medium, the audit's rating (AUD-003). A fail-loud gap on a documented surface: the DELETE commits, and the next read fails. No silent corruption or physical data loss was observed.

## Why

On format v2 with merge-on-read, a `DELETE` whose position-delete files are Avro or ORC commits, and the next read fails with `Failed to load Parquet metadata … Corrupt footer`. The fork's delete loader opens non-Puffin position deletes with the Parquet-only reader (fork `crates/iceberg/src/arrow/caching_delete_file_loader.rs`, lines 265 to 271). The fork's writer selects the requested format (`crates/integrations/datafusion/src/physical_plan/delete_position_deletes.rs`, line 328). The reader and writer disagree.

## What was measured

Audit Appendix B, for each of `parquet`, `avro` and `orc`: a fresh table with `format-version` 2, `write.format.default` set to the format and `write.delete.mode` set to `merge-on-read`; `INSERT` of 0 to 4; `SELECT`; `DELETE … WHERE id BETWEEN 1 AND 3`; `SELECT`. The Parquet control returns 0 and 4. The Avro and ORC tables read 0 to 4 before the DELETE and fail on the read after it. The audit's probe and reproduction are recorded in the report's appendix; the orchestrator reproduced the finding on 2026-10-28.

## The fix

The RePark guard writes Parquet position deletes unless the delete format is set explicitly. An explicit non-Parquet setting refuses before publication, with a hint that names the supported format. The statement must not publish a delete file the read cannot consume. The fork's read side is F-POSDEL-FORMAT-READ-1 (fork card), on a separate Muse MAX lane.

## Pins

- Parquet, Avro and ORC data files with merge-on-read DELETE: the Parquet case reads the survivors 0 and 4; an explicit non-Parquet delete format refuses before publication, and the table's snapshot and rows are unchanged after the refusal.
- Each pin reads the survivors after the DELETE and after a reopen, not only the file suffix or magic.

## Out of scope

- The fork loader (F-POSDEL-FORMAT-READ-1).
- The position-delete batching (RP-POSDEL-BATCH-2, RP-60).
- Equality-delete keys after a drop (F-EQDEL-DROPPED-KEY-1).

Up: [map.md](map.md).

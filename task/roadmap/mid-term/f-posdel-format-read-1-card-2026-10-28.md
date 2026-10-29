# F-POSDEL-FORMAT-READ-1: the fork's position-delete loader dispatches on the delete file's format, not Parquet only (fork card)

**Filed:** 2026-10-28. **Filed by:** the audit clerk round (`claude-haiku-5-5`), from the independent audit ([iceberg-v2-label-independent-audit-2026-10-28.md](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md)) and the orchestrator's reproduction of 2026-10-28.

**Status:** dispatched 2026-10-28 on a Muse MAX lane. Landed on the fork as PR `#375` (squash `56aa8913`, 2026-10-28) and pinned by RP-61 (PR #1209, merged 2026-10-29 as `6ec819ef`).

**Target:** the owned `iceberg-rust` fork, at the pin `ea0b7741a57eb81ff25ee9dc18487d641b435a1b` (the five `rev` lines of RePark's root `Cargo.toml`). This is the owner's fork work; no RePark lane implements it.

**Consumer:** RePark's merge-on-read DELETE. The RePark guard ([ICE-DELETE-FORMAT-GUARD-1](ice-delete-format-guard-1-card-2026-10-28.md)) refuses an explicit non-Parquet delete format until this fork unit lands; lifting that refusal is a RePark change after the repin.

**Severity:** Medium, the audit's rating (AUD-003), on the fork side. A fail-loud gap; no silent corruption was observed.

## The fault

`crates/iceberg/src/arrow/caching_delete_file_loader.rs`, lines 265 to 271: non-Puffin position-delete files are opened with the Parquet-only loader. The writer in `crates/integrations/datafusion/src/physical_plan/delete_position_deletes.rs` (line 328) selects the requested format. A v2 merge-on-read DELETE with Avro or ORC position deletes commits, and the next read fails with a Parquet footer error. The audit's Appendix B reproduces it on the fresh-table statement sequence the RePark card gives.

## The fix

The loader dispatches on the delete file's format (Parquet, Avro, ORC), with the same column contract the Parquet path uses (the `file_path` and `pos` columns). A format the loader does not read stays a loud refusal with the format named. The writer's output and the Parquet path are unchanged.

## Pins

- Red-first on the audit's sequence for Avro and ORC: the survivors 0 and 4 after the DELETE and after a reopen, in the fork's datafusion integration tests.
- The existing suffix and magic checks in `crates/integrations/datafusion/tests/mor_delete_format.rs` stay; the new pins read the rows.
- A format the loader does not read refuses with the format named.

## Out of scope

- The RePark guard (ICE-DELETE-FORMAT-GUARD-1).
- Equality-delete key handling (F-EQDEL-DROPPED-KEY-1).

Up: [map.md](map.md).

# BRANCH-CLOCK-SKEW-METADATA-1 — under clock skew, a metadata-only commit after a branch update can publish metadata the next reader rejects

**Rating: proposed S2 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-6`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. Read, not measured: the replay read the code and found the risk holds.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S2 (proposed).** An availability risk; multi-writer.

## Finding

All `[R]` facts are read from code at the fork pin `b6e578dc`, which is also the fork pin at `dac2af61`.

- **[R] Build.** `build()` runs `try_normalize()` at table_metadata_builder.rs:1102 and pushes the previous-file log entry, stamped with the previous `last_updated_ms` (builder:142-145), at :1105, after validation.
- **[R] Reload check.** The check of `last_updated_ms` against the last metadata-log entry (table_metadata.rs:689-712, error at :707) runs on every deserialize (table_metadata.rs:1025, :1278).
- **[R] Snapshot-log checks.** The main snapshot-log checks (builder:340-365; table_metadata.rs:660-686) skip a branch-only update.
- **[R] Consequence as read.** A skewed writer's metadata-only commit after a branch update can therefore publish metadata that the next reader rejects.

## Why S2

An availability risk. It needs clock skew of more than 1 minute across writers, which is outside the single-writer scope.

## Scope

Multi-writer tables with clock skew. The single-writer scope does not reach it. Not measured.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).

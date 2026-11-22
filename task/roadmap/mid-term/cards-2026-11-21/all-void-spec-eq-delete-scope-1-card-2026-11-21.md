# ALL-VOID-SPEC-EQ-DELETE-SCOPE-1 — an equality delete under an all-void spec never reaches a data file written under another spec

**Rating: proposed S2 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-read-1`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. Read, not measured: the replay read the code and found the risk holds.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S2 (proposed).** A silent wrong read, on a table history written elsewhere.

## Finding

Read, not measured. The replay's line citations name no tree; `delete_file_index.rs` and `spec/partition.rs` are fork files, and `ENGINE_CONTRACT.md` is not in RePark's tree at main `5a3db151`.

- **[R]** `delete_file_index.rs:413-420` sends an equality delete to the global list only when the delete's partition tuple has no fields.
- **[R]** An all-void spec carries a non-empty tuple of nulls, so the delete lands in the spec-scoped map.
- **[R]** The lookup at `:504-511` uses the data file's own spec id, so an older file under another spec never sees the delete.
- **[R]** `PartitionSpec::is_unpartitioned` (spec/partition.rs:102-107) counts all-void as unpartitioned, but this path does not use it.
- **[R]** `ENGINE_CONTRACT.md:330-333` records this as backlog.

## Why S2

A silent wrong read: rows that should be deleted come back. It happens only on a table history written elsewhere.

## Scope

An equality delete under an all-void spec, and a data file written under another spec. Read only.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).

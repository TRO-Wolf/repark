# Card FORK-TABLECOMMIT-UPDATES-1: the fork lets a catalog decorator read a commit's updates

**Date:** 2026-10-09. **Filed by:** Claude (Opus 5.5), ENC-1 fold 2.

**Status:** open. Not scheduled. Fork work (`TRO-Wolf/iceberg-rust`), then a RePark repin.

**Retires:** when `iceberg::TableCommit` has a read accessor for its updates, and RePark's ENC-1 commit guard matches on `TableUpdate` values instead of reading `Debug` text.

## Why

`TableCommit` (fork rev `076d5f9`, `crates/iceberg/src/catalog/mod.rs`) exposes its updates only through `take_updates`, which empties the commit, and its builder is crate-private. A catalog decorator cannot read the updates and still forward the commit.

RePark's ENC-1 guard must decide, on a table that carries `encryption.key-id`, whether a commit adds a file. It reads the variant names from the commit's pretty `Debug` rendering ([encryption_guard.rs](../../../crates/repark-iceberg/src/catalog/encryption_guard.rs), `update_variants`). The orchestrator accepted this for v1.5.4 on 2026-10-09. It fails closed, it is an allow-list, and a pin fails to compile if the fork enum gains a variant. It still depends on the shape of derived `Debug` output.

## The change

- Fork: `pub fn updates(&self) -> &[TableUpdate]` on `TableCommit` (and `requirements`, for symmetry).
- RePark, after the repin: `is_metadata_only` becomes a `match` on `&TableUpdate` with no wildcard. `update_variants` and its pins on rendered text go away. The pins on behaviour stay.

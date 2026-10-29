# F-EQDEL-CONVERT-BATCH-1: the equality-delete conversion writes its position pairs in bounded batches, not one array per equality delete (fork card)

**Filed:** 2026-10-27. **Filed by:** the POSDEL docs clerk round (`claude-haiku-5-5`), from the orchestrator's measured facts of 2026-10-27 and the Opus 5.5 verifier's finding V-POSDEL-3 on the F-POSDEL-BATCH-1 review. **Dispatched:** not dispatched. The owner charters it.

**Target:** the owned `iceberg-rust` fork, at fork main `ea0b7741` (the squash commit of F-POSDEL-BATCH-1). A fix lands with red-first pins, and a later RePark repin takes it. This is the owner's fork work, and no RePark lane implements it.

**Consumer:** a later RePark repin. No RP number yet. RePark does not expose the `convert_equality_delete_files` action through the Spark door, so no RePark card carries it today.

**Severity:** P3. The failure is loud (the same `offset overflow` panic), but it sits in a maintenance action, not in a Spark-door DML statement. The owner has made no report of it, and each call is bounded by the data files that one equality delete matches, not by the table.

## What is already there at the pin

- `ConvertEqualityDeleteFiles` in `crates/iceberg/src/maintenance/convert_equality_delete_files.rs` (its `execute`) is the `convert_equality_delete_files` maintenance action. It runs one call per live equality-delete file.
- At fork head `ea0b7741`, line 538 of that file builds one `StringArray::from(Vec<&str>)` over every converted `(file, position)` pair of the equality delete. It writes that array as a single RecordBatch and closes the writer.
- `chunk_position_delete_pairs` is the chunking helper that F-POSDEL-BATCH-1 added. It is private to `crates/integrations/datafusion/src/physical_plan/delete_position_deletes.rs`, the position-delete writer that [F-POSDEL-BATCH-1](f-posdel-batch-1-card-2026-10-27.md) fixed.

## The fault

One equality delete matches a set of data files. The conversion puts every matched data-file path, with its position, into one `Utf8` array and writes that array as one RecordBatch. `Utf8` offsets are 32-bit, so the array panics with `offset overflow` once the path text passes `i32::MAX` bytes. The array's size follows the data files that one equality delete matches, so a single equality delete with enough long data-file paths reaches the guard.

## The fix as specified

1. Lift `chunk_position_delete_pairs` to a shared home in the `iceberg` crate, so that the position-delete writer and the conversion call one helper. The datafusion crate keeps its call site and imports the helper from the shared home.
2. Cut the converted pairs with that helper and write one RecordBatch per chunk, keeping `Utf8`. The two bounds are those of F-POSDEL-BATCH-1: a row bound, and 256 MiB of path text. Where this action reads its row bound is not fixed by this card; the owner names it at charter.
3. At the guard, return an error instead of panicking.

## Pins

The same shape as F-POSDEL-BATCH-1:

- Unit pins on the shared helper's cut, covering the row bound and the byte bound.
- An integration pin with a tiny batch: one equality delete converted into several position-delete files, and the table reads back the correct rows.
- An ignored stress pin that reproduces the panic on the pre-fix shape: one equality delete that matches data files whose path text passes `i32::MAX` bytes.

## Second item, dated 2026-10-28: three Parquet-only delete opens in the maintenance actions

Found by the fresh Opus 5.5 verifier of fork PR `#375` (F-POSDEL-FORMAT-READ-1, round 2) at fork `56aa8913`. All three are pre-existing and untouched by that PR. The format dispatch that the position-delete loader now has fits each of them. The priority stays P3.

- `crates/iceberg/src/maintenance/rewrite_position_delete_files.rs`: its caller skips non-Parquet position-delete files with `continue`. Avro and ORC position deletes are left uncompacted; the data stays correct.
- `crates/iceberg/src/maintenance/rewrite_table_path.rs`: its caller refuses non-Parquet delete files with a typed error.
- `crates/iceberg/src/maintenance/convert_equality_delete_files.rs`: there is no format gate. An Avro or ORC equality delete converted by this action fails loud with the Parquet footer error. This is by code reading; it was not run.

The fix for each is the same format dispatch on the delete file's format that the loader has, with red-first pins per action that read back the survivors.

## Out of scope

- The RePark exposure of the action through the Spark door. No RePark step is named here.
- The position-delete writer at the F-POSDEL-BATCH-1 site, which that fix owns. The lift touches that file only to import the shared helper.
- The memory held for the converted pair list before the cut. This card does not bound it. It is the same follow-up shape as the memory side on the F-POSDEL-BATCH-1 card.

## Dispatch

Not dispatched. The owner charters it or declines it.

Up: [map.md](map.md).

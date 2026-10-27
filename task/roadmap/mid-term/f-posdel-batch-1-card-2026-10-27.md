# F-POSDEL-BATCH-1: the position-delete writer streams its sorted pairs in bounded batches, not one array per partition (fork card)

**Filed:** 2026-10-27. **Filed by:** the POSDEL docs clerk round (`claude-haiku-5-5`), from the orchestrator's measured facts of 2026-10-27. **Dispatched:** 2026-10-27, fork unit F-POSDEL-BATCH-1, on a Muse MAX lane, with an Opus 5.5 verifier after it.

**Target:** the owned `iceberg-rust` fork, at the pin `d80a2884316e7a6fb457c29799be923bbedf977c` (the five `rev` lines of the root `Cargo.toml`). A fix lands with red-first pins, and RP-60 repins RePark. This is the owner's fork work, and no RePark lane implements it.

**Consumer:** RP-60, [RP-60-POSDEL-REPIN-1](rp-60-posdel-repin-1-card-2026-10-27.md), which repins RePark to the fixed fork and adds a Spark-door pin.

**Severity:** P2. The failure is loud: a panic that RePark's Python boundary surfaces as "repark internal error in PyReparkSession.sql: offset overflow". The measured record is a panic, not a wrong answer, and each measured workaround runs. It is not P1 for those reasons. It is not lower than P2 because one bulk `DELETE` on a merge-on-read v2 table of this size cannot complete at the pin, and the format-v2 label (2026-11-10) carries it as a dated carve-out if it does not land first.

## What is already there at the pin

- `write_position_deletes_for_partition` in `crates/integrations/datafusion/src/physical_plan/delete_position_deletes.rs` (pin `d80a2884`) writes one partition's position deletes. It holds the partition's deleted `(file, position)` pairs as one `Utf8` column of full data-file paths, built with `StringArray::from(Vec<&str>)`, and writes them in a single pass. The overflow panic is raised at a guard in this function. The writer already orders the pairs, and the fix changes how they are written, not their order.
- The metadata-delete door in RePark runs before the statement reaches the fork. `try_metadata_delete_door` (`crates/repark-spark/src/router/delete_update.rs`) calls `try_metadata_delete` (`crates/repark-iceberg/src/write/meta_delete.rs`), which asks `can_delete_using_metadata`. This is RePark's code, cited only to show the path a statement takes to the fork.

## The fault

A `DELETE ... WHERE` on a format-v2 merge-on-read Iceberg table that matches roughly 10 to 15 million rows in one partition panics with `offset overflow` inside `write_position_deletes_for_partition`. The writer builds one `StringArray` of every deleted row's full data-file path for the partition. Arrow's `Utf8` offsets are 32-bit, so the array fails once the path text passes `i32::MAX` bytes.

Measured by the owner in live testing on 2026-10-27: a ten-day date-range delete on a table partitioned by `months()` of that date, about 200 mostly-string columns, about 43 s to the panic, peak RSS about 145 GiB, the host not memory-bound.

Why the metadata delete did not take it: RePark runs the metadata-delete door first, using the partition-level rule Spark calls `canDeleteWhere` (`can_delete_using_metadata`). A ten-day range inside a month partition is not strictly decidable from partition values, so the door declined, and the statement passed through to the fork's merge-on-read delete plan. The table carried `write.delete.mode = merge-on-read`. The fork's default is copy-on-write.

## Spark's behaviour

Spark's merge-on-read DELETE feeds `(file, position)` pairs to the delete writer as rows arrive, and rolls files at the target size. It never builds one array per partition. Spark also writes one position delete per row on a v2 table here; the difference is streaming, not the count.

## The fix as specified

1. Write the sorted pairs in bounded batches. A batch closes at whichever bound comes first: a row bound, taken from the new table property `write.delete.position-delete-batch-rows` (default 1,048,576), or a byte bound of 256 MiB of path text. The byte bound keeps each batch far below the 2 GiB that a 32-bit offset can address.
2. Keep `Utf8`. The offsets stay 32-bit, and the batching keeps each array under them.
3. At the guard, return an error instead of panicking.
4. Put the batch cut in a pure chunking helper, with unit pins.
5. With the property at 10 rows, one partition's position deletes are written as several delete files, and the surviving rows read back correct.

## Pins

- Unit pins on the pure chunking helper, covering both the row bound and the byte bound.
- An integration pin with the property at 10 rows: several delete files for one partition, and the correct survivors on read.
- An ignored stress pin that reproduces the panic on the pre-fix shape. Suggested repro: a narrow table `(d DATE, x BIGINT)` partitioned by `months(d)`, 30 million rows spread over ten days of one month (20 million sits near the boundary with short local paths), then a `DELETE` of the ten-day range.

## Out of scope and follow-up

- The memory side. Before a batch is cut, the writer still holds one owned path string per deleted row, so peak memory on a 10 to 15 million row partition is not bounded by this unit. The follow-up, noted here and not charted, is a streaming producer: the position-delete writer takes `(file, position)` pairs from the scan as they arrive, as Spark does, instead of collecting them first.
- The copy-on-write delete path, the RePark metadata door and the Python facade. None changes in this unit.

## Workarounds

Recorded on the card until the fix lands and RP-60 repins:
- Set `write.delete.mode` to `copy-on-write` for the bulk delete.
- Phrase bulk deletes on whole partition values, so the metadata door takes them.
- The per-day delete loop the owner used works, but it multiplies delete files.

## Dispatch

Fork unit F-POSDEL-BATCH-1, dispatched 2026-10-27 on a Muse MAX lane, with an Opus 5.5 verifier after it. The card leaves the roadmap when RP-60 repins RePark to the merged fix.

Up: [map.md](map.md).

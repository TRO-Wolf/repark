# RP-POSDEL-BATCH-2: RePark's own position-delete writers stream their pairs in bounded batches (RePark card)

**Filed:** 2026-10-27. **Filed by:** the POSDEL docs clerk round (`claude-haiku-5-5`), from the orchestrator's measured facts of 2026-10-27.

**Status:** filed, not scheduled. The owner charters it.

**Depends on:** nothing on the fork. RePark may copy the fork's `chunk_position_delete_pairs` into its own code until the fork helper is shared; [F-EQDEL-CONVERT-BATCH-1](f-eqdel-convert-batch-1-card-2026-10-27.md) asks for that lift.

**Grade:** product unit, Opus 5.5 builder with the standing verifiers.

**Severity:** P2, for the same reasons as the fork card ([F-POSDEL-BATCH-1](f-posdel-batch-1-card-2026-10-27.md)): the failure is a loud panic, and a workaround exists (copy-on-write mode for the bulk statement).

## Why

RP-60 repins the fork to the batched writer, so the owner's reported statement, a plain `DELETE ... WHERE` on a merge-on-read table, completes: it passes through to the fork's plan. RePark has its own position-delete writers for other statement forms, and both still build one array over a whole partition or a whole identity set:

- `write_position_deletes_for_partition` in `crates/repark-iceberg/src/write/position_delete.rs` (line 326 on main `d1304a46`) builds one `StringArray::from(paths)` over all of a partition's pairs and writes a single RecordBatch.
- `register_identity_table` in `crates/repark-iceberg/src/write/predicate_dml.rs` (line 464) builds one `StringArray` MemTable over every identity pair.

RePark's writers serve two routes: the merge-on-read MERGE path, the format-v2 arm in `crates/repark-iceberg/src/write/merge/dv_close.rs`, and the identity DELETE and UPDATE forms (`WHERE id IN (subquery)`), which route through `predicate_dml`. They do not serve plain `DELETE ... WHERE`. So after RP-60, a merge-on-read MERGE, or an identity-IN DELETE or UPDATE, that touches more than about 2 GiB of data-file path text in one partition still panics in RePark's own writer.

## The fix

The same chunked write as F-POSDEL-BATCH-1, at both sites:

- A batch closes at whichever bound comes first: the row bound from the table property `write.delete.position-delete-batch-rows`, or 256 MiB of path text.
- `Utf8` stays, and each batch stays under the 32-bit offset limit.
- At the guard, return an error instead of panicking.
- The cut uses the fork's helper once it is shared, or a RePark-local copy until then.

## Pins

- Unit pins on the cut, covering the row bound and the byte bound.
- A Spark-door merge-on-read MERGE pin with `write.delete.position-delete-batch-rows` set to a tiny value: the surviving rows read back correct, at least one position-delete file is live for the partition, and the property set to `0` is refused through the door (the wiring proof).
- An identity-IN DELETE pin with the same property set tiny: the surviving rows read back correct, at least one position-delete file is live, and the property set to `0` is refused through the door (the wiring proof).
- An ignored stress pin per site that reproduces the panic on the pre-fix shape.

No pin asserts a delete-file count. The delete writer rolls files at 1,000-row multiples and ignores `write.delete.target-file-size-bytes`, and the [F-POSDEL-BATCH-1 card](f-posdel-batch-1-card-2026-10-27.md) records that a tiny batch cannot be shown to produce several files, so a count would not be reachable at this pin.

## Workaround

Until this card lands, run the bulk statement in copy-on-write mode: set the table's mode property for that statement's kind to `copy-on-write`.

## Out of scope

- The plain `DELETE ... WHERE` path. RP-60 covers it through the fork.
- The fork units: F-POSDEL-BATCH-1 (landed) and F-EQDEL-CONVERT-BATCH-1 (not dispatched).
- The memory side named on the fork card: a streaming producer that takes the pairs from the scan as they arrive.
- The Python facade.

Up: [map.md](map.md).

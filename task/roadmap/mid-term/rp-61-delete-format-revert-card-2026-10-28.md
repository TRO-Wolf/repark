# Card RP-61-DELETE-FORMAT-REVERT: restore the inherited position-delete format once the fork reads Avro and ORC delete files

**Date:** 2026-10-28. **Filed by:** the ICE-POST-PUBLISH-OUTCOME-1 and ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the coordinator's ruling Q2 on the builder's round-2 hand-back.

**Status:** open. Grade C (clerk). Not dispatched. Depends on fork F-POSDEL-FORMAT-READ-1 (fork PR `#375`) merging on the fork and RePark repinning to it (this card's unit, RP-61).

**Why:** the interim guard DELETE-FORMAT-GUARD-1 (ledger [ice-delete-format-guard-1](../../ledgers/staging/ice-delete-format-guard-1-ledger.md)) makes RePark's own merge-on-read writers write Parquet position-delete files on Avro and ORC tables whose delete format is only inherited, and refuses the statements that would write Avro or ORC delete files. The fork at pin `ea0b7741` reads only Parquet delete files, so the refusals and the Parquet default are a workaround. The fork fix is F-POSDEL-FORMAT-READ-1, fork PR `#375`. Once the fork reads Avro and ORC delete files, the interim guard is the wrong behaviour: Spark and Java write delete files in the table's data format, and the divergence in the registry row is only dated.

**What reverts** (the builder's round-2 list, measured on `24e0c3c7`):

- `crates/repark-iceberg/src/write/data_format.rs`: `resolve_delete_format` takes the data format again and falls back to it when the delete format is only inherited, as on `321c6095`. The fallback's unit test, `delete_falls_back_to_data_format` (base line 130), returns with it. The explicit Avro and ORC refusal stops being a refusal and becomes a read-back. `mor_delete_format` and `refuse_unreadable_merge_delete_format` are removed with the guard.
- `crates/repark-spark/src/router/delete_update.rs`: `refuse_unreadable_delete_format` and its `reaches_fork_writer` test are removed from `execute_delete` and `execute_update`.
- `crates/repark-spark/src/merge.rs`: the up-front `refuse_unreadable_merge_delete_format` call is removed.
- `python/repark/tests/writes/test_ice_orc_avro_1.py`: the C-013 pin (`test_mor_delete_on_orc_writes_parquet_delete_file`, expecting `[1, "PARQUET", 1]`) is re-pinned to `[[0, "ORC", 3], [1, "ORC", 1]]`, as ICE-ORC-AVRO-1 C-013 stated before this unit.
- `crates/repark-spark/tests/delete_format_guard.rs`: the refusal pins flip. A DELETE, UPDATE or MERGE on an inherited Avro or ORC table completes and reads back, with its delete files in the data format; the explicit-format pins that refuse, flip to complete with their delete files in the explicit format. The Parquet controls stay.
- The registry row DELETE-FORMAT-GUARD-1 retires, the dated line on ICE-WRITE-OPTIONS-ORC-AVRO retires, and the release-notes line of DELETE-FORMAT-GUARD-1 is replaced by a line that states the reads.

**Measured:** the interim guard's behaviour, on the unchanged fork pin, in the builder's round 1 and round 2 runs (the red and green tables of the ledger). The post-repin behaviour is not measured. The unit that reverts the guard owes its own red-then-green run against the repinned fork.

**Retires:** this card when the revert merges and the registry row DELETE-FORMAT-GUARD-1 is closed with the repin's date.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (C-008 is the divergence this card lifts); [ICE-ORC-AVRO-1 ledger](../../ledgers/staging/ice-orc-avro-1-ledger.md) (its C-013 amendment); [RP-POSDEL-BATCH-2 card](rp-posdel-batch-2-card-2026-10-27.md) (a different delete-writer gap on the same fork writer); [NATIVE-DOOR-DELETE-FORMAT-1 card](native-door-delete-format-1-card-2026-10-28.md) (the native door's exposure, which this revert does not reach by itself).

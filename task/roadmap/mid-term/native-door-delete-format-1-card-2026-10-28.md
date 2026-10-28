# Card NATIVE-DOOR-DELETE-FORMAT-1: the native door's fork-route DELETE, UPDATE and MERGE can still write position-delete files the fork cannot read

**Date:** 2026-10-28. **Filed by:** the ICE-DELETE-FORMAT-GUARD-1 clerk round (`claude-haiku-5-5`), from the coordinator's ruling Q1 on the builder's round-2 hand-back (the native door's exposure is a follow-up, not part of AUD-003).

**Status:** open. Grade C (clerk). Not dispatched. Not measured. No build is owed until the owner charters it.

**Why:** ICE-DELETE-FORMAT-GUARD-1 (ledger [ice-delete-format-guard-1](../../ledgers/staging/ice-delete-format-guard-1-ledger.md), AUD-003) guards the Spark door: its router (`crates/repark-spark/src/router/delete_update.rs`) and its MERGE router (`crates/repark-spark/src/merge.rs`). The native door (`repark.sql()`, `crates/repark-sql`) has no such guard. A native DELETE or UPDATE that reaches the fork writer on a format-version 2 merge-on-read table whose delete format is explicit Avro or ORC, or inherited from `write.format.default` Avro or ORC, may still write a delete file that the fork cannot read, after the statement reports success. The shared `resolve_delete_format` in `repark-iceberg` now writes Parquet on RePark's own route for an inherited format, so the exposure left is the fork-route statements.

Native MERGE has no up-front check. Its RePark writer refuses an explicit Avro or ORC format only at the position-delete write, so the data files the statement wrote earlier are not committed, and whether every such file is removed is not measured.

**Measured:** none on the native door. The exposure is read in the code. The builder did not run the native door, and the Spark-door red runs do not cover it.

**Fix (proposed):** mirror the Spark-door guard on the native door. Refuse an explicit Avro or ORC format on every route before passthrough. Refuse an inherited Avro or ORC format on a statement that would reach the fork writer. Add an up-front MERGE check before any file is written. Pin each route the way `crates/repark-spark/tests/delete_format_guard.rs` does, with the rows, the snapshot and the file count unchanged after the refusal. Measure the native door first: the card is not a fix until a red run shows the exposure.

**Retires:** the card when the native door has a red run that shows the exposure and a green run after the guard, or when RP-61 reverts the interim guard and the native door's fork route reads its own delete files.

**Relation:** [ICE-DELETE-FORMAT-GUARD-1 ledger](../../ledgers/staging/ice-delete-format-guard-1-ledger.md) (its C-009 is the Spark-door UPDATE check residual; the native-door check shares its shape); [RP-61 revert card](rp-61-delete-format-revert-card-2026-10-28.md) (the revert of the interim guard); [NATIVE-DOOR-POST-PUBLISH-1 card](native-door-post-publish-1-card-2026-10-28.md) (the native door's refresh gap from the same unit).

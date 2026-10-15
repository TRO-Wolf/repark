# Card MB-ENC-UNDELETED-FIELD-1: the keyed micro-batch refusal names the sink but not the undeleted file

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 109 (c).

**Status:** parked under the streaming freeze (the track frozen 2026-10-11), not a later round.

**Retires:** when the variant carries the undeleted paths (a public field, needs the owner when
the freeze lifts).

**Registry row:** [MB-ENC-UNDELETED-FIELD-1](../../../docs/spark-sql-iceberg-parity.md)
(dated row 2026-10-15, owner ruling 109 (c)).

## Risk

A micro-batch keyed refusal names the sink but not a staged file it could not delete.

## Sequence

`MicroBatchError::EncryptedSinkRefused { sink }` (a public variant of a `#[non_exhaustive]`
enum in repark-core's micro-batch module) cannot carry the name of a staged file the keyed
sweep failed to delete; adding it is a public field. The sweep logs each undeletable file
through a `tracing::warn`. The session error already names the file.

## Note

Owner ruling 109 (c) accepts F4 as pinned. The micro-batch error field is a card parked under
the streaming freeze, not a later round. The warning carries the full path of the file it could
not delete.

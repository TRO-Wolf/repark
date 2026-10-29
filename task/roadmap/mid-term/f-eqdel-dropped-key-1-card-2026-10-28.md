# F-EQDEL-DROPPED-KEY-1: the fork's equality-delete read fails once a delete's key column is dropped (fork card)

**Filed:** 2026-10-28. **Filed by:** the audit clerk round (`claude-haiku-5-5`), from the independent audit ([iceberg-v2-label-independent-audit-2026-10-28.md](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md)) and the orchestrator's reproduction of 2026-10-28.

**Status:** dispatched 2026-10-28 on a Muse MAX lane. Landed on the fork as PR `#377` (fold 2, squash `935cc55c`, 2026-10-28). RePark pinned it in RP-62 (card `rp-62-eqdel-repin-card-2026-10-28.md`, built 2026-10-29): Parquet data reads back the survivors through both doors; Avro and ORC data are fork card F-EQDEL-DROPPED-KEY-2.

**Target:** the owned `iceberg-rust` fork, at the pin `ea0b7741a57eb81ff25ee9dc18487d641b435a1b`. This is the owner's fork work; no RePark lane implements it.

**Consumer:** every RePark scan of a v2 table that has equality deletes whose key column is later dropped.

**Severity:** Medium, the audit's rating (AUD-004). A fail-loud schema-evolution gap: the surviving data is not returned at all, and no silent wrong value was observed.

## The fault

`crates/iceberg/src/arrow/caching_delete_file_loader.rs`, lines 305 to 314, evolves equality-delete keys against the current schema. `crates/iceberg/src/arrow/delete_file_loader.rs`, lines 176 to 186, passes that schema and the historical equality ids to `RecordBatchTransformer`. Once the key column is absent from the current schema, the transformer cannot find it and the scan fails with `field not found`.

## What was measured

Audit Appendix D, through the fork's public Rust APIs with production data and equality writers, transactions and the Arrow scanner: create a v2 table `{id: long, data: string}`; append `(10,a), (20,b), (30,c), (40,d), (50,e)`; commit an equality delete for ids 20 and 40; the baseline scan returns three rows; commit `Transaction::update_schema().delete_column("id")`; scan the `data` column. The scan returns `Unexpected => field not found`. The Iceberg specification's Equality Delete Files section requires continued use of a dropped equality-delete column.

## The fix

The equality-delete path applies the deletes with the historical key field, read from the delete file's own schema or from the historical schema by id, even though the field is absent from the current projection. The projection and the current schema are unchanged.

## Pins

- Red-first on the audit's sequence: the scan returns `a`, `c`, `e`.
- A second drop, a rename of the key and a re-add of a column with a new id, each with equality deletes committed before the change.
- The fork's read path covers deletes that other engines wrote as well as the fork's own writer output.

## Out of scope

- The RePark SQL route to the same schema change (not run by the audit; the RePark owner's measurement follows the fork merge).
- Position-delete formats (F-POSDEL-FORMAT-READ-1).

Up: [map.md](map.md).

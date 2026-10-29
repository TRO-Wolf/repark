# F-EQDEL-DROPPED-KEY-2: the Avro and ORC data readers refuse a dropped equality-delete key that the Parquet data path projects (fork card)

**Filed:** 2026-10-29. **Filed by:** the RP-62 clerk round (`claude-haiku-5-5`), from the RP-62 builder's measurements at `935cc55c` (the RP-62 ledger, C-006 and C-007).

**Status:** filed, not dispatched. Priority P2. This is the owner's fork work; no RePark lane implements it.

**Target:** the owned `iceberg-rust` fork, at the pin `935cc55c8e2b20ee337c01eca45314ecb10e0c2f` (RePark's pin after RP-62). The parent fork card is [F-EQDEL-DROPPED-KEY-1](f-eqdel-dropped-key-1-card-2026-10-28.md) (fork PR `#377`). The RePark card is [RP-62](rp-62-eqdel-repin-card-2026-10-28.md).

**Consumer:** every RePark scan of an Avro or ORC data file whose equality-delete key column was later dropped, or dropped and re-added under a new field id.

**Severity:** fail-loud. The scan refuses; no wrong rows were observed at either pin.

## The fault

On Avro and ORC data files, the scan projects the equality-delete key by its historical field id, which the current schema no longer has, and refuses the file. The error is the same at `56aa8913` and at `935cc55c`, whatever the delete format:

`External error: DataInvalid => Projected field id 1 is not present in the scan schema for data file '<tmp>/ns/t/data/<uuid>-00000.avro'`

ORC data gives the same text with `.orc`. The Parquet data path reads the survivors at `935cc55c` (F-EQDEL-DROPPED-KEY-1, fork PR `#377`).

## What was measured

RP-62 at `935cc55c`, through RePark SQL on the Spark door and the native door: an Avro or ORC data file with an equality delete in the same format or in Parquet. After `ALTER TABLE ... DROP COLUMN id` the read refuses; after the drop and a re-add (`ADD COLUMN id INT`, a new field id) it refuses the same way.

The fork's #377 tests (`crates/iceberg/tests/mor_eqdel_format.rs`, `eqdel_dropped_key.rs`, `eqdel_key_paths.rs`) write Parquet data only (`write_data_file` uses `ParquetWriterBuilder`), so the Avro and ORC data readers' projection of a dropped key was not measured there.

## The fix (proposed)

The Avro and ORC data-file readers project a dropped equality-delete key by field id, as the Parquet data path does after #377: the historical key field comes from the delete file's own schema or from the historical schema by id, and the data file's column is read by that id. The scan schema and the current projection are unchanged.

## Pins (when it lands)

- `crates/repark-sql/tests/eqdel_dropped_key_door.rs`: `a_dropped_equality_delete_key_on_avro_data_is_loud_or_correct` and `a_dropped_equality_delete_key_on_orc_data_is_loud_or_correct` accept the refusal today. When the fix lands they tighten to the three survivors alone, red on the refusal at `935cc55c`, and RP-62 ledger C-006 and C-007 move to PROVEN.

## Out of scope

- The Parquet data path (F-EQDEL-DROPPED-KEY-1, pinned by RP-62).
- The three Parquet-only delete opens in the maintenance actions (F-EQDEL-CONVERT-BATCH-1).
- The promoted-key `_pos` defect (F-EQDEL-PROMOTED-KEY-POS-1).

Up: [map.md](map.md).

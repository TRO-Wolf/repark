# map — repark-spark/src/view_ddl/frame_settings/held_iceberg

## Purpose

The unit tests of `../held_iceberg.rs` (ICE-HELD-FRAME-SNAPSHOT-1 unit 1a, 2026-11-20), on a full Spark-door session
over a load-counting memory catalog under the encryption guard (fix round 1: the session decorates only a guarded
handle): a held action makes no catalog load with or without a same-session write (the
zero-inner-load proof); the re-bound wrapper appears only in the optimised plan, never in the frame's plan or a
DataFrame view's; an own-plan memo keeps its first binding; a recreated table refuses with `TABLE_ID_MISMATCH`; an
added column is read with the frame's columns and a dropped one refuses with `COLUMNS_MISMATCH`; a view name picks
the binding mode; a frame built over a DataFrame view refuses a later recreate by table id and a later dropped column
with `COLUMNS_MISMATCH`, also when a write came between the view and the frame, while a frame built after the change
reads the view's error or the new table (delta 2, D1); the memo's kept table is the frame's own (delta 2, C-5); and
(ENC-1 condition 2) on a keyed table, a refused `INSERT`, `INSERT OVERWRITE`, `DELETE`,
`UPDATE` and `MERGE` each leave the session epoch where it was and the held frame answering its two rows.
Unit 2 (2026-11-21), a lazy cache table over a held frame: it keeps its fill (the same buffer) across a write to
another table, a property change and an added column, refills after an insert into its table and keeps the refill; a
recreate refuses its refill with `TABLE_ID_MISMATCH` and a drop with `TABLE_OR_VIEW_NOT_FOUND`; an own-plan memo
noted at registration keeps that binding after an insert while a fresh read refills, and a memo that first read the
cache keeps that fill's binding after a second insert.

## Contents

- `tests.rs` — the cells above.

Up: [../map.md](../map.md).

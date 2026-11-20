# map — repark-spark/src/view_ddl/frame_settings/held_iceberg

## Purpose

The unit tests of `../held_iceberg.rs` (ICE-HELD-FRAME-SNAPSHOT-1 unit 1a, 2026-11-20), on a full Spark-door session
over a load-counting memory catalog: a held action makes no catalog load with or without a same-session write (the
zero-inner-load proof); the re-bound wrapper appears only in the optimised plan, never in the frame's plan or a
DataFrame view's; an own-plan memo keeps its first binding; a recreated table refuses with `TABLE_ID_MISMATCH`; an
added column is read with the frame's columns and a dropped one refuses with `COLUMNS_MISMATCH`; a view name picks
the binding mode.

## Contents

- `tests.rs` — the cells above.

Up: [../map.md](../map.md).

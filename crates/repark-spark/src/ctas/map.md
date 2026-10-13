# map — repark-spark/src/ctas

## Purpose

Child modules of `../ctas.rs`, the staged CTAS and RTAS seat.

## Contents

- `nanosecond_ask.rs` — **ICE-TSNS-CREATE-DOOR-1 (2026-10-13):** the Spark door's adapter
  between the session and `repark_iceberg::write::nanosecond_door`. `asked_columns(ctx, plan)`
  is `None` when `repark.sql.allowImplicitTimestampNanos` is on, else the flags of
  `repark_functions::timestamp_ns_cast::asked_nanosecond_columns` over the eagerly analyzed
  plan. `ask_of` turns flags into a `NanosecondAsk` that carries this door's remedies: the
  nanosecond cast can be written here, microseconds are `TIMESTAMP_NTZ` and `TIMESTAMP`.
  `nanosecond_ask` is the two together, and is the function `merge.rs` puts in
  `MergeSpec::nanosecond_ask`. Used by `../ctas.rs`, `../merge.rs` and
  `../insert_by_name/evolution.rs`. The work order names the file `src/nanosecond_ask.rs`;
  it lives here because the crate root is at its module-line ceiling and ceilings do not
  move in this unit.
  pins: ice-tsns-create-door-1/C-005, C-006

  **Fold 1 (2026-10-13):** the remedies are this door's whole sentences: the setting as
  `ReparkSession.builder.config` read when the session is built; microseconds as
  `CAST(<expression> AS TIMESTAMP_NTZ)` and `CAST(<expression> AS TIMESTAMP)`; and three
  ways to a v3 table (a create, a replace, an existing table). `merge_nanosecond_ask` is the
  MERGE seat's function: inside a MERGE source `CAST(… AS TIMESTAMP)` is nanoseconds, so
  its zoned microsecond spelling is `to_timestamp(<expression>)`. `target(requested,
  existing)` answers the door's `Target` for the CTAS seat: the requested format version
  when the statement names one, else the replaced table's. The flags come from
  `asked_nanosecond_columns_in`, which does not analyze a plan with no nanosecond column.
  `temp_view_plan` is the resolver the walk is given: a scan of a SQL temp view answers
  `view_ddl::temp_view::definition_plan`, the plan of the view's text at definition, so a
  cast written in `CREATE TEMP VIEW` is an ask. The view re-plans the same text when it is
  scanned.
  pins: ice-tsns-create-door-1/C-012, C-016

## Pointers

- Up: [../map.md](../map.md)

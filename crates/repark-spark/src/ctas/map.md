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

## Pointers

- Up: [../map.md](../map.md)

# map — repark-iceberg/src/write/insert_defaults

## Purpose

Unit tests for `../insert_defaults.rs`. The parent module declares
`#[cfg(test)] mod tests;`.

## Contents

- `tests.rs` — literal-converter pins, INSERT target/list helpers, and the
  load-count pins: an INSERT without a `DEFAULT` marker loads no table, and
  `fill_insert_plan` reuses the marker pass table instead of a second load
  (counting test catalog).
  pins: ice-v3-write-default-1/C-004, C-006, C-010

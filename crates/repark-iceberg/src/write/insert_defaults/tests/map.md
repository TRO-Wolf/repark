# map — repark-iceberg/src/write/insert_defaults/tests

## Purpose

Unit tests for `../../insert_defaults.rs`, moved from `insert_defaults/tests.rs` on
2026-09-18 (run 21b round 2) so the C-009 setter guard's `tests` path exemption covers
the test-only `with_write_default` builder. pins: ice-v3-write-default-1/C-024

## Contents

- `mod.rs` — literal-converter pins, INSERT target/list helpers, and the
  load-count pins: an INSERT without a `DEFAULT` marker loads no table, and
  `fill_insert_plan` reuses the marker pass table instead of a second load
  (counting test catalog).
  pins: ice-v3-write-default-1/C-004, C-006, C-010
- `map.md` — this file.

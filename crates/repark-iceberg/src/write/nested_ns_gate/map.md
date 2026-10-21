# map — repark-iceberg/src/write/nested_ns_gate

## Purpose

The unit tests of the parent `nested_ns_gate.rs`, the one gate for a naive timestamp leaf below
the top level of a column (R-015 for `timestamp_ns`, R-011 for microsecond `TIMESTAMP_NTZ`).
They moved here from an inline `mod tests` on 2026-10-20 (ICE-NTZ-NESTED-REFUSE-1), when the
parent grew past the 1000-line ceiling; the parent's prose stays in [../map.md](../map.md).

## Contents

- `tests.rs` — one case per container kind and per statement form for the nanosecond walk and
  the supply model (R-015), and for the microsecond walk, the zone test at every depth and
  layout, the field-path tests, and the feeds each statement form names (R-011).
  pins: ice-tsns-merge-wall-1/C-040, ice-ntz-nested-refuse-1/C-001

## Pointers

- Up: [../map.md](../map.md)

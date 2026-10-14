# map — repark-iceberg/src/write/store_overflow

## Purpose

The test half of [`../store_overflow.rs`](../map.md), moved out by ICE-STRUCT-PAIRING-1
(2026-10-14) so the module stays under the file-size ceiling.

## Contents

- `tests.rs` — the CAST-OVERFLOW-INSERT-1 tests of `StoreOverflowCast`, unchanged: constant,
  `VALUES`, column and subquery overflows, truncated stores, the lazy UPDATE and the divisor
  guard swap.

Up: [../map.md](../map.md).

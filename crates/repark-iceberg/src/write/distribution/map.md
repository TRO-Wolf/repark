# map — repark-iceberg/src/write/distribution

## Purpose

Children of the `write::distribution` module (`write/distribution.rs` declares them). The
hash distribution rule and its stream/sort drivers live in the parent file; this directory
holds the pieces split out of it.

## Contents

- `tests.rs` — the distribution unit battery, all three generations in one module:
  WRITE-DISTRIBUTION-1's plan-path layouts (one value → one writer, determinism, sparse and
  empty inputs, NULL values, `bucket(4, id)` + `day(ts)`, the unpartitioned bypass, the missing
  source column), WRITE-DISTRIBUTION-2's stream-path layouts (one value → one writer,
  determinism, NULL values, a two-field spec, MERGE inserts through the MERGE entry, the
  `truncate` cast over a view-typed string, the partitioned abort), and WRITE-ORDER-DIST-1's
  mode gate and sort stage (the `none`/`hash`/`range` layouts, the unknown mode, the `none`
  round-robin stream layout, cross-batch sorting, monotone committed files).
  pins: write-distribution-1/C-001, C-002, C-003, C-004, C-005, C-006, C-008
  pins: write-distribution-2/C-001, C-002, C-004, C-005, C-006, C-008
  pins: write-order-dist-1/C-007, C-008, C-010

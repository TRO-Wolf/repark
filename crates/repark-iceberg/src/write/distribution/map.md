# map — repark-iceberg/src/write/distribution

## Purpose

Children of the `write::distribution` module (`write/distribution.rs` declares them). The
hash distribution rule and its stream/sort drivers live in the parent file; this directory
holds the pieces split out of it.

## Contents

- `canonical_float.rs` — `CanonicalFloatExpr::wrap`, applied to every `Float32` / `Float64` sort
  key in `default_sort_lex_ordering`. Arrow's total order places a negative NaN below every
  value; Spark's `Float.compare` treats all NaN as one value above every value, and the fork's
  INSERT path gets there by canonicalising NaN before the sort (`iceberg-datafusion`
  `physical_plan/sort.rs`, private on the pinned rev — this is its equivalent, not a second
  rule). Measured Spark answer for the float cell: NULLS FIRST, values ascending, one solid NaN
  block at the end.
  pins: ice-sorted-insert-1/C-008
- `router.rs` — the stream dispatcher rule (WRITE-DISTRIBUTION-2): `PartitionRouter` splits
  each batch by hash of the writer's partition values and `send_routed` delivers each part to
  its slot's worker. `route_partitioned_stream` honours `write.distribution-mode` — `none`
  deals whole batches round-robin instead of routing (WRITE-ORDER-DIST-1 merge).
  pins: write-distribution-2/C-001, C-004, C-005
  pins: write-order-dist-1/C-007
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
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the MERGE-insert stream pin stages
  through `merge::session_staging::write_new_data_files_from_stream_with` with an
  empty `WriterStagingOverrides`, so the call keeps its layout while the session
  conf travels on the new path.
- `sort_order_tests.rs` — round-2 sort-order pins split out of `tests.rs` at the 1000-line
  ceiling: the dotted nested sort field sorts on the nested value (null structs sort as
  null), and a transform sort order refuses the write loud (the WRITE-ORDER-TRANSFORM-1
  red-when-fixed pin). Shares `tests.rs` helpers (`memory_catalog`, `declare_order`,
  `iceberg_schema`, `shuffled_full_batches`) through `pub(super)` visibility.
  pins: write-order-dist-1/C-008
  **WO U5 PR2b (2026-09-24):** the `WriteSortField` literals here and in `tests.rs` name
  `Transform::Identity`, the field the struct gained for transform terms.

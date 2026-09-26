# map — repark-iceberg/src/write/partition_spec

## Purpose

Holds the unit tests for `../partition_spec.rs`. The parent module declares
`#[cfg(test)] mod tests;`, which resolves to `tests.rs` here; `../alter.rs` sits at its
file-size ceiling, so new partition-spec pins land in this file.

## Contents

- `tests.rs` — **U11-EDGE-1 round 2 (2026-09-26):** `struct_text_renders_docs_and_decimal_like_java`
  renders the verifier's measured table (`id BIGINT NOT NULL COMMENT 'the key'`, `DECIMAL(10,2)`,
  list, map, struct, timestamp_ntz, binary, float, double, date, boolean, with Java's field ids)
  through `java_struct_text` and asserts the exact text Spark printed:
  `struct<1: id: required long (the key), 2: d: optional decimal(10, 2), …>`
  (`target/probe-u11-edge-1/vx3-spark.json`, `add`). V-005. pins: u11-edge-1/C-021
  `replace_field_wrong_case_source_refuses_like_spark` and
  `replace_by_transform_wrong_case_source_refuses_like_spark` pin the `ReplaceField` and
  `ReplaceFieldByTransform` arms of the partition-source check: on a table partitioned by
  identity `cat`, a replacement whose source is `CAT` refuses the measured
  `Cannot find field 'CAT' in struct: struct<1: id: optional int, 2: cat: optional string>`
  and the default spec keeps `cat` (mutation: dropping either arm from `bound_source` turns
  both red). V-012. pins: u11-edge-1/C-012
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)

# map — repark-functions/src/merge_schema

## Purpose

File-backed tests for `../merge_schema.rs` (IPI-19 + IPI-37, 2026-09-20): the
`spark.sql.iceberg.merge-schema` session conf carrier. The conf lives beside the
ANSI, case-sensitivity and time-zone carriers because it is read by a write-path
decision rather than stored, and every Spark session knob in this repository is
one `ConfigExtension`.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../merge_schema.rs`.
  The absent key is `false` (Spark's default); `true` / `false` parse
  case-insensitively and padded; a non-boolean refuses with Spark's
  `[INVALID_CONF_VALUE.TYPE_MISMATCH]` / `22022` shape; the carrier round-trips
  through `SessionConfig`; a direct `repark.merge-schema.*` set refuses, naming
  the user-facing key; and only the hyphenated Iceberg spelling is a session key
  (the camel-case `mergeSchema` spelling is a **write option**, parsed in
  `repark-spark`'s `write_options.rs`, not a conf).
  pins: ipi-19-56-37-schema-evolution-write/C-004, C-012

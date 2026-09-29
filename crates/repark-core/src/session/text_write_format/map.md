# map — repark-core/src/session/text_write_format

## Purpose

Spark-compatible TIMESTAMP / TIMESTAMP_NTZ / DATE formatting for CSV and JSON
writes, shared by the local and s3a routes. The writer builds a `SELECT` that
wraps temporal columns in the `repark_write_format_text` UDF; the UDF renders
each value with the session zone and the user formats, and the sink writes
plain strings. Non-temporal columns keep `SELECT *`, so their bytes never
change; partition columns stay unwrapped.

## Contents

- `render.rs` — the pattern renderer: Spark `DateTimeFormatter` letters for
  date, time, offset, localized offset, zone id, era, quarter, week-aligned
  `F`, and modified-Julian `g`, plus the three default renders. Zone-name
  letters (`v`, `z`) fail: names need JRE locale data, so the compiler refuses
  them eagerly for LTZ and the renderer fails them lazily for NTZ/DATE.
- `udf.rs` — the volatile `ScalarUDF` and its Arrow recursion (timestamp, date,
  struct, list, map values; map keys untouched). Return-type mapping mirrors
  the recursion so plans see `Utf8` where strings come out.
- `select.rs` — `build_text_write_select`: per-kind user-pattern validation
  (Spark error classes) plus the projection builder, and the
  `ReparkSession::text_write_select_sql` entry over a `DataFrame` schema.

## Debug

- A `timestampFormat` write fails before any file appears: the compiler
  rejected the pattern in `select.rs` validation; replay the pattern through
  `compile_write_pattern` in `tests/text_write_format.rs`.
- A write fails mid-stream with `Unsupported field` / `Unable to extract
  ZoneId`: a compiled pattern met a value kind that lacks the field (Spark
  fails the same write lazily); check the value kind against the pattern.
- Wrong wall or offset, right shape: the zone id or the instant is wrong, not
  the renderer — compare `UNIX_MICROS` of the literal with Spark first.

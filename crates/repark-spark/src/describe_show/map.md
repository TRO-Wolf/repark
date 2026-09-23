# map — repark-spark/src/describe_show

## Purpose

File-backed tests for the SHOW handlers (`../describe_show.rs`): the
`SHOW [USER] FUNCTIONS IN <catalog>.system` parser pins (USER-optional match,
bare-SHOW fallthrough, non-`system` scope and trailing-junk fall-through), the
one-column `function` batch shape, and the `<cat>.system.<fn>(` pre-parse
rewrite pins (all seven names, multi-call statements, unknown-catalog /
two-part / CALL / string-literal / quoted-identifier / missing-paren
non-claims, `system`+function case-insensitivity with exact catalog match, and
the internal-name ↔ registered-UDF identity that kills name drift). It also
owns the metadata-table `DESCRIBE` intercept (`metadata_table.rs`): one row per
column of the metadata table from the same provider schema `SELECT *` resolves,
with the four-part parser for names the metadata rewrite leaves untouched.

## Contents

- `metadata_table.rs` — **IPI-23-MT-DESCRIBE-1 (2026-09-22; moved from
  `../describe_metadata_table.rs` 2026-09-23):** `try_describe_metadata_table`
  serves the rewritten `$` form only; a real table at the written four-part
  path keeps the plain compound-identifier refusal (real table wins, via the
  shared `../metadata_tables.rs::table_exists_parts` probe — the same rule
  SELECT applies); a missing base or namespace answers TABLE_OR_VIEW_NOT_FOUND
  naming the name as written — the four written parts, or the written
  `cat.ns.t$suffix` name when the `$` form was written directly — any other base
  error surfaces as-is, and
  anything without `$` falls through to the plain path
  (no default-namespace recovery since critic r1).
  `try_parse_describe_metadata_table` parses the un-rewritten four-part form
  (missing base, `EXTENDED`/`FORMATTED`, which print the column rows only).
  `../describe_show.rs` carries the five-line hook and the router a two-line
  `or_else`; the plain path maps a missing namespace to the same 42P01 answer.
  pins: ipi-23-mt-describe-1/C-001, C-002, C-003, C-004, C-007, C-009, C-012, C-013, C-015, C-016, C-017, C-018, C-019
- `tests.rs` — `#[cfg(test)] mod tests;` in `../describe_show.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `<cat>.system.<fn>` call stops resolving | The rewrite pins (`cargo test -p repark-spark describe_show::`); then whether the catalog is a live Iceberg entry |
| `SHOW USER FUNCTIONS IN <cat>.system` falls through to DataFusion | The parser pins; then the `catalogs.get` gate on the router block |

First checks: `cargo test -p repark-spark describe_show::`. Escalate to: [../map.md#debug](../map.md).

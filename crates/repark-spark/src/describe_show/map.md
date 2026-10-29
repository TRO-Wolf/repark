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
  serves the rewritten `$` form only, splitting the name at the last `$` —
  the pinned fork's `rsplit_once` rule, so a base containing `$` resolves; a
  real table at the written four-part
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
  **WO-B11 (2026-09-23):** `rewrites_metadata_path` answers false when this parser claims
  the statement, so the router leaves #806's four-part DESCRIBE unrewritten.
- `tests.rs` — `#[cfg(test)] mod tests;` in `../describe_show.rs`. **NS-TRUTHUP-1 (2026-10-26):**
  `describe_shows_timestamp_ns_and_timestamptz_ns` pins the spec names `timestamp_ns` and
  `timestamptz_ns` that `describe_ddl_type_name` gives nanosecond columns, and the names it keeps for
  the microsecond ones (`timestamp_ntz`, `timestamp`).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `<cat>.system.<fn>` call stops resolving | The rewrite pins (`cargo test -p repark-spark describe_show::`); then whether the catalog is a live Iceberg entry |
| `SHOW USER FUNCTIONS IN <cat>.system` falls through to DataFusion | The parser pins; then the `catalogs.get` gate on the router block |

First checks: `cargo test -p repark-spark describe_show::`. Escalate to: [../map.md#debug](../map.md).

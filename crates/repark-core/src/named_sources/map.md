# map — repark-core/src/named_sources

## Purpose

File-backed pins for named database sources (`../named_sources.rs`) — CFG-2 step 1: the
declared-source listing, the `source(name)` handle, and the refusing catalog provider
that answers for a registered source name until its connector lands (roadmap 1.10).

## Contents

- `tests.rs` — the eight step-1 pins (`#[cfg(test)] mod tests;` in `../named_sources.rs`):
  a `SELECT` under a registered source name answers the D-1 connector message naming the
  source path, the kind, and `1.10` (never the engine's not-found); `CREATE TABLE` and
  `DROP TABLE` under the name answer the same refusal (the plan guard's doing — DROP
  swallows provider errors upstream); registration over an unroutable host opens no
  connection; `ping()` refuses the same message; `sources()` lists name / kind spelling /
  key path / `auto_register` with secret props redacted; `auto_register = false` lists
  without registering (the engine's not-found answers SQL under the name); a catalog
  registered over a source name refuses as a duplicate; an unknown handle refuses naming
  the declared sources. Config fixtures are forced temp files, so no pin reads the
  ambient environment.
  pins: cfg-2/C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-011

  **SOURCE-URL-REDACT-1 (2026-10-06):** `sources_listing_masks_a_password_inside_a_url_shaped_value`: a source's
  `url` and keyword `dsn` list with the password masked and the host and user kept, and the
  `SourceSpec` `Debug` carries no password. pins: source-url-redact-1/C-007
  **C-2d (2026-10-07):** the three CFG-2 Postgres refusal pins are retired by name and
  replaced by mount pins: `configured_source_select_refuses_with_connector_message` (C-003)
  by `configured_source_select_resolves_through_the_postgres_mount` (a source without `user`
  answers its settings refusal at first resolution, naming the source; no `1.10`);
  `configured_source_create_table_refuses_with_connector_message` (C-011) by
  `configured_source_ddl_refuses_as_read_only` (`DROP SCHEMA` and `CREATE DATABASE` answer
  `CONNECT-DECL-pg-ddl`; a SQL Server source keeps `1.10`); `source_ping_refuses_until_connector`
  (C-005) by `source_ping_resolves_through_the_mount` (mounted and unmounted sources). New:
  `mounted_postgres_sources_are_read_only_catalogs`. Every other CFG-2 assertion is unchanged.
  pins: c-2/C-097, C-099, C-101, C-104
## Pointers

- Up: [../map.md](../map.md)
- The implementation: [../named_sources.rs](../named_sources.rs)
- The loader stage that parses `SourceSpec`s: [../config_file/sources.rs](../config_file/sources.rs)

## Debug

First checks: `cargo test -p repark-core named_sources`.

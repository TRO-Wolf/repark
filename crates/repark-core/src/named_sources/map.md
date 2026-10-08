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
  **Fold 1, N2:** `configured_source_ddl_refuses_as_read_only` adds `CREATE SCHEMA`,
  `CREATE SCHEMA IF NOT EXISTS` and `CREATE DATABASE`, each `IF NOT EXISTS` or not, naming
  `company_db.fresh`. pins: c-2/C-112
  **Residual of N2:** four pins, `bare_create_schema_if_not_exists_refuses_under_a_mounted_default_catalog`,
  `bare_create_schema_refuses_under_a_mounted_default_catalog`,
  `bare_create_database_if_not_exists_refuses_under_a_mounted_default_catalog` and
  `bare_drop_schema_refuses_under_a_mounted_default_catalog`, run a bare name after
  `SET datafusion.catalog.default_catalog` names the mount; the control
  `bare_schema_ddl_succeeds_under_the_ordinary_default_catalog` keeps the ordinary default
  creating and dropping a schema. pins: c-2/C-118
  **Fold 1, N6:** three pins for the verifier's surviving mutations.
  `a_source_named_like_an_engine_catalog_refuses_as_duplicate` covers a catalog registered on
  the `SessionContext` alone (V1). `a_mounted_schema_refuses_table_registration_as_read_only`
  covers `register_table` and `deregister_table` on a mounted schema (V3).
  `sources_listing_masks_a_percent_encoded_url_password_key` covers a userinfo token and a
  `pass%77ord` query key, which only `redact_source_prop` decodes (V4). pins: c-2/C-113
  **Fold 1, N5:** `catalog_apis_resolve_a_mounted_source_instead_of_an_unknown_catalog`:
  `table_exists("company_db.public.t")` resolves through the mount (a source without `user`
  answers its settings refusal), and `list_iceberg_table_names` under the source refuses with
  the read-only text, never `unknown catalog`. pins: c-2/C-115
## Pointers

- Up: [../map.md](../map.md)
- The implementation: [../named_sources.rs](../named_sources.rs)
- The loader stage that parses `SourceSpec`s: [../config_file/sources.rs](../config_file/sources.rs)

## Debug

First checks: `cargo test -p repark-core named_sources`.

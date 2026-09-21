# map — repark-iceberg/src/view

## Purpose

The Iceberg view service: each Spark view statement mapped onto the owned fork's
`Catalog` view methods, `ViewCreation`, and the `ReplaceViewVersionAction` commit
path. Memory catalog only; Glue and S3 Tables refuse per statement off the
fork's `FeatureUnsupported` (Spark's measured wording).

## Contents

- `mod.rs` — `ViewTarget` / `ViewDefinition` / `ViewReadSpec`;
  `create_or_replace_view` (plain create, `IF NOT EXISTS` noop, `OR REPLACE`
  through `replace_version`, duplicate/table-collision `VIEW_ALREADY_EXISTS`;
  R2: on `view_exists` `FeatureUnsupported` the path routes straight to the
  mutating call, so the per-statement A-9 refusal wins over any existence
  state and no refusal is ever emitted from the existence check);
  `drop_catalog_view` (`VIEW_NOT_FOUND`, `DROP VIEW` over a table refuses);
  `list_catalog_views` (empty on `FeatureUnsupported`, never an error);
  `view_schema_for_output` (declared aliases become field names, arity mismatch
  by direction); `split_view_properties` (`COMMENT` becomes `comment`, derived
  `provider`/`location`/`format-version` never stored); `view_location`
  (`{warehouse}/{ns}/{view}`, no trailing slash); `view_read_spec` (stored SQL
  plus defaults plus output column names).
  pins: ice-views-1/C-001, C-002, C-003, C-004, C-005, C-010
- `tests.rs` — memory-catalog pins for every service row plus the
  `ViewlessCatalog` double (required `Catalog` methods delegate to memory, view
  methods inherit the `FeatureUnsupported` default) binding the A-9 CREATE
  rows with exact `UnsupportedMarker` downcast assertions.
  pins: ice-views-1/C-001, C-002, C-003, C-004, C-005, C-010

## Pointers

- Up: [../map.md](../map.md)
- Read path: [../../../repark-spark/src/view_ddl/map.md](../../../repark-spark/src/view_ddl/map.md)

## Debug

First checks: `cargo test -p repark-iceberg --lib view::`. Escalate to:
[../map.md#debug](../map.md).

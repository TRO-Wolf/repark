# map — repark-iceberg/src/catalog/session_tables

## Purpose

ICE-HELD-FRAME-SNAPSHOT-1 unit 1a (2026-11-20), seat A: the session's own record of each Iceberg table it has seen,
so a held frame can be re-bound at execution as Spark 4.1.2 re-binds it (`V2TableRefreshUtil`, through its
`CachingCatalog`), with no catalog call beyond the ones the session already makes. The parent
[../map.md](../map.md) is at its size ceiling, so the module is a directory with its own map.

## Contents

- `mod.rs` —
  - `SessionTables`, a session-config extension (`with_session_tables`, installed by the Spark door's
    `frame_settings::install`): `(catalog name, identifier) → (epoch, Live(Table) | Dropped)`. The epoch is a session
    counter taken at each change, so entries are ordered by what the session observed, never by another writer's
    `last-updated-ms` (Fable's Q2 change).
  - `session_catalog_provider`, called at `ReparkSession::register_iceberg_catalog_with_policy` before
    `build_iceberg_catalog_provider`: it wraps the registered handle in `SessionTablesCatalog` when the extension is
    present, so the fork schema provider's own `catalog` field (its `INSERT`, `DELETE`, `UPDATE`, `MERGE` execs) and the
    session registry (every RePark write door and `CALL` procedure) both commit through it (Fable's Q1 condition). It
    sits outside `EncryptionGuardCatalog`, which the builders apply first, so a guard refusal records nothing.
  - The decorator records what `update_table`, `create_table`, `register_table`, `publish_create_table` and
    `publish_replace_table` return, a drop marker on `drop_table`, a drop marker for the old name and no entry for the
    new one on `rename_table`, and every `load_table` (Q2 (b)). The race guard: a load reads the entry's epoch before
    it starts and records only if the epoch has not moved, so a load that began before a same-session commit cannot
    put the older table back. A plain reload of the same metadata location keeps the epoch; `refresh_load` (`REFRESH
    TABLE`, `catalog.refreshTable`) moves it even when nothing changed, as Spark re-caches.
  - `capturing_schema`, applied by `ReparkCatalogProvider::schema`: while the fork resolves a table, the decorator
    hands the planning load to the resolving schema provider (a poll-scoped slot, never across threads), and the
    provider object is registered in a process-wide binding table (`binding`, keyed by the provider's address with a
    weak reference, swept as it grows). `rebound_clone` gives a view a clone of the provider bound in view or command
    mode. `rebound_provider` builds the fork's public `IcebergTableProvider::try_new` over a one-table shim catalog that
    serves only the decided table, polled once (it completes without I/O), so the re-bound scan makes no catalog call
    (Fable's Q9 carrier).
  - **The frozen epoch signature** (Q8, for TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 and unit 2):
    `repark_iceberg::catalog::session_tables::table_epoch(config: &SessionConfig, catalog: &str, ident: &TableIdent)
    -> Option<u64>` (`None` when the session has no table state; `Some(0)` for a table it has not seen), and
    `SessionTables::epoch(&self, catalog: &str, ident: &TableIdent) -> u64`. It moves on every same-session commit, drop,
    create, rename, refresh, and on a load that sees a new metadata location.
  - Memory: one `Table` (its metadata `Arc`, shared with the metadata cache) per table the session has loaded, and one
    binding per live provider. The cross-read walk's resident size was not measured in this unit; the bound is the
    touched tables' metadata, which the map keeps alive past the metadata cache's eviction.
  - pins: ice-held-frame-snapshot-1/1a.
- `tests.rs` — over a memory catalog wrapped in a load-counting catalog: a commit through the decorated handle moves
  the state and the epoch; a load older than a commit does not put the older table back (the race guard); a plain
  reload keeps the epoch and a refresh load moves it; a drop records a drop marker; a provider resolved through the
  capturing schema carries its planning load; a provider built over the shim scans with no load on the counting
  catalog.

Up: [../map.md](../map.md).

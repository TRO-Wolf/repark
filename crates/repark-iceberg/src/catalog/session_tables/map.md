# map — repark-iceberg/src/catalog/session_tables

## Purpose

ICE-HELD-FRAME-SNAPSHOT-1 unit 1a (2026-11-20), seat A: the session's own record of each Iceberg table it has seen,
so a held frame can be re-bound at execution as Spark 4.1.2 re-binds it (`V2TableRefreshUtil`, through its
`CachingCatalog`), with no catalog call beyond the ones the session already makes. The parent
[../map.md](../map.md) is at its size ceiling, so the module is a directory with its own map.

## Contents

- `mod.rs` —
  - `SessionTables`, a session-config extension (`with_session_tables`, installed by the Spark door's
    `frame_settings::install`): `(catalog name, identifier) → (epoch, Live(HeldTable) | Dropped)`. The epoch is a session
    counter taken at each change, so entries are ordered by what the session observed, never by another writer's
    `last-updated-ms` (Fable's Q2 change). **Unit 2 delta 1 (F1):** each entry also carries a `LocalEpoch`
    (`local_epoch`): `changed`, the tick of the last same-session commit, drop, create, rename or refresh, and
    `refreshed`, the tick of the last refresh. A load that sees another writer's state moves the epoch and the state,
    and carries the local epoch unchanged; a new entry starts with `changed` at its tick. The lazy cache reads it, so
    a cached fill outlives another writer's commit as Spark's does, while held frames and fresh reads still see it.
  - `session_catalog_provider`, called at `ReparkSession::register_iceberg_catalog_with_policy` before
    `build_iceberg_catalog_provider`: it wraps the registered handle in `SessionTablesCatalog` when the extension is
    present and the handle is one `EncryptionGuardCatalog::install` returned, so the fork schema provider's own
    `catalog` field (its `INSERT`, `DELETE`, `UPDATE`, `MERGE` execs) and the session registry (every RePark write door
    and `CALL` procedure) both commit through it (Fable's Q1 condition). Any other handle registers undecorated, as on
    main; only test catalogs take that path, since every builder returns through the guard.
  - The decorator records what `update_table`, `create_table`, `register_table`, `publish_create_table` and
    `publish_replace_table` return, a drop marker on `drop_table`, a drop marker for the old name and no entry for the
    new one on `rename_table`, and every `load_table` (Q2 (b)). The race guard: a load reads the entry's epoch before
    it starts and records only if the epoch has not moved, so a load that began before a same-session commit cannot
    put the older table back. A plain reload of the same metadata location keeps the epoch; `refresh_load` (`REFRESH
    TABLE`, `catalog.refreshTable`) moves it even when nothing changed, as Spark re-caches.
  - `capturing_schema`, applied once where `ReparkCatalogProvider` registers each fork schema (`present_fork_schema`,
    `unlisted_namespace_schema`), never in `schema()`: a repeated `schema()` returns the registered `Arc`, and a schema
    registered from outside (a memory schema under an Iceberg catalog, the Spark door's view wrapper) comes back as
    registered. Round 0 wrapped on every `schema()` call; the Spark door's `ensure_view_wrappers` compares the installed
    view wrapper with `Arc::ptr_eq`, so each call re-wrapped and re-registered one more `ViewSchemaProvider` and
    `CapturingSchema` layer, every Iceberg table reference walked all of them, and C-012's `|ice|` cells grew from
    about 1 ms to about 50 ms over a session (fix round 1, F0 and F1). While the fork resolves a table, the decorator
    hands the planning load to the resolving schema provider (a poll-scoped slot, never across threads), and the
    provider object is registered in a process-wide binding table (`binding`, keyed by the provider's address with a
    weak reference, swept as it grows). `rebound_clone` gives a view a clone of the provider bound in view or command
    mode. `referenced_clone` (delta 2) gives a frame built over a view, at its build, a clone bound in query mode to the
    table the session holds then: the view's provider when the state is the view's, else one built on that table
    through the shim when its schema is the view's (otherwise the view's binding stays), as Spark analyses a frame over
    a view. `rebound_provider` builds the fork's public `IcebergTableProvider::try_new` over a one-table shim catalog that
    serves only the decided table, polled once (it completes without I/O), so the re-bound scan makes no catalog call
    (Fable's Q9 carrier).
  - **The frozen epoch signature** (Q8, for TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 and unit 2):
    `repark_iceberg::catalog::session_tables::table_epoch(config: &SessionConfig, catalog: &str, ident: &TableIdent)
    -> Option<u64>` (`None` when the session has no table state; `Some(0)` for a table it has not seen), and
    `SessionTables::epoch(&self, catalog: &str, ident: &TableIdent) -> u64`. It moves on every same-session commit, drop,
    create, rename, refresh, and on a load that sees a new metadata location; the delta leaves it as it was.
  - `HeldTable` is the only table type the map, the bindings, the own-plan memo and `OneTableCatalog` hold. Its field is
    private to this module, and the module mints one only from what the decorator's guarded handle returned.
  - Memory: one `Table` (its metadata `Arc`, shared with the metadata cache) per table the session has loaded, and one
    binding per live provider. The cross-read walk's resident size was not measured in this unit; the bound is the
    touched tables' metadata, which the map keeps alive past the metadata cache's eviction.
  - pins: ice-held-frame-snapshot-1/1a.
- **ENC-1 (owner, 2026-11-20): `SessionTablesCatalog` and `OneTableCatalog` are on the `impl Catalog for` allowlist of
  `tests/enc_1_gate.rs`, on three conditions.**
  1. **They wrap the guarded handle only.** `SessionTablesCatalog` holds a `GuardedCatalog`, which only
     `EncryptionGuardCatalog::guarded` mints, and only for a live handle `install` returned (the guard keeps a weak
     list of what it installed; an address match against a weak entry is the same allocation, since a weak reference
     keeps it reserved). `OneTableCatalog` holds a `HeldTable`. Test:
     `a_handle_the_guard_did_not_return_is_never_wrapped` (a raw catalog and the decorated handle itself both register
     undecorated, and the map stays empty).
  2. **A refused commit records nothing.** Every recording follows a successful inner call (`?` first). Tests, one per
     refusal the guard makes: `a_refused_snapshot_or_statistics_commit_records_nothing`,
     `a_refused_commit_on_a_handle_loaded_before_the_key_records_nothing`, `a_refused_file_write_records_nothing`,
     `a_refused_staged_create_or_replace_publish_records_nothing`; each holds the epoch, the local epoch, the entry and
     the entry count.
  3. **A map-served table behaves as a fresh guarded load.** The entry is what the guard returned, so a keyed one
     carries the guarded `FileIO`. Tests: `a_map_served_keyed_table_refuses_as_a_fresh_guarded_load_does` (the same
     refusal text from the map entry, from the shim and from a fresh load, on a data file and on a snapshot commit; the
     shim itself commits nothing) and `a_key_added_or_removed_by_a_same_session_commit_reaches_the_map_as_a_fresh_load`.
     The facade cells are the `enc_1` pairs of `python/repark/tests/iceberg/held_frame/`.
- `tests.rs` — over a memory catalog wrapped in a load-counting catalog under the guard: a commit through the decorated
  handle moves the state and the epoch; a load older than a commit does not put the older table back (the race
  guard); a plain reload keeps the epoch and a refresh load moves it; a load of another writer's commit (through the
  guard's handle) moves the epoch and not the local epoch, a refresh moves both local ticks and an own commit only
  `changed`; a drop records a drop marker; a provider resolved
  through the capturing schema carries its planning load; a provider built over the shim scans with no load on the
  counting catalog; and the ENC-1 tests above.

Up: [../map.md](../map.md).

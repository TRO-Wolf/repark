# ICE-HELD-FRAME-SNAPSHOT-1 — the insertion inventory (the owner's rule of 2026-11-09)

Every place a RePark Iceberg frame binds its table, its snapshot or its provider, with file:line on main `0436fdf9`
(product tree `52f468ce`) and the owned fork at the pinned rev `b6e578dc` (`fork:` below is
`crates/integrations/datafusion/src/` of that rev). "Binds" means the place fixes which `Table` (metadata, snapshot,
schema) a later scan reads. Read, not built; the cells that show each binding are in
[domain-table.md](domain-table.md).

## 1. The one binding that makes the defect: the planning load

| # | site | what it binds | when |
|---|---|---|---|
| B1 | fork `schema.rs:146-163` `IcebergSchemaProvider::resolve_table` | `catalog.load_table(ident)` (`:154-158`), then `IcebergTableProvider::from_planning_load(catalog, table)` (`:159`): a new provider per planning round | when DataFusion resolves the name: `spark.table`, `read.table`, `spark.sql`, a SQL view's re-plan, a fresh frame |
| B2 | fork `table/loaded.rs:31-49` `from_planning_load` | stores the loaded `Table` in `planning_table: Some(table)` (`table/mod.rs:81`) and fixes the advertised `schema` from its current schema | at B1 |
| B3 | fork `table/mod.rs:190-198` `IcebergTableProvider::scan` → `table/loaded.rs:52-61` `planning_table_or_load` | **every scan of that provider reads `planning_table`**; only a provider without one (`try_new`, `refreshed`) loads at scan | every physical planning of a plan that holds the provider: each `count`, `collect`, `show`, write, SQL over a DataFrame view |
| B4 | fork `table/mod.rs:244` `insert_into` (and the DML execs) → `loaded.rs:64-69` `load_table_with_current_schema` | writes load the **current** table, not `planning_table` | at a write's planning |

A held DataFrame is a DataFusion `LogicalPlan` whose `TableScan` holds the B1 provider object. Every action
re-optimises and re-plans that same `LogicalPlan`, so B3 hands each action the B1 snapshot, forever. A write (B4)
commits against the current table and never touches a provider already held. That is the whole mechanism: main and
main-before-unit-1 bind identically (domain table: base equals main on every cell).

**Correction to the record of the step 0 that found it (Fable §13, "a commit re-registers the namespace's
providers and a held plan keeps the old one"):** the re-registration (§4) replaces only the namespace's name
directory. No provider is cached anywhere: B1 builds a fresh one for every planning round. The pin is B2/B3, the
fork's planning fast path ("`loaded.rs` — `from_planning_load`, planning fast path", fork `table/map.md:39`), which
saves the second `load_table` a fresh query would otherwise pay at scan. It is not catalog-specific: Glue and S3 Tables
reach B1 to B3 through the same schema provider.

## 2. The catalog provider and its registration

| # | site | role |
|---|---|---|
| R1 | `crates/repark-core/src/session.rs:458-482` `register_iceberg_catalog_with_policy` | builds the provider (`:472`, `build_iceberg_catalog_provider`) and stores the `Arc<dyn Catalog>` handle in the session registry (`:480`): the one handle every door reads and writes through |
| R2 | `crates/repark-iceberg/src/catalog/provider.rs:24-30`, `:35-42` `ReparkCatalogProvider` | namespace → fork schema provider map; no table cache |
| R3 | `provider.rs:106-112` `CatalogProvider::schema` | hands DataFusion the fork schema provider, whose `table()` (fork `schema.rs:246-275`) is B1 |
| R4 | `provider.rs:373-386` `present_fork_schema` / `freeze_fork_name_directory` | lists names once at snapshot time; binds names only |
| R5 | `crates/repark-iceberg/src/catalog/encryption_guard.rs:46`, `:327` `EncryptionGuardCatalog` | the existing `Catalog` decorator over the session handle: the precedent for seat A's decorator (§7) |
| R6 | fork `crates/iceberg/src/catalog/memory/catalog.rs:185-190`, `:444-448` (`type=hadoop`) | `load_table` reads the in-process pointer (`root_namespace_state`), then the metadata (cached by location): no I/O for the pointer; a second session or process is never seen (domain table §6) |
| R7 | fork `crates/catalog/glue/src/catalog.rs:683-686` (`load_table_with_version_id`, `get_table()` at `:748`, `:786`) | Glue: one `GetTable` per `load_table`, then the metadata by location from the session cache |
| R8 | fork `crates/catalog/s3tables/src/catalog.rs:757-759` (`load_table_with_version_token`, `:412`) | S3 Tables: one metadata-location call per `load_table`, then the metadata by location |

## 3. The facade doors that hold a table

| # | door | site | binds through |
|---|---|---|---|
| F1 | `spark.table(t)` | `python/repark/src/repark/spark/session/session_core.py:964` → `python/repark/src/repark/spark/catalog_surface.py:340-366` `session_table` (`:366` records the frame in `_frame_identities`: the one place a bare `spark.table`/`read.table` frame is known as bare) | `inner.sql_built("SELECT * FROM t")` (`crates/repark-python/src/session.rs:202-215` → `crates/repark-core/src/session/write_options.rs:66-77`): B1 at the call |
| F2 | `spark.read.table(t)` | `session/reader.py:368-386` | no travel option: F1 (`:386`); a travel option: §5 T2 |
| F3 | `spark.sql(...)` | `session_core.py:149` → the native SQL planner | B1 for every Iceberg name in the statement, at the call |
| F4 | the path read `format("iceberg").load(path)` | `session/reader.py:468-472` → `reader_iceberg_path.py:25` → `crates/repark-core/src/iceberg_path.rs:41` | `IcebergStaticTableProvider::try_new_from_table`: a static provider, pinned at load by design (Spark refreshes it; domain table §2 `path`) |
| F5 | a derived frame (`filter`, `select`, `groupBy`, `join`) | the DataFusion `LogicalPlan` builders | inherits the parent's B1 provider object; no new binding |
| F6 | `DataFrame.cache()` / `persist` | `dataframe/core.py:701`, `:314` `_materialize_cache_if_needed` | materialises the B1 snapshot at the first action into a `MemTable`; held thereafter (§6) |

## 4. The write doors' commit and the re-registration that follows

Every Spark-door write commits through the R1 handle, by the fork's write and DML execs (B4) or RePark's write module
(`crates/repark-iceberg/src/write/append.rs`, `alter.rs`, `staged_publish.rs`, `table_admin.rs`), and then calls
`reregister` (`crates/repark-spark/src/catalog_ops.rs:266-275` → `reregister_namespaces` `:278-291` →
`provider.rs:218-251` `invalidate_catalog_namespaces_after_commit` → `:179-215` `invalidate_catalog_namespaces`). That
rebuilds the namespace's name directory with no table load. The sites (30 calls in 24 files), by door:

| door | re-registration site |
|---|---|
| `INSERT OVERWRITE` (static, dynamic, `replaceWhere`) | `crates/repark-spark/src/insert_overwrite.rs:175`, `:295`, `:536`; `router/insert_positional/replace_where.rs` |
| `INSERT` by name, with schema evolution | `insert_by_name.rs:205`, `:288`; `insert_by_name/evolution.rs` |
| `writeTo`/`saveAsTable` append with options | `append_with_options.rs:111` |
| CTAS, RTAS, `writeTo().create/replace/createOrReplace` | `ctas.rs:322`, `:751`; `create_table.rs:613` |
| `ALTER TABLE` (columns, rename, properties, write order, nested) | `alter.rs:133`, `:172`, `:742`, `:763`; `column_move.rs`; `nested_column_ddl.rs`; `table_props_ddl.rs`; `router/table_props_ddl.rs`; `router/hive_change_column.rs`; `alter_write_order.rs` |
| branch and tag DDL, `CALL` procedures | `ref_ddl.rs`; `call.rs` (4); `call/branch_ops.rs`; `call/add_files.rs`; `call/rewrite_data_files.rs`; `call/rewrite_manifests.rs`; `call/compute_table_stats.rs`; `call/compute_partition_stats.rs` |
| `TRUNCATE`, namespace DDL | `truncate.rs:53`; `namespace_ddl.rs` (3) |
| `DELETE`, `UPDATE`, `MERGE`, plain `INSERT` | no re-registration: `router/delete_update.rs:11`, `:60`; `merge.rs:46-81`; the fork's `insert_into` (B4). Names do not change. |

None of these touches a held provider, a held frame, a DataFrame view's held plan or a cached frame. **For seat A
this list is not the insertion list**: every one of them commits through the R1 handle's `Catalog` methods
(`update_table`, `publish_create_table`, `publish_replace_table`, `create_table`, `drop_table`, `rename_table`,
`register_table`), so one decorator over that handle sees them all (§7).

## 5. The bindings that must stay pinned (time travel, refs, metadata tables)

| # | door | site | binding |
|---|---|---|---|
| T1 | SQL `VERSION AS OF`, `TIMESTAMP AS OF`, `VERSION AS OF '<ref>'` | `crates/repark-spark/src/time_travel.rs:145`, `:149` | `IcebergStaticTableProvider::try_new_from_table_ref` / `_snapshot`: resolved at analysis, static |
| T2 | reader `versionAsOf`, `timestampAsOf`, `branch` options | `crates/repark-core/src/time_travel.rs:352`, `:359` | the same static providers |
| T3 | `t.branch_b`, `t.tag_g` identifiers | the facade's identifier path `crates/repark-core/src/time_travel.rs:139-142` (`RefSelector`), and `crates/repark-spark/src/time_travel.rs:434`, `:494-495` (the `branch_`/`tag_` suffix), then the T1 constructors | static at analysis; Spark refreshes the identifier form (domain table §5) |
| T4 | metadata tables `t.snapshots`, `t.files`, `t.history` | fork `schema.rs:250-262` → `table/mod.rs:166-173` `metadata_table` | `load_table` at resolve, then `IcebergMetadataTableProvider` over that `Table`: pinned at B1's moment |
| T5 | write-audit-publish reads | `crates/repark-spark/src/wap.rs:220` | static by snapshot id |

## 6. Temp views and the cache's freshness check (C-009)

| # | site | binding |
|---|---|---|
| V1 | `df.createOrReplaceTempView(v)`: `python/repark/src/repark/spark/catalog_surface.py:320-331` `_register_temp_view` → `crates/repark-python/src/session.rs:362` → `crates/repark-core/src/session/temp_views.rs:45-73` → `crates/repark-spark/src/view_ddl/frame_settings.rs:156`, `:261` `frame_view` (`FrameView.plan`; `:255` `ViewTable::new(self.plan.clone())`) | the frame's `LogicalPlan`, so the B1 provider; every SQL over `v` re-plans it and B3 reads the old snapshot |
| V2 | SQL `CREATE TEMPORARY VIEW v AS SELECT`: `crates/repark-spark/src/view_ddl/temp_view.rs:120-151` (`TempViewProvider::scan`) | **re-plans the definition at every scan** (`plan_definition`, `:142-147`), so B1 runs afresh: EQUAL in every cell |
| V3 | Iceberg catalog views: `crates/repark-spark/src/view_ddl/read.rs:117-147` | re-expanded at each resolution |
| C1 | C-009 identity token: `python/repark/src/repark/spark/catalog_surface.py:233-243` `_cache_identity_token` (`current-snapshot-id` via `DESCRIBE TABLE EXTENDED`), `:246-251`, `:254-276` `_drop_stale_identity_frames`, `:278-292` `_note_frame_cached` | compared only inside `session_table` (`:350-363`) and `isCached`: a new `spark.table(t)` or `isCached` evicts a stale cache. The held cached frame's own `count` is never checked (domain table §7 `C`) |
| C2 | `dataframe/cache_handle.py:102` `bind_registered_view` | records the token baseline at materialise time |
| C3 | unit 1's own-plan memo: `python/repark/src/repark/spark/dataframe/surface_a.py:51`, `:399-413` (`_PARSE_MEMOS`, `_parse_memo`, `_own_plan`), `crates/repark-core/src/extension.rs:84-136` `ReaderParseMemo` | the per-frame-object memo entered around every own-plan action: the seat both proposals reuse for Spark's per-`QueryExecution` pin |
| C4 | what that memo holds today: `ReaderParseMemo { decided: Mutex<Vec<(LiveOptions, Arc<ConfigOptions>)>> }` (`crates/repark-core/src/extension.rs:84-86`), reader options only | **no table decision**: either seat adds a slot for `(catalog, ident) → Table` decisions; the decision is taken in a **synchronous** `OptimizerRule` (`ReaderParseRule`, `crates/repark-spark/src/view_ddl/frame_settings.rs:425`) |

## 6b. Sites added by the revision (the critic's round, 2026-11-20)

| # | site | binding or role |
|---|---|---|
| K1 | fork `table/mod.rs:118-137` `IcebergTableProvider::refreshed(&self)` | **public**: a new provider over a fresh `load_table` through the provider's own catalog, with the current schema, `planning_table: None`, the flags (`uuid_as_string`, commit branch, properties, output spec) kept: the hook of the fork-free wrapper (sketch §4) |
| K2 | `python/repark/src/repark/spark/catalog.py:768-777` `refreshTable` → `python/repark/src/repark/spark/catalog_surface.py:731-747` `refresh_table` | unpersists and rebuilds the catalog provider (`refresh_catalog_provider`); touches no held frame: an epoch source for seat A |
| K3 | SQL `REFRESH TABLE`: `crates/repark-spark/src/router.rs:600-612` (`try_parse_refresh`) | the same, by SQL: an epoch source |
| K4 | `CALL` procedures: `crates/repark-spark/src/call/branch_ops.rs:190`, `:241`, `:279`, `:354`, `call/rewrite_manifests.rs:80` (`tx.commit(catalog)` through the handle passed in), `call.rs:266` `execute_expire_snapshots` | commit through the R1 handle, so seat A's decorator sees them; `expire_snapshots` deletes files a held old snapshot still names, which is main's `Failed to load manifest list` RAISE |
| K5 | the frame write doors: `python/repark/src/repark/spark/dataframe/core.py:2916` `writeTo`; `dataframe/writer_readwriter.py:194` `saveAsTable`, `:215` `insertInto`, `:797` `create`, `:813` `createOrReplace`, `:851` `append` (998 lines, under a 1,000-line ceiling) | each plans the held frame's own `LogicalPlan` as the write's input: where Spark's bare/derived split (sketch §1 point 7) must be applied |

## 7. What each seat must touch (cross-reference to [sketch.md](sketch.md) §4, revised)

- **Both seats:** one synchronous rebinding rule for every catalog-backed Iceberg scan at optimisation (B3's
  consumer), keyed by `(catalog, ident)` once per plan, skipping T1, T2 and T5 static providers, re-binding T3's
  identifier refs and T4's metadata tables; a new table-decision slot in C4's memo for own-plan actions; Spark's
  identity and column checks, with a command scope entered by the K5 write doors (`PROHIBIT_CHANGES`); the bare-frame
  re-plan: the native frame carries its name (F1, F2) or SQL text (F3), and the `writeTo` entry of K5 re-plans a frame
  that has one; V1 needs nothing more (its scan re-plans through the same rule); F4 becomes catalog-backed or stays a
  named divergence (Q4).
- **Seat A only:** a decorator over the R1 handle (beside R5) that records what every same-session commit returns
  (inventory §4 and K4 included), a writer at K2 and K3 that loads the table once and records it, and the per-table
  epoch A1's lazy table and C1 compare. With the fork-free wrapper (Q9 (a)) the re-bound provider comes from K1
  through the decorator in a map-serving mode; with Q9 (b), from two fork constructors.
- **Seat B only:** a `load_table` at the rebinding point (R6 to R8 costs), behind a `block_on` in the synchronous rule
  or in the async wrapper; nothing at the commit doors.

Up: [map.md](map.md).

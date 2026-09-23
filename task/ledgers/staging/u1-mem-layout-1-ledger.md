# Unit ledger — U1 MEM-LAYOUT-1 · memory-catalog default create location `<warehouse>/<ns>/<table>` (Spark dialect)

**Unit:** U1 MEM-LAYOUT-1, work order layout-r1 · **Date:** 2026-09-23 · **Branch:** `fix/mem-catalog-layout` · **Base:** `88b6f59f`
**Model:** Claude Opus 5.5 (claude-opus-5-5) — delegated implementation tier
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Ruling.** Q-55-7 (ii) (orchestrating session, 2026-09-22; spec
`v1-5-0-remainder-spec-2026-09-23.md` §U1). Q-55-6: a table's own subdirectory is sweepable, the
warehouse root is not.

**Design (orchestrator).** No new `LocationPolicy` variant, and `TempFallbackAllowed` keeps its
meaning. `CatalogRegistry` records a warehouse layout root per catalog entry.
`register_memory_catalog`, which the configured `type=memory|hadoop` path also calls, sets that
root to `memory_warehouse_fallback_root(warehouse)`, the same value the policy carries. The
Spark create resolver applies this precedence: explicit `LOCATION` > namespace `location` /
`location_uri` > recorded layout root → `<root>/<ns…>/<table>` > policy match (the
`repark_ctas` arm is unchanged).

**Not in this step:** the ANSI door (`repark_ansi_ctas`, no measured oracle), `call.rs`,
`view_ddl/`, the Python tests that hard-code `repark_ctas` (a separate follow-up round),
`v<N>.metadata.json` names (a follow-up), `map.md`, `STATUS.md`.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

## Before — U1 MEM-LAYOUT inventory (pasted verbatim from `/tmp/xo-xo-opus60/wo/u1-inventory.md`)

### U1 MEM-LAYOUT inventory (before), 2026-09-23 06:5x — Spark leg sb/out, RePark scoreboard 2026-09-23

| cell | Spark status | Spark location/metadata strings | RePark status | RePark strings |
|---|---|---|---|---|
| D-SHOW-CREATE-PLAIN | ok | <wh>/ns/t_d_show_create_plain | error |  |
| D-SHOW-CREATE | ok | <wh>/ns/t_d_show_create | error |  |
| R-MT-ENTRIES-SCHEMA | ok |  | ok |  |
| V-SHOW-CREATE | ok | <wh>/ns/vw_t_v_show_create | error |  |
| P-ORPHAN-DEFAULT | ok | file:<wh>/hc/ns/t_p_orphan_default/data/orphan-file.parquet | error |  |
| P-ORPHAN-DRY-RUN | ok | file:<wh>/hc/ns/t_p_orphan_dry_run/data/orphan-file.parquet | error |  |
| P-ORPHAN-LOCATION | ok | file:<wh>/hc/ns/t_p_orphan_location/data/orphan-file.parquet | error |  |
| P-ORPHAN-MAX-CONCURRENT | ok | file:<wh>/hc/ns/t_p_orphan_max_concurrent/data/orphan-file.parquet | error |  |
| P-ORPHAN-PREFIX-MODE | ok | file:<wh>/hc/ns/t_p_orphan_prefix_mode/data/orphan-file.parquet | error |  |
| P-ORPHAN-EQUAL-SCHEMES | ok | file:<wh>/hc/ns/t_p_orphan_equal_schemes/data/orphan-file.parquet | error |  |
| P-ORPHAN-PREFIX-LISTING | ok | file:<wh>/hc/ns/t_p_orphan_prefix_listing/data/orphan-file.parquet | error |  |
| P-ORPHAN-STREAM-RESULTS | ok | file:<wh>/hc/ns/t_p_orphan_stream_results/data/orphan-file.parquet | error |  |
| P-ORPHAN-FILE-LIST-VIEW | ok | <wh>/hc/ns/t_p_orphan_file_list_view/data/orphan-file.parquet | error |  |
| P-TABLE-STATS-DEFAULT | ok | <wh>/ns/t_p_table_stats_default/metadata/5403172835552297315-53d9ff31-0b4e-482d-a91e-aafe7921c29e.stats | ok | <wh>/repark_ctas/sc/ns/t_p_table_stats_default/metadata/8001141607003362924-a4f0abb9-b4a2-45cc-b107-7c469a44bcd7.stats |
| P-TABLE-STATS-SNAPSHOT | ok | <wh>/ns/t_p_table_stats_snapshot/metadata/2215583112995696087-bcaea824-9deb-45e4-ae00-dfa58f523ea3.stats | ok | <wh>/repark_ctas/sc/ns/t_p_table_stats_snapshot/metadata/2016010246163229127-2be84d7a-b2d5-4945-839c-2d2bb6be0cb7.stats |
| P-TABLE-STATS-COLUMNS | ok | <wh>/ns/t_p_table_stats_columns/metadata/2596545206281803208-992764a4-af10-496b-ace6-2e057cea4f6c.stats | ok | <wh>/repark_ctas/sc/ns/t_p_table_stats_columns/metadata/3173727125905441564-2685682a-8fde-43da-a652-cdfbc92c5186.stats |
| P-PART-STATS-DEFAULT | ok | <wh>/ns/t_p_part_stats_default/metadata/partition-stats-7853456528967498394-64109cc3-bd7d-4167-b407-cae5b4498254.parquet | ok | <wh>/repark_ctas/sc/ns/t_p_part_stats_default/metadata/partition-stats-5123052776826061068-12c09cdc-d0c5-49bc-bada-eb339a881eed.parquet |
| P-PART-STATS-SNAPSHOT | ok | <wh>/ns/t_p_part_stats_snapshot/metadata/partition-stats-7736687660218246162-20661f5c-c56f-49c8-8544-ae8c1d0b1c36.parquet | ok | <wh>/repark_ctas/sc/ns/t_p_part_stats_snapshot/metadata/partition-stats-1827288786059034678-46408714-95c7-4a81-b699-f860766624eb.parquet |
| P-RTP-DEFAULT | ok | <wh>/hc/ns/t_p_rtp_default/metadata/copy-table-staging-24d1256f-c15f-4fa3-a7c8-4a279fd91158/file-list<br>v4.metadata.json | ok | 00003-1f670d08-d840-4a15-8add-2ce2335fe181.metadata.json<br><wh>/hc/repark_ctas/hc/ns/t_p_rtp_default/metadata/copy-table-staging-18d7e867089a05cf-3457ee/file-list |
| P-RTP-STAGING | ok | <wh>/rtp-staging/t_p_rtp_staging/file-list<br>v4.metadata.json | ok | 00003-821d2ade-b457-4402-a9b9-80fd388047f3.metadata.json<br><wh>/rtp-staging/t_p_rtp_staging/file-list |
| P-RTP-NO-FILE-LIST | ok | v4.metadata.json | ok | 00003-f7dc4501-faa4-494d-9920-fcedf8b6bcec.metadata.json |

## Create paths that reach the location resolver

Every Spark-dialect create path that can reach the `repark_ctas` fallback goes through
`resolve_create_plan_for` (`crates/repark-spark/src/ctas.rs:443`). So every one of them now takes
the new layout.

| Path | Entry | Hop into the resolver | New layout |
|---|---|---|---|
| `CREATE TABLE … AS SELECT` (CTAS) | `crates/repark-spark/src/router.rs:320` → `ctas.rs::execute_ctas` | `ctas.rs:195` `resolve_create_plan` → `ctas.rs:428` | yes |
| `CREATE OR REPLACE TABLE … AS SELECT`, table absent | same, `existed == false` | `ctas.rs:195` | yes |
| `CREATE OR REPLACE` / `REPLACE`, table present | `ctas.rs` `CtasMode::Replace`; `create_table.rs:448` | keeps the existing table location; the resolver is not reached | n/a |
| Column-def `CREATE TABLE` | `crates/repark-spark/src/create_table.rs` | `create_table.rs:492` | yes |
| `df.write.saveAsTable` | `python/repark/src/repark/spark/dataframe/writer_readwriter.py:194` | CTAS SQL text → router → `ctas.rs:195` | yes |
| `df.writeTo(t).create()` / `.createOrReplace()` / `.replace()` | `writer_readwriter.py:870/886/896` | CTAS SQL text → router → `ctas.rs:195` | yes |
| `spark.catalog.createTable` / `createExternalTable` | `python/repark/src/repark/spark/catalog.py:736/761` → `catalog_surface.py:573` | column-def DDL → `create_table.rs:492` | yes |
| Service-managed (S3 Tables) create | `ctas.rs:190`, `create_table.rs:476` | routed before the resolver | n/a |
| ANSI door | `crates/repark-sql/src/create_table.rs:373` (`repark_ansi_ctas`) | own resolver | unchanged (out of scope) |
| `testing_oob_create_table` (`#[doc(hidden)]`, test only) | `crates/repark-core/src/session.rs:~634` | composes its own location | n/a |

## PROPOSITION LEDGER — U1 MEM-LAYOUT-1 — 2026-09-23

Every pin below was proven red one mutation at a time (the harness applies a mutation, runs the
named pin, and restores the file byte for byte), then green on the restored tree.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `register_memory_catalog` and the configured `spark.sql.catalog.<c>.type=memory` path both keep `TempFallbackAllowed { root = warehouse }`, and both record a warehouse layout root equal to the warehouse. | `crates/repark-core/src/session/tests/session.rs::register_memory_catalog_fallback_root_is_the_warehouse`, `::configured_memory_catalog_fallback_root_is_the_warehouse` | **PROVEN** | M1 (delete the `set_warehouse_layout_root` call in `register_memory_catalog`): both pins FAILED (`0 passed; 2 failed`). Green on the restored tree: `cargo test -p repark-core --lib session` → `171 passed; 0 failed`. |
| C-002 | A memory catalog, a namespace created with no location, and a CTAS `ns.t` put the table at `<wh>/ns/t`, with metadata and data files under it and nothing under `<wh>/repark_ctas`. The same holds for a column-def CREATE and for the rewritten A13 door pin. | `crates/repark-spark/src/tests/mem_layout.rs::mem_layout_ctas_lands_at_warehouse_namespace_table`, `::mem_layout_column_def_create_lands_at_warehouse_namespace_table`, `crates/repark-spark/src/tests/ctas.rs::register_memory_catalog_location_less_ctas_lands_under_warehouse` | **PROVEN** | M2 (the resolver call site passes `None` instead of the recorded root): all three FAILED. M1: `mem_layout_ctas_lands…` FAILED. |
| C-003 | An explicit table `LOCATION` wins over the recorded layout root. | `mem_layout.rs::mem_layout_explicit_location_wins` | **PROVEN** | M3 (ignore `custom_location` when a layout root is recorded): FAILED. |
| C-004 | A namespace created with a `location` property wins over the recorded layout root. | `mem_layout.rs::mem_layout_namespace_location_wins` | **PROVEN** | M4 (move the layout-root branch above the namespace-location check): FAILED. |
| C-005 | A multi-level namespace `a.b` nests each level: `<wh>/a/b/t`. | `mem_layout.rs::mem_layout_multi_level_namespace_nests_each_level` | **PROVEN** | M5 (push only the last namespace level): FAILED. |
| C-006 | A `TempFallbackAllowed` entry with NO recorded layout root (`CatalogRegistry::from`) still resolves exactly `<root>/repark_ctas/<cat>/<ns>/<t>`. | `mem_layout.rs::mem_layout_unrecorded_temp_fallback_keeps_repark_ctas_path` | **PROVEN** | M6 (`CatalogRegistry::insert` records a layout root for every entry): FAILED. |
| C-007 | Path-escape identifiers (`..`, `/`) are refused before any path is built, even with a recorded layout root. | `mem_layout.rs::mem_layout_refuses_path_escape_identifiers` | **PROVEN** | M7 (move the layout-root branch above the `reject_path_escape_ident` calls): FAILED. |
| C-008 | A `file:///…` warehouse gives the same plain local location as the bare path, end to end. A `file:/…` root, normalised by `memory_warehouse_fallback_root`, gives the plain path through the registry → resolver seam. `register_memory_catalog` itself refuses a `file:/…` warehouse as a malformed location. That refusal predates this unit and is unchanged. | `mem_layout.rs::mem_layout_file_uri_warehouse_gives_the_plain_location`, `::mem_layout_single_slash_file_warehouse_root_gives_the_plain_location` | **PROVEN** | M8 (record `PathBuf::from(warehouse)` instead of the normalised root): FAILED. M9 (do not strip `file:`): FAILED. |
| C-009 | The Q-55-6 guard pins stay green, and their files are untouched. | `cargo test -p repark-spark --lib call_orphan` | **PROVEN** | `14 passed; 0 failed`. `git diff 88b6f59f -- crates/repark-spark/src/call.rs crates/repark-spark/src/call crates/repark-spark/src/tests/call_orphan.rs crates/repark-spark/src/view_ddl crates/repark-sql/src/create_table.rs` is empty. |
| C-010 | Every Spark-dialect create path that could reach the `repark_ctas` fallback reaches it only through `resolve_create_plan_for`, so all of them take the new layout. | The table above; grep `resolve_create_plan_for`, `resolve_table_create_location`, `repark_ctas` | **PROVEN** | The resolver has one caller (`ctas.rs:473`). `resolve_create_plan_for` has two (`ctas.rs:428`, `create_table.rs:492`). The Python writer and catalog helpers build SQL text that reaches those two. |
| C-011 | Two memory catalogs registered on the SAME warehouse, both `CREATE TABLE ns.t`, share `<wh>/ns/t`. Ruled 2026-09-23 (orchestrator, `u1-cotenancy-guard.md` with the tick-8 addendum): the layout stays, and under `TempFallbackAllowed` `remove_orphan_files` refuses a scan path that holds another table's files. It has two rules. (a) Before listing and before a `file_list_view`, every `*.metadata.json` under the scan path at any depth, other than the swept table's current metadata file and its `metadata_log` files, is read. A different `table-uuid` refuses, and so does an unreadable file. The same uuid does not refuse. (b) In the catalog walk, a scan path inside another registered table's location and outside the swept table's own refuses. Both rules compare through `normalize_orphan_scan_path` and component-wise `Path::starts_with`. | `crates/repark-spark/src/call/remove_orphan_files.rs::refuse_scan_over_foreign_metadata`, `::refuse_scan_over_other_tables`; C-012 to C-020 | **RULED** (guard landed) | The pins below. See "Two catalogs, one warehouse". |
| C-012 | Two memory catalogs `m1` and `m2` on one warehouse each create `ns.t` at `<wh>/ns/t` and insert a row. `CALL m1.system.remove_orphan_files(table => 'ns.t')` refuses and names `m2`'s first metadata file in sorted order. The bare path, `file:///` and `file:/` scan locations give the same refusal. The planted orphan and both rows survive. | `crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_on_one_warehouse_refuse` | **PROVEN** | M-a (the probe returns `Ok` at once): FAILED. |
| C-013 | Two sessions each register catalog `ice` on one warehouse. The same outcome as C-012. | `call_orphan_cotenancy.rs::call_orphan_cotenancy_two_sessions_with_one_catalog_name_refuse` | **PROVEN** | M-a: FAILED. |
| C-014 | `location => '<wh>/ns/b/data'` swept as `ns.a` (`b` in the same catalog, its files aged 10 days) refuses and names `ice.ns.b`. `b`'s live file and row survive. | `call_orphan_cotenancy.rs::call_orphan_cotenancy_location_inside_another_table_refuses` | **PROVEN** | M-b (the inside-another-table comparison is dropped): FAILED. |
| C-015 | A copy of the swept table's own logged metadata file, put in its `data/` dir and aged, is listed and deleted as an orphan. The logged file and the row survive. | `call_orphan_cotenancy.rs::call_orphan_cotenancy_own_history_metadata_copy_is_swept` | **PROVEN** | M-uuid (a same-uuid file refuses): FAILED. |
| C-016 | A stray `metadata/stray.json` that is not table metadata is not probed. It is swept as an orphan. | `call_orphan_cotenancy.rs::call_orphan_cotenancy_stray_non_metadata_file_is_swept` | **PROVEN** | M-filter (probe every listed file): FAILED, along with 34 other `call_orphan` pins. |
| C-017 | A single catalog on the warehouse, `ns.t` at `<wh>/ns/t` with two snapshots: the default sweep lists and deletes the planted orphan, and both rows survive (P-ORPHAN-DEFAULT shape). | `call_orphan_cotenancy.rs::call_orphan_cotenancy_single_catalog_default_sweep_deletes_the_orphan` | **PROVEN** | M-naive (every `*.metadata.json` under the scan refuses, own files included): FAILED, along with 31 other `call_orphan` pins. |
| C-018 | Nested namespace `a.t` holds `x` at `<wh>/a/t/x`, and `a.y` sits at `<wh>/a/y`. Scan `<wh>/a/t/x/data` passes all three guards, both with no table `a.t` and after a table `a.t` is created at `<wh>/a/t`. A `CALL` cannot name `a.t.x` (`resolve_table_ident` takes one namespace level), so this pin calls the guards directly. Through `CALL`, a table `o.x` with `LOCATION '<wh>/a/t/x'` inside table `a.t` sweeps its own `data` dir. The host table's live file and both tables' rows survive. | `call_orphan_cotenancy.rs::call_orphan_cotenancy_nested_namespace_table_data_dir_passes_the_guards`, `::call_orphan_cotenancy_table_located_inside_another_table_sweeps_its_data_dir` | **PROVEN** | M-unless (the own-location exception is dropped): both FAILED. |
| C-019 | The `file:///` and `file:/` spellings of the scan location give the bare path's verdict for C-012 and C-014. | the spellings loops in C-012, C-013 and C-014 | **PROVEN** | M-norm (the catalog walk compares the raw scan string): C-014 FAILED, and so did #807's `call_remove_orphan_files_refuses_a_location_holding_another_table`. The C-012 half has no single normaliser mutation that reds it. The probe lists through the fork's local storage, which strips `file:` itself, and it compares only listed plain paths. |
| C-020 | Fail closed: an unreadable `00009-broken.metadata.json` under the scan path refuses and names the file. It is not deleted, and the row survives. | `call_orphan_cotenancy.rs::call_orphan_cotenancy_unreadable_metadata_file_refuses` | **PROVEN** | M-open (an unreadable file is skipped): FAILED. M-a: FAILED. |
| C-021 | Facade: a memory table sits at `<wh>/ns/events`. The default sweep lists and deletes the 10-day orphan and keeps the 1-day one, and the table reads `[1]`. A `location` at the warehouse and at `<wh>/repark_ctas` still refuses with the unchanged shared-root text. | `python/repark/tests/test_maintenance_call.py::test_remove_orphan_files_sweeps_a_memory_table_but_never_the_shared_root` | **PROVEN** | `15 passed` on a fresh `make develop`. The old name, with the `repark_ctas/mem/ns/events` path, failed on this branch (brief). |
| C-022 | The Q-55-6 pins stay green. After the rebase, #807's scope fixture (`call_orphan_scope.rs::ctas` and two `namespace_dir` lines) still built `repark_ctas/ice/ns/<t>` paths, and 20 `call_orphan` pins went red. Only those paths moved to `<wh>/ns/<t>`, and no assertion changed. | `cargo test -p repark-spark --lib call_orphan` | **PROVEN** | Before the fixture change: `22 passed; 20 failed`. After it: `42 passed`. With the guard and new pins: `51 passed; 0 failed`. |

## Two catalogs, one warehouse (measured 2026-09-23; guarded, C-011)

Ruled the same day: the shared directory stays, and `remove_orphan_files` refuses to sweep it
(C-011 to C-020). The measurement below is unchanged.

Scratch test, same session: `register_memory_catalog("m1", wh)` and `register_memory_catalog("m2", wh)`,
then `CREATE NAMESPACE`, `CREATE TABLE <c>.ns.t (id INT)`, and `INSERT … VALUES (<n>)` on each.

- Both creates succeed. Both inserts succeed.
- Both tables report `location = <wh>/ns/t`, so they share one directory. `ns/t/data/` holds two
  parquet files, and `ns/t/metadata/` holds two `00000-<uuid>` and two `00001-<uuid>` metadata
  files, plus two manifests and two snapshot lists. The UUID-named metadata files do not collide.
- Each catalog reads back only its own row (`rows=1` each). Neither catalog sees the other's table.
- Two sessions that each register catalog `ice` on the same warehouse show the same result.
- Before this unit, two catalogs with different names on one warehouse had separate trees
  (`repark_ctas/m1/…`, `repark_ctas/m2/…`). Two sessions with the same catalog name already shared
  `repark_ctas/ice/ns/t`.
- Inferred, not measured (the 24-hour floor stops a fresh run): `remove_orphan_files` on `m1.ns.t`
  now clears the `repark_ctas` refusal, because the table is no longer under it. The sweep would
  list `<wh>/ns/t` and treat `m2`'s live files (older than the cutoff) as orphans. Before this unit,
  a location-less memory-catalog table was always refused.

## Gates

- `cargo test -p repark-core --lib session` → `171 passed; 0 failed`
- `cargo test -p repark-spark --lib ctas` → `83 passed; 0 failed; 1 ignored`
- `cargo test -p repark-spark --lib call_orphan` → `14 passed; 0 failed`
- `cargo test -p repark-spark --lib mem_layout` → `9 passed; 0 failed`
- `cargo clippy -p repark-core -p repark-spark --all-targets -- -D warnings -A clippy::disallowed_methods`
  (the `make rust-clippy` form) → exit 0. `cargo clippy -p repark-core -p repark-spark --lib --bins -- -D warnings`
  (the panic-ban form) → exit 0.
- `scripts/check_rust_file_size.sh` → clean. `tests/ctas.rs`'s baseline ratchets from 1361 to 1357.

Layout-r4 (co-tenancy guard), 2026-09-23:

- `cargo test -p repark-spark --lib call_orphan` → `51 passed; 0 failed`
- `cargo test -p repark-spark --lib call_orphan_cotenancy` → `9 passed; 0 failed`
- `cargo test -p repark-spark --lib ctas` → `83 passed; 0 failed; 1 ignored`
- `cargo test -p repark-core --lib session` → `171 passed; 0 failed`
- `cargo test -p repark-spark --lib mem_layout` → `9 passed; 0 failed`
- `cargo clippy -p repark-core -p repark-spark --all-targets -- -D warnings -A clippy::disallowed_methods`
  → exit 0. `cargo clippy -p repark-core -p repark-spark --lib --bins -- -D warnings` → exit 0.
  Without `-A clippy::disallowed_methods`, `--all-targets` exits 101. The errors are 1481 test-code
  `unwrap`/`expect` uses across the two crates' test targets, and none is in
  `call/remove_orphan_files.rs`.
- `python -m pytest python/repark/tests/test_maintenance_call.py -q` → `15 passed`

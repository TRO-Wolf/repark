# Unit ledger — WO CATALOG-1 · the session catalog is `spark_catalog`; `spark.sql.defaultCatalog`; `USE`; `type=memory` refuses like Spark; toml `session.default_catalog`

**Date:** 2026-09-26 · **Branch:** `feat/catalog-1` · **Base:** `269f2268` (rebased from `f28a122f` on 2026-09-26)
(`origin/main`) **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (session catalog resolution; no write-path change).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Five cells of the 2026-09-26 scoreboard answered differently from Spark:
`CAT-CURRENT-CATALOG` (`[["sc"]]` for Spark's `[["spark_catalog"]]`), `CAT-DEFAULT-CATALOG`
(the runtime `spark.sql.defaultCatalog` was stored and ignored), `CAT-USE-CATALOG-NS` (the final
`USE spark_catalog.default` refused `` `sc`.`spark_catalog`.`default` ``), `CAT-TYPE-MEMORY`
(`type=memory` registered a catalog where Spark raises `Unknown catalog type: memory`) and
`E-CATALOG-LISTDATABASES` (`["ns"]` for Spark's `[]`). The owner rulings R1–R5 of 2026-09-26
(`/tmp/oc-worker/run29/claims.txt` 08:13) are binding: no carve-out.

**What Spark does (measured 2026-09-26, Spark 4.1.2 + Iceberg 1.11.0).** The probe is
`target/probe-catalog-1/spark_probe.py` (modes `main`, `rt-default`, `build-default-sc`,
`build-default-missing`); answers `target/probe-catalog-1/spark-*.json`; the owner's probe
`/tmp/oc-worker/direct/probes/cat-mem-spark-2026-09-26.json`. A fresh session's current catalog
is `spark_catalog` / `default` whatever catalogs are configured. `spark.sql.defaultCatalog`
decides the current catalog until `USE` or `setCurrentCatalog` pins a name; after that neither
`conf.set` nor `conf.unset` moves it (`m1_*`). A non-session catalog starts with the empty
namespace (`USE sc` → `sc`, `""`). A default naming no configured catalog builds, answers
`SELECT 1`, and raises `CatalogNotFoundException` `[CATALOG_NOT_FOUND] The catalog `nope` not
found. Consider to set the SQL config "spark.sql.catalog.nope" to a catalog plugin. SQLSTATE:
42P08` on `current_catalog()`, bare `SHOW NAMESPACES`, a two-part `CREATE TABLE`, `USE sc`,
`currentCatalog()` and `listDatabases()`. `type=memory` alone: nothing at conf time, every use
(`USE`, `SHOW NAMESPACES IN`, `CREATE NAMESPACE`, `SELECT`, `setCurrentCatalog`, `tableExists`)
raises `UnsupportedOperationException` `Unknown catalog type: memory`, the catalog is never listed
by `SHOW CATALOGS` / `listCatalogs`, and the current catalog stays. `type` beside `catalog-impl`
raises `IllegalArgumentException` `Cannot create catalog c_both, both type and catalog-impl are
set: type=memory, catalog-impl=org.apache.iceberg.inmemory.InMemoryCatalog` the same way. The
DataFrame door (`table`, `writeTo`, `saveAsTable`) follows the runtime default like SQL.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A fresh session's current catalog is `spark_catalog` / `default` with catalog blocks configured and with none; registering a catalog (builder block, runtime block, `register_memory_catalog`) registers it and moves nothing. The facade's single-configured-catalog start, the first-registration auto-flip and the native `set_session_catalog` call are gone; the facade's current catalog is seeded from and synced to the engine's. | `current_catalog()` / `currentCatalog()` on the harness session, a bare session, a one-block session; the cell replay. | PROVEN | Facade `test_current_catalog_cell_is_spark_catalog`, `test_a_session_without_catalog_blocks_starts_in_spark_catalog`, `test_register_memory_catalog_registers_and_nothing_else`; Rust `a_fresh_session_is_in_spark_catalog_beside_configured_and_registered_catalogs`, core `a_fresh_session_starts_in_spark_catalog_with_catalogs_configured`. Cell `CAT-CURRENT-CATALOG` replays EQUAL (`target/probe-catalog-1/replay-cat-final.json`). |
| C-002 | `spark_catalog` always exists as the session catalog: the auto memory catalog registers beside configured blocks and beside a different `spark.sql.defaultCatalog`; a user `spark.sql.catalog.spark_catalog[.*]` block (either prefix) takes its place; `repark.sql.autoMemoryCatalog=false` turns it off. The decision is the engine's (`ReparkSession::auto_session_catalog_wanted`). `spark_catalog.ns.t` names that catalog only: the facade alias `_alias_catalog_name` is gone (behavior change: a table written to `local.ns.t` is not reachable as `spark_catalog.ns.t`). | Rust decision pins; facade listCatalogs and alias pins. | PROVEN | Core `the_session_catalog_is_wanted_beside_other_catalog_blocks`, `a_user_session_catalog_block_or_the_switch_takes_the_auto_catalog_away`; facade `test_configured_catalogs_sit_beside_the_session_catalog`, `test_a_user_session_catalog_block_takes_the_auto_catalog_place`, `test_show_catalogs_lists_the_session_catalog`, `test_spark_catalog_is_not_an_alias_of_another_catalog`, `test_e2_readwriter.py::test_resolve_spark_catalog_names_the_session_catalog_only`, `…::test_spark_catalog_table_exists_names_the_session_catalog`, `…::test_spark_catalog_writer_paths_land_in_the_session_catalog`; Rust `spark_catalog_names_the_session_catalog_only`. |
| C-003 | `spark.sql.defaultCatalog` on the builder sets the first current catalog (registry and the `current_catalog()` carrier); runtime `conf.set` / `conf.unset` move it until `USE` / `setCurrentCatalog` pins the current catalog, after which neither moves it; a non-`spark_catalog` current catalog starts at namespace `""`. Two-part SQL names and the DataFrame door (`table`, `writeTo`, `saveAsTable`, `tableExists`, `listDatabases`, `listTables`) follow it; `getOrCreate` on a live session applies the key like `conf.set`. | Measured shapes 1 and 6; the cell replay. | PROVEN | Facade `test_default_catalog_cell_follows_the_runtime_conf`, `test_the_runtime_default_catalog_drives_the_dataframe_door`, `test_use_pins_the_current_catalog_against_the_default_conf`, `test_the_default_catalog_conf_at_build_is_the_first_current_catalog`, `test_catalog_surface.py::test_default_catalog_from_builder_config`; Rust `the_default_catalog_conf_at_build_is_the_first_current_catalog`, `the_runtime_default_catalog_moves_current_until_use_pins_it`, core `the_default_catalog_conf_sets_the_first_current_catalog`, `use_pins_the_current_catalog_and_the_default_conf_stops_moving_it`. Cell `CAT-DEFAULT-CATALOG` replays EQUAL. |
| C-004 | A `spark.sql.defaultCatalog` naming no configured catalog builds (the check reads an unpinned current catalog; `USE` validates the catalog it pins); `SELECT 1` and three-part names answer; `current_catalog()` / `current_database()`, bare `SHOW NAMESPACES` / `SHOW TABLES`, a two-part `CREATE TABLE`, a one-part `USE`, `currentCatalog()` and `listDatabases()` raise Spark's `[CATALOG_NOT_FOUND] …` text as `AnalysisException`. | Measured shape 2 (builder and runtime). | PROVEN | Facade `test_a_default_catalog_naming_no_catalog_answers_catalog_not_found`, `test_auto_memory_catalog.py::test_a_foreign_default_catalog_keeps_the_session_catalog`; Rust `a_default_catalog_that_names_no_catalog_refuses_where_spark_resolves_it`, core `a_default_catalog_that_names_no_catalog_is_catalog_not_found_at_first_use`. Residue R-1 (class). |
| C-005 | `USE c` → (`c`, `""`); `USE c.ns`; `USE ns` in the current catalog; `USE spark_catalog` and `USE spark_catalog.default` → (`spark_catalog`, `default`); `USE zz.yy`, `USE zz`, `USE zz.yy.xx`, `USE sc.nope`, `USE hc.ns.x` refuse `[SCHEMA_NOT_FOUND] The schema <rendered> cannot be found. …` (condition `SCHEMA_NOT_FOUND`, SQLSTATE `42704`) with Spark's rendering and leave the current catalog alone. | Measured `use_*` keys; the cell replay. | PROVEN | Facade `test_use_catalog_ns_cell_and_the_final_reset`, `test_use_forms_answer_as_spark`; Rust `use_forms_answer_as_spark`. Cell `CAT-USE-CATALOG-NS` replays EQUAL (`target/probe-catalog-1b/replay-USE-CATALOG-NS.json`). No code change: the pins pass on the part-1 tree (R5 registered `spark_catalog`); bite-proven by mutation (dropped current catalog in the two-part rendering, same-catalog `USE` resetting the namespace — both fail the pin). Measured 2026-09-26 (`target/probe-catalog-1b/spark-open.json`): same-catalog `USE` keeps the namespace on both doors (`use_sc_from_same_cur`, `use_session_from_same_cur`). `USE NAMESPACE zz` is outside the listed forms: Spark answers `SCHEMA_NOT_FOUND`, RePark's parser refuses it (recorded, not pinned). |
| C-006 | A catalog block whose kind comes only from `type=memory` (builder or runtime `conf.set`) builds quietly and is not listed by `SHOW CATALOGS` / `listCatalogs`; every use — a statement naming it (dotted first part, `USE`, `SHOW … IN/FROM`), `SHOW NAMESPACES IN c`, `USE c`, `setCurrentCatalog`, `tableExists`, the engine's catalog handle — raises `UnsupportedOperationException` `Unknown catalog type: memory` (condition and SQLSTATE `None`), each time; the current catalog stays. A later runtime block with the long form replaces the refusal. `catalog-impl = org.apache.iceberg.inmemory.InMemoryCatalog` is the memory catalog on both doors (CAT-IMPL-INMEMORY stays EQUAL); `type=hadoop` is unchanged (CAT-TYPE-HADOOP stays EQUAL). | Measured `c_mem_*` keys; the cell replay. | PROVEN | Facade `test_type_memory_cell_refuses_at_first_use`, `test_type_memory_on_the_builder_door_builds_and_refuses_at_first_use`, `test_the_catalog_impl_long_form_works_on_both_doors`; Rust `a_bare_memory_type_refuses_every_first_use_with_sparks_text`, `refusal_tests::a_bare_memory_type_is_refused_at_first_use_with_sparks_text`, `…::a_memory_type_without_a_warehouse_is_refused_not_a_build_error`, `…::the_catalog_impl_long_form_and_hadoop_type_stay_catalogs`, core `a_bare_memory_type_builds_and_refuses_at_first_use`, `a_runtime_memory_type_block_is_refused_and_a_later_long_form_registers`. Cell `CAT-TYPE-MEMORY` replays EQUAL (`target/probe-catalog-1b/replay-CAT-TYPE-MEMORY.json`, identical error record); CAT-CATALOG-IMPL-INMEMORY and CAT-TYPE-HADOOP replay EQUAL. Measured 2026-09-26 (`target/probe-catalog-1b/spark-open.json`): the `type` match is case-insensitive and the message echoes the raw value (`c_upper_show_ns`, `c_mixed_show_ns`). A `defaultCatalog` naming a refused catalog refuses on the SQL surface (measured 2026-09-26, `target/probe-catalog-1b/spark-poison-default.json`); residue R-4: Spark's `catalog.currentCatalog()` API answers `spark_catalog` there while its SQL refuses — RePark refuses on both. Bite-proven by mutation (memory-type never refuses; the guard scan disabled — the pins fail). |
| C-007 | A block with both `type` and `catalog-impl` set is refused at first use with `IllegalArgumentException` `Cannot create catalog <name>, both type and catalog-impl are set: type=<t>, catalog-impl=<class>` (raw values), replacing both the old agreeing-pair acceptance and the build-time disagreeing-pair refusal. | Measured `c_both_*` keys. | PROVEN | Facade `test_both_kind_keys_refuse_at_first_use_on_both_doors`; Rust `both_kind_keys_refuse_every_first_use_as_illegal_argument`, `refusal_tests::both_type_and_catalog_impl_are_refused_with_sparks_text`, core `both_kind_keys_build_and_refuse_at_first_use_as_illegal_argument`. Measured 2026-09-26 (`target/probe-catalog-1b/spark-open.json` key `both_glue_hadoop_show_ns`): the both-keys text echoes raw values for any pair. |
| C-008 | `repark.sql.catalogExtensions=true` (builder; any case of the key, value `true`) makes `type=memory` the memory kind on the builder and runtime doors and keeps the old both-keys rule (agreeing pair accepted, disagreeing pair refused at build). Default off. | Rust and facade opt-in pins. | PROVEN | Facade `test_the_catalog_extensions_opt_in_restores_the_memory_type`; Rust `the_catalog_impl_long_form_and_the_opt_in_stay_catalogs`, `refusal_tests::the_catalog_extensions_opt_in_keeps_the_memory_type_and_the_agreeing_pair`, `…::the_opt_in_reads_only_a_true_value`, core `the_catalog_extensions_opt_in_makes_the_memory_type_a_catalog_on_both_doors`. The facade forwards the effective opt-in value with each late block, so a builder-set opt-in also covers runtime blocks (a runtime-set one works the same way). |
| C-009 | `spark.catalog.listDatabases("ns*")` on the harness session answers `[]`; `listDatabases()` answers `["default"]`; after `setCurrentCatalog("sc")` it answers `["ns"]`. | The cell replay. | PROVEN | Facade `test_list_databases_cell_lists_the_session_catalog`. Cell `E-CATALOG-LISTDATABASES` replays EQUAL (`target/probe-catalog-1b/replay-E-CATALOG-LISTDATABASES.json`). Pin-only (the cell already replayed EQUAL); bite-proven by mutation (a `listDatabases` pinned to `sc` fails the pin). |
| C-010 | `repark.toml`: a catalog block's `type = "memory"` is rewritten to `catalog-impl = "org.apache.iceberg.inmemory.InMemoryCatalog"`, replacing `type` (a memory-kind `catalog-impl` already present is kept; a non-memory one keeps both keys for C-007); `[<profile>.session] default_catalog` (a string) emits `spark.sql.defaultCatalog`; the typed mirror `repark.config.SessionConfig` carries `default_catalog`. The owner's shape (`impl`, `type = "memory"`, `warehouse`) loads unchanged, the session starts in `spark_catalog`, and `default_catalog = "local"` starts it in `local`. | Emitted key sets; session builds from a file on the Rust and Python doors. | PROVEN | Rust `config_file::tests::session_catalog::*` (7 tests: the long-form rewrite across `memory`/`MEMORY`/`Memory` casings with the toml-Memory versus flat-Refused split, the kept memory-kind impl, both-keys with the `IllegalArgument` text, the `spark.sql.defaultCatalog` emission, the non-string refusal, the owner shape starting in `spark_catalog`, `default_catalog = "local"` starting in `local`), `native_type_catalog_blocks_match_the_flat_config_path` (the memory arm is now the still-matching `hadoop` arm), `file_built_session_registers_the_same_catalogs_as_config_calls` (new `type=memory`-file versus long-form-calls arm); facade `test_the_owner_toml_loads_and_starts_in_spark_catalog`, `test_session_default_catalog_in_toml_moves_the_first_current_catalog`, `test_the_typed_session_table_renders_default_catalog_for_the_engine`; `docs/guide/repark-toml.md` documents `session.default_catalog`, the rewrite and `repark.sql.catalogExtensions`. |
| C-011 | Every existing test that relied on the auto-flip, the single-catalog start, the alias or a bare `type=memory` is rewritten to say what it now means (none deleted); the list is below. | The facade, dbt, parity and Rust suites green. | PROVEN | The C-011 sweep ran the whole facade suite (13792 passed, 481 skipped, 149 xfailed, 0 failed), the dbt suite (64 passed, 1 skipped), the parity suite (785 passed, 3 skipped, 12 xfailed) and `check_example_coverage.py --require-execute` green: no suite test needed a rewrite beyond the earlier commits' list below (none deleted anywhere), and the one docs example changed on this branch that assumed the flip was rewritten the same way (`docs/examples/catalog/list_tables.py` — sets the registered catalog current before the one-part listings); `docs/examples/session/register_catalog.py` and `docs/examples/catalog/set_current_names.py` were rewritten by part 1 on main. |
| C-012 | `spark.catalog.databaseExists("c_mem.n1")` on a refused `type=memory` catalog raises `UnsupportedOperationException` `Unknown catalog type: memory` (condition and SQLSTATE `None`) instead of answering `False`; the current catalog stays. | Measured `databaseExists` key. | PROVEN | Facade `test_database_exists_on_a_refused_catalog_raises_like_spark` (fails on the old catch-all, which swallowed the refusal and answered `False`). Oracle `target/probe-catalog-1b/spark_mem.json` `databaseExists` (probe `spark_mem.py` beside it). |
| C-013 | `spark.catalog.listTables("c_mem.n1")` and `spark.catalog.getTable("c_mem.n1.t")` on a refused catalog raise the same refusal instead of `[SCHEMA_NOT_FOUND]` / `[TABLE_OR_VIEW_NOT_FOUND]`; the catalog part routes through the same refusal check the other doors use and the native refusal propagates unmapped; the current catalog stays. | Measured `listTables_db`, `getTable` keys. | PROVEN | Facade `test_list_tables_and_get_table_on_a_refused_catalog_raise_like_spark` (fails on the old re-mapping). Oracle `target/probe-catalog-1b/spark_mem.json` `listTables_db`, `getTable`. |
| C-014 | A two-part SQL name on a refused catalog (`SELECT * FROM c_mem.t`, `INSERT INTO c_mem.t`, `DESCRIBE c_mem.t`) refuses `UnsupportedOperationException` `Unknown catalog type: memory` on the engine door instead of planning `spark_catalog.c_mem.t`; a two-part name whose first word names no refused catalog still plans. | Measured `select_2part` key. | PROVEN | Rust `a_bare_memory_type_refuses_every_first_use_with_sparks_text` (three new two-part arms) and `a_two_part_alias_on_the_default_catalog_still_plans_beside_a_refusal` (fails an implementation that refuses every two-part name). Oracle `target/probe-catalog-1b/spark_mem.json` `select_2part`. |

## Residues

| id | text |
|---|---|
| R-1 | Dated 2026-09-26 (verifier round 1, class residue). A `spark.sql.defaultCatalog` naming no configured catalog: RePark's first use raises `AnalysisException`; Spark raises a Py4J-wrapped `CatalogNotFoundException` (the registry row CAT-DEFAULT-CATALOG carries both texts). Same message shape, different class; pinned as a divergence. |
| R-2 | Dated 2026-09-26 (verifier round 1 V-002; `target/verify/probe1.py` `sc_ns_t_select`, `target/probe-catalog-1/spark-main.json` `spark_catalog_ns_t_select`). `SELECT * FROM spark_catalog.ns.t0` when `ns` exists only in `sc`: Spark `[TABLE_OR_VIEW_NOT_FOUND] The table or view `spark_catalog`.`ns`.`t0` cannot be found. …`; RePark `Error during planning: table 'spark_catalog.ns.t0' not found`. Both refuse; the text differs (the registered `Error during planning:` prefix class plus the bare `table … not found` shape). The pins assert the refusal class; the text is a CATALOG-1B or CASESENS follow-up. |
| R-3 | Dated 2026-09-26 (verifier round 1 V-002). `CREATE TABLE spark_catalog.ns.t0 (id INT) USING iceberg` when `ns` exists only in `sc`: Spark `IllegalArgumentException: Cannot open table: path is not set`; RePark `NamespaceNotFound => No such namespace: NamespaceIdent(["ns"])`. Both refuse; different class and text; not worked in this unit. |
| R-4 | Dated 2026-09-26 (fold round; `target/probe-catalog-1b/spark-poison-default.json` `poison_default_api_current` vs `poison_default_current`). A `spark.sql.defaultCatalog` naming a refused catalog: Spark's `catalog.currentCatalog()` API answers `spark_catalog` while its SQL (`SELECT current_catalog()`) refuses `UnsupportedOperationException` `Unknown catalog type: memory`; RePark refuses on both. Same SQL refusal; the API answer differs. |

## Tests rewritten

Seeded by the CAT-TYPE-MEMORY commit (bare `type=memory` / both-keys rewrites), extended by
the C-010 toml-keys commit, and completed by the C-011 sweep: the sweep itself rewrote no
suite test (every failure the earlier commits' rewrites had already covered stayed green;
none deleted anywhere) and rewrote the one docs example changed on this branch below the
same way.

- `crates/repark-core/src/catalog_config.rs` (inline): `cross_prefix_duplicates_merge_or_fail_loud`
  (the merged memory block now parses to the refused kind),
  `type_short_forms_resolve_each_kind` (the memory arm is refused),
  `memory_without_warehouse_names_the_warehouse_key` and
  `memory_with_blank_warehouse_is_rejected` (the warehouse requirement now pins via the
  catalog-impl long form), `conflicting_impl_and_type_errors` (the old conflict error now
  pins under the opt-in), `catalog_spec_debug_redacts_secret_prop_values` (the new
  `refusal` field), `i5_catalog_config_acceptance_matrix_ok` (the SparkCatalog+type arm is
  refused; the bare `= memory` arms still resolve to `Memory`),
  `i5_catalog_config_acceptance_matrix_loud` (the memory and conflict arms are refused).
  The touched tests share an `entry` helper and class-name consts (rustfmt keeps only
  short tuples single-line); the file drops 1006 → 965 and its size row retires.
- `crates/repark-core/src/catalog_config/hadoop_naming_tests.rs`:
  `memory_type_adds_no_metadata_naming`, `explicit_metadata_naming_passes_through_verbatim`
  (memory-type naming now pins under the opt-in).
- `crates/repark-core/src/session/tests/session.rs`:
  `late_catalog_registration_adds_new_names_and_skips_existing` (`type=hadoop` keeps the
  registration/idempotence meaning, including the warehouse-missing arm),
  `configured_memory_catalog_fallback_root_is_the_warehouse` (`type=hadoop`; the stale
  `type=memory` doc line is dropped, 1407 → 1406).
- `crates/repark-core/src/session/tests/aws_gate.rs`:
  `offline_session_finalize_never_resolves_aws_sdk_config` (`type=hadoop`).
- `crates/repark-core/src/session/tests/hadoop_naming.rs`: the shared helper builds with
  the opt-in (inert for non-memory blocks) so the two memory-type naming tests keep
  working catalogs.
- `crates/repark-core/src/config_file/tests/wiring.rs`:
  `file_built_session_registers_the_same_catalogs_as_config_calls` (`type=hadoop` both
  doors; C-010 extends it for the `type=memory` rewrite).
- `crates/repark-spark/src/tests/hadoop_rename.rs`,
  `crates/repark-sql/src/alter/hadoop_rename_tests.rs`: the shared helpers build with the
  opt-in so the memory-vs-hadoop rename distinction keeps working catalogs.
- `crates/repark-python/tests/bindings.rs`:
  `config_driven_memory_catalog_registers_through_the_constructor` (the measured block
  plus the opt-in).
- `crates/repark-distributed/tests/codec.rs` and
  `crates/repark-distributed/tests/iceberg_scan.rs`: `CatalogSpec` literals gain
  `refusal: None` (two sites in `codec.rs`, one in `iceberg_scan.rs`, commit `8fbf5cf2`);
  the scan codec maps the refused kind (round-trips; refused specs never travel the wire).
- `python/repark/tests/test_catalog_flow.py`: `test_config_driven_catalog_publish_flow`,
  `test_repark_prefixed_catalog_config_registers_identically` (the measured blocks plus
  the opt-in).
- `python/repark/tests/test_catalog_surface.py`:
  `test_default_catalog_from_builder_config` (`type=hadoop`; the C-003 meaning is the
  default seeding, not the memory spelling).
- `python/repark/tests/test_getorcreate_catalogs.py`: the late/keep builders move to
  `type=hadoop`; the malformed block (`type=hadoop`, no warehouse) still raises on
  `warehouse`.
- `python/repark/tests/test_ice_catalog_session_1.py`: the five runtime registrations and
  the EAGER-1 builder block move to `type=hadoop`.
- Seeded further by the C-010 toml-keys commit:
  `crates/repark-core/src/config_file/tests/mod.rs`
  `native_type_catalog_blocks_match_the_flat_config_path` (the memory arm becomes the
  `hadoop` arm — a toml `type = "memory"` now resolves `Memory` while the flat spelling
  refuses, pinned in `session_catalog.rs`) and
  `crates/repark-core/src/config_file/tests/wiring.rs`
  `file_built_session_registers_the_same_catalogs_as_config_calls` (new `type=memory`-file
  versus long-form-calls arm).
- `crates/repark-spark/tests/ddl_sessions.rs`:
  `config_driven_memory_catalog_registers_and_runs` declares the `catalog-impl` long form
  instead of the bare `type=memory` (commit `af93a027`).
- The part-1 auto-flip and alias rewrites (commits `59fff2e5`, `9d9290a8`), recorded here
  for the complete list: the fixtures of `python/repark/tests/test_f1_sql_expander.py`,
  `test_g1_stat_and_expander.py`, `test_e2_readwriter.py`, `test_iceberg_load_path.py`,
  `test_time_travel.py` now `USE` their catalog; the alias and flip pins of
  `test_e2_readwriter.py`, `test_catalog_surface.py`, `test_auto_memory_catalog.py`,
  `test_ice_catalog_session_1.py`, `test_ice_views_4_showprops.py`,
  `test_declare_sorted_tighten.py` state the new meaning;
  `test_production_file_size.py` drops the five removed session symbols; the dbt
  `test_statement_surface.py` session sets `spark.sql.defaultCatalog` to the fixture
  catalog.
- The C-011 sweep rewrote no suite test and deleted none; on this branch it rewrote one
  docs example the same way: `docs/examples/catalog/list_tables.py` (sets the registered
  catalog current before the one-part listings). `docs/examples/session/register_catalog.py`
  (current stays `spark_catalog` after register; the example moves there explicitly) and
  `docs/examples/catalog/set_current_names.py` (register stays put; `setCurrentCatalog`
  moves) were rewritten by part 1 on main.

## Coverage attestation (lane xc-catalog2, 2026-09-26)

Filed at the C-011 close, when the last OPEN clause flipped. Residues R-1, R-2, R-3
and R-4 stand as recorded below (R-1 in C-004, R-4 in C-006).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: catalog-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause was checked against its measured Spark text or shape, never a paraphrase, and the five scoreboard cells plus CAT-IMPL-INMEMORY, CAT-TYPE-HADOOP, CAT-SHOW-CATALOGS, E-CATALOG-LISTDATABASES and P-CALL-NO-CATALOG replay EQUAL through the harness.
      artifacts: [task/ledgers/staging/catalog-1-ledger.md, python/repark/tests/test_catalog_1.py, target/probe-catalog-1c/replay-CAT-TYPE-MEMORY.json]
    - id: AT-2
      status: ATTACKED
      evidence: Case variants of the memory type, both-keys raw-value echo, unknown and empty default catalogs, non-string toml values, missing warehouses, dotted catalog names and empty blocks are all exercised on the builder, runtime and file doors.
      artifacts: [crates/repark-core/src/catalog_config/refusal_tests.rs, crates/repark-core/src/config_file/tests/session_catalog.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Refused catalogs raise on every use with the current catalog unchanged, a later long-form block replaces the refusal, and a default naming no catalog fails only at first resolution with the CATALOG_NOT_FOUND text.
      artifacts: [python/repark/tests/test_catalog_1.py::test_type_memory_cell_refuses_at_first_use, python/repark/tests/test_catalog_1.py::test_a_default_catalog_naming_no_catalog_answers_catalog_not_found]
    - id: AT-4
      status: ATTACKED
      evidence: The USE pin against later conf set and unset, the runtime block replacement order, and the builder-versus-runtime door split are pinned step by step, and the current catalog lives in the one engine registry both doors read.
      artifacts: [python/repark/tests/test_catalog_1.py::test_use_pins_the_current_catalog_against_the_default_conf, crates/repark-core/src/session/tests/session_catalog.rs]
    - id: AT-5
      status: N/A
      justification: No authentication, secret, deserialization or path handling is added. Warehouse paths flow through the existing scheme validation and toml parse errors still redact source lines.
    - id: AT-6
      status: ATTACKED
      evidence: The spark_catalog alias removal is pinned as the documented behavior change, existing toml files load unchanged through the rewrite, and refused specs never cross the distributed scan wire.
      artifacts: [python/repark/tests/test_catalog_1.py::test_spark_catalog_is_not_an_alias_of_another_catalog, crates/repark-core/src/config_file/tests/session_catalog.rs::the_owner_toml_shape_loads_and_starts_in_spark_catalog]
    - id: AT-7
      status: N/A
      justification: No hot path, unbounded growth or new query-time work is added. A refused catalog is one registry placeholder whose uses raise without I/O.
    - id: AT-8
      status: ATTACKED
      evidence: Every Spark-visible text was measured on Spark 4.1.2 plus Iceberg 1.11.0 before it was pinned, and both error classes exist on the Rust and Python doors with condition and SQLSTATE pinned.
      artifacts: [target/probe-catalog-1/spark_probe.py, crates/repark-spark/src/tests/session_catalog.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every failure surface carries its diagnosing text verbatim in the pin, the conf dump keeps an origin per merged file key, and the cloud-catalog discovery warning is unchanged.
      artifacts: [crates/repark-core/src/catalog_config/refusal_tests.rs, crates/repark-core/src/config_file/tests/wiring.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Every clause pins per entry point on the Arrow path with bite-proving mutations recorded in C-005, C-006 and C-009, every added branch has a nameable flipping input, and the facade, dbt, parity and config_file suites are green with no deletion.
      artifacts: [python/repark/tests/test_catalog_1.py, crates/repark-core/src/config_file/tests/session_catalog.rs, crates/repark-spark/src/tests/session_catalog.rs]
  reattested: []
  complete: true
```

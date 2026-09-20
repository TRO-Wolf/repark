# Unit ledger — ICE-CATALOG-SESSION-1 · catalog and session SQL (IPI-32, RePark half)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when ICE-CATALOG-SESSION-1 merges, or when the owner closes the slate row.

**Unit:** ICE-CATALOG-SESSION-1 · **Date:** 2026-09-20 · **Executor:** Muse Spark
(muse-spark-1.3-contributor), delegated implementation tier ·
**Branch:** `fix/ipi-32-catalog-session` · **Base:** `6d029ab8`
**Model:** muse-spark-1.3-contributor
**risk_tier:** standard.
**Path:** STANDARD.

Eleven inventory cells replay EQUAL: a real CURRENT CATALOG / CURRENT NAMESPACE on the
session (Spark `USE`, `current_catalog()` / `current_schema()` / `current_database()`, name
completion, `SHOW CATALOGS`, `SHOW COLUMNS`, `SHOW TABLES`, `REFRESH` / `CACHE` / `UNCACHE
TABLE`, catalog-less `CALL`, runtime `spark.sql.catalog.*` registration, and the
`hadoop` / `InMemoryCatalog` / `table-default.*` / `table-override.*` config gaps.

**Not in this unit:** `STATUS.md`, version bump, tags, `Cargo.toml` / `Cargo.lock` / pin
changes, the fork (RePark-only), `scripts/check_rust_file_size.py` ceiling raises (ratchet
down only), `temp_view.rs` home moves, postgres registration refusal changes, AWS commands.

**Oracle:** the eleven cells' recorded Spark answers
(`/tmp/oc-worker/qe/cells/ipi-32-catalog-session.txt`), probe N-1..N-13
(`/tmp/oc-worker/qe/probe/p3.json`), and three live Spark 4.1.2 probes run 2026-09-20 for
the semantics the packet left open (USE order, USE CATALOG, LIKE shape, SHOW schemas,
setCurrentCatalog, USE DEFAULT). Committed verbatim as
`python/repark/tests/ice_catalog_session_1_spark_oracle.json`.

**Probe corrections to the packet (all measured on live Spark 4.1.2, all pinned):**

- P-1: one-part `USE` is CATALOG-first, not namespace-first (D-1/T-4/mutation-2 said
  namespace-first from an unmeasured reading of N-5). A name that is both a namespace in
  the current catalog and a catalog switches the catalog. pins: ice-catalog-session-1/C-003
- P-2: `USE CATALOG x` is a Spark PARSE error, not a third spelling (S-5). RePark refuses
  it as `ParseException`. pins: ice-catalog-session-1/C-007
- P-3: catalog-switch namespace is per catalog TYPE: `spark_catalog` restores `default`,
  v2 catalogs clear to `""` (N-2's `""` is v2-only); self-`USE` keeps the namespace.
  pins: ice-catalog-session-1/C-002
- P-4: `SHOW ... LIKE` is a glob (`*`, `|`), not SQL `LIKE` and not full regex; match is
  full-string. pins: ice-catalog-session-1/C-016
- P-5: `SHOW TABLES` excludes temp views unless the current catalog is the session
  catalog; this unit lists catalog tables only (deferred, unmeasured by any cell).
- P-6: `REFRESH` without `TABLE` is accepted; `CACHE TABLE ... AS SELECT` with a qualified
  name is a Spark parse error. The AS SELECT form stays `NotImplemented`.
- P-7: `USE DEFAULT` is accepted by Spark (sets namespace `DEFAULT`); RePark routes it
  through the standard one-part rule, so it refuses `SCHEMA_NOT_FOUND` unless a
  case-exact `DEFAULT` namespace exists (Spark matches case-insensitively; the engine is
  case-sensitive for namespace idents — ledgered divergence, no cell covers it).
- P-8 (open measurement, NOT fixed): `SHOW TABLES LIKE 'tlike?'` returned `[]` on 4.1.2
  (literal `?`) while the 4.0.0 `SHOW NAMESPACES` truth table pins `.*` matching all
  (regex `.`). One datum does not overturn thirty; `filter_pattern_matches` is reused
  unchanged and the conflict is recorded here as suspected oracle drift.
- T-1 (tree measurement, H-06 deviation): the Databricks executing parse reads paren-less
  `current_catalog` as a bare `Identifier`, never as a `Function` — so it cannot resolve
  as the UDF (H-06's parenthetical does not hold on this path; proven by
  `session_names::tests::bare_session_name_does_not_resolve_as_call`). `REFUSING` loses
  the three names per H-06, but the column-error mapper keeps them (`MAPPED`) so the
  paren-less spellings still refuse `UNRESOLVED_COLUMN` like Spark and like the
  `now` / `current_timezone` siblings. `Q14_REFUSING_BARE` keeps all six names; H-06's
  drop instruction is declined with this measurement (hand-back open question).
- T-2 (tree measurement): H-01 lives in `SparkDialect::on_session_built`, not
  `SparkExtension::configure`. The temp-view home is captured from the same build-time
  config between the two, and a `spark_catalog` / `default` home breaks every temp-view
  write (the facade registers `spark_catalog` over it). Diagnosis: `q14` errored on the
  fixture's `createOrReplaceTempView` until the move; green after.
- T-3 (recorded-cell proof, H-04 deviation): `RENAME TO` resolves against the SOURCE
  table's catalog/namespace, not the session. D-1/H-04 route source AND dest through
  the session completer, but D-RENAME-TABLE-SHORT records Spark `ok` with the session
  current at `spark_catalog` while source and dest live in `sc` — a session-relative
  dest would cross catalogs and refuse. `rename_dest` in `use_ddl.rs` anchors 1/2-part
  dests on the source; the ALTER token pre-parsers' premature 3-part gates (which fired
  before form detection, claiming even RENAME) are removed so short names reach the AST
  path, and token forms complete at execute. `alter.rs` 1449 → 1444, row ratcheted.
  pins: ice-catalog-session-1/C-022

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `USE <catalog>.<ns>` sets both session defaults. | `tests::use_ddl::use_two_part_sets_catalog_and_namespace` in `crates/repark-spark/src/tests/use_ddl.rs`. | **PROVEN** | Green: `cargo test -p repark-spark --offline --lib tests::use_ddl` 16/16. pins: ice-catalog-session-1/C-001 |
| C-002 | `USE <catalog>` clears a v2 namespace to `""`, restores `default` on `spark_catalog`, and keeps the namespace on self-`USE`. | `use_catalog_only_clears_v2_namespace_to_empty`, `use_catalog_onto_session_catalog_restores_default`, `use_self_catalog_keeps_namespace`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-002 |
| C-003 | One-part `USE` prefers a catalog over a same-named namespace, else resolves the namespace in the current catalog. | `use_one_part_prefers_catalog_over_namespace`, `use_one_part_namespace_in_current_catalog`. | **PROVEN** | Same run. Corrects D-1 per P-1. pins: ice-catalog-session-1/C-003 |
| C-004 | One-part `USE` of a missing name refuses `SCHEMA_NOT_FOUND` naming `` `<current>`.`<part>` `` and leaves the defaults untouched. | `use_missing_one_part_names_current_catalog_and_part`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-004 |
| C-005 | Two-part `USE` with a missing namespace names both parts; with an unknown first part names current plus both. | `use_two_part_missing_namespace_names_both_parts`, `use_two_part_unknown_first_names_current_and_both`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-005 |
| C-006 | `USE DATABASE` / `USE SCHEMA` never resolve a catalog; one-part sets the namespace in the current catalog. | `use_database_spelling_is_namespace_only`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-006 |
| C-007 | `USE CATALOG x` refuses as `ParseException`, like Spark. | `use_catalog_spelling_is_a_parse_refusal`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-007 |
| C-008 | `USE` returns zero rows. | `use_returns_no_rows`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-008 |
| C-009 | `USE DEFAULT` reaches the USE arm (never the generic wildcard). | `use_default_reaches_the_use_arm`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-009 |
| C-010 | The shared completer expands 1/2/3-part names and refuses a bare name under an empty namespace with `TABLE_OR_VIEW_NOT_FOUND`. | `complete_name_expands_one_and_two_part_names`, `complete_name_bare_table_under_empty_namespace_is_not_found`. | **PROVEN** | Same run. pins: ice-catalog-session-1/C-010 |
| C-011 | Session build defaults are `spark_catalog` / `default` unless the builder map sets the DataFusion keys explicitly. | `SparkDialect::on_session_built` in `crates/repark-spark/src/dialect.rs` + `test_q14_session_names_sql_answer`. | **PROVEN** | q14 green (88 passed, 2 xfailed). pins: ice-catalog-session-1/C-011 |
| C-012 | `CAT-CURRENT-CATALOG` replays EQUAL: `SELECT current_catalog()` answers `spark_catalog` at session start. | `test_current_catalog_answers_spark_catalog` in `python/repark/tests/test_ice_catalog_session_1.py`. | OPEN | S8. |
| C-013 | `current_schema()` / `current_database()` track `USE`, including `""` after catalog-only `USE`. | `test_use_catalog_leaves_namespace_empty`, `test_current_database_alias`. | OPEN | S8. |
| C-014 | `CAT-USE-CATALOG-NS` replays EQUAL. | `test_use_catalog_ns_cell` + `test_two_part_name_resolves_against_current_catalog`. | OPEN | S8. |
| C-015 | `CAT-SHOW-CATALOGS` replays EQUAL (sorted, `spark_catalog` included; eager listing is a lenient divergence). | `test_show_catalogs_lists_registered` + `test_show_catalogs_like_filters`. | OPEN | S8. |
| C-016 | `SHOW TABLES` answers Spark's `(namespace, tableName, isTemporary)` shape with `IN` / `FROM` / `LIKE`-glob. | `test_show_tables_after_use_lists_current_namespace`. | OPEN | S8. |
| C-017 | `D-SHOW-COLUMNS` replays EQUAL in declaration order (not the harness sort), all three spellings, winning over `information_schema`. | `test_show_columns_is_declaration_order` + `test_show_columns_beats_information_schema`. | OPEN | S8. |
| C-018 | Bare `SHOW NAMESPACES` / `SCHEMAS` / `DATABASES` list the current catalog (NS-1 retired). | `test_show_namespaces_bare_uses_current_catalog` + rewritten `describe_show.rs` pins. | OPEN | S8. |
| C-019 | `CAT-REFRESH-TABLE` replays EQUAL; a missing table refuses `TABLE_OR_VIEW_NOT_FOUND`. | `test_refresh_table_cell` + `test_refresh_missing_table_raises_table_not_found`. | OPEN | S8. |
| C-020 | `CAT-CACHE-TABLE` replays EQUAL; SQL `CACHE TABLE` sets `isCached` and a write invalidates. | `test_cache_table_cell` + `test_cache_table_then_write_then_read_sees_the_write`. | OPEN | S8. |
| C-021 | `UNCACHE TABLE` on a missing table refuses `TABLE_OR_VIEW_NOT_FOUND`; `IF EXISTS` answers ok. | `test_uncache_missing_table` + `test_uncache_if_exists_missing_is_ok`. | OPEN | S8. |
| C-022 | `D-RENAME-TABLE-SHORT` replays EQUAL; cross-catalog `RENAME TO` still refuses. | `test_rename_to_two_part_cell` + `test_rename_across_catalogs_still_refuses` + `rename_two_part_dest_anchors_on_the_source_catalog` + `rename_three_part_dest_across_catalogs_still_refuses` + `alter_source_and_rename_dest_complete_short_names`. | OPEN | Mechanism pinned S3; cell pins S8. |
| C-023 | `P-CALL-NO-CATALOG` replays EQUAL: two-part `system.<proc>` uses the current catalog. | `test_call_no_catalog_cell` + `call_two_part_resolves_current_catalog` + `resolve_call_target_two_part_uses_current_catalog`. | OPEN | Mechanism pinned S3; cell pins S8. |
| C-024 | Runtime `conf.set` of `spark.sql.catalog.*` accumulates; first complete block registers, later sets update the side map without re-registering. | `test_runtime_catalog_registration_then_create` + `test_late_table_default_key_lands_on_create`. | OPEN | S8. |
| C-025 | `CAT-TYPE-HADOOP` replays EQUAL (`hadoop` aliased to `Memory`; UUID metadata names, not `vN`). | `test_hadoop_type_cell` + `kind_from_type` unit pins. | OPEN | S8. |
| C-026 | `CAT-CATALOG-IMPL-INMEMORY` replays EQUAL. | `test_catalog_impl_inmemory_cell` + `kind_from_catalog_impl` unit pins. | OPEN | S8. |
| C-027 | `CAT-TABLE-DEFAULT-OVERRIDE` replays EQUAL with override > user > default precedence on all three legs. | `test_table_default_override_cell` + `test_table_override_beats_user_property` + `test_user_property_beats_table_default` + `test_both_default_and_override_resolves_to_override`. | OPEN | S8. |
| C-028 | Facade `_catalog_state` agrees with the engine after `USE`, `setCurrentCatalog` / `setCurrentDatabase`, and `SET datafusion.catalog.*`. | `test_use_updates_facade_state`, `test_set_current_catalog_updates_engine_state`, `test_set_datafusion_catalog_keys_updates_facade_state`. | OPEN | S8. |
| C-029 | `current_catalog` / `current_schema` / `current_database` leave `bare_nullary.rs` `REFUSING`; the q14 pins assert answers. | Rewritten `test_q14_session_names_sql_divergence` + `test_q14_session_names_sql_answer` + `parenthesised_current_catalog_survives_demotion` + `bare_current_catalog_column_error_stays_mapped`. | **PROVEN** | q14 green; `bare_nullary` 11/11; UDF 3/3. H-06 drop declined per T-1. pins: ice-catalog-session-1/C-029 |
| C-030 | `catalog_config.rs` extraction ratchets down with `hadoop` / `InMemoryCatalog` arms and updated refusal texts. | `scripts/check_rust_file_size.py` green + `type_short_forms_resolve_each_kind` siblings. | OPEN | S7. |

VERDICT: 30 clauses, 12 PROVEN, 18 OPEN, 0 REJECTED.

## Coverage attestation

Filed once no clause is OPEN (S8).

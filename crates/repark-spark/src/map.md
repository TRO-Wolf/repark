# map — repark-spark/src

## Purpose

Source for the Spark SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. Module bodies are ported from v1 `repark-sql` at the port-source pin (declared edit
classes only: prefix renames, the remaining PR-3b refuse arms, seam adaptation). PR-3a restored
the DDL handler modules (`ctas`, `create_table`, `alter`, `namespace_ddl`) and completed
`catalog_ops`/`normalize`.

## Contents

- `router.rs` — `execute` / `execute_with_read_only` / `execute_inner` + pre-parse intercepts
  (alter I6/I7, create-namespace, describe/show) and the remaining PR-3b TEMPORARY refuse arms
  (MERGE / INSERT OVERWRITE / CALL / ref DDL — each names its construct + restoring PR;
  [router/map.md](router/map.md) for the tests).
- `ctas.rs` — CTAS staged create/replace (fork `StagedTableTransaction`, one catalog publish),
  service-managed (S3 Tables) create-first path, create-clause refuse helpers.
- `create_table.rs` — column-def `CREATE TABLE` (I5 schema-only staged create) + the
  Spark-SQL→iceberg type mapping; 3 in-module tests (`type_mapping_tests`).
- `alter.rs` — ALTER TABLE handlers (SET/UNSET TBLPROPERTIES, RENAME TO, schema evolution I6,
  I7 partition-field DDL, residual refusals) + the ALTER token rewrites the normalizer runs;
  9 in-module tests.
- `namespace_ddl.rs` — CREATE/DROP NAMESPACE|DATABASE + DROP TABLE handlers, the
  create-namespace hand parser, `consume_word`.
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into v1's positional `execute_with_read_only` call). Tests: [dialect/map.md](dialect/map.md).
- `extension.rs` — `SparkExtension: repark_core::SessionExtension` (`configure` = cardinality
  `repark.sql.*` config; `register` = `repark_functions::register_all` + analyzer rules; the
  DF-54.1 subquery guard stays a core session default, G8; repark-ta rider returns PR-4).
  Tests: [extension/map.md](extension/map.md).
- `normalize.rs` — token normalisers (`USING` strip, `PARTITIONED BY` extraction,
  `NAMESPACE`→`SCHEMA`, the ALTER rewrites + GenericDialect switch), statement sniffers,
  multi-statement refuse (BUG-010), MoR multi-spec DML gate (BUG-001), partition-spec builders.
  The MERGE star rewrite returns with `merge` (PR-3b).
- `spark_ast.rs` — the Spark passthrough: ORDER BY null-placement defaults, eager analysis,
  eager DML/`COPY` commands (F-BR-2), SEC-02 gate call. 6 in-module tests.
- `describe_show.rs` — Group Z `DESCRIBE NAMESPACE` + Group AB `SHOW NAMESPACES`
  (pyspark-4.0.0 v2-oracle-pinned rendering, LIKE patterns, secret redaction).
- `metadata_tables.rs` — I2 metadata-table path rewrite (`.snapshots` → `$snapshots`);
  15 in-module tests.
- `time_travel.rs` — I1 SQL-TEXT half: token span scan + FROM/JOIN splice to snapshot-pinned
  static providers; the pin half (spec/parsers/resolution/`read_table_at`) is
  `repark_core::time_travel`. 8 in-module tests (2 rode the phase-1 hoist).
- `local_fs_ddl.rs` — SEC-02 local-filesystem DDL gate (r24 SB1); 9 in-module tests.
- `catalog_ops.rs` — catalog lookup, P11 refusals, `iceberg_err`, path-escape reject, the
  r24 P7 `reregister*` provider-invalidation family (complete — PR-2 PARTIAL rider closed).
- `lib.rs` — manifest: module decls, `execute`/`SparkDialect`/`SparkExtension` re-exports, and
  the v1 domain-module `pub(crate) use` groups for the landed handler modules.

## I want to...

| ...do this | go to |
|---|---|
| Trace statement routing / refuse arms | `router.rs` |
| CTAS lowering / location policy | `ctas.rs` |
| Column-def CREATE / type mapping | `create_table.rs` |
| ALTER TABLE / token rewrites | `alter.rs` |
| Namespace / DROP TABLE DDL | `namespace_ddl.rs` |
| Pin a refuse message | `router/tests.rs` |
| ORDER BY / eager-command passthrough semantics | `spark_ast.rs` |
| Namespace introspection rendering | `describe_show.rs` |
| Time-travel span scanning | `time_travel.rs` (pin half: `repark-core/src/time_travel.rs`) |

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| Statement unexpectedly passes to DataFusion | `router.rs` arm order; `normalize::parse_single_normalized` returned `None` |
| Time-travel clause not rewritten | `time_travel::sql_has_time_travel` span scan (comments/strings never match) |
| P11 refusal missing | read-only set threading: `execute_with_read_only` → registry snapshot |
| Doc comment names a crate that doesn't exist | v1-port doc text re-homes to `repark_core` (verify-panel fix); report any straggler |

First checks: `cargo test -p repark-spark <module>::`. Escalate to: [../map.md#debug](../map.md).

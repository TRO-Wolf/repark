# map — repark-spark/src

## Purpose

Source for the Spark SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. Module bodies are ported from v1 `repark-sql` at the port-source pin (declared edit
classes only: prefix renames, seam adaptation). PR-3a restored the DDL handler modules
(`ctas`, `create_table`, `alter`, `namespace_ddl`) and completed `catalog_ops`/`normalize`;
PR-3b restored the DML/ref modules (`merge`, `insert_overwrite`, `ref_ddl`, `call`) — the
router now matches v1's execute family end-to-end. The MoR BUG-001 valve predicate is hoisted
to `repark_iceberg::write` (PR-3b declared rename); `normalize.rs` keeps the resolution
wrapper.

## Contents

- `router.rs` — `execute` / `execute_with_read_only` / `execute_inner` + pre-parse intercepts
  (alter I6/I7, create-namespace, describe/show, ref DDL) + the r25 T2 write-to-branch sniff;
  full v1 arm set ([router/map.md](router/map.md) for the tests).
- `merge.rs` — MERGE INTO lowering (sqlparser AST → `repark_iceberg::write::merge::MergeSpec`,
  star-sentinel rewrite); 10 in-module tests.
- `insert_overwrite.rs` — INSERT OVERWRITE: empty probe/validate/provider-wipe (C1-Q-001) +
  non-empty r23 OV1 stage-then-swap; 2 in-module tests (`assignment_type_unit_tests`).
- `ref_ddl.rs` — I5 snapshot-ref DDL (CREATE/DROP/REPLACE BRANCH|TAG, retention) + the
  write-to-branch sniff; 14 in-module tests.
- `call.rs` — I3 maintenance `CALL` procedures (expire_snapshots / rewrite_data_files /
  rollback_to_snapshot; LOCAL catalogs only); 3 in-module tests.
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
  `repark.sql.*` config; `register` = `repark_functions::register_all` + analyzer rules + the
  composed `repark_ta::TaExtension`, in v1 `build()`'s order; the DF-54.1 subquery guard stays a
  core session default, G8). The TA half is **composed, not re-implemented** — the TA set is
  door-neutral (design Q11), so this door installs the owning crate's extension.
  Tests: [extension/map.md](extension/map.md).
- `normalize.rs` — token normalisers (`USING` strip, `PARTITIONED BY` extraction,
  `NAMESPACE`→`SCHEMA`, the ALTER rewrites + GenericDialect switch), statement sniffers,
  multi-statement refuse (BUG-010), the MoR multi-spec DML gate's resolution wrapper (BUG-001
  — predicate hoisted to `repark_iceberg::write::refuse_mor_unpartitioned_multi_spec_dml`),
  the MERGE star rewrite call, partition-spec builders.
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
- `lib.rs` — manifest: module decls, `execute`/`SparkDialect`/`SparkExtension` re-exports, the
  v1 domain-module `pub(crate) use` groups, and the `#[cfg(test)]` root imports that
  reconstruct v1's crate-root scope for the battery's `use super::*`.
- `tests.rs` — the ported v1 lib-root battery (move-only identity unit, 334 census names:
  342 at the pin − 6 `postgres_p11_tests` (post-milestone-one) − 2 time-travel parser pins
  hoisted to repark-core in phase 1). Includes the `bug001_*` MoR-valve set and the
  `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` / `service_managed_ctas`
  groups.

## I want to...

| ...do this | go to |
|---|---|
| Trace statement routing / targeted refusals | `router.rs` |
| MERGE lowering / star sentinel | `merge.rs` |
| INSERT OVERWRITE probe / OV1 swap | `insert_overwrite.rs` |
| Branch/tag DDL, write-to-branch sniff | `ref_ddl.rs` |
| Maintenance CALL procedures | `call.rs` |
| CTAS lowering / location policy | `ctas.rs` |
| Column-def CREATE / type mapping | `create_table.rs` |
| ALTER TABLE / token rewrites | `alter.rs` |
| Namespace / DROP TABLE DDL | `namespace_ddl.rs` |
| Pin a router behavior end to end | `tests.rs` (lib-root battery) |
| Pin a door-native gate (TRUNCATE/P11/BUG-010) | `router/tests.rs` |
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

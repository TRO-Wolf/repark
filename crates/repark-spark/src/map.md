# map — repark-spark/src

## Purpose

Source for the Spark SQL door. `lib.rs` is a manifest (check_lib_rs); the router body lives in
`router.rs`. Module bodies are ported from v1 `repark-sql` at the port-source pin (declared edit
classes only: prefix renames, PR-2 refuse arms, seam adaptation).

## Contents

- `router.rs` — `execute` / `execute_with_read_only` / `execute_inner` + pre-parse intercepts
  and the PR-2 TEMPORARY refuse arms (each names its construct + restoring PR;
  [router/map.md](router/map.md) for the tests).
- `dialect.rs` — `SparkDialect: repark_core::SqlDialect` (seam adapter; unpacks `EngineContext`
  into v1's positional `execute_with_read_only` call). Tests: [dialect/map.md](dialect/map.md).
- `extension.rs` — `SparkExtension: repark_core::SessionExtension` (`configure` = cardinality
  `repark.sql.*` config; `register` = `repark_functions::register_all` + analyzer rules; the
  DF-54.1 subquery guard stays a core session default, G8; repark-ta rider returns PR-4).
  Tests: [extension/map.md](extension/map.md).
- `normalize.rs` — token normalisers (`USING` strip, `PARTITIONED BY` extraction,
  `NAMESPACE`→`SCHEMA`), statement sniffers, multi-statement refuse (BUG-010), MoR multi-spec
  DML gate (BUG-001). The ALTER/MERGE token rewrites return with their modules (PR-3a/3b).
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
- `catalog_ops.rs` — PR-2 PARTIAL rider: catalog lookup, P11 refusals, `iceberg_err`; the
  `reregister*` family + path-escape helper return with the PR-3a/3b handlers.
- `namespace_ddl.rs` — PR-2 PARTIAL rider: `consume_word` only (handlers return PR-3a).
- `lib.rs` — manifest: module decls + `execute`/`SparkDialect`/`SparkExtension` re-exports.

## I want to...

| ...do this | go to |
|---|---|
| Trace statement routing / refuse arms | `router.rs` |
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

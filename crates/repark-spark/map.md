# map — repark-spark

## Purpose

The **Spark SQL door** (tier 3): v1 `repark-sql` ported over the phase-1 seams. A statement
router (`execute` / `execute_with_read_only`) parses with DataFusion's `sqlparser` (Databricks
dialect + token-level normalisers for Spark-isms), intercepts the forms DataFusion cannot
execute against Iceberg, and passes everything else through the Spark passthrough
(`spark_ast` — ORDER BY null-placement defaults + eager analysis + eager DML/COPY commands).
`SparkDialect` adapts the router to `repark_core::SqlDialect`; `SparkExtension` installs the
v1 `build()` registrations (function registry + analyzer rules + cardinality/`repark.sql.*`
config) via `repark_core::SessionExtension`.

**PR-2 ports the SPINE only.** Live: DESCRIBE/SHOW namespace (Groups Z + AB), metadata tables
(I2), the time-travel scanner (I1 — the pin half lives in `repark_core::time_travel`), the
multi-statement / P11 / MoR-valve / SEC-02 guards, TRUNCATE targeted refuse, and the DML
passthrough. TEMPORARY refuse arms (loud `NotImplemented` naming construct + restoring PR):
CTAS, column-def CREATE TABLE, DROP TABLE, CREATE/DROP NAMESPACE, ALTER (PR-3a); MERGE,
INSERT OVERWRITE, CALL, branch/tag ref DDL (PR-3b). The v1 lib-root integration battery
(200 tests) and the census close ride PR-3b. `repark-ta` registration in `SparkExtension` is a
declared temporary omission restored in PR-4.

## Contents

- `Cargo.toml` — deps: repark-core, repark-iceberg, repark-functions, datafusion + fork family,
  regex (SHOW … LIKE), async-trait (dialect seam).
- [src/map.md](src/map.md) — module-by-module navigation.
- [tests/map.md](tests/map.md) — integration tests (Session + SparkExtension + SparkDialect;
  deferred test #1 lives here).

## I want to...

| ...do this | go to |
|---|---|
| Follow a SQL statement through the router | [src/map.md](src/map.md) → `router.rs` |
| Add/adjust a Spark-ism normaliser | `src/normalize.rs` |
| Change what the extension registers | `src/extension.rs` |
| See why a construct refuses with "lands in phase-2 PR-3x" | `src/router.rs` (TEMPORARY refuse arms) |

## Pointers

- Up: [../map.md](../map.md). Design: `../../docs/design/sql-doors.md`; brief:
  `../../briefs/phase-2-sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| `NotImplemented … lands in phase-2 PR-3a/3b` | Expected in PR-2 — the handler module has not landed yet (see src/router.rs refuse arms) |
| Spark ORDER BY nulls in the wrong place | The session must route through `SparkDialect` (plain `DataFusionDialect` keeps DF defaults) |
| Spark function unknown (`weekofyear`, …) | The session must install `SparkExtension` (register hook) |

First checks: `cargo test -p repark-spark`. Escalate to: [../map.md#debug](../map.md).

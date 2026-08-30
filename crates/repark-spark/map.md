# map — repark-spark

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

The **Spark SQL door** (tier 3): a statement
router (`execute` / `execute_with_read_only`) parses with DataFusion's `sqlparser` (Databricks
dialect + token-level normalisers for Spark-isms), intercepts the forms DataFusion cannot
execute against Iceberg, and passes everything else through the Spark passthrough
(`spark_ast` — ORDER BY null-placement defaults + eager analysis + eager DML/COPY commands +
the G5b / G5b-R temporal-`RANGE` conformance call).
`SparkDialect` adapts the router to `repark_core::SqlDialect`; `SparkExtension` installs the
`build()` registrations (function registry + analyzer rules + cardinality/`repark.sql.*`
config) via `repark_core::SessionExtension`, and **composes `repark_ta::TaExtension`** for the
TA window UDFs.

Live: CTAS + column-def CREATE TABLE, DROP
TABLE, namespace DDL, ALTER (I6/I7), MERGE INTO, INSERT OVERWRITE (empty + stage-then-swap), CALL
(I3), branch/tag ref DDL (I5) + the write-to-branch sniff, DESCRIBE/SHOW namespace (Groups
Z + AB), metadata tables (I2), the time-travel scanner (I1 — the pin half lives in
`repark_core::time_travel`), the multi-statement / P11 / MoR-valve / SEC-02 guards, TRUNCATE
targeted refuse, and the DML passthrough. The unit battery is under `src/tests/` (navigation
[src/tests/map.md](src/tests/map.md)).

## Contents

- `Cargo.toml` — deps: repark-core, repark-iceberg, repark-functions, repark-ta (feature
  `datafusion`, for the composed `TaExtension`), datafusion + fork family, regex (SHOW … LIKE),
  async-trait (dialect seam); dev-deps add chrono + futures (battery) and repark-common (the
  `surfaces` registry the `#[cfg(test)]` Q13 matrix audits this door against — dev-only because
  no shipped code reads it).
- [src/map.md](src/map.md) — module-by-module navigation.
- [tests/map.md](tests/map.md) — integration tests using Session + SparkExtension + SparkDialect.

## I want to...

| ...do this | go to |
|---|---|
| Follow a SQL statement through the router | [src/map.md](src/map.md) → `router.rs` |
| Add/adjust a Spark-ism normaliser | `src/normalize.rs` |
| Change temporal / unit-less `RANGE` window-frame semantics | `src/window_range.rs` |
| Change what the extension registers | `src/extension.rs` |
| See why a construct refuses | `src/router.rs` and the owning handler |

## Component contract

- **Owns:** the Spark SQL door — the statement router (`execute` / `execute_with_read_only`),
  `SparkDialect` (adapts to `repark_core::SqlDialect`), `SparkExtension` (installs the function
  registry + analyzer rules + cardinality config + `parse_float_as_decimal=true`, and composes `repark_ta::TaExtension`), the
  Spark-ism normalizers + the `spark_ast` passthrough. **U5:** `SparkExtension::configure`
  installs `spark.sql.ansi.enabled` (default TRUE).
- **Does not own:** the shared Iceberg machinery (repark-iceberg); the function / analyzer
  implementations (repark-functions); TA kernels (repark-ta); the ANSI door (no door↔door edge).
- **Public inputs:** a `SessionContext` + `CatalogRegistry` + Spark-dialect SQL text; via the seams, a
  session at build time.
- **Public outputs:** DataFusion `DataFrame`s; the installed Spark semantics on a session.
- **State & lifecycle:** per-call routing over an `EngineContext` snapshot; registrations installed
  once at build via `SparkExtension`.
- **Allowed internal deps:** `repark-core`, `repark-iceberg`, `repark-functions`, `repark-ta` (feature
  `datafusion`) — same-tier edges to functions / ta are legal. Dev-only: `repark-common` (surface
  matrix).
- **Failure model:** `DataFusionError` propagation + targeted loud refusals for unsupported / wrong-form
  statements; folds to the session taxonomy in core.
- **Extension points:** add / adjust a Spark-ism normalizer (`normalize.rs`); change what the
  extension registers (`extension.rs`); add a router arm (`router.rs` + a handler module).
- **Test strategy:** `cargo test -p repark-spark` — Session + `SparkExtension` + `SparkDialect`
  integration; the lib-root unit battery; the Q13 surface-matrix audit.
- **Known limitations:** open issues for this door are listed in
  [../../STATUS.md](../../STATUS.md) "Known correctness issues". Every divergence that has been
  *disposed of* — declared, or backlogged with an intent to fix — has its semantics, its pin and
  its rationale in the divergence registry
  ([../../docs/spark-sql-iceberg-parity.md](../../docs/spark-sql-iceberg-parity.md)); this door's
  statement-surface gaps are its §2. Link, never restate.

## Pointers

- Up: [../map.md](../map.md). Design: `../../docs/design/sql-doors.md`; brief:
  `../../docs/history/port-v2/phase-2-sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| Unexpected `NotImplemented` | Check the owning handler and its targeted refuse contract |
| Spark ORDER BY nulls in the wrong place | The session must route through `SparkDialect` (plain `DataFusionDialect` keeps DF defaults) |
| Spark function unknown (`weekofyear`, …) | The session must install `SparkExtension` (register hook) |
| `ta_ema`/`ta_adx`/… unknown | Same hook — `SparkExtension` composes `repark_ta::TaExtension`; a bare `SessionContext` has no TA UDFs |

First checks: `cargo test -p repark-spark`. Escalate to: [../map.md#debug](../map.md).

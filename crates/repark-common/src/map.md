# map — repark-common/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

Source for `repark-common` — shared types, the `Error` enum, and concise API contracts. See [../map.md](../map.md).

## Contents
- `tests.rs` — unit tests for Error / ErrorClass

- `surfaces.rs` — the dialect-neutral **SQL surface registry** (design
  `docs/design/sql-doors.md` §2 Q13, graft G2): 50 capability IDs (`CTAS`, `MERGE`,
  `TABLE_OPTION_PARTITIONING`, `GUARD_MULTI_STATEMENT`, `SEMANTICS_NULL_ORDERING`, …)
  named by CAPABILITY rather than by spelling, so one ID covers the ANSI `WITH (…)` form
  and the Spark `TBLPROPERTIES` form; plus
  `ALL` (the audit's universe), `Row { Tested { test, profile } | DeliberatelyAbsent { reason,
  adr } }`, `SessionProfile { Unit, Native, SparkExtended, TwoSession }` (graft G5 — evidence
  is only meaningful when the session profile is explicit) and `audit()`, which each door's
  `matrix.rs` calls from a `#[test]`. It lives here, at tier 0, because both tier-3 doors must
  reach it without a door→door edge (design §1). Tests: [surfaces/map.md](surfaces/map.md).

- `lib.rs` — `Error` (variants: `NotImplemented(String)` — the deterministic scope-gate /
  unsupported-feature class (U4: no longer a scaffolding placeholder; `engine_err` folds
  `DataFusionError::NotImplemented` + iceberg `FeatureUnsupported` into it, verbatim `{0}`);
  `DataFusion(String)` — the catch-all engine bucket; `Parse(String)` / `Analysis(String)` — the
  syntax / analysis-plan sub-classes split out so the PyO3 boundary can raise
  `repark.errors.ParseException` / `AnalysisException` (both render the inner engine text
  verbatim, `#[error("{0}")]`, preserving the diagnostic in `str(exc)`); `Config(String)` — the
  session/catalog config-mapping error for malformed `spark.sql.catalog.*` /
  `repark.sql.catalog.*` blocks and dual-prefix conflicts; messages name keys, not secret-bearing
  values; `Iceberg(String)` — the iceberg residual (commit conflicts, invalid data, unexpected —
  U4), verbatim `{0}` whose text leads with the structured iceberg kind name) + `Result<T>`. Plus
  `ErrorClass { Parse, Analysis, Unsupported, IllegalArgument, Base }` + `Error::exception_class()`
  — the WG-3/U4/Group-X error-taxonomy routing (`NotImplemented → Unsupported` →
  `repark.errors.UnsupportedOperationException`, the PySpark class for a JVM
  `UnsupportedOperationException`; **Group X:** `Config → IllegalArgument` →
  `repark.errors.IllegalArgumentException`, what live pyspark 4.0.0 raises for an invalid
  `SQLConf` value; `Iceberg → Base`): an **exhaustive, no-`_`** match so a new
  variant fails to compile until explicitly routed to a Python exception partition (the "no
  silent default arm" guarantee). Stays at the bottom of the DAG: no heavy deps, so engine errors
  are carried as a formatted string and **classified** into the
  parse/analysis/unsupported/iceberg/base partition by the originating crate
  (`repark-session::engine_err` / `classify_iceberg_error`, which inspect the live
  `DataFusionError` / iceberg `ErrorKind`) before conversion. **Error-boundary honesty
  (C1-CRATE-001):** not every crate returns `repark_common::Error` end-to-end today —
  intermediate layers still surface `iceberg::Result` / `DataFusionError` and fold here.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo check -p repark-common`. Escalate to: [../map.md#debug](../map.md).

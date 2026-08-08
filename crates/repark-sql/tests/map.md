# map — repark-sql/tests

## Purpose

Integration tests for the ANSI door. The end-to-end door battery lives IN the crate
(`../src/tests.rs`) because it drives `crate::execute` against an `EngineContext` directly; what
belongs out here is what must be observed from outside the crate.

## Contents

- `parser_productions.rs` — the R1 spike, kept as assertions. Pins that every production M1
  depends on parses on the stock DataFusion-re-exported sqlparser (Generic dialect), and that the
  three PR-6 productions (`ALTER … SET PROPERTIES`, `ALTER … EXECUTE`, `FOR … AS OF`) still do
  NOT — so PR-6's recognizer obligations stay honest, and an upstream parser change is a visible
  signal rather than a silent redundancy.

- `session_wiring.rs` — the door's REACHABILITY: `AnsiDialect` installed on a real
  `ReparkSession` through `ReparkSessionBuilder::with_sql_dialect`, driving schema DDL, CTAS,
  INSERT and a typed read through `session.sql`, plus a refusal that must survive the session
  boundary. It lives out here because "reachable through a session" is precisely what a unit
  test calling `AnsiDialect.execute(...)` on a bare `SessionContext` cannot show
  (`surfaces::SQL_DIALECT_SEAM`).

- `introspection.rs` (PR-6, Q8) — `SHOW TABLES` / `DESCRIBE` / `information_schema` DELEGATED
  through the door, on a session whose `information_schema` was enabled the product way
  (`.config("datafusion.catalog.information_schema", "true")` — the repark-core R2 fix PR-6
  landed). Carries the negative half (without the conf the same door refuses, so the delivery is
  attributable to the fix) and the honest caveat row (`$`-suffixed metadata tables currently
  enumerate; the filter decision is an open fork/core product question, not a door parser). Also
  carries the leak pin: a `FOR … AS OF` read must leave no `__repark_ansi_tt_*` relation behind,
  which only became observable once this PR turned `information_schema` on.

- `ta_toll.rs` (PR-6, Q11) — `TaExtension` on a **native** session, one kernel driven through
  ANSI-door SQL as a window function and compared `f64::to_bits` against the recorded C TA-Lib
  golden, plus the non-literal-period refuse and the "absent until you opt in" row. Needs the
  `repark-ta` dev-dep (feature `datafusion`).

- `cross_door.rs` (PR-6, Q13 / graft G5) — the **two-session** cross-door protocol: a native
  `AnsiDialect` session and a Spark-extended `SparkDialect` session, each over its OWN in-memory
  catalog, compared on the Arrow path (value AND type). Rows: CTAS, INSERT, ALTER (schema
  evolution + table rename), MERGE, time travel, identifier case folding, the single-session
  legality boundary (pure catalog DDL), and the session-scope guard rail that explains why one
  session cannot do this job. Needs the `repark-spark` dev-dep — the ONLY place either door may
  name the other, and legal because the crate-DAG guard scopes layering to normal edges.

## Pointers

- Up: [../map.md](../map.md). Spike record: `../../../task/p2f-ansi-m1-ledger.md`;
  PR-6 record: `../../../task/p2g-ansi-m2-ledger.md`. Seam freeze + the session-scope rule:
  `docs/design/session-api.md`.

## Debug

| Symptom | First check |
|---|---|
| `m2_productions_still_need_a_pre_parse_recognizer` RED | Good news — upstream learned the form. Revisit the PR-6 plan, then update the pin |
| An M1 production stopped parsing | The matching handler is now unreachable; check the DataFusion/sqlparser version bump |
| `session_wiring` RED on the catalog-visible read | The dialect is probably not installed (session default fell back to `DataFusionDialect`, whose CTAS makes a `MemTable`) |
| `introspection` RED with "not supported unless information_schema is enabled" | The repark-core builder→`SessionConfig` plumbing (`apply_datafusion_config_keys`) regressed; check `cargo test -p repark-core --lib builder_datafusion` first |
| `cross_door` RED on ONE door only | The doors' lowerings drifted — that is the row doing its job (design §6 R3). Compare the two handlers, do not relax the assertion |
| `extensions_are_session_scoped_not_dialect_scoped` RED | Extension scoping changed. Every `TwoSession` matrix row in BOTH doors needs re-reading before anything else |
| `ta_toll` RED on bit-exactness | Compare against `crates/repark-ta/tests/goldens.rs` first — if THAT is green, the divergence is in the window-UDF wrapper or the door, not the kernel |

First checks: `cargo test -p repark-sql --test parser_productions`,
`cargo test -p repark-sql --test session_wiring`, `--test introspection`, `--test ta_toll`,
`--test cross_door`.
Escalate to: [../map.md#debug](../map.md).

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

## Pointers

- Up: [../map.md](../map.md). Spike record: `../../../task/p2f-ansi-m1-ledger.md`.

## Debug

| Symptom | First check |
|---|---|
| `m2_productions_still_need_a_pre_parse_recognizer` RED | Good news — upstream learned the form. Revisit the PR-6 plan, then update the pin |
| An M1 production stopped parsing | The matching handler is now unreachable; check the DataFusion/sqlparser version bump |
| `session_wiring` RED on the catalog-visible read | The dialect is probably not installed (session default fell back to `DataFusionDialect`, whose CTAS makes a `MemTable`) |

First checks: `cargo test -p repark-sql --test parser_productions`,
`cargo test -p repark-sql --test session_wiring`.
Escalate to: [../map.md#debug](../map.md).

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

## Pointers

- Up: [../map.md](../map.md). Spike record: `../../../task/p2f-ansi-m1-ledger.md`.

## Debug

| Symptom | First check |
|---|---|
| `m2_productions_still_need_a_pre_parse_recognizer` RED | Good news — upstream learned the form. Revisit the PR-6 plan, then update the pin |
| An M1 production stopped parsing | The matching handler is now unreachable; check the DataFusion/sqlparser version bump |

First checks: `cargo test -p repark-sql --test parser_productions`.
Escalate to: [../map.md#debug](../map.md).

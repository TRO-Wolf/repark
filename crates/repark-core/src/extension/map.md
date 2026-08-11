# map — repark-core/src/extension

## Purpose

File-backed tests for the registration seam (`../extension.rs`): `SessionExtension` hook ORDER
during `ReparkSessionBuilder::build` and the no-extension pure-DataFusion baseline (new-seam
tests, additive — not part of the ported v1 census). The tests drive `ReparkSession`, so they
wire up (with `extension.rs` itself) when the session module wires.

## Contents

- `tests.rs` — configure-then-register order pin + default-noop-hooks pin
  (`#[cfg(test)] mod tests;` in `../extension.rs`). The order pin also asserts the H-1a split B
  addition: `configure` receives a `SessionBuildConf` carrying the RESOLVED
  `spark.sql.session.timeZone` beside the raw conf map, and the test sets a PADDED value so a
  door that re-parsed the map instead of taking the resolved one would be visible.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| Spark functions / TA UDFs missing on a session | No extension installed — phase 1 has no-op hooks; the phase-2 Spark door ships the extension holding v1's inline registrations. |
| A door needs another value `build()` already computed | Add it to `SessionBuildConf` rather than re-deriving it in the door. That struct exists so "resolved once at build" stays literally true (H-1a split B, for the session timezone). |

First checks: `cargo test -p repark-core extension`. Escalate to: [../map.md#debug](../map.md).

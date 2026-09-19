# map — repark-core/src/time_travel

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

File-backed tests for the hoisted time-travel pins (`../time_travel.rs`): `TimeTravelSpec`
parsing (`parse_version_value` / `parse_timestamp_to_ms`) and snapshot resolution. Hoisted
MOVE-ONLY from the v1 SQL crate's `time_travel` module at the port-source pin; the SQL-text
rewrite half (and its tests) is deferred with the phase-2 statement router — see
`task/port/deferred-tests.md`.

## Contents

- `tests.rs` — parser + resolution pins (`#[cfg(test)] mod tests;` in `../time_travel.rs`).
- `sql_text.rs` — SQL-text timestamp parsing, zone math, token extraction (re-exported at
  `../time_travel.rs`). pins: ice-tt-resolve-1/C-010
- `sql_ast.rs` — column-refusal check of the `AS OF` expression.
  pins: ice-tt-resolve-1/C-010
- `sql_eval.rs` — constant-expression evaluation of the `AS OF` value (`evaluate_sql_timestamp_asof`).
  **ICE-TT-RESOLVE-1 round 3 (2026-09-19):** non-determinism decided from the planned
  expression (`Volatility::Volatile` scalar functions, subquery plans included), not a name
  list. pins: ice-tt-resolve-1/C-003
  **ICE-TT-RESOLVE-1 round 3 (2026-09-19):** every string (reader `timestampAsOf`, bare
  SQL literals, `Utf8` scalars) resolves through the engine `CAST(... AS TIMESTAMP)` in the
  session zone; the hand parser is gone. pins: ice-tt-resolve-1/C-002
  pins: ice-tt-resolve-1/C-010

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `TIMESTAMP AS OF` string fails to parse | Accepted forms: epoch seconds (reader integers), anything the engine `CAST(... AS TIMESTAMP)` takes in the session zone — a cast failure is the `INPUT` refusal naming the text. |

First checks: `cargo test -p repark-core time_travel`. Escalate to: [../map.md#debug](../map.md).

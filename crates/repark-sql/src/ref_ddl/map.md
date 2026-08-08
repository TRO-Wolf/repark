# map — repark-sql/src/ref_ddl

## Purpose

File-backed tests for `../ref_ddl.rs` — branch / tag DDL.

Recognizer pins only: which statements this door claims, what it parses them into, and which
malformed shapes refuse loud instead of falling through to an opaque parse error. The execution
half is the tier-1 `ManageSnapshots` seam, pinned end to end in `../tests.rs`.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../ref_ddl.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A statement was not recognized | `does_not_claim_other_statements` lists what must NOT be claimed; the top-level `CREATE BRANCH b IN t` is Spark-door surface |
| A quoted ref name matched a keyword | it must not — a quoted identifier becomes `Sig::Quoted`, and `Sig::keyword` matches bare words only |

First checks: `cargo test -p repark-sql ref_ddl::`. Escalate to: [../map.md#debug](../map.md).

# map — repark-sql/src/ref_ddl

## Purpose

File-backed tests for `../ref_ddl.rs` (branch and tag DDL).

Recognizer pins only: which statements this door claims, what it parses them into, and which
malformed shapes refuse loud instead of falling through to an opaque parse error. The execution
half is the tier-1 `ManageSnapshots` seam, pinned end to end in `../tests.rs`.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../ref_ddl.rs`.
  pins: ref-branch-tag-wap/C-003

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A statement was not recognized | `does_not_claim_other_statements` lists what must NOT be claimed; the top-level `CREATE BRANCH b IN t` is Spark-door surface |
| A quoted ref name matched a keyword | it must not — a quoted identifier becomes `Sig::Quoted`, and `Sig::keyword` matches bare words only |
| A statement ran with a clause the recognizer did not read | `reject_trailing` refuses on ANY leftover token (`trailing_non_identifier_tokens_refuse_too`); the ONE exception is a trailing `;` (`a_trailing_semicolon_is_not_a_trailing_clause`) |
| `WITH SNAPSHOT RETENTION n SNAPSHOTS k DAYS` lost the age half | it must not — `parses_both_snapshot_retention_halves` holds both fields; the age half is taken only when a count and a duration unit follow, so the reversed order stays a refused trailing clause (`reversed_snapshot_retention_order_refuses`). pins: ref-branch-tag-wap/C-003 |

First checks: `cargo test -p repark-sql ref_ddl::`. Escalate to: [../map.md#debug](../map.md).

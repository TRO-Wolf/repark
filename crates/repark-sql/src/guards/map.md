# map — repark-sql/src/guards

## Purpose

File-backed tests for `../guards.rs`. The guard set. Every guard is a behavior and every REFUSAL is a behavior, so each refusal
message class has its own test alongside an acceptance case proving the guard is not simply
refusing everything.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../guards.rs`. The text/plan guards are
  pinned at unit level (they read scrubbed text or a `LogicalPlan`); **the G3-E8
  subquery-predicate DML valve** is pinned BOTH at unit level (`parsed()` feeds it the same
  `Statement` the router passes) and **end to end** through `crate::execute` over a real
  memory-catalog Iceberg table — the end-to-end row is the one that asserts the table is
  untouched after a refusal, which is the whole point of a data-loss valve. It lives here rather
  than in `../tests.rs` because that file is at its `scripts/check_rust_file_size.py` ceiling,
  and because this IS the guard's home. Six pins in all: detector, verb/target message, the
  parsed-target rendering (quoted / FROM-less / comment-bearing spellings), the end-to-end
  refuse, the **valve-ORDER** pin against BUG-001 (`mor_valve_runs_after_the_g3e8_valve`, the
  ANSI mirror of the Spark door's), and `router_parse_dialect_matches_the_session_default` —
  the attachment-class net: this door's router parse and the parse `delegate` plans must stay
  the same dialect, because a guard wired to a parse the executor does not use is fail-open
  (the class that produced the Spark door's bypass). The `AnsiDoor` harness is shared by the two
  end-to-end pins.
  **G15 (2026-08-12):** five collation pins — expression `COLLATE`, `ORDER BY COLLATE`,
  `CREATE TABLE` column `COLLATE`, a string-literal negative, and an end-to-end refuse +
  default `SELECT 1` untouched. Ledger:
  [`../../../../task/y7-collation-refuse-ledger.md`](../../../../task/y7-collation-refuse-ledger.md).

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A guard fires on a string literal | it cannot — the guards read scrubbed text; check `../scan.rs`'s tests |
| A delegated DELETE/UPDATE with a subquery `WHERE` was refused | By design (G3-E8). It over-refuses the uncorrelated-scalar spelling deliberately: the correlated twin is the same parse tree and empties the table. `task/g3e8-guard-ledger.md` lists every over-refused spelling |
| A delegated DELETE/UPDATE was not gated by the BUG-001 valve | the wrapper only resolves 3+-part names against a REGISTERED catalog; `mor_valve_wrapper_passes_what_it_cannot_or_must_not_gate` lists every pass-through branch. Note it now runs INSIDE the router's `DELETE`/`UPDATE` arm, after G3-E8 — a statement that does not parse to `Delete`/`Update` no longer reaches it (it gets the parse error instead, which is the more informative one) |
| A DML guard did not run at all | Check WHICH parse the statement took. `router.rs` parses with `PARSER_DIALECT`; `delegate` re-parses through `create_logical_plan` with the session's `sql_parser.dialect`. They are the same today and `router_parse_dialect_matches_the_session_default` keeps them so — if that pin ever reds, every guard in the arm is fail-open for the forms the two parsers disagree about (the Spark door's L1 M-1 bypass class) |

First checks: `cargo test -p repark-sql guards::`. Escalate to: [../map.md#debug](../map.md).

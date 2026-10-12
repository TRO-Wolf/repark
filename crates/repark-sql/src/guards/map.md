# map — repark-sql/src/guards

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

File-backed tests for `../guards.rs`. Every guard and refusal is a behavior, so each refusal
message class has its own test alongside an acceptance case proving the guard is not simply
refusing everything.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../guards.rs`. The text/plan guards are
  pinned at unit level (they read scrubbed text or a `LogicalPlan`); **the G3-E8
  subquery-predicate DML valve** is pinned at unit level too (`parsed()` feeds it the same
  `Statement` the router passes): the detector, verb/target message, and the parsed-target
  rendering (quoted / FROM-less / comment-bearing spellings, also covering still-refused scalar /
  UPDATE IN). `router_parse_dialect_matches_the_session_default` is the attachment-class net:
  this door's router parse and the parse `delegate` plans must stay the same dialect, because a
  guard wired to a parse the executor does not use is fail-open (the class that produced the
  Spark door's bypass). **G15:** unit collation pins — expression `COLLATE`, `ORDER BY COLLATE`,
  `CREATE TABLE` column `COLLATE`, `CAST AS STRING COLLATE`, SET / parenthesized SET, and a
  string-literal negative. The MoR-valve wrapper test passes a parsed `Statement`.
- `ansi_door.rs` — the `#[cfg(test)] mod ansi_door;` declared in `../guards.rs`. The `AnsiDoor`
  harness (a live door over a memory-catalog Iceberg table) and every end-to-end pin that drives
  it through `crate::execute`: the G3-E8 end-to-end refuse (the row that asserts the table is
  untouched after a refusal, which is the whole point of a data-loss valve), the IN-DELETE
  execute pin (`dml_subquery_in_delete_executes_and_deletes_exactly_the_match`), the NOT IN +
  3VL execute pin (`dml_subquery_not_in_delete_executes_and_honors_three_valued_logic`), the
  EXISTS ± correlation execute pin
  (`dml_subquery_exists_delete_executes_uncorrelated_and_correlated`), the correlated-IN +
  identity-UPDATE execute pin (`dml_subquery_correlated_in_and_update_in_execute`), the
  **valve-ORDER** pin against BUG-001 (`mor_valve_runs_after_the_g3e8_valve`, also covering
  aggregate IN), and the G15 end-to-end refuse + default `SELECT 1` untouched (also covering
  CAST + SET). It lives here rather than in `../tests.rs` because that file is at its
  `scripts/check_rust_file_size.py` ceiling, and because this IS the guards' home; it lives
  apart from `tests.rs` because that file hit its own ceiling first (the live-door family moved
  out verbatim under C-4 step 2).

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A guard fires on a string literal | it cannot — the guards read scrubbed text; check `../scan.rs`'s tests |
| A delegated DELETE/UPDATE with a subquery `WHERE` was refused | By design (G3-E8 residual). Uncorrelated `DELETE … col IN` / `NOT IN (SELECT …)`, `[NOT] EXISTS` ± correlation, correlated IN, and identity `UPDATE … IN` execute. Other spellings stay refused. |
| A delegated DELETE/UPDATE was not gated by the BUG-001 valve | the wrapper completes short names from session defaults and gates the resulting REGISTERED catalog; `mor_valve_wrapper_passes_what_it_cannot_or_must_not_gate` lists every pass-through branch. Note it now runs INSIDE the router's `DELETE`/`UPDATE` arm, after G3-E8 — a statement that does not parse to `Delete`/`Update` no longer reaches it (it gets the parse error instead, which is the more informative one) |
| A DML guard did not run at all | Check WHICH parse the statement took. `router.rs` parses with `PARSER_DIALECT`; `delegate` re-parses through `create_logical_plan` with the session's `sql_parser.dialect`. They are the same today and `router_parse_dialect_matches_the_session_default` keeps them so — if that pin ever reds, every guard in the arm is fail-open for the forms the two parsers disagree about (the Spark door's L1 M-1 bypass class) |
| An end-to-end pin overflows the test-thread stack (`fatal runtime error: stack overflow`) | The pin's body future holds the whole nested execution tree (~1.8MB in a debug build) on the 2MB stack while the deep DELETE/CTAS chain polls beneath it, so any codegen shift can tip it. `dml_subquery_exists_delete_executes_uncorrelated_and_correlated` heap-boxes its body through the sync `boxed_dml_subquery_exists_delete` (a sync boundary is what separates the fat construction from the deep poll — `Box::pin` at an await site does not). Copy that shape if another fat pin tips; do not `RUST_MIN_STACK` the suite. |

First checks: `cargo test -p repark-sql guards::`. Escalate to: [../map.md#debug](../map.md).

**ICE-TT-RESOLVE-1 round 2 (2026-09-19):** call sites use the 3-arg `EngineContext::new` again; the zone flows only through `new_with_time_zone`. pins: ice-tt-resolve-1/C-003

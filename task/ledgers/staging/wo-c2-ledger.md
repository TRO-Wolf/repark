# Unit ledger — WO-C2 · SHOW CREATE TABLE refusal contracts

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-5.6-terra · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the WO-C2 implementation commit lands.

**Why now.** The PR #810 critic found weak refusal pins and a lexer-error fallthrough in the
new `SHOW CREATE TABLE` intercept. The recorded Spark 4.1.2 probes in `m4.json` define the
class, condition, SQLSTATE, and exact answer or refusal for this repair.

**Not in this unit:** view behavior, `describe_show.rs`, registry changes, Cargo files, version
changes, AWS commands, pushes, or rebases.

## Plan

- [ ] Make every recognized malformed `SHOW CREATE TABLE` form return Spark's typed invalid-statement parse error.
- [ ] Refuse a four-part table name as Spark's typed table-or-view-not-found analysis error.
- [ ] Strengthen Rust refusal pins to assert error variants and complete messages.
- [ ] Replace permissive near-miss tests with exact answer or refusal pins.
- [ ] Add facade class, condition, SQLSTATE, and complete-message pins.
- [ ] Sweep branch-changed tests for permissive error assertions and run the required gates.

## PROPOSITION LEDGER — WO-C2 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Once `SHOW CREATE TABLE` is recognized, a missing target, bad target, bad `AS` word, trailing text, or tokenizer failure returns `INVALID_STATEMENT_OR_CLAUSE` / `42601` as a `DataFusionError::SQL(ParserError::ParserError(_, _))` parse path. | Parser unit pins and execute-path pins. | OPEN | Recorded m4 `bare`, `trailing_junk`, `as_other`, and four unclosed-identifier rows. |
| C-002 | A four-part `SHOW CREATE TABLE` name returns `TABLE_OR_VIEW_NOT_FOUND` / `42P01` as an analysis path, naming all four quoted parts. | Parser and execute-path exact-message pins. | OPEN | Recorded m4 `four_part`. |
| C-003 | AS SERDE, missing-table, and bare-table Rust pins assert variant, condition, SQLSTATE, and complete message. | `tests/show_create.rs` typed refusal pins. | OPEN | Recorded m4 `as_serde`, `missing`, and `bare`. |
| C-004 | Every listed near miss has one exact RePark outcome; the three neighboring SHOW forms keep their exact schemas. | Named Rust answer/refusal pins. | OPEN | Views remain owned by V-SHOW-CREATE. |
| C-005 | The Python facade reports Spark's exception class, condition, SQLSTATE, and complete native message for AS SERDE, a missing table, and bare SHOW CREATE TABLE. | `test_show_create_table.py` facade pins. | OPEN | Native parser errors map to `ParseException`; plan errors map to `AnalysisException`. |


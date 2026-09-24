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

- [x] Make every recognized malformed `SHOW CREATE TABLE` form return Spark's typed invalid-statement parse error.
- [x] Refuse a four-part table name as Spark's typed table-or-view-not-found analysis error.
- [x] Strengthen Rust refusal pins to assert error variants and complete messages.
- [x] Replace permissive near-miss tests with exact answer or refusal pins.
- [x] Add facade class, condition, SQLSTATE, and complete-message pins.
- [x] Sweep branch-changed tests for permissive error assertions and run the required gates.

## PROPOSITION LEDGER — WO-C2 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Once `SHOW CREATE TABLE` is recognized, a missing target, bad target, bad `AS` word, trailing text, or tokenizer failure other than an unclosed bracketed comment (WO-C5/C-002 and WO-C10/C-001 answer `UNCLOSED_BRACKETED_COMMENT`) returns `INVALID_STATEMENT_OR_CLAUSE` / `42601` as a `DataFusionError::SQL(ParserError::ParserError(_))` parse path. | Parser unit pins and execute-path pins. | PROVEN | `show_create.rs` and `tests/show_create.rs` pin bare, trailing, invalid-`AS`, and four malformed identifiers. |
| C-002 | A four-part `SHOW CREATE TABLE` name returns `TABLE_OR_VIEW_NOT_FOUND` / `42P01` as an analysis path, naming all four quoted parts. | Parser and execute-path exact-message pins. | PROVEN | Parser and execute pins name `ice`.`sales`.`x`.`t`. |
| C-003 | AS SERDE, missing-table, and bare-table Rust pins assert variant, condition, SQLSTATE, and complete message. | `tests/show_create.rs` typed refusal pins. | PROVEN | `show_create.rs` uses SQL and Plan variant helpers with complete measured messages. |
| C-004 | Every listed near miss has one exact RePark outcome; the three neighboring SHOW forms keep their exact schemas. | Named Rust answer/refusal pins. | PROVEN | Six named Rust pins cover both view forms, SHOW CREATE, SHOW TABLES, SHOW COLUMNS, and SHOW TBLPROPERTIES. |
| C-005 | The Python facade reports Spark's exception class, condition, SQLSTATE, and complete native message for AS SERDE, a missing table, and bare SHOW CREATE TABLE. | `test_show_create_table.py` facade pins. | PROVEN | Four facade tests pass; parser wrapper extraction exposes `INVALID_STATEMENT_OR_CLAUSE`. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-c2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each charter clause is pinned by the parser, execute-path, or facade tests against the recorded Spark 4.1.2 contract.
      artifacts: [crates/repark-spark/src/show_create.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_show_create_table.py]
    - id: AT-2
      status: ATTACKED
      evidence: The tests exercise bare, trailing, invalid-AS, four-part, and four malformed quoted identifier shapes, plus near-miss heads that must not intercept.
      artifacts: [crates/repark-spark/src/show_create.rs, crates/repark-spark/src/tests/show_create.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Parser errors and planning errors are separately asserted by DataFusion variant, while the facade asserts their native Python classes and SQLSTATE values.
      artifacts: [crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_show_create_table.py]
    - id: AT-4
      status: N/A
      justification: This parser and error-mapping repair adds no shared state, ordering rule, or concurrent operation.
    - id: AT-5
      status: N/A
      justification: The repair accepts SQL text only and adds no authorization, credential, filesystem, or secret-handling surface.
    - id: AT-6
      status: ATTACKED
      evidence: The four-part and missing-table paths preserve Spark's quoted identifier spelling and SQLSTATE compatibility without changing table data.
      artifacts: [crates/repark-spark/src/show_create.rs, crates/repark-spark/src/tests/show_create.rs]
    - id: AT-7
      status: N/A
      justification: The raw three-word prefix check is bounded and this unit adds no loop, allocation growth, or resource-owning path.
    - id: AT-8
      status: ATTACKED
      evidence: Spark's measured conditions and SQLSTATE values are preserved through DataFusion errors and the native Python exception API.
      artifacts: [crates/repark-spark/src/show_create.rs, python/repark/src/repark/errors.py, python/repark/tests/test_show_create_table.py]
    - id: AT-9
      status: ATTACKED
      evidence: Exact error messages retain the condition and SQLSTATE that callers use to diagnose a refusal.
      artifacts: [crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_show_create_table.py]
    - id: AT-10
      status: ATTACKED
      evidence: Unit and execute-path tests make every new recognized-error branch observable, and the branch-wide sweep removed permissive error acceptance.
      artifacts: [crates/repark-spark/src/show_create.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_describe_table.py]
  complete: true
```

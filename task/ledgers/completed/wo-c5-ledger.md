# Unit ledger — WO-C5 · SHOW CREATE TABLE parser refusal pins

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-6-luna · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger is completed and frozen in `task/ledgers/completed/`.

## PROPOSITION LEDGER — WO-C5 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark multi-statement refusals expose SQLSTATE `42601` and retain the SQL parser error variant. | Exact router message and Python exception contract. | PROVEN | `bug010_multi_statement_refuses_parse_class`; `test_parser_wrapper_refusals_report_the_native_error_condition`. |
| C-002 | An unclosed bracketed comment after the SHOW CREATE TABLE head returns Spark's `UNCLOSED_BRACKETED_COMMENT` parser message. | Pin the error variant, full Rust parser and rendered messages, Python type, condition, SQLSTATE, and full text. | PROVEN | `show_create_unclosed_bracketed_comments_keep_spark_parse_class`; `test_show_create_unclosed_bracketed_comment_has_spark_parse_contract`. |
| C-003 | An unclosed comment that hides TABLE falls through to the tokenizer error, and SHOW CREATE TABLE followed by another statement keeps Spark's invalid-statement answer. | Pin both exact outcomes and their parser error class. | PROVEN | `show_create_comment_before_table_keyword_keeps_tokenizer_fallthrough`; `show_create_multi_statement_keeps_spark_invalid_statement_class`; matching facade pins. |
| C-004 | Refusal tests touched by WO-C5 pin error variants or exception types and complete error messages. | Audit all five test files changed on the branch. | PROVEN | Updated Rust SHOW CREATE refusals and the multi-statement router pin; Python SHOW CREATE and multi-statement pins. Other checked refusals already pin the complete contract or are constructor/parser mechanism tests. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-c5
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 through C-004 have named Rust and Python refusal pins.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-2
      status: ATTACKED
      evidence: The tests cover multi-statement SQL, unclosed comments after the SHOW CREATE head, and a comment that hides TABLE.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal pins assert parser variants or exception types and complete error text.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-4
      status: N/A
      justification: This parser and rendering unit adds no shared state, lock, task, or ordering-sensitive write.
    - id: AT-5
      status: N/A
      justification: This parser and rendering unit adds no credential, network, authorization, or destructive operation path.
    - id: AT-6
      status: ATTACKED
      evidence: Exact parser classes, conditions, SQLSTATEs, and complete messages are pinned across Rust and Python entry points.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
```

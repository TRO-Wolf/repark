# Unit ledger — WO-C10 · front-door unclosed bracketed comment contract

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-5.6-terra · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger is completed and frozen in `task/ledgers/completed/` when WO-C10's parser and facade pins merge.

## PROPOSITION LEDGER — WO-C10 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every non-hint unclosed bracketed SQL comment reaches Spark's `UNCLOSED_BRACKETED_COMMENT` parser contract at the Spark front door. | Pin the Rust parser variant and complete rendered message for the measured input set. | PROVEN | `unclosed_bracketed_comments_use_spark_parser_contract`; `show_create_unclosed_before_table_keywords_use_spark_parse_contract`; facade parser-contract rows. |
| C-002 | Closed nested comments, quoted comment markers, and line-comment markers keep their measured one-row answers; an unclosed hint falls through. | Pin exact single rows and the hint's existing parser outcome. | PROVEN | `bracketed_comment_near_misses_keep_exact_single_rows` on Rust and Python; the exact hint tokenizer outcome is pinned in `malformed_multi_statement_near_misses_keep_exact_outcomes`. |
| C-003 | The router's malformed multi-statement near misses assert complete exact outcomes. | Pin the unclosed-comment contract and both IPI-51 parser divergences by equality. | PROVEN | `malformed_multi_statement_near_misses_keep_exact_outcomes` compares all three exact RePark outcomes. The unterminated quote and unclosed hint remain the measured IPI-51 caret-block divergences. |
| C-004 | The touched tokenizer-failure and refusal assertions either pin full exact text or are recorded for later measurement. | Sweep the branch diff and classify every matching assertion. | PROVEN | The `origin/main...HEAD` sweep found the required shortcut, an unrelated property check, the removed absence-only assertion, two full IPI-51 tokenizer pins, and two removed Python assertions. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-c10
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The front-door parser rule has Rust and facade pins.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-2
      status: ATTACKED
      evidence: Nested, quoted, line-comment, and hint boundaries have explicit pins.
      artifacts: [crates/repark-spark/src/tests/router.rs, python/repark/tests/test_e1_errorclass.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every new refusal pin compares its complete rendered text.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-4
      status: N/A
      justification: This parser unit adds no shared state, lock, task, or ordering-sensitive write.
    - id: AT-5
      status: N/A
      justification: This parser unit adds no credential, network, authorization, or destructive operation path.
    - id: AT-6
      status: ATTACKED
      evidence: Rust and Python pins cover the parser class, SQLSTATE, rendered text, and rows.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-7
      status: N/A
      justification: This unit makes no performance claim and adds no data scan or background task.
    - id: AT-8
      status: ATTACKED
      evidence: The error condition and SQLSTATE match the measured Spark contract.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-9
      status: ATTACKED
      evidence: Exact messages prevent a fallback tokenizer answer from passing.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
    - id: AT-10
      status: ATTACKED
      evidence: The named tests execute every clause input through the Spark router.
      artifacts: [crates/repark-spark/src/tests/router.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py, python/repark/tests/test_show_create_table.py]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-9, AT-10]
```

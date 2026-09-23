# Unit ledger — WO-C10 · front-door unclosed bracketed comment contract

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-5.6-terra · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `task/ledgers/completed/` when WO-C10's parser and facade pins merge.

## PROPOSITION LEDGER — WO-C10 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every non-hint unclosed bracketed SQL comment reaches Spark's `UNCLOSED_BRACKETED_COMMENT` parser contract at the Spark front door. | Pin the Rust parser variant and complete rendered message for the measured input set. | OPEN | Implement and test the shared guard. |
| C-002 | Closed nested comments, quoted comment markers, and line-comment markers keep their measured one-row answers; an unclosed hint falls through. | Pin exact single rows and the hint's existing parser outcome. | OPEN | Test the scanner boundaries and hint fall-through. |
| C-003 | The router's malformed multi-statement near misses assert complete exact outcomes. | Pin the unclosed-comment contract and both IPI-51 parser divergences by equality. | OPEN | Replace the absence-only assertion. |
| C-004 | The touched tokenizer-failure and refusal assertions either pin full exact text or are recorded for later measurement. | Sweep the branch diff and classify every matching assertion. | OPEN | Record the sweep result in the completed ledger. |

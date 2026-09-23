# Unit ledger — WO-C3 · SHOW CREATE TABLE critic remediation

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-5.6-terra · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the WO-C3 remediation commit lands.

**Why now.** The second critic for SHOW CREATE TABLE found five incomplete pins and one raw-text
comment scanner gap. The recorded Spark 4.1.2 probes `m2.json`, `m4.json`, `m8.json`, and
`m9.json` define this repair's answers and refusal contracts.

**Not in this unit:** STATUS, views, `describe_show.rs`, Cargo files, version changes, AWS commands,
pushes, or rebases.

## Plan

- [x] Replace the raw SHOW CREATE prefix split with a nested-comment-aware scanner and sweep matching raw keyword checks.
- [ ] Pin full RePark refusal text and narrow the SHOW CREATE parity claim for IPI-51's caret-block residue.
- [ ] Pin the complete multi-term sort-order CREATE text from the measured m2 answer.
- [ ] Pin ParserError condition extraction for the measured INSERT BY NAME and multi-statement shapes.
- [ ] Pin complete SHOW TABLES, SHOW COLUMNS, and SHOW TBLPROPERTIES rows and Arrow types.

## PROPOSITION LEDGER — WO-C3 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A SHOW CREATE TABLE lexical failure remains the typed invalid-statement refusal when line or nested bracket comments occur before or between its keywords. | Rust pre-check and parser pins plus facade labels from m8. | PROVEN | `comment_aware_show_create_*` and `test_show_create_comments_*`; the shared scanner also replaces router's raw default-SET precheck. |
| C-002 | SHOW CREATE parse refusals expose Spark's condition, SQLSTATE, and first line while retaining RePark's exact no-caret rendering. | Rust and facade exact-message pins; registry scope statement. | OPEN | IPI-51 owns caret rendering. |
| C-003 | The multi-term sort-order fixture matches the complete measured m2 CREATE text after only catalog/name/location substitution. | Exact Rust CREATE-text pin. | OPEN | `write.distribution-mode=range` is part of Spark's output. |
| C-004 | ParserError-wrapped parse refusals report their bracketed condition without classifying malformed wrappers. | Facade and direct native-exception pins. | OPEN | INSERT BY NAME and multi-statement text remains owned elsewhere. |
| C-005 | SHOW TABLES, SHOW COLUMNS, and SHOW TBLPROPERTIES near misses preserve complete Spark row shapes and Arrow field types. | Exact Rust rows and schema pins. | OPEN | No view behavior changes. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-c3
  categories:
    - id: AT-1
      status: OPEN
      evidence: Pending the Rust and facade pins for the five critic findings.
      artifacts: []
  complete: false
```

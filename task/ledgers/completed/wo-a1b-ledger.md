# Unit ledger — WO-A1b

**Date:** 2026-09-23 · **Branch:** `xd/show-tblprops` · **Base:** `92c03ada` (`origin/main`)
**Model:** Codex (gpt-5.6-terra) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

## Plan

- Record the exact current refusal and near-miss paths for the SHOW TABLE EXTENDED parser.
- Preserve lexer errors for the exact statement head and render their Spark parse-class text.
- Resolve a PARTITION pattern as one literal table name before the partition-management refusal.
- Strengthen Rust, facade, and dbt pins to assert one typed, full outcome per probe.
- Run the requested Rust, Python, structural, and comment-ban gates before departure.

## Risk ledger

- A lexer error must not cause the parser to claim a near-miss statement.
- A PARTITION wildcard must not select an unrelated existing table.
- Exact pins must retain the exception variant, condition, SQLSTATE, and complete message.
- Existing SHOW TABLES, SHOW TBLPROPERTIES, and SHOW CREATE paths must remain unchanged.

## Proposition ledger

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Lexer failures after the exact SHOW TABLE EXTENDED head return Spark parse errors while every near miss returns `None`. | Parser unit pins for three delimiters and five near misses. | **PROVEN** | `cargo test -p repark-spark show_table_extended` passes the delimiter and near-miss pins. |
| C-002 | PARTITION uses its pattern as one literal table name and preserves the shared scope failure contract. | End-to-end Rust pins for present, absent, wildcard, and scope cases. | **PROVEN** | Rust end-to-end pins cover present, absent, literal wildcard, and shared scope outcomes. |
| C-003 | Rust and facade refusal pins assert the typed full error contract, and each swept test has one exact outcome. | Targeted Rust, facade, dbt, and structural gates. | **PROVEN** | Requested Rust, facade, dbt, structural, comment-ban, and `make verify` gates pass. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

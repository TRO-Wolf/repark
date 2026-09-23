# Unit ledger — DESCRIBE-COLUMN-1 · Spark `DESCRIBE table column` rows and time-travel refusals

## Round 1 (2026-09-23)

**Date:** 2026-09-23 · **Branch:** `xd/describe` · **Base:** `origin/main` ·
**Model:** Codex (gpt-5.6-terra) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Scope:** The Spark hand parser accepts one column identifier after a table name, returns Spark's
three `info_name` / `info_value` rows, preserves qualified and default-namespace facade expansion,
and refuses Spark's time-travel tails at parse altitude. The table-level DESCRIBE rows, router
intercept, `SHOW` surfaces, and partition-information section stay unchanged.

## Plan

- [ ] S0: ledger and red-first Rust and Python pins.
- [ ] S1: parser carrier and time-travel refusal.
- [ ] S2: column execution module and error rows.
- [ ] S3: facade expansion for one column tail.
- [ ] S4: registry, maps, gates, and ledger retirement.

## PROPOSITION LEDGER — DESCRIBE-COLUMN-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DESCRIBE [TABLE] [EXTENDED|FORMATTED] t col` parses one plain, quoted, or dotted column path and leaves unrelated tails on their existing doors. | Parser pins include table, namespace, function, query, JSON, and two-word tail near misses. | **OPEN** | Live Spark 4.1.2 measurement supplied in the work order. |
| C-002 | A top-level column answers non-null `info_name` / `info_value` rows for name, Spark DDL type, and comment text or literal `NULL`. | End-to-end memory-catalog pins cover comment, no comment, EXTENDED, FORMATTED, struct, case, and backticks. | **OPEN** | Live Spark 4.1.2 measurement supplied in the work order. |
| C-003 | A nested path and a missing column refuse with Spark's measured class and text. | End-to-end pins cover `st.a` and `nope`, including suggestion order. | **OPEN** | Live Spark 4.1.2 measurement supplied in the work order. |
| C-004 | `VERSION AS OF`, `TIMESTAMP AS OF`, and `FOR VERSION AS OF` return the measured `PARSE_SYNTAX_ERROR` near token. | Parser and session pins cover all four measured forms. | **OPEN** | Live Spark 4.1.2 measurement supplied in the work order. |
| C-005 | The facade retains a single column tail while qualifying both a three-part and a default-namespace table name. | Python facade pins cover qualified and bare table forms. | **OPEN** | Live Spark 4.1.2 measurement supplied in the work order. |
| C-006 | The parity registry and every touched map describe the delivered scope, and the required gates and comment-ban check pass. | Registry and map diffs plus recorded gate exits. | **OPEN** | Pending final validation. |

## Self Logic Review — SLR-001

The hand parser owns the new grammar because the router already receives its parsed carrier. The
column execution lives outside `describe_show.rs` to preserve its file-size ceiling. Missing-column
text uses the existing Spark error helper; suggestions use Spark's measured similarity order.

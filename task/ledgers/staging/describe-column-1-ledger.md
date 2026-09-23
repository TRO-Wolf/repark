# Unit ledger — DESCRIBE-COLUMN-1 · Spark `DESCRIBE table column` rows and time-travel refusals

## Round 1 (2026-09-23)

**Date:** 2026-09-23 · **Branch:** `xd/describe` · **Base:** `origin/main` ·
**Model:** Codex (gpt-5.6-terra) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Scope:** The Spark hand parser accepts one column identifier after a table name, returns Spark's
three `info_name` / `info_value` rows, preserves qualified and default-namespace facade expansion,
and refuses Spark's time-travel tails at parse altitude. Round 2 adds Spark's identity-partition
DESCRIBE section and CREATE-family owner stamping. `SHOW` surfaces stay unchanged.

## Plan

- [x] S0: ledger and red-first Rust and Python pins.
- [x] S1: parser carrier and time-travel refusal.
- [x] S2: column execution module and error rows.
- [x] S3: facade expansion for one column tail.
- [x] S4: registry, maps, and B1 gates.
- [ ] S5: red-first identity partition and owner pins.
- [ ] S6: identity partition section.
- [ ] S7: owner stamping and reserved-property refusal.
- [ ] S8: Owner metadata row and property filtering.
- [ ] S9: registry, maps, B2 gates, and ledger retirement.

## PROPOSITION LEDGER — DESCRIBE-COLUMN-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DESCRIBE [TABLE] [EXTENDED|FORMATTED] t col` parses one plain, backticked, or dotted column path and leaves unrelated tails on their existing doors. | Parser pins include table, namespace, function, query, JSON, and two-word tail near misses. | **PROVEN** | `cargo test -p repark-spark` exits 0; the parser pins cover the claimed forms and near misses. |
| C-002 | A top-level column answers non-null `info_name` / `info_value` rows for name, Spark DDL type, and comment text or literal `NULL`. | End-to-end memory-catalog pins cover comment, no comment, EXTENDED, FORMATTED, struct, case, and backticks. | **PROVEN** | `cargo test -p repark-spark` exits 0; the column session pin covers every measured answer. |
| C-003 | A nested path and a missing column refuse with Spark's measured class and text. | End-to-end pins cover `st.a` and `nope`, including suggestion order. | **PROVEN** | `cargo test -p repark-spark` exits 0; the refusal pin checks both measured texts. |
| C-004 | `VERSION AS OF`, `TIMESTAMP AS OF`, and `FOR VERSION AS OF` return the measured `PARSE_SYNTAX_ERROR` near token. | Parser and session pins cover all four measured forms. | **PROVEN** | `cargo test -p repark-spark` exits 0; the parser and session pins check each near token. |
| C-005 | The facade retains a single column tail while qualifying both a three-part and a default-namespace table name. | Python facade pins cover qualified and bare table forms. | **PROVEN** | `make develop` and `.venv/bin/python -m pytest python/repark/tests/test_describe_table.py -q` exit 0. |
| C-006 | The parity registry and every touched map describe the delivered scope, and the required gates and comment-ban check pass. | Registry and map diffs plus recorded gate exits. | **PROVEN** | `make rust-clippy` and `make rust-panic-ban` exit 0. Maps, size, comment-ban, Rust test, and facade test gates also exit 0. |

## Self Logic Review — SLR-001

The hand parser owns the new grammar because the router already receives its parsed carrier. The
column execution lives outside `describe_show.rs` to preserve its file-size ceiling. Missing-column
text uses the existing Spark error helper; suggestions use Spark's measured similarity order.

## Validation (2026-09-23)

`cargo test -p repark-spark`, `python3 scripts/check_rust_file_size.py`,
`bash scripts/check_map_md.sh`, `make check-map-md`,
`python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xd-show origin/main HEAD`, and
`.venv/bin/python -m pytest python/repark/tests/test_describe_table.py -q` exit 0.
`make rust-clippy` and `make rust-panic-ban` exit 0. This ledger remains in `staging/` for Round 2.

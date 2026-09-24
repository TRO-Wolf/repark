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
- [x] S5: red-first identity partition and owner pins.
- [x] S6: identity partition section.
- [x] S7: owner stamping and reserved-property refusal.
- [x] S8: Owner metadata row and property filtering.
- [x] S9: registry, maps, B2 gates, and ledger retirement.

## PROPOSITION LEDGER — DESCRIBE-COLUMN-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DESCRIBE [TABLE] [EXTENDED|FORMATTED] t col` parses one plain, backticked, or dotted column path and leaves unrelated tails on their existing doors. | Parser pins include table, namespace, function, query, JSON, and two-word tail near misses. | **PROVEN** | `cargo test -p repark-spark` exits 0; the parser pins cover the claimed forms and near misses. |
| C-002 | A top-level column answers non-null `info_name` / `info_value` rows for name, Spark DDL type, and comment text or literal `NULL`. | End-to-end memory-catalog pins cover comment, no comment, EXTENDED, FORMATTED, struct, case, and backticks. | **PROVEN** | `cargo test -p repark-spark` exits 0; the column session pin covers every measured answer. |
| C-003 | A nested path and a missing column refuse with Spark's measured class and text. | End-to-end pins cover `st.a` and `nope`, including suggestion order. | **PROVEN** | `cargo test -p repark-spark` exits 0; the refusal pin checks both measured texts. |
| C-004 | `VERSION AS OF`, `TIMESTAMP AS OF`, and `FOR VERSION AS OF` return the measured `PARSE_SYNTAX_ERROR` near token. | Parser and session pins cover all four measured forms. | **PROVEN** | `cargo test -p repark-spark` exits 0; the parser and session pins check each near token. |
| C-005 | The facade retains a single column tail while qualifying both a three-part and a default-namespace table name. | Python facade pins cover qualified and bare table forms. | **PROVEN** | `make develop` and `.venv/bin/python -m pytest python/repark/tests/test_describe_table.py -q` exit 0. |
| C-006 | The parity registry and every touched map describe the delivered scope, and the required gates and comment-ban check pass. | Registry and map diffs plus recorded gate exits. | **PROVEN** | The full B2 gate set exits 0 on 2026-09-23. |
| C-007 | SHOW TABLE EXTENDED `information` gains `Owner: <session user>` between `Provider:` and `Table Properties:` once CREATE stamps the owner, Spark 4.1.2's measured answer (`u4a_live.json` `ste_*` rows); the inherited #816 pins carry it. | The eight Rust `show_table_extended_*` pins and the Python and dbt SHOW TABLE EXTENDED pins expect the session owner, derived from the session owner source, in full-text `information`. | **PROVEN** | `cargo test -p repark-spark show_table_extended` and `pytest python/dbt-repark/tests/test_statement_surface.py` exit 0 on 2026-09-23; the catalog-surface pin carries the Owner line but stays red on its inherited `Location:` line, which predates MEM-LAYOUT-1. |

## Self Logic Review — SLR-001

The hand parser owns the new grammar because the router already receives its parsed carrier. The
column execution lives outside `describe_show.rs` to preserve its file-size ceiling. Missing-column
text uses the existing Spark error helper; suggestions use Spark's measured similarity order.

## Validation (2026-09-23)

`cargo test -p repark-spark`, `make rust-clippy`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py`, `bash scripts/check_map_md.sh`,
`python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xd-show origin/main HEAD`, and
`.venv/bin/python -m pytest python/repark/tests/test_describe_table.py -q` exit 0.
`make develop` exits 0 before the facade test. The required owner-property grep found no
additional exact creation-property-set pin to update; the old session-owner pins now assert
creation-time ownership and direct unowned-table omission.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: describe-column-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The Rust and facade pins check each delivered DESCRIBE and owner clause against the recorded Spark rows and exact refusal text.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs, python/repark/tests/test_describe_table.py]
    - id: AT-2
      status: ATTACKED
      evidence: The partition pins exercise one and two identity fields, mixed identity and days transforms, bucket transforms, and an unpartitioned table.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs]
    - id: AT-3
      status: ATTACKED
      evidence: CREATE and CTAS reject only lowercase owner before catalog access, while Owner and owner.x remain stored properties.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs, crates/repark-spark/src/create_table.rs, crates/repark-spark/src/ctas.rs]
    - id: AT-4
      status: N/A
      justification: The unit changes synchronous table metadata construction and DESCRIBE rendering only. It adds no shared state, lock, async task, or ordering dependency.
    - id: AT-5
      status: N/A
      justification: The unit adds no credential, authorization, network, filesystem, or deserialization surface. Reserved-property validation is local planning logic.
    - id: AT-6
      status: ATTACKED
      evidence: The direct catalog table pin verifies backward compatibility for metadata without owner, and extended rendering omits only the reserved owner property.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs, crates/repark-spark/src/describe_show.rs]
    - id: AT-7
      status: N/A
      justification: DESCRIBE builds a bounded row list from table schema and partition metadata. Table creation adds one property lookup and no input-scaled execution path.
    - id: AT-8
      status: ATTACKED
      evidence: Exact Spark partition headers, owner metadata value, filtered property output, and the reserved-property error contract are pinned through Rust and Python entry points.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs, python/repark/tests/test_describe_table.py]
    - id: AT-9
      status: ATTACKED
      evidence: The reserved-property pins assert the full planning error, including its Spark error class text and SQLSTATE, before any catalog error can mask it.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The new pins ran red before implementation and green after it; each new partition and owner branch has a named input that changes observed rows or planning output.
      artifacts: [crates/repark-spark/src/tests/describe_owner.rs, python/repark/tests/test_describe_table.py, task/ledgers/staging/describe-column-1-ledger.md]
  complete: true
```

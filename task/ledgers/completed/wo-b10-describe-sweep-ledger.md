# Unit ledger — WO-B10 · DESCRIBE-COLUMN-1 class sweep after the restack onto PR A

**Date:** 2026-09-23 · **Branch:** `xd/describe` · **Base:** `xd/show-tblprops` (`3c55a589`)
**Model:** Claude (claude-opus-5-5) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** completed with the final WO-B10 commit.

**Why now.** The critics of PR C and PR A found five defect classes: unpinned near misses,
under-pinned near-miss answers, superseded tokenizer pins, untested scanner branches, and
superseded or carried-over map rows. This sweep applies each class to PR B's own diff and
applies the orchestrator's 19:20 ruling on four-part DESCRIBE names.

**Oracle.** Every near miss and refusal pinned here was measured on Spark 4.1.2 with Iceberg
1.11.0 (`ice` hadoop catalog; the view cells use an in-memory catalog). The answers are recorded in
`/tmp/xo-xo-opus65/wo/spark-4.1.2-b10-measured.json`, together with the RePark facade answers
at the measurement head. An answer that differs from Spark is not pinned; it is listed as a
hand-back question.

## Plan

- [x] Step -1: restack the 38 commits onto PR A's head `3c55a589`.
- [x] Step 0b: a four-part name whose last part names a metadata table, with no column tail,
  goes to the #806 metadata-table path; re-pin the unknown-suffix rows; update the IPI-23 ledger.
- [x] Classes 1-4: measure, pin, and remove the branches no input can distinguish.
- [x] Class 5: remove superseded and carried-over map and ledger rows.

## PROPOSITION LEDGER — WO-B10 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A four-part DESCRIBE name whose last part names a metadata table, with no column tail, leaves `try_parse_describe_table` for the #806 metadata-table path. Every other four-part name answers `TABLE_OR_VIEW_NOT_FOUND` / `42P01` naming the written parts. | Parser and session pins for `snapshots`, `SNAPSHOTS`, `DESC`, missing base, unknown suffix, EXTENDED unknown suffix, and a non-metadata column tail. | PROVEN | `four_part_metadata_suffix_answers_metadata_rows` and `four_part_missing_base_and_unknown_suffix_name_the_written_parts`; `describe_metadata_table_real_table_at_written_path_wins` and every `test_ice_mt_describe_1.py` pin pass. |
| C-002 | Every added near miss of the column-tail, identity-partition, owner, and view arms answers Spark 4.1.2's measured rows or refusal, and every success pin asserts the full Arrow schema. | Session pins compare schema and rows, or the complete refusal text. | PROVEN | `describe_near_miss.rs`, the `describe_owner.rs` additions, and the `test_describe_table.py` `_answer` pins. |
| C-003 | No test or ledger row in B's diff pins `TokenizerError` text. An unclosed `/*` answers `[UNCLOSED_BRACKETED_COMMENT] ... SQLSTATE: 42601` with condition, SQLSTATE, and full string. | Grep of B's diff; Rust and Python front-door pins. | PROVEN | `unclosed_bracketed_comment_answers_the_front_door_refusal`; `test_describe_unclosed_comment_answers_unclosed_bracketed_comment`; the non-table inputs are pinned only as classifier fall-through. |
| C-004 | Each branch of B's hand-written scanners has a test that fails when the branch is mutated. Branches no input can distinguish are removed without changing any measured answer. | Recorded mutation runs over the quote scan, head classifier, facade tail check, and partition-information scan. | PROVEN | Killed mutants: comment skip, in-quote guard, quote close, `query` and `function` heads, the metadata-suffix arm, the partition-information header and `# col_name` skip, and the facade whitespace strip. Removed: the doubled-quote lookahead, the unreachable DESCRIBE guard in `sql_has_time_travel`, and the blank-row reset. |
| C-005 | B's Rust row helpers assert the full Arrow schema of every DESCRIBE answer they read. | `describe_rows` and `describe_column_rows` assert names, types, and nullability. | PROVEN | `describe_owner.rs`, `describe_table.rs`, and `describe_column_errors.rs` helpers. |
| C-006 | The maps and ledgers in B's diff hold only B's current rows: no duplicate of a C or A row, no row naming a missing file, and no text describing replaced behaviour. | Review of every map and ledger in `git diff 3c55a589..HEAD`. | PROVEN | Removed the duplicate SHOW CREATE rows in `scripts/map.md` and `python/repark/tests/map.md`, the misplaced `show_create.rs` and `time_travel.rs` sentences, and the repark-core row for A's code. The describe rows are restated to the current behaviour, and the WO-B4/WO-B6 ledger text is trued. |
| C-007 | WO-B11: the Rust-door `DESCRIBE cat.ns.t.snapshots`, bare or with a trailing line or block comment, answers Spark 4.1.2's six snapshots rows; SELECT from a metadata path keeps the rewrite, and the four-part column tail, a literal `dc$snapshots`, and an unknown suffix keep their answers. | Session pins with full schema and rows; near-miss pins; a mutation run of the router guard. | PROVEN | `four_part_metadata_suffix_answers_metadata_rows` and `metadata_rewrite_still_serves_every_other_metadata_path`; reverting the guard fails the first. |
| C-008 | WO-B11: `DESCRIBE t PARTITION (spec)` with no column, on an Iceberg table, answers `[_LEGACY_ERROR_TEMP_1111] DESCRIBE does not support partition for v2 tables.`; a missing table answers 42P01, a view answers its rows, and a column tail keeps `DESC_TABLE_COLUMN_PARTITION`. | Rust session pins, a Python end-to-end pin, and one parser case per scan branch. | PROVEN | `partition_spec_without_column_answers_spark_v2_refusal`, `partition_only_tail_scan_covers_each_branch`, `test_describe_table_partition_without_column_is_v2_refusal`; every scan-branch mutant fails. |
| C-009 | WO-B11: an unresolved DESCRIBE column names itself with doubled backticks re-escaped and orders suggestions as Spark 4.1.2 does. | Rust and Python full-string pins. | PROVEN | `unresolved_column_re_escapes_backticks_in_spark_order`, `test_describe_table_doubled_backtick_column_is_re_escaped`; both mutants fail. |
| C-010 | WO-B11: an identity partition source that is not a bare identifier is written back-quoted in the Partition Information rows; a bare name stays unquoted. | Full-row Rust pin. | PROVEN | `identity_partition_rows_quote_only_names_that_need_quoting`. |

## Self Logic Review — SLR-001

The four-part arm keeps the column tail on B's path because the ruling covers only
`column is None`. Spark answers column rows for a metadata table with a column tail, so that shape is a
hand-back question and stays unpinned. The removed branches were each shown equivalent: a doubled quote
closes and reopens the same quote; the front door refuses every DESCRIBE time-travel tail before the
rewrite runs; an EXTENDED answer always follows the partition-information blank row with a section header.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-b10-describe-sweep
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every pinned near miss was measured on Spark 4.1.2 before it was pinned.
      artifacts: [crates/repark-spark/src/tests/describe_near_miss.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Four-part, column-tail, comment, keyword, quote, and whitespace shapes are enumerated.
      artifacts: [crates/repark-spark/src/tests/describe_near_miss.rs, python/repark/tests/test_describe_table.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusals pin condition, SQLSTATE, and the full text; divergent answers stay unpinned.
      artifacts: [crates/repark-spark/src/tests/describe_near_miss.rs, python/repark/tests/test_ice_mt_describe_1.py]
    - id: AT-4
      status: N/A
      evidence: The sweep adds no shared mutable state.
      justification: Parser and test changes only.
    - id: AT-5
      status: N/A
      evidence: No catalog, credential, or file access changes.
      justification: Parser and test changes only.
    - id: AT-6
      status: ATTACKED
      evidence: Facade pins cover the tail-check branches and the listColumns identity flags.
      artifacts: [python/repark/tests/test_describe_table.py, python/repark/tests/test_catalog_surface.py]
    - id: AT-7
      status: ATTACKED
      evidence: The scanners stay single forward passes; removed branches shorten them.
      artifacts: [crates/repark-spark/src/describe_column.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Mutation runs killed each remaining scanner branch.
      artifacts: [crates/repark-spark/src/tests/describe_near_miss.rs]
    - id: AT-9
      status: N/A
      evidence: No logging or metrics change.
      justification: Parser and test changes only.
    - id: AT-10
      status: ATTACKED
      evidence: The scoped Rust, facade, map, ledger, format, clippy, and comment-ban gates were run.
      artifacts: [crates/repark-spark/src/tests/describe_near_miss.rs]
  complete: true
```

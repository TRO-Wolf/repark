# Unit ledger — SHOW-TABLE-EXTENDED-1

**Date:** 2026-09-23 · **Branch:** `xd/show-tblprops` · **Base:** `92c03ada` (`origin/main`)
**Model:** Codex (gpt-5.6-terra) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

> **Errata (2026-09-23, WO-A5):** `/tmp/xd-create-scratch/m13.json` measures the two numeric
> suffix errors and the malformed TIMESTAMP literal on Spark 4.1.2. The TIMESTAMP producer text
> matches. `FNP-4B-NUMERIC-RANGE-MESSAGE-1` remains a named residue: Spark strips the suffix,
> names the type and bounds, includes advice and SQLSTATE `22003`; RePark keeps its existing
> qualified-token producer text without SQLSTATE.

## WO-A6 near-miss sweep

| Recognizer / near miss | Pin |
|---|---|
| `try_parse_show_table_extended`: truncated `SHOW TABLE EXTENDED`, `... LIKE`, `IN`, unclosed pattern quote, unclosed PARTITION, extra `)`, and trailing text after LIKE or PARTITION | Existing `parse_refuses_required_syntax_shapes`; new `parse_near_misses_keep_exact_answers` |
| `try_parse_show_table_extended`: empty pattern, lowercase keywords, terminal `--` comment, and unclosed comment | New `parse_near_misses_keep_exact_answers` pins the parser-level answers, including `None` for an unclosed comment. End to end, the router front door (WO-C10) answers an unclosed `/*` with `UNCLOSED_BRACKETED_COMMENT` / `42601`; `show_table_extended_near_miss_probes_keep_their_exact_outcomes` pins that full rendered text. These cases were not separately measured: unmeasured pins. |
| `try_parse_show_table_extended` wrong-case boundary, `SHOW TABLES EXTENDED`, `SHOW TABLE EXTENDEDX` | Existing `show_table_extended_near_miss_probes_keep_their_exact_outcomes`; lowercase acceptance is in `parse_near_misses_keep_exact_answers`. |
| Comment-aware keyword scanner: inter-keyword unclosed comment, comment-ending line, and keyword near misses | Existing `comment_aware_keyword_scanner_leaves_show_table_near_misses_alone` and `show_table_extended_near_miss_probes_keep_their_exact_outcomes`; terminal line comment is in `parse_near_misses_keep_exact_answers`. |
| `spark_parse_message` bracket prefix: `[`, `[X`, leading-space ` [X] y`, empty message | New `parser_error_bracket_near_misses_keep_complete_messages`; all were unmeasured. |
| Router / SHOW CREATE intercept routing: wrong-case keywords, `SHOW TABLES EXTENDED`, `SHOW TABLE EXTENDEDX`, and `SHOW TABLE EXTENDED IN` | Existing `show_table_extended_near_miss_probes_keep_their_exact_outcomes` and `parse_near_misses_keep_exact_answers`. `IN` currently enters the intercept and returns a syntax refusal; full refusal is pinned by the latter. |

Spark 4.1.2 (orchestrator run m14, 2026-09-23; `sc.ns.x` exists) accepts `SHOW TABLE EXTENDED IN sc.ns LIKE 'x';` with rows and columns `[namespace, tableName, isTemporary, information]`; it also accepts the same query with `;;` and `; ;`. `SELECT 1;;` returns one row `[1]`. RePark matches the `;;` and `; ;` answers, pinned by `parse_near_misses_keep_exact_answers` and `show_table_extended_near_miss_probes_keep_their_exact_outcomes`.

Spark 4.1.2 refuses `SHOW TABLE EXTENDED IN sc.ns LIKE 'x';;x` with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'x': extra input 'x'. SQLSTATE: 42601`. RePark refuses it at the front-door multi-statement guard with `SQL error: ParserError("[PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported (Spark parity). Only a single statement is accepted; a trailing semicolon, whitespace, or comment after that statement is allowed. SQLSTATE: 42601")`. This is a known divergence, repo-wide and out of scope. The query does not reach the SHOW TABLE EXTENDED intercept. No parser logic change is needed.

## WO-A8 front-door alignment

- The router front door (WO-C10) answers an unclosed `/*` with `UNCLOSED_BRACKETED_COMMENT` /
  `42601` before this parser runs: leading, inter-keyword and trailing forms are pinned by
  `show_table_extended_near_miss_probes_keep_their_exact_outcomes` and
  `show_table_extended_scanner_stops_at_an_unterminated_block_comment`. The two end-to-end unclosed-comment probes pin that full
  rendered text; no SHOW TABLE EXTENDED pin claims a tokenizer outcome for an unclosed comment.
- The near-miss tests moved to `tests/show_table_extended_near_miss.rs`. Their successful
  fall-throughs compare the complete Arrow schema and every row, and every SHOW TABLE EXTENDED
  answer compares its complete four-column Arrow schema.
- The facade `Location` pin follows the MEM-LAYOUT-1 memory-catalog layout
  `<warehouse>/ns1/entity`.

## WO-A5 follow-up audit

- **V-002 strengthened:** `show_table_extended_tracks_snapshot_and_plain_information` now
  compares the post-INSERT complete row. `show_table_extended_filters_alternation_case_and_ambient_scope`
  now compares complete ordered rows for alternation, case folding, and ambient scope.
- **V-002 checked and left:** `show_table_extended_answers_exact_partitioned_information`,
  `show_table_extended_keeps_v3_and_unicode_property_scalars`,
  `show_table_extended_lists_sorted_tables_and_excludes_views`,
  `show_table_extended_reports_location_management_owner_and_tree`, and
  `show_table_extended_skips_leading_and_inter_keyword_comments` already compare whole rows.
  The three SHOW TABLE EXTENDED refusal helpers, the SHOW TBLPROPERTIES refusal, and the
  near-miss probes already compare full messages. The facade and dbt row pins compare whole
  Arrow rows; the branch-added parser refusal pins in `column_move.rs`, `nested_column_ddl.rs`,
  `router.rs`, `truncate.rs`, `test_e1_errorclass.py`, and `test_show_create_table.py` already
  compare complete messages.
- **V-003 citation audit:** `crates/repark-spark/src/map.md` keeps `wo-a1b/C-001` for the lexer
  pins and now uses an ordinary test-file pointer. `crates/repark-spark/src/tests/map.md` cites
  `wo-a1b/C-002, C-003` for literal PARTITION and full refusal pins, and
  `show-table-extended-1/C-001, C-002, C-003, C-004` for parser, metadata, scope, and facade/dbt
  pins. `python/repark/tests/map.md` cites `wo-a1b/C-003` for facade refusals. Its WO-A4 note
  re-cites no WO-C3 or WO-C5 clause; the INSERT BY NAME (`wo-c3/C-004`) and multi-statement
  (`wo-c5/C-001`) citations stay on the base rows.
- **V-005 provenance:** C-001 and C-003 cite `/tmp/xo-xo-opus61/measure/u4a_live.json`; C-002
  cites that measurement and `/tmp/xd-create-scratch/m13.json`; C-004 is facade-derived with the
  recorded `DBT-TBLPROPS-1` leg. The deep tree now has a complete Rust row pin whose tree lines
  match `/tmp/xd-create-scratch/m13.json` key `ste_deep`; the row omits Spark's `Owner:` line
  (residue R-U4-16).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

## Plan

- Add red Rust pins for the parsed grammar, four-column result, information text, scope,
  error, filtering, property, and schema-tree cases.
- Add the token-level parser and route it before sqlparser.
- Build the metadata text from the shared Spark property view and the Arrow schema.
- Add the facade and dbt statement-surface pins, then correct the parity registry row.
- Run the requested Rust, Python, structural, and comment-ban gates before departure.

## Risk ledger

- Parser recognition must not claim `SHOW TABLES`, `SHOW TBLPROPERTIES`, or SHOW CREATE forms.
- Scope lookup must preserve the existing `SHOW TABLES` ambient and missing-namespace contracts.
- Metadata output must preserve Spark's line order, Unicode character splitting, and trailing newlines.
- Nested schemas must use Spark type names rather than DDL names.

## Proposition ledger

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The Spark dialect recognizes only `SHOW TABLE EXTENDED [IN\|FROM namespace] LIKE 'pattern' [PARTITION (...)]` before sqlparser. | Parser and near-miss pins. | **PROVEN** | Spark 4.1.2's recorded leg `/tmp/xo-xo-opus61/measure/u4a_live.json` keys `ste_from`, `ste_no_in`, `ste_no_like`, and `ste_partition_spec` shows the measured grammar boundary. Facade-derived parser pins cover the remaining keyword and delimiter permutations. pins: show-table-extended-1/C-001 |
| C-002 | The executor returns Spark's four columns and Spark's Iceberg metadata text line for line except Spark's session-user `Owner:` line, which RePark prints only when the table carries an `owner` property (residue R-U4-16), including sorted table names, property redaction, Unicode scalar splitting, snapshot changes, and the schema tree. | Memory-catalog end-to-end rows and exact-string pins. | **PROVEN** | Spark 4.1.2's `/tmp/xo-xo-opus61/measure/u4a_live.json` keys `ste_plain`, `ste_after_insert`, `ste_all_star`, `ste_location`, and `ste_v3` record the primitive rows. `/tmp/xd-create-scratch/m13.json` keys `ste_nested` and `ste_deep` match the nested and deep tree lines. `tests/show_table_extended.rs` pins both trees. Every recorded Spark row carries `Owner: john`; the RePark pins (e.g. `show_table_extended_tracks_snapshot_and_plain_information`) omit it, which is R-U4-16. pins: show-table-extended-1/C-002 |
| C-003 | Scope resolution and refusal behavior match the stated Spark surface without widening to views or session temporary views. | Ambient, missing namespace, partition, filtering, alternation, case, and view pins. | **PROVEN** | Spark 4.1.2's recorded `/tmp/xo-xo-opus61/measure/u4a_live.json` keys `ste_alt`, `ste_case`, `ste_from`, `ste_missing_ns`, `ste_partition_spec`, `ste_temp`, and `ste_view` show the scoped outcomes. The Rust pins cover the same rows and refusals. Session temporary views remain the named residue. pins: show-table-extended-1/C-003 |
| C-004 | The facade and dbt statement surfaces expose the four-column result, while `SHOW TBLPROPERTIES` remains refused and the registry states the measured boundary. | Targeted Python/dbt pins and requested repository gates. | **PROVEN** | Facade-derived: `test_catalog_surface.py` and `test_statement_surface.py` pin whole four-column Arrow rows. The recorded `DBT-TBLPROPS-1` leg in `docs/spark-sql-iceberg-parity.md` retains `R-SHOW-TBLPROPERTIES` as the deliberate RePark refusal. pins: show-table-extended-1/C-004 |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: show-table-extended-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each statement form, row shape, information line, and residue in the work order has a named Rust or facade pin.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs, python/repark/tests/test_catalog_surface.py]
    - id: AT-2
      status: ATTACKED
      evidence: The pins exercise no LIKE, optional scope, FROM, alternation, case folding, Unicode properties, v3, empty tables, snapshots, and nested types.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The suite names the parser, missing namespace, missing table, and partition-management failures.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs]
    - id: AT-4
      status: N/A
      justification: The statement reads catalog metadata and adds no concurrent state, retry, or cleanup path.
    - id: AT-5
      status: ATTACKED
      evidence: The diff makes no AWS, IAM, dependency, workflow, or outward-facing change; the comment-ban gate reports zero hits.
      artifacts: [crates/repark-spark/src/show_table_extended.rs, task/ledgers/staging/show-table-extended-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Existing SHOW statement paths remain pinned. The intercept claims a statement only when its head is exactly `SHOW TABLE EXTENDED`; a truncated or malformed tail after that head returns the Spark parse error (`parse_refuses_required_syntax_shapes`, `parse_leaves_near_misses_alone`).
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs]
    - id: AT-7
      status: N/A
      justification: The change has no migration, persisted format, or upgrade path.
    - id: AT-8
      status: ATTACKED
      evidence: The implementation reuses the existing scope resolver and shared Spark property view, with no new dependency.
      artifacts: [crates/repark-spark/src/use_ddl.rs, crates/repark-spark/src/table_props_view.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Returned information and refusal text remain directly observable through the Spark SQL and facade entry points.
      artifacts: [python/repark/tests/test_catalog_surface.py, python/dbt-repark/tests/test_statement_surface.py]
    - id: AT-10
      status: ATTACKED
      evidence: The parser unit tests in `show_table_extended.rs` and the end-to-end modules `tests/show_table_extended.rs` and `tests/show_table_extended_near_miss.rs` enter the parser branches; WO-A10 to WO-A13 hand mutations fail named tests. The facade/dbt pins verify the public entry points.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs, python/repark/tests/test_catalog_surface.py, python/dbt-repark/tests/test_statement_surface.py]
```

## Completion record

Implementation and requested verification are complete on this branch. The unit moves to
`../completed/` in its last commit.

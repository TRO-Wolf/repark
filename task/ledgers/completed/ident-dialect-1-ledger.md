# Unit ledger — IDENT-DIALECT-1 · a table identifier reads Spark's grammar on every head and both doors

**Date:** 2026-11-02 · **Branch:** `fix/ident-dialect-1` · **Base:** `c68017f6` (`main`, SPARK-DOOR-TEMP-VIEW-DDL-1, merged as #1233) · **Head:** `914e5bbb`
**Model:** builder `claude-opus-5-5` · verifiers `claude-opus-5-5` (round 1, delta re-check) and `muse-spark-1.3-contributor` (round 1, delta re-check) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-02 at `914e5bbb`, on the ruled clause. Clause C-012 is OPEN and carded as [IDENT-DIALECT-1B](../../roadmap/mid-term/ident-dialect-1b-card-2026-11-02.md) under owner ruling A (2026-11-02).

**Why.** RePark read a table identifier in a head's target position with rules that differ from Spark 4.1.2's grammar. A double- or single-quoted segment changed the table or view it named, where Spark refuses at the token. A second target after a comma was accepted on heads that take one target. A non-ASCII unquoted word reached the temporary-view registry, where Spark refuses it as an invalid identifier. A digit-leading name and a trailing comment were refused, where Spark accepts them. The unit makes one grammar apply on every head and on the Spark door, through the facade and the Rust router door.

**Clause.** The measured table: 1144 cells at `4fd5c6c1` (13 heads × 22 target spellings × a temporary view of the name or none × the memory and hadoop catalogs; Spark 4.1.2 answers on the hadoop catalog), plus 28 delta cells measured 2026-11-02 (14 statements × both catalogs). The committed copy is `python/repark/tests/sql/data/ident_dialect_1_cells.jsonl` (1172 rows, SHA-256 `ec4135c044ff7182f601eff4c260f468e4c32b99e3db7dec13262932cb35b767`). The facade test is `python/repark/tests/sql/test_ident_dialect_1.py` (1173 cases: 1172 cells and one coverage case). The native router door test is `crates/repark-spark/src/tests/ident_dialect.rs`.

**Ruling (2026-11-02, from the AUD-015 domain table).** A table identifier follows Spark's grammar on every head and both doors. A double- or single-quoted segment is a PARSE_SYNTAX_ERROR at that token (Spark's condition, token and SQLSTATE 42601). A second target after a comma is a PARSE_SYNTAX_ERROR at the comma. A trailing comment is stripped. A non-ASCII unquoted identifier is INVALID_IDENTIFIER (new condition in `crates/repark-common/src/spark_error.rs`, SQLSTATE 42602). After the delta, every relation after a top-level comma in a FROM clause reads through the same grammar.

**Red counts before the fix (step 1, against Spark's answers).** Class 1, quoted segment: 252 of 312 red. Class 2, non-ASCII unquoted: 104 of 104 red. Class 4, over-refusals: 54 of 156 red. Class 5, multi-name target: 32 of 52 red. Controls: 520 cells, none moved by the fix. Delta: 16 of 28 red at `c3c2a76b`, 28 of 28 green at `914e5bbb`.

## PROPOSITION LEDGER — IDENT-DIALECT-1 — 2026-11-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The clause is the measured table: the committed cells file holds the 1144 measured rows and the 28 delta rows, byte-equal to the measured copy, and the facade test asserts every unit cell against Spark's refusal kind and state change. | `python/repark/tests/sql/test_ident_dialect_1.py` green on the cells file at `914e5bbb`; the cells file SHA-256 matches the data map. | PROVEN | Round 1 `cmp` of the first 1144 rows against the measured table: equal. Facade test: 1145 passed at `c3c2a76b`; 1173 passed at `914e5bbb` (delta re-check). SHA-256 equal to the data map. |
| C-002 | Class 1, a double- or single-quoted segment anywhere in a table identifier (312 unit cells: every head, both catalogs, view present or absent): RePark answers PARSE_SYNTAX_ERROR at the quoted token with Spark's state change. | Table cells and the Rust router door cases for the DROP, TRUNCATE, DELETE and SELECT heads. | PROVEN | Red at step 1: 252 of 312. Green at `c3c2a76b`. Round 1 re-measured the 620 non-capability unit cells on both catalogs: 602 are EQUAL or EQUAL_REFUSE, and the other 18 are C-009. |
| C-003 | Class 2, a non-ASCII unquoted word (`ü1`, `١`, `t²`, fullwidth `ｔ`; 104 unit cells, both catalogs): RePark answers Spark's refusal, INVALID_IDENTIFIER with SQLSTATE 42602, and the temporary-view registry never sees the token. | Table cells and the Rust router door cases. | PROVEN | Red at step 1: 104 of 104. Green at `c3c2a76b`. Round 1: 104 of 104 refusal texts equal to Spark's, SQLSTATE 42602. |
| C-004 | Class 4, over-refusals (156 unit cells): a digit-leading unquoted name is read where Spark reads it, and a trailing line or block comment after the target is stripped before the identifier is read. | Table cells on the digit, trail_comment and block_comment spellings. | PROVEN | Red at step 1: 54 of 156. Green at `c3c2a76b`. The 18 cells whose answer kind differs from Spark's are the residual of C-009. |
| C-005 | Class 5, a second target after a comma (52 unit cells): PARSE_SYNTAX_ERROR at the comma on every head that takes one target. | Table cells and the Rust door case `the_grammar_refuses_a_target_on_heads_that_need_more_and_on_a_second_target`. | PROVEN | Red at step 1: 32 of 52. Green at `c3c2a76b`. Round 1 walked the multi-name cells on both catalogs. |
| C-006 | The 520 control cells (every non-unit spelling, both catalogs, view present or absent) answer as at `4fd5c6c1`: answer text and state change equal the recorded `repark` column. | The control cells of the table test, asserted against the `4fd5c6c1` column. | PROVEN | Moved: 0 of 520 at step 1 and at head. Round 1 compared all 520 with the `4fd5c6c1` column: equal in answer text and state change. |
| C-007 | The 488 unit cells that Spark refuses with PARSE_SYNTAX_ERROR (384) or INVALID_IDENTIFIER (104) carry Spark's message text, with the catalog token re-spelled for the memory catalog. | Text probe over the unit cells at build time; the Rust door pins some spellings. | PROVEN | Builder probe: 488 of 488. Round 1: 488 of 488 (384 PARSE_SYNTAX_ERROR at SQLSTATE 42601, 104 INVALID_IDENTIFIER at SQLSTATE 42602). The facade test asserts the kind on strict cells only; the texts are pinned by the probe and by the Rust door for some spellings (round 1, AT-10). |
| C-008 | The 28 delta cells (a relation after a top-level comma or a JOIN in a FROM clause, 14 statements × both catalogs): a quoted or non-ASCII relation there is refused at its token with Spark's state change, and an unquoted or back-quoted relation reads as before. | The delta rows of the cells file (facade); the Rust door cases `the_grammar_refuses_a_target_on_heads_that_need_more_and_on_a_second_target` (comma and JOIN). | PROVEN | Red at `c3c2a76b`: 16 of 28. Green at `914e5bbb`: 28 of 28 (commits `e48e7996`, `ee461086`, `914e5bbb`). Round 2 re-measured the 28 cells on both catalogs: 28 of 28 hold. Mutation with the comma arm removed, and separately with the JOIN arm removed, each fails the Rust test (exit 101). |
| C-009 | 18 unit cells answer as their head's back-quoted control does, with Spark's state change: DELETE through a view for the digit, block_comment and trail_comment spellings (6 cells), UPDATE through a view for the digit and block_comment spellings (4 cells), and INSERT and UPDATE without a view for the digit and block_comment spellings (8 cells). The residual is the head's own divergence, recorded in the AUD-015 closure of SPARK-DOOR-TEMP-VIEW-DDL-1 (its C-039 and C-040 rows), and is not an open cell of this unit. | The table test asserts the state change; the answer kind is the control's on the same head, catalog and view. | PROVEN | Round 1 walked all 18 cells: state change equal to Spark's, answer kind equal to the back-quoted control at the same head, catalog and view. Recorded as a control, not as an open cell. |
| C-010 | The eight AUD-015 pins that accepted a quoted or `$`-spelled target now assert the measured answer. `ALTER TABLE "t"` through a view name is PARSE_SYNTAX_ERROR at `'"t"'` (not EXPECT_TABLE_NOT_VIEW) in `temp_view_alter.rs` and `test_temp_view_ddl_target_1.py`; two `dml.rs` quoted DELETE targets and the `metadata_tables.rs` `$` spelling are re-spelled back-quoted; body hashes are re-pinned. | Re-pinned tests, bound in the sidecar; each keeps its state assertion. | PROVEN | The table cells `dq/alter_rename` and `dq/alter_add` measure Spark's PARSE_SYNTAX_ERROR at `'"t"'` with no state change. Round 1 walked each re-pin; the reopen assertion lost in one re-pin is restored in the delta (C-011, F3). |
| C-011 | The round-1 findings are closed in the delta: F1 (S1, a relation after a comma escapes the grammar), F2 (S3, the JOIN arm is unpinned) and F3 (S3, a re-pin drops the reopen check). | The delta commits; the mutation probes; the restored reopen assertion. | PROVEN | F1 closed by `ee461086` (the comma arm of `relation_refusal` reads every relation after a top-level comma in a FROM clause); removing the comma arm fails the Rust test. F2 closed by the JOIN pin in the same Rust test; removing the JOIN arm fails it. F3 closed by restoring the reopen assertion in `test_a_double_quoted_target_naming_a_view_refuses_and_never_renames` (6 of 6 pass). |
| C-012 | A relation after a top-level comma that follows a `FROM VALUES … AS alias` row list, or a `LATERAL (subquery) alias` relation, is still read past the escape: the scan of the FROM clause stops there, so a quoted relation after it is not refused, and INSERT OVERWRITE and MERGE USING act on the temporary view it names. Spark refuses each measured statement with PARSE_SYNTAX_ERROR at the quoted token. | Owner ruling A (2026-11-02): the round cap is exhausted, so the clause is ruled out of this unit and carded as IDENT-DIALECT-1B with its own pin plan. | OPEN | Round 2 (G1, S1): the INSERT OVERWRITE and MERGE statements act on the view (`a` goes from `[7]` to `[1]`; the MERGE deletes the matched row). Spark 4.1.2 refuses each measured statement at the quoted token with no state change. Carded: [IDENT-DIALECT-1B](../../roadmap/mid-term/ident-dialect-1b-card-2026-11-02.md). |

## Evidence

**Gates, as the verifiers ran them.** Round 1 at `c3c2a76b`: the facade table test (1145 passed), the Rust router door (`tests::ident_dialect`, 2 of 2), `cargo test -p repark-spark` (lib 2892 passed, 5 ignored), `cargo test -p repark-core` (1581 passed), `cargo test -p repark-sql` (645 passed), and the pytest sql and AUD-015 pin files (2369 passed with the pins). Round 2 at `914e5bbb`: the facade table test (1173 passed); the Rust door (2 of 2); `cargo test --locked -p repark-core -p repark-spark --lib` (repark-core 1534 passed, 1 ignored; repark-spark 2892 passed, 5 ignored); `temp_view_doors.rs` (10 of 10); `tests::temp_view_alter` (14 of 14); `python/repark/tests/sql` with the AUD-015 pins (2386 passed, 1 skipped; the skip is an empty fuzz repro set); `make rust-fmt-check rust-clippy py-lint py-format-check check-map-size check-map-sync check-lib-rs check-rust-file-size` (exit 0). The builder recorded one timing-bound repark-core test that failed once under parallel load and passed on its own rerun.

**The 2026-11-02 delta measurement on Spark 4.1.2.** The statements of C-012 were measured on the hadoop catalog and seeded as the AUD-015 table: each G1 statement refuses with PARSE_SYNTAX_ERROR at the quoted token and changes nothing; the back-quoted control `FROM VALUES (1) AS v(k), \`t\`` acts; `FROM a SELECT id, 'z'` returns rows and `FROM a INSERT OVERWRITE TABLE t SELECT id, 'z'` acts, both refused by RePark at `'z'` (routed to IDENT-DIALECT-2).

**Count of the table's unit cells.** Unit cells 624 = class 1 (312) + class 2 (104) + class 4 (156) + class 5 (52). Controls 520. Total 1144 measured plus 28 delta cells.

## Scope notes

- The ANSI-dialect `repark.sql()` door keeps standard delimited identifiers, where a double-quoted segment is an ANSI delimited identifier. Ruled out of scope on 2026-11-02; a scope note, no card. The native door of this unit is the Rust router door.
- Identifier positions outside the head target and the FROM, JOIN and comma relations are the card IDENT-DIALECT-2: column references, MERGE USING sources, CREATE, DESCRIBE and CTAS targets, RENAME destinations, nested and between-segment comments, numeric segments, and the from-first forms. Carded in [IDENT-DIALECT-2](../../roadmap/mid-term/ident-dialect-2-card-2026-11-02.md).
- Unmeasured candidates recorded by the verifiers, not carded by this unit: the token order of a quoted relation after a nested comma inside a subquery's FROM; the downstream text of a quoted relation after a VALUES row list (the state is held); an unquoted relation after a VALUES row list or a LATERAL subquery, which does not resolve under the current database; and a back-quoted relation with a doubled backtick after a comma, which raises a projection display error outside this grammar.
- Four hadoop-catalog RENAME cells (the digit and block_comment spellings of `alter_rename` without a view) refuse `Cannot rename Hadoop tables` as Spark does, and the memory catalog renames. This is catalog capability, unchanged from `4fd5c6c1`, recorded as a capability row and not as a finding.
- The eight moved pins and the 18 residual cells are recorded in C-009 and C-010 and are not open.
- The facade test pins the answer kind on strict cells; on the over-refusal spellings it accepts either Spark's kind or the pre-unit kind. The texts are pinned by the probe and by the Rust door for some spellings.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ident-dialect-1
  complete: true
  reattested: [AT-1, AT-3, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verifiers walked every changed line of the packet and traced relation_refusal, comma_relation_refusal and read_identifier against the lexer. Round 1 re-measured the unit cells and the controls on both catalogs; round 2 re-measured the 28 delta cells and 51 edge statements. The G1 escape was found by this walk and is carded (C-012).
      artifacts: [crates/repark-core/src/idents.rs, python/repark/src/repark/spark/_idents.py, python/repark/src/repark/spark/session/sql_relations.py, python/repark/tests/sql/data/ident_dialect_1_cells.jsonl]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..914e5bbb
    - id: AT-3
      status: ATTACKED
      evidence: Both doors run the grammar refusal before expansion and dispatch (run_sql before the rewrites and the router before the head dispatch). Every refusing unit cell and probe holds its state; no partial write was observed. The G1 acts are the one state change where the grammar says refuse, carded in C-012.
      artifacts: [python/repark/src/repark/spark/session/sql_run.py, crates/repark-spark/src/router.rs, python/repark/tests/sql/test_ident_dialect_1.py]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..914e5bbb
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..914e5bbb
    - id: AT-6
      status: N/A
      justification: N/A — untouched by origin/main..914e5bbb
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..914e5bbb
    - id: AT-8
      status: ATTACKED
      evidence: INVALID_IDENTIFIER uses SQLSTATE 42602 and Spark's text (104 of 104); PARSE_SYNTAX_ERROR uses SQLSTATE 42601 with Spark's near-token rules, including the dot, extra input and end of input (384 of 384). The PyO3 path raises ParserError, which maps to ParseException. The parse_table_identifier_segments change (a digit-first segment unless it is a numeric literal) reaches the DataFrame and writer name paths and is consistent with Spark's multipart grammar.
      artifacts: [crates/repark-common/src/spark_error.rs, crates/repark-core/src/idents.rs, crates/repark-python/src/session.rs]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..914e5bbb
    - id: AT-10
      status: ATTACKED
      evidence: The pins are the facade table test (1173 cases) and the Rust door (2 tests). Mutation of the comma arm and of the JOIN arm each fails the Rust test (exit 101). The facade test asserts the kind on strict cells only, and the 488 refusal texts are pinned by the probe and by the Rust door for some spellings. The re-pinned AUD-015 tests keep their state assertions; the reopen assertion lost in one re-pin was restored (F3).
      artifacts: [python/repark/tests/sql/test_ident_dialect_1.py, crates/repark-spark/src/tests/ident_dialect.rs, python/repark/tests/writes/test_temp_view_ddl_target_1.py]
```

## Review findings

```yaml
FINDING:
  id: F1
  severity: S1
  category: AT-1
  clause: C-008
  claim: A FROM relation after a top-level comma escaped relation_refusal, so INSERT OVERWRITE with a quoted relation after a comma replaced the target rows on RePark where Spark refuses.
  evidence: Round 1 verifier, at c3c2a76b, both catalogs, no view needed; reproduced by the orchestrator, and Spark measured on 2026-11-02 for the same statements.
  disposition: REMEDIATED (ee461086 reads every relation after a top-level comma in a FROM clause; pinned by the delta cells and the Rust door comma case)
```

```yaml
FINDING:
  id: F2
  severity: S3
  category: AT-10
  clause: C-008
  claim: The JOIN arm of relation_refusal was live but unpinned: removing it survived both Rust suites and the facade table had no JOIN cell.
  evidence: Round 1 verifier mutation probe on the JOIN arm.
  disposition: REMEDIATED (the JOIN case is pinned in crates/repark-spark/src/tests/ident_dialect.rs; removing the JOIN arm now fails it, exit 101)
```

```yaml
FINDING:
  id: F3
  severity: S3
  category: AT-10
  clause: C-010
  claim: A re-pin of test_a_double_quoted_target_naming_a_view_refuses_and_never_renames replaced the shared refusal-and-held check with an inline block that dropped the reopened-state assertion.
  evidence: Round 1 verifier, re-pin strength (ruling 3 of the records).
  disposition: REMEDIATED (the reopened-state assertion is restored; the test passes 6 of 6)
```

```yaml
FINDING:
  id: G1
  severity: S1
  category: AT-1
  clause: C-012
  claim: A relation after a comma that follows a FROM VALUES row list or a LATERAL subquery escapes the comma grammar, so a quoted relation resolving to a temporary view lets INSERT OVERWRITE replace rows and MERGE USING delete them.
  evidence: Round 2 verifier, at 914e5bbb (verdict FAIL on this finding alone; everything else in the delta packet held). The orchestrator reproduced it and measured Spark 4.1.2: every measured G1 statement is PARSE_SYNTAX_ERROR at the quoted token.
  disposition: OPEN (owner ruling A, 2026-11-02: the round cap is exhausted; carded as IDENT-DIALECT-1B, the next one-round unit)
```

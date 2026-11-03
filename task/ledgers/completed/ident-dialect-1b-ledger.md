# Unit ledger — IDENT-DIALECT-1B · a relation after a VALUES row list or a LATERAL subquery reads Spark's grammar, and every table door reads its identifier the same way

**Date:** 2026-11-03 · **Branch:** `fix/ident-dialect-1b` · **Base:** `ad2773fe` (`fix/ident-dialect-1`, PR #1244) · **Head:** `11afbf6e` · **PR:** #1245
**Model:** builder `claude-opus-5-5` · verifiers `claude-opus-5-5` (rounds 1 and 2) and `muse-spark-1.3-contributor` (rounds 1 and 2) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-03 at `11afbf6e`, on the ruled clause. Clause C-013 is OPEN and carded as residue rows of [IDENT-DIALECT-2](../../roadmap/mid-term/ident-dialect-2-card-2026-11-02.md). Clause C-014 is REJECTED: the from-first forms stay outside this unit.

**Why.** IDENT-DIALECT-1 read every FROM relation after a top-level comma with Spark's grammar, except two escapes: the row list of `FROM VALUES … AS alias` skipped the comma scan up to the alias, and any `LATERAL` word ended the scan. A quoted relation after either escape reached the temporary-view registry, and INSERT OVERWRITE and MERGE USING acted on it. This unit closes both escapes, and makes every facade table door that takes an identifier string read it with the same grammar. Round 1 added a regression that the unit's own fix closed: a `values` column after a dot fired the VALUES skip.

**Clause.** The measured table of the card (rows 1 to 6 on the memory and hadoop catalogs, Spark 4.1.2 answers on the hadoop catalog) and the four from-first control cells held at RePark's refusal. The committed copy is `python/repark/tests/sql/data/ident_dialect_1_cells.jsonl` (1200 rows, SHA-256 `0507e12cfeefc7f6514a32517d2a4a972a91b647838cd3f8427b6b68a8d73377`). The facade test is `python/repark/tests/sql/test_ident_dialect_1.py` (1201 cases: 1200 cells and one coverage case). The door pins are `python/repark/tests/sql/test_ident_dialect_doors.py` (65 pins). The native router door test is `crates/repark-spark/src/tests/ident_dialect.rs`.

**Ruling (2026-11-02, owner ruling A).** The clause is ruled out of IDENT-DIALECT-1 and carded as IDENT-DIALECT-1B with its own pin plan. The sixteen-cell pin plan splits into twelve cells here (rows 1 to 6) and four from-first cells under IDENT-DIALECT-2.

**Red counts before the fix.** Step 1 (`f8fd0329`): 10 of 16 cells match Spark before the fix; the twelve card cells and the four from-first cells are pinned. Delta step 1 (`0319bc30`): the 12 `f1b_*` cells and the single-quoted spelling on the fifteen doors are pinned red, and at `038ad239` 4 of 12 `f1b_*` cells answer Spark's way.

**Rounds.** Round 1 at `038ad239`: Opus FAIL on F1 (S1); Muse PASS. Delta re-check at `11afbf6e`: Opus PASS and Muse PASS, no findings. Round 1 F2 (S3, the single-quoted spelling on the doors) is closed by the delta. F3 (S3, the from-first controls) is carded.

## PROPOSITION LEDGER — IDENT-DIALECT-1B — 2026-11-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The clause is the measured rows: the cells file (1200 rows) holds the twelve card cells (rows 1 to 6 on both catalogs) and the four from-first cells held at RePark's refusal, byte-equal to the measured Spark answers, and the facade test asserts each cell's refusal kind or state change. | `test_ident_dialect_1.py` green on the 1200-row file; the 1B rows equal the measured statements and answers. | PROVEN | Delta claim 1 (Opus): 1201 collected, 1266 passed with the doors file, exit 0; the 12 `f1b_*` rows equal `domain-ident/spark_f1b.jsonl` (12 of 12). Muse AT-1 HOLD. Red and green: 10 of 16 at `f8fd0329`, 16 of 16 at `9196e2c0`; delta 4 of 12 at `038ad239`, 12 of 12 at `11afbf6e`. |
| C-002 | A quoted or single-quoted relation after the row list of `FROM VALUES (…) AS alias`, in INSERT OVERWRITE and in MERGE USING (rows 1, 2 and 5), refuses at its token with PARSE_SYNTAX_ERROR and Spark's state change; the back-quoted control (row 3) reads and acts as Spark. | The Rust door test `the_grammar_refuses_a_target_on_heads_that_need_more_and_on_a_second_target` (VALUES cases and the back-quoted control) and the facade cells. | PROVEN | Round 1 (Opus, both catalogs): the VALUES comma cells, double- and single-quoted, refuse with `changed = []`; the back-quoted control acts. Delta (Opus): the 12 cells hold on mem and hd. |
| C-003 | A quoted relation after a `LATERAL (subquery) alias` relation (row 4) refuses at its token with Spark's state change. | The Rust door test (LATERAL subquery case) and the facade cells. | PROVEN | Round 1 (Opus): the LATERAL comma cell refuses on mem and hd with `changed = []`. Delta: 12 of 12 cells. |
| C-004 | A quoted relation after a `LATERAL VIEW` alias list (row 6) refuses at its token with PARSE_SYNTAX_ERROR, where the engine had answered `LATERAL VIEWS unimplemented`. | The Rust door test (LATERAL VIEW case) and the facade cells. | PROVEN | Round 1 (Opus): the LATERAL VIEW cell refuses on mem and hd with `changed = []`. Delta: 12 of 12 cells. |
| C-005 | The VALUES skip of the FROM comma scan fires only where a relation starts (the scan start, after a depth-0 comma, or after JOIN). A `values` word that is a column (`b.values`) does not hide a following quoted relation, so INSERT OVERWRITE of `a` refuses with no state change. | The Rust door case `INSERT OVERWRITE t SELECT a.id, 'z' FROM a JOIN t ON a.id = t.values, "a"` and the `f1b_join_values_col` cells on both catalogs. | PROVEN | Round-1 F1 (S1) closed in `11afbf6e` by `relation_starts_at`. Delta (Opus): `f1b_join_values_col` dq and sq refuse with `a` unchanged at `[7, 8]`; the bt control overwrites to `[7]` as Spark does. Mutation: `relation_starts_at` returning `true` fails the Rust test (exit 101, panic at `ident_dialect.rs:216`); round 1's mutation of the early VALUES return failed at `ident_dialect.rs:212`. |
| C-006 | Every facade door that takes a table identifier string reads it with `refuse_table_identifier_grammar`: a double-quoted or non-ASCII spelling refuses at Spark's token with no state change; a single-quoted segment after a dot refuses at the dot, as Spark answers; a back-quoted spelling reads. | `test_ident_dialect_doors.py`: the refusal test over fifteen doors and three spellings (65 pins), and the back-quoted read test. | PROVEN | Round-1 F2 (S3: the single-quoted spelling answered at `''t''`, Spark at `'.'`) closed in `0319bc30` and `11afbf6e`. Delta (Opus): 65 of 65 pass; dq, sq and uni refuse on every door with zero state change; bt reads or writes. Muse AT-8 HOLD. |
| C-007 | `format("iceberg").load()` reports the legacy time-travel refusals (`snapshot-id`, `as-of-timestamp`, `tag`) before the identifier grammar; a numeric identifier (`load('123')`) refuses at parse; `table()` reads the grammar at its entry. | `test_load_reports_the_legacy_snapshot_option_before_the_identifier_grammar` and `test_load_refuses_a_numeric_identifier_at_parse`. | PROVEN | Round 1 (Opus): the snapshot-id cell answers the legacy text on mem and hd; code read of `reader.py` puts the grammar before the semantic-option check in `table()` and after the travel options and window in `load()`. Delta: the door file passes 65 of 65. |
| C-008 | The writer path-escape pin (`038ad239`): a double-quoted segment in `saveAsTable` refuses at parse with PARSE_SYNTAX_ERROR; the back-quoted twin keeps the path-traversal refusal. | `python/repark/tests/io/test_writer.py::test_save_as_table_rejects_path_escape_segment`. | PROVEN | Round 1 (Opus claim 4): the io writer tests are green in the 2488-test suite. Delta (Opus claim 4): 766 passed across the AUD-015 pins, `io/test_writer.py` and the production-size test, exit 0. |
| C-009 | The card's retirement condition holds: rows 1 to 6 answer as Spark does on both catalogs, pinned, and the two Rust door cases pass. | The facade cells test and the Rust door test (C-002 to C-005). | PROVEN | Delta (Opus claims 1 and 3): 12 of 12 cells on mem and hd; `tests::ident_dialect` 2 passed, exit 0. |
| C-010 | Neighbours of the escape refuse at the quoted token with no state change on mem and hd, and the back-quoted controls read: nested parentheses in the row list, a row list followed by JOIN or CROSS JOIN, an alias without AS, a CTE, an IN subquery, MERGE USING without AS, LATERAL VIEW OUTER, and two LATERAL VIEWs. | The verifiers' edge probes; no pin. | PROVEN | Round 1 and delta (Opus claim 3): edge probes e1 to e5 and the neighbour set on INSERT OVERWRITE and temp view `t`; every refusing shape holds its state. Spark is unmeasured for these shapes except the measured ones. Muse differential (85 statements): the only state changes are the F1 and F2 scope changes. |
| C-011 | The suites and gates are green at `11afbf6e`: `tests/sql` (1749 passed, 1 skipped, the empty fuzz repro set); the AUD-015 pins with `io/test_writer.py` and the production-size test (766 passed); `repark-core` (1534 passed) and `repark-spark` lib (2892 passed); `make rust-clippy py-lint py-format-check check-map-size check-map-sync check-lib-rs check-rust-file-size` exit 0. | The verifiers' command logs (not tracked). | PROVEN | Delta (Opus claim 4 and gate list); Muse gate list (all exit 0 except the expected-fail mutation run). |
| C-012 | Hygiene: the packet adds no code comments, each commit carries one `Authored-By` trailer, and no scratch or home path, session link, account id, bucket name, region or ARN appears in the added lines. | Diff inspection by the verifiers. | PROVEN | Delta (Opus claim 4): `0319bc30` and `11afbf6e` have one trailer each; the added-line scan is clean. Muse hygiene: `comments_added` 0, `secrets` none. |
| C-013 | The residue rows (the bare-alias VALUES row list, `listColumns(tableName, dbName)`, `load('123')`, the `save` and `createTable` text, and the candidates K1 and K2) are carded as IDENT-DIALECT-2 rows and are not closed by this unit. | Closing question: does each row answer as Spark does on both catalogs and get pinned, or does the owner decline it? | OPEN | Round 1 (Opus): the bare alias `VALUES 1, 2 v, "t"` gives RePark's own parser text with no state change. Muse (`listColumns`): grammar refusal; Spark table-not-found. Delta (Opus K1, K2): both refuse with no state change; Spark unmeasured. |
| C-014 | The from-first forms (`FROM a SELECT id, 'z'` and `FROM a INSERT OVERWRITE TABLE t SELECT id, 'z'`) are in this unit's scope. | The four from-first cells, mem and hd. | REJECTED | Not in scope. RePark refuses at `'z'` with no state change (an over-refusal, S3); Spark acts. The owner's routing of 2026-11-02 carded them as IDENT-DIALECT-2 rows 7 and 8, and the four cells are pinned as held. |

## Evidence

**Gates, as the verifiers ran them.** Round 1 at `038ad239`: the facade table test and doors (1239 passed), `cargo test --locked -p repark-spark --lib tests::ident_dialect` (2 of 2), the committed rows against the measurement (0 differences). Delta at `11afbf6e`: the facade table test (1201 collected, 1266 passed with the doors file), the Rust door test (2 of 2), the mutation of `relation_starts_at` (exit 101, restored byte-equal), the 85-statement base and head differential (no state change on a refusing shape), `pytest tests/sql` (1749 passed, 1 skipped), `cargo test --locked -p repark-core -p repark-spark --lib` (1534 and 2892 passed), and the make gates listed under C-011 (exit 0).

**The 2026-11-03 records gate run** is in the records round's own log directory (not tracked).

## Review findings

```yaml
FINDING:
  id: F1
  severity: S1
  category: AT-3
  clause: C-005
  claim: The new VALUES arm of the FROM comma scan matched any depth-0 word `values`, including a qualified column such as `b.values`; the scan then ran to the end of the statement and never read a following quoted relation, so INSERT OVERWRITE acted on a temporary view where Spark refuses (introduced by 9196e2c0; the statement refused at ad2773fe).
  evidence: Round 1 verifier at 038ad239, both catalogs, reproduced by a temporary Rust test that passes at ad2773fe and fails at 038ad239; the mutation restored the early VALUES return and the Rust test failed.
  disposition: REMEDIATED (11afbf6e: the VALUES branch takes the relation only where a relation starts, `relation_starts_at`; pinned by the Rust case and the `f1b_join_values_col` cells)
```

```yaml
FINDING:
  id: F2
  severity: S3
  category: AT-8
  clause: C-006
  claim: Every facade table door answered the single-quoted spelling `<cat>.ns.'t'` at `''t''`; Spark answers at `'.'`, and the doors test did not pin the spelling.
  evidence: Round 1 verifier, `spark_doors.jsonl` (all fourteen SQL-text doors) against RePark on mem and hd.
  disposition: REMEDIATED (0319bc30 pins the single-quoted spelling on the fifteen doors; 11afbf6e reports it at the dot, as the SQL door does)
```

```yaml
FINDING:
  id: F3
  severity: S3
  category: AT-1
  clause: C-014
  claim: Four from-first control cells (`FROM a SELECT id, 'z'` and `FROM a INSERT OVERWRITE TABLE t SELECT id, 'z'`, mem and hd) differ from Spark: Spark acts; RePark refuses at `'z'` with no state change.
  evidence: Round 1 verifier; unchanged from ad2773fe; the owner's routing of 2026-11-02 carded them as IDENT-DIALECT-2 rows 7 and 8.
  disposition: OPEN (owner routing 2026-11-02: carded as IDENT-DIALECT-2 rows 7 and 8; not closed by this unit)
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ident-dialect-1b
  complete: true
  reattested: [AT-1, AT-3, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verifiers walked the packet's changed lines and traced the VALUES and LATERAL arms of the comma scan and the identifier reader. Round 1 re-measured the twelve card cells and the controls on both catalogs; the delta re-measured the twelve f1b cells and ran the neighbour and edge probes. F1 (the `values` column) was found by this walk and is closed (C-005).
      artifacts: [crates/repark-core/src/idents.rs, python/repark/tests/sql/test_ident_dialect_1.py, python/repark/tests/sql/data/ident_dialect_1_cells.jsonl]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by ad2773fe..11afbf6e
    - id: AT-3
      status: ATTACKED
      evidence: Every table door runs the identifier grammar before its work; refusing shapes hold their state (the delta's mutation and probe runs), and the one state change the grammar must refuse (F1) is fixed. The acting cells change exactly as Spark does (`a` goes from `[7, 8]` to `[7]`).
      artifacts: [crates/repark-core/src/idents.rs, python/repark/src/repark/spark/session/reader.py, python/repark/src/repark/spark/catalog_surface.py, python/repark/src/repark/spark/dataframe/writer_readwriter.py, python/repark/src/repark/spark/catalog.py]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by ad2773fe..11afbf6e
    - id: AT-5
      status: N/A
      justification: N/A — untouched by ad2773fe..11afbf6e
    - id: AT-6
      status: N/A
      justification: N/A — untouched by ad2773fe..11afbf6e
    - id: AT-7
      status: N/A
      justification: N/A — untouched by ad2773fe..11afbf6e
    - id: AT-8
      status: ATTACKED
      evidence: The error texts are Spark's: PARSE_SYNTAX_ERROR at the quoted token and at the dot (SQLSTATE 42601), INVALID_IDENTIFIER for a non-ASCII word; the doors and the Rust router produce them byte-equal to the measured Spark answers for the pinned spellings. F2 (the single-quoted spelling) is closed.
      artifacts: [crates/repark-core/src/idents.rs, crates/repark-python/src/session.rs, python/repark/tests/sql/test_ident_dialect_doors.py]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by ad2773fe..11afbf6e
    - id: AT-10
      status: ATTACKED
      evidence: The pins are the facade cells test (1200 cells), the door pins (65), the two Rust door tests, and the io writer re-pin. Mutation of `relation_starts_at` fails the Rust test (exit 101, restored byte-equal). The suites and gates are green (C-011); the hygiene checks are clean (C-012).
      artifacts: [python/repark/tests/sql/test_ident_dialect_1.py, python/repark/tests/sql/test_ident_dialect_doors.py, crates/repark-spark/src/tests/ident_dialect.rs, python/repark/tests/io/test_writer.py]
```

## Scope notes

- The ANSI `repark.sql()` door keeps standard delimited identifiers, where a double-quoted segment is an ANSI delimited identifier. The native `repark.sql()` door was checked in the delta differential (0 of 3 base and head diffs); it has no grammar pre-scan. Ruled out of scope, no card.
- Identifier positions outside the head target and the FROM, JOIN and comma relations are IDENT-DIALECT-2, with the residue rows of this unit appended to its table.
- `dropTempView` and `dropGlobalTempView` take a plain view name, not a multipart identifier, and are not routed through the grammar. Spark agrees.
- Observed, not carded by this unit: `recoverPartitions` answers `ok` on a v2 table where Spark answers NOT_SUPPORTED_COMMAND_FOR_V2_TABLE (round-1 verifier). It is outside the identifier grammar and is recorded here for the orchestrator's ruling.
- The three iceberg test files that ad2773fe re-pinned keep their existing map rows; no row describes the four re-pinned assertions individually.

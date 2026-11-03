# Card IDENT-DIALECT-1B: a relation after a comma that follows a VALUES row list or a LATERAL subquery escapes the FROM grammar, and INSERT OVERWRITE and MERGE act on it

**Date:** 2026-11-02. **Filed by:** the IDENT-DIALECT-1 records round (`claude-haiku-5-5`), for the orchestrator.

**Unit:** IDENT-DIALECT-1B. **Grade:** A. **Priority:** P1 for the v2 label, because the acting cells are S1 on the deleting head (INSERT OVERWRITE) and S2 on MERGE.

**Status:** closed 2026-11-03 at `11afbf6e` (the IDENT-DIALECT-1B PR, #1245), on the ruled clause. Ruled out of IDENT-DIALECT-1 by owner ruling A on 2026-11-02.

**Why:** IDENT-DIALECT-1's delta (ledger clause C-008) made every relation after a top-level comma in a FROM clause read with Spark's grammar. The round-2 verifier (Opus 5.5, `claude-opus-5-5`) found that `comma_relation_refusal` stops reading the FROM clause before the comma in two places: after the row list of `FROM VALUES ... AS alias`, where it skips the whole clause, and at any `LATERAL` word, which includes a `LATERAL (subquery) alias` relation. A quoted relation after either escape is not refused, and a temporary view it names acts. The orchestrator reproduced it and measured Spark 4.1.2 on 2026-11-02 (hadoop catalog, seeded as the AUD-015 table). The ledger is [ident-dialect-1-ledger.md](../../ledgers/completed/ident-dialect-1-ledger.md), clause C-012.

**Clause (the measured cells).** Spark answers are the 2026-11-02 measurement. RePark answers are as reproduced at `914e5bbb`, with a temporary view `t` in scope. Each statement is measured on the memory and the hadoop catalog, so each row is two cells.

| # | Statement | Spark 4.1.2 | RePark at `914e5bbb` | Severity | Routing |
|---|---|---|---|---|---|
| 1 | `INSERT OVERWRITE a SELECT k FROM VALUES (1) AS v(k), "t"` | PARSE_SYNTAX_ERROR at `"t"`, no change | acts: `a` goes from `[7]` to `[1]` | S1 | IDENT-DIALECT-1B |
| 2 | `INSERT OVERWRITE a SELECT k FROM VALUES (1) AS v(k), 't'` | PARSE_SYNTAX_ERROR at `'t'`, no change | acts: `a` goes to `[1]` | S1 | IDENT-DIALECT-1B |
| 3 | ``INSERT OVERWRITE a SELECT k FROM VALUES (1) AS v(k), `t` `` (control) | ok, `a` changed | acts, as Spark | control | IDENT-DIALECT-1B (must stay green) |
| 4 | `INSERT OVERWRITE a SELECT l.k FROM a, LATERAL (SELECT 1 AS k) l, "t"` | PARSE_SYNTAX_ERROR at `"t"`, no change | acts: `a` goes to `[1]` | S1 | IDENT-DIALECT-1B |
| 5 | `MERGE INTO a USING (SELECT k AS id FROM VALUES (7) AS v(k), "t") s ON a.id = s.id WHEN MATCHED THEN DELETE` | PARSE_SYNTAX_ERROR at `"t"`, no change | acts: the matched row of `a` is deleted | S2 | IDENT-DIALECT-1B |
| 6 | `SELECT * FROM a LATERAL VIEW explode(array(1)) x AS e, "t"` | PARSE_SYNTAX_ERROR at `"t"`, no change | refuses with the engine text `LATERAL VIEWS unimplemented`, no change | S3 (text) | IDENT-DIALECT-1B |
| 7 | `FROM a SELECT id, 'z'` | rows `[[7, "z"]]` | refuses PARSE_SYNTAX_ERROR at `'z'`, no change (over-refusal) | S3 | IDENT-DIALECT-2 |
| 8 | ``FROM a INSERT OVERWRITE TABLE t SELECT id, 'z'`` | ok, `t` changed | refuses PARSE_SYNTAX_ERROR at `'z'`, no change (over-refusal) | S3 | IDENT-DIALECT-2 |

Rows 1 to 6 are this card's twelve cells. Rows 7 and 8 are the from-first forms. The owner's routing of 2026-11-02 moves them to [IDENT-DIALECT-2](ident-dialect-2-card-2026-11-02.md) as S3 over-refusals; the verifier's table column reads S1 for row 8 by head only (INSERT OVERWRITE); the filed severity is S3, an over-refusal, because RePark refuses the statement with no state change. The sixteen-cell count in the owner's pin plan therefore splits into twelve here and four under IDENT-DIALECT-2.

**Fix direction (from the round-2 verdict).** In `comma_relation_refusal` (`crates/repark-core/src/idents.rs`): after a `FROM VALUES`, skip only the parenthesised row list up to its alias, then keep the comma scan; end the scan at `LATERAL` only when `VIEW` follows it, not at a `LATERAL (subquery) alias` relation. The MERGE USING source escapes the same way, so the same scan serves both. Every relation after a top-level comma in a FROM clause then reads with IDENT-DIALECT-1's grammar.

**Pin plan.** Twelve facade cells: rows 1 to 6 on the memory and the hadoop catalog, each asserting Spark's refusal kind and text, or Spark's state change for the control row, with the view present. Two Rust door cases in `crates/repark-spark/src/tests/ident_dialect.rs`: one for the VALUES row-list escape (rows 1, 2 and 5) and one for the LATERAL subquery escape (row 4). Write the pins red first.

**Retires:** the card when rows 1 to 6 answer as Spark does on both catalogs, pinned, and the two Rust door cases pass. The round-2 verifier's edge probes (the statements of the verdict's table) are repeated as the unit's independent check.

## Closure (2026-11-03)

Closed at `11afbf6e` on `fix/ident-dialect-1b`, stacked on `fix/ident-dialect-1` (PR #1244). Commits: `f8fd0329` (pins red: sixteen cells), `9196e2c0` (the VALUES and LATERAL scan fix; every facade table door reads its identifier through `refuse_table_identifier_grammar`), `038ad239` (the writer path-escape re-pin), `0319bc30` (the delta pins red: twelve `f1b_*` cells and the single-quoted spelling on the doors), `11afbf6e` (the delta fix: `relation_starts_at`, and the dot position for a single-quoted segment).

Rounds. Round 1 at `038ad239`: Opus FAIL on F1 (S1: a `values` column after a dot fired the VALUES skip, so INSERT OVERWRITE acted on the view); F2 (S3, the single-quoted spelling not pinned on the doors) and F3 (S3, the from-first controls, carded). Muse PASS. Delta at `11afbf6e`: Opus PASS and Muse PASS; F1 and F2 closed, no findings.

Counts. The cells file holds 1200 rows (table test 1201 cases); door pins 65; Rust door cases 8 and 1. Spark 4.1.2 answers from the hadoop catalog, RePark on the memory and hadoop catalogs.

Doors through `refuse_table_identifier_grammar`: `spark.table`, `spark.read.table`, `format("iceberg").load` (after the legacy time-travel refusals), `DataFrame.writeTo`, `mergeInto`, `write.insertInto`, `write.saveAsTable`, `write.format("iceberg").save(<identifier>)` (refuses; text not pinned), `catalog.tableExists`, `listColumns`, `getTable`, `cacheTable`, `isCached`, `uncacheTable`, `refreshTable`, `recoverPartitions`, `createTable` and `createExternalTable`. `dropTempView` and `dropGlobalTempView` take a plain view name and are not routed.

Ledger: [ident-dialect-1b-ledger.md](../../ledgers/completed/ident-dialect-1b-ledger.md) (C-001 to C-012 PROVEN, C-013 OPEN, C-014 REJECTED). Residue rows: [IDENT-DIALECT-2](ident-dialect-2-card-2026-11-02.md).

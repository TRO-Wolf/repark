# Card ALTER-TABLE-KEYWORD-VIEW-NAME-1: a temporary view named with a keyword, referenced unquoted, parses differently from Spark

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 verifier's round-2 finding F7 and the Muse MAX critic's round-2 finding F4.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** A temporary view named `only`, referenced unquoted after `ALTER TABLE`, is read by sqlparser as the `ONLY` modifier. `ALTER TABLE only DROP COLUMN v` gives sqlparser's ParserError (it finds `COLUMN`), `ALTER TABLE only SET TBLPROPERTIES ('k1'='x')` gives a ParserError (it finds `TBLPROPERTIES`), and `ALTER TABLE only RENAME TO t2` gives a ParserError (it finds `TO`). Spark 4.1.2 answers `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` for the first, `EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW` for the second, and renames the view for the third. The table is held in every case, on both catalogs. The quoted form (`` ALTER TABLE `only` `` ...) refuses or renames exactly as Spark does, and the keyword-named views `iceberg` and `if` are Spark-equal. This is the same parse-gap class as [ALTER-TABLE-IDENTIFIER-CLAUSE-PARSE-1](alter-table-identifier-clause-parse-1-card-2026-11-02.md): the refusal text differs from Spark's, and no table changes.

**Fix shape (not built):** measure the unquoted keyword-named view on the head before any change. The verifier's round-2 probe and its Spark 4.1.2 answers are the only measurement so far. Then either read the target as Spark does, so that the view check sees the name, or decline the form with the charter. Do not change the ALTER parse path without that measurement.

**Retires:** the card when an unquoted view named `only` gives Spark 4.1.2's answers through the view's name, pinned, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (round-2 kept divergence, clause C-030 (a)).

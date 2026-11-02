# Card QUOTED-TARGET-NO-VIEW-1: with no temporary view in scope, a double- or single-quoted write or ALTER target changes the table, where Spark 4.1.2 refuses it

**Status (2026-11-02): folded into IDENT-DIALECT-1 (2026-11-02).** Its classes are rows of [ident-dialect-1-card-2026-11-02.md](ident-dialect-1-card-2026-11-02.md). This card is kept as the record of its finding; nothing here is built or dispatched.

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the round-5 Opus 5.5 delta verdict, findings table, row F17 (S2), at `618df399`.

**Status:** open. Severity S2 (the statements change a table). Carded, not fixed in SPARK-DOOR-TEMP-VIEW-DDL-1: the unit is past its cycle cap (owner rule 2026-11-02: a second FAIL after the granted round parks or cards). Not built; not dispatched; awaiting the owner's charter.

**Why:** With no temporary view of the name in scope, these statements against an Iceberg table `t` (`id INT, v STRING`, rows `(1,'a')` and `(2,'b')`) change the table on RePark. Spark 4.1.2 refuses each with `PARSE_SYNTAX_ERROR` at the quoted token and holds the table:
- `INSERT INTO "t" VALUES (5, 'q')` adds a row;
- `INSERT OVERWRITE "t" SELECT 6, 'w'` replaces the table with `(6,w)`;
- `DELETE FROM "t" WHERE id = 1` deletes a row;
- `UPDATE "t" SET v = 'u'` updates both rows;
- `ALTER TABLE "t" ADD COLUMN z INT` and `ALTER TABLE 't' ADD COLUMN z INT` add a column;
- on the memory catalog only, `ALTER TABLE t RENAME TO` with a double-quoted or single-quoted destination (and the other quoted spellings the verdict lists) renames the table.

The write heads and `ALTER TABLE ADD COLUMN` were measured on both catalogs. With a view of the name in scope, the same spellings hold both objects and refuse with the view check's text, not the parse error. `DROP TABLE`, `DROP VIEW` and `TRUNCATE TABLE` with a quoted target are not in this card: they refuse with the parse error on both catalogs (ledger clause C-039). By code reading the behaviour predates the unit: the write-head expansion path and the view-scoped quoted-destination refusal do not cover the no-view case. The round-5 verdict did not run it on a base wheel.

**Fix shape (not built):** the fix belongs to the identifier-quoting dialect rule (a double- or single-quoted identifier is not a table target on Spark's write and ALTER heads), not to the temporary-view unit. Before any change, write a pin for each statement above on both catalogs against Spark 4.1.2's answer, and see it fail on the current head. Then refuse the quoted target at the token with `PARSE_SYNTAX_ERROR`, holding the table, as the DROP and TRUNCATE heads already do (C-039). Refusal text must not name a data-deleting action (ruling 76(a)).

**Retires:** the card when every statement above refuses as Spark 4.1.2 does on both catalogs, with a pin, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md), clause C-039 (narrowed 2026-11-02 to exclude this case); the round-5 verdict, row F17. Family with [ALTER-RENAME-QUOTED-DESTINATION-1](alter-rename-quoted-destination-1-card-2026-11-02.md), which covers the view-scoped case (C-037).

# Card IDENT-DIALECT-2: identifier positions outside the head target and the FROM, JOIN and comma relations read differently from Spark's grammar

**Date:** 2026-11-02. **Filed by:** the IDENT-DIALECT-1 records round (`claude-haiku-5-5`), for the orchestrator.

**Unit:** IDENT-DIALECT-2. **Grade:** C. **Priority:** P2. **Status:** open, not built, not dispatched.

**Why:** IDENT-DIALECT-1 applies Spark's table-identifier grammar to the head target and to every relation in FROM and JOIN clauses (ledger [ident-dialect-1-ledger.md](../../ledgers/completed/ident-dialect-1-ledger.md), clauses C-002 to C-008). The positions below are outside that clause. The owner routed them to this card on 2026-11-02. Spark's answers are measured. RePark's answers are as the verifier recorded them at `c3c2a76b` where it ran them, and `unmeasured` where it did not.

**Leads.**
- **L, the grammar on every identifier position:** column references, MERGE USING sources, CREATE, DESCRIBE and CTAS targets, RENAME destinations, nested and between-segment comments, and numeric segments.
- **F, from-first forms (over-refusals):** `FROM a SELECT id, 'z'` and `FROM a INSERT OVERWRITE TABLE t SELECT id, 'z'`. Spark accepts both. RePark refuses at `'z'` with no state change. S3 by the owner's routing; the measured rows are rows 7 and 8 of [IDENT-DIALECT-1B](ident-dialect-1b-card-2026-11-02.md).

**Severity:** by the AUD-015 legend: S2 for `UPDATE t SET "v"` and `MERGE USING "a"`, S3 for the rest.

**Domain.** The table below is the measured card domain of 2026-11-02 (hadoop catalog, seeded as the AUD-015 table), copied verbatim. Its RePark column is as measured at `c3c2a76b`; the cells marked `unmeasured` were not run by the verifier.

| cell | statement | Spark | RePark at c3c2a76b (Opus verifier) | note |
|---|---|---|---|---|
| c4 quoted column | `SELECT t.'id' FROM t` | PARSE_SYNTAX_ERROR at `''id''` | reads | column reference |
| c4 dq column | `SELECT "id" FROM t` | rows `["id"]` × 2 (string literal) | unmeasured | literal, not an identifier |
| c4 dq SET column | `UPDATE t SET "v" = 'u'` | PARSE_SYNTAX_ERROR at `'"v"'` | updates (S2) | column reference |
| c5 dq rename destination | `ALTER TABLE t RENAME TO "t2"` | PARSE_SYNTAX_ERROR at `'"t2"'` | renames on mem without a view, refuses with one | destination |
| c6 dq CREATE | `CREATE TABLE "x" (id INT) USING iceberg` | PARSE_SYNTAX_ERROR | creates | DDL target |
| c6 dq DESCRIBE | `DESCRIBE TABLE "t"` | PARSE_SYNTAX_ERROR | describes | |
| c6 dq MERGE USING | `MERGE INTO t USING "a" s ON t.id = s.id WHEN MATCHED THEN DELETE` | PARSE_SYNTAX_ERROR at `'"a"'` | acts (S2) | source relation |
| c6 sq MERGE USING | same with `'a'` | PARSE_SYNTAX_ERROR at `''a''` | unmeasured | |
| c6 unicode MERGE USING | same with `ü1` | INVALID_IDENTIFIER | unmeasured | |
| c6 digit MERGE USING | same with `1t` | ok, deletes t's row 1 | unmeasured | valid on Spark |
| c6 unicode CREATE | `CREATE TABLE ü2 (id INT) USING iceberg` | INVALID_IDENTIFIER | unmeasured | |
| c6 dq CTAS source | `CREATE TABLE x2 USING iceberg AS SELECT * FROM "t"` | PARSE_SYNTAX_ERROR | unmeasured | |
| c1 nested block comment | `DROP TABLE t /* /* */ */` | ok, drops t | refuses | over-refusal (Python stripper, no nesting) |
| c1 nested, TRUNCATE | `TRUNCATE TABLE t /* /* */ */` | ok, truncates | refuses | |
| c2 comment between segments, DROP | `DROP TABLE ns./* c */t` | ok, drops t | refuses | |
| c2 comment between, TRUNCATE | `TRUNCATE TABLE ns./* c */t` | ok, truncates | truncates | EQUAL |
| c2 comment between, SELECT | `SELECT * FROM ns./* c */t` | rows of t | expanded wrongly | ANSWER_DIFFERS |
| c7 numeric segment | `DROP TABLE 1` | PARSE_SYNTAX_ERROR at `'1'` | RePark's own refusal text | REFUSE_TEXT |
| c7 numeric SELECT | `SELECT * FROM 1e3` | PARSE_SYNTAX_ERROR, extra input `'1e3'` | own text | REFUSE_TEXT |
| c7 hex | `DROP TABLE 0x1` | TABLE_OR_VIEW_NOT_FOUND `0x1` (a valid identifier) | own refusal | |
| F from-first select | `FROM a SELECT id, 'z'` | rows `[[7, "z"]]` | refuses PARSE_SYNTAX_ERROR at `'z'`, no change | over-refusal, S3 |
| F from-first insert | ``FROM a INSERT OVERWRITE TABLE t SELECT id, 'z'`` | ok, `t` changed | refuses PARSE_SYNTAX_ERROR at `'z'`, no change | over-refusal, S3 |

**Retires:** each row when it answers as Spark does, pinned, or the owner declines it. The RENAME destination of [ALTER-RENAME-QUOTED-DESTINATION-1](alter-rename-quoted-destination-1-card-2026-11-02.md) is one of these rows and retires with it.

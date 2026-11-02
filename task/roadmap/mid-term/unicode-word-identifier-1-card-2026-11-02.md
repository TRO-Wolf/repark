# Card UNICODE-WORD-IDENTIFIER-1: non-ASCII word characters reach the temporary-view registry as table names, where Spark 4.1.2 refuses the token

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the round-5 Opus 5.5 delta verdict, findings table, row F18 (S3), at `618df399`.

**Status:** open. Severity S3. Carded, not fixed in SPARK-DOOR-TEMP-VIEW-DDL-1: the unit is past its cycle cap (owner rule 2026-11-02). Not built; not dispatched; awaiting the owner's charter.

**Why:** Ledger clause C-039 says a token Spark cannot parse never reaches the temporary-view registry. For non-ASCII word characters that is false. Both the core's raw temporary-view token check (`char::is_alphanumeric`) and the facade's `_spark_unparsable_target` (`str.isalnum`) accept Unicode letters and digits that Spark's lexer rejects. With a view and a table of the same name, `ü1`, `١` (U+0661), `t²` or `ｔ` (U+FF54), on both catalogs:
- `DROP TABLE <name>` drops the view, so the name then reads the table;
- `SELECT * FROM <name>` reads the view.

Spark 4.1.2 answers `INVALID_IDENTIFIER` (for `ü1` and `ｔ`, the unquoted identifier must be back quoted) or `PARSE_SYNTAX_ERROR` (at `'١'`, and `'²': extra input '²'` for `t²`), and holds the view and the table. On RePark the table is held on both engines, and the write heads refuse with `EXPECT_TABLE_NOT_VIEW`. This is the class of the round-4 finding F13 (C-039), for non-ASCII tokens.

**Fix shape (not built):** before any change, write pins for the four names on `DROP TABLE`, `SELECT` and the write heads, on both catalogs, against Spark 4.1.2's answers in the round-5 verdict, and see them fail on the current head. Then make both raw-token checks accept only Spark's unquoted identifier characters (ASCII letters, digits and underscore), keeping the digit-leading names Spark parses (C-031), so that the token is refused before it reaches the registry.

**Retires:** the card when the four names answer as Spark 4.1.2 does on both catalogs, with pins, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md), clause C-039 (its sentence "never reaches the temporary-view registry" is narrowed 2026-11-02 for these tokens); the round-5 verdict, row F18.

# Name resolution at the SQL door: the reference rule and its cells (2026-11-03)

CORPUS-2-NAMES, family 2. This file ports Spark's name-resolution rule for the statements that
the AUD-015 table does not hold, says how the cells were built, and summarises them. The cells are
[names-cells-2026-11-03.jsonl](names-cells-2026-11-03.jsonl) (2362 cells). The inventory that
selected the golden files is [names-inventory-2026-11-03.md](names-inventory-2026-11-03.md). The
AUD-015 table (`python/repark/tests/sql/data/ident_dialect_1_cells.jsonl`) was not measured again.

Upstream pin: `apache/spark` tag `v4.1.2` (commit `f0bb2e6`), and `apache/iceberg` tag
`apache-iceberg-1.11.0` (commit `6976e020`) for the Iceberg catalog. A Spark path below starts at
`sql/`. An Iceberg path starts at the repository root.

## The rule

1. **Temporary views resolve first.** A one-part name is looked up as a local temporary view
   before any catalog table. A two-part name is looked up as a view only when its first part is
   the global temporary database. Any other multi-part name never resolves to a temporary view.
   Sources: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RelationResolution.scala:85`
   (`lookupTempView`), and `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala:761`
   (`getRawLocalOrGlobalTempView`: `Seq(v)` is the local view, `Seq(db, v)` with
   `isGlobalTempViewDB(db)` is the global view, anything else is `None`). The consequences for
   each head: reads see the view, DML and `ALTER TABLE` refuse with `EXPECT_TABLE_NOT_VIEW`, and
   `DROP TABLE` drops the view and leaves the table.
2. **`IDENTIFIER(expr)` is a name.** `identifierReference` is either `IDENTIFIER_KW ( expression )`
   or a `multipartIdentifier`
   (`sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4:688`). Every
   head that takes `identifierReference` takes the clause too: `USE` (line 201), the namespace
   DDL (lines 208 to 218), `ALTER VIEW` (line 302), `SHOW TABLES` (line 320), `SHOW TABLE
   EXTENDED` (line 323), `SHOW VIEWS` (line 328) and `DESCRIBE` (line 342). The expression must be
   a constant string. `ResolveIdentifierClause`
   (`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveIdentifierClause.scala:32`)
   folds the string and parses it as a multi-part name. After that, rule 1 applies to the result.
3. **The legacy identifier clause.** `spark.sql.legacy.identifierClause=true` sets
   `legacy_identifier_clause_only` (`SqlBaseParser.g4:55`). This turns off the
   `IDENTIFIER('literal')` alternative of `strictIdentifier` (line 1622). In legacy mode the
   clause is kept only as a whole `identifierReference`, or as an expression that names a column
   or function. Part-wise names (`s.IDENTIFIER('tab')`), aliases, window names, field access,
   `USING` columns and column definitions give a parse error. Measured: 46 of the 237
   statements answer differently from the default file.
4. **Double-quoted identifiers.** With `spark.sql.ansi.doubleQuotedIdentifiers` off (the
   default), `"x"` is a string literal (`SqlBaseParser.g4:1739`). In a name position it is a
   parse error. With the config on, `quotedIdentifier` accepts `DOUBLEQUOTED_STRING` (line 1637,
   flag at line 42). `"x"` is then an identifier everywhere, and string literals need single
   quotes, so `COMMENT "x"` refuses.
5. **One-part namespaces use the current catalog.** `CREATE SCHEMA s`, `DROP SCHEMA s`, `USE s`
   and `SHOW TABLES IN s` resolve `s` in the current catalog
   (`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveCatalogs.scala:100`
   to 104). `USE` sets the current catalog before the namespace
   (`sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/SetCatalogAndNamespaceExec.scala:27`).
   `default` is an ordinary non-reserved name, so `USE default` keeps its case.
6. **Listing heads.** On a v2 catalog, `SHOW TABLES` lists the catalog's tables only
   (`sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/ShowTablesExec.scala:40`).
   `SHOW VIEWS` lists the local temporary views in any namespace. `SHOW VIEWS IN global_temp`
   adds the global views. Both heads take the pattern with or without `LIKE` (lines 321 and 329).
7. **Persistent views need a view catalog.** Iceberg's `SparkCatalog` refuses `CREATE VIEW`
   when the underlying catalog has no view support, as `type=hadoop` does:
   `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java:566`.

## How the cells were built

- **Statements.** Each golden file's statements were taken from its results file
  (`sql/core/src/test/resources/sql-tests/results/<file>.out`, the `-- !query` blocks), so the
  `--IMPORT` files expand exactly as the Spark harness runs them. That gives 598 statements:
  identifier-clause 237, identifier-clause-legacy 237, double-quoted-identifiers 30,
  double-quoted-identifiers-enabled 30, show-tables 27, show-views 20 and
  columnresolution-views 17. Each file runs as one script in a fresh session, in file order.
  Each run starts with `CREATE NAMESPACE <cat>.default` and `USE <cat>.default`, then the file's
  `--SET` config. One rewrite was made: `USING parquet` and `USING csv` become `USING iceberg`
  on `CREATE TABLE`, `REPLACE TABLE` and CTAS, and the one provider-less `CREATE TABLE t(col1
  INT)` gets `USING iceberg`. A temporary view's `USING` was kept as written.
- **Doors.** Spark 4.1.2 ran on a fresh hadoop warehouse (`hd`) per pass, one JVM at a time.
  RePark ran at `d2f3313e` through the facade (`session.sql`) on `mem` and on `hd`. It also ran
  through the native door (`repark.sql`'s session), which has no `USE`, so its setup is
  `CREATE SCHEMA <cat>.default` only. The Spark reference for both RePark catalogs is the hadoop
  catalog.
- **The view axis.** A second pass runs each script again. Before each statement whose head
  names a target, it creates `CREATE TEMPORARY VIEW <last name part> AS SELECT 99 AS id`. The
  heads are SELECT ... FROM, INSERT, DELETE, UPDATE, MERGE, DROP TABLE, DROP VIEW, ALTER TABLE,
  ALTER VIEW, CREATE and REPLACE TABLE, TRUNCATE, DESCRIBE, SHOW COLUMNS, SHOW TBLPROPERTIES,
  SHOW CREATE TABLE, SHOW PARTITIONS, CACHE and UNCACHE, ANALYZE, REFRESH, REPAIR and
  `COMMENT ON TABLE`, with the name read through `IDENTIFIER('a' || 'b')` too. The view is not
  created when a temporary view of that name already exists. It is dropped after the statement
  with `catalog.dropTempView`. Only statements that received the view are recorded. Sixteen
  RePark view cells, where Spark's attempt to make the view gave a different outcome, are not
  tabled.
- **State.** The state has three keys: `tbl:<ns>.<table>` (the table's metadata listing in the
  warehouse), `namespaces` (`SHOW NAMESPACES IN <cat>`) and `views` (`SHOW VIEWS`). A key is
  changed when its value differs across the statement. Table contents are not compared, so a
  wrong stored value shows only as a metadata change.
- **Schema.** The schema is the AUD-015 table's. Two fields have a different meaning here.
  `spelling` is `<golden file>:<statement number>`, 1-based in the results file. `view` is
  `false`, `true` (the view pass) or `"native"` (the native door, view-free pass). `head` is the
  statement's leading keywords. In `stmt`, `spark` and `repark`, the warehouse path reads `<wh>`
  and `#` reads `<hash>`. Error texts are kept whole, up to the SQLSTATE.
- **Native door reach.** Five hundred eighteen native-door answers are a parse error from the
  native parser: identifier-clause 121 per catalog, the legacy file 121, show-tables 9, the two
  double-quoted files 3 each, and show-views 2. These statements do not reach the door, so they
  are not tabled.
- **Classes and severity.** The classes are `compare.py`'s `classify` (the AUD-015 harness),
  unchanged. Severity follows the AUD-015 legend. S1 is a deleting head where RePark acts with a
  change and Spark refuses. S2 is another RePark-acts change, an over-refusal, or an act with no
  change. S3 is refusal text and a differing answer. A panic is a blocker; none occurred.
- **Cascades.** A script is sequential, so one early divergence changes the state that later
  statements see. A group with the suffix `c` holds the later cells that name an object whose
  creating statement diverged, or that write to a different namespace after a diverged `USE` or
  `CREATE SCHEMA`.

## Summary

### Cells per golden file and axis

Facade counts are per catalog (`mem`, `hd` equal).

| Golden file | facade, no view | facade, view pass | native door reached |
|---|---|---|---|
| identifier-clause.sql | 237 | 105 | 116 |
| identifier-clause-legacy.sql | 237 | 107 | 116 |
| double-quoted-identifiers.sql | 30 | 8 | 27 |
| double-quoted-identifiers-enabled.sql | 30 | 8 | 27 |
| show-tables.sql | 27 | 6 | 18 |
| show-views.sql | 20 | 2 | 18 |
| columnresolution-views.sql | 17 | 8 | 17 |
| total (both catalogs) | 1196 | 488 | 678 |

### Classes

| Class | facade | native door |
|---|---|---|
| EQUAL | 182 | 16 |
| EQUAL_REFUSE | 138 | 28 |
| REFUSE_TEXT | 532 | 272 |
| REPARK_REFUSES_SPARK_ACTS | 644 | 310 |
| ANSWER_DIFFERS | 154 | 48 |
| REPARK_ACTS_SPARK_REFUSES_NOOP | 22 | 4 |
| REPARK_ACTS_SPARK_REFUSES_CHANGED | 12 | 0 |

Severity on the facade: S1 2, S2 676, S3 686. On the native door: S2 314, S3 320. Ten facade
EQUAL cells differ in row text. They are `SHOW TABLES`, `SHOW TABLE EXTENDED` and `SHOW VIEWS`
listings, each a D4, D7 or D9 cascade (the wrong namespace or the wrong views).

### Defect groups (facade; cells cover both catalogs)

| D | What diverges | Cells | Severity | Card |
|---|---|---|---|---|
| D1 | `IDENTIFIER(...)` is outside the facade's grammar in table, column, function, schema, view, CTE, alias, window, hint and DDL positions. The facade gives a parse error or names `IDENTIFIER` as a routine. Spark resolves the name, or refuses in legacy mode. | 782, plus 36 D1c | S2 430, S3 352; D1c S2 20, S3 16 | ALTER-TABLE-IDENTIFIER-CLAUSE-PARSE-1 for the 46 `ALTER TABLE` cells; candidate for the rest |
| D2 | `SET spark.sql.ansi.doubleQuotedIdentifiers=true` is accepted and has no effect. `"x"` stays a string or a parse error, and `COMMENT "x"` creates the view that Spark refuses. | 32, plus 2 D2c | S2 14, S3 18; D2c S1 2 | candidate |
| D3 | With the config off, a double-quoted name is accepted as an identifier in alias, column-alias, `ADD COLUMN` and `USE SCHEMA` positions, where Spark gives a parse error. | 18 | S2 4, S3 14 | IDENT-DIALECT-2 (positions outside the head target) |
| D4 | A one-part `CREATE` or `DROP SCHEMA` or `DATABASE` refuses with "expected a two-part `catalog.namespace` name". Spark uses the current catalog. | 44, plus 166 D4c | S2 38, S3 6; D4c S2 50, S3 116 | candidate |
| D5 | `USE default` folds the name to `DEFAULT` and refuses with `SCHEMA_NOT_FOUND`. | 8 | S2 | candidate |
| D6 | Global temporary views are not implemented: `CREATE GLOBAL TEMP VIEW`, reads through `global_temp`, `DROP VIEW global_temp.v` and `SHOW VIEWS IN global_temp`. | 48 | S2 | candidate (gap recorded at `crates/repark-spark/src/tests/temp_view_routing.rs:103`) |
| D7 | `CREATE TEMPORARY VIEW v(cols) USING <source>`, a data-source view with no query, refuses. | 4, plus 4 D7c | S2 | candidate |
| D8 | `SHOW TABLES '<pattern>'` without `LIKE` and `SHOW VIEWS FROM` refuse. `SHOW VIEWS IN` a missing namespace refuses, where Spark lists the temporary views. | 8 | S2 | candidate |
| D9 | `CREATE VIEW` on `hd` creates a persistent view, which Spark's hadoop catalog refuses (rule 7). Two of the four cells are `mem`: there the difference is catalog capability against a hadoop reference, not a defect. | 4, plus 2 D9c | S2; D9c S3 | candidate (the `hd` half) |
| D10 | `SELECT view1.* FROM view1` refuses with "Invalid qualifier" over a temporary view. | 2 | S2 | candidate |
| D11 | Refusal text: DataFusion and iceberg-rust texts (`Error during planning: table ... not found`, `TableNotFound => ...`) appear where Spark names `TABLE_OR_VIEW_NOT_FOUND`, `UNRESOLVED_ROUTINE` and the like. | 52 | S3 | candidate |
| D12 | Session variables (`DECLARE`, `SET VAR`), `EXECUTE IMMEDIATE`, parameter markers and `SET hivevar` substitution are not implemented. | 124 | S2 38, S3 86 | candidate; outside the names rule, listed for the owner |
| D14 | `INSERT` answers a one-row count where Spark answers no rows. | 24 | S3 | none; the AUD-015 table carries the same class |
| D15 | `SHOW CURRENT SCHEMA` is unsupported. | 4 | S2 | candidate |

D13 is not used, because the `/*+ IDENTIFIER('BROADCAST')(t) */` hint form falls in D1. The
two S1 cells are D2c. `DROP VIEW v` drops the temporary view that RePark created at
statement 19 of the double-quoted-identifiers-enabled file, where Spark refused. No table data
is involved. No cell reads different rows from Spark on the same state. No cell panics.

The native door's 678 reached cells are a reach census, not a parity table. That door's contract
is its own. One-part names resolve to `datafusion.public` there (as in
BARE-READ-DEFAULT-NAMESPACE-1), and it has no `USE`. Its non-equal cells sort into the facade
groups: D1 176, D12 84, D11 58, D6 28, D2 26, D3 26, D9 4, D15 4 and D8 2, with 226 cells outside
these groups. Those 226 fall into three kinds. Some are unqualified names that the door does
not find (`datafusion.public`, or `Table 'x' doesn't exist`). Some are heads that it does not
implement (`USE`, `SHOW TABLES IN`, `SHOW VIEWS IN`). The other 44 answer `ok` on both doors
but change different state. None of them changes state where Spark refuses.

### Hours per fixture kind

All cells are SQL only. Spark ran 14 passes (1196 statements) from 19:03:31 to 19:07:03 UTC, 0.06
hours. RePark ran 42 passes from 19:03:31 to 19:04:50, and the native door again from 19:07:47 to
19:07:53, 0.02 hours. The lane took about 0.4 hours, from its first command at 18:57 UTC to its commit at about
19:20 UTC, against the inventory's estimate of 1.5 to 3 hours. The harness took 0.1 hours.
Classification and this file took the rest.

### Label draft rows the cells support

From the draft's family 2 row ("The AUD-015 heads through `spark.sql`, both catalogs, with and
without a same-named temp view"), the cells support three claims on the facade, on both
catalogs:

- With a same-named temporary view, the view wins: 166 view-pass cells are EQUAL or
  EQUAL_REFUSE. They include 66 where `DROP TABLE` drops the view and leaves the table, 24
  where `INSERT` refuses with `EXPECT_TABLE_NOT_VIEW`, 14 for the same refusal on `ALTER TABLE`,
  and 42 reads that resolve to the view.
- Local temporary-view column resolution: columnresolution-views statements 1 and 3 to 8 are
  EQUAL. Statement 2 is D10.
- The `SHOW TABLE EXTENDED` partition and pattern refusals are EQUAL_REFUSE (statements 15 to
  21).

The cells do not support any claim for `IDENTIFIER()`, for the double-quoted-identifiers config,
for global temporary views, or for one-part namespace DDL (D1, D2, D4, D6).

# DOOR-PROTECTION-INVENTORY-1, Part 2: measured

Each cell ran on a fresh table with a single-statement seed, on the Python native door
(`PyReparkSession.native()` plus `register_memory_catalog`, driven through `repark.sql()`), on the
facade `spark.sql()` (the Spark door) and, where the statement is Spark SQL, on the Spark 4.1.2
oracle with an Iceberg Hadoop catalog. Wheel: the main release build `fdc61f26` (`origin/main`
`a4284f4d` differs only in docs). The native door's reference is Trino and no Trino oracle exists
here, so a native verdict is judged against the router's documented behaviour (the Rust pin named in
[inventory.md](inventory.md) and in the `router` column). Cells with no router pin (L1 to L3) are
judged against Spark: no row may change. The verdict is mechanical (see `native_verdict` in
[probe_doors.py](probe_doors.py)): the router column says what the router does, the rows and
snapshot counts come from the table before and after, and "state changed" also covers a new table
or a new schema.

`{t}` and `{s}` in a statement stand for the cell's fresh tables.

The `facade vs spark` column compares the facade with the oracle on the post-statement rows and on
refused-versus-answered; it is not a verdict.

Verdict counts: PROTECTED 27, UNPROTECTED-WRONG-WRITE 7, UNPROTECTED-WRONG-READ 0, UNPROTECTED-OTHER 6, NOT-REACHABLE 7, PANIC 0, NOT-MEASURED 0

| cell | statement | router | native outcome | verdict | facade | spark | facade vs spark |
|---|---|---|---|---|---|---|---|
| A1 | `SELECT 1; SELECT 2` | refuse | refused: UnsupportedOperationException This feature is not implemented: The context currently only supports a single SQL | PROTECTED | refused: ParseException [PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported | refused: ParseException  [PARSE_SYNTAX_ERROR] Syntax error at or near 'SELECT': extra input 'SELECT'. SQLSTATE: | EQUAL |
| A2 | `INSERT INTO {t} VALUES (4, 40); INSERT INTO {t} VALUES (5, 50)` | refuse | refused: UnsupportedOperationException This feature is not implemented: The context currently only supports a single SQL | PROTECTED | refused: ParseException [PARSE_SYNTAX_ERROR] Syntax error: multiple SQL statements in one call are not supported | refused: ParseException  [PARSE_SYNTAX_ERROR] Syntax error at or near 'INSERT': extra input 'INSERT'. SQLSTATE: | EQUAL |
| B1 | `INSERT INTO {t}.branch_dev VALUES (4, 40)` | refuse | refused: AnalysisException Error during planning: Unsupported compound identifier 'ice.ns.b1_t.branch_dev'. Expected | PROTECTED | refused: AnalysisException Error during planning: Cannot use branch (does not exist): dev | refused: Py4JJavaError An error occurred while calling o43.sql. : | EQUAL |
| C1 | `SELECT 'a' COLLATE UNICODE` | refuse | refused: UnsupportedOperationException This feature is not implemented: Unsupported ast node in sqltorel: Collate { expr: | PROTECTED | refused: UnsupportedOperationException This feature is not implemented: repark does not implement collation: requested | answered [['a']] | DIFFERS (refused vs answered) |
| C2 | `CREATE TABLE {n1} (s STRING COLLATE UNICODE)` | refuse | answered | UNPROTECTED-WRONG-WRITE | refused: UnsupportedOperationException This feature is not implemented: repark does not implement collation: requested | answered | DIFFERS (refused vs answered) |
| C3 | `SELECT CAST('a' AS STRING COLLATE UNICODE)` | refuse | refused: ParseException SQL error: ParserError("Expected: ), found: COLLATE at Line: 1, Column: 27") | PROTECTED | refused: UnsupportedOperationException This feature is not implemented: repark does not implement collation: requested | answered [['a']] | DIFFERS (refused vs answered) |
| D1 | `SELECT java_method('java.lang.Math', 'abs', -1)` | refuse | refused: UnsupportedOperationException This feature is not implemented: java_method is unreachable: it loads a Java class by | PROTECTED | refused: UnsupportedOperationException This feature is not implemented: java_method is unreachable: it loads a Java class by | answered [['1']] | DIFFERS (refused vs answered) |
| D2 | `SELECT hll_sketch_agg(id) FROM {t}` | refuse | refused: UnsupportedOperationException This feature is not implemented: hll_sketch_agg is reachable without a JVM and is | PROTECTED | refused: UnsupportedOperationException This feature is not implemented: hll_sketch_agg is reachable without a JVM and is | answered [["b'\\x02\\x01\\x07\\x0c\\x03\\x00\\x03\\x08+\\xf2\\xfb\\x0 | DIFFERS (refused vs answered) |
| E1 | `CREATE EXTERNAL TABLE ext_x STORED AS PARQUET LOCATION '{ext}'` | refuse | refused: AnalysisException Error during planning: CREATE EXTERNAL TABLE to local filesystem path | PROTECTED | refused: UnsupportedOperationException This feature is not implemented: CREATE EXTERNAL TABLE is not supported for Iceberg | skip | no oracle |
| E2 | `COPY (SELECT 1) TO '{ext}.parquet'` | refuse | refused: AnalysisException Error during planning: COPY TO to local filesystem path `<warehouse>/ext/e2.parquet` is | PROTECTED | refused: AnalysisException Error during planning: COPY TO to local filesystem path `<warehouse>/ext/e2.parquet` is | skip | no oracle |
| F1 | `DELETE FROM {t} WHERE id IN (SELECT id FROM {s})` | execute | refused: AnalysisException Error during planning: DELETE with a subquery in its WHERE clause is refused on | UNPROTECTED-OTHER | answered, rows [[1, 10]], +1 snap | answered, rows [[1, 10]], +1 snap | EQUAL |
| F2 | `UPDATE {t} SET v = 5 WHERE id IN (SELECT id FROM {s})` | execute | refused: AnalysisException Error during planning: UPDATE with a subquery in its WHERE clause is refused on | UNPROTECTED-OTHER | answered, rows [[1, 10], [2, 5], [3, 5]], +1 snap | answered, rows [[1, 10], [2, 5], [3, 5]], +1 snap | EQUAL |
| F3 | `UPDATE {t} SET v = 9 WHERE id NOT IN (SELECT id FROM {s})` | refuse | refused: AnalysisException Error during planning: UPDATE with a subquery in its WHERE clause is refused on | PROTECTED | refused: AnalysisException Error during planning: UPDATE with a subquery in its WHERE clause is refused on | answered, rows [[1, 9], [2, 20], [3, 30]], +1 snap | DIFFERS (refused vs answered) |
| F4 | `DELETE FROM {t} WHERE id = (SELECT max(id) FROM {s})` | refuse | refused: AnalysisException Error during planning: DELETE with a subquery in its WHERE clause is refused on | PROTECTED | refused: AnalysisException Error during planning: DELETE with a subquery in its WHERE clause is refused on | answered, rows [[1, 10], [2, 20]], +1 snap | DIFFERS (refused vs answered) |
| F5 | `DELETE FROM {t} WHERE id = ANY (SELECT id FROM {s})` | refuse | refused: AnalysisException Error during planning: DELETE with a subquery in its WHERE clause is refused on | PROTECTED | refused: AnalysisException Error during planning: DELETE with a subquery in its WHERE clause is refused on | refused: ParseException  [PARSE_SYNTAX_ERROR] Syntax error at or near '('. SQLSTATE: 42601 (line 1, pos 38)  == | EQUAL |
| F6 | `DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM {s} WHERE {sn}.id = {tn}.id)` | execute | refused: AnalysisException Error during planning: DELETE with a subquery in its WHERE clause is refused on | UNPROTECTED-OTHER | answered, rows [[1, 10]], +1 snap | answered, rows [[1, 10]], +1 snap | EQUAL |
| G1 | `UPDATE {t} SET v = (SELECT max(w) FROM {s})` | refuse | refused: AnalysisException Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this | PROTECTED | refused: AnalysisException Error during planning: UPDATE with SET column = (SELECT ...) is not supported on this | answered, rows [[1, 99], [2, 99], [3, 99]], +1 snap | DIFFERS (refused vs answered) |
| G2 | `UPDATE {t} SET v = 'abc'` | refuse | refused: PySparkException datafusion engine error: Optimizer rule 'simplify_expressions' failed caused by Arrow | PROTECTED | refused: AnalysisException Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write | refused: AnalysisException [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the | EQUAL |
| H1 | `INSERT INTO {t} VALUES (4)` | refuse | refused: AnalysisException Error during planning: Inconsistent data length across values list: got 1 values in row 0 | PROTECTED | refused: AnalysisException Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot | refused: AnalysisException [INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot write to `hc`.`ns`.`h1_t`, | EQUAL |
| H2 | `INSERT INTO {t} (v) VALUES (5)` | refuse | refused: PySparkException Arrow error: Invalid argument error: Column 'id' is declared as non-nullable but contains | PROTECTED | refused: PySparkException datafusion engine error: Arrow error: Invalid argument error: Column 'id' is declared as | refused: AnalysisException [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the | EQUAL |
| H3 | `INSERT INTO {t} (id) VALUES (4)` | execute | answered, rows [[1, 10], [2, 20], [3, 30], [4, None]], +1 snap | PROTECTED | answered, rows [[1, 10], [2, 20], [3, 30], [4, None]], +1 snap | answered, rows [[1, 10], [2, 20], [3, 30], [4, None]], +1 snap | EQUAL |
| I1 | `INSERT OVERWRITE {t} SELECT 7 AS id, 70 AS v` | refuse | answered, rows [[7, 70]], +1 snap | UNPROTECTED-WRONG-WRITE | answered, rows [[7, 70]], +1 snap | answered, rows [[7, 70]], +1 snap | EQUAL |
| J1 | `CALL {cat}.system.expire_snapshots(table => '{ns}.{tn}')` | refuse | refused: UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: CALL | PROTECTED | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | EQUAL |
| J2 | `ALTER TABLE {t} EXECUTE optimize` | refuse | refused: ParseException SQL error: ParserError("Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, | PROTECTED | refused: ParseException SQL error: ParserError("Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, | refused: ParseException  [PARSE_SYNTAX_ERROR] Syntax error at or near 'EXECUTE'. SQLSTATE: 42601 (line 1, pos | EQUAL |
| K1 | `TRUNCATE TABLE {t}` | execute | refused: UnsupportedOperationException TRUNCATE operation on table 'ice.ns.k1_t' caused by This feature is not implemented: | UNPROTECTED-OTHER | answered, rows [], +1 snap | answered, rows [], +1 snap | EQUAL |
| L1 | `DELETE FROM {t} WHERE false` | unmeasured | answered, rows [], +1 snap | UNPROTECTED-WRONG-WRITE | answered, rows [], +1 snap | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | DIFFERS (rows) |
| L2 | `UPDATE {t} SET v = 0 WHERE NULL` | unmeasured | answered, rows [[1, 10], [2, 20], [3, 30]], +1 snap | UNPROTECTED-OTHER | answered, rows [[1, 10], [2, 20], [3, 30]], +1 snap | refused: Py4JJavaError An error occurred while calling o43.sql. : | DIFFERS (answered vs refused) |
| L3 | `DELETE FROM {t} WHERE 1 = 0` | unmeasured | answered, rows [], +1 snap | UNPROTECTED-WRONG-WRITE | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | EQUAL |
| L4 | `DELETE FROM {t} WHERE id = 99` | execute | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | UNPROTECTED-OTHER | answered, rows [[1, 10], [2, 20], [3, 30]], +1 snap | answered, rows [[1, 10], [2, 20], [3, 30]], +1 snap | EQUAL |
| M1 | `DELETE FROM {t} WHERE v <> 10` | execute | answered, rows [[1, 10], [2, None]], +1 snap | PROTECTED | answered, rows [[1, 10], [2, None]], +1 snap | answered, rows [[1, 10], [2, None]], +1 snap | EQUAL |
| M2 | `UPDATE {t} SET v = v + 1 WHERE id = 2` | execute | answered, rows [[1, 10], [2, 21], [3, 30]], +1 snap | PROTECTED | answered, rows [[1, 10], [2, 21], [3, 30]], +1 snap | answered, rows [[1, 10], [2, 21], [3, 30]], +1 snap | EQUAL |
| M3 | `DELETE FROM {t} WHERE id IN (1, 2)` | execute | answered, rows [[3, 30]], +1 snap | PROTECTED | answered, rows [[3, 30]], +1 snap | answered, rows [[3, 30]], +1 snap | EQUAL |
| N1 | `MERGE INTO {t} USING {s} ON {tn}.id = {sn}.id WHEN MATCHED THEN UPDATE SET v = {sn}.w` | execute | refused: UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: MERGE INTO ice.ns.n1_t USING | NOT-REACHABLE | answered, rows [[1, 10], [2, 20], [3, 99]], +1 snap | answered, rows [[1, 10], [2, 20], [3, 99]], +1 snap | EQUAL |
| O1 | `ALTER TABLE {t} ADD COLUMN c INT` | execute | refused: UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: ALTER TABLE ice.ns.o1_t ADD | NOT-REACHABLE | answered, rows [[1, 10, None], [2, 20, None], [3, 30, None]], +0 snap | answered, rows [[1, 10, None], [2, 20, None], [3, 30, None]], +0 snap | EQUAL |
| P1 | `SELECT * FROM {t} FOR VERSION AS OF {snap1} ORDER BY id` | execute | refused: ParseException SQL error: ParserError("Expected: one of UPDATE or SHARE, found: VERSION at Line: 1, | NOT-REACHABLE | answered [[1, 10], [2, 20], [3, 30]] | answered [[1, 10], [2, 20], [3, 30]] | EQUAL |
| Q1 | `CREATE TABLE {n1} (ts TIMESTAMP(9))` | refuse | answered | UNPROTECTED-WRONG-WRITE | answered | skip | no oracle |
| Q2 | `CREATE TEMPORARY TABLE {n1} (id INT)` | refuse | refused: UnsupportedOperationException This feature is not implemented: Temporary tables not supported | PROTECTED | refused: UnsupportedOperationException This feature is not implemented: CREATE TEMPORARY TABLE is not supported for Iceberg | refused: ParseException  CREATE TEMPORARY TABLE without a provider is not allowed. == SQL (line 1, position 1) | EQUAL |
| Q3 | `CREATE TABLE {n1} (id INT) COMMENT 'x'` | refuse | refused: UnsupportedOperationException This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.q3_n1 (id | PROTECTED | answered | answered | EQUAL |
| Q4 | `CREATE TABLE {cat}.{ns}."a/b" (id INT)` | refuse | answered | UNPROTECTED-WRONG-WRITE | refused: AnalysisException Error during planning: table identifier "a/b" must not contain path separators | refused: ParseException  [PARSE_SYNTAX_ERROR] Syntax error at or near '"a/b"'. SQLSTATE: 42601 (line 1, pos | EQUAL |
| Q5 | `CREATE TABLE {n1} (id INT) AS SELECT 1 AS id` | refuse | refused: PySparkException Unexpected => register_table does not support tables with data. | PROTECTED | refused: AnalysisException Error during planning: Schema may not be specified in a Create Table As Select (CTAS) | refused: ParseException  Operation not allowed: Schema may not be specified in a Create Table As Select (CTAS) | EQUAL |
| R1 | `DROP SCHEMA {cat}.{ns2}` | refuse | refused: PySparkException datafusion engine error: Execution error: Cannot drop schema n2r1 because other tables | PROTECTED | refused: AnalysisException Error during planning: Namespace n2r1 is not empty. Contains 1 table(s). | refused: Py4JJavaError An error occurred while calling o43.sql. : | EQUAL |
| R2 | `CREATE SCHEMA {cat}.{ns3} WITH (foo = 'bar')` | refuse | answered | UNPROTECTED-WRONG-WRITE | answered | skip | no oracle |
| S1 | `SELECT v + 1 AS r FROM {t}` | refuse | refused: ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return | PROTECTED | refused: ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return | refused: ArithmeticException [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return | EQUAL |
| T1 | `DELETE FROM {t} WHERE id = 1` | refuse | fixture | NOT-REACHABLE | refused: AnalysisException Error during planning: refusing SQL DELETE on Iceberg table ``ice`.`ns`.`t1_t``: write | answered, rows [], +1 snap | DIFFERS (refused vs answered) |
| T2 | `INSERT INTO {t} VALUES (2, 20)` | refuse | fixture | NOT-REACHABLE | refused: UnsupportedOperationException Table ns.t2_t carries property 'encryption.key-id': RePark has no table encryption and | skip | no oracle |
| T3 | `DELETE FROM {t} WHERE part = 'a'` | execute | fixture | NOT-REACHABLE | answered, rows [[2, 'b']], +1 snap | answered, rows [[2, 'b']], +1 snap | EQUAL |
| T4 | `ALTER TABLE {t} CREATE BRANCH dev` | execute | refused: ParseException SQL error: ParserError("Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, | NOT-REACHABLE | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | answered, rows [[1, 10], [2, 20], [3, 30]], +0 snap | EQUAL |

## Verdict reasons

- A1 (P01) PROTECTED: refused, nothing changed
- A2 (P01) PROTECTED: refused, nothing changed
- B1 (P02) PROTECTED: refused, nothing changed
- C1 (P03) PROTECTED: refused, nothing changed
- C2 (P03) UNPROTECTED-WRONG-WRITE: executed and changed state where the router refuses
- C3 (P03) PROTECTED: refused, nothing changed
- D1 (P04) PROTECTED: refused, nothing changed
- D2 (P04) PROTECTED: refused, nothing changed
- E1 (P05) PROTECTED: refused, nothing changed
- E2 (P05) PROTECTED: refused, nothing changed
- F1 (P06) UNPROTECTED-OTHER: refused where the router executes; nothing changed
- F2 (P06) UNPROTECTED-OTHER: refused where the router executes; nothing changed
- F3 (P06) PROTECTED: refused, nothing changed
- F4 (P06) PROTECTED: refused, nothing changed
- F5 (P06) PROTECTED: refused, nothing changed
- F6 (P06) UNPROTECTED-OTHER: refused where the router executes; nothing changed
- G1 (P07) PROTECTED: refused, nothing changed
- G2 (P08) PROTECTED: refused, nothing changed
- H1 (P09) PROTECTED: refused, nothing changed
- H2 (P10) PROTECTED: refused, nothing changed
- H3 (P10) PROTECTED: matches the router's result
- I1 (P11) UNPROTECTED-WRONG-WRITE: executed and changed state where the router refuses
- J1 (P12) PROTECTED: refused, nothing changed
- J2 (P12) PROTECTED: refused, nothing changed
- K1 (P13) UNPROTECTED-OTHER: refused where the router executes; nothing changed
- L1 (P14) UNPROTECTED-WRONG-WRITE: rows changed where no row should change
- L2 (P14) UNPROTECTED-OTHER: rows unchanged but 1 new snapshot(s)
- L3 (P14) UNPROTECTED-WRONG-WRITE: rows changed where no row should change
- L4 (P14) UNPROTECTED-OTHER: rows equal, snapshot delta 0 where the router commits 1
- M1 (P15) PROTECTED: matches the router's result
- M2 (P15) PROTECTED: matches the router's result
- M3 (P15) PROTECTED: matches the router's result
- N1 (P16) NOT-REACHABLE: the native door refuses the statement as unsupported syntax
- O1 (P17) NOT-REACHABLE: the native door refuses the statement as unsupported syntax
- P1 (P18) NOT-REACHABLE: the native door refuses the statement as unsupported syntax
- Q1 (P19) UNPROTECTED-WRONG-WRITE: executed and changed state where the router refuses
- Q2 (P19) PROTECTED: refused, nothing changed
- Q3 (P19) PROTECTED: refused, nothing changed
- Q4 (P19) UNPROTECTED-WRONG-WRITE: executed and changed state where the router refuses
- Q5 (P19) PROTECTED: refused, nothing changed
- R1 (P20) PROTECTED: refused, nothing changed
- R2 (P20) UNPROTECTED-WRONG-WRITE: executed and changed state where the router refuses
- S1 (P21) PROTECTED: refused, nothing changed
- T1 (P22) NOT-REACHABLE: the native door cannot create the fixture (no table-property syntax)
- T2 (P23) NOT-REACHABLE: the native door cannot create the fixture (no table-property syntax)
- T3 (P24) NOT-REACHABLE: the native door cannot create the fixture (no table-property syntax)
- T4 (P25) NOT-REACHABLE: the native door refuses the statement as unsupported syntax

# Delete files: the reference rule and the measured cells (2026-11-03)

CORPUS-4, family 4, Parquet delete files only. The rule for applying position deletes, equality
deletes and v3 deletion vectors across schema evolution, ported from the upstream sources the
[inventory](deletes-inventory-2026-11-03.md) pins: Apache Iceberg tag `apache-iceberg-1.11.0`
(commit `6976e020`); every line number below is in that checkout. The cells that measure it are
[deletes-cells-2026-11-03.jsonl](deletes-cells-2026-11-03.jsonl), 6464 rows in the AUD-015 table
schema, measured on RePark `d2f3313e` against Spark 4.1.2 with the Iceberg 1.11.0 runtime jar. The
unit measured and fixed nothing; each defect below is a cells row group and a card candidate. Avro
and ORC delete files are excluded from the v2 label this round by owner ruling, so no cell builds
one; every data file and delete file here is Parquet.

## R1. Sequence numbers scope every delete

- Every file a commit adds inherits the snapshot's sequence number (`format/spec.md` lines 92 to
  98, line 96).
- A position delete file applies to a data file whose data sequence number is less than or equal
  to its own, in the same partition, and to the named file only when `referenced_data_file` is set
  (lines 864 to 868). Equal numbers apply, so a commit can delete rows it added (line 876).
- An equality delete file applies to a data file whose data sequence number is strictly less than
  its own (lines 869 to 870), in the same partition or globally when the delete's spec is
  unpartitioned (lines 871, 875). A row inserted after an equality delete, with a key the delete
  names, survives.
- A deletion vector applies to its one referenced data file at a sequence number less than or
  equal to its own (lines 860 to 863).

## R2. Position delete files

- A row is deleted when a delete entry names its data file path and its zero-based position
  (lines 1148 to 1154). The columns are `file_path` (field id 2147483546, string) and `pos` (field
  id 2147483545, long), reserved ids at lines 389 to 390; an optional `row` struct carries the
  deleted values with table field ids (lines 1156 to 1168). Entries are sorted by `file_path`, then
  `pos` (line 1170).
- Position delete files must not be added to a v3 table; existing ones stay valid (line 1115).

## R3. Deletion vectors (v3)

- A deletion vector is a `deletion-vector-v1` Puffin blob holding a bitmap of deleted positions of
  one data file (lines 1132 to 1144). At most one vector may exist per data file in a snapshot, and
  a new one must carry every older position delete for that file (lines 1123, 1144), so a reader
  may ignore position delete files for a file that has a vector (line 868).

## R4. Equality delete files

- An equality delete file stores any subset of the table's columns under the table's field ids;
  the delete columns are the ids in the manifest's `equality_ids` (field 135, line 669; lines 1177
  to 1179). A data row is deleted when it equals every delete column of any applicable delete row
  (line 1181); several columns are an AND, and a null delete value matches a null, like `IS NULL`
  (line 1183).
- A delete column later dropped from the table must still be used when applying the delete
  (line 1224). A column added later and used as a delete column reads null for older data files by
  the projection rules (line 1224, with lines 340 onward).
- Identifier fields are primitive, required and not float or double (lines 368 to 374); dropping
  an identifier-field column is refused while it is one (`core/src/test/java/org/apache/iceberg/TestSchemaUpdate.java:2186`).
- Scan predicates may prune delete files by their column bounds just as data files are pruned
  (line 852). Equality delete files that carry non-key columns therefore depend on those columns
  holding the deleted rows' values; see O1.

## R5. The evolution steps against the delete kinds

| Step | DDL (Spark SQL) | What the rule requires |
|---|---|---|
| none | none | R1 to R4 as written |
| drop | `DROP COLUMN k` | an equality delete on `k` still applies to older files (line 1224); Spark resolves the dropped id through every historical schema (`spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/BaseReader.java` lines 259 to 290); `TestSparkReaderDeletes.java:701` drops the delete column at line 739 and asserts the survivors |
| rename | `RENAME COLUMN k TO k2` | ids are unchanged, so deletes and predicates follow the id |
| readd | `DROP COLUMN k`, `ADD COLUMN k` | the new `k` is a new id; the old delete keeps field 2; older files read null for the new id (line 340 onward) |
| promote | `k` INT to BIGINT, DECIMAL(10,2) to DECIMAL(12,2); for the other six types `p` INT to BIGINT | type promotion keeps the id (line 318); deletes compare the promoted values |
| ident | `SET IDENTIFIER FIELDS k` at create, then `DROP COLUMN k` (refused), `DROP IDENTIFIER FIELDS k`, `DROP COLUMN k` | the first drop is refused; then as `drop` |

Upstream reference failure on `readd` (O2): Spark's delete filter adds field 2 to the file projection
under its old name (`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` lines 278 to
328), so the projection holds two fields named `k` and every scan that projects the new `k` fails
`Invalid schema: multiple fields for name k: 4 and 2`. The Java generic reader resolves ids through
the current schema only (`DeleteFilter.java` line 76) and fails `Cannot find required field for ID
2` once the key is dropped. Line 1224 holds for both; the cells take the spec model below as the
reference where Spark fails.

## Fixture kind A: constructed tables

Spark 4.1.2 on a hadoop warehouse creates `(id INT, k <type>, p INT)` with `format-version` 2 (3
for `dv`), `write.format.default`, `write.delete.mode`, `write.update.mode` and
`write.merge.mode` set to Parquet and merge-on-read, `spark.sql.leafNodeDefaultParallelism=1` so
each `INSERT` writes one multi-row file. The key types are int, long, string, decimal(10,2), date,
timestamp, boolean and binary. Every fixture runs one statement plan:

1. Two `INSERT` statements (ids 1 to 4 and 5 to 8, two data files); id 4 has a null key outside `ident`; id 7
   repeats id 3's key.
2. The round-one delete of the fixture's delete kind (below).
3. The evolution step's DDL.
4. One `INSERT` of ids 9 to 11 in the evolved schema; id 9 repeats id 3's key, so a sequence-number
   error shows as a missing row 9.
5. The round-two delete of the same kind.

| Delete kind | Round one | Round two | Delete files produced (Spark's `delete_files`) |
|---|---|---|---|
| pos | `DELETE WHERE id IN (2, 6)` | `DELETE WHERE id IN (1, 10)`, `DELETE WHERE k = <id 8's key>` | position, Parquet |
| eq | equality delete of ids 3 and 4's keys | equality delete of id 10's key (`id` when the key is gone) | equality, Parquet |
| dv (v3) | as `pos` | as `pos` | Puffin deletion vectors |
| mixed | `DELETE`, `UPDATE`, `MERGE ... THEN DELETE`, equality delete | `UPDATE`, `DELETE`, equality delete | position and equality, Parquet |
| mixsql | as `mixed` with `DELETE WHERE k = ...` for the equality delete | `UPDATE`, `DELETE`, `MERGE ... THEN DELETE` | position, Parquet |
| eqw | equality delete, then `DELETE` and `UPDATE` | `DELETE`, `MERGE ... THEN DELETE` | position and equality, Parquet |

Spark SQL cannot write an equality delete, so the equality deletes are written the Flink way,
through the Iceberg Java API in the Spark JVM: `GenericFileWriterFactory` with `equalityFieldIds`,
an `EqualityDeleteWriter` from `newEqualityDeleteWriter`, committed by `newRowDelta().addDeletes`.
A key-column delete stores the full live rows read by `IcebergGenerics` (Flink's non-upsert
layout); an `id`-keyed delete stores the key column only. The `mixsql` and `eqw` kinds exist for
the writer axis: `mixsql` is the SQL-only twin of `mixed`, and `eqw` puts RePark's DML on a table
that already holds an equality delete. Each kind times six steps times eight types is 48 tables;
the four delete kinds the estimate names are 192 tables and the 384 table-door pairs it counted.

A spec model (the lane's `model.py`) replays the plan's statements Spark committed under R1 to R4
and predicts every read. It agrees with Spark on 1341 of the 1368 reads Spark answers; the 27
others are O1.

Cell spelling and view: `spelling` is the fixture id `<kind>_<type>_<step>` (for example
`eq_decimal_readd`); `view` names the door and the writer. `head` is the operation; `stmt` its
text with the table scrubbed to `<t>` or `hd.n.<t>`; `spark` and `repark` are the two answers,
rows compared in full and sorted, errors extracted whole with warehouse paths scrubbed to `<wh>`
and task and stage numbers to `N`.

| `cat` | `view` | What is compared |
|---|---|---|
| A-read | `facade-path-view`, `native-path-view` | RePark reads Spark's table at its path (`session.read.format("iceberg").load(path)` as a temp view, then `session.sql`; the native door `read_iceberg_path`) against Spark's path-view read |
| A-cat | `facade-catalog-registered` | RePark registers Spark's table (`CALL hd.system.register_table`) and reads `_deleted` and `<t>.position_deletes` against Spark's catalog read |
| A-write-stmt | `facade-sql-writer` | RePark runs the plan's statements after Spark's create and inserts on a registered Spark-written table; each statement's outcome against Spark's |
| A-write | `facade-path-view-over-repark-write`, `spark-path-view-over-repark-write` | RePark's own read and Spark's read of the RePark-written table against Spark's read of its own run |
| A-maint | `*-after-repark-maint`, `*-after-spark-maint` | survivors after RePark's `rewrite_position_delete_files` (`pos`) or `rewrite_data_files` (the rest), read by RePark and by Spark, and RePark's reads after Spark's `rewrite_position_delete_files` with `rewrite-all` on `pos` |

The operations are `star` (`SELECT * FROM v`), `ids` (the key not projected), `cnt`
(`count(*)`), `filt` (`WHERE id >= 3`) and `keyf` (`WHERE <key> IS NOT NULL`, or `p IS NOT NULL`
once the key is gone).

Severity: a deleted row returned is S2; a delete applied to the wrong rows, or a RePark-written
delete that removes the wrong rows on Spark, is S1; a refusal is S3. A writer-axis cell whose
difference follows from a statement RePark refused is S3. Where Spark fails or diverges from the
spec model, a RePark answer equal to the model carries no severity.

## Summary

**Panics: none. S1: none.** No cell on either door, in reads, writes or maintenance, raised a panic
or removed the wrong rows.

Cells per axis value:

| Axis | Values (cells) |
|---|---|
| Category | A-read 2880, A-write-stmt 1232, A-write 1152, A-maint 672, A-cat 528 |
| View | facade-path-view 1440, native-path-view 1440, facade-sql-writer 1232, spark-path-view-over-repark-write 960, facade-catalog-registered 528, facade-path-view-after-repark-maint 288, spark-path-view-after-repark-maint 288, facade-path-view-over-repark-write 192, facade-path-view-after-spark-maint 48, native-path-view-after-spark-maint 48 |
| Delete kind | pos 1296, eq 624, dv 1200, mixed 672, mixsql 1408, eqw 1264 |
| Key type | 808 each of int, long, string, decimal, date, timestamp, boolean, binary |
| Evolution step | none 1040, drop 1056, rename 1072, readd 1104, promote 1072, ident 1120 |
| Operation | star, ids, cnt, filt, keyf 768 each; statements 1232; `_deleted` 288, `position_deletes` 240; maintenance 672 |

Class counts:

| Class | A-read | A-cat | A-write-stmt | A-write | A-maint | All |
|---|---|---|---|---|---|---|
| EQUAL | 2736 | 456 | 1185 | 1011 | 624 | 6012 |
| EQUAL_REFUSE | 0 | 0 | 0 | 24 | 8 | 32 |
| REFUSE_TEXT | 0 | 0 | 32 | 0 | 0 | 32 |
| REPARK_REFUSES_SPARK_ACTS | 0 | 72 | 15 | 0 | 0 | 87 |
| REPARK_ACTS_SPARK_REFUSES_NOOP | 144 | 0 | 0 | 8 | 40 | 192 |
| ANSWER_DIFFERS | 0 | 0 | 0 | 109 | 0 | 109 |

Severity: S3 196, none 6268.

What reads equal: every Spark-written table reads the same survivors on both doors, for all six
kinds, eight types and six steps: 2736 of 2880 read cells equal, and the other 144 are the `readd`
reads Spark itself fails (O2), where both doors answer the spec model. The four delete kinds the
estimate names are 1920 of those cells (1824 equal, 96 O2). RePark's own position deletes (v2) and
Puffin deletion vectors (v3), written on Spark tables, read back the same on Spark wherever RePark
ran the statement.

Defect groups (every non-equal cell falls in exactly one):

| D | Defect | Cells | Severity | Card |
|---|---|---|---|---|
| D1 | The `_deleted` metadata column refuses on a table whose equality-delete key was dropped (`drop`, `readd`, `ident`): `FeatureUnsupported => cannot apply equality deletes on dropped column ids [2] through the residual read path`; the plain read of the same table answers | 72 | S3 | candidate, beside the AUD-004 fix (the guard is the fork's `crates/iceberg/src/arrow/row_filter_plan.rs:86`) |
| D2 | A DML predicate on a BINARY key, `DELETE ... WHERE k = CAST('b8' AS BINARY)`, refuses `Unsupported SQL type BINARY`; the read door's BINARY rewrite (BL-11) does not reach the DML filter | 85 (14 statements, 71 writer-axis reads that follow) | S3 | candidate beside BL-11 |
| D3 | `MERGE ... WHEN MATCHED THEN DELETE` on a table whose INT equality-delete key was promoted to BIGINT fails `Unexpected => equality-delete set fast path: expected Int64Array, got Int32`; reads of the same table answer | 7 (1 statement, 6 reads that follow) | S3 | candidate (fork, `crates/iceberg/src/arrow/equality_delete_set.rs:606`) |
| D4 | Dropping an identifier-field column is refused on both sides with different texts (`Unsupported table change: Cannot delete identifier field 2: k: required int ...` against `DataInvalid => Cannot delete identifier field k ...`) | 32 | S3 | candidate (text only) |
| O1 | Oracle: Spark's merge-on-read `UPDATE ... WHERE id = 5` updates and so resurrects a row a full-row equality delete removed by its boolean key, because the delete file's `id` bounds (3 to 4) prune it from the scan (R4, line 852). RePark applies the delete and leaves the row deleted, as the spec model does | 32 (`eqw_boolean`, writer axis) | none | upstream candidate; no RePark card |
| O2 | Oracle: Spark fails every read projecting the re-added `k` when an equality delete targets the dropped field of the same name (R5); RePark answers the spec model on both doors and after maintenance | 224 (144 reads, 40 maintenance, 8 RePark-written reads, 32 cells where Spark fails on both sides) | none | upstream candidate; no RePark card |

Measured, not compared: RePark's `rewrite_data_files` rewrote no file on the 48 pure-equality
tables (`0` in every count) and compacted the rest, leaving the survivors unchanged on both
readers. RePark's `rewrite_position_delete_files` without options rewrote no file on `pos`;
Spark's with `rewrite-all` rewrote three delete files into three on every `pos` table, and RePark reads the result equal on
both doors. Neither side ran the other's option set, so these are survivor cells, not
procedure-output cells.

Not measured: partitioned tables (partition-scoped and global equality deletes, R1); several
equality-delete schemas on one table (`DeleteReadTests.java:514`); struct equality keys
(`DeleteReadTests.java:613`); key-only equality deletes on a non-`id` key; RePark-written
equality deletes (RePark's SQL writes position deletes); the unregistered catalog door, where the
`hadoop` type is RePark's memory catalog and does not discover Spark's tables (the pilot's
`catalog_select` cells).

Hours per fixture kind: lane wall clock 0.65 h (about 0.3 h of fixture-kind A harness, smoke runs and the Spark seed, 0.15 h of the writer and maintenance axes, 0.2 h of write-up and gates). Machine time: the Spark seed 480 s for 240 tables
plus 144 writer seeds, and 128 s for the 48 `eqw` tables; RePark's reads on both doors 93 s;
RePark's writer axis 106 s; RePark's maintenance and catalog reads 87 s; Spark's second pass
(reads of RePark's writes, post-maintenance reads, Spark's maintenance) 172 s; RePark's
post-maintenance reads 7 s. Harness, smoke runs and the spec model took the rest of the lane.

Label rows the cells support (the draft and the estimate are not edited):

- Family 4 row "Parquet position, equality and v3 deletion-vector deletes read back; a dropped
  equality key reads the survivors": supported, widened to six kinds, eight key types and six
  evolution steps on both doors (2880 reads, 0 non-equal outside O2). Exclusions: `_deleted` over
  a dropped equality key (D1).
- Candidate row: RePark's merge-on-read DELETE, UPDATE and MERGE on Spark-written v2 and v3
  Parquet tables write position deletes and deletion vectors that Spark reads with the same
  survivors (1152 writer-axis reads, 0 S1, 0 S2); exclusions: DML predicates on BINARY (D2),
  MERGE after an equality key's INT to BIGINT promotion (D3).
- Candidate row: `position_deletes` and `_deleted` on a registered table match Spark (456 of 528);
  exclusion D1.

## Special values (2026-11-08, EMPTY-DML-SHARED-GUARD-1)

[deletes-special-values-cells-2026-11-08.jsonl](deletes-special-values-cells-2026-11-08.jsonl) is this family's special-values row: the Step 0.3 empty-DML predicates (`WHERE false`, `WHERE NULL`, `1 = 0`, `prim_col = NULL`, `id IS NULL` on a required column, the struct and array spellings, and a uuid column compared with NULL) on DELETE and UPDATE, one line per cell, verb and format on each of the two doors (`native` is `repark.sql()`, `facade` is `spark.sql()`); 150 lines in all. The Spark answer is the measured Spark 4.1.2 answer for that cell, verb and format, shared by both doors. The RePark answer is the measured answer on `main` at `fdc61f26`; the PyPI 1.5.5 wheel gives the same text on every line. The field layout follows the file above; `class` carries the Step 0.3 labels verbatim: ROWS-DIFFER (RePark's rows differ from Spark's), LINEAGE-DIFFER (v3 row lineage differs), EXTRA-COMMIT (RePark commits a snapshot Spark does not), ACCEPTED-WHERE-SPARK-REFUSED (Spark refuses, RePark answers), REPORT-SHAPE (the same effect with a different affected-row report), SETUP-REFUSED (the native door cannot create the table kind, so no DML ran), ORACLE-MISSING (Spark cannot declare a uuid column). `severity` is S1 where a DELETE whose predicate matches no row changes RePark's rows (22 lines), S3 where the native door refuses to set up the table (54 lines), and blank elsewhere; an UPDATE that rewrites files or advances lineage without changing a row is unrated here and carried by its class. `spark_changed` and `repark_changed` list what each statement changed (`rows`, `snapshot`, `files`, `persisted`, `lineage`). Spark's own refusals (`FILTER_NOT_BOOLEAN` on `DELETE` and `UPDATE … WHERE NULL` for v2-mor; `INTERNAL_ERROR` on `UPDATE … WHERE NULL` for v2-cow and v3) and its empty-snapshot commits are declared divergences under ruling D of 2026-11-08: the fixed head writes and commits nothing, so those cells are not equal to Spark by design. The uuid cells are unrated against Spark because the oracle is missing; their RePark answers are recorded as measured. This file measures; it does not state the fix's result.

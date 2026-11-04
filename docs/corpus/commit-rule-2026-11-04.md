Commit outcome: the reference rule and the fault cells (2026-11-04)
====================================================================

CORPUS-5-COMMIT, family 5 of the CORPUS-1 reference corpus (AUD-002, AUD-011). It extends
[commit-inventory-2026-11-03.md](commit-inventory-2026-11-03.md), whose 61 upstream rows and
"Interaction axes" section it measures, and it does not edit that file. The measured cells are
[commit-cells-2026-11-04.jsonl](commit-cells-2026-11-04.jsonl).

Upstream pins are the inventory's: `apache/iceberg` tag `apache-iceberg-1.11.0` (commit
`6976e020`) and `apache/spark` tag `v4.1.2` (commit `f0bb2e6`). Every path below is relative to
one of those checkouts, and an Iceberg path starts at the repository root. No Spark JVM ran for
this unit. The `spark` column of every cell is the reference contract read from the upstream
source and tests cited here. It is not a JVM run, and the head notes in R1 name the source for
each head.

The rule
--------

**R1. The command-result contract, per head.** The twelve heads are CTAS, RTAS, INSERT,
INSERT OVERWRITE, DELETE, UPDATE, MERGE, TRUNCATE, ALTER TABLE ADD/DROP/RENAME COLUMN, ALTER TABLE
SET TBLPROPERTIES, DROP TABLE and CREATE TABLE. Each head is one statement with one commit point.
When the commit returns, the statement succeeds, and the change is visible to the next read. When
the commit fails, the statement fails and no new version exists.

| Head | Upstream source of the contract | Commit point (Iceberg Java) |
|---|---|---|
| CTAS | `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestCreateTableAsSelect.java:85` (`testUnpartitionedCTAS`): the table exists with the rows | staged create transaction commit |
| RTAS | the same file, `:156` (`testRTAS`) and `:206` (`testCreateRTAS`) | staged replace transaction commit |
| INSERT | `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/UnpartitionedWritesTestBase.java:54` (`testInsertAppend`) | `SparkWrite` append, `SnapshotProducer.commit` |
| INSERT OVERWRITE | the same file, `:75` (`testInsertOverwrite`) | `SparkWrite` overwrite, `SnapshotProducer.commit` |
| DELETE | `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestDeleteFrom.java:82`; `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestDelete.java:383` (a false condition commits nothing) | row-level or metadata delete, `SnapshotProducer.commit` |
| UPDATE | `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestUpdate.java:315`, `:417` | row-level write, `SnapshotProducer.commit` |
| MERGE | `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMerge.java:801` | row-level write, `SnapshotProducer.commit` |
| TRUNCATE | `sql/core/src/test/scala/org/apache/spark/sql/execution/command/TruncateTableSuiteBase.scala:51` to `:58` (the statement returns, then `SELECT` answers `Nil`); `TestDeleteFrom.java:185` | delete of every file, `SnapshotProducer.commit` |
| ALTER ADD/DROP/RENAME COLUMN | `sql/core/src/test/scala/org/apache/spark/sql/execution/command/AlterTableAddColumnsSuiteBase.scala:41` to `:51`; `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestAlterTable.java:63`, `:167`, `:179` | `core/src/main/java/org/apache/iceberg/SchemaUpdate.java:473` to `:475` (`ops.commit(base, update)`) |
| SET TBLPROPERTIES | `TestAlterTable.java:311` | `core/src/main/java/org/apache/iceberg/PropertiesUpdate.java:100` |
| DROP TABLE | `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestDropTable.java:55` | `core/src/main/java/org/apache/iceberg/hadoop/HadoopCatalog.java:248` to `:266`: no rename; the table directory is deleted recursively, with or without `PURGE` |
| CREATE TABLE | `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestCreateTable.java:97` | catalog create (the first metadata version) |

**R2. A failure after the commit does not fail the statement.** In
`core/src/main/java/org/apache/iceberg/SnapshotProducer.java`, the commit is `taskOps.commit` at
line 501. The comment at line 517 reads "at this point, the commit must have succeeded". The
refresh at line 521 and the cleanup that follows sit inside a `catch (Throwable e)` that only logs
the error (lines 538 to 541: "Failed to load committed table metadata or during cleanup, skipping
further cleanup"). So a listing, load or refresh failure after the commit is reported as success.
`SchemaUpdate.commit` and `PropertiesUpdate.commit` end at the `ops.commit` call and make no catalog
call after it. The inventory states the same rule as axes: reporting failure for a committed change
is S2, and reporting success for an uncommitted change is S1.

**R3. Hadoop's commit is the rename, and a failed rename fails the commit.**
`core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java`:

- `commit` (lines 130 to 170) writes the new metadata under a random temporary name (line 155).
  It then calls `renameToFinal` to rename it to `v<N+1>.metadata.json` (line 162; the comment at
  line 161 calls it "the atomic commit operation"). Only after that does it write the hint (line
  167) and delete removed metadata files (line 169).
- `renameToFinal` (lines 361 to 391) throws `CommitFailedException` in each of these cases: when
  the lock is not acquired (line 364), when the destination exists ("Version %d already exists",
  lines 368 to 370), when `rename` returns false (line 381), and when it throws (line 390). Each
  path deletes the temporary file.
- `writeVersionHint` (lines 292 to 303) is best effort. An `IOException` is logged ("Failed to
  update version hint", line 302) and the commit stands.
- `refresh` (lines 101 to 124) starts at the hint version and walks forward to `N+1, N+2, …` while
  a file exists (lines 113 to 118). This is R4 of
  [hadoop-discovery-rule-2026-11-03.md](hadoop-discovery-rule-2026-11-03.md).

The tests `core/src/test/java/org/apache/iceberg/hadoop/TestHadoopCommits.java` pin the commit
rules. `testRenameReturnFalse` (line 340) and `testRenameThrow` (line 348) drive
`testRenameWithFileSystem` (lines 361 to 392). That helper asserts `CommitFailedException` (line
385) and asserts that only `v1` and `v2` remain, with no temporary file left (line 391).
`testCommitFailedToAcquireLock` (line 475) asserts "Failed to acquire lock on file" (line 487).
`testStaleVersionHint` (line 254) rolls the hint back (line 271), and the reader still finds the
newest version.

**R4. Commit-state unknown.**
`api/src/main/java/org/apache/iceberg/exceptions/CommitStateUnknownException.java` (lines 25 to
41) means "Cannot determine whether the commit was successful or not". Its message tells the
operator to check before retrying, and to remove orphan files only if the commit really failed.
`SnapshotProducer.java:504` to `:505` rethrows it before the cleanup branch, so no written file is
deleted. Spark surfaces it as the statement's error:

- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:266` to `:272`
  sets `cleanupOnAbort` only for a `CleanableFailure`. `CommitFailedException` is one
  (`api/src/main/java/org/apache/iceberg/exceptions/CommitFailedException.java:24`); the unknown
  exception is not, so the job abort keeps the files.
- `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/source/TestSparkDataWrite.java:749`
  (`testCommitUnknownException`): the statement throws `CommitStateUnknownException` (lines 802
  and 811), and because the append landed the rows are readable afterwards (lines 814 to 820).
- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMergeOnReadDelete.java:270`:
  a merge-on-read `DELETE` throws it (line 315).

RePark's contract for this case is the registry row ICE-COMMIT-UNKNOWN-1 in
[spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md), pinned by
`crates/repark-spark/src/tests/commit_unknown_doors.rs:381` (24 cells, insert and `MERGE` doors).
This unit did not inject an unknown outcome. Its failure points are definite failures, and none of
them was reported as unknown.

**R5. The local catalog's commit point (RePark's registered rule).** The registry row
ICE-LOCAL-COMMIT-POINT-1 in [spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) states
the rule. RePark's `type=memory` and `type=hadoop` catalogs keep the table pointer in process
memory. After a process dies, the table is reopened with `register_table` on the newest metadata
file. A staged CTAS or replace writes its metadata file before the publish call. So a writer killed
at that publish reads back as committed, and this is pinned by
`crates/repark-spark/src/tests/kill_mid_write.rs:837`. The cells use three reads:

1. **fresh session**: a new session over the same in-process catalog. This is the read the brief
   compares outcomes on.
2. **reopen**: a new catalog with `register_table` on the newest metadata file (R5).
3. **Java refresh walk** (`type=hadoop` only): the hint, then the forward walk (R3). This is the
   version a Java reader of the same directory takes as current.

The fixture (kind C, fault injection)
-------------------------------------

The fixture is a throwaway Rust test in `crates/repark-sql/tests` of the lane clone. It was left
untracked, is not committed, and is not product code. It extends the `FaultCatalog` of
`crates/repark-spark/tests/post_publish_outcome.rs`, whose CTAS and RTAS pins are at lines 327,
345 and 366, and it reuses the child-process kill shape of `kill_mid_write.rs`. Every commit call
(`create_table`, `publish_create_table`, `publish_replace_table`, `update_table`, `drop_table`) is
wrapped. Each cell seeds `ns.t (id INT, v INT)`, format v2, with rows 1, 2 and 3. It then runs one
statement on one door and one catalog, with one fault armed:

| Failure point | Injection |
|---|---|
| `none` | no fault (the baseline) |
| `listing_after_publish` | after the commit call returns, `list_tables` fails |
| `load_after_publish` | after the commit call returns, `load_table` fails |
| `refresh_after_publish` | after the commit call returns, `namespace_exists` fails (the namespace refresh) |
| `catalog_update_before_publish` | the first commit call fails without forwarding |
| `metadata_rename` | `type=hadoop` only: a copy of the current `vN` is placed at `v<N+1>.metadata.json`, the destination the commit must take (R3, "Version %d already exists"); a memory-catalog commit has no rename |
| `kill_before_publish` | a child process is killed with SIGKILL while it holds the first commit call; the parent reads the directory |

The doors are the Spark door (`SparkDialect` with `SparkExtension`) and the native door
(`AnsiDialect`), both through `ReparkSession::sql`, the Rust router. The catalogs are the memory
catalog and `type=hadoop` (the memory catalog with Hadoop file names). There are 14 spellings, one
per head except ALTER COLUMN, which has three (ADD, DROP and RENAME). The native door spells
CTAS and RTAS with `WITH (format_version = 2)` and SET TBLPROPERTIES with
`SET PROPERTIES (extra_properties = MAP(...))`.

Each cell records the following:

- the reported outcome: success, the failure text, or killed;
- the commit calls made;
- whether the in-process pointer moved;
- the three reads from R5, and the same session's read after the fault clears;
- orphan data files (files under `data/` that no snapshot references);
- the hint and the metadata file counts.

The classes are as follows:

- `EQUAL`: the outcome and the state agree with R1 to R3.
- `FAILURE_REPORTED_COMMITTED` (S2): a failure is reported, but the in-process pointer moved.
- `FAILURE_REPORTED_COMMITTED_ON_REOPEN` (S2): a failure is reported, and the in-process pointer
  did not move, but the R5 reopen reads the statement's change.
- `EQUAL_REGISTERED`: a kill at a staged publish that reads back as committed under R5, a declared
  deviation from R3.
- `STATE_DIFFERS`: the state after a reported success is not the one the contract names.
- `REFUSED_BEFORE_COMMIT`: the head is refused on that door before any commit call.

A panic would have been class `PANIC`, reported first. A success reported for an uncommitted change
would have been S1.

Summary
-------

**Panics: none. S1: none.** 364 cells. On every one of the 364, the outcome or the state was read
back.

Cells per head and failure point. Each count covers both doors and both catalogs; the
`metadata_rename` column is `type=hadoop` only.

| Head | none | listing after | load after | refresh after | before publish | rename | kill | All |
|---|---|---|---|---|---|---|---|---|
| CTAS | 4 | 4 (2 D1) | 4 | 4 (2 D1) | 4 (4 D2) | 2 | 4 (4 G1) | 26 |
| RTAS | 4 | 4 (2 D1) | 4 | 4 (2 D1) | 4 (4 D2) | 2 | 4 (4 G1) | 26 |
| INSERT | 4 | 4 | 4 | 4 | 4 | 2 | 4 | 26 |
| INSERT OVERWRITE | 4 (2 R1) | 4 (2 R1) | 4 (2 R1) | 4 (2 R1) | 4 (2 R1) | 2 (1 R1) | 4 (2 R1) | 26 |
| DELETE | 4 | 4 | 4 | 4 | 4 | 2 | 4 | 26 |
| UPDATE | 4 | 4 | 4 | 4 | 4 | 2 | 4 | 26 |
| MERGE | 4 | 4 | 4 | 4 | 4 | 2 | 4 | 26 |
| TRUNCATE | 4 | 4 (2 D1) | 4 | 4 (2 D1) | 4 | 2 | 4 | 26 |
| ALTER ADD/DROP/RENAME COLUMN | 12 | 12 (6 D1) | 12 | 12 (6 D1) | 12 | 6 | 12 | 78 |
| SET TBLPROPERTIES | 4 | 4 | 4 | 4 | 4 | 2 | 4 | 26 |
| DROP TABLE | 4 (2 D3) | 4 (2 D1, 1 D3) | 4 (2 D3) | 4 (2 D1, 1 D3) | 4 | 2 (2 D3) | 4 | 26 |
| CREATE TABLE | 4 | 4 (2 D1) | 4 | 4 (2 D1) | 4 (4 D2) | 2 | 4 (4 G1) | 26 |

Class counts by door and catalog:

| Class | Spark memory | Spark hadoop | native memory | native hadoop | All |
|---|---|---|---|---|---|
| EQUAL | 74 | 91 | 54 | 68 | 287 |
| EQUAL_REGISTERED | 3 | 3 | 3 | 3 | 12 |
| FAILURE_REPORTED_COMMITTED (S2) | 0 | 0 | 16 | 16 | 32 |
| FAILURE_REPORTED_COMMITTED_ON_REOPEN (S2) | 3 | 3 | 3 | 3 | 12 |
| STATE_DIFFERS (S3) | 4 | 1 | 2 | 1 | 8 |
| REFUSED_BEFORE_COMMIT | 0 | 0 | 6 | 7 | 13 |
| All | 84 | 98 | 84 | 98 | 364 |

Severity: S2 44, S3 8, none 312.

**What holds.** The Spark door reports the committed outcome on all 84 post-publish cells (listing,
load and refresh, 14 spellings, both catalogs). This extends the AUD-002 pin from
CTAS and RTAS to every head. The `load_after_publish` point tripped on every cell that reached a commit, on both doors,
and failed no statement. Every `catalog_update_before_publish` and `metadata_rename` failure left no new version
in process for its writer. A non-staged head (INSERT through SET TBLPROPERTIES) killed at its
commit call leaves the table as it was on every reader. INSERT, INSERT OVERWRITE, DELETE, UPDATE
and MERGE leave 1 or 2 orphan data files, which R4 and the registry's orphan ceiling allow.

**Defect groups.** Every non-equal cell falls in exactly one group.

| G | Defect | Cells | Severity | Card |
|---|---|---|---|---|
| D1 | On the native door, a listing or namespace-refresh failure after a committed CTAS, RTAS, TRUNCATE, ADD/DROP/RENAME COLUMN, DROP TABLE or CREATE TABLE is returned as the statement's error. After CTAS and CREATE TABLE, the same session still answers `table 'fault.ns.c' not found` once the fault is cleared (8 cells; the name directory stays frozen). After DROP TABLE, it still lists the dropped name and its load fails `TableNotFound` (4 cells). Native INSERT, DELETE, UPDATE, MERGE and SET PROPERTIES report the committed outcome. | 32 | S2 | NATIVE-DOOR-POST-PUBLISH-1 (AUD-011). These cells add RTAS, DROP COLUMN, RENAME COLUMN, DROP TABLE and CREATE TABLE to the AUD-011 measurement of CTAS, TRUNCATE and ADD COLUMN. |
| D2 | A staged CTAS, RTAS or CREATE TABLE whose publish call fails reports the failure and moves no in-process pointer. The staged metadata file stays, though, so the R5 reopen reads the created or replaced table with the statement's rows. On `type=hadoop`, the staged RTAS file is `v3.metadata.json`, so a Java reader (R3 refresh walk) also takes the replace as current. The staged CTAS and CREATE files are named `00000-<uuid>`, outside the walk. Both doors. `post_publish_outcome.rs:366` fails the lookup before staging and does not reach this point. | 12 | S2 | candidate (beside ICE-LOCAL-COMMIT-POINT-1: the staged file is the commit on reopen, and a failed publish should remove it or not report failure) |
| D3 | After a successful DROP TABLE on the memory catalog, the earlier metadata files stay. The R5 reopen then brings the table back at its first (empty) version. On `type=hadoop`, every version is removed but the data files stay, and an extra `v<N+1>` placed beside the table (the rename fault) brings it back. Java's `HadoopCatalog.dropTable` deletes the directory (R1). | 8 | S3 | candidate |
| G1 | A staged CTAS, RTAS or CREATE TABLE killed at its publish reads back as committed under R5 on both doors and both catalogs. The Java walk agrees for RTAS on `type=hadoop` and reads the CTAS and CREATE tables as absent. These are the measurement card NATIVE-DOOR-REPLACE-KILL-1 asks for on the native door (format v2 only). | 12 | registered | ICE-LOCAL-COMMIT-POINT-1, NATIVE-DOOR-REPLACE-KILL-1 |
| R1 | The native door refuses `INSERT OVERWRITE` before any commit ("a Spark spelling with no Trino" equivalent). This is a door design choice, not a commit defect. | 13 | none | none |

**Hours per fixture kind.** Kind C, fault injection: 0.3 lane hours from the first harness edit to
the last matrix run. That covers seven incremental builds of the test binary (the first was about
seven minutes, the later ones under one minute) and four full matrix runs of 95 to 112 seconds
each, including 56 child-process kills. Each round of the estimate's "each round is a crate build"
cost under two minutes once the crate was warm.

**Label rows these cells support.** The label draft's family-5 row is
[v2-label-supported-behaviour-draft-2026-11-03.md](v2-label-supported-behaviour-draft-2026-11-03.md),
row "5 Commit outcome". It is not edited here. The cells support it as follows:

- On the Spark door, every head reports the committed outcome after a post-publish failure (84 of
  84), so the row can name all twelve heads, not only CTAS and RTAS.
- The row cannot extend to the native door while D1 is open (32 S2 cells).
- D2 (12 S2 cells) is a failure at the publish call, not after it, so it does not contradict the
  row as written. A label claim for CTAS, RTAS or CREATE TABLE under a failed publish is not
  supported until D2 is ruled on.

The estimate row ("about 144 … 12 heads x 6 failure points x 2 doors", 2.5 to 5 hours) is not
edited either. This unit ran 364 cells (the catalog axis doubled the count) in 0.3 hours (0.5 for the whole lane, rule reading and gates included).

**Not measured.**

- format v3;
- merge-on-read DELETE, UPDATE and MERGE (copy-on-write defaults only);
- partitioned tables;
- an injected commit-state-unknown outcome (R4, pinned elsewhere);
- the facade door (`repark.sql` through Python) and the DataFrame writers;
- Glue, S3 Tables and REST catalogs, which keep their own pointer;
- concurrent writers and retries (the `TestTransaction` rows of the inventory);
- a failure inside the version-hint write;
- the `REPLACE TABLE` and `ALTER TABLE RENAME TO` spellings.

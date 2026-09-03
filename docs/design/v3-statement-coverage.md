# v3 statement coverage — every served statement and procedure, measured against PySpark

**Measured 2026-09-03 · V3-COV · base `a0cd39e` · oracle live PySpark 4.1.2 + Iceberg 1.11.0
(`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `REPARK_PARITY_LIVE=1`) ·
ledger [`task/ledgers/completed/v3-cov-statement-coverage-ledger.md`](../../task/ledgers/completed/v3-cov-statement-coverage-ledger.md)**

This document discharges [the v1.0 north star](../../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§2 pillar 4 — *full statement-coverage comparison against PySpark on v3 tables*. It is the
inventory, not a summary of one: every row below was run on both engines against the same v3
seed on 2026-09-03, and every divergence carries a registry row in
[docs/spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md).

## 1. Totals

| | Count |
|---|---|
| Statement programs measured | **81** |
| Statement classes covered | 12 groups (create · insert · delete · update · merge · alter · lifecycle · metadata · lineage · time travel · refs · call) |
| Comparison cells (statements + probes) | 267 |
| **EQUAL** — repark and Spark agree on every cell | **72** |
| **REFUSED** — both engines refuse the statement | **1** |
| **DIVERGES** — a registry row | **8** |
| Registry rows filed by this unit | 6 (`V3-COV-3` **FIXED at RP-8, 2026-09-03** · `V3-COV-4` BACKLOG · `V3-COV-5` BACKLOG · `V3-COV-6` DECLARED · `V3-COV-7` BACKLOG · `V3-COV-8` BACKLOG) |
| Registry rows an existing row already covers | 2 (`DML-1`, `G3-E8` ×2); `B-MOR-3` FIXED 2026-09-03 |
| Defects FIXED inside this unit | 2 (`V3-COV-1`, `V3-COV-2`) |
| Live runtime, matrix co-collected with the nightly live legs | 1 min 57 s |

## 2. How a row is read

- **Fixture** — the v3 seed the statement runs on. Every seed is single-file per partition on both
  engines (repark `INSERT … VALUES`, Spark `createDataFrame(…).coalesce(1).writeTo().append()`),
  so a file-shape probe is comparable. Flat seed is `(1,'a') … (4,'d')`; the partitioned seed adds
  `part` = 10, 10, 20, 20. `v2` seeds create at `format-version = 2` for the upgrade rows.
- **Probes** — how many result sets are compared after the statement: rows, row lineage
  (`_row_id` / `_last_updated_sequence_number`), and the metadata tables that matter for that
  statement (`delete_files`, `files`, `snapshots`, `manifests`, `partitions`, `refs`, `entries`).
- **Verdict** — `EQUAL` (both engines answer identically on every cell), `REFUSED` (both refuse
  the statement itself; a probe both engines refuse by design, like reading a dropped table, is an
  agreement and leaves the row `EQUAL`),
  `DIVERGES` (at least one cell differs; the row column names the registry row).
- **Pin** — every row is `python/repark/tests/test_v3_statement_coverage.py`:
  `test_v3_statement_row_reproduces_the_measured_repark_answer[<row>]` always runs, and
  `test_v3_statement_row_matches_the_live_spark_oracle[<row>]` runs the same program on the live
  oracle behind `REPARK_PARITY_LIVE=1` and re-asserts the verdict. The inventory is
  `python/repark/tests/_v3_statement_coverage_programs.py`; the measured halves are the committed
  `_v3_statement_coverage_repark.py` and `_v3_statement_coverage_spark.py`, joined with each row's
  verdict by `_v3_statement_coverage_golden.py`.

## 3. The matrix

| Row | Group | Statement(s) | Fixture | Probes | repark | Apache Spark | Verdict | Registry |
|---|---|---|---|---|---|---|---|---|
| `create-v3-flat` | create | `CREATE TABLE t (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '3')` | — | 2 | as Spark | as Spark | **EQUAL** | — |
| `create-v3-partitioned` | create | `CREATE TABLE t (id INT, name STRING, part INT) USING iceberg PARTITIONED BY (part) TBLPROPERTIES ('format-version' …` | — | 2 | as Spark | as Spark | **EQUAL** | — |
| `create-v3-bucket-transform` | create | `CREATE TABLE t (id INT, name STRING, part INT) USING iceberg PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('format-…` | — | 2 | as Spark | as Spark | **EQUAL** | — |
| `create-v3-write-order` | create | `CREATE TABLE t (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '3') WRITE ORDERED BY id` | — | 1 | parse-refuses | parse-refuses | **REFUSED** | — |
| `create-v3-properties` | create | `CREATE TABLE t (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'me…` | — | 2 | stores the three `write.*` keys the DDL set | adds `write.parquet.compression-codec = zstd` | **DIVERGES** | `V3-COV-7` |
| `ctas-v3` | create | `CREATE TABLE t USING iceberg TBLPROPERTIES ('format-version' = '3') AS SELECT 1 AS id, 'a' AS name` | — | 2 | derives `id: long, required` | derives `id: int, optional` | **DIVERGES** | `V3-COV-8` |
| `insert-into` | insert | `INSERT INTO t VALUES (5, 'e')` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `insert-into-select` | insert | `INSERT INTO t SELECT id + 10, name FROM t` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `insert-overwrite-table` | insert | `INSERT OVERWRITE t VALUES (9, 'z')` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `insert-overwrite-partition-static-values` | insert | `INSERT OVERWRITE t PARTITION (part = 10) VALUES (CAST(7 AS INT), 'g')` | part MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `insert-overwrite-partition-static-select` | insert | `INSERT OVERWRITE t PARTITION (part = 10) SELECT CAST(id AS INT), CAST(name AS STRING) FROM t WHERE id = 1` | part MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `insert-overwrite-partition-dynamic` | insert | `INSERT OVERWRITE t PARTITION (part) SELECT CAST(7 AS INT), CAST('g' AS STRING), CAST(10 AS INT)` | part MoR v3 | 2 | replaces only `part = 10` | default-STATIC wipes the table | **DIVERGES** | `DML-1` |
| `delete-where-mor` | delete | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `delete-where-cow` | delete | `DELETE FROM t WHERE id = 2` | flat COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `delete-where-partitioned-mor` | delete | `DELETE FROM t WHERE id = 2` | part MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `delete-where-partitioned-cow` | delete | `DELETE FROM t WHERE id = 2` | part COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `delete-in-subquery-mor` | delete | `DELETE FROM t WHERE id IN (SELECT id FROM src)` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `delete-not-in-subquery-mor` | delete | `DELETE FROM t WHERE id NOT IN (SELECT id FROM src)` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `delete-exists-subquery-mor` | delete | `DELETE FROM t WHERE EXISTS (SELECT 1 FROM src WHERE src.id = t.id)` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `delete-not-exists-subquery-mor` | delete | `DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM src WHERE src.id = t.id)` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `delete-in-subquery-cow` | delete | `DELETE FROM t WHERE id IN (SELECT id FROM src)` | flat COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `delete-all-rows-mor` | delete | `DELETE FROM t WHERE id > 0` | flat MoR v3 | 4 | one PUFFIN DV covering all 4 rows | drops the data file, no delete file | **DIVERGES** | `V3-COV-4` |
| `update-where-mor` | update | `UPDATE t SET name = 'z' WHERE id = 2` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `update-where-cow` | update | `UPDATE t SET name = 'z' WHERE id = 2` | flat COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `update-where-partitioned-mor` | update | `UPDATE t SET name = 'z' WHERE id = 2` | part MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `update-in-subquery-mor` | update | `UPDATE t SET name = 'z' WHERE id IN (SELECT id FROM src)` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `update-not-in-subquery-mor` | update | `UPDATE t SET name = 'z' WHERE id NOT IN (SELECT id FROM src)` | flat MoR v3 | 3 | refuses (G3-E8 valve) | updates ids 1, 3, 4 | **DIVERGES** | `G3-E8` |
| `update-exists-subquery-mor` | update | `UPDATE t SET name = 'z' WHERE EXISTS (SELECT 1 FROM src WHERE src.id = t.id)` | flat MoR v3 | 3 | refuses (G3-E8 valve) | updates id 2 | **DIVERGES** | `G3-E8` |
| `update-partition-key-cow` | update | `UPDATE t SET part = 30 WHERE id = 2` | part COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `merge-matched-update-mor` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = 'z'` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-matched-update-cow` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = 'z'` | flat COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `merge-matched-delete-mor` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-matched-delete-cow` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE` | flat COW v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `merge-not-matched-insert` | merge | `MERGE INTO t AS t USING (SELECT 9 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, …` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-not-matched-by-source-delete` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-not-matched-by-source-update` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.name = 'z'` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-mixed-arms` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id UNION ALL SELECT 9) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name…` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-matched-conditional` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED AND t.name = 'b' THEN UPDATE SET t.name =…` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-partitioned-mor` | merge | `MERGE INTO t AS t USING (SELECT 2 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE` | part MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `merge-source-table-mor` | merge | `MERGE INTO t AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = 'z'` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `alter-add-column` | alter | `ALTER TABLE t ADD COLUMN extra INT` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `alter-drop-column` | alter | `ALTER TABLE t DROP COLUMN name` | part MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `alter-rename-column` | alter | `ALTER TABLE t RENAME COLUMN name TO label` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `alter-alter-column-type` | alter | `ALTER TABLE t ALTER COLUMN id TYPE BIGINT` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `alter-add-partition-field` | alter | `ALTER TABLE t ADD PARTITION FIELD name · INSERT INTO t VALUES (5, 'e')` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `alter-drop-partition-field` | alter | `ALTER TABLE t DROP PARTITION FIELD part · INSERT INTO t VALUES (5, 'e', 30)` | part MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `alter-replace-partition-field` | alter | `ALTER TABLE t REPLACE PARTITION FIELD part WITH bucket(2, id) · INSERT INTO t VALUES (5, 'e', 30)` | part MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `alter-add-column-partitioned` | alter | `ALTER TABLE t ADD COLUMN extra INT` | part MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `alter-set-tblproperties` | alter | `ALTER TABLE t SET TBLPROPERTIES ('write.delete.granularity' = 'partition') · DELETE FROM t WHERE id = 2` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `alter-unset-tblproperties` | alter | `ALTER TABLE t UNSET TBLPROPERTIES ('write.delete.mode') · DELETE FROM t WHERE id = 2` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `alter-write-ordered-by` | alter | `ALTER TABLE t WRITE ORDERED BY id` | flat MoR v3 | 2 | refuses — sort-order evolution unimplemented | sets the write order | **DIVERGES** | `V3-COV-5` |
| `alter-set-format-version-3` | alter | `ALTER TABLE t SET TBLPROPERTIES ('format-version' = '3') · DELETE FROM t WHERE id = 2` | flat v2 (COW default) | 3 | as Spark | as Spark | **EQUAL** | — |
| `alter-set-format-version-3-mor` | alter | `ALTER TABLE t SET TBLPROPERTIES ('format-version' = '3') · DELETE FROM t WHERE id = 2` | flat v2 MoR | 3 | as Spark | as Spark | **EQUAL** | — |
| `truncate-table` | lifecycle | `TRUNCATE TABLE t` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `drop-table` | lifecycle | `DROP TABLE t` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-snapshots` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-files` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-delete-files` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-manifests` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-history` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-refs` | metadata | `ALTER TABLE t CREATE TAG t1` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-partitions` | metadata | `DELETE FROM t WHERE id = 2` | part MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-entries` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-all-data-files` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `meta-position-deletes` | metadata | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | refuses — scan not ported (schema only) | one `pos` row | **DIVERGES** | `V3-COV-6` |
| `lineage-projection` | lineage | `UPDATE t SET name = 'z' WHERE id = 2` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `time-travel-version-as-of` | time travel | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `time-travel-timestamp-as-of` | time travel | `DELETE FROM t WHERE id = 2` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `branch-create-and-read` | refs | `ALTER TABLE t CREATE BRANCH b1 · DELETE FROM t WHERE id = 2` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `branch-write` | refs | `ALTER TABLE t CREATE BRANCH b1 · INSERT INTO t.branch_b1 VALUES (5, 'e')` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `branch-create-replace-and-drop` | refs | `ALTER TABLE t CREATE BRANCH b1 · ALTER TABLE t CREATE OR REPLACE BRANCH b1 · ALTER TABLE t DROP BRANCH b1` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `tag-create-and-read` | refs | `ALTER TABLE t CREATE TAG t1 · DELETE FROM t WHERE id = 2` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `tag-retention` | refs | `ALTER TABLE t CREATE TAG t1 RETAIN 10 DAYS` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `branch-retention` | refs | `ALTER TABLE t CREATE BRANCH b1 WITH SNAPSHOT RETENTION 2 SNAPSHOTS` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `call-expire-snapshots` | call | `DELETE FROM t WHERE id = 2 · CALL cat.system.expire_snapshots(table => 'ns.t', older_than => TIMESTAMP '2999-01-01 …` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `call-remove-orphan-files` | call | `CALL cat.system.remove_orphan_files(table => 'ns.t', older_than => TIMESTAMP '2020-01-01 00:00:00')` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `call-rewrite-data-files` | call | `DELETE FROM t WHERE id = 2 · CALL cat.system.rewrite_data_files(table => 'ns.t')` | flat MoR v3 | 3 | as Spark | as Spark | **EQUAL** | — |
| `call-rewrite-manifests` | call | `CALL cat.system.rewrite_manifests(table => 'ns.t')` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
| `call-rewrite-position-delete-files` | call | `DELETE FROM t WHERE id = 2 · CALL cat.system.rewrite_position_delete_files(table => 'ns.t')` | flat MoR v3 | 2 | returns `0, 0` | returns `0, 0` | **EQUAL** | — |
| `call-rollback-to-snapshot` | call | `DELETE FROM t WHERE id = 2 · CALL cat.system.rollback_to_snapshot(table => 'ns.t', snapshot_id => …)` | flat MoR v3 | 2 | as Spark | as Spark | **EQUAL** | — |
| `call-register-table` | call | `CALL cat.system.register_table(table => 'ns.t_reg', metadata_file => '…')` | flat MoR v3 | 1 | as Spark | as Spark | **EQUAL** | — |
## 4. The divergences, in one place

| Registry row | Statement | What differs | Class | Owner |
|---|---|---|---|---|
| `DML-1` | `INSERT OVERWRITE t PARTITION (part) …` | repark replaces only the source partitions; Spark SQL's default `partitionOverwriteMode=STATIC` wipes the table | DECLARED residue on a FIXED row (2026-08-30, DML-B) | repark, deliberate |
| `G3-E8` | `UPDATE … WHERE col NOT IN (SELECT …)` and `UPDATE … WHERE EXISTS (…)` | repark refuses at the valve; Spark updates the matching rows | DEFECT, partial fix | repark |
| `B-MOR-3` | `CALL system.rewrite_position_delete_files` | **FIXED 2026-09-03** — both engines answer four zeros on a DV-only table | FIXED (owner ruling: build); floor residue `B-MOR-3-FLOOR-1` | — |
| `V3-COV-3` | partitioned `INSERT INTO` on v3 | `_row_id` was assigned by an unstable data-file order — two permutations across twelve runs | **FIXED (RP-8, 2026-09-03)** — the fork's `FanoutWriter::close` drains ascending, 12 of 12 runs give Spark's mapping | fork `IcebergTableProvider::insert_into` |
| `V3-COV-4` | `DELETE FROM t WHERE id > 0` (MoR) | repark writes one Puffin DV covering every row and keeps both data files live (`t.files` `[(0, 4), (1, 4)]`); Spark drops the data file and leaves `t.files` and `t.delete_files` empty | BACKLOG | repark |
| `V3-COV-5` | `ALTER TABLE t WRITE ORDERED BY id` | repark refuses (sort-order evolution outside I7); Spark sets the write order | BACKLOG | repark |
| `V3-COV-6` | `SELECT … FROM t.position_deletes` | repark refuses (`FeatureUnsupported`, schema-only port); Spark returns the positions | DECLARED, fork TRIGGER | fork metadata-table scan |
| `V3-COV-7` | `CREATE TABLE … TBLPROPERTIES (…)` | Spark stamps `write.parquet.compression-codec = zstd` beside the DDL's keys; repark stamps only the DDL's | BACKLOG | repark |
| `V3-COV-8` | `CREATE TABLE … AS SELECT 1 AS id, 'a' AS name` | repark derives `id: long, required`; Spark derives `id: int, optional` | BACKLOG | repark |

## 5. What this unit fixed rather than declared

| Registry row | Defect | Fix | Red-first |
|---|---|---|---|
| `V3-COV-1` | `INSERT OVERWRITE t PARTITION (k = v) SELECT …` failed with `column types must match schema types, expected Utf8 but found Utf8View` — the static-partition injection positionally mapped source columns without the store-assignment cast the append path applies | `crates/repark-iceberg/src/write/partition_overwrite.rs::store_assign_source_column` — the existing `refuse_unless_write_store_assignable` guard, then a strict cast | the row's pin failed on `a0cd39e` and passes on the fix |
| `V3-COV-2` | `ALTER TABLE t ALTER COLUMN id TYPE BIGINT`, then a `_row_id` projection, raised `lineage scan could not rebuild batch: expected Int64 but found Int32` — the lineage scan rebuilt the batch under the promoted schema without casting | `crates/repark-iceberg/src/catalog/lineage_columns.rs::conform_batch` — strict cast when the scan's type differs from the declared field | the row's pin failed on `a0cd39e` and passes on the fix |

## 6. What is deliberately not a row, and what is outside the matrix

- **`_row_id` values on a partitioned seed ARE pinned again (RP-8, 2026-09-03).** While
  `V3-COV-3` was open the partitioned programs pinned `_last_updated_sequence_number` alone,
  because pinning an unstable value is the false green this matrix exists to prevent. The fork
  repin to `c1d6c9de` (fork **F-20**, `#261`) drains `FanoutWriter::close` in ascending
  partition-value order, so the delegated `INSERT` is stable: twelve runs of the identical
  statement gave Spark's `{1:0, 2:1, 3:2, 4:3}` twelve times. Every partitioned program's
  lineage probe is `SELECT id, _row_id, _last_updated_sequence_number` again — nine rows'
  golden entries re-measured on both engines — and the instability cell became a stability
  cell, `test_v3_partitioned_insert_row_id_mapping_is_stable_and_spark_ordered`, beside its
  incidental control `test_v3_ctas_partitioned_row_id_mapping_is_stable_and_spark_ordered`
  (the RePark-owned CTAS writer sorts partitions through
  `write::file_order::ascending_partition_order` and was always stable).
- **Error text is not compared across engines.** A cell where both engines refuse is `REFUSED`, not
  `DIVERGES`; each engine's own message is pinned on its own side of the golden. Every *other*
  cell kind — rows and metadata facts alike — is compared, so a cell kind cannot be added and
  silently never checked.
- **Snapshot ids, file paths and byte counts are not probed.** They are content-derived and would
  pin nothing.

**Scope: one arm per row unless the row says otherwise.** Each `CALL system.*` program runs the
procedure's **bare** form (`table =>` plus whatever the procedure requires). Their optional
arguments are outside this matrix and are covered, where they are covered at all, by their own
rows and units:

| Procedure | Arms outside this matrix | Where they are covered |
|---|---|---|
| `rewrite_data_files` | `where`, `strategy`, `sort_order`, `options` | MAINT-rewrite-data-files-options; registry `RDF-SORT-1`, `RDF-1` |
| `rewrite_manifests` | `spec_id`, `rewrite_if`, `use_caching` | MW-6; registry `MANIFEST-1`, `MANIFEST-2`, `MANIFEST-3` |
| `remove_orphan_files` | the `dry_run = false` sweep, `location`, `max_concurrent_deletes` | registry `ORPHAN-1`, `ORPHAN-2` (owner decision OD-2) |
| `expire_snapshots` | `stream_results`, `snapshot_ids`, `max_concurrent_deletes` | not covered anywhere; recorded here as owed |
| `rollback_to_snapshot` | `set_current_snapshot`, `cherrypick_snapshot` siblings | not served |
| `register_table` | S3 Tables / Glue targets | registry `S3T-1`; the acceptance legs |

The same rule holds for the DDL rows: `ALTER TABLE` covers ADD / DROP / RENAME COLUMN, ALTER COLUMN
TYPE, ADD / DROP / **REPLACE** PARTITION FIELD, SET / UNSET TBLPROPERTIES, the `format-version`
upgrade on both write modes, and `WRITE ORDERED BY`; branch and tag DDL covers CREATE, CREATE OR
REPLACE, DROP and both retention clauses. `RENAME TO`, `REPLACE TABLE`, and `ALTER TABLE … EXECUTE`
are not in the matrix.

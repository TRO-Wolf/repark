# v1.5.0 release notes — DRAFT, not a release

**DRAFT — not a release.** The tag, the version bump and the release PR are the owner's. This
draft covers `origin/main` from `2c7a4d25` (release v1.4.2, 2026-09-15) to `707e8a52`
(2026-09-27), 224 commits. Every line below comes from a commit subject in that range, the
[v1.5.0 remainder spec](v1-5-0-remainder-spec-2026-09-23.md), a staging ledger, or the
2026-09-27 02:25 matrix — pending the evening rerun wherever the matrix is stated as final.

## Spark–Iceberg parity: what now matches

Matrix 2026-09-27 02:25 (main `9aa1c185`): **705 EQUAL / 132 SPARK-CANNOT / 5
REFUSED-REGISTERED / 0 DIFFERENT, 842 cells** — pending the evening rerun. The five
REFUSED-REGISTERED cells are the dated carve-outs C-1 (3 streaming cells), C-2
(`D-NS-NESTED`) and C-4 (`TY-VARIANT-V3`); `P-RDF-PARTIAL-PROGRESS` (C-3) and
`L-INSERT-OVERWRITE` (C-5) count as EQUAL under their dated harness rules.

Inspection DDL: `SHOW CREATE TABLE` answers Spark's text; `SHOW TBLPROPERTIES` (whole map,
one key, missing key) answers Spark's rows; `SHOW TABLE EXTENDED` answers Spark's rows;
`DESCRIBE` (column, extended, identity partitions, owner stamp) answers Spark; `DESCRIBE t
VERSION AS OF` refuses where Spark refuses.

ALTER and nested DDL: nested `ALTER COLUMN st.a / arr.element / m.value TYPE`, `UNSET
TBLPROPERTIES IF EXISTS`, `ALTER NAMESPACE … SET DBPROPERTIES`, `ALTER COLUMN … COMMENT`,
`WRITE ORDERED BY` transform terms, `CREATE BRANCH` on an empty table, format-version 1
create, hive-style typed `PARTITIONED BY` columns, `RENAME TO` reading its target inside the
source catalog, `SET LOCATION`, `SET/DROP IDENTIFIER FIELDS`, `REPLACE COLUMNS`,
`ALTER COLUMN … FIRST/AFTER`, partition transform aliases and argument order.

Reads: Iceberg metadata tables load through the DataFrame reader and answer `VERSION /
TIMESTAMP AS OF`; `_file`, `_pos`, `_spec_id`, `_partition`, `_deleted` serve; `DESCRIBE`
answers metadata-table columns; `input_file_name()` works over a single Iceberg scan;
`format('iceberg').load(<table location>)` and `.load(<metadata.json>)` load through a
static table; legacy reader options refuse like Spark 4.1.2; branch reads use the current
schema; time-travel resolution (`versionAsOf`/`timestampAsOf`, `TIMESTAMP AS OF <expr>`)
answers on every door; incremental reads and the changelog; timestamp filters reach the
scan; page-index row selection on.

Writes, DataFrame door: `save(name)` treats the target as a table name; `saveAsTable`
overwrite replaces the table (fresh table, history reset); `writeTo.option('branch', …)`;
`writeTo.overwrite(condition)` overwrites by filter on partitioned and unpartitioned
tables; `bucketBy` errors with Spark's class; the write lands under the output spec id;
write options answer Spark (out-of-band channel, collision rule, writer knobs);
`session spark.sql.iceberg.snapshot-property.*` and the compression codec reach every
write; writers honour `write.metadata.metrics.*` and Java's position-delete properties.

Writes, SQL door: `INSERT INTO … REPLACE WHERE` (Spark 4 syntax), `INSERT INTO …
PARTITION (…)`, positional sources with repeated names, bucketed INSERT, nested
struct-field assignment in `UPDATE` and `MERGE`, `INSERT … BY NAME`, omitted columns fill
from the schema's write-default, `CREATE OR REPLACE … AS SELECT` records
overwrite/delete, dynamic partition overwrites honour `partitionOverwriteMode`, whole-file
DELETE deletes files, merge-schema writes refuse where Spark refuses (refusal parity, not
a flag), short `VALUES` INSERT stamps `INSERT_COLUMN_ARITY_MISMATCH`,
duplicate-source MERGE stamps `MERGE_CARDINALITY_VIOLATION`.

Types: `TIMESTAMP_LTZ` columns and literals, `TIMESTAMP_NTZ` literals and casts,
`TIMESTAMP_NS`/`TIMESTAMPTZ_NS` on the SQL door, `map<string,int>` end to end, VOID as
Iceberg `unknown` on v3, `uuid` columns presenting as strings, `CAST(string AS TIMESTAMP)`
following `stringToTimestamp` (no more silent NULLs), `CAST` to `MAP<…>` on every door,
ORC reads through a Rust scan.

Procedures and maintenance: the CALL binder routes `add_files`, `rewrite_data_files`
(branch, sort/zorder parser, options map), `rewrite_position_delete_files`,
`rewrite_manifests` (delete manifests, `spec_id`, sort_by), `expire_snapshots`,
`remove_orphan_files` (Spark defaults, table-owned sweep, `file_list_view`),
`ancestors_of`, `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path`,
`register publish_changes` with `spark.wap.id` staging, `fast_forward`,
`cherrypick_snapshot`, `set_current_snapshot`, `rollback_to_timestamp`, `spark.wap.branch`
redirects, `DROP NAMESPACE` on a non-empty namespace refuses like Spark.

Catalog and session: a fresh session starts in `spark_catalog` beside configured catalogs;
`spark.sql.defaultCatalog` moves the current catalog until `USE` pins it; `USE` forms land
as Spark; `type=memory` refuses at first use (`repark.sql.catalogExtensions=true` restores
the old kind); `listDatabases` reads the session catalog; catalog and session commands;
Iceberg system functions; Glue and S3 Tables share the session's metadata and manifest
caches.

Views: `DESCRIBE` on a view, `ALTER VIEW SET/UNSET TBLPROPERTIES` and `RENAME TO`, `SHOW
TBLPROPERTIES` and `SHOW CREATE TABLE` on a view, SQL `CREATE TEMPORARY VIEW`.

v3 row lineage: a delegated `INSERT` commits its files in Spark's fanout-writer order
(`R-MC-ROW-ID-V3` EQUAL in six runs); deterministic row-id order; owned writers sort by
the default order before stamping `sort_order_id`.

Error conditions: the Spark error-condition catalogue in `repark-common`, parsed on
native exceptions and stamped across the doors (`UNRESOLVED_COLUMN` shapes,
`TABLE_OR_VIEW_NOT_FOUND`/`ALREADY_EXISTS`, `NOT_SUPPORTED_COMMAND_FOR_V2_TABLE`,
`PARTITION_MANAGEMENT_IS_UNSUPPORTED`, `UNSUPPORTED_FEATURE` geospatial/table-operation,
`INCOMPATIBLE_DATA_FOR_TABLE` on UPDATE, `UNRESOLVED_ROUTINE` for unknown functions).

Functions and SQL surface: `grouping_id`, `bround`, `conv`, `mask`, `hash`,
`format_number`, `split`, `bin`/`rint`, `json_tuple`, `from_csv`, `schema_of_csv`,
`posexplode[_outer]`, `inline[_outer]`, window/`window_time`/`session_window`,
`DataFrame.scalar`/`exists`/`lateralJoin`/`asTable`, `freqItems`/`stat.freqItems`/
`transpose`, RLIKE, `TIMESTAMP_LTZ`/`NTZ` spellings, struct dot-access, `range()` shapes,
integral literal typing and promotion, Spark's smallint/tinyint/float/binary widths,
DOUBLE/FLOAT text byte-identical to JDK 17, runtime SET of ANSI and the session time
zone, `startswith` with Spark semantics, NaN equality and IN on scans, case-insensitive
unquoted mixed-case columns, `from_xml`/`schema_of_xml` declared.

Fork repins RP-21 through RP-54 (no RP-48 commit on main): nested reads and writes,
position deletes, the changelog reader, conflict validation scoping and retry jitter,
rewrite options/sort/zorder, sorted INSERT, promoted-type reads, manifest and
delete-manifest merging, exact row counts, footer and catalog caches, page pruning,
branch/commit/WAP shapes, v1 refs and replace-by-name, Hadoop naming and staged
creates, delete granularity, unknown/uuid presentation.

## Behaviour changes a user can observe

A fresh session's current catalog is `spark_catalog` (a table in another catalog is no
longer reachable through a `spark_catalog` name); `spark.sql.defaultCatalog` is honoured;
`type=memory` catalog blocks refuse at first use unless
`repark.sql.catalogExtensions=true`; the memory catalog creates tables at
`<warehouse>/<ns>/<t>` with Hadoop-style `v<N>.metadata.json` names; output columns keep
the query's spelling and partition sources bind case-sensitively; `saveAsTable`
overwrite replaces the table instead of overwriting in place; accept-any-schema writes
now refuse where Spark refuses; local orphan paths print with the `file:` scheme;
`count(*)` folds to the scan's exact row count; tight memory pools spill or refuse a
nested-loop join instead of panicking; catalog listing-cost pins count calls, not
wall-clock.

## Performance

`count(*)` folds to the scan's exact row count; one shared bounded Parquet footer cache
per session; Glue and S3 Tables catalogs share scoped metadata and manifest caches;
timestamp filters and page-index row selection reach the scan; 120 appends run 29 %
cheaper through manifest merging; the read-performance bed (counted Iceberg I/O, the
`ice_read_perf` bench, an AWS leg) and the v1.5.0 re-measure gate (warm footer reads to
zero, the 1 % window 170 MB → 1 MB) hold the gains.

## Fixes

Reads, DML and inspect tables after a legal type promotion (no silent row loss, no
duplicate MERGE inserts); MERGE/UPDATE/DELETE after ADD/RENAME COLUMN; concurrent
disjoint-partition MERGE and DELETE commit as in Spark; plain-`WHERE` UPDATE storms
commit 4/4; stale Hadoop writers raise `CatalogCommitConflicts` and lose nothing;
nested-evolved tables read; copy-on-write DELETE with a compound predicate over a nested
column; cross-process locks for the fixed-path test fixtures.

## Carved out of v1.5.0

In plain words, the five dated owner rulings: C-1 (2026-09-19) — structured streaming
(`readStream`/`writeStream` over Iceberg) leaves for v1.6.0, card ICE-STREAMING. C-2
(2026-09-24) — nested namespaces (`CREATE NAMESPACE sc.a.b`, listing inside one) leave
for v1.6.0; the card is not yet filed (readiness finding F-6). C-3 (2026-09-26) —
`rewrite_data_files` partial-progress commit order is not compared exactly, because
Spark's own order depends on JVM object addresses and flips run to run; RePark commits
file groups in ascending partition order. C-4 (2026-09-27) — the `variant` type leaves
for v1.6.0, card ICE-VARIANT. C-5 (2026-09-27) — the `L-INSERT-OVERWRITE` id → `_row_id`
assignment is compared order-insensitively, because Spark's own assignment flips run to
run; RePark commits to one Spark-valid outcome.

## Known follow-ups

NTZ-1 slices 2 (store assignment) and 3 (storage surface, TZ-6); the `uuid_cast.rs`
byte-offset window bug (card proposed); CASESENS-1 (the U11-EDGE and namespace-case
residues); the three-part upper-case catalog text (`C_MEM.n1.t`); the ICE-VARIANT build;
the ICE-STREAMING card; S3 path writes (U12, card S3-PATH-WRITE-1), moved to v1.5.1 by the owner on
2026-09-27. Records: §5 of the
[release-readiness report](v1-5-0-release-readiness-2026-09-27.md).

# Spark–Iceberg parity inventory (2026-09-19, night run 23c)

**Status:** intake, measured 2026-09-18 21:40 → 2026-09-19 (Eastern). Owner direction of 2026-09-18 20:55: *"with the Iceberg
work, target a goal of making 1 to 1 parity with spark's integration with iceberg, every read, write, system command,
everything, NO STONES LEFT."* The 2026-09-16 rating and the 2026-09-18 re-rating measured the shapes someone thought to
probe. This inventory works the other way round: it enumerates the whole surface of Spark 4.1.2 with
`iceberg-spark-runtime-4.1_2.13` 1.11.0 from the runtime jar and the 1.11 documentation, measures every cell on Spark and
on RePark, and ranks what is left as a unit slate. No product code changed; every number below was measured.

**Measured on** RePark main `6a140eb3` (fork pin `18ab9761`), release native. Main has since taken RP-31 (`3e6a172c`,
fork #299 and #300); the two units RP-31 touches are marked *held* below and should be re-measured, not assumed closed.

## 0. Owner ruling (2026-09-19) — this slate gates v1.5.0

The owner, 2026-09-19 evening: "I want it to gate 1.5, I need to really start using RePark for production pipelines at
work and for that we need full Iceberg support and I mean full." This amends ruling R-1 of
[ice-read-perf-slate-2026-09-18.md](ice-read-perf-slate-2026-09-18.md): v1.5.0 now waits for the read-performance
wave **and** for this slate.

**The gate, as a measurement.** The inventory harness (§6) is re-run on the release candidate's head beside Spark
4.1.2 + Iceberg 1.11.0, and the matrix of §1 must read:

| verdict | at the inventory (main `6a140eb3`) | at the v1.5.0 gate |
|---|---|---|
| DIFFERENT | 57 | **0** |
| REFUSED-UNREGISTERED | 126 | **0** |
| NOT-PARSED | 28 | **0** |
| REFUSED-REGISTERED | 87 | **0**, except the cells a dated owner ruling below carves out (C-1: the 3 streaming cells) |
| EQUAL + SPARK-CANNOT | 544 | everything else |

A registry row marked DECLARED is not an exemption: a refusal is an end state only where Spark refuses too
(SPARK-CANNOT) or where a dated owner ruling below names the cell. The 2026-09-16 rating's probes and the run-23
still-true list are part of the same gate, and so is a green `aws-acceptance` run at the release head.

**Rulings that carve a cell out.**

- **C-1 (owner, 2026-09-19) — IPI-47, structured streaming read and write of Iceberg tables, moves to v1.6.0.** The
  owner agreed to the orchestrating session's proposal ("I like it, let's get it recorded"). The three streaming cells
  (`R-STREAM-READ`, `R-STREAM-READ-SKIP`, `W-STREAM-WRITE-FILESRC`) are out of the v1.5.0 gate; every other unit of
  §2 stays in. Reason: every other gate item makes RePark answer an Iceberg statement the way Spark does, while
  streaming needs a runtime RePark does not have (`readStream` / `writeStream`, triggers, checkpoints, offsets,
  restart recovery, exactly-once commits). Its Iceberg half is built by v1.5.0 anyway — incremental append and
  changelog reads (IPI-22) are what Spark's streaming read loops over. The card is
  [ice-streaming-1-6.md](ice-streaming-1-6.md). v1.5.0's claim is therefore full Spark–Iceberg parity **for batch**;
  registry rows SES-DECL-readStream and SES-DECL-streams stay until v1.6.0 and the release notes say so.

Candidates not yet ruled, each with its default until the owner rules: IPI-17 (Spark's `merge-schema` +
`INSERT … VALUES` adds `col1..colN` — default: do not copy, DECLARED), IPI-18 (RePark accepts three reader options
Spark 4.1 refuses — default: keep accepting, DECLARED). Every other unit, IPI-40 (views) and IPI-41 (ORC and Avro data
files) included, is in the gate.

## 1. The matrix in numbers

842 cells. One verdict each:

| verdict | meaning | cells |
|---|---|---|
| EQUAL | same rows, schema and table metadata (logical snapshot summary, spec, sort order, refs, properties) | 424 |
| DIFFERENT | both answer, the answers differ (silent), or RePark accepts what Spark refuses | 57 |
| REFUSED-UNREGISTERED | Spark answers, RePark raises, and no registry row covers it | 126 |
| NOT-PARSED | Spark answers, RePark's parser rejects the statement | 28 |
| REFUSED-REGISTERED | Spark answers, RePark raises under a registry row | 87 |
| SPARK-CANNOT | Spark itself refuses (not a parity gap; the error-class comparison is in §5) | 120 |

Of the 57 DIFFERENT cells, 12 are RePark accepting a statement Spark refuses; the rest are silent differences in
rows, schema or table metadata.

| group | EQUAL | DIFFERENT | REFUSED-UNREGISTERED | NOT-PARSED | REFUSED-REGISTERED | SPARK-CANNOT |
|---|---|---|---|---|---|---|
| PROPS | 35 | 11 | 2 | 0 | 2 | 5 |
| DDL | 105 | 9 | 19 | 19 | 21 | 32 |
| PUSHDOWN | 0 | 0 | 9 | 0 | 0 | 0 |
| WRITE | 107 | 11 | 10 | 2 | 24 | 24 |
| READ | 73 | 12 | 24 | 3 | 16 | 18 |
| PROC | 54 | 9 | 31 | 0 | 15 | 30 |
| EDGE | 15 | 3 | 2 | 0 | 0 | 1 |
| V3-LINEAGE | 12 | 2 | 0 | 0 | 0 | 0 |
| FUNCTIONS | 0 | 0 | 15 | 0 | 0 | 0 |
| VIEWS | 1 | 0 | 0 | 3 | 5 | 1 |
| TYPES | 16 | 0 | 7 | 0 | 4 | 5 |
| CATALOG | 6 | 0 | 7 | 1 | 0 | 4 |

Also measured, outside the per-cell verdicts:

- **Cross-engine interop, 14 table shapes each way** (types incl. NaN / -0.0 / NULL, nested, every partition transform,
  merge-on-read v2, v3 deletion vectors with row lineage, schema and partition evolution, branches and tags, nested
  evolution, sorted copy-on-write MERGE, metadata tables, `TIMESTAMP_NTZ`, v3 ADD COLUMN): a Spark-written table
  adopted by RePark through `register_table`, and a RePark-written table read by Spark through a HadoopCatalog pointer,
  answer the writer's own reads on every query of 13 of 13 shapes in both directions (RePark cannot write the
  `TIMESTAMP_NTZ` shape; it reads Spark's).
- **Filter pushdown correctness:** 41 predicates (NULL, NaN, ±Infinity, pre-epoch timestamps, month and leap-day
  boundaries, LIKE prefixes, IN / NOT IN, compound OR) over eight partition layouts and an unpartitioned control, each
  table carrying a merge-on-read delete: 369 evaluations, 340 equal, **none silently different**; 27 RePark refusals
  (`startswith()` unresolved; a decimal literal against a NaN / Infinity DOUBLE column, ICE-NAN-DECIMAL-LITERAL-1) and 2
  Spark `INTERNAL_ERROR`s (`cat IN (…)` on an identity-partitioned merge-on-read table).
- **Column metrics:** `readable_metrics` bounds, null, NaN and value counts equal Spark's for every primitive type,
  string truncation included; only the written byte sizes differ.
- **v3 row lineage** after UPDATE, DELETE and MERGE, copy-on-write and merge-on-read, across partitions, after
  ADD COLUMN and after `rewrite_data_files`: `_row_id`, `_last_updated_sequence_number` and `next-row-id` equal Spark on
  12 of 14 cells; the two others are the row-id *order* family (IPI-15).
- **Stability:** the 736 cells of the first six families ran twice on each engine. RePark: 0 changed. Spark: 12 changed —
  ten carry raw snapshot ids or statistics-file paths in procedure output that RePark refuses anyway, so no verdict
  moves; `P-RDF-PARTIAL-PROGRESS` is nondeterministic in Spark itself and `CAT-DEFAULT-CATALOG` depends on cell order.

## 2. The ranked slate

Silent differences first, then unregistered refusals and parser gaps, then registered refusals — under the owner's
direction a DECLARED row is a work target too, ranked by how much everyday Spark usage it blocks. *Held* means another
run already owns it tonight (see §3). Size: XS < ½ day, S ≈ 1 day, M 2–4 days, L a week, XL more. Each unit's oracle is
the named cells: their Spark answers are recorded and replayable (§6).

| rank | unit | band | what | likely home | size | registry rows | oracle cells |
|---|---|---|---|---|---|---|---|
| 1 | IPI-01 | silent DIFFERENT | DataFrameReader time travel: `versionAsOf` / `timestampAsOf` options are ignored and the current snapshot is read | RePark (reader option mapping, Rust scan builder) | S | — | `R-DF-OPT-VERSIONASOF`, `R-DF-OPT-TIMESTAMPASOF`, `R-DF-OPT-VERSIONASOF-TAG`, `R-DF-OPT-VERSIONASOF-TABLE-API` |
| 2 | IPI-02 | silent DIFFERENT | `TIMESTAMP AS OF <expression>` reads the current snapshot; `TIMESTAMP AS OF <integer>` is read as milliseconds where Spark reads seconds | RePark (SQL time-travel planner) | S | — | `R-TT-TIMESTAMP-AS-OF-EPOCH`, `R-TT-TIMESTAMP-AS-OF-EXPR` |
| 3 | IPI-03 | silent DIFFERENT | INSERT OVERWRITE with a dynamic `PARTITION (col)` clause in Spark's default static mode, and the `overwrite-mode=dynamic` writer option: RePark overwrites the wrong partition set (DML-1 is marked FIXED; this shape contradicts it) | RePark (overwrite planner) | S | DML-1 | `W-INSERT-OVERWRITE-PART-DYNAMIC`, `W-DF-OPT-OVERWRITE-MODE` |
| 4 | IPI-05 | silent DIFFERENT | Write-audit-publish: `spark.wap.id` and `spark.wap.branch` are ignored, so staged writes commit to main; `publish_changes` is missing; SQL `SET spark.wap.*` fails | RePark session conf + fork (staged commit) | M | REF-3, CONF-WAP-1 | `P-CHERRYPICK-WAP`, `P-PUBLISH-CHANGES`, `SC-WAP-BRANCH-READ`, `SC-SET-SQL-WAP`, `W-INSERT-WAP-BRANCH`, `W-INSERT-WAP-ID` |
| 5 | IPI-52 | silent DIFFERENT | A table partitioned by a column whose name is not a valid Avro name (`my col`) is written, then cannot be read: the manifest's partition struct keeps the raw name (Spark sanitises it); every later read fails | fork (manifest writer Avro field-name sanitisation) | S | — | `E-QUOTED-SPACE-COL` |
| 6 | IPI-04 | silent DIFFERENT | `DROP NAMESPACE` on a non-empty namespace succeeds without CASCADE and orphans its tables (Spark raises NamespaceNotEmpty, also under CASCADE) | RePark (catalog DDL) | XS | — | `D-NS-DROP-CASCADE-HADOOP`, `D-NS-DROP-NONEMPTY-HADOOP`, `D-NS-DROP-CASCADE`, `D-NS-DROP-NONEMPTY-ERR` |
| 7 | IPI-06 | silent DIFFERENT | Writer option `output-spec-id` is ignored (writes land in the default spec) | RePark writer → fork | S | ICE-WRITE-OPTIONS-1 | `W-DF-OPT-OUTPUT-SPEC-ID` |
| 8 | IPI-07 | silent DIFFERENT | Reading a branch uses the snapshot's schema; Spark reads a branch with the table's current schema | fork (scan schema selection) / RePark | S | — | `R-BRANCH-SCHEMA` |
| 9 | IPI-08 | silent DIFFERENT | A merge-on-read DELETE matching every row of a data file writes a delete file or DV where Spark drops the file (v2 and v3) | fork (delete planning) | M | V3-COV-4 | `R-MT-FILES`, `W-DELETE-WHOLE-FILE-MOR`, `W-DELETE-WHOLE-FILE-MOR-V3` |
| 10 | IPI-55 | silent DIFFERENT | `ALTER TABLE … REPLACE COLUMNS` keeps the old data under the same names; Spark drops and re-adds the columns with new ids, so existing rows read NULL | RePark DDL | S | — | `D-X-REPLACE-COLUMNS` |
| 11 | IPI-13 | silent DIFFERENT | Delete-file granularity: `rewrite_position_delete_files` and `write.delete.granularity=file` write partition-scoped delete files where Spark writes one per data file | fork (delete writer) | M | MOR-2, ICE-RDF-RPD-COMMITS-1 | `P-RPD-REWRITE-ALL`, `P-RPD-MIN-INPUT`, `TP-DELETE-GRANULARITY-FILE` |
| 12 | IPI-09 | silent DIFFERENT | Metrics properties `write.metadata.metrics.default` / `.column.*` are ignored; full bounds are always written (bounds can hold sensitive values) | fork (writer metrics config) | M | — | `TP-METRICS-NONE`, `TP-METRICS-COUNTS`, `TP-METRICS-COLUMN` |
| 13 | IPI-11 | silent DIFFERENT | Manifest merging: `commit.manifest.min-count-to-merge` / merge-on-commit is not applied; `rewrite_manifests` leaves delete manifests alone | fork (snapshot producer, rewrite_manifests) | M | MANIFEST-1 | `P-RM-DELETE-MANIFESTS`, `TP-MANIFEST-MIN-MERGE` |
| 14 | IPI-10 | silent DIFFERENT | Location properties `write.object-storage.enabled`, `.partitioned-paths` and `write.metadata.path` are ignored | fork (location provider) | M | — | `TP-OBJECT-STORAGE`, `TP-OBJECT-STORAGE-PARTITIONED-PATHS`, `TP-METADATA-PATH` |
| 15 | IPI-14 | silent DIFFERENT | Session confs `spark.sql.iceberg.snapshot-property.*` and `spark.sql.iceberg.compression-codec` are ignored (the writer options and table properties work; measured from the Parquet footer) | RePark (session conf → writer) | XS | — | `C-CODEC-CONF-GZIP`, `SC-SNAPSHOT-PROPERTY` |
| 16 | IPI-12 | silent DIFFERENT | `rewrite_data_files` retires still-applicable position deletes; Spark keeps them (fork #301 merged 21:23, rides 23b's RP-32 bump) | fork #301 → RP-32 (23b) | held (23a/23b) | ICE-RDF-COW-BYTES-1, ICE-RDF-DANGLE-2 | `P-RDF-DELETE-THRESHOLD`, `P-RDF-DELETE-RATIO`, `P-RDF-REMOVE-DANGLING`, `P-RDF-MOR-REWRITE-ALL` |
| 17 | IPI-50 | silent DIFFERENT | `rewrite_data_files` partial-progress commit and file-group granularity | held by 23a (fork #302 F-RDF-GRANULARITY-1) | held (23a) | ICE-RDF-GRANULARITY-1 | `P-RDF-PARTIAL-PROGRESS` |
| 18 | IPI-15 | silent DIFFERENT | v3 `_row_id` assignment order: across partitions RePark follows ascending partition order where Spark follows hash-task order (declared residual, RP-31); new shapes — INSERT OVERWRITE / INSERT … SELECT that read the target table assign ids in a different scan order | RP-31 (23b) for partitions; scan-order shapes unowned — RePark | held (23b) | V3-COV-3, V3-FILEORDER-1 | `L-INSERT-OVERWRITE`, `L-CTAS-SOURCE`, `R-MC-ROW-ID-V3` |
| 19 | IPI-16 | silent DIFFERENT | Metadata-only differences: no `owner` property; a no-match DELETE commits no snapshot (Spark commits an empty one); `saveAsTable(mode=overwrite)` is INSERT OVERWRITE rather than a replace; DESCRIBE EXTENDED partition section | RePark | S | ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE, DESC-1 | `D-CREATE-DEFAULT-PROPS`, `D-DESCRIBE-EXTENDED`, `W-DELETE-NULL-PRED`, `W-DF-SAVEASTABLE-OVERWRITE` |
| 20 | IPI-57 | layout (logically equal) | File layout: MERGE and UPDATE write 2–3 data files per commit where single-core Spark writes one (rows, metadata and snapshot summaries otherwise equal) — small-file growth on every DML | RePark DML writer (one writer per partition per commit) | S | CUTOVER-MERGE-FILES-1 | `L-MERGE-UPSERT-COW`, `L-MERGE-UPSERT-MOR`, `W-UPDATE-SUBQUERY`, `W-MERGE-UPSERT`, `W-MERGE-UPSERT-MOR`, `W-MERGE-UPSERT-V3`, `W-MERGE-EXPLICIT-SET`, `W-MERGE-COND-CLAUSES`, `W-MERGE-ALL-CLAUSES`, `W-MERGE-ALL-CLAUSES-MOR`, `W-MERGE-SUBQUERY-SOURCE`, `W-MERGE-NON-EQUI`, `W-MERGE-BRANCH`, `W-MERGE-NESTED` |
| 21 | IPI-53 | silent DIFFERENT | Identifier case: `SELECT ID` output column keeps the table's case (Spark echoes the query's), `ADD PARTITION FIELD CAT` accepted with the upper-case name (Spark refuses), `catalog.listDatabases` scope; an unaliased `CAST(col AS STRING)` beside `col` collides on name | RePark resolution / SQL projection naming | S | ID-1 | `E-TZ-TIMESTAMP-AS-OF`, `E-CASE-SELECT`, `E-CASE-PARTITION-FIELD`, `E-CATALOG-LISTDATABASES` |
| 22 | IPI-17 | silent DIFFERENT | `spark.sql.iceberg.merge-schema` with a SQL `INSERT … VALUES` into an accept-any-schema table: Spark adds `col1..colN` columns (a Spark quirk; recommend DECLARED) | ruling | XS | — | `W-INSERT-MERGE-SCHEMA-CONF`, `W-ACCEPT-ANY-INSERT-VALUES` |
| 23 | IPI-18 | lenient (RePark accepts, Spark refuses) | RePark accepts what Spark refuses: catalog-qualified `RENAME TO`, `DESCRIBE … VERSION AS OF`, the reader options `snapshot-id` / `as-of-timestamp` / `tag` that Iceberg 1.11 removed | ruling (keep lenient or refuse) | XS | — | `D-RENAME-TABLE`, `D-DESCRIBE-VERSION`, `R-DF-OPT-SNAPSHOT-ID`, `R-DF-OPT-AS-OF-TIMESTAMP`, `R-DF-OPT-TAG`, `R-DF-OPT-SNAPSHOT-TABLE-API` |
| 24 | IPI-19 | unregistered refusal / not parsed | Schema evolution on write: DataFrame `mergeSchema` / `merge-schema` appends (v1 and v2 writers) and `MERGE WITH SCHEMA EVOLUTION` refuse | RePark writer + MERGE parser, fork UpdateSchema | M | DML-3, ICE-WRITE-OPTIONS-1 | `W-MERGE-SCHEMA-EVOLUTION-BIGINT`, `W-MERGE-SCHEMA-EVOLUTION-NO-NEW-COL`, `W-DF-OPT-MERGE-SCHEMA-NAMED`, `W-DF-OPT-MERGE-SCHEMA-ICEBERG-NAMED`, `W-DF-V2-MERGE-SCHEMA` |
| 25 | IPI-56 | unregistered refusal / not parsed | DataFrame `mergeInto`: Spark's own condition form (qualifying the target by its short table name) fails with `No field named <table>.id`; `withSchemaEvolution()` refuses | RePark facade → MERGE planner | S | EX-DF-9 | `W-DFMERGE-UPSERT`, `W-DFMERGE-DELETE`, `W-DFMERGE-UPDATE-COLS`, `W-DFMERGE-NOT-MATCHED-BY-SOURCE`, `W-DFMERGE-CONDITIONAL`, `W-DFMERGE-SCHEMA-EVOLUTION` |
| 26 | IPI-32 | unregistered refusal / not parsed | Catalog and session SQL: `USE catalog[.ns]`, `SHOW CATALOGS`, `current_catalog()`, `REFRESH TABLE`, `CACHE TABLE`, `SHOW COLUMNS`, runtime `spark.sql.catalog.*` registration via conf.set, the `hadoop` catalog type, `table-default.*` / `table-override.*` | RePark session + catalog config | M | — | `D-RENAME-TABLE-SHORT`, `D-SHOW-COLUMNS`, `P-CALL-NO-CATALOG`, `CAT-TYPE-HADOOP`, `CAT-CATALOG-IMPL-INMEMORY`, `CAT-TABLE-DEFAULT-OVERRIDE`, `CAT-USE-CATALOG-NS`, `CAT-CURRENT-CATALOG`, `CAT-SHOW-CATALOGS`, `CAT-REFRESH-TABLE`, `CAT-CACHE-TABLE` |
| 27 | IPI-21 | unregistered refusal / not parsed | `DROP TABLE … PURGE` does not parse; `gc.enabled=false` purge guard | parser | XS | — | `D-DROP-TABLE-PURGE` |
| 28 | IPI-25 | unregistered refusal / not parsed | `REPLACE TABLE` and `REPLACE TABLE … AS SELECT` do not parse (CREATE OR REPLACE works) | parser | S | RTAS-OPS-1 | `D-REPLACE`, `D-RTAS`, `D-RTAS-TIME-TRAVEL` |
| 29 | IPI-22 | unregistered refusal / not parsed | Incremental and changelog reads: `start-snapshot-id` / `end-snapshot-id`, `t.changes`, `create_changelog_view` (all arguments) | fork (incremental append / changelog scan) + RePark | L | — | `P-CHANGELOG-DEFAULT`, `P-CHANGELOG-DEFAULT-NAME`, `P-CHANGELOG-OPTIONS`, `P-CHANGELOG-COMPUTE-UPDATES`, `P-CHANGELOG-NET-CHANGES`, `R-DF-OPT-INCREMENTAL`, `R-DF-OPT-INCREMENTAL-OPEN`, `R-DF-INCREMENTAL-OVERWRITE-ERR`, `R-CHANGES-TABLE`, `R-CHANGES-READER`, `R-CHANGES-TS` |
| 30 | IPI-20 | unregistered refusal / not parsed | Metadata columns `_file`, `_pos`, `_spec_id`, `_partition`, `_deleted` | held by 23b (owner list 20:58) — RePark scan + fork | held (23b) | — | `R-MC-FILE`, `R-MC-FILE-DISTINCT`, `R-MC-POS`, `R-MC-POS-MOR`, `R-MC-SPEC-ID`, `R-MC-PARTITION`, `R-MC-PARTITION-UNPART`, `R-MC-DELETED`, `R-MC-FILE-FILTER`, `R-MC-SPEC-ID-EVOLVED`, `R-MC-SPEC-ID-EVO` |
| 31 | IPI-23 | unregistered refusal / not parsed | Time-travel selectors `t.snapshot_id_<id>`, `t.at_timestamp_<ms>`, and metadata tables composed with time travel | RePark identifier resolution (MT-1 held by 23b) | S | MT-1 | `R-TT-SNAPSHOT-ID-SELECTOR`, `R-TT-AT-TIMESTAMP-SELECTOR`, `R-REF-BRANCH-FILES`, `R-DF-LOAD-META-FILES-VERSIONASOF`, `R-MT-SNAPSHOTS-TT`, `R-MT-HISTORY-TT`, `R-MT-METADATA-LOG-ENTRIES-TT`, `R-MT-REFS-TT`, `R-MT-MANIFESTS-TT`, `R-MT-FILES-TT`, `R-MT-DATA-FILES-TT`, `R-MT-DELETE-FILES-TT`, `R-MT-ENTRIES-TT`, `R-MT-PARTITIONS-TT`, `R-MT-POSITION-DELETES-TT` |
| 32 | IPI-24 | unregistered refusal / not parsed | Nested-field DML: `UPDATE … SET st.a = …`, MERGE nested assignment | RePark DML planner | M | — | `W-UPDATE-NESTED-FIELD` |
| 33 | IPI-26 | unregistered refusal / not parsed | ALTER surface the parser rejects: nested `ALTER COLUMN st.a / arr.element / m.value TYPE`, `ADD COLUMNS` with a complex type, `UNSET TBLPROPERTIES IF EXISTS`, `SET LOCATION`, `COMMENT ON TABLE`, `SET/DROP IDENTIFIER FIELDS`, `ALTER NAMESPACE … SET DBPROPERTIES` | parser + RePark DDL | M | — | `D-ADD-COL-STRUCT`, `D-ALTER-TYPE-NESTED`, `D-ALTER-TYPE-ARRAY-ELEM`, `D-ALTER-TYPE-MAP-VALUE`, `D-UNSET-PROPS-IF-EXISTS`, `D-SET-LOCATION`, `D-COMMENT-ON`, `D-SET-IDENTIFIER`, `D-DROP-IDENTIFIER`, `D-NS-ALTER-PROPS`, `D-X-CHANGE-COLUMN-TYPE`, `D-X-ADD-COL-MAP-KEY-STRUCT` |
| 34 | IPI-27 | unregistered refusal / not parsed | CREATE clauses: table `COMMENT`, column `COMMENT` in CREATE, `LOCATION`, `OPTIONS`, format-version 1, the `date()` / `date_hour()` transform aliases, `REPLACE PARTITION FIELD <transform>`, the old `bucket(col, n)` argument order, CREATE BRANCH on an empty table | RePark DDL | M | DBT-CTASCLAUSE-1, DBT-RELCOMMENT-1 | `D-CREATE-PART-DATE-ALIAS`, `D-CREATE-PART-DATEHOUR-ALIAS`, `D-CREATE-COMMENT`, `D-CREATE-LOCATION`, `D-CREATE-V1`, `D-CREATE-OPTIONS`, `D-CTAS-COMMENT`, `D-CTAS-LOCATION`, `D-CTAS-OPTIONS`, `D-ADD-PART-FIELD-OLD-SYNTAX-BUCKET`, `D-ADD-PART-FIELD-OLD-SYNTAX-TRUNC`, `D-REPLACE-PART-FIELD`, `D-REF-BRANCH-ON-EMPTY`, `D-SHOW-CREATE-PLAIN`, `D-DESCRIBE-COLUMN-PLAIN`, `D-CREATE-COL-COMMENT`, `D-SHOW-CREATE`, `D-DESCRIBE`, `D-DESCRIBE-COLUMN`, `D-X-CLUSTERED-BY`, `D-X-PARTITIONED-COLDEF`, `TP-FORMAT-V1-DELETE` |
| 35 | IPI-29 | unregistered refusal / not parsed | Iceberg SQL functions `<catalog>.system.bucket / truncate / years / months / days / hours / iceberg_version`, and `SHOW FUNCTIONS IN <catalog>.system` | RePark (Rust UDFs over the fork's transforms) | S | — | `F-BUCKET-LONG`, `F-BUCKET-STRING`, `F-BUCKET-DATE`, `F-BUCKET-DECIMAL-BINARY`, `F-TRUNCATE-STRING`, `F-TRUNCATE-LONG`, `F-TRUNCATE-DECIMAL`, `F-TRUNCATE-BINARY`, `F-YEARS`, `F-MONTHS`, `F-DAYS`, `F-HOURS`, `F-ICEBERG-VERSION`, `F-BUCKET-IN-WHERE`, `F-SHOW-FUNCTIONS` |
| 36 | IPI-54 | unregistered refusal / not parsed | Filter functions and literals: `startswith()` is unresolved; a decimal literal against a DOUBLE column holding NaN / ±Infinity raises (every partition layout; 27 of 369 predicate evaluations; none silent) | RePark SQL | S | ICE-NAN-DECIMAL-LITERAL-1 | `PD-IDENT`, `PD-BUCKET`, `PD-TRUNC`, `PD-DAYS`, `PD-MONTHS`, `PD-YEARS`, `PD-HOURS`, `PD-MULTI`, `PD-UNPARTITIONED` |
| 37 | IPI-30 | unregistered refusal / not parsed | Missing procedures: `ancestors_of`, `add_files` (all arguments), `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path` | fork (actions) + RePark CALL router | L | — | `P-POS-ANCESTORS`, `P-ANCESTORS-OF`, `P-ANCESTORS-OF-ID`, `P-ADD-FILES-PARTITIONED`, `P-ADD-FILES-UNPARTITIONED`, `P-ADD-FILES-PARTITION-FILTER`, `P-ADD-FILES-PARALLELISM`, `P-TABLE-STATS-DEFAULT`, `P-TABLE-STATS-SNAPSHOT`, `P-TABLE-STATS-COLUMNS`, `P-PART-STATS-DEFAULT`, `P-PART-STATS-SNAPSHOT`, `P-RTP-DEFAULT`, `P-RTP-STAGING`, `P-RTP-NO-FILE-LIST` |
| 38 | IPI-31 | unregistered refusal / not parsed | Procedure arguments: `expire_snapshots` snapshot_ids / stream_results / max_concurrent_deletes / clean_expired_metadata; `rewrite_data_files` branch; `rewrite_manifests` sort_by; `rewrite_position_delete_files` where; CALL mixing positional and named arguments | RePark CALL router + fork | M | — | `P-POS-RDF`, `P-POS-RPD`, `P-EXPIRE-SNAPSHOT-IDS`, `P-EXPIRE-STREAM-RESULTS`, `P-EXPIRE-MAX-CONCURRENT`, `P-EXPIRE-CLEAN-METADATA`, `P-RDF-BRANCH`, `P-RM-SORT-BY`, `P-RPD-WHERE`, `P-CALL-MIXED-ARGS` |
| 39 | IPI-35 | unregistered refusal / not parsed | Writer surface: `INSERT INTO … PARTITION (k = v)`, `INSERT INTO … REPLACE WHERE`, `save('<cat.ns.t>')` with format iceberg, `writeTo('t.branch_b')`, unaliased projections in INSERT … SELECT, `insertInto` column-name mismatch, `overwritePartitions` on an unpartitioned table | RePark writer | M | EX-W2-4 | `W-INSERT-BUCKETED`, `W-INSERT-PARTITION-CLAUSE`, `W-INSERT-OVERWRITE-WHERE`, `W-DF-SAVE-NAME`, `W-DF-SAVE-OVERWRITE-NAME`, `W-DF-V2-OVERWRITE-PARTITIONS-UNPART`, `W-DF-V2-APPEND-BRANCH-NAME` |
| 40 | IPI-38 | unregistered refusal / not parsed | Path-based reads: `load('<table location>')` and `load('<metadata.json>')` | RePark reader + fork StaticTable | S | — | `R-DF-LOAD-PATH`, `R-DF-LOAD-METADATA-JSON` |
| 41 | IPI-33 | unregistered refusal / not parsed | Types: `TIMESTAMP_LTZ`, `uuid`, `void` / unknown, the empty `map()` literal | RePark type mapping | S | — | `TY-TIMESTAMP-LTZ`, `TY-MAP`, `TY-UNKNOWN-VOID`, `TY-UUID-READ` |
| 42 | IPI-34 | unregistered refusal / not parsed | IS [NOT] NULL on struct / list / map columns (`Accessor for Field … not found`) | held — fork #299 in RP-31 (23b); CoW compound predicate is F-LIST-NULL-ACCESSOR-2 (23a) | held (23b/23a) | — | `TY-STRUCT`, `TY-ARRAY`, `TY-NESTED-DEEP` |
| 43 | IPI-28 | unregistered refusal / not parsed | `truncate(width, binary)` partition transform | held by 23a (owner list 20:58) — fork | held (23a) | — | `D-CREATE-PART-TRUNC-BIN` |
| 44 | IPI-36 | unregistered refusal / not parsed | `input_file_name()` on Iceberg scans; `load('t.snapshots')` metadata table via the reader; `SHOW TABLE EXTENDED` / `SHOW PARTITIONS` / JSON describe as Spark does | RePark | S | — | `R-MT-DESCRIBE`, `R-DF-LOAD-META`, `R-INPUT-FILE-NAME` |
| 45 | IPI-37 | unregistered refusal / not parsed | `spark.sql.iceberg.merge-schema` on SQL INSERT BY NAME with an extra column | RePark writer | XS | — | `SC-MERGE-SCHEMA-SQL` |
| 46 | IPI-40 | registered refusal | Iceberg catalog views (every CREATE / ALTER / SHOW / DESCRIBE / DROP VIEW form) and temporary views | RePark + fork (view catalog) | L | DBT-VIEW-1, DBT-TEMPVIEW-1 | `D-VIEW-CREATE`, `D-VIEW-CREATE-OR-REPLACE`, `D-VIEW-SHOW-DROP`, `D-VIEW-ALTER-PROPS`, `D-TEMP-VIEW`, `V-PROPS-COMMENT`, `V-IF-NOT-EXISTS`, `V-SHOW-CREATE`, `V-DESCRIBE`, `V-DESCRIBE-EXTENDED`, `V-SHOW-TBLPROPERTIES`, `V-ALTER-UNSET`, `V-TIME-TRAVEL-INSIDE` |
| 47 | IPI-44 | registered refusal | `remove_orphan_files` defaults (older_than, dry_run) and its six unported arguments | RePark CALL router + fork | M | ORPHAN-1, ORPHAN-2 | `P-ORPHAN-DEFAULT`, `P-ORPHAN-DRY-RUN`, `P-ORPHAN-YOUNG`, `P-ORPHAN-LOCATION`, `P-ORPHAN-MAX-CONCURRENT`, `P-ORPHAN-PREFIX-MODE`, `P-ORPHAN-EQUAL-SCHEMES`, `P-ORPHAN-PREFIX-LISTING`, `P-ORPHAN-STREAM-RESULTS`, `P-ORPHAN-FILE-LIST-VIEW` |
| 48 | IPI-41 | registered refusal | ORC and Avro data files: write (write.format.default / write-format), then DELETE / UPDATE / MERGE on them | fork (ORC/Avro writers) | L | ICE-WRITE-OPTIONS-ORC-AVRO, IO-ORC-1 | `D-CREATE-FMT-ORC`, `D-CREATE-FMT-AVRO`, `D-X-SET-FORMAT-ORC-THEN-INSERT`, `TP-FORMAT-ORC`, `W-DF-OPT-WRITE-FORMAT-ORC`, `W-DF-OPT-WRITE-FORMAT-AVRO`, `W-FMT-ORC-DELETE`, `W-FMT-ORC-UPDATE`, `W-FMT-ORC-MERGE`, `W-FMT-AVRO-DELETE`, `W-FMT-AVRO-UPDATE`, `W-FMT-AVRO-MERGE`, `W-READ-FOREIGN-ORC` |
| 49 | IPI-46 | registered refusal | SHOW statements: `SHOW TABLES IN`, `SHOW TBLPROPERTIES`, `SHOW TABLE EXTENDED`, nested `SHOW NAMESPACES` | RePark | S | ST-1, DBT-TBLPROPS-1, NS-2 | `D-SHOW-TBLPROPERTIES`, `D-SHOW-TBLPROPERTIES-KEY`, `D-SHOW-TABLES`, `D-SHOW-TABLE-EXTENDED`, `D-NS-NESTED` |
| 50 | IPI-48 | registered refusal | `TIMESTAMP_NTZ` columns on the SQL door; `variant` columns | RePark types | M | TZ-6, V3-VARIANT-SHRED-1 | `TY-TIMESTAMP-NTZ`, `TY-VARIANT-V3`, `TY-TIMESTAMP-NTZ-V3` |
| 51 | IPI-49 | registered refusal | Remaining registered refusals: ALTER COLUMN COMMENT, `WRITE ORDERED BY <transform>`, `rewrite_manifests spec_id`, mixed static+dynamic PARTITION overwrite, `overwritePartitions` with empty source, writer option `branch`, `bucketBy`, NaN vs decimal literal, map<string,list> inserts | RePark / fork | M | DBT-COLCOMMENT-1, V3-COV-5, MANIFEST-2, EX-W2-2, EX-W2-3, IO-BUCKET-1, ICE-NAN-DECIMAL-LITERAL-1, CAST-MAP-SPELL-1 | `D-ALTER-COMMENT`, `D-WRITE-ORDERED-TRANSFORM`, `P-RM-SPEC-ID`, `TY-MAP-LIST`, `R-NAN-FILTER`, `W-INSERT-OVERWRITE-PART-MIXED`, `W-DF-V2-OVERWRITE-PARTITIONS-EMPTY`, `W-DF-V2-OPTION-BRANCH`, `W-DF-V1-BUCKETBY-ERR`, `W-DF-V2-OVERWRITE-COND-PART`, `W-DF-V2-OVERWRITE-COND-ROWS` |
| 52 | IPI-45 | registered refusal | `position_deletes` metadata table scan | fork | M | V3-COV-6 | `R-MT-POSITION-DELETES`, `R-MT-POSITION-DELETES-V3` |
| 53 | IPI-42 | registered refusal | `CREATE BRANCH|TAG IF NOT EXISTS` and `DROP BRANCH|TAG IF EXISTS` | held by 23b (REF-2, owner list 20:58) | held (23b) | REF-2 | `D-REF-CREATE-BRANCH-IF-NOT-EXISTS`, `D-REF-TAG-IF-NOT-EXISTS`, `D-REF-DROP-BRANCH-IF-EXISTS`, `D-REF-DROP-TAG-IF-EXISTS` |
| 54 | IPI-43 | registered refusal | `rewrite_data_files` sort / sort_order / zorder | held by 23a (RDF-SORT-1, owner list 20:58) | held (23a) | RDF-SORT-1 | `P-RDF-SORT`, `P-RDF-SORT-TABLE-ORDER`, `P-RDF-ZORDER` |
| 55 | IPI-47 | registered refusal | Streaming read and write of Iceberg tables | RePark (no streaming engine) | XL | SES-DECL-readStream, SES-DECL-streams | `R-STREAM-READ`, `R-STREAM-READ-SKIP`, `W-STREAM-WRITE-FILESRC` |
| 56 | IPI-51 | error contract | RePark's Iceberg refusals carry no error condition or SQLSTATE where Spark does (§5) | RePark error mapping | M | — | every SPARK-CANNOT cell |

## 3. What runs 23a and 23b already hold

- **IPI-12** — `rewrite_data_files` keeps still-applicable position deletes: fork #301 merged (`29ea7f6d`), rides 23b's RP-32 bump.
- **IPI-50** — partial-progress and file-group granularity: fork #302 (23a).
- **IPI-13** overlaps 23a's F-RPD-COMMITS-1 and registry rows MOR-2 / ICE-RDF-RPD-COMMITS-1; the per-file output shape is
  the new part.
- **IPI-15** — row-id order across partitions: RP-31 on main (Q-23b-2, declared residual). The two self-read shapes
  (`L-INSERT-OVERWRITE`, `L-CTAS-SOURCE`) are new and unowned.
- **IPI-34** — IS [NOT] NULL on struct / list / map: fork #299 is on main through RP-31; the copy-on-write compound
  predicate is F-LIST-NULL-ACCESSOR-2 (23a).
- **IPI-20** metadata columns, **IPI-42** `IF [NOT] EXISTS` on refs, and the time-travel × metadata-table half of
  **IPI-23** (MT-1) are on 23b's list; **IPI-28** `truncate(binary)` and **IPI-43** sort / zorder compaction are on 23a's.

## 4. What this changes in the registry

- **DML-1 is marked FIXED; IPI-03 contradicts it.** `INSERT OVERWRITE t PARTITION (cat) SELECT …` in Spark's default
  static mode replaces the whole table on Spark and only the touched partitions on RePark; the `overwrite-mode=dynamic`
  writer option is ignored the other way round (a static wipe). Both are silent.
- **ICE-WRITE-OPTIONS-1 is marked FIXED;** `output-spec-id` is still ignored (IPI-06).
- **V3-COV-4 is not v3-only:** the whole-file merge-on-read DELETE writes a position-delete file on v2 as well (IPI-08).
- **REF-3 / CONF-WAP-1** understate WAP: `spark.wap.id` and `spark.wap.branch` do not merely store — the write lands on
  main (IPI-05).
- **EX-DF-9:** RePark's DataFrame `mergeInto` does not accept Spark's own qualifier form (IPI-56).
- New rows are needed for every unregistered unit (IPI-01, -02, -04, -07, -09, -10, -14, -19, -21, -22, -24 through
  -27, -29 through -33, -35 through -38, -52 through -55, -57). A unit that closes one files the row in the same PR.

## 5. Error-class parity where both engines refuse

118 cells refuse on both engines. RePark raises the same Python exception type as Spark on 38 and the same
error condition on 3; on 41 Spark names a condition (for example
`TABLE_OR_VIEW_ALREADY_EXISTS`, `REQUIRED_PARAMETER_NOT_FOUND`, `UNRECOGNIZED_PARAMETER_NAME`,
`DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE`, `NOT_SUPPORTED_CHANGE_COLUMN`) and RePark
exposes none through `getCondition()` / `getSqlState()`. Many of Spark's refusals on Iceberg paths are raw Java
exceptions surfacing as `Py4JJavaError` or `IllegalArgumentException`, so a class-for-class match is not the target;
the named conditions are. That is IPI-51.

## 6. Method

**Enumeration, not memory.** From the runtime jar (`javap -p -constants` on the class files): the 20 procedure classes
and every parameter each declares (`ProcedureParameter` required / optional), the 27 `SparkReadOptions`, 25
`SparkWriteOptions` and 30 `SparkSQLProperties` constants, the 16 `MetadataTableType` values, the 12 `MetadataColumns`,
the 10 statement rules of the SQL extensions grammar, and 124 `TableProperties` keys. From the 1.11 documentation (DDL,
queries, writes): the view forms and the `<catalog>.system` SQL functions the jar pass did not surface. From the
registry on origin/main: the row each refusal cites.

**One engine-neutral cell per shape.** Each cell is one Python function run unchanged against a PySpark 4.1.2 session
(`local[1]`; an Iceberg InMemoryCatalog `sc` and a HadoopCatalog `hc` for cells that need files on disk) and against a
RePark session (`register_memory_catalog` for both names), each cell on its own table. A cell records: the rows
(NaN-safe, sorted, snapshot ids mapped to ordinals, warehouse paths masked), the Spark-visible schema, and the table
metadata read from the metadata JSON (Spark's through `TableMetadataParser`, RePark's from disk): format version,
schema with requiredness, docs and defaults, identifier fields, the default spec and sort order, refs with retention,
properties, and the logical snapshot summary. File and delete-file counts are recorded separately as *layout* and do
not decide a verdict on their own. A Spark or RePark error is recorded with its type, condition, SQLSTATE and the
step that raised it.

**Discipline.** Every harness defect found in review was fixed and the family re-run on both engines (in-memory FileIO
for path and orphan cells, fixtures whose DELETE covered a whole file, a leaked session conf, duplicate cell ids). The
top silent differences were re-measured outside the harness. A read-only critic (Claude Sonnet, fresh context, working
from a copy of the evidence) re-derived every silent verdict from the raw observations and found no harness artefact
among them; its two P1s — layout-only differences hidden as EQUAL, and a codec cell that could not see the codec —
became IPI-57 and the footer-reading `C-CODEC-*` cells. Every Spark JVM ran under the shared one-JVM lock.

## 7. Limits

- One catalog kind per engine (Iceberg InMemory / Hadoop on Spark, RePark's memory catalog). Glue, S3 Tables, REST and
  Hive were not measured tonight (no AWS dispatch in this run); `migrate` and `snapshot` need a Hive session catalog and
  refuse on this Spark as configured.
- Concurrency, isolation and commit retries are not in this matrix; the rating's probes cover them.
- Table properties: every key with a reader- or writer-visible effect was probed once; Parquet page, row-group,
  bloom-filter and compression keys were checked only for read-back, not for the bytes on disk.
- Streaming is measured only as far as the refusal; Spark's streaming-read options beyond the two skip flags are not.
- `SPARK-CANNOT` is Spark as configured: geospatial types and column defaults are disabled in Spark 4.1 by default and
  were not switched on.
- Spark runs with one core so file layout is comparable; Spark's multi-core file counts and row-id order (hash-task
  order) differ by design and are only covered by IPI-15.
- RePark was measured on `6a140eb3`; later merges (RP-31 and whatever run 23 lands) are not reflected.

## 8. Owner questions

1. **Leniency (IPI-18, IPI-53).** RePark accepts statements Spark refuses: catalog-qualified `RENAME TO`, `DESCRIBE …
   VERSION AS OF`, the reader options Iceberg 1.11 removed (`snapshot-id`, `as-of-timestamp`, `tag`), and
   `ADD PARTITION FIELD CAT` against a lower-case column. Recommendation: refuse the removed reader options with Spark's
   message (code written against RePark would break on Spark), keep the rest lenient and document them.
2. **IPI-17.** With `spark.sql.iceberg.merge-schema=true` on an accept-any-schema table, Spark turns a plain `INSERT …
   VALUES` into three new columns `col1..col3` and NULLs in the real ones. Recommendation: DECLARED, do not copy.
3. **IPI-04.** Spark's Iceberg catalog refuses `DROP NAMESPACE` on a non-empty namespace even with CASCADE.
   Recommendation: refuse without CASCADE as Spark does; for CASCADE, match Spark (refuse) rather than drop tables.
4. **IPI-16.** Spark stamps `owner` (the OS user) on every table. Recommendation: stamp it too; it is cheap and tools
   read it.

## 9. Evidence

Durable on the campaign disk under `~/repark-lanes/campaign/oc-worker/nc-inventory/`: `harness.py` and the
`cells_*.py` families (the replayable probes), `out/` (each engine's raw observations), `matrix.json` and `matrix.md`
(every cell with both answers), `slate.json`, `xeng.py` with `xeng-*.json` (the cross-engine runs), `stability.json`,
`procedure-params.txt` and `tableprops.txt` (the enumeration), and `jar/` (the `javap` listings). Replay one family:
`run-engine.sh spark <name> cells_<family>.py` under the one-JVM lock, `run-engine.sh repark <name> cells_<family>.py`,
then `compare.py`, `errparity.py`, `units.py`, `render.py`.

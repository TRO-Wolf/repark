# Production Iceberg status: assessment at 2026-09-14

**Superseded 2026-09-16.** This assessment is superseded by the 2026-09-16
measured Iceberg rating, run at `a92a68db` (repark 1.4.2, fork `edc38c6a`).
This 2026-09-14 assessment ran nothing: no cargo, pytest, JVM or AWS call.
The rows the rating corrects are rewritten in place below, and each carries
"(corrected 2026-09-17 from the 2026-09-16 rating)". Nothing else in this
file changed.

Class: campaign; a dated, read-only assessment for the production cutover decision. It supersedes
the 2026-09-06 assessment (main `e100f72d`, v1.1.1). Archive it to `docs/history/` when the
cutover decision it informs is recorded or a successor assessment is accepted. It is a snapshot of
evidence, not a live capability register: capability status stays in the registry
([../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md)), the fork's
`docs/parity/GAP_MATRIX.md`, and [../../STATUS.md](../../STATUS.md).

## 1. Verdict

RePark at `main` `c9b03c67` (1.4.0, fork pin `3ebf7d36`) has a broad Iceberg integration for AWS
Glue and S3 Tables. Scans apply position deletes, equality deletes and deletion vectors. INSERT,
CTAS, INSERT OVERWRITE, DELETE, UPDATE and a RePark-owned MERGE run in copy-on-write and
merge-on-read. Format v3, schema and partition evolution and the Spark maintenance procedures are
served. Most of this is pinned on the local memory catalog in per-PR CI. A nightly tier compares
it with live Spark 4.1.2.

For the admitted silver workload, an operator can rely on row-level Spark parity on Glue v2
copy-on-write tables. That covers CTAS IF NOT EXISTS and a MERGE `UPDATE SET *` / `INSERT *` that
adds nothing when it runs a second time. The weekly maintenance CALLs have the same standing.
These shapes ran green on real Glue and S3 Tables on 2026-09-04. That run was at 1.0.1 and fork
pin `189a73ed`, not at today's revision.

An operator cannot rely on the daily gold dbt rebuild on Glue. On an existing Glue table,
`CREATE OR REPLACE TABLE … AS` publishes through a fork catalog method that the Glue catalog does
not implement at the pinned revision. The first `dbt run` creates the gold tables. Every later
run fails at publish, and the gold tables already exist in production. This review establishes
that from source code. It did not execute it.

No live AWS run is recorded after 1.0.1, and four fork pins have moved since the 2026-09-06
assessment. The shadow, writer-switch, rollback and maintenance stages C2–C5 have no completion
record in this repository. Three further items are not established: Spark or Athena reading
RePark-written silver tables, an ambiguous commit that an operator can tell apart from a refused
one, and a process-memory ceiling at production width.

Recommendation: re-accept one named candidate revision live, then run C2–C3 for silver. Do not
switch gold (C6) until a Glue replace path exists and a second `dbt run` has passed on Glue.
*(Update 2026-09-14, RP-20 + ICE-GOLD-TWICE-1: the path landed — fork `edc38c6a` carries
`GlueCatalog::publish_replace_table` (F-GLUE-REPLACE-1, #282), and the gold module runs `dbt run`
twice in `aws-acceptance.yml`; the first run is the post-merge dispatch.)*

## 2. Baseline and method

| Item | Value |
|---|---|
| RePark commit | `main` `c9b03c67` (2026-09-14), a clean clone |
| Workspace version | 1.4.0 (`Cargo.toml` `version`); the last tag is v1.4.0, 2026-09-12 (`d3a20d53`, #533) |
| Consumed fork pin | the owned iceberg-rust fork ([AGENTS.md](../../AGENTS.md) "Hard rules") at `3ebf7d36c2c1fe3664572e23ebc89a698ce2b55c`, identical on all five `[patch.crates-io]` lines of `Cargo.toml` (RP-19, `1faa8dcc`, #541, 2026-09-12) |
| Superseded assessment | 2026-09-06, `main` `e100f72d`, v1.1.1, fork pin `85db42f2` (RP-15); 164 commits lie between the two baselines |
| Fork documents | `docs/ENGINE_CONTRACT.md` and `docs/parity/GAP_MATRIX.md`, read from Cargo's checkout of the exact pinned revision |

### What was read

- Contract and state: [AGENTS.md](../../AGENTS.md), [STATUS.md](../../STATUS.md),
  [README.md](../../README.md), [ARCHITECTURE.md](../../ARCHITECTURE.md) (runtime flow 3),
  [PROJECT.md](../../PROJECT.md) (Iceberg and memory goals), and the cutover rows of
  [briefs/next-sequence.md](../../briefs/next-sequence.md).
- Maps: [crates/repark-iceberg/map.md](../../crates/repark-iceberg/map.md) and its `src/catalog`,
  `src/write` and `src/write/merge` maps; the `python/dbt-repark` maps.
- Source: `crates/repark-spark/src/ctas.rs`;
  `crates/repark-iceberg/src/write/{overwrite.rs,overwrite_commit.rs,partition_overwrite.rs,predicate_dml.rs,append.rs}`;
  `crates/repark-iceberg/src/write/merge/snapshot_commit.rs`;
  `crates/repark-iceberg/src/catalog/provider.rs`;
  `crates/repark-core/src/{catalog_config.rs,session.rs,error_map.rs}`;
  `crates/repark-common/src/lib.rs`; `crates/repark-python/src/exceptions.rs`;
  `python/repark/src/repark/spark/session/reader.py`;
  `python/repark/src/repark/spark/dataframe/writer_readwriter.py`;
  `python/dbt-repark/src/dbt/adapters/repark/impl.py` and the adapter macros.
- Release gate and registry: [the v1.0 north star](../../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
  §3, §3.1, §5 and §6. From [the registry](../spark-sql-iceberg-parity.md): MT-1, REF-2, REF-3,
  DML-1 to DML-5, ENC-1, the §2.5 dbt rows, V3-GEO-1, V3-VARIANT-SHRED-1, V3-COV-1/2/4/5/6/7/8,
  BL-3/4/5, G3-E8, G3-E8-NULL, ORPHAN-1, ORPHAN-2, both ORPHAN-S3TABLES-1 rows, MOR-1, MOR-2, RDF-1,
  RDF-SCHEMA-EVO-1, the five CUTOVER-* rows, RDF-SORT-1, MANIFEST-1/2/3, S3T-1, S3T-V3-1,
  V3-FILEORDER-1, V3-UPGRADE-V4-1, and the WRITE-* and PERF-CATALOG-* queue entries.
- Cutover and guides: [inventory.md](inventory.md), [map.md](map.md),
  [dbt-on-repark.md](../guide/dbt-on-repark.md), [iceberg-guide.md](../guide/iceberg-guide.md)
  (writing and maintenance sections), the headings of
  [maintenance-policy.md](../guide/maintenance-policy.md), and the leg table of
  [tier2-aws.md](../tier2-aws.md).
- Performance and memory: [spill-coverage-matrix-2026-09-11.md](../perf/spill-coverage-matrix-2026-09-11.md);
  the accounting and cell tables of [spill-matrix-baseline.md](../perf/spill-matrix-baseline.md);
  [torture-1-2026-09-11.md](../perf/torture-1-2026-09-11.md); the headings and residual lines of
  [iceberg-write-baseline.md](../perf/iceberg-write-baseline.md) and
  [iceberg-catalog-io-baseline.md](../perf/iceberg-catalog-io-baseline.md); the method of
  [engine-iceberg-analysis-2026-09-04.md](../perf/engine-iceberg-analysis-2026-09-04.md); the
  Iceberg cells of [config-profiles-2026-09-12.md](../perf/config-profiles-2026-09-12.md). The
  AP-1 re-measures were read through the RP-17 and RP-18 ledgers.
- Ledgers: `task/ledgers/staging/` `rp-16`, `rp-17`, `rp-18`, `rp-19`, `orphan-s3tables-1` and
  `silver-s0`; `task/ledgers/completed/` `sql-harden-2-cow-shapes` and `nightly-live-1` in full,
  and `dbt-1-adapter`, `neveroom-1` and `h3-spill-residue-1` in part. The archived v3, G3, RDF and
  MANIFEST ledgers were read only through the north star's and the registry's citations.
- Tests and CI: `python/repark/tests/test_aws_acceptance.py`,
  `python/dbt-repark/tests/test_aws_acceptance_gold.py`, the test list of
  `python/dbt-repark/tests/test_gold_models.py`, `.github/workflows/{ci,parity-live,aws-acceptance}.yml`,
  [.github/workflows/map.md](../../.github/workflows/map.md), the tier sections of
  [docs/testing.md](../testing.md), and the `Makefile` targets.
- Fork at `3ebf7d36`: `docs/ENGINE_CONTRACT.md` §1, §2, §4, §5, §7a, §8, §8a and §9;
  `docs/parity/GAP_MATRIX.md` rows R88–R91, R94, R95, R97, R98, R100–R107, R110, R113, R114,
  R117, R122, R123, R126, R133, R135–R137, R157, R158, R166 and R167;
  `crates/iceberg/src/catalog/mod.rs` (the `Catalog` trait); `crates/iceberg/src/transaction/staged_table.rs`
  (`commit`); `crates/catalog/s3tables/src/catalog.rs` (`publish_replace_table`); a search of
  `crates/catalog/glue`.
- History: `git log e100f72d..c9b03c67`, with file lists for each Iceberg-relevant commit.

### What was run

Only read-only commands: `git log`, `git show`, `grep`, `sed`, `awk` and `find`, over this clone
and the fork checkout. Two of those results carry weight here. The `Catalog` trait at the pin has
14 required and 16 defaulted methods, and `NamespaceScopedCatalog` forwards 27 of them. No
`publish_replace_table` override exists anywhere in the fork's Glue catalog crate.

### What was not run

No `cargo` build or test, pytest, dbt, JVM, wheel build, AWS call or other network call. The
GitHub CI state and the nightly results at `c9b03c67` were not checked, because both need the
network. A PROVEN row is proven at the revision or date its evidence names. It does not claim
that the pin passes today.

### Status legend

- **PROVEN** — a committed test pins the capability, or a recorded run measured it. The row names
  the evidence, and the date or revision when the evidence is a run.
- **DECLARED** — a document or contract states it, but no test or recorded run in this repository
  pins it for RePark.
- **NOT ESTABLISHED** — this review found no pin and no recorded run. It does not mean the
  capability failed, unless the row names a source-level failure path.

### Where the evidence runs

| Tier | What runs | Constraint |
|---|---|---|
| Per PR | `ci.yml`: `cargo test --workspace`; every `repark-iceberg` test is AWS-free on `MemoryCatalog`. `wheels.yml` `smoke`: the full facade suite `python/repark/tests` and the dbt suite `python/dbt-repark/tests`, against the built wheel. | No secrets; memory catalog only |
| Nightly and dispatch | `parity-live.yml`: the facade suite with `REPARK_PARITY_LIVE=1` against PySpark 4.1.2 and Iceberg 1.11.0. This includes the v3 live oracle, the v3 statement coverage and the SQL-HARDEN cutover cells. | Merged code only |
| Nightly and dispatch, environment approval | `aws-acceptance.yml`: `python/repark/tests/test_aws_acceptance.py` (the Glue and S3 Tables legs) and `python/dbt-repark/tests/test_aws_acceptance_gold.py` (the Glue gold module — ICE-GOLD-TWICE-1 joined it 2026-09-14; first run pending the post-merge dispatch) | OIDC role; `TABLE_BUCKET_ARN` secret; scratch namespace |
| Tag, and a nightly platform matrix | `release.yml` and `wheels.yml` `platform-matrix`: five abi3 wheels, import and one collect | No Iceberg acceptance |
| No workflow | `make py-test-spill-matrix` and `make py-test-torture` | Run by hand |

Live runs recorded in the tree:

- aws-acceptance run 33333274383, 2026-08-30, green (MW-10).
- aws-acceptance run 33635288918, 2026-09-02, green on `main` `8c4bc55`, 6 passed (LIVE-v3).
- aws-acceptance run 33699342417, 2026-09-03, a re-dispatch with the `_row_id` assertion.
- aws-acceptance run 33916856419, dated with the 2026-09-04 S6 leak guard. It failed on a raw
  `tableExists` call, and the test was then changed
  ([python/repark/tests/map.md](../../python/repark/tests/map.md)).
- SQL-HARDEN-2's AWS legs, run with a local environment on 2026-09-04 at `c70a306` (1.0.1, fork
  `189a73ed`): 2 passed in 268 s.
- The C6 dbt-on-Glue statement in the inventory, dated 2026-09-05. No run output, run id or wheel
  identity is recorded for it.

The parity-live nightly was red every night from 2026-09-05 to 2026-09-11, until #505
(`6fa17a47`). No nightly result after that fix is recorded in the tree.

## 3. Capability table

One row per area and one status per row. Row ids (C, R, W, V, E, M, K, X, S, I, D) are used in
sections 5–7. "Doors" means the native ANSI SQL door, the Spark SQL door and the facade.

### Catalogs

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| C1 | **Glue.** Register a Glue catalog, resolve `glue_catalog.ns.table`, then create, read, write, run DML and run the maintenance CALLs through the session. | `crates/repark-core/src/session.rs` (Glue arm of `register_catalog_spec`). Runs 33635288918 (2026-09-02) and 33699342417 (2026-09-03). SQL-HARDEN-2 AWS legs, 2026-09-04 ([ledger](../../task/ledgers/completed/sql-harden-2-cow-shapes-ledger.md) §5): S1–S9 replayed on Glue; copy-on-write MERGE left 0 delete files and 1 data file. | The evidence is for 1.0.1-era revisions only. Glue gets no metadata or manifest cache (`PERF-CATALOG-AWS-CACHE-1`, BACKLOG, performance only). Credentials resolve inside the fork through the AWS SDK default chain. Replacing an existing table is row C2. | PROVEN |
| C2 | **Glue replace of an existing table.** `CREATE OR REPLACE TABLE … AS`, `writeTo().createOrReplace()` and `writeTo().replace()`. | **FIXED 2026-09-14 at fork pin `edc38c6a`** (F-GLUE-REPLACE-1, fork #282, repin RP-20): `GlueCatalog::publish_replace_table` is a version-id-checked `UpdateTable` of the metadata location through the Glue commit transport — the staged metadata is read back and uuid-matched before the send, a stale expected base is a retryable `CatalogCommitConflicts`, a lost response a typed `CommitStateUnknown`. Pre-fix record (kept): `execute_ctas` calls `StagedTableTransaction::begin_replace` (`crates/repark-spark/src/ctas.rs:243`) and `commit`; the trait default returned `FeatureUnsupported`, reached Python as `UnsupportedOperationException`, and the streamed SELECT left unreferenced files. | The fork's offline commit-transport pins prove the publish path; the live cell is `test_create_or_replace_twice_against_glue` plus the gold-twice module in `aws-acceptance.yml` (ICE-GOLD-TWICE-1). aws-acceptance run 34901483202 on `0b33f5b7` (2026-09-14) passed both: 10 silver legs in 308.55 s, including `test_create_or_replace_twice_against_glue` and `…_against_s3tables`, and the gold module in 36.19 s. The memory catalog proves the leg shape offline. | PROVEN (run 34901483202) |
| C3 | **S3 Tables.** The same session surface; the service assigns the table location. | Runs 33333274383, 33635288918 and 33699342417. SQL-HARDEN-2 AWS legs, 2026-09-04. Registry `S3T-V3-1` FIXED. | `register_table` refuses before any AWS call (`S3T-1`, DECLARED service gap). `remove_orphan_files` refuses before any IO (`ORPHAN-S3TABLES-1`, DECLARED, 2026-09-12). The service's own compaction and expiry commit concurrently (fork `docs/ENGINE_CONTRACT.md` §8). The fork implements replace publish; the `replace2` leg joins the nightly under ICE-GOLD-TWICE-1 (first run pending the post-merge dispatch). Production silver is on Glue, so this row is outside the admitted workload. | PROVEN |
| C4 | **REST, Hive, SQL/JDBC and Hadoop catalogs.** Not offered. | `CatalogKind` has Memory, Glue, S3Tables and a refused Postgres kind (`crates/repark-core/src/catalog_config.rs:22`, `:220-223`). `type = "rest"` refuses ([REST intake](../../task/roadmap/mid-term/rest-catalogs-intake-2026-09-12.md) §1). | The fork ships REST, SQL and HMS catalogs (GAP_MATRIX R126), but RePark consumes none of them. REST is tentatively slated for 1.8; release-roadmap row 1.14 makes multi-writer and REST first-class. The memory catalog serves development and tests. A Hadoop `vN.metadata.json` table can be adopted with `register_table` (`V3-ADOPT-1` FIXED). | NOT ESTABLISHED |

### Reads

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| R1 | **Scans with deletes.** v2 and v3 tables, partitioned or not; position deletes, equality deletes and Puffin deletion vectors applied. `= NaN` / `IN (NaN)` silently return nothing, and no `count(*)` fold runs on delete-free snapshots (corrected 2026-09-17 from the 2026-09-16 rating). | North star §3.1 rows 1–2: `crates/repark-spark/src/tests/v3e3.rs`, `python/repark/tests/test_v3e3_fixtures.py`. Nightly `python/repark/tests/test_v3_live_oracle.py` over Spark-written fixtures. TORTURE-1 full tier: 1,000,000 rows written, 200,000 deleted, 800,000 read on both doors ([report](../perf/torture-1-2026-09-11.md)). The claimed `PERF-ICE-COUNTSTAR-1` fold (RP-14) is absent at the current pin: `EXPLAIN SELECT count(*)` keeps `IcebergTableScan` under `AggregateExec`, and the fold pins in `python/repark/tests/test_perf_ice_scan_1.py` self-skip (`fork pin predates F-27`); the answer is correct, only the fold is absent. NaN equality measured silent on v2 and v3 (rating V2-26, probes `p_nan_eq`, `p_pruning_edges`). | The Iceberg scan takes no memory-pool reservation (row X2). The nightly live leg was red from 2026-09-05 to 2026-09-11 for a harness reason (NIGHTLY-LIVE-1). | PROVEN in part; silent NaN answers and the missing count(*) fold measured failing |
| R2 | **v3 row lineage.** `_row_id` and `_last_updated_sequence_number` on single-table, current-snapshot v3 reads, all doors. | `crates/repark-spark/src/tests/v3_lineage.rs`, `python/repark/tests/test_v3_lineage_columns.py`. | Lineage columns in joins, CTEs, subqueries and time travel refuse (`V3-ROWID-2`, DECLARED). | PROVEN |
| R3 | **Time travel and refs.** `VERSION AS OF` and `TIMESTAMP AS OF`, branch and tag DDL, reads and writes on a branch, `rollback_to_snapshot`. | `crates/repark-spark/src/tests/v3e4.rs`, `python/repark/tests/test_v3e4_refs_time_travel.py`. `REF-1` and `REF-4` FIXED. | A metadata table with `AS OF` refuses (`MT-1`, DECLARED). `IF [NOT] EXISTS` on ref DDL refuses (`REF-2`, DECLARED). Write-audit-publish is absent and fail-closed (`REF-3`, BACKLOG). A rollback needs the snapshot and its files to still exist. | PROVEN |
| R4 | **Metadata tables and DESCRIBE.** 15 tables Spark-equal on the same metadata (corrected 2026-09-17 from the 2026-09-16 rating); `DESCRIBE [EXTENDED\|FORMATTED]` returns Spark's rows. | V3-COV matrix (`python/repark/tests/test_v3_statement_coverage.py`); `crates/repark-spark/src/tests/metadata_tables.rs`; SQL-DESCRIBE-1 (`7db61bfa`, `crates/repark-spark/src/tests/describe_table.rs`). Rating probe `p_meta_stats`: 15 tables equal to Spark on the same metadata (this review listed nine). | A `position_deletes` scan refuses (`V3-COV-6`, DECLARED, fork-routed). `SHOW TABLES` hides metadata tables by design (ADR-0006). | PROVEN |
| R5 | **Incremental and changelog reads.** Not served, by a dated owner ruling (corrected 2026-09-17 from the 2026-09-16 rating). | The facade refuses `start-snapshot-id` and `end-snapshot-id` (`python/repark/src/repark/spark/session/reader.py:950`); `create_changelog_view` refuses too (rating probe `p_refs`). The north star (§6) rules both out of the v1.0 gate: owner ruling dated 2026-08-23. | The fork has whole-data-file `IncrementalAppendScan` and `IncrementalChangelogScan` (GAP_MATRIX R122, R123); RePark serves neither. The admitted workloads do not use them. | DECLARED |

### Writes

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| W1 | **INSERT and append.** `INSERT INTO`, `writeTo().append()`, `saveAsTable(mode="append")`, bulk `append`. | SQL INSERT runs through the fork `iceberg-datafusion` provider. `crates/repark-iceberg/src/write/append.rs` (`append_snapshot_stamps_engine_operation_id`). The WI-1/WI-2 store-assignment gate. | Plain `INSERT INTO` and `saveAsTable(append)` run inside the fork. They wrote 64 files where Spark writes 8 on the 1e6 bed (`WRITE-DISTRIBUTION-2`, 2026-09-06). RP-16 took fork F-INSERT-DIST-1 without re-measuring that cell (RP-16 ledger). INSERT files carry the table codec only since RP-16; before it they were uncompressed (F-WRITE-COMPRESS-1). A failed stream write leaves its rolled parquet files on storage (`WRITE-ABORT-INSERT-1`, BACKLOG). | PROVEN |
| W2 | **CTAS create.** `CREATE TABLE [IF NOT EXISTS] … AS SELECT`, `writeTo().create()`, `saveAsTable` on a new table; the SELECT streams into a staged create that publishes once. | `crates/repark-spark/src/ctas.rs` (`StagedCreate`; `publish_create_table` defaults to `register_table`). Fork R158 ✅. S1, S7, S8 and S9 CTAS rows on memory, Glue and S3 Tables (SQL-HARDEN-1/2); `python/repark/tests/test_sql_harden_cutover.py`; `python/repark/tests/test_write_distribution_1.py`. | Column requiredness and nullability match Spark (`CUTOVER-CTAS-REQ-1` and `CUTOVER-DEDUP-SCHEMA-1`, FIXED 2026-09-04). Spark stamps `write.parquet.compression-codec = zstd` at CREATE and RePark does not (`V3-COV-7`, BACKLOG, metadata only). The file layout, and with it v3 `_row_id`, is not reproducible between runs (`WRITE-GROUPING-CTAS-1`). `LOCATION`, `OPTIONS`, `CLUSTERED BY` and `COMMENT` refuse on CTAS (`DBT-CTASCLAUSE-1`, `DBT-RELCOMMENT-1`). | PROVEN |
| W3 | **INSERT OVERWRITE.** Whole table, static `PARTITION (k=v)`, dynamic `PARTITION (k)`, `writeTo().overwritePartitions()`, and `saveAsTable(mode="overwrite")` on an existing table; the files are staged, then swapped in one commit. `partitionOverwriteMode=dynamic` is ignored on a PARTITION-less overwrite, so the whole table is replaced (corrected 2026-09-17 from the 2026-09-16 rating). | `crates/repark-iceberg/src/write/overwrite.rs`, `partition_overwrite.rs`. `DML-1` and `V3-COV-1` FIXED. S4, S8 and S9 `overwritePartitions` rows equal Spark on memory, Glue and S3 Tables (2026-09-04). Rating V2-24b with orchestrator replay: the conf reads `dynamic`, yet `INSERT OVERWRITE owm SELECT 20,'b'` leaves `[(20,'b')]` where Spark keeps three rows; `overwritePartitions()` stays equal (probe `p_overwrite_mode`, twin cell #62). | The default `write.overwrite.isolation-level` is `snapshot` (`overwrite.rs:22-50`). A concurrent append is not refused, and the overwrite replaces its file. The SILVER-S0 probes show this (C-003, C-007); they ran on a fork probe branch, not the pin. The release native reproduces the replacement of a concurrent append. Transform-field static partitions, mixed static and dynamic lists, and an empty dynamic overwrite refuse (`DML-1`, DECLARED residue). | PROVEN in part; silent PARTITION-less dynamic-overwrite replacement measured failing |
| W4 | **DELETE and UPDATE.** Plain `WHERE` through the fork provider; subquery `WHERE` through RePark's identity path; copy-on-write or merge-on-read by `write.delete.mode` and `write.update.mode`. | North star §3.1 rows 9–10 (`crates/repark-spark/src/tests/v3_mor_dml.rs`, `v3_subquery_dml.rs`, `python/repark/tests/test_v3_cow_dml.py`); `python/repark/tests/test_dml_subquery_parity.py`; `crates/repark-iceberg/src/write/predicate_dml.rs`. | `UPDATE … NOT IN`, `[NOT] EXISTS`, correlated `UPDATE … IN` and every `ANY`/`ALL` form refuse loud (`G3-E8` partial fix; `G3-E8-NULL`). A v3 merge-on-read DELETE that covers a whole file writes a full DV where Spark drops the file (`V3-COV-4`, BACKLOG, storage shape). Merge-on-read DELETE/UPDATE on an unpartitioned table with multi-spec history refuses loud (the BUG-001 valve). | PROVEN |
| W5 | **MERGE INTO.** RePark-owned executor: `WHEN MATCHED` UPDATE and DELETE, `WHEN NOT MATCHED` INSERT, `WHEN NOT MATCHED BY SOURCE`; copy-on-write or merge-on-read by `write.merge.mode`; Spark's cardinality check; one snapshot per statement. | S2, S8 and S9 run MERGE twice. They match Spark row for row, and the second pass adds nothing. Copy-on-write leaves 0 delete files and 1 data file on memory, Glue and S3 Tables (2026-09-04). Also `crates/repark-spark/src/tests/merge.rs`, `merge_nmbs.rs`; `crates/repark-iceberg/src/write/merge/tests/occ.rs`, `occ_conflict.rs`. `BL-5` CLOSED: a rejected commit deletes the files it wrote. | The default `write.merge.isolation-level` is serializable with an always-true conflict filter. A concurrent append into any partition aborts the MERGE (`DML-5`, DECLARED). A `CommitStateUnknown` failure skips file cleanup (`BL-5`). Forms the parser cannot read refuse (`DML-3`). Merge-on-read MERGE writes more delete files than Spark (`CUTOVER-MERGE-FILES-1`, BACKLOG); copy-on-write writes none. | PROVEN |
| W6 | **Write distribution and order.** Hash distribution by partition-transform value on CTAS, INSERT OVERWRITE and MERGE inserts; `write.distribution-mode` `none`/`hash`/`range`; `ALTER TABLE … WRITE ORDERED BY / DISTRIBUTED BY / UNORDERED` with a per-writer sort. A plain `INSERT INTO` ignores the declared sort order (corrected 2026-09-17 from the 2026-09-16 rating). | `WRITE-DISTRIBUTION-1`, `WRITE-DISTRIBUTION-2` and `WRITE-ORDER-DIST-1` (all FIXED 2026-09-06); `V3-COV-5` FIXED. `crates/repark-iceberg/src/write/distribution/tests.rs`; `python/repark/tests/test_write_distribution_1.py`, `test_write_distribution_2.py`, `test_write_order_dist_1.py`, whose live halves compare with Spark. Rating V2-12 (probe `p_sort_overwrite`, cell #40): `INSERT INTO` into a table with `WRITE LOCALLY ORDERED BY id` wrote two unsorted 1,000-row files with `sort_order_id` NULL; Spark's twin wrote ascending files with `sort_order_id = 2`. | `range` is the hash layout plus a per-writer sort, not a global range shuffle (`WRITE-RANGE-1`, BACKLOG). Transform sort orders refuse at DDL, and a write into a transform-ordered table refuses (`WRITE-ORDER-TRANSFORM-1`, BACKLOG). The plain `INSERT INTO` path is row W1. | PROVEN in part; plain INSERT INTO ignoring the declared sort order measured failing |

Copy-on-write and merge-on-read, per statement:

| Statement | Copy-on-write | Merge-on-read, v2 | Merge-on-read, v3 | Selected by | Default isolation |
|---|---|---|---|---|---|
| INSERT / append | not applicable (append) | — | — | — | append; RePark-owned appends stamp `engine.operation-id` |
| CTAS | not applicable (staged create or replace) | — | — | — | create publish is all-or-nothing |
| INSERT OVERWRITE | `OverwriteFiles` or `ReplacePartitions` | — | — | — | `snapshot` (`write.overwrite.isolation-level`) |
| DELETE / UPDATE, plain `WHERE` | fork rewrite (`OverwriteFiles`) | Parquet position deletes, one per `(spec, partition)` group | one Puffin DV per data file | `write.delete.mode`, `write.update.mode` | serializable (fork `docs/ENGINE_CONTRACT.md` §1) |
| DELETE / UPDATE, subquery `WHERE` | RePark rewrite | Parquet position deletes, file granularity by default | Puffin DVs | same | serializable (`predicate_dml.rs`) |
| MERGE | RePark rewrite (`OverwriteFiles`) | Parquet position deletes, one per data file by default (`MOR-2`) | Puffin DVs | `write.merge.mode` | serializable (`write/merge/snapshot_commit.rs`) |

The cutover's silver tables are v2 copy-on-write (inventory §1), so the MERGE row's copy-on-write
column is the admitted path.

### Format v3 and evolution

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| V1 | **Format v3.** Opt-in CREATE and CTAS behind `repark.sql.allowCreateFormatVersion3` (default false); in-place v2→v3 upgrade; row lineage on every served write; merge-on-read DML through deletion vectors; legacy position deletes merged into DVs; `timestamp_ns`. A schema-carried `write-default` is not applied: RePark writes NULL where Spark fills the default (corrected 2026-09-17 from the 2026-09-16 rating). | North star §3.1 rows 3–11 and 16–19 with their pins (`python/repark/tests/test_v3_create_opt_in.py`; `crates/repark-spark/src/tests/v3_upgrade.rs`, `v3_mor_dml.rs`, `v3_legacy_delete.rs`, `v3_types.rs`). Live legs `test_v3_dv_dml_maintenance_against_glue` and `_s3tables` in runs 33635288918 and 33699342417. v3 statement coverage: 81 programs ([v3-statement-coverage.md](../design/v3-statement-coverage.md)). Rating V3-03b (probes `p_write_default`, `p_followup`, cells #17/#18): on a v3 column carrying `write-default: 5`, RePark `INSERT (id,name)` and MERGE `INSERT (id,name)` write NULL and a DataFrame append missing the column refuses, where Spark fills 5 on all three doors. | DECLARED exclusions: `V3-GEO-1`; `V3-VARIANT-SHRED-1`; `ENC-1` (encryption keys are stored, never applied); `V3-UPGRADE-V4-1`; `V3-FILEORDER-1`; `V3-ROWID-2`. Athena does not support v3, per the AWS documentation as read on 2026-08-23 (north star §5). The admitted silver tables are v2, so v3 is outside this cutover. | PROVEN in part; silent write-default NULL measured failing |
| E1 | **Schema and partition evolution.** Top-level add/drop/rename reads and partition-spec evolution are interop-proven. Type promotion gives silent wrong answers on reads and MERGE; MERGE, UPDATE and DELETE fail right after ADD/RENAME COLUMN until an append lands; a nested field add makes a Spark table unreadable (corrected 2026-09-17 from the 2026-09-16 rating). | Fork GAP_MATRIX R94 and R95 ✅, with bidirectional Java interop. `RDF-SCHEMA-EVO-1` FIXED at RP-15 with 11 pins (`python/repark/tests/test_rdf_schema_evo_1.py`). Rating V2-10c/V2-06b: range filters on a promoted `int→bigint` / `float→double` column silently drop pre-promotion rows (2/10 predicates correct on RePark, 10/10 on Spark over the same metadata), and MERGE on the promoted key inserts duplicates (cells #29, #55–#57; probes `p_promote_read`, `p_isolate_merge`, `p_merge_evo`). Rating V2-10e: 24/24 DML shapes fail after ADD/RENAME COLUMN with `DataInvalid => Column extra not found in table`, including RePark `MERGE … UPDATE SET * / INSERT *` on a just-evolved Spark-created table — the "silver MERGE absorbing a new bronze column" cell this review called unmeasured now fails loud (probes `p_addcol_dml`, `p_ddl_then_dml`, `p_merge_star`, cells #50/#51). Rating V2-10d: a Spark nested struct/list-element add reads `Arrow Schema Error … expected 2 got 1` (cells #32/#33). | `ALTER COLUMN … COMMENT` refuses (`DBT-COLCOMMENT-1`). Top-level add/drop/rename DDL plus reads, and spec evolution with 17 single transforms at 15/15 partition tuples, stay proven. | PROVEN in part; promotion, nested add, and evolve-then-DML measured failing |

### Maintenance

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| M1 | **`rewrite_data_files`** (binpack, `where`). Every `options` map is refused, on every key (corrected 2026-09-17 from the 2026-09-16 rating). | North star §3.1 row 12 (`crates/repark-spark/src/tests/call_v3.rs`, `call_v3_dv.rs`). `RDF-1` FIXED for a delete file that names one data file (`crates/repark-spark/src/tests/call_rewrite_dangling.rs`). `RDF-SCHEMA-EVO-1` FIXED. RP-17 and RP-18: rewrite output carries the table codec and no dead dictionary pages; live outputs were 1,839,168 B and 1,755,749 B against 2,840,672 B of zstd-3 input ([re-measure 3](../perf/adapt-part-ap1-remeasure-3-2026-09-12.md)). Rating V2-08: `CALL rewrite_data_files options map is not supported in v1` on every tried key (`min-input-files`, `rewrite-all`, `target-file-size-bytes`); `where` works; no registry row covers the refusal. | `sort` and `sort_order` refuse (`RDF-SORT-1`, DECLARED fork ceiling). A position-delete file that names two or more data files is not selected (`RDF-1` residue, fork F-16 residue 2); that shape exists only on merge-on-read v2. The AP-1 numbers come from synthetic beds on the memory catalog. | PROVEN in part; every options map refused, measured failing |
| M2 | **`rewrite_position_delete_files`.** | `B-MOR-3` and `B-MOR-3-FLOOR-1` FIXED; `MOR-1` FIXED; pins in `crates/repark-spark/src/tests/call_v3_dv.rs`. | Matters only for merge-on-read tables; copy-on-write tables hold no delete files. | PROVEN |
| M3 | **`rewrite_manifests`.** | MW-6 (`crates/repark-spark/src/tests/call_manifests.rs`). SCALE-v3 exercised it on v3 (59 manifests → 1). | Rewrites data manifests only (`MANIFEST-1`, BACKLOG, fork work). It refuses when the data leg has nothing to do while two or more delete manifests exist; a copy-on-write table has no delete manifests. `spec_id` refuses (`MANIFEST-2`, DECLARED). Above the target size, the added-manifest count differs from Spark (`MANIFEST-3`, BACKLOG). | PROVEN |
| M4 | **`expire_snapshots`.** | `crates/repark-spark/src/tests/v3e4.rs`. The live v3 legs (runs 33635288918, 33699342417: 14 snapshots → 1). MW-10 run 33333274383: the S3 Tables delete permission allows expiry. | Expiry removes rollback targets. The inventory's rollback plan requires `retain_last` to cover the cutover week (inventory §5). | PROVEN |
| M5 | **`remove_orphan_files`.** | `crates/repark-spark/src/tests/call.rs` (the ORPHAN pins); `python/repark/tests/test_maintenance_call.py`; `crates/repark-spark/src/tests/call_orphan.rs` for S3 Tables. The S5 maintenance program replayed on Glue and S3 Tables on 2026-09-04. | `older_than` is required (`ORPHAN-1`, DECLARED); the inventory's script passes it. `dry_run` defaults to true (`ORPHAN-2`, DECLARED): the same call Spark runs lists the orphans and deletes nothing, unless the script adds `dry_run => false`. A 24-hour floor applies. On S3 Tables the call refuses (`ORPHAN-S3TABLES-1`). | PROVEN |
| M6 | **`run_maintenance`, `plan_partitioning`, `apply_partitioning`.** | `crates/repark-spark/src/tests/run_maintenance.rs`, `apply_partitioning.rs`; `python/repark/tests/test_maintenance_policy_1.py`. AP-1-CLOSE-1 (`d85883fd`) pins `projected_files_at_target` as an upper bound. | Memory-catalog evidence only, with no live AWS cell. Both are dry runs by default. The admitted weekly script does not use them. | PROVEN |

### Concurrency and commit safety

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| K1 | **Fork contract: conflict validation, optimistic retry, commit-outcome reconciliation.** | Fork `docs/ENGINE_CONTRACT.md` §5, NORMATIVE: every isolation recipe is verified against Java 1.10.0 with a cross-engine conflict scenario. §8: retries only on retryable errors, never on `CommitStateUnknown`; an unknown outcome of a snapshot-producing commit is reconciled in the library by searching the reloaded snapshot history. GAP_MATRIX R110 🟡 and R157 🟡. | The fork itself keeps both rows partial. Credentialed Glue and S3 Tables conformance, and one accepted-then-lost append per catalog, are still pending (R110, R157, PR-7 re-audit of 2026-09-02). Metadata-only commits are not reconciled (§8). A failed validation is non-retryable by design. RePark does not re-prove these recipes on a live catalog. | DECLARED |
| K2 | **RePark commit policy.** Isolation defaults and conflict handling in the write adapter. | MERGE and predicate-DML isolation and conflict batteries on `MemoryCatalog` (`crates/repark-iceberg/src/write/merge/tests/occ.rs`, `occ_conflict.rs`, `write/predicate_dml/tests/`). The DML-4 and DML-5 pins. `parse_overwrite_isolation` (`crates/repark-iceberg/src/write/overwrite.rs`). The BL-5 cleanup pins. `NamespaceScopedCatalog` forwards all 14 required and 13 of 16 defaulted `Catalog` methods at the pin; the count is this review's, and the pins are in `crates/repark-iceberg/src/catalog/tests/namespace_scoped.rs`. | MERGE, DELETE and UPDATE default to serializable; INSERT OVERWRITE defaults to snapshot. All of this evidence is on the memory catalog. | PROVEN |
| K3 | **An ambiguous commit that an operator can act on.** A typed `CommitStateUnknownException` now names the unknown outcome, but it has never been drilled live (corrected 2026-09-17 from the 2026-09-16 rating). | `repark.errors.CommitStateUnknownException` carries the `operation_id` (ICE-COMMIT-UNKNOWN-1, `acbb6a8e`). Pinned offline: Rust `commit_unknown` 4 + 3 passed, `test_errors.py` passed. No live drill on a real catalog is recorded. | A caller can tell an unknown outcome from a conflict or a validation refusal by type. Written files are left on disk with no retry. After an unknown outcome RePark skips file cleanup (`BL-5`). | PROVEN offline; no live drill |
| K4 | **Concurrent writers.** On an adopted Hadoop `vN` table a stale-base RePark commit overwrites `v(N+1).metadata.json`, silently losing the other writer's commit (corrected 2026-09-17 from the 2026-09-16 rating). | The single-writer rule (inventory §2). Release-roadmap row 1.14 plans multi-writer. `DML-5`. Fork `ENGINE_CONTRACT.md` §8 on S3 Tables service commits. SILVER-S0 C-003 on the overwrite rebase. Rating V2-20c (probe `p_failures` `conc`/`conc2`, cells #46/#47): two RePark catalogs, then Spark plus a stale RePark, on one Hadoop table — the other commit is silently lost, and Spark's next read fails `RuntimeIOException`. Glue and S3 Tables use conditional updates and were not re-measured. | No multi-writer acceptance exists. Whether AWS-managed table optimizers write to the production Glue tables was not assessed (Q-ICE-6). | Measured failing on the adopted-Hadoop path only |

### Memory and scale

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| X1 | **Spill or loud refusal under a memory pool.** | NEVEROOM-1: 27 cells, 24 stable at 2×/4×/8× of 1 GB; `hash_join` 4× killed; `hash_aggregate` 2× and `window_unbounded` 2× unstable ([matrix](../perf/spill-coverage-matrix-2026-09-11.md); repark 1.1.1 release module). H3-SPILL-1: 180 cells with 0 aborts and 0 wrong answers, including `iceberg_scan_dv` and `merge_staging` ([baseline](../perf/spill-matrix-baseline.md)). `H3-SPILL-NLJ-1` and `H3-SPILL-COLLECT-1` FIXED. | The 27-cell matrix uses synthetic `range()` input, not Iceberg tables. At 1 GB the usual outcome is a refusal, not a spill. The CI tier `make py-test-spill-matrix` runs in no workflow. | PROVEN |
| X2 | **A process-memory ceiling for Iceberg scan, MERGE and CTAS at production shape.** | The Iceberg scan and the facade boundary are not pool-accounted, and `toPandas()` aborted under a tight address-space limit ([baseline](../perf/spill-matrix-baseline.md)). MERGE joins source to target, and the matrix records no spill path for DataFusion's hash join. SCALE-v3 peaked at 4,792 MiB RSS at 1e7 rows × 50 MERGEs (north star row 20). | Nothing is measured at the production silver or gold width, row count or skew. A configured pool is not a process limit. | NOT ESTABLISHED |
| S1 | **Scale.** | SCALE-v3: 1e7 rows × 50 MERGEs on v2 and v3, then the full maintenance sequence, 2026-09-02 ([ledger](../../task/ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md) §3). MW-7 on v2. TORTURE-1 at 1M rows. PERF-ICE-CATALOG-IO-3 at 2,000–8,000 small tables. Write baselines at 1e6 rows. | One box, the memory catalog, no AWS latency. Write-side timings are cross-run and uncontrolled (north star row 20). Nothing is measured at production width or skew. | PROVEN |

### Interoperability and dbt

| Id | Capability | Evidence | Qualification | Status |
|---|---|---|---|---|
| I1 | **Spark 4.1.2 reads a RePark-written table.** Measured equal on local catalogs; still unmeasured on Glue, S3 Tables, Athena and Trino (corrected 2026-09-17 from the 2026-09-16 rating). | The V3-0 audit of 2026-08-21: Spark read RePark's v3 append (858 rows, `_row_id = 1000`) ([format-v3-track.md](../design/format-v3-track.md) §2). The fork's Java interop scenarios run both ways at fork revisions (GAP_MATRIX R94, R95, R98, R114, R158). Rating §2: Spark reads RePark-written tables equal on local catalogs for v2 CoW/MoR and v3 CoW/MoR including lineage, evolved tables, multi-transform partitions and RTAS; PyIceberg reads them too (cells #1, #2, #8, #10–#12, #30, #31, #35, #36, #40–#44). | Not measured on Glue, S3 Tables, Athena or Trino. The parity-live cutover cells run the same SQL on each engine separately and do not cross-read. Inventory check 4 is unmet on AWS. | PROVEN on local catalogs |
| I2 | **Athena or Trino reads a RePark-written table.** | None found. | Athena does not support v3 (north star §5). No v2 measurement exists. | NOT ESTABLISHED |
| I3 | **RePark reads and changes Spark-written v3 tables.** | Spark-written v3 fixtures with DV DELETE, time travel and refs (`crates/repark-spark/src/tests/v3e3.rs`, `v3e4.rs`); the nightly live oracle; `register_table` adoption (`crates/repark-spark/src/tests/call_register.rs`); the Spark-generated TORTURE-1 `v3_dv` fixture. | These are small fixtures. | PROVEN |
| I4 | **RePark MERGE and maintenance on a Spark-created v2 copy-on-write table with production properties.** Now pinned live and extended past the admitted shape (corrected 2026-09-17 from the 2026-09-16 rating). | `test_ice_spark_table_1.py::test_live_spark_created_table_roundtrip` passed (ICE-SPARK-TABLE-1, `293fbcf6`). Rating probes extended it to MoR, v3 DVs, equality deletes and evolved specs, all equal to Spark (cells #5, #6, #25–#28, #52, #53). | The C2 shadow plan now has Spark create the shadow tables (owner ruling Q-ICE-3). A Spark-created table with a transform sort order refuses writes (`WRITE-ORDER-TRANSFORM-1`). | PROVEN |
| D1 | **dbt-repark on the memory catalog.** | `python/dbt-repark/tests/test_gold_models.py`: `dbt run` builds both gold models with the S6 rows; `dbt test` passes ten blocks; a second `dbt run` replaces the tables in place (`test_dbt_run_is_idempotent`); `--full-refresh` works. Also 30 statement-surface cases. Runs per PR in `wheels.yml` `smoke`. | Only `materialized='table'` with `file_format='iceberg'`. `view`, `incremental` and `snapshot` refuse (`DBT-VIEW-1`, `DBT-TEMPVIEW-1`), as do `persist_docs`, `location_root`, `options` and `clustered_by` (registry §2.5). The memory catalog implements replace, so the rebuild pin does not cover Glue (row C2). | PROVEN |
| D2 | **dbt-repark gold on Glue.** | Inventory §6 C6 and §7 ruling 2 (commit `897151dd`, #372) record that `dbt run` built both models over Glue and `dbt test` passed ten blocks, dated 2026-09-05. The module is `python/dbt-repark/tests/test_aws_acceptance_gold.py`. | No run output, run id, wheel or revision is recorded. The DBT-1 ledger (C-005) records the Glue leg as written and skipped. The recording commit is dated 2026-09-04 −04:00; the module then used a constant stem and one `dbt run`, so that build can only have exercised a first build. The module now uses per-run stems, two `dbt run` passes and `dbt test` (ICE-GOLD-TWICE-1) and joined `aws-acceptance.yml`. Its first run, aws-acceptance 34901483202 on `0b33f5b7` (2026-09-14), passed: two `dbt run` passes and `dbt test` (10 blocks) on Glue in 36.19 s. | PROVEN (run 34901483202) |

**Row counts:** 36 rows — 26 PROVEN, 1 DECLARED, 9 NOT ESTABLISHED (C2 and D2 proven by aws-acceptance
run 34901483202 on 2026-09-14).

## 4. What changed since the 2026-09-06 assessment

The baseline moved from `e100f72d` (v1.1.1, fork `85db42f2`) to `c9b03c67` (v1.4.0, fork
`3ebf7d36`). Three minors shipped in between: v1.2.0 (`e46ef7a9`, #506, the torture suite), v1.3.0
(`0dde3faa`, #508, the Never-OOM matrix) and v1.4.0 (`d3a20d53`, #533, the maintenance minor).

| Commit | Change | Effect here |
|---|---|---|
| `864e3483` (#510, 2026-09-11) | RP-16, fork `090bc821`. Fork #276 (F-WRITE-COMPRESS-1): `INSERT INTO` data files carry `write.parquet.compression-codec`; before this every INSERT file was uncompressed, at the prior baseline too. Fork #274 fixes `PERF-CATALOG-CACHE-WEIGHT-1`. Fork #273 (F-INSERT-DIST-1) was taken without a RePark re-measure. | Row W1. Closes a storage defect the prior assessment did not record. Leaves the INSERT layout unmeasured. |
| `f6d5466f` (#523, 2026-09-12) | RP-17, fork `41e25ba2`. Fork #278 (F-WRITE-COMPRESS-2): the compaction, copy-on-write/merge-on-read rewrite and position-delete writers carry the codec; compaction had written uncompressed files. | Row M1 |
| `dae79c40` (#531, 2026-09-12) | RP-18, fork `9e3522e3`. Fork #280: the rewrite drops dead dictionary pages, and an unset compression level means zstd level 3, as in Java. | Row M1 |
| `1faa8dcc` (#541, 2026-09-12) | RP-19, fork `3ebf7d36`. Fork #281 (F-S3ROOT-1): a bare-bucket location resolves to the bucket root. | Rows C3, M5 |
| `c81ba043` (#409, 2026-09-06) | WRITE-ORDER-DIST-1: `ALTER TABLE … WRITE ORDERED BY / DISTRIBUTED BY / UNORDERED`; the write path honours `write.distribution-mode` and the default sort order. `V3-COV-5` FIXED. | Row W6. Opens `WRITE-RANGE-1` and `WRITE-ORDER-TRANSFORM-1`. |
| `7db61bfa` (#428), `e4dafea7` (#482) | SQL-DESCRIBE-1 and REVIEW-FIX-5: `DESCRIBE [TABLE] [EXTENDED\|FORMATTED]` returns Spark's rows on Iceberg tables. | Row R4 |
| `62373134` (#465); `04a2e430` (#462); `838532d1` (#472); `ecdcc58b` (#498); `7c7fa4f1` (#512); `01d11e96` (#526); `d85883fd` (#532) | MAINT-POLICY-1 (`CALL run_maintenance`) and the adaptive-partitioning chain AP-0, AP-1, AP-2, AP-3 and AP-1-CLOSE-1 (`plan_partitioning`, `apply_partitioning`). All are dry runs by default. | Row M6 |
| `c768e0f4` (#539, 2026-09-12) | ORPHAN-S3TABLES-1: `remove_orphan_files` refuses before any IO on S3 Tables, and `run_maintenance` skips that step with a reason. | Rows C3, M5 |
| `149147c1` (#467), `862a0f1a` (#473), `3f4a8adc` (#497), `65116847` (#499), `75406e42` (#500) | TORTURE-1, including the `v3_dv` family at 1M rows. | Rows R1, S1 |
| `373c8a63` (#475), `f7a7b61f` (#503), `6e13b3de` (#507) | NEVEROOM-1: the 27-cell spill-coverage matrix and its CI golden. | Row X1 |
| `6fa17a47` (#505, 2026-09-11) | NIGHTLY-LIVE-1: parity-live had been red every night since 2026-09-05; the shared PySpark context now survives the suite. | Section 2 evidence tiers |
| `d5c36c98` (#543, 2026-09-12) | SILVER-S0, a reading unit. Fork probes, run on a probe branch, measured the commit rebase, the run-marker lifetime and the outcome taxonomy per catalog. | Rows W3, K3, K4 |
| `abed32c9` (#565), `30d57ef9` (#569) | EAGER-OWN-1 and EAGER-BUDGET-1: ownership and a byte budget for the facade's eager cache. | Facade memory; not Iceberg-specific |
| `07a96902` (#460), `0eddabfe` (#466), `0b12deb5` (#469), `6027b93d` (#470), `e756c8e0` (#501), `a10062b8` (#509) | Ballista milestones: Iceberg scans through distributed executors behind the `cluster` feature. | Read-only and not a production path; not assessed |
| `774779e1` (#440) through `5d188bf9` (#455); `8b2673fd` (#479); `b8c076b9` (#525) | CFG-1 `repark.toml` catalogs and profiles, with secret redaction; PROFILES-1 read and write profiles. | Configuration surface only |
| `2e256438` (#548), `7528f5d4` (#552), `7125462e` (#554) | PLATFORM-1: a five-leg abi3 wheel matrix on tags and nightly. | Deployment; no Iceberg acceptance |
| `16b08923` (#537), `c4cb3ff5` (#538) | REST catalog intake and its roadmap placement (documents only). | Row C4 |

**Closed or narrowed since the prior assessment.**

1. Compaction wrote uncompressed output and then dead dictionary pages. RP-17 and RP-18 close both
   (row M1).
2. `INSERT INTO` wrote uncompressed data files at the prior baseline. RP-16 closes it (row W1); the
   prior assessment did not know about it.
3. Table write-order DDL was missing (`V3-COV-5`). WRITE-ORDER-DIST-1 closes it (row W6).
4. The parity-live nightly was red from 2026-09-05 to 2026-09-11, a span that includes the prior
   assessment's date. #505 closes it.
5. S3 Tables orphan removal failed with an opaque error. It is now a loud, documented refusal
   (rows C3, M5).
6. `PERF-CATALOG-CACHE-WEIGHT-1` is FIXED at RP-16.

**Unchanged since the prior assessment.** C2–C5 still have no completion record, and the newest
live AWS evidence is still dated 2026-09-04 or earlier. The memory limits (rows X1, X2) are
unchanged, and so are `RDF-1`'s residue, `RDF-SORT-1`, `MANIFEST-1`, `MANIFEST-3`, the partial
`G3-E8` fix, `V3-COV-4`, `V3-COV-6`, `V3-COV-7` and the single-writer rule.

**Newly opened or newly visible.**

1. On Glue, `CREATE OR REPLACE TABLE … AS` against an existing table has no publish path at the pin
   (row C2). This review did not check whether that also held at earlier fork pins.
2. The default INSERT OVERWRITE isolation, `snapshot`, silently replaces a concurrent append. The
   SILVER-S0 probes measured this on 2026-09-12 (rows W3, K4).
3. `F-INSERT-DIST-1` was consumed at RP-16 without a RePark re-measure (row W1).
4. `WRITE-RANGE-1` and `WRITE-ORDER-TRANSFORM-1` are open BACKLOG rows (rows W6, I4).
5. `remove_orphan_files` cannot run on S3 Tables, by design (`ORPHAN-S3TABLES-1`).
6. Four fork pins, RP-16 to RP-19, have no live AWS run after them (section 6).

## 5. Boundaries that matter for production

Each boundary names its registry row. "No registry row" means the boundary is recorded only where
this report cites it.

**Commit safety and concurrency.**

- **One writer per table.** Inventory §2 sets the rule, and no multi-writer acceptance exists
  (row K4). A serializable MERGE aborts on a concurrent append into any partition (`DML-5`). A
  full-table INSERT OVERWRITE defaults to `snapshot` isolation and replaces a concurrent append.
  No registry row covers that default; it is the fork's engine-defined default (fork
  `ENGINE_CONTRACT.md` §1), and the probes are SILVER-S0 C-003 and C-007.
- **An ambiguous commit outcome.** The fork reconciles in the library for snapshot-producing
  commits, but its conformance on real catalogs is pending (GAP_MATRIX R157 🟡). After an unknown
  outcome RePark skips file cleanup (`BL-5`). No registry row covers the undistinguished exception
  class or the unreturned operation id (row K3).
- **No cross-table atomicity.** Each statement commits one snapshot on one table, so silver and
  gold never commit together. Rollback is per table (inventory §5). No registry row; this is an
  Iceberg property.
- **Rollback needs retained snapshots and files.** `expire_snapshots` retention must cover the
  rollback window (inventory §5). `ORPHAN-1` and `ORPHAN-2` guard the destructive step.
- **S3 Tables maintenance is a concurrent writer.** The service commits its own compaction and
  expiry (fork `ENGINE_CONTRACT.md` §8). See also `ORPHAN-S3TABLES-1` and `S3T-1`.

**Create and replace.**

- On Glue, replacing an existing table has no publish path at the pin (row C2). No registry row.
- CTAS does not stamp Spark's codec property (`V3-COV-7`). This is metadata only.
- `LOCATION`, `OPTIONS`, `CLUSTERED BY` and `COMMENT` refuse on CTAS (`DBT-CTASCLAUSE-1`,
  `DBT-RELCOMMENT-1`).

**DML shapes.**

- Subquery UPDATE forms refuse; they never return wrong rows (`G3-E8`, `G3-E8-NULL`).
  Transform-field and mixed PARTITION overwrites refuse (`DML-1`). Some MERGE forms refuse
  (`DML-3`).
- Storage shape differs from Spark: `V3-COV-4`, `MOR-2` (fork DELETE and UPDATE write
  partition-granularity deletes), `CUTOVER-MERGE-FILES-1` (merge-on-read only).
- A failed stream write orphans the files it rolled (`WRITE-ABORT-INSERT-1`).
- File layout and v3 `_row_id` are not reproducible between runs (`WRITE-GROUPING-CTAS-1`,
  `WRITE-ORDER-INSERT-1`).
- `range` distribution is hash plus a per-writer sort (`WRITE-RANGE-1`). Transform sort orders
  refuse (`WRITE-ORDER-TRANSFORM-1`).

**Maintenance.**

- `RDF-1` residue (multi-referent position deletes), `RDF-SORT-1`, `MANIFEST-1`, `MANIFEST-2`,
  `MANIFEST-3`, `S3T-1`.
- `ORPHAN-1` and `ORPHAN-2`. With the dry-run default, the Spark script's orphan call lists and
  deletes nothing unless it adds `dry_run => false`.
- `ORPHAN-S3TABLES-1`: on S3 Tables the service removes unreferenced files, not RePark.

**Format and read exclusions.**

- `V3-GEO-1`, `V3-VARIANT-SHRED-1`, `V3-UPGRADE-V4-1`, `V3-FILEORDER-1`, `V3-ROWID-2`, `MT-1`,
  `REF-2`, `REF-3`, `V3-COV-6`.
- `ENC-1` concerns Iceberg table encryption keys. It says nothing about S3 object encryption, which
  needs its own configuration and evidence.
- Incremental and changelog reads are out of scope by the north star's §6 ruling. No registry row.

**Memory.**

- Several paths take no pool reservation: the Iceberg scan, windows, unnest and the facade boundary
  ([spill-matrix-baseline.md](../perf/spill-matrix-baseline.md)). No registry row covers the
  Iceberg scan.
- The matrix's three blocked cells wait on upstream DataFusion issues #24768 and #22758
  (NEVEROOM-1).
- `H3-SPILL-NLJ-1` and `H3-SPILL-COLLECT-1` are FIXED.

**Readers.**

- Athena does not read v3 (north star §5). Athena on v2 and Spark on RePark-written v2
  copy-on-write snapshots are unmeasured (rows I1, I2). No registry row.

**Performance only; no effect on correctness.**

- `PERF-CATALOG-AWS-CACHE-1`, `PERF-CATALOG-COMMIT-CACHE-1` and `PERF-CATALOG-CACHE-BOUND-1`, all
  BACKLOG.

## 6. Operational acceptance state

The inventory's canary plan (§6), as of `c9b03c67`:

| Stage | Inventory gate | State today | Evidence | Waits on |
|---|---|---|---|---|
| C0 | Fresh-venv wheel, Glue and S3 Tables canaries | Recorded for 1.0.1 only | Inventory §6 says "done for 1.0.1". No canary record exists for 1.1.0, 1.1.1, 1.2.0, 1.3.0 or 1.4.0. The release workflow's per-leg smoke is an import and one collect, with no catalog. | A named candidate wheel or revision (Q-ICE-4), then an aws-acceptance dispatch on it with the run id recorded |
| C1 | Copy-on-write MERGE cell on all three catalogs, rows equal | Done on 2026-09-04 at `c70a306` (1.0.1, fork `189a73ed`) | SQL-HARDEN-2 ledger §5: 2 passed in 268 s | A re-run on the candidate; four fork pins and three minors have landed since |
| C2 | Shadow namespace `<ns>_silver_repark`, one `ds`, checks 1–4 | Not started; no completion record | Inventory §7 ruling 4; [briefs/next-sequence.md](../../briefs/next-sequence.md) row 2 | `SHADOW-1` in the pipeline, which has no record here. The other precondition, `CUTOVER-SCHEMA-1` on `main`, is met (`63c68c53`, #375, 2026-09-04). This repository has no harness for check 4, the Spark and Athena reads (rows I1, I2). |
| C3 | Seven consecutive shadow days with zero divergences | Not started | None | C2 |
| C4 | Silver writer switched to RePark, Spark task paused, rollback rehearsed once, checks 1–5 | Not started | None | C3; a rollback rehearsal record; check 4; and, per this review, a RePark write onto Spark-created tables before the switch (row I4) |
| C5 | Maintenance on RePark, check 6 | Not started | None | C4. The migrated script must pass `dry_run => false` to delete orphans (`ORPHAN-2`). |
| C6 | Gold on RePark, check 5 and the ten dbt tests | A Glue first build is recorded as a dated statement (2026-09-05) with no run artifact; the switch has not started | Inventory §6 row C6; commit `897151dd` (#372) | C4 holding (inventory), plus a Glue replace publish path and a second `dbt run` on Glue (row C2, gap G-1) — the path landed at `edc38c6a` (RP-20) and the twice leg is in `aws-acceptance.yml` (ICE-GOLD-TWICE-1); first run pending the post-merge dispatch |

**SHADOW-1.** Inventory §7 records it as a pipeline-side unit launched on 2026-09-04. In this
repository it appears only in inventory §7 ruling 4 and the note under it, in
[docs/cutover/map.md](map.md), and in [briefs/next-sequence.md](../../briefs/next-sequence.md)
row 2. No ledger, run record, diff output or completion record exists here. This review therefore
does not establish its state. That does not mean it failed, or that it never ran outside this
repository.

**The packet still owed before C4** (carried from the prior assessment and updated):

1. The exact wheel, RePark commit, fork pin, catalog, namespace, table properties and session
   configuration.
2. A recorded aws-acceptance run on that revision, with the S1–S9 cutover legs and the dbt Glue
   gold leg run twice.
3. Daily row-multiset and schema diffs for all six silver entities, including a rerun of the same
   input (checks 1–3).
4. Spark 4.1.2 and Athena reads of the RePark-written snapshots (check 4).
5. A rollback rehearsal with retained snapshots and files, and no second writer.
6. Maintenance results on the same table shapes (check 6).
7. Peak process memory, wall time and file counts per entity, on a host like production.

## 7. Gaps ranked by production impact

"Admitted workloads" means silver CTAS and MERGE on Glue v2 copy-on-write tables, gold dbt, and
the weekly maintenance.

| Rank | Gap | Blocks the admitted cutover? |
|---|---|---|
| G-1 | Glue replace of an existing table has no publish path (row C2) — **CLOSED 2026-09-14**: fixed at pin `edc38c6a` (F-GLUE-REPLACE-1 + RP-20) and live-proven in aws-acceptance run 34901483202 (the replace-twice legs and gold twice, ICE-GOLD-TWICE-1) | Closed |
| G-2 | No live acceptance at a current candidate revision | Yes: the C4 switch; recommended before C2 |
| G-3 | The first RePark write onto Spark-created production tables happens at the switch (row I4) | Yes: C4, unless the shadow covers it (Q-ICE-3) |
| G-4 | No cross-reader acceptance: Spark and Athena reading RePark writes (rows I1, I2) | Yes: check 4, which gates C2–C4 |
| G-5 | No process-memory measurement at production shape (row X2) | Not a stated gate; needed to size C4 |
| G-6 | An ambiguous commit is not distinguishable by the caller (row K3) | No, if retries are limited to idempotent statements (Q-ICE-7) |
| G-7 | AWS-managed optimizers may be concurrent writers (row K4) | Only if optimizers are enabled (Q-ICE-6) |
| G-8 | Migrated maintenance semantics: orphan dry-run default, no copy-on-write maintenance cell | C5 storage outcome, not rows |
| G-9 | Plain `INSERT INTO` layout not re-measured; failed stream writes orphan files (row W1) | No: broader adoption |
| G-10 | Merge-on-read maintenance residuals and DML refusals (rows M1, M3, W4) | No: broader adoption |
| G-11 | Surfaces not offered: REST and Hive catalogs, incremental reads, write-audit-publish (rows C4, R5, R3) | No: broader adoption |

**G-1 — Glue replace publish.**

**CLOSED 2026-09-14 (F-GLUE-REPLACE-1 + RP-20), live-proven:** fork #282 landed
`GlueCatalog::publish_replace_table` as a version-id-checked `UpdateTable` through the Glue
commit transport with the staged metadata read-validated, and the repin to `edc38c6a` consumed
it. ICE-GOLD-TWICE-1 added the `CREATE OR REPLACE … AS` twice legs and the two-`dbt run` gold
module to `aws-acceptance.yml`; the first run, aws-acceptance 34901483202 on `0b33f5b7`, passed every leg. The scenario below is
the pre-fix record, kept.

- *Failing scenario.* The first scheduled gold run after C6 finds `gold_fct` and `gold_agg`
  already present. dbt-repark marks every listed relation `is_iceberg=True`
  (`python/dbt-repark/src/dbt/adapters/repark/impl.py:70`). dbt-spark's `table` materialization
  therefore skips the drop and emits `create or replace table … using iceberg … as`. RePark
  streams the SELECT into data files, and then the publish returns `FeatureUnsupported`. The model
  fails, and the staged files stay on storage unreferenced. `writeTo().createOrReplace()` fails the
  same way.
- *Evidence.* The source chain in row C2. The Glue gold test uses the constant stem
  `ACCEPTANCE_TABLE_PREFIX + "dbt1"` and a plain `CREATE TABLE` seed
  (`python/repark/tests/_sql_harden_cutover_run.py`, `_seed_gold_sql`), so only a first build can
  have passed. The local replace pin (`test_dbt_run_is_idempotent`) runs on the memory catalog,
  which implements replace.
- *Smallest closing unit.* In the fork, implement `publish_replace_table` for the Glue catalog as a
  version-checked `UpdateTable` of the metadata location, as the S3 Tables catalog already does
  (fork `crates/catalog/s3tables/src/catalog.rs:752`), with an offline commit-transport pin. That
  wiring must also address the two replace-publish residues the fork names on GAP_MATRIX R158: the
  staged metadata is not read-validated before the pointer swap, and metadata-file versioning
  restarts. In RePark, repin; give `test_aws_acceptance_gold.py` unique stems and two `dbt run`
  passes; and add a "`CREATE OR REPLACE TABLE … AS` twice" leg to `test_aws_acceptance.py` so the
  nightly carries it. An interim route needs a ruling (Q-ICE-1).
- *Blocks.* C6. Silver too, if the dimension job rebuilds with a replace (Q-ICE-2).

**G-2 — No live acceptance at the candidate revision.**

- *Failing scenario.* Something from RP-16 to RP-19 behaves differently on Glue than on the memory
  catalog, and production is the first place it is seen. Candidates: the zstd-3 default, the
  codec on the rewrite writers, the consumed INSERT distribution change, the bare-bucket parser.
  The same applies to anything from 1.1.1 to 1.4.0.
- *Evidence.* The run list in section 2: the newest live run is dated 2026-09-04, at `c70a306`.
  aws-acceptance needs environment approval, and the Glue gold leg is in no workflow.
  **Corrected in the addendum (§10):** the nightly aws-acceptance schedule has run green on `main`
  every night through 2026-09-14; the gold leg's absence from that workflow is the part that stands.
  *(Updated 2026-09-14: the gold module joined `aws-acceptance.yml` under ICE-GOLD-TWICE-1; its
  first run is the post-merge dispatch.)*
- *Smallest closing unit.* Dispatch `aws-acceptance.yml` on the candidate revision and record the
  run id and per-leg counts in the inventory. Add the dbt Glue module to that workflow; it is a
  `.github` change, which the owner approves.
- *Blocks.* The C4 switch. Recommended before C2 starts.

**G-3 — First write onto Spark-created tables at the switch.**

- *Failing scenario.* The shadow tables are created by RePark, so they never carry what Spark
  leaves on a production table. At the switch, RePark's MERGE meets that metadata for the first
  time: a transform sort order (which refuses, `WRITE-ORDER-TRANSFORM-1`), a
  `write.distribution-mode`, Spark's stamped codec, older partition specs, Spark-written manifests
  and history. The failure lands after the Spark task is paused.
- *Evidence.* Row I4; the C2 wording of inventory §6; no pinned cell found.
- *Smallest closing unit.* Either have SHADOW-1 create the shadow tables with Spark from the
  production DDL and properties (Q-ICE-3), or add one scratch leg that does the same and then runs
  RePark MERGE twice and the weekly CALLs on it.
- *Blocks.* C4.

**G-4 — Cross-reader acceptance.**

- *Failing scenario.* A downstream Spark job or an Athena query reads a RePark-written silver
  snapshot wrongly, or refuses it. Nothing in this repository would catch that.
- *Evidence.* Rows I1 and I2; inventory §4 check 4.
- *Smallest closing unit.* In the C2 diff, read each shadow table with Spark 4.1.2 (`EXCEPT ALL`
  against the RePark answer) and with Athena (`count(*)` plus a checksum), and record the results
  daily. The Athena permission is IAM, which only the owner changes (Q-ICE-5).
- *Blocks.* Check 4, and so C2–C4 as the inventory writes them.

**G-5 — Memory at production shape.**

- *Failing scenario.* The largest silver entity's MERGE, or the gold join, outgrows the host. The
  MERGE hash-joins source to target and carries scan batches the pool does not account. The
  process is killed mid-run without a loud refusal.
- *Evidence.* Rows X1 and X2.
- *Smallest closing unit.* Record peak RSS and the pool setting for each silver entity and gold
  model during C2–C3, on a host sized like production. Set `datafusion.runtime.memory_limit` and
  the container limit from those numbers.
- *Blocks.* No stated gate. Sizing C4 safely depends on it.

**G-6 — Ambiguous commit handling.**

- *Failing scenario.* A Glue `UpdateTable` response is lost, and the fork cannot read the catalog
  within its reconciliation budget. The task sees a base `PySparkException`. A retried MERGE or
  CTAS IF NOT EXISTS is row-safe. A retried append duplicates rows. A retried replace or
  overwrite leaves the same rows plus an extra snapshot.
- *Evidence.* Row K3; fork GAP_MATRIX R157 🟡.
- *Smallest closing unit.* Map `CommitStateUnknown` to its own exception subclass, or to a stable
  error-class attribute, in `crates/repark-core/src/error_map.rs` and the binding. Pin it with a
  test catalog that returns that kind. Optionally return the commit's `engine.operation-id` to the
  caller.
- *Blocks.* Broader adoption. For the admitted workloads, only the retry policy (Q-ICE-7).

**G-7 — AWS-managed concurrent writers.**

- *Failing scenario.* Glue table optimizers compact or expire the silver tables. A concurrent
  compaction commit makes a serializable copy-on-write MERGE fail validation, which is
  non-retryable. Optimizer retention removes snapshots that the rollback plan relies on.
- *Evidence.* The single-writer rule (inventory §2); fork `ENGINE_CONTRACT.md` §8 on service
  commits; no Glue optimizer assessment.
- *Smallest closing unit.* The owner states the optimizer configuration per table (Q-ICE-6). If
  optimizers are on, add one leg that runs a concurrent `rewrite_data_files` during a MERGE.
- *Blocks.* C4, only if optimizers are enabled.

**G-8 — Migrated maintenance semantics.**

- *Failing scenario.* The weekly DAG calls `remove_orphan_files(table, older_than)` exactly as it
  did on Spark. RePark lists the orphans, deletes nothing and reports success, so storage grows
  week after week.
- *Evidence.* `ORPHAN-2`. The maintenance row of inventory §2 mentions only `ORPHAN-1`. The S5
  maintenance program runs with merge-on-read properties, and no copy-on-write maintenance program
  exists (`python/repark/tests/_sql_harden_cutover_programs.py`).
- *Smallest closing unit.* Add `dry_run => false` to the migrated script, and check the orphan
  count at C5. Add an S8/S9-style copy-on-write maintenance program to the cutover matrix.
- *Blocks.* The C5 storage outcome, not the rows.

**G-9 — The INSERT path.**

- *Failing scenario.* An append job writes many more files per partition than Spark, or a failed
  write leaves orphans (`WRITE-ABORT-INSERT-1`, 70 files and 103 MB measured).
- *Smallest closing unit.* Re-measure the WRITE-DISTRIBUTION-2 INSERT cell at the current pin, and
  carry the CTAS attempt sweep over to the stream write path.
- *Blocks.* Broader adoption only.

**G-10 — Merge-on-read residuals and DML refusals.**

- `RDF-1` residue, `MANIFEST-1`, `MANIFEST-3`, `RDF-SORT-1`, `G3-E8` and `V3-COV-4`. Each row
  names its fork or engine unit.
- None of them affects a v2 copy-on-write table's rows.
- *Blocks.* Broader adoption only.

**G-11 — Surfaces not offered.**

- REST and Hive catalogs (row C4), incremental reads (row R5), write-audit-publish (`REF-3`), and
  v3 behind Athena.
- *Blocks.* Broader adoption only.

## 8. Owner questions

- **Q-ICE-1 — Gold rebuild on Glue.** The fork's Glue catalog cannot yet publish a replace (G-1).
  Until it can, which route applies?
  - (a) Block C6 on the fork change and a repin. This is the recommendation.
  - (b) An interim dbt-repark change that rebuilds an existing gold table with
    `INSERT OVERWRITE`. History and rollback survive, but a schema change is not applied to the
    table.
  - (c) dbt-spark's drop-then-create path. The table is missing between the drop and the create,
    and snapshot history and rollback are lost.
- **Q-ICE-2 — Dimension rebuilds.** How does `silver_dim_jobs.py` do its "full rebuild" (inventory
  §1)? With `CREATE OR REPLACE TABLE … AS`, `writeTo().createOrReplace()`, `DROP` followed by
  CTAS, or `INSERT OVERWRITE`? The first two make G-1 block silver as well.
- **Q-ICE-3 — Shadow table origin.** Should SHADOW-1 create its shadow tables with Spark, from the
  production DDL and properties? Then RePark's MERGE and maintenance meet Spark-created metadata
  before C4. The alternative is to accept that the switch is the first such write (G-3).
- **Q-ICE-4 — Candidate revision.** Which revision is the cutover candidate: the v1.4.0 tag, or a
  later `main`? Does the owner approve an aws-acceptance dispatch on it, and adding the dbt Glue
  gold module to that workflow (G-2)?
- **Q-ICE-5 — Production readers.** Which engines read silver and gold in production: Spark only,
  or also Athena or Trino? If Athena, will the owner grant the acceptance role Athena query
  permission (G-4)?
- **Q-ICE-6 — Glue optimizers.** Are AWS Glue table optimizers (compaction, snapshot retention,
  orphan file deletion) enabled on the production silver or gold tables? If so, the single-writer
  rule does not hold as written (G-7).
- **Q-ICE-7 — Retries.** May the Airflow tasks retry RePark statements automatically? Until G-6
  closes, the recommendation is: retry MERGE and CTAS IF NOT EXISTS only, never an append, and
  alert on any error message that names `CommitStateUnknown`.

## 9. Source reconciliation notes

Each note says where the overview lags and where the detailed evidence lives. This report changes
none of those documents; each fact should be amended in its own home.

1. **Inventory §2, gold row.** It still says the dbt path is "NOT a 1.0.x deliverable" and that
   gold stays on Spark/Glue until one exists. Inventory §6 row C6 and §7 ruling 2 record that
   DBT-1 landed on 2026-09-04. The closing note, "C6 waits on `DBT-1`", is also stale.
2. **Inventory §3, blocker column.** S1 ("required-ness of columns") and S3 ("schema nullability")
   were both FIXED on 2026-09-04 (`CUTOVER-CTAS-REQ-1`, `CUTOVER-DEDUP-SCHEMA-1`). Check 3 in §4
   still asks whether they are blockers, although ruling 1 answered that. S4's "v3 next-row-id
   codec" misdescribes `V3-COV-7`, which concerns the CREATE-time codec property.
3. **Inventory §6, C6 "measured green on Glue"** (also in [map.md](map.md)). No run output, run id
   or wheel is recorded. The DBT-1 ledger (C-005) records the Glue leg as written and skipped. The
   record is dated 2026-09-05, but commit `897151dd` is dated 2026-09-04 19:10 −04:00. The module
   can only exercise a first build (G-1).
4. **Inventory §2, maintenance row.** It names `ORPHAN-1` but not `ORPHAN-2`, whose dry-run
   default changes what the migrated script does (G-8).
5. **Inventory §6, C0 "done for 1.0.1".** [STATUS.md](../../STATUS.md) records 1.4.0 as the
   release, and no later canary record exists.
6. **[STATUS.md](../../STATUS.md).**
   - The format-v3 "Next" line lists `V3-COV-5` as open. It was FIXED on 2026-09-06
     (WRITE-ORDER-DIST-1).
   - The H-2 entry still says "Next: those two" for `H3-SPILL-NLJ-1` and `H3-SPILL-COLLECT-1`.
     Both were FIXED on 2026-09-06 (H3-SPILL-RESIDUE-1).
   - The dbt workstream says "validate on the 1.0.1 wheel" and names only the sibling adapter. The
     in-repository `python/dbt-repark` (DBT-1) appears only in AGENTS.md's crate map.
7. **[README.md](../../README.md).** "v1.0.0 on PyPI (2026-09-03)". STATUS.md records v1.4.0
   (2026-09-12).
8. **[ARCHITECTURE.md](../../ARCHITECTURE.md), runtime flow 3.** "CTAS is create-or-replace — no
   drop-then-insert window." That holds for create on every catalog and for replace on the memory
   catalog and S3 Tables. On Glue, replace publish is unsupported at the pin (row C2).
   *(Resolved 2026-09-14: the repin to `edc38c6a` made the claim true on Glue — row C2 FIXED.)*
9. **[dbt-on-repark.md](../guide/dbt-on-repark.md).** It describes `materialized='table'` as
   rebuilding "in one Iceberg snapshot" and gives a `glue_catalog` rollback example. On Glue only
   the first build has evidence (rows C2, D2).
   *(Updated 2026-09-14: the rebuild path exists at `edc38c6a`; the two-run evidence lands with
   the ICE-GOLD-TWICE-1 gold module in the nightly — first run pending the post-merge dispatch.)*
10. **[v3-statement-coverage.md](../design/v3-statement-coverage.md).** The totals still read
    72 EQUAL and 8 DIVERGES, and rows `ctas-v3` and `alter-write-ordered-by` still read DIVERGES.
    The registry records `V3-COV-8` FIXED on 2026-09-05 and `V3-COV-5` FIXED on 2026-09-06, with
    both verdicts flipped to EQUAL. The document last changed on 2026-09-04.
11. **[The registry](../spark-sql-iceberg-parity.md) has two `ORPHAN-S3TABLES-1` rows.** The §7 row
    says "OPEN, half fixed" and waits on the loud refusal. The later S3 Tables row records that
    refusal as the DECLARED end state (2026-09-12).
12. **[crates/repark-iceberg/map.md](../../crates/repark-iceberg/map.md), "Known limitations".** The
    G17 `Catalog` trait re-enumeration is recorded only through RP-11 (`189a73ed`), yet AGENTS.md's
    "Version-pin contract" puts every repin's re-verification there. At `3ebf7d36` this review
    counted 14 required and 16 defaulted methods, with `NamespaceScopedCatalog` forwarding 27. The
    claim still holds, but RP-12 to RP-19 left no record.
13. **`task/ledgers/staging/`** still holds the RP-16, RP-17, RP-18, RP-19, ORPHAN-S3TABLES-1 and
    SILVER-S0 ledgers. Each says it moves to `completed/` in its unit's last commit, and each unit
    has merged.
14. **[docs/testing.md](../testing.md)** calls `make py-test-spill-matrix` the spill matrix's "CI
    tier". No workflow runs it.
15. **[iceberg-write-baseline.md](../perf/iceberg-write-baseline.md) §6 and registry
    `WRITE-DISTRIBUTION-2`** record plain `INSERT INTO` at 64 files. RP-16 consumed fork
    F-INSERT-DIST-1 without a re-measure, so that number describes the pin before RP-16 only.
16. **The superseded assessment (2026-09-06)** cited the nightly v3 oracle as green. The
    NIGHTLY-LIVE-1 ledger records parity-live red every night from 2026-09-05 to 2026-09-11.
    STATUS.md does not record that interval.
17. **`python/repark/tests/test_aws_acceptance.py`.** The `_assert_aws_cutover_core` docstring says
    S6 "pins the DATE() refusal". The code asserts the namespace instead, as the SQL-HARDEN-2
    ledger §5 records.

## 10. Addendum — orchestrator, 2026-09-14 12:50 EDT

Written after the review, from the GitHub Actions history and the owner's rulings. The reviewer
worked offline from the tree and did not see the workflow runs.

**G-2 is narrower than written.** `aws-acceptance.yml` runs on its nightly schedule against
`main` and has completed green every night from 2026-09-07 to 2026-09-14 (run 34824917757 on
`2bebc9da`, one commit before `c9b03c67`). Each run executes the eight live legs of
`python/repark/tests/test_aws_acceptance.py` on Glue and S3 Tables: the process-silver
acceptance, merge-on-read MERGE + compact + expire, the v3 deletion-vector DML + maintenance, and
the SQL-HARDEN cutover shapes. So the admitted silver shapes are live-accepted at the current
fork pin `3ebf7d36` nightly; rows C0 and C1 "waits on a re-run" are met by those runs. What still
stands from G-2: the dbt Glue gold leg is in no workflow, and the run ids are not recorded in the
inventory. *(Updated 2026-09-14: the gold module joined `aws-acceptance.yml` under
ICE-GOLD-TWICE-1; the first run's id and per-leg counts land in inventory §8 after the post-merge
dispatch.)*

**Owner rulings, 2026-09-14** (Q-ICE-1..7):

| Q | Ruling |
|---|---|
| Q-ICE-1 | (a): C6 waits on a fork Glue `publish_replace_table` and a repin; no interim route. |
| Q-ICE-2 | Both silver stages (facts and dimensions) use `CREATE TABLE IF NOT EXISTS` and `MERGE INTO`; no replace. G-1 blocks gold only. |
| Q-ICE-3 | Yes: the shadow tables are created by Spark from the production DDL and properties. The owner adds that most tables in another pipeline are Spark-created and RePark must write into them — writing into Spark-created tables is a first-class requirement, not a cutover detail. |
| Q-ICE-4 | The candidate is `main` once the Glue replace fix and repin land; the owner approves the aws-acceptance dispatch on it and adding the dbt Glue gold module to that workflow. |
| Q-ICE-5 | Spark and Trino read silver and gold in production. Athena is not a production reader; check 4 and G-4 are re-scoped to Spark and Trino. |
| Q-ICE-6 | Glue table optimizers are not enabled. The single-writer rule holds as written. |
| Q-ICE-7 | Airflow may retry, under the recommendation: MERGE and CTAS IF NOT EXISTS only, never an append, alert on any error naming `CommitStateUnknown`. |

**Units chartered from this assessment** (run 14): `F-GLUE-REPLACE-1` (fork) + `RP-20` (repin),
`ICE-GOLD-TWICE-1` (gold acceptance run twice, the replace leg in the nightly, the gold module in
the workflow), `ICE-SPARK-TABLE-1` (RePark MERGE and maintenance on a Spark-created table, Spark
reads it back), `ICE-COMMIT-UNKNOWN-1` (its own exception class). Trino cross-read is a card
behind the 1.8 Trino qualification.

## 11. Audit trail — corrections from the 2026-09-16 rating

Added 2026-09-17. The 2026-09-16 measured Iceberg rating, run at `a92a68db`
(repark 1.4.2, fork `edc38c6a`), §9 lists eleven repository claims its
measurement did not support. `File:line` is cited at `a92a68db`. Rows this
unit rewrote say so; the rest are recorded here only and owned elsewhere.

| # | Claim | File:line (at `a92a68db`) | What was measured |
|---|---|---|---|
| C-1 | Append fills from a schema-carried `write_default` (V3-6). | `STATUS.md:134-135` | On a v3 column carrying `write-default: 5`, RePark `INSERT` and MERGE `INSERT` write NULL and a DataFrame append missing the column refuses; Spark fills 5 on all three doors. Cutover V1 rewritten above. |
| C-2 | Append fills an omitted column from a schema-carried `write_default` (north star §3 row, marked done). | `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md:62` | Same as C-1. Not rewritten here. |
| C-3 | `FanoutWriter::close` drains in ascending partition-value order, one rule on every writer (V3-COV-3, FIXED). | `docs/spark-sql-iceberg-parity.md:1276-1278` | `INSERT … SELECT` from a temp view measured ascending in 1 of 12 runs; `INSERT … VALUES` and CTAS 12 of 12. Registry row rewritten, FIXED to OPEN. |
| C-4 | `rewrite_data_files` (binpack, `where`, options), status PROVEN (row M1). | `docs/cutover/production-iceberg-status-2026-09-14.md:215` | Every `options` map refused on every tried key; `where` works. Cutover M1 rewritten above. |
| C-5 | Lineage carry and merge-on-read are complete on every served DML shape. | `STATUS.md:156` | MERGE after `ALTER COLUMN id TYPE BIGINT` fails on single-era tables and silently skips updates plus inserts duplicates on mixed-era tables, v2 and v3, CoW and MoR. Not rewritten here. |
| C-6 | Schema and partition evolution PROVEN, incl. type promotion (row E1). | `docs/cutover/production-iceberg-status-2026-09-14.md:209` | Promotion, post-ADD/RENAME DML, and nested-add gaps as listed in E1. Cutover E1 rewritten above. |
| C-7 | The write path honors a declared default order on every writer (V3-COV-5). | `docs/spark-sql-iceberg-parity.md:4416-4418` | Plain `INSERT INTO` into an ordered table wrote unsorted files with `sort_order_id` NULL; Spark's twin sorted with id 2. Cutover W6 rewritten above. |
| C-8 | `statistics` / `partition-statistics` keys are absent from repark commits. | `docs/spark-sql-iceberg-parity.md:6843` | Holds for tables without prior statistics; on a Spark table with stats both entries carry forward through INSERT and expiry. Registry sentence rewritten. |
| C-9 | `IcebergTableScan::statistics()` reports exact counts so DataFusion folds `count(*)` (PERF-ICE-COUNTSTAR-1, FIXED); row R1 claimed the fold. | `docs/spark-sql-iceberg-parity.md:7094-7101`; `docs/cutover/production-iceberg-status-2026-09-14.md:173` | No fold at the current pin (plan keeps `IcebergTableScan`); the answer is correct and the fold pins self-skip. Registry row rewritten, FIXED to OPEN; cutover R1 rewritten above. |
| C-10 | The ordinary read path promoted correctly (V3-COV-2, FIXED), and E1 PROVEN incl. type promotion. | `docs/spark-sql-iceberg-parity.md:1241-1242`; `docs/cutover/production-iceberg-status-2026-09-14.md:209` | Range predicates on a promoted column silently drop pre-promotion rows (2 of 10 correct on RePark, 10 of 10 on Spark). Registry row not rewritten here. |
| C-11 | Unquoted identifiers agree with Spark (ID-1). | `docs/spark-sql-iceberg-parity.md:670-671` | Holds for lower-case columns; a stored `userId` fails unquoted on the SQL door. Registry row not rewritten here. |

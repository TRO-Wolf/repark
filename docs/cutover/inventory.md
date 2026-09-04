# Production cutover inventory (filed 2026-09-04, read from the cutover pipeline's DAGs, Spark scripts, dbt project and Glue config)

Scope: the owner's post-release point 2 ([../../STATUS.md](../../STATUS.md) "What happens next" item 3). The pipeline is named only as "the cutover pipeline" here; its namespace is written `<ns>_silver`.

## 1. Workloads, in run order

| # | Stage | Trigger | Engine today | Statement shapes | Iceberg? |
|---|---|---|---|---|---|
| 1 | Bronze extraction (6 entities: clinic, provider, appointment, appointment_cpt, survey, patient_visit) | cron 05:00 US/Eastern daily | Airflow transfer operator, DSQL SQL → S3 parquet under `bronze/<entity>/<ds>.parquet` | plain SELECT on the source DB | no (parquet objects) |
| 2 | Silver dimensions (`silver_dim_jobs.py`, dim_dates) | 06:00 US/Eastern spark-submit DAG (branch) | Spark 4.x on the Airflow worker (also Glue Job and EMR variants of the DAG) | CTAS IF NOT EXISTS, full rebuild | yes, `glue_catalog`, v2 |
| 3 | Silver facts (`process_silver.py`, six entities) | same DAG, `process_silver` task; emits the `silver/facts` asset | same | parquet read → row_number dedup (partition by id, order by ingestion_timestamp desc) → CREATE TABLE IF NOT EXISTS (parquet temp view) → MERGE INTO … UPDATE SET * / INSERT * | yes, `glue_catalog`, **format-version 2, copy-on-write** delete/update/merge, 256 MiB target files |
| 4 | Gold dbt (2 models, `materialized='table'`, `file_format='iceberg'`; 10 test blocks) | asset-scheduled on `silver/facts` | dbt over Spark Thrift (`glue_spark`) or dbt-glue Interactive Sessions (`glue_session`), both with the same `glue_catalog` conf and `defaultCatalog=glue_catalog` | join + aggregate; `date()`, `unix_timestamp()`, `cast` | yes (dbt table = drop + CTAS) |
| 5 | Maintenance (`iceberg_maintenance.py`) | `@weekly` | Spark | CALL expire_snapshots(older_than, retain_last) · rewrite_manifests · rewrite_data_files (binpack, opt-in) · remove_orphan_files (older_than floor) | yes |
| 6 | Object deletion | manual | Airflow | S3 delete | no |

## 2. Table ownership

| Table set | Writer today | Single-writer after cutover |
|---|---|---|
| bronze parquet objects | Airflow transfer operator | unchanged; RePark reads them |
| silver dims (`<ns>_silver.dim_*`) | silver_dim_jobs (Spark) | RePark CTAS; the Spark task paused |
| silver facts (6 tables) | process_silver MERGE (Spark) | RePark MERGE; the Spark task paused; no concurrent Spark writer |
| gold (2 dbt tables) | dbt (Thrift or Glue session) | dbt stays; dbt-repark adapter or dbt-spark over the RePark Thrift server is NOT a 1.0.x deliverable → gold stays on Spark/Glue until a dbt path exists (owner call) |
| maintenance | Spark CALL | RePark CALL for expire/rewrite_manifests/rewrite_data_files; `remove_orphan_files` needs `older_than` on RePark (ORPHAN-1) — the script always passes it |

Single-writer rule: one engine per table per run. RePark and Spark never both write a table in the same DAG run; the cutover flips the writer per stage, silver first.

## 3. What is already measured (SQL-HARDEN-1, PR #344; DATE-FN-1, PR #346)

| Matrix row | Verdict on memory / Glue / S3 Tables | Blocker |
|---|---|---|
| S1 CTAS IF NOT EXISTS from a parquet temp view, v2 props | rows equal; metadata `CUTOVER-CTAS-REQ-1` | required-ness of columns |
| S2 MERGE UPDATE SET * / INSERT * twice | rows equal, idempotent; delete-file packing count host-dependent (≥ 2) | none for rows |
| S3 row_number dedup + coalesce/cast | rows equal; `CUTOVER-DEDUP-SCHEMA-1` | schema nullability |
| S4 overwritePartitions | rows equal; `V3-COV-7` | v3 next-row-id codec |
| S5 the four maintenance CALLs | tuples equal | none |
| S6 gold join + agg + INSERT OVERWRITE | rows equal after DATE-FN-1 (`CUTOVER-DATE-1` FIXED) | none for rows |
| S7 = S1/S2/S4 at v3 | same as above | same |

Measured 2026-09-04 (SQL-HARDEN-2, PR #351): the production tables' **v2 copy-on-write** properties, as rows S8 (v2) and S9 (v3) — CTAS, MERGE twice, overwritePartitions — on memory, Glue and S3 Tables. No delete file under CoW on either engine; row sets, snapshot operations, MERGE idempotence and v3 next-row-id Spark-equal; the only divergences are the two metadata classes above (requiredness, stamped compression codec).

## 4. Acceptance checks (per stage, run in `testing_repark_acceptance` first, then on a shadow copy of the production namespace)

1. Row-set equality, silver facts: RePark MERGE vs Spark MERGE from the same bronze day (`ds`), all six entities; compare `EXCEPT ALL` both ways = 0 rows.
2. Idempotence: the same `ds` twice → second MERGE adds 0 rows, snapshot summary `added-records = 0`.
3. Schema: `DESCRIBE` equal (names, types, nullability) — this is where `CUTOVER-DEDUP-SCHEMA-1` and `CUTOVER-CTAS-REQ-1` bite; decide whether they are release blockers or accepted metadata differences.
4. Reader compatibility: Spark 4.1.2 and Athena read the RePark-written snapshot (v2 CoW) with equal counts.
5. Gold: dbt on Spark/Glue reads the RePark-written silver tables and the two gold tables match the previous day's Spark-written run within the day's deltas.
6. Maintenance: the weekly CALLs run on RePark against the RePark-written tables; snapshot count and file count after equal Spark's.

## 5. Rollback

Every RePark write is one Iceberg snapshot. Rollback = `CALL rollback_to_snapshot` (or `rollback_to_timestamp`) to the last Spark-written snapshot, then re-enable the Spark task. Keep `retain_last` high enough (weekly expiry, retain 3 → raise to cover a full cutover week) so the Spark snapshot survives the maintenance run.

## 6. Canary plan

| Step | What | Gate |
|---|---|---|
| C0 | fresh-venv wheel + Glue + S3 Tables canaries (done for 1.0.1) | green |
| C1 | CoW MERGE matrix cell on all three catalogs — DONE 2026-09-04 (SQL-HARDEN-2) | rows equal |
| C2 | shadow namespace `<ns>_silver_repark`: RePark runs stages 2–3 for one `ds` beside Spark | checks 1–4 |
| C3 | seven consecutive days in shadow | checks 1–4 daily, zero divergences |
| C4 | flip silver writer to RePark; Spark task paused, not deleted | checks 1–5; rollback rehearsed once |
| C5 | maintenance on RePark | check 6 |
| C6 | gold on RePark — **the dbt path exists (DBT-1, PR #370, 2026-09-04) and the Glue leg is measured green** (`python/dbt-repark/tests/test_aws_acceptance_gold.py`, namespace `testing_repark_acceptance`, 2026-09-05: `dbt run` built both gold models on RePark over Glue Iceberg, `dbt test` passed all ten blocks). Flip gold after C4 holds. | checks 5 + the ten dbt tests |

## 7. The four decisions — RULED by the owner, 2026-09-04

| # | Question | Ruling | Follow-through |
|---|---|---|---|
| 1 | Are `CUTOVER-CTAS-REQ-1` / `CUTOVER-DEDUP-SCHEMA-1` (metadata-only: column requiredness, nullability after `coalesce`/`cast`) blockers? | **Match Spark.** RePark derives nullability the way Spark does; neither row is accepted as a difference. | Unit `CUTOVER-SCHEMA-1` (Muse Spark 1.3 actor, Opus critic), launched 2026-09-04: readers nullable-by-default, CTAS `required: false`, Spark's `coalesce`/`cast` nullability rules; both rows flip FIXED. |
| 2 | Gold stays on Spark/Glue, or a dbt path? | **Queue the dbt unit.** Gold stays on Spark/Glue only until it lands — **landed 2026-09-04 (DBT-1, #370)**; C6 measured green on Glue 2026-09-05. | Unit `DBT-1` queued on the slate: design ledger first (adapter vs Thrift; expected winner an in-process `dbt-repark` adapter), then the thinnest adapter that runs the two gold models and their ten tests; acceptance = cutover step C6. |
| 3 | Shadow namespace and retention | **Recommended:** `<ns>_silver_repark` in the same Glue catalog and warehouse bucket; shadow tables kept 14 days past the canary, then dropped. | The shadow writer writes only to that namespace; retention is a DAG param. |
| 4 | Who runs the daily shadow diff | **Recommended:** an Airflow task beside the Spark silver task, asset-triggered on the silver facts asset, failing the run on any row-set divergence. | Pipeline-side unit `SHADOW-1` (Muse Spark 1.3), launched 2026-09-04: the silver job on RePark into the shadow namespace, the diff script (counts, `EXCEPT ALL` both ways, schema with nullability reported separately until `CUTOVER-SCHEMA-1` lands, snapshot summaries), and the DAG with a retention task. |

Canary step C2 starts when `SHADOW-1` is in the pipeline and `CUTOVER-SCHEMA-1` is on `main`; C3 is the seven shadow days; C6 waits on `DBT-1`.

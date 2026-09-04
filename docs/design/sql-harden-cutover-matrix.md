# SQL-HARDEN-1 — the cutover pipeline cutover Iceberg SQL shapes, measured against Spark

**Measured 2026-09-04 · SQL-HARDEN-1 · base `e6ebd40` · oracle live PySpark 4.1.2 + Iceberg 1.11.0
(`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `REPARK_PARITY_LIVE=1`) ·
ledger [`task/ledgers/staging/sql-harden-1-cutover-shapes-ledger.md`](../../task/ledgers/staging/sql-harden-1-cutover-shapes-ledger.md)**

This file closes when the cutover matrix is filed on `main`. H-2 scoped to the the cutover pipeline
pipeline. Memory-catalog Spark cells are the oracle; Glue and S3 Tables prove repark's catalog
path.

## 1. Totals

| | Count |
|---|---|
| Programs | **9** (S1–S6 plus S7 = v3 of S1/S2/S4) |
| **EQUAL** | **0** |
| **DIVERGES** | **9** |
| Registry rows filed | 4 (`CUTOVER-CTAS-REQ-1`, `CUTOVER-MERGE-FILES-1`, `CUTOVER-DEDUP-SCHEMA-1`, `CUTOVER-DATE-1`) |
| Existing row cited | 1 (`V3-COV-7` on S4 and S7-overwrite) |
| Defects FIXED in this unit | **0** |

Row values that the pipeline depends on (dedup output, MERGE row set, overwritePartitions
survivors, maintenance CALL results, v3 `next-row-id`) match Spark. Schema requiredness,
MoR delete-file counts, parquet-read `string_view`, the gold `DATE(ts)` spelling, and Spark's
stamped `zstd` codec do not.

## 2. How a row is read

- **Fixture** — one single-file bronze parquet (VARCHAR id, TIMESTAMP, DECIMAL(10,4), INT,
  nullable STRING, INT part). S2/S5/S7-merge apply the S3 dedup transform before CTAS/MERGE.
- **Probes** — rows, Arrow schema (type + nullability), snapshot operations, delete-file
  kinds, files, metadata JSON (format version, Iceberg schema requiredness, write-properties,
  `next-row-id`, snapshot summary counts). S2/S7-merge add a second-MERGE row-idempotence cell.
- **Verdict** — `EQUAL` / `DIVERGES`. A DIVERGES cell is a registry row; the pin is repark's
  current answer.
- **Pin** — `python/repark/tests/test_sql_harden_cutover.py`. Always-run repark half;
  live Spark behind `REPARK_PARITY_LIVE=1`. Catalog `sqlh1`.

## 3. The matrix

| Row | Shape | Doors | memory | Glue | S3 Tables | Verdict | Registry |
|---|---|---|---|---|---|---|---|
| `s1-ctas-if-fresh` | S1 parquet temp-view CTAS IF NOT EXISTS, v2 MoR properties | Spark SQL | DIVERGES | PASS | PASS | **DIVERGES** | `CUTOVER-CTAS-REQ-1` (+ `V3-COV-7`) |
| `s2-merge-idempotent` | S2 MERGE UPDATE SET * / INSERT * twice | Spark SQL | DIVERGES | PASS | PASS | **DIVERGES** | `CUTOVER-MERGE-FILES-1` |
| `s3-dedup-coalesce-cast` | S3 row_number dedup + coalesce/cast | facade DF | DIVERGES | PASS | PASS | **DIVERGES** | `CUTOVER-DEDUP-SCHEMA-1` |
| `s4-overwrite-partitions` | S4 `writeTo.overwritePartitions` on a partitioned table | facade DF | DIVERGES | PASS | PASS | **DIVERGES** | `V3-COV-7` |
| `s5-maintenance-calls` | S5 expire / rewrite_data_files binpack / remove_orphan_files / rewrite_position_delete_files | Spark SQL CALL | DIVERGES | PASS | PASS | **DIVERGES** | `CUTOVER-DEDUP-SCHEMA-1` (required-after-dedup schema), V3-COV-7 (codec) |
| `s6-gold-incremental` | S6 gold join + agg + INSERT OVERWRITE | Spark SQL | DIVERGES | path | path | **DIVERGES** | `CUTOVER-DATE-1` |
| `s7-ctas-if-fresh` | S7 = S1 at format-version 3 | Spark SQL | DIVERGES | PASS | PASS | **DIVERGES** | `CUTOVER-CTAS-REQ-1` |
| `s7-merge-idempotent` | S7 = S2 at format-version 3 | Spark SQL | DIVERGES | PASS | PASS | **DIVERGES** | `CUTOVER-MERGE-FILES-1` |
| `s7-overwrite-partitions` | S7 = S4 at format-version 3 | facade DF | DIVERGES | PASS | PASS | **DIVERGES** | `V3-COV-7` |

| Shape | Spark-equal cells | Divergence |
|---|---|---|
| S4 / S7-overwrite | rows, snapshot operations, v3 `next-row-id` | META codec (`V3-COV-7`) |
| S2 / S7-merge | row set, second-MERGE idempotence, delete-file kinds | packing count (host-dependent; ≥ Spark's 2) |
| S5 | CALL tuples: expire six zeros, `rewrite_data_files` five zeros, `remove_orphan_files` empty, `rewrite_position_delete_files` four zeros | schema / codec as S1 |

## 4. What was not a cell

Error text is not compared. Snapshot ids, file paths and byte counts are not probed.
Spark-side AWS cells are not required (memory Spark is the oracle).

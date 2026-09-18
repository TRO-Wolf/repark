# map — fixtures/torture/data/ice_evo_dml_1 (Spark oracle + one adopted Iceberg table)

## Purpose

The recorded Spark 4.1.2 answers for ICE-EVO-DML-1 — MERGE, UPDATE, DELETE, INSERT and
INSERT OVERWRITE on a table evolved by `ADD COLUMN` or `RENAME COLUMN` (including a rename of
the key, a rename that swaps two names, DROP then ADD of one name, and RENAME onto a dropped
name) with no write since, the added column in a predicate / merge key / `NOT MATCHED BY SOURCE`
arm, the v3 `ADD COLUMN … DEFAULT` refusal, and the v3 `_row_id` /
`_last_updated_sequence_number` read of the same evolved tables — and one Spark-created,
Spark-evolved table the JVM-free tier adopts with `register_table`. Recorded 2026-09-17 by
`python/repark/tests/_record_ice_evo_dml_1.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 (ANSI on, Hadoop catalog). Four recordings as the catalog
grew (109 cases, then 141 with `rename_swap`, then 145 with `lineage_read`, then 183 with the
round-3 L-02 shapes) agree on every shared case. Never hand-edited: re-run the driver.

## Contents

- `truth.json` — `oracle` (pyspark, runtime GAV, ANSI), `catalog_sha256` (the SHA-256 of the
  driver's `build_cases()`; the pin module fails if the driver and the recording drift), and
  one `answers` line per case: the statement's SQL answer (`{"ok": true}` or the error type and
  first message line), the check query's `rows` / Arrow `types` / `columns`,
  `dataframe_steps` from a twin table driven through Spark's DataFrame door where the statement
  has one (facade `mergeInto`, `writeTo().append()`, `writeTo().overwritePartitions()`), and
  `frozen` (baked table root + metadata file) for the adopted cases. 183 cases, 70,035 bytes.
- `adopt_v2/` — the Spark-written table root (`data/` two parquet files, `metadata/v1…v4.metadata.json`,
  one manifest, one manifest list, `version-hint.text`): `(id BIGINT, w STRING)` format v2
  copy-on-write, two rows, then `RENAME COLUMN w TO v` and `ADD COLUMN extra STRING` with no
  write since. Metadata carries absolute paths under `/tmp/repark-ice-evo-dml-1/ns/`, so the
  pins copy the root there under a directory lock and remove the copy on exit. 20,191 bytes.
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)
- Cells: [../../../../../repark/tests/test_ice_evo_dml_1.py](../../../../../repark/tests/test_ice_evo_dml_1.py)
- Driver: [../../../../../repark/tests/_record_ice_evo_dml_1.py](../../../../../repark/tests/_record_ice_evo_dml_1.py)
- Ledger: `task/ledgers/staging/ice-evo-dml-1-ledger.md`

pins: ice-evo-dml-1/C-001, C-009, C-016, C-017

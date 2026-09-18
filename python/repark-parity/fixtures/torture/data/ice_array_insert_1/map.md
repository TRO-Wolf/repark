# map — fixtures/torture/data/ice_array_insert_1 (Spark oracle: inserts into array columns)

## Purpose

The measured Spark 4.1.2 answers for ICE-ARRAY-INSERT-1: writes into `ARRAY<INT>`,
`ARRAY<STRUCT<a INT, b STRING>>` and `MAP<STRING, ARRAY<INT>>` columns through five doors
at format versions 2 and 3. Recorded 2026-09-18 on PySpark 4.1.2 +
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, Hadoop catalog, `local[2]`.
The facade pins in `python/repark/tests/test_ice_array_insert_1.py` read every expectation
from the oracle file; no row or field id is a literal in a test body.

## Contents

- `spark_array_insert_oracle.json` — `oracle` plus 30 `cells` keyed
  `{shape}_{door}_v{version}`: shapes `list_int` / `list_struct` / `map_list`, doors
  `sql_values` / `sql_select` / `writeto_append` / `insert_into` / `save_as_table_append`,
  versions `v2` / `v3`. Each cell carries `table_ddl` (with a `<t>` table placeholder),
  `door`, `statement` (the `<t>` placeholder on the two SQL doors, the bare SELECT on the
  three DataFrame doors), `rows` (Spark's read-back ordered by id), `schema` (Spark's
  simple string), `data_files` (Spark's file count, recorded but NOT pinned — it follows
  Spark's task count) and `parquet_field_ids` (the first data file's footer walk as
  `[dotted path, id]` pairs; a map's repeated group reports as `m.m` with an empty id).
- `map.md` — this file.

## Provenance

Never hand-edited: re-run `python/repark/tests/_record_ice_array_insert_1.py` on live
PySpark to re-record. Raw-recording SHA-256: `spark_array_insert_oracle.json`
`d41216e5156cb7eef11b24664e525399b1d20424d5552be0a6699dd50a8d7040`
(byte-identical to the recording).

## Reading the cells

- Every cell holds a NULL, an empty and a NULL-bearing value per shape: `list_int` rows
  `[1,2,3]` / NULL / `[]` / `[5,NULL,7]`, `list_struct` rows of two structs / NULL / `[]`,
  `map_list` rows `{k1:[1,2],k2:[]}` / NULL / `{k3:NULL}`.
- `data_files` is Spark's file count (4 on every recorded cell) and is NOT pinned:
  Spark's file count follows its task count, and the substitute re-measure below wrote 3
  files for the same rows and ids.
- The four non-VALUES `map_list` doors' recorded statements spell the NULL-map row
  `CAST(NULL AS MAP<STRING, ARRAY<INT>>)`, which RePark's parser refuses
  (registry row CAST-MAP-SPELL-1, BACKLOG). The pins write those rows through a substitute
  source instead: the NULL-map row reads `CASE WHEN false THEN map('k1', array(1, 2)) END`.
  Measured 2026-09-18 on live Spark 4.1.2 (UTC): all 8 substitute cells (4 doors x v2/v3)
  answer the recorded rows and the recorded footer field ids byte-identically.

pins: ice-array-insert-1/C-001, C-005

## Pointers

- Up: [../map.md](../map.md)
- Driver: [../../../../../repark/tests/_record_ice_array_insert_1.py](../../../../../repark/tests/_record_ice_array_insert_1.py)
- Ledger: `task/ledgers/staging/ice-array-insert-1-ledger.md`

# map — fixtures/torture/data/ice_list_null_1 (Spark oracle: DELETE and UPDATE with IS NULL on nested columns)

## Purpose

The measured Spark 4.1.2 answers for ICE-LIST-NULL-1: `DELETE` and
`UPDATE ... WHERE <list | map | struct column> IS [NOT] NULL` over four shapes at
format versions 2 and 3, under copy-on-write and merge-on-read. Recorded 2026-09-18
on PySpark 4.1.2 + `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`,
Hadoop catalog, `local[2]`, `spark.sql.shuffle.partitions=1`. The pins in
`python/repark/tests/test_ice_list_null_1.py` read every expectation from the oracle
file; no id list or snapshot operation is a literal in a test body.

## Contents

- `spark_list_null_oracle.json` — `spark`, `iceberg`, plus 128 `cells`: shapes
  `list_int` / `list_struct` / `map_int` / `struct` x predicates `xs IS NULL` /
  `xs IS NOT NULL` / `id > 1 AND xs IS NULL` / `xs IS NULL OR id = 1` x statements
  `delete` / `update` x modes `copy-on-write` / `merge-on-read` x versions 2 / 3.
  Each cell carries `sql` (with a `<t>` table placeholder), `ok`, `ids` (the ids
  left behind by a delete, the ids including the `+100` rewrites for an update),
  `operation` (the newest snapshot's operation), `added_delete_files` and
  `added_dvs` (the newest snapshot summary's `added-delete-files` / `added-dvs`,
  `"0"` when absent).
- `map.md` — this file.

## Provenance

Never hand-edited: re-run `python/repark/tests/_record_ice_list_null_1.py` on live
PySpark to re-record. Raw-recording SHA-256: `spark_list_null_oracle.json`
`08b8b5f7219dd1e0cfd50bfcec19009c7624caa631972335544e21901c77291f`
(byte-identical to the recording).

## Reading the cells

- Every shape seeds four rows: a non-null value, a NULL column, an empty value, and
  a value carrying a NULL inside. `list_int` rows `[1,2]` / NULL / `[]` / `[NULL]`;
  `list_struct` rows `[{a:1}]` / NULL / `[]` / `[NULL]`; `map_int` rows `{k:1}` /
  NULL / `{}` / `{k:NULL}`; `struct` rows `{a:1}` / NULL / `{a:NULL}` / `{a:4}`.
- `operation` is `overwrite` on every cell except the eight copy-on-write delete
  cells with predicate `xs IS NULL OR id = 1` (every shape x v2/v3, ids `[3, 4]`
  left), which record `delete`. All 32 merge-on-read delete cells record
  `delete`; all 32 merge-on-read update cells record `overwrite`.
- `added_delete_files` / `added_dvs` follow Spark's task count, not the row
  content: every merge-on-read cell adds 1 delete file, except the sixteen
  `xs IS NOT NULL` cells (delete and update, v2 and v3, all four shapes), which
  add 2. On v3 the same count lands in `added_dvs` (1, or 2 on the
  `IS NOT NULL` cells); on v2 `added_dvs` is 0 everywhere. The pins assert them
  where RePark agrees (see the ledger) and pin RePark's measured values where
  the two engines differ.
- The `map_int` seed spells its empty-map row `CAST(map() AS MAP<STRING, INT>)`,
  which RePark's parser refuses (registry row CAST-MAP-SPELL-1, BACKLOG). The pins
  seed that row through `map_from_arrays(CAST(array() AS ARRAY<STRING>),
  CAST(array() AS ARRAY<INT>))` instead, which RePark parses and which reads back
  equal to Spark's seed (`{k:1}` / NULL / `{}` / `{k:NULL}`, measured 2026-09-18
  on the release native).
- Fork #299 residue: copy-on-write DELETE with a compound predicate over the
  nested column still refuses `Accessor for Field xs not found` (sixteen cells:
  every shape x v2/v3 x the two compound predicates). The pins run those cells
  verbatim under strict xfail; the sixteen merge-on-read `xs IS NOT NULL` cells
  pin RePark's measured 1 delete file (1 DV on v3) against Spark's 2 (2 DVs).

pins: ice-list-null-1/C-001, C-002, C-007

## Pointers

- Up: [../map.md](../map.md)
- Consumer: [../../../../../repark/tests/test_ice_list_null_1.py](../../../../../repark/tests/test_ice_list_null_1.py)
- Driver: [../../../../../repark/tests/_record_ice_list_null_1.py](../../../../../repark/tests/_record_ice_list_null_1.py)
- Ledger: `task/ledgers/staging/ice-list-null-1-ledger.md`

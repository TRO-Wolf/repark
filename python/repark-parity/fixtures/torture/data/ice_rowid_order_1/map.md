# map — fixtures/torture/data/ice_rowid_order_1 (Spark oracle: v3 row-id assignment order)

## Purpose

The measured Spark 4.1.2 answers for ICE-ROWID-ORDER-1: which partition's data
file takes which `first_row_id` when one statement writes several partitions of a
format-v3 table. Two recordings, both on PySpark 4.1.2 +
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`. The pins in
`python/repark/tests/test_ice_rowid_order_1.py` read every expectation from these
files; no row id is a literal in a test body.

## Contents

- `spark_rowid_abc_oracle.json` — the a/b/c recording (run 23a, `local[8]`,
  `spark.sql.shuffle.partitions=4`): cells `rowid_sc_select`, `rowid_sc_values`,
  `rowid_sc_ctas` (Hadoop catalog) and the `rowid_mc_*` twins (InMemory catalog).
  Each is 12 runs of `INSERT INTO t SELECT id, cat, v FROM s1` on a v3 table
  `(id BIGINT, cat STRING, v DOUBLE)` partitioned by `cat`, with the source temp
  view over `range(300)` mapping `cat = a/b/c` by `id % 3`; every run in every
  cell answers partition to `min(_row_id)` = a:0, b:100, c:200 (12 of 12). The
  `rowid_*_values` cells (six literal rows) answer a:0, b:2, c:4 in 12 of 12;
  `rowid_*_select_files` is the first run's `(partition, record_count,
  first_row_id)` listing. The file also carries the draft recorder's
  `*_16_concurrent_inserts` storm cells, which belong to ICE-APPEND-RETRY-1 (see
  `ice_occ_scoped_1/spark_occ_oracle4.json`): the pins here read only the eight
  `rowid_*` keys, and the recorder below does not reproduce the storm.
- `spark_rowid_order_oracle.json` — the eight-category recording (run 23a):
  categories `d, a, z, m, b, q, c, x`, twelve configurations
  (`spark.sql.adaptive.enabled` on/off x rows 400/200000 x
  `write.distribution-mode` hash/none/range), six runs each, each run the
  `(partition, record_count, first_row_id)` file listing in `first_row_id`
  order. Under the default configuration (`aqe=True rows=400 dist=hash`) Spark's
  file order is its hash-partitioner task order `z, x, m, a, q, b, c, d` in 6 of
  6 runs; it differs with AQE off, with `range`, and with `none` (see the
  `distinct_file_orders` per cell). Deterministic per configuration, not
  partition-value order; the a/b/c shape is where hash order and lexical order
  coincide.
- `map.md` — this file.

## Provenance

Never hand-edited: re-run `python/repark/tests/_record_ice_rowid_order_1.py` on
live PySpark to re-record (it reproduces the eight `rowid_*` a/b/c cells and the
default-configuration order cell; the storm cells and the non-default order
configurations re-record through its parameters). Raw-recording SHA-256:
`spark_rowid_abc_oracle.json`
`ff500c59429d64124465b1e8c86415286b6acd7f8235a10d05e25ada985e869d`,
`spark_rowid_order_oracle.json`
`06d6a8b266e553835c37f8698b883c2c53bf85e430e56189d2a30aa6739c1c82`
(both byte-identical to the recordings).

## Reading the cells

- Ruling Q-23b-2 (orchestrator): RePark is now deterministic (ascending partition
  value, then write-task index). The pins assert the a/b/c `INSERT ... SELECT`
  cell over 12 runs gives one mapping and it equals Spark's recorded a:0,
  b:100, c:200, VALUES and CTAS the same; and the eight-category shape over 12
  runs gives one mapping, RePark's ascending order, with a DECLARED-divergence
  assertion that Spark's recorded default-configuration order differs from it
  (so a future convergence reds it).
- Row ids stay spec-correct on both engines either way: contiguous, unique, every
  file's `first_row_id` the running sum of the files before it.

pins: ice-rowid-order-1/C-001, C-002, C-007

## Pointers

- Up: [../map.md](../map.md)
- Consumer: [../../../../../repark/tests/test_ice_rowid_order_1.py](../../../../../repark/tests/test_ice_rowid_order_1.py)
- Driver: [../../../../../repark/tests/_record_ice_rowid_order_1.py](../../../../../repark/tests/_record_ice_rowid_order_1.py)
- Ledger: `task/ledgers/staging/ice-rowid-order-1-ledger.md`

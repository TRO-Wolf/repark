# map — python/repark-parity/fixtures/torture/data

## Purpose

Committed fixture data for the torture suite — TORTURE-1's single committed-data
exception (card D-5): the `v3_dv` family needs Spark-written Puffin deletion vectors,
which no pure-Python generator can produce, so one ≤ 1 MB table instance is checked in
here. Every other byte the suite reads is generated at test time and never committed;
the generators' repository-output refusal does not apply here because this directory is
the committed landing zone, not a generation target.

## Contents

- [v3_dv/](v3_dv/map.md) — a Spark-written format-v3 merge-on-read Iceberg table with
  live deletion vectors and its `truth.json` record.
- [ice_spark_table_1/](ice_spark_table_1/map.md) — a Spark-written format-v2
  copy-on-write Iceberg table with the production properties, Spark's own CoW MERGE
  and a `hash` distribution stamp in its history, and its `truth.json` record
  (ICE-SPARK-TABLE-1; 67,799 bytes, second committed-data exception alongside
  `v3_dv`).
- [ice_hadoop_vn_1/](ice_hadoop_vn_1/map.md) — a Spark-written format-v2 Hadoop
  table at `v2` with one seed row, the adoption base for the stale-`vN`-writer
  pins (ICE-HADOOP-VN-1).
  pins: ice-hadoop-vn-1/C-007
- [ice_promote_read_1/](ice_promote_read_1/map.md) — the recorded Spark 4.1.2 answers for
  reads and DML after a legal type promotion (`truth.json`, 126 cases) and two
  Spark-created promoted mixed-era tables (v2, v3) for the adoption cells
  (ICE-PROMOTE-READ-1; third committed-data exception).
- [ice_nested_evo_1/](ice_nested_evo_1/map.md) — the recorded Spark 4.1.2 answers for
  nested schema evolution and nested DDL (`oracle.json`) and four Spark-written tables whose
  struct, list-element struct or map-value struct gained a child after the first write
  (ICE-NESTED-EVO-1, 2026-09-17; fourth committed-data exception).
- [ice_dyn_overwrite_1/](ice_dyn_overwrite_1/map.md) — recorded Spark answers for
  ICE-DYN-OVERWRITE-1: the static/dynamic overwrite matrix plus three disk-verified
  overwrite-vs-append interleaves (`spark_oracle.json`, read by
  `python/repark/tests/test_ice_dyn_overwrite_1.py`).
- [ice_evo_dml_1/](ice_evo_dml_1/map.md) — the recorded Spark 4.1.2 answers for DML after
  `ADD COLUMN` / `RENAME COLUMN` with no write since (`truth.json`, 183 cases) and one
  Spark-created, Spark-evolved v2 table for the adoption cells (ICE-EVO-DML-1; fourth
  committed-data exception, 77,324 bytes).
- [ice_occ_scoped_1/](ice_occ_scoped_1/map.md) — the recorded Spark 4.1.2 concurrent-DML commit
  storms (`spark_occ_oracle.json` unscoped, `spark_occ_oracle2.json` partition- and
  range-scoped): commits out of N, the losers' error text, the rows left behind
  (ICE-OCC-SCOPED-1; JSON only, no table data).
- [ice_v3_write_default_1/](ice_v3_write_default_1/map.md) — seven Spark-written
  format-v3 tables whose Java-API-added columns carry `initial-default` /
  `write-default`, with the 22-cell Spark oracle `truth.json` and its recording
  script (ICE-V3-WRITE-DEFAULT-1; 225,094 bytes, third committed-data exception).
- [ice_array_insert_1/](ice_array_insert_1/map.md) — the recorded Spark 4.1.2 answers for
  inserts into array columns (`spark_array_insert_oracle.json`, 30 cells: three shapes by
  five doors by two format versions) with rows and footer field ids per cell
  (ICE-ARRAY-INSERT-1, 2026-09-18; JSON only, no table data).
  pins: ice-array-insert-1/C-001
- [ice_write_options_rp_1/](ice_write_options_rp_1/map.md) — the recorded Spark 4.1.2
  answers for a caller-supplied `snapshot-property.replace-partitions` value on a
  replace-partitions commit (`spark_rp_oracle.json`, 5 cells: dynamic `insertInto`
  overwrite and `writeTo(t).overwritePartitions()` with the option `false` / `true`,
  plus the unrelated-option control) with the commit flag and the newest summary's
  `replace-partitions` and `k` per cell (ICE-WRITE-OPTIONS-RP-1, 2026-09-18; JSON
  only, no table data).
  pins: ice-write-options-rp-1/C-001
- [ice_list_null_1/](ice_list_null_1/map.md) — the recorded Spark 4.1.2 answers for
  DELETE and UPDATE with IS NULL on nested columns (`spark_list_null_oracle.json`,
  128 cells: four shapes by four predicates by delete/update by copy-on-write /
  merge-on-read by v2/v3) with the ids left, the newest snapshot's operation and
  its `added-delete-files` / `added-dvs` per cell (ICE-LIST-NULL-1, 2026-09-18;
  JSON only, no table data).
  pins: ice-list-null-1/C-001
- [ice_rowid_order_1/](ice_rowid_order_1/map.md) — the recorded Spark 4.1.2 answers
  for v3 row-id assignment order: the a/b/c recording (`spark_rowid_abc_oracle.json`,
  twelve runs each of INSERT INTO SELECT, literal VALUES and CTAS on Hadoop and
  InMemory catalogs) and the eight-category recording
  (`spark_rowid_order_oracle.json`, six runs per configuration over adaptive,
  row count and distribution mode) (ICE-ROWID-ORDER-1, 2026-09-18; JSON only, no
  table data).
  pins: ice-rowid-order-1/C-001

## Pointers

- Up: [../map.md](../map.md)
- Suite: [../../../tests/torture/map.md](../../../tests/torture/map.md)

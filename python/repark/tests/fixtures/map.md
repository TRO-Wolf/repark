# `python/repark/tests/fixtures` map

Binary fixtures Spark wrote for facade IO pins, copied byte-identical from the
oracle (`.crc` sidecars skipped).

- [`orc/`](orc/map.md) — **IO-ORC-1 (2026-09-16):** the Spark-written ORC fixture
  directories behind `facade_orc_oracle.json`.
- [`ice_nan_pushdown_1/`](ice_nan_pushdown_1/map.md) — **ICE-NAN-PUSHDOWN-1
  (2026-09-17, round 2):** the Spark-written v2/v3 NaN warehouses behind
  `ice_nan_pushdown_1_oracle.json`.
- [`ice_sorted_insert_1/`](ice_sorted_insert_1/map.md) — **ICE-SORTED-INSERT-1
  (2026-09-17):** the Spark-written `days(ts), id` ordered warehouse behind
  `ice_sorted_insert_1_spark_oracle.json`.
- [`ice_page_prune_1/`](ice_page_prune_1/map.md) — **ICE-PAGE-PRUNE-1
  (2026-09-19, round 1):** the five Spark-written page-prune warehouses plus
  the compacted `truth.json`.
- [`ntz_1_spark_table/`](ntz_1_spark_table/map.md) — **WO NTZ-1 slice 3
  (2026-09-27):** the Spark-written one-row `xc.ns.x`
  (`TIMESTAMP_NTZ` + `TIMESTAMP`) warehouse behind `ntz-xc-spark.json`.

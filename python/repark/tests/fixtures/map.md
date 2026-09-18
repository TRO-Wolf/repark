# `python/repark/tests/fixtures` map

Binary fixtures Spark wrote for facade IO pins, copied byte-identical from the
oracle (`.crc` sidecars skipped).

- [`orc/`](orc/map.md) — **IO-ORC-1 (2026-09-16):** the Spark-written ORC fixture
  directories behind `facade_orc_oracle.json`. pins: io-orc-1/C-002
- [`ice_nan_pushdown_1/`](ice_nan_pushdown_1/map.md) — **ICE-NAN-PUSHDOWN-1
  (2026-09-17, round 2):** the Spark-written v2/v3 NaN warehouses behind
  `ice_nan_pushdown_1_oracle.json`. pins: ice-nan-pushdown-1/C-009, C-012
- [`ice_sorted_insert_1/`](ice_sorted_insert_1/map.md) — **ICE-SORTED-INSERT-1
  (2026-09-17):** the Spark-written `days(ts), id` ordered warehouse behind
  `ice_sorted_insert_1_spark_oracle.json`. pins: ice-sorted-insert-1/C-003, C-004

# `python/repark/tests/fixtures/ice_sorted_insert_1` map

Spark-written Iceberg warehouse for ICE-SORTED-INSERT-1, recorded by
`../../_record_ice_sorted_insert_1_oracle.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 and copied byte-identical from the
canonical `/tmp/repark-ice-sorted-insert-1/wh` path (`.crc` sidecars skipped).
The pin test copies the warehouse back to its canonical path before
`CALL system.register_table`, so the absolute file URIs in the manifests stay
valid. The recorder replaces only the table directory on `--rewrite` and
keeps this map. pins: ice-sorted-insert-1/C-003, C-004

## Contents

- [`days/`](days) — format-version 2, `(id BIGINT, ts TIMESTAMP)` with
  `WRITE ORDERED BY days(ts), id` (default order 1, one 2,000-row file sorted
  day-major, stamped 1). RePark cannot declare transform orders, so this
  Spark-written table is the only way to pin a transform-ordered write.

# `python/repark/tests/fixtures/ice_nan_pushdown_1` map

Spark-written Iceberg warehouses for ICE-NAN-PUSHDOWN-1, recorded by
`../../_record_ice_nan_pushdown_1.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 and copied byte-identical from the
canonical `/tmp/repark-ice-nan-pushdown-1/wh` paths (`.crc` sidecars skipped).
The pin test copies each warehouse back to its canonical path before
`CALL system.register_table`, so the absolute file URIs in the manifests stay
valid. pins: ice-nan-pushdown-1/C-009, C-012

## Contents

- [`v2/mixed/`](v2/mixed) — format-version 2, `(id INT, d DOUBLE, f FLOAT)`:
  ids 1..3 `(NaN,NaN)`, `(1.0,1.0)`, `(NULL,NULL)` in file one, ids 4..6
  `(0.5,0.5)`, `(NaN,NaN)`, `(-2.0,-2.0)` in file two.
- [`v2/nan_only/`](v2/nan_only) — format-version 2, ids 1..2 with every `d`
  and `f` NaN, one file.
- [`v3/mixed/`](v3/mixed) — the same rows as `v2/mixed` at format-version 3.
- [`v3/nan_only/`](v3/nan_only) — the same rows as `v2/nan_only` at
  format-version 3.

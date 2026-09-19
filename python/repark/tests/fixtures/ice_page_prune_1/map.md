# `python/repark/tests/fixtures/ice_page_prune_1` map

Spark-written Iceberg warehouses for ICE-PAGE-PRUNE-1, recorded by
`../../_record_ice_page_prune_1.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 (same seeds, properties and predicates as
the fork lane's `/tmp/oc-worker/pb-oracle/record_page_prune.py`, whose truth
the recorder matches cell for cell). Each table was rewritten with
`CALL …rewrite_table_path` to the canonical `/tmp/repark-ice-page-prune-1/wh`
prefix and copied here with only its live files (600,773 bytes total,
`truth.json` included). The pin test copies each warehouse back to its
canonical path before `CALL system.register_table`, so the absolute file URIs
in the manifests stay valid. The recorder replaces only the five table
directories on `--rewrite` and keeps this map. pins: ice-page-prune-1/C-001, C-002

## Contents

- [`base_v2/`](base_v2) — format-version 2 seed: ids 0..1999 in four 500-row
  files, `page-row-limit` 100, `d` NaN on 700..799, `n` NULL below 500.
- [`base_v3/`](base_v3) — the same rows as `base_v2` at format-version 3.
- [`del_v2/`](del_v2) — the seed minus `(id % 7 = 0 below 600)` and
  1500..1510, as position-delete files beside the four data files.
- [`del_v3/`](del_v3) — the seed minus `id % 7 = 0` below 500, below 600 and
  1500..1510, as puffin deletion vectors, plus `i + 1000000` on 1000..1020.
- [`evo_v2/`](evo_v2) — the seed, then `i` to BIGINT, `f` to DOUBLE, `dec` to
  `(18,2)`, `s` renamed to `s2`, `n` dropped and re-added, `addc` added, and
  ids 2000..2299 appended.
- [`truth.json`](truth.json) — the compacted Spark answers (66,161 bytes):
  id runs, or row-id and sequence segments on v3, decoded by
  `expand_cell` in the recorder.

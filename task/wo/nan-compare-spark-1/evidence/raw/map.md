# task/wo/nan-compare-spark-1/evidence/raw/ — the measured answers

Written by the probes in [../probes/](../probes/map.md); scratchpad paths are shortened to `<scratchpad>`.

- [cells.tsv](cells.tsv) — one row per compared cell: ANSI mode, door, source, column, operation, Spark's
  answer, then base's, 1325's and head3's (`=` where the build answered as Spark did). Spark's answer is the
  same door's on the same source; the native door and `nat_parquet_l5` are compared as the domain table says. An
  ANSI-off row is kept only where it differs from the same cell's ANSI-on row (21 of 4,061 do).
- [producers.jsonl](producers.jsonl) — the ten NaN producers, folded and over a column, on every engine, door
  and ANSI mode, with the result's NaN bits; Spark's `jvm_raw` form is `doubleToRawLongBits` inside the JVM.
- [stats.jsonl](stats.jsonl) — every Iceberg table's data-file metrics (value, null and NaN counts, lower and
  upper bounds with their bits), the partitioned tables' manifest partition summaries, and every Parquet
  footer's statistics (pyarrow, RePark and Spark writers, plain files and Iceberg data files).
- [write-rb.jsonl](write-rb.jsonl), [write-sp.jsonl](write-sp.jsonl) — the two writer runs: what each wrote.
- [critic-cells.jsonl](critic-cells.jsonl) — §9's cells: group, cell, door, Spark's answer and each build's.
- [critic-plans.jsonl](critic-plans.jsonl) — each §9 leg's provenance record and its truncated TopK scan lines.
- [ingest.jsonl](ingest.jsonl) — the ingestion cells, both engines.

Up: [../map.md](../map.md).

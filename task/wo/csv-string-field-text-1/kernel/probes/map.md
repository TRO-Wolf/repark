# task/wo/csv-string-field-text-1/kernel/probes/ — comment-free Python

- `added.py` — Spark 4.1.2's answer over the added texts: 66 texts × 19 types, each a one-row read, header off,
  UTC, recorded as `CAST(x AS STRING)`, `x IS NULL` and the collected value.
- `tables.py` — the kernel's test tables from Spark's recorded cells: the step 0 oracle (`O/`) and zone (`Z/`)
  cells of `../../evidence/raw/revision-cells-spark.jsonl.gz` and `../raw/added-spark.jsonl`, in one canonical form
  (`N` NULL, integers, Python float reprs, decimal units, epoch days and microseconds, `=` the text), written for
  the Rust test (`tables.rs.txt`) and the facade pin (`tables.py.txt`).
- `cost.py` — the kernel against arrow's `CAST` on the 2M-row `INT` file: one process per sample, a warm-up, then
  ten timed queries inside a `perf stat` control window.

Up: [../map.md](../map.md).

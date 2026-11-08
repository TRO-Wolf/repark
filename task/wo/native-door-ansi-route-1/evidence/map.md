# task/wo/native-door-ansi-route-1/evidence/ — the dry-run sizing of the native door on AnsiDialect

Measured 2026-11-08 by the Claude Haiku 5.5 measurement clerk, on `origin/main` at `03f925c5` and the same commit plus the prototype patch. The result sizes NATIVE-DOOR-ANSI-ROUTE-1 and changes no product code.

- [dry-run.md](dry-run.md) — the bucket counts, the call sites per bucket, the 14 changed tests with their scrubbed excerpts, and the findings by cause.
- [provenance.md](provenance.md) — the trees, the environment, the exact commands, the classification rules, the scrubbing and the hashes.
- [prototype.patch](prototype.patch) — the throwaway prototype: three added lines, never pushed.
- [raw/base.xml](raw/base.xml) and [raw/prototype.xml](raw/prototype.xml) — the JUnit output of the two runs, scrubbed.
- [probe_write_cells.py](probe_write_cells.py) — the write-cell probe: one leg per interpreter (base, proto, spark) and \`--render\` for the table and the counts.
- [write-cells.md](write-cells.md) — the write cells (groups W, E, R) per door and leg against Spark, with the labels and the counts by label.
- [raw/write-cells-base.jsonl](raw/write-cells-base.jsonl), [raw/write-cells-proto.jsonl](raw/write-cells-proto.jsonl) and [raw/write-cells-spark.jsonl](raw/write-cells-spark.jsonl) — the raw rows of the three legs.

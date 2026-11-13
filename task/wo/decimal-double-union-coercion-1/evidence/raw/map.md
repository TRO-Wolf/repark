# task/wo/decimal-double-union-coercion-1/evidence/raw/ — the raw cells

One JSON object per line; the first line of each file is its `provenance` row. RePark files omit the SQL
text (the key names it; the Spark file of the same set carries it).

- `select-spark-ansi-true.jsonl`, `select-spark-ansi-false.jsonl` — 1,985 SQL cells on Spark 4.1.2.
- `select-repark-ansi-true.jsonl`, `select-repark-ansi-false.jsonl` — the same cells on the RePark
  Spark door.
- `select-native.jsonl` — the same cells on `repark.sql()` (no ANSI switch).
- `frames-spark-ansi-true.jsonl`, `frames-spark-ansi-false.jsonl`, `frames-repark-ansi-true.jsonl`,
  `frames-repark-ansi-false.jsonl` — 203 DataFrame API cells per file.
- `write-spark-ansi-true.jsonl`, `write-spark-ansi-false.jsonl`, `write-repark-ansi-true.jsonl`,
  `write-repark-ansi-false.jsonl` — the 30 write cells: the write outcome and the writer's own read.
- `readback-spark-ansi-true.jsonl`, `readback-spark-ansi-false.jsonl`, `readback-repark-ansi-true.jsonl`,
  `readback-repark-ansi-false.jsonl` — Spark's read of each table, by writer.
- `revision-spark-ansi-true.jsonl`, `revision-spark-ansi-false.jsonl`, `revision-repark-ansi-true.jsonl`,
  `revision-repark-ansi-false.jsonl` — the revision's 28 select and 24 write cells per mode (RePark ANSI
  on adds the four `explain/*` pruning cells).
- `revision-readback-spark-ansi-true.jsonl`, `revision-readback-spark-ansi-false.jsonl`,
  `revision-readback-repark-ansi-true.jsonl`, `revision-readback-repark-ansi-false.jsonl` — Spark's read
  of every revision table, by writer.
- `replay-888-repark.jsonl` — #888's 612 Spark-recorded cells on RePark, with the root and verdict.
- [rule-lists.json](rule-lists.json) — the analyzer lists of both doors, the plans, and the PREPARE,
  cache and native-view cells.

Up: [../map.md](../map.md).

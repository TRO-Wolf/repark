# task/wo/ice-held-frame-snapshot-1/built/evidence/ — unit 1a's probes and recordings

Scratch paths read `S` (the lane scratchpad) and `N` (the session scratchpad); none is in the repository.

Scrub: scratch paths became `N` or `S`, the home-directory prefix became `~`, and a truncated scratch-path fragment inside two error
texts (the main answer of a RAISE cell, in both verdict files) became `N`. The D recordings held no scratch paths. Each
file is gzipped with `gzip -9n`.

- [probes/decide.py](probes/decide.py) — the D group's probe: the deciding cell and the schema-change writes under a held
  table, with the own-plan actions after `collect` and `localCheckpoint`. Run as
  `decide.py <engine> <warehouse parent> <out> <ansi>`. It imports `common`, which is not committed here (the lane's probe
  directory holds it), so it runs from there.
- [probes/barcmp.py](probes/barcmp.py) — pairs each recorded cell with Spark's answer and writes one verdict per cell, with
  the counts. Run as `barcmp.py <step 0 evidence/raw> <D raw dir> <bar dir> <verdicts out> main head`. Its bar run
  directories are not committed.
- [raw/head-58c7e307-verdicts.jsonl.gz](raw/head-58c7e307-verdicts.jsonl.gz) — the head `58c7e307` verdicts: 1980 rows (both
  ANSI modes, D included), each with the key, Spark's answer, main's and the head's answers, both verdicts, the stored
  flags and the unit tag.
- [raw/main-9a48932f-verdicts.jsonl.gz](raw/main-9a48932f-verdicts.jsonl.gz) — the same for main `9a48932f`: 1980 rows,
  with main's answer and verdict only.
- [raw/D-spark-ansi-true.jsonl.gz](raw/D-spark-ansi-true.jsonl.gz) — Spark 4.1.2's answers for the 16 D cells, ANSI on.
- [raw/D-spark-ansi-false.jsonl.gz](raw/D-spark-ansi-false.jsonl.gz) — the same 16 cells, ANSI off.

Up: [../map.md](../map.md).

# task/wo/ice-held-frame-snapshot-1/built/evidence/ — unit 1a's probes and recordings

Scratch paths read `S` (the lane scratchpad) and `N` (the session scratchpad); none is in the repository.

Scrub: scratch paths became `N` or `S`, the home-directory prefix became `~`, and a truncated scratch-path fragment inside two error
texts (the main answer of a RAISE cell, in both verdict files) became `N`. The D recordings held no scratch paths. Each
file is gzipped with `gzip -9n`.

- [probes/decide.py](probes/decide.py) — the D group's probe: the deciding cell and the schema-change writes under a held
  table, with the own-plan actions after `collect` and `localCheckpoint`. Run from this directory as
  `decide.py <engine> <warehouse parent> <out> <ansi>`; the Spark engine reads the runtime jar from `ICEBERG_JAR`.
- [probes/common.py](probes/common.py) — the session builder (`lc`, a `type=hadoop` catalog, one per engine), the
  condition reader and the row shapes `decide.py` imports (fix round 1, F5).
- [probes/barcmp.py](probes/barcmp.py) — pairs each recorded cell with Spark's answer and writes one verdict per cell, with
  the counts. Run as `barcmp.py <step 0 raw> <D raw> <bar> <verdicts out> <label> <label>`. It reads exactly three
  directories, and only the first is in the repository:
  1. `<step 0 raw>`: [../../evidence/raw/](../../evidence/raw/map.md), its `spark.jsonl.gz` and `rev-spark.jsonl.gz`.
  2. `<D raw>`: `N/ice-1a-out/raw/`, holding `D-spark-true.jsonl` and `D-spark-false.jsonl`. They are the two
     `raw/D-spark-ansi-*.jsonl.gz` files below, unzipped and renamed.
  3. `<bar>`: `N/ice-1a-out/bar/`, one sub-directory per label, each holding the `<group>-<ansi>.jsonl` files the bar
     runner writes (`cells.py` for `HW`, `WA`, `HA`, `P`, `S` and `X`; `rev.py` for `B`, `B2` and `M`; `decide.py` for
     `D`). The labels are `main` (`9a48932f`), `head` (`58c7e307`) and `head-fix1` (this round's package). The bar
     probes other than `decide.py` are in that session directory, not here.
- [raw/head-58c7e307-verdicts.jsonl.gz](raw/head-58c7e307-verdicts.jsonl.gz) — the head `58c7e307` verdicts: 1980 rows (both
  ANSI modes, D included), each with the key, Spark's answer, main's and the head's answers, both verdicts, the stored
  flags and the unit tag.
- [raw/main-9a48932f-verdicts.jsonl.gz](raw/main-9a48932f-verdicts.jsonl.gz) — the same for main `9a48932f`: 1980 rows,
  with main's answer and verdict only.
- [raw/D-spark-ansi-true.jsonl.gz](raw/D-spark-ansi-true.jsonl.gz) — Spark 4.1.2's answers for the 16 D cells, ANSI on.
- [raw/D-spark-ansi-false.jsonl.gz](raw/D-spark-ansi-false.jsonl.gz) — the same 16 cells, ANSI off.

Up: [../map.md](../map.md).

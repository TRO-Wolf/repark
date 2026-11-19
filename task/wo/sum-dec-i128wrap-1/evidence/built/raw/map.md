# task/wo/sum-dec-i128wrap-1/evidence/built/raw/ — the head's cells and verdicts

The step 0's probes run on `pkg-sum-a8016e75` (`../../probes/`), compacted the step 0's way (`compact.py` for the main
pass, `trim_revision.py` plus the same scratch-path scrub for the revision). Spark's side is the step 0's
(`../../raw/`).

- `repark-true.jsonl`, `repark-false.jsonl`, `repark-mp-true.jsonl`, `repark-mp-false.jsonl`, `native.jsonl` — the
  head's main pass, multi-partition pass and native door.
- `rev-repark-true.jsonl`, `rev-repark-false.jsonl` — the head's revision cells; `rev-cross-true.jsonl`,
  `rev-cross-false.jsonl` — Spark 4.1.2 reading the head's revision warehouses, per Spark ANSI mode.
- `verdicts.jsonl`, `rev-verdicts.jsonl`, `summary.md` — `analyze.py` and `analyze_revision.py` over the step 0's
  Spark raw and these files.

Up: [../map.md](../map.md).

# task/wo/ice-held-frame-snapshot-1/evidence/raw/ — the cells, the verdicts, the cost samples

- `spark.jsonl.gz`, `main.jsonl.gz`, `base.jsonl.gz` — every cell per engine: one JSON record per cell with `group`,
  `hold`, `write`, `action`, `ansi` and `got` (sorted rows, a count, the printed `show` rows, a sequence, or
  `ERROR <condition>`); groups `HW`, `WA`, `HA`, `P`, `S` (with `C` and `Q`) and `X`, both ANSI modes. Scratch paths in
  error texts read `N` and `S`.
- `verdicts.jsonl.gz` — one record per verdict cell (`X` excluded): the key, the three answers, `v_main`, `v_base`,
  `stored`, `spark_raises`; from `probes/cmp.py`.
- `summary.txt` — `cmp.py`'s counts by build, group and mode, and the mode-dependence count (0).
- `matrix.txt` — `probes/matrix.py`'s matrices over the verdicts (the source of domain table §1 to §5).
- `rev-spark.jsonl.gz`, `rev-main.jsonl.gz`, `rev-base.jsonl.gz` — the revision round's cells (groups `B`, `M`), the
  same record shape; `rev-verdicts.jsonl.gz`, `rev-summary.txt`, `rev-matrix.txt` from `cmp.py` and `matrix.py`;
  `rev-classes.txt` from `classes.py` (domain table §8.1's class table and the unit split).
- No cost samples: the exclusive measure lock was not obtainable in the window ([../../sketch.md](../../sketch.md) §6).

Up: [../map.md](../map.md).

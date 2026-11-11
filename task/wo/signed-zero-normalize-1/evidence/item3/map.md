# map — task/wo/signed-zero-normalize-1/evidence/item3

## Contents

- [moves.md](moves.md) — the after-table moves, p15 against the truth, `UPDATE-SCHEMA-BATCH-MISMATCH-1`, and the moved
  coercion pins.
- [schema_batch_repeat.py](schema_batch_repeat.py) — the 50-repeat `UPDATE-SCHEMA-BATCH-MISMATCH-1` probe.
- [dfc_oracle_check.py](dfc_oracle_check.py) — matches the moved Rust coercion pin's Spark column to the recorded oracle.
- [p15_labels.py](p15_labels.py) — labels the 26 p15 envelope cells against the truth `{4, 6}`, base against head.
- [delta_probe.py](delta_probe.py) — the delta's cells (verifier B1, A3, A1) on Spark (`--engine spark`) or a
  RePark build (`--engine repark`): native grouping sets, `selectExpr` / `F.expr` names, and native `DELETE` /
  `UPDATE` over computed and FLOAT operands with the touched ids.
- `raw/` — the delta legs (`delta-spark.jsonl`, `delta-repark-68b09d18.jsonl`, `delta-repark-head.jsonl`), head legs (`repark-015fda5ae-*.jsonl`), label outputs, repeat results, and `domain-moves.json` (the domain
  label moves that `../item4/cost_a_split.py` reads).

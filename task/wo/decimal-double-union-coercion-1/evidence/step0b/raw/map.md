# task/wo/decimal-double-union-coercion-1/evidence/step0b/raw/ — the small raw sets

Each JSONL file opens with a `provenance` row naming the engine and its loaded library, relative to the scratchpad.
The large raw sets (the fuzz, the structured set and the kernels) stay in the session scratchpad; their counts are
in [../cells/](../cells/map.md).

- `dml-spark-ansi-*.jsonl`, `dml-main-repark-ansi-*.jsonl`, `dml-main-native-ansi-*.jsonl` are step 0's DML cells
  on Spark and on main's two doors.
- `dml-head-<seat_e|k1k2>-<repark|native>-ansi-*.jsonl` are the widened forms on main.
  `dml-head-*-spark-ansi-true.jsonl` are Spark's answers for the same forms.
- `fexpr-spark-ansi-*.jsonl` and `fexpr-main-ansi-*.jsonl` are the twelve `F.expr` / DataFrame column cells.
- `zero-fold-6f9f.jsonl` and `zero-fold-dc6b.jsonl` are the `zero_eq` shapes, each with the plan after each rule.
- `rule-lists-dc6b.json` is main's live analyzer and optimizer lists on both doors.

Up: [../map.md](../map.md).

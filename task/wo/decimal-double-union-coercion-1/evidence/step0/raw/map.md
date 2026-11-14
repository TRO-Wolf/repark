# task/wo/decimal-double-union-coercion-1/evidence/step0/raw/ — the small raw sets

One JSON object per line; the first line of each file is its `provenance` row. The large sets (the domain-table
re-measure, the coercion fuzz, the structured family and the DECIMAL→DOUBLE probe) stay in the session
scratchpad; their joined form is in [../cells/](../cells/map.md).

- `dml-<engine>-ansi-<mode>.jsonl` — the DML cells ([../probes/dml.py](../probes/dml.py)); `<engine>` is `spark`,
  `main-repark`, `main-native`, `ff34-repark` or `ff34-native`.
- `dml-widened-<engine>-ansi-<mode>.jsonl` — the same cells in their widened form (`dml.py … widened`), on
  `ff34baf5` (`ff34-repark`, `ff34-native`) and on Spark (ANSI on, the faithfulness check).
- `fexpr-<engine>-ansi-<mode>.jsonl` — the `F.expr` and DataFrame column cells
  ([../probes/fexpr_extra.py](../probes/fexpr_extra.py)); `<engine>` is `spark`, `main` or `ff34`.

Up: [../map.md](../map.md).

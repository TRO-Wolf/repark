# task/wo/decimal-divide-literal-1/evidence/built/scripts/ — the build's scripts (comment-free Python and shell)

- `record_oracle.py` — records Spark 4.1.2's answers for the pin file's cells (both ANSI modes, default and
  `pickMinimumPrecision=false`) into `decimal_divide_literal_1_spark_oracle.json`.
- `compare.py` — pairs the head's and main's step 0 cells with Spark's using step 0's `classify` (run beside a copy of
  [../../../../avg-decimal-round-1/evidence/probes/](../../../../avg-decimal-round-1/evidence/probes/map.md)).
- `ddl_classes.py` — the transitions, the EQUAL → non-EQUAL and protected-cell checks, and the named classes.
- `ddl_data.py`, `ddl_cost.py`, `run.sh`, `analyze.py` — the cost data, one sample, the alternating runner and the
  table (modelled on AVG-DECIMAL-ROUND-1's).

Up: [../map.md](../map.md).

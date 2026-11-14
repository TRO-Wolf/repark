# task/wo/round-double-exact-1/evidence/step0/probes/ — the step 0 runners

All take their paths as arguments. RePark runs use the lane venv; Spark runs use the oracle interpreter
with `JAVA_HOME` on JDK 17 and the Iceberg 1.11.0 runtime jar.

- `gen_values.py` (with `f32.py`) — writes the value set ([../raw/values.json](../raw/values.json)); `f32.py`
  holds the float32 bit helpers and the correctly rounded rational-to-float32 conversion.
- `jdk_scan.py` — the Spark scan for doubles whose JDK 17 text is not the shortest.
- `run.py` (with `common.py`) — the four doors × five contexts runner for either engine and ANSI mode.
- `misc.py` — the NULL, column, missing, BIGINT and extreme scales, the INT and DECIMAL controls, and the
  card's cells.
- `kcheck.py` — RePark only: step 1's kernel at the `DECIMAL(38,s)` store for s from 0 to 18.
- `model.py` — Spark's rule in Python's `decimal` ([../spark-rule.md](../spark-rule.md)).
- `analyze.py` — the verdicts, the model check and the kernel check.
- `render.py` — renders [../domain-table.md](../domain-table.md) and `../raw/sel-diff.tsv`.

Up: [../map.md](../map.md).

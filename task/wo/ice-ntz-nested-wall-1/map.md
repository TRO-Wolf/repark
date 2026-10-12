# task/wo/ice-ntz-nested-wall-1/ — ICE-NTZ-NESTED-WALL-1 step 0

The measurement for card
[ICE-NTZ-NESTED-WALL-1](../../roadmap/mid-term/ice-ntz-nested-wall-1-card-2026-10-10.md).
Step 0 records what Spark 4.1.2 and main store. It changes no product code.
The ledger is
[ice-ntz-nested-wall-1-ledger.md](../../ledgers/staging/ice-ntz-nested-wall-1-ledger.md).

- [probe.py](probe.py) — the inventory. `run` writes one JSON line per cell.
  `summarize` writes the comparison table. `summary-head` writes the five head
  counts and maps each remaining UTC-wall cell to `SET *` or to the untyped
  literal. The warehouse, the output path and Spark's ivy directory come from
  arguments or the environment.
- [join_round12.py](join_round12.py) — the round 12 joined-head recorder.
  `measure` runs the eight nested-NTZ-target write shapes and the six F1
  statements on one engine in one zone. `summarize` merges head, live Spark
  and main rows into the decision table. Decision: continue, 0 halt rows.
- [wall_first_round15.py](wall_first_round15.py) — the round 15 recorder. `record`
  runs twelve mixed-struct write shapes with the wall operand first and with the
  instant operand first, in both write modes, plus the named cells (depth-2 NOT NULL
  `UPDATE`, a temp view read after a zone change, `EXPLAIN UPDATE` of a zoned struct,
  `EXPLAIN ANALYZE INSERT` of a mixed union) on one engine in one zone. `merge`
  writes the facade pin's expected file from the Spark runs and one main run.
  `compare` counts head cells against main and Spark.
- [evidence/](evidence/map.md) — the engine outputs, the head probe, the five
  counts, and the disk check from before the build.
- [PROGRESS.md](PROGRESS.md) — step, commit, next action.

Up: [../map.md](../map.md).

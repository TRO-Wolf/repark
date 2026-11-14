# task/wo/round-double-exact-1/evidence/fix/ — the build evidence

The build of ROUND-DOUBLE-EXACT-1 after the inventory critic and the orchestrator's step 0 rulings (2026-11-14).

- [fix.md](fix.md) — the seat and files, the red and green runs, the step 0 table on the head against main,
  and the cost table with the halt.
- `moves.py` — compares main's and the head's step 0 output cell by cell against Spark's answer.
- `make_oracle.py` — writes the facade test's Spark oracle from the step 0 raw.
- `cost.py`, `cost_run.sh`, `cost_sample.sh`, `cost_summary.py` — the 10M-row cost probe, the alternating
  harness and its summary table. The scripts read `S`, `BUILD_SLOTS` and `C012_DIR` from the environment.
- [raw/](raw/map.md) — the movement summaries and the cost samples.

Up: [../map.md](../map.md).

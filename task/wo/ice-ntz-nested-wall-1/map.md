# task/wo/ice-ntz-nested-wall-1/ — ICE-NTZ-NESTED-WALL-1 step 0

The measurement for card
[ICE-NTZ-NESTED-WALL-1](../../roadmap/mid-term/ice-ntz-nested-wall-1-card-2026-10-10.md).
Step 0 records what Spark 4.1.2 and main store. It changes no product code.
The ledger is
[ice-ntz-nested-wall-1-ledger.md](../../ledgers/staging/ice-ntz-nested-wall-1-ledger.md).

- [probe.py](probe.py) — the inventory. `run` writes one JSON line per cell.
  `summarize` writes the comparison table. The warehouse, the output path and
  Spark's ivy directory come from arguments or the environment.
- [evidence/](evidence/map.md) — the two engine outputs, the comparison, and the
  disk check from before the build.
- [PROGRESS.md](PROGRESS.md) — step, commit, next action.

Up: [../map.md](../map.md).

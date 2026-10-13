# task/wo/ice-tsns-coercion-1/ — slice 0 measurement

The twin fixture, the Spark lattice and the later censuses for ICE-TSNS-COERCION-1.
No product code. The ledger is
[ice-tsns-coercion-1-ledger.md](../../ledgers/staging/ice-tsns-coercion-1-ledger.md).

- `PROGRESS.md` (untracked; `.gitignore` keeps it local) — the step, the base hash, and the next action.
- [evidence/](evidence/map.md) — disk space before the run, the generated summary, and the slice 1 round 1 gate-open rate.
- [gate_rate_plugin.py](gate_rate_plugin.py) — pytest plugin for the slice 1 round 1 rate. It snapshots `_native._tsns_gate_counts()` around every facade item and writes the run JSON. Not product code.
- [interleaved_cost.py](interleaved_cost.py) — one-set cost driver for the round 1 cost follow-up. Loads the slice-0 timing script's own setup and statements, runs a named subset for one label, and writes the set JSON with counter snapshots. Not product code. Slice 1 fold 2 (2026-10-13) added two ungated open-gate statements, selected only through `--statements`: `nest8` (a `coalesce(…, NULL)` nested eight deep over `timestamp_ns`) and `union200` (one nanosecond branch and 200 NULL branches).

Up: [../map.md](../map.md).

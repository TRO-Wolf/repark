# map — python/repark-parity/bench/spill

## Purpose

**H3-SPILL-1** — the Never-OOM truth table. For every operator the engine can plan, under a
bounded `FairSpillPool`, does it spill, degrade, fail cleanly, or take the process down? The
harness measures; it changes no product code.

Why the shape it has:

- **One subprocess per cell.** Allocator state, the DataFusion `RuntimeEnv` and the tokio
  runtime all outlive a session inside one process, so a second cell in the same process is
  measuring the first cell's arena. Every cell is a fresh `python -m spill.cell_worker`.
- **An address-space cap per cell.** `RLIMIT_AS` is set in the worker before repark is
  imported. An operator whose memory is not pool-accounted climbs until the cap kills it —
  recorded as `abort_at_cap`, never as a dead box. The cap is a knob (`--as-cap-bytes`,
  default 12 GiB) and is written into every record, because an outcome that depends on a
  limit is meaningless without the limit.
- **Peak RSS is read from `/proc/<pid>/status` `VmHWM` by the parent**, polled at 50 ms.
  `VmHWM` is a kernel high-water mark, so polling cannot miss a peak that happened between
  reads, and a worker that aborts still leaves its number behind. A surviving worker also
  reports `ru_maxrss`; the record takes the larger.
- **Outcome comes from metrics, not from wall time.** `EXPLAIN ANALYZE` gives
  `spill_count` / `spilled_bytes` / `skipped_aggregation_rows` per physical operator, which
  are deterministic at a chosen pool; wall is recorded beside them but never asserted on.
- **The answer probe is a second small-output query.** Each roster row carries a
  `digest_sql` whose result is a handful of rows (counts, checksums, a sort-inversion count),
  so the wrong-answer check does not itself need the memory the cell is measuring. The driver
  compares every bounded cell's digest against the unbounded (`pool=none`) run at the same
  scale, and re-labels a mismatch `wrong`.

## Contents

- `roster.py` — the operator rows, the pool and scale axes, the wide base projection
  (~120 varied bytes/row, so 1e7 rows exceed 1 GiB).
- `cell_worker.py` — one cell: cap, session, view, `EXPLAIN ANALYZE`, digest, JSON out.
  Facade and Iceberg rows (`collect`, `toPandas`, `dynamicFlatten`, the DV scan, the MERGE
  staging join) run as `api` cells, which have no plan metrics by construction.
- `plan_metrics.py` — `EXPLAIN ANALYZE` text to per-operator-class counter totals.
- `measure.py` — the driver: plan, spawn, poll, classify, repeat non-deterministic cells,
  and rewrite the report after every cell so a crash costs one cell.
- `models.py` — the pydantic records the report is made of.
- `map.md` — this file.

pins: h3-spill-1/C-001, C-002, C-003

## I want to…

| I want to… | Go to |
|---|---|
| Run the whole matrix | `measure.py --scratch <dir> --json-out <file>` |
| Run one operator row | `measure.py --operators sort --scratch <dir> --json-out <file>` |
| Run one cell by hand | `cell_worker.py --operator sort --pool 64M --scale 1000000 --as-cap-bytes 12884901888 --json-out <file>` |
| Read the measured matrix | `docs/perf/spill-matrix-baseline.md` (lands with the run) |
| Read the pins | `python/repark/tests/test_h3_spill_matrix.py` (lands with the run) |

## Debug

| Symptom | Check |
|---|---|
| Every cell `abort_at_cap` at once | `--as-cap-bytes` too small for the tokio thread stacks; raise it and re-record |
| `spill_count` always 0 under a small pool | the operator has no spill path in DataFusion 54.1 — that is the finding, not a bug in the cell |
| A cell outlives `--cell-timeout-s` | the worker is killed and recorded `abort`; raise the timeout rather than reading the kill as a defect |

## Pointers

- Up: [../map.md](../map.md)

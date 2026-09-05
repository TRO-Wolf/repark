# map — python/repark-parity/bench/spill

## Purpose

**H3-SPILL-1** — the Never-OOM truth table. For every operator the engine can plan, under a
bounded `FairSpillPool`, does it spill, degrade, fail cleanly, or take the process down? The
harness measures; it changes no product code.

Why the shape it has:

- **One subprocess per cell.** Allocator state, the DataFusion `RuntimeEnv` and the tokio
  runtime all outlive a session inside one process, so a second cell in the same process is
  measuring the first cell's arena. Every cell is a fresh `python -m spill.cell_worker`.
- **A resident-memory watchdog per cell, and an address-space backstop behind it.**
  `RLIMIT_AS` alone is the wrong instrument here and the first draft of this harness proved
  it: a worker that has merely built a session and counted a view already reserves **8.2 GiB
  of virtual address space** against 183 MiB resident (64 tokio worker stacks, arrow and
  jemalloc arenas), and at a 4 GiB cap `import pyarrow` itself fails to map its own shared
  objects. A 12 GiB `RLIMIT_AS` therefore left ~3 GiB of usable headroom and killed cells for
  reasons that had nothing to do with the operator. So the real guard is the parent: it polls
  `VmHWM` and kills a worker that passes `--rss-cap-bytes` (default 8 GiB), recording
  `abort_at_cap`. `--as-cap-bytes` (default 32 GiB) stays as a backstop against a pathological
  virtual allocation. Both caps are written into every record, because an outcome that depends
  on a limit is meaningless without the limit.
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
- **Every digest is order-independent or order-forcing, or it is not a digest.** Two traps
  cost this unit five false `wrong` cells before they were caught. `lag(h) OVER ()` over a
  sorted subquery does not see a sorted stream — the optimizer drops a sort nothing depends
  on, so the inversion count came back ~500,004 and varied run to run at a *fixed* pool. The
  probe now says `lag(h) OVER (ORDER BY h)`, which makes the sort load-bearing. And a `double`
  sum over 1e7 rows is order-dependent in its last bits, so every float checksum is
  `sum(cast(s AS bigint))`: integer addition is associative, and `v = id * 1.5` keeps each
  per-row value exact in a double.
- **A caught Rust panic is its own outcome (`internal_error`), never `error` and never
  `clean_error`.** The distinction is the whole point of the row: a bounded pool that answers
  with a typed refusal is Never-OOM working; one that answers with a panic caught at the
  Python boundary is not.

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
- `report.py` — the outcome matrix and numbers tables the baseline doc carries.
- `spark_cells.py` — the two or three Apache Spark comparison cells on the same fixture
  under a bounded `--driver-memory`, with spill read out of the Spark event log.
- `map.md` — this file.

pins: h3-spill-1/C-001, C-002, C-003

## I want to…

| I want to… | Go to |
|---|---|
| Run the whole matrix | `measure.py --scratch <dir> --json-out <file>` |
| Run one operator row | `measure.py --operators sort --scratch <dir> --json-out <file>` |
| Run one cell by hand | `cell_worker.py --operator sort --pool 64M --scale 1000000 --as-cap-bytes 34359738368 --json-out <file>` |
| Read the measured matrix | [../../../../docs/perf/spill-matrix-baseline.md](../../../../docs/perf/spill-matrix-baseline.md) |
| Read the pins | [../../../repark/tests/test_h3_spill_matrix.py](../../../repark/tests/test_h3_spill_matrix.py) |

## Debug

| Symptom | Check |
|---|---|
| Every cell `abort_at_cap` at once | `--rss-cap-bytes` below the fixture's own footprint; raise it and re-record |
| `ImportError: failed to map segment` | `--as-cap-bytes` under the ~8 GiB baseline virtual footprint — that is the floor, not a finding |
| `spill_count` always 0 under a small pool | the operator has no spill path in DataFusion 54.1 — that is the finding, not a bug in the cell |
| A cell outlives `--cell-timeout-s` | the worker is killed and recorded `abort`; raise the timeout rather than reading the kill as a defect |

## Pointers

- Up: [../map.md](../map.md)

# map — python/repark-parity/bench/mw7

## Purpose

**MW-7 scale measurement** — what a partitioned Iceberg v2 table costs as MERGEs
accumulate, and what the maintenance sequence gives back. Measurement only: nothing here
changes engine behaviour, and no fix belongs in this directory.

Local filesystem and a memory catalog. Never AWS, never `REPARK_*` acceptance envs.
The generator is checked in; the data it makes never is (PROJECT.md, torture-dataset rule).

## Contents

- `measure.py` — the driver. Pydantic v2 records (`ScanTiming`, `FileCensus`, `Checkpoint`,
  `MaintenanceStep`, `LegResult`, `RunResult`), the polars seed and per-MERGE source
  frames, the census over `files` / `manifests` / `snapshots`, the timed scan battery, the
  five-procedure maintenance sequence, and `run_scale_measurement` which drives one leg per
  write mode in ONE process (peak RSS is process-wide).
- `run_mw7.py` — CLI. `--rows`, `--merges`, `--partitions`, `--touch-fraction`,
  `--checkpoint-every`, `--reps`, `--target-file-size-bytes`, `--modes`, `--scratch`,
  `--out`, `--project-to` (the charter's feasibility projection).
- `__init__.py` — package marker; the CI pin imports `mw7.measure` through it.
- `map.md` — this file.

## The three scan probes, and why each exists

| Probe | SQL shape | What it answers |
|---|---|---|
| `count_star` | `COUNT(*)` | the MW-0/MW-5 continuity probe — the answer must never move |
| `predicate_partition` | one partition, `value >= 500` | the charter's fixed predicate scan |
| `predicate_point` | a 2,000-id window | prunes to a few data files, but reads EVERY delete file in the partitions it touches — the MW-9 probe |

Both predicates aggregate the integer `quantity`. Summing the float `value` moved the
answer by one ULP across `rewrite_data_files`, because compaction re-groups rows and float
addition is order-dependent. That is correct engine behaviour; an integer sum makes the
before/after identity check exact.

## I want to…

| I want to… | Go to |
|---|---|
| Reproduce the MW-7 calibration | `run_mw7.py --rows 1000000 --merges 10 --partitions 8 --checkpoint-every 1 --reps 7 --target-file-size-bytes 4194304 --scratch <dir>` |
| Reproduce the MW-7 full run | `run_mw7.py --rows 10000000 --merges 50 --partitions 8 --checkpoint-every 10 --reps 7 --target-file-size-bytes 4194304 --scratch <dir>` |
| Project a calibration onto a bigger run | add `--project-to 10000000:100` |
| Read the numbers | [../../../../task/ledgers/completed/mw-7-scale-measurement-ledger.md](../../../../task/ledgers/completed/mw-7-scale-measurement-ledger.md) |
| Run the CI pin on this machinery | `python/repark/tests/test_mw7_scale_smoke.py` |

## Constraints

- Measure only. An engine defect this harness surfaces is an OPEN finding in the ledger
  plus a registry candidate, never a fix here.
- Never commit a scratch tree, a warehouse, or a result JSON.
- Wall-clock is one machine's number. Ratios are the deliverable; nothing here is a CI pin.
- `--target-file-size-bytes` is load-bearing at these row counts: leave it at the engine
  default and a 1e7-row table writes one data file per partition, which gives the
  delete-file layout nothing to attach to.

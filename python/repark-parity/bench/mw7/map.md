# map — python/repark-parity/bench/mw7

CC-2 slice complete: comments and docstrings condensed; oracle discriminators, pins, mutation payloads, and safety contracts kept byte-exact; history narration deleted.

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
  write mode in ONE process (peak RSS is process-wide). **MW-9:** MOR CTAS sets
  `write.delete.granularity = 'partition'` so the recorded arithmetic stays the MW-7
  measurement (Spark's unset default is now `file`).
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

## Reading the MOR-minus-COW gap (2026-08-24, Critic remediation)

The copy-on-write leg writes no delete files, so it is the control — but the gap between the
legs is **not** a delete-file cost alone, and `measure.py`'s module docstring says so. Every
MERGE on the merge-on-read leg APPENDS the updated rows instead of rewriting in place, so at
1e7 × 50 that leg also carried **16.3× the control's data files** and **1.83× its live bytes**.
The gap is the delete files plus that fan-out. Separating them needs a third leg that compacts
the deletes at every checkpoint; this driver does not run one.

## I want to…

| I want to… | Go to |
|---|---|
| Reproduce the MW-7 calibration | `run_mw7.py --rows 1000000 --merges 10 --partitions 8 --checkpoint-every 1 --reps 7 --target-file-size-bytes 4194304 --scratch <dir>` |
| Reproduce the MW-7 full run | `run_mw7.py --rows 10000000 --merges 50 --partitions 8 --checkpoint-every 10 --reps 7 --target-file-size-bytes 4194304 --scratch <dir>` |
| Project a calibration onto a bigger run | add `--project-to 10000000:100` |
| Read the numbers | [../../../../task/ledgers/completed/mw-7-scale-measurement-ledger.md](../../../../task/ledgers/archive/2026-08/2026-08-24-mw-7-scale-measurement-ledger.md) |
| Run the CI pin on this machinery | `python/repark/tests/test_mw7_scale_smoke.py` |
| See how the runbook reclaims a delete-laden file, and the one shape it still cannot | registry row `RDF-1`; pin `test_mw7_scale_smoke.py::test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies` (C-011, flipped 2026-09-02); the residue is a delete file naming two or more data files, fork ask F-16 |

## RDF-1 measurement (2026-09-02)

This module's C-011 shape is what measured RDF-1 on both engines: 2,500 rows, `mor`,
`write.delete.granularity = 'partition'`, `write.target-file-size-bytes` 64 KiB, one MERGE
deleting every seeded row.

| Engine | `file_path` bounds (field `2147483546`) | `rewrite_data_files` | after the five-step sequence |
|---|---|---|---|
| RePark before | absent (parquet stats truncated at 64 B) | rewritten 4, `removed_delete_files_count` 0 | 3 data files, 1 delete file, 2,500 delete records |
| RePark after | exact; lower == upper == the 103-byte seeded path | rewritten 5, `removed_delete_files_count` 1 | 2 data files, 0 delete files, 0 delete records, 2,500 rows |
| PySpark 4.1.2 + Iceberg 1.11.0 | exact; lower == upper (both granularities) | rewritten 3, `removed_delete_files_count` 0 | 1 data file, 1 dangling delete file, 2,500 delete records, 2,500 rows |

pins: rdf-1-position-delete-bounds/C-001

## Constraints

- Measure only. An engine defect this harness surfaces is an OPEN finding in the ledger
  plus a registry candidate, never a fix here.
- Never commit a scratch tree, a warehouse, or a result JSON.
- Wall-clock is one machine's number. Ratios are the deliverable; nothing here is a CI pin.
- `--target-file-size-bytes` is load-bearing at these row counts: leave it at the engine
  default and a 1e7-row table writes one data file per partition, which gives the
  delete-file layout nothing to attach to.

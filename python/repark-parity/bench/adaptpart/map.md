# map — python/repark-parity/bench/adaptpart

## Purpose

**AP-0 (measure)** — the ADAPT-PART analysis engine's measurement bed. One script builds
three local Iceberg beds, reads each bed's `files` and `partitions` metadata tables through
RePark's own SQL, generates the P-2 candidate partition specs, scores them with the P-3
target-band model, and prints one ranked table per bed. Measurement only: nothing here
changes engine behaviour, and no fix belongs in this directory.

Local filesystem and a memory catalog. Never AWS. The generator is checked in; the data it
makes never is — beds live under the `--scratch` root the caller names.

## Contents

- `run_adaptpart.py` — the whole unit in one command: the futures CTAS bed, the two
  generated synthetic beds (uniform and skewed schedules over one seed parquet, one
  append per batch so the writer lands 200+ narrow files), the manifest-first reader
  (`files` rows plus `readable_metrics` lower/upper bounds, `partitions` census),
  the bounded head sample for string/int distinct counts (bounds-proven constants take
  no sample), P-2 generation (truncate grains for temporal columns, identity at or
  below 1 000 sampled distinct values, `bucket(N)` above it, plus no-partitioning,
  plus the three two-field pairs from the top three single fields), P-3 scoring (a
  file spanning k values contributes 1/k to each; the 0.25x/4x target band; projected
  partition and file counts at the target size), and the ranked printer. Pydantic v2
  records throughout (`BedReport`, `CandidateScore`, `SampleNote`); one-line
  docstrings; no nested defs.
  pins: ap-0/C-001, C-002, C-003, C-004
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Reproduce every row of the AP-0 document | `.venv/bin/python python/repark-parity/bench/adaptpart/run_adaptpart.py --scratch /tmp/ap0-bed` on a fresh scratch root |
| Read the measured numbers | [docs/perf/ap-0-partition-candidates-2026-09-10.md](../../../../docs/perf/ap-0-partition-candidates-2026-09-10.md) |
| Read the clause table | [task/ledgers/staging/ap-0-ledger.md](../../../../task/ledgers/staging/ap-0-ledger.md) |
| Run smaller (fewer batches, fewer rows per batch) | `--batches N --batch-rows M` (the document's rows need the defaults) |
| Change the P-3 target or the sample size | `--target-file-size-bytes B --sample-rows N` (the document's rows need 524288 and 200000) |

## Constraints

Every bed is unpartitioned, so the `partitions` table carries the aggregate census row
with no `partition` column (fork 194 Java parity) and every projection comes from file
bound ranges. Bucket candidates assume a uniform hash spread and two-field candidates a
per-file cross-product spread; both assumptions are stated in the document, not in the
rankings. The 20 percent prediction check needs a real rewrite and lives outside this
directory as the orchestrator's O-run.

## Pointers

- The numbers this script produces:
  [docs/perf/ap-0-partition-candidates-2026-09-10.md](../../../../docs/perf/ap-0-partition-candidates-2026-09-10.md).
- The clause table: [task/ledgers/staging/ap-0-ledger.md](../../../../task/ledgers/staging/ap-0-ledger.md).
- Up: [bench/map.md](../map.md).

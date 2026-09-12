# AP-1 RP-17 re-measure — `plan_partitioning` with compaction under one codec (2026-09-12)

ADAPT-PART residue AP-1-R-001 third re-measure on the RP-17 fork pin
(`41e25ba2`, fork `#278` F-WRITE-COMPRESS-2). Run 8's re-measure
([adapt-part-ap1-remeasure-2026-09-11.md](adapt-part-ap1-remeasure-2026-09-11.md))
found `rewrite_data_files` writing UNCOMPRESSED live files (uniform 7 928 680,
skewed 7 672 169) off zstd inputs, so the projection and the actual were measured
under different codecs. `#278` makes the maintenance, COW/MoR rewrite and
position-delete writers honour `write.parquet.compression-codec` (default zstd),
closing residue sites F-WRITE-COMPRESS-1-R-001…R-004. This round rebuilds the
three AP-0 beds with run 8's exact commands, re-runs `CALL plan_partitioning`,
re-runs the `identity(grp)` rewrite on fresh copies, and reads the 20 percent
check against live actuals whose footers are now zstd. No Rust or Python source
changed.

Environment: Linux 6.8.0-138-generic, Linux Mint 22.2, 64 threads, release
`maturin develop --release` module at commit `3a813537` (the pin bump) with
`repark._native.__debug_assertions__ == False` verified before any measurement.
1-minute load 14.46 at bed-run start. No `java` PID on the box; this session did
not start a JVM and did not set `REPARK_PARITY_LIVE`. The beds live under
`/tmp/ap1r-bed` and the rewrite copies under `/tmp/ap1r-orun` (never committed).
The futures source is `/tmp/ap1r-src/test_futures.parquet`, md5-identical to
`~/CodeRepos/myTemp/reTest/test_futures.parquet` (the AP-0 documented source).

Build walls on this run: futures CTAS 0.3 s, uniform 15.5 s, skewed 22.3 s;
rewrite-copy builds 14.6 s each.

**Closing note (AP-1-CLOSE-1, 2026-09-12):** the 20 % check below is retired —
S2-27 rules AP-1-R-001 closed as an estimator property:
`projected_files_at_target` is an upper bound from the inputs' compressed
bytes (a same-codec rewrite into fewer, larger files does not compress worse),
pinned on the three beds with the candidate ranking unchanged. This document's
measurements stand; its residue verdict is superseded by the closing errata at
the top of
[task/ledgers/completed/ap-1-ledger.md](../../task/ledgers/completed/ap-1-ledger.md).

## Measured codecs and byte ratios

Independent recompute with `pyarrow.parquet` over
`/tmp/ap1r-bed/warehouse/repark_ctas/ap/ns/<bed>/data/*.parquet`, summing
`total_compressed_size` / `total_uncompressed_size` across every row group and
column. Every column chunk on every bed reads **ZSTD** — cell for cell run 8's
bed numbers.

| Bed | Codec | Files | On-disk bytes (Iceberg `file_size`) | Σ compressed | Σ uncompressed | byte_ratio | Frame says | Run 8 |
|---|---|---|---|---|---|---|---|---|
| futures | zstd (CTAS, unchanged) | 3 | 26 729 684 | 26 670 018 | 57 643 097 | 0.462675 | `byte_ratio=0.46 (footers)` | identical |
| uniform | zstd (`INSERT`) | 206 | 3 074 844 | 2 831 692 | 7 529 566 | 0.376076 | `byte_ratio=0.38 (footers)` | identical |
| skewed | zstd (`INSERT`) | 206 | 3 074 844 | 2 831 692 | 7 529 566 | 0.376076 | `byte_ratio=0.38 (footers)` | identical |

The 0.55 fallback never fired.

## futures — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.futures', target_file_size_bytes =>
524288)` — verbatim top three (all columns):

```text
rank 1
  candidate: months(event_date)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 57f5666e4136ff8d
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 2
  candidate: months(event_date_utc)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date_utc)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: fa797bbf23fb9d1b
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 3
  candidate: months(event_timestamp)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_timestamp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 7d0d3ad7bb3d2b27
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
```

Same shape as run 8: the months() block leads at 36 projected files,
`unpartitioned` projects 24. No rewrite was run on this bed (no `identity(grp)`
column), so no actual exists to compare.

## uniform — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.uniform', target_file_size_bytes =>
524288)` — 10 rows, verbatim top three:

```text
rank 1
  candidate: years(ts)
  score: 0.0
  projected_partitions: 2
  projected_files_at_target: 4
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD years(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: f38de065a2f84e67
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 2
  candidate: identity(grp)
  score: 0.0
  projected_partitions: 20
  projected_files_at_target: 20
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD identity(grp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 89a8ec51cb9bb23d
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 3
  candidate: unpartitioned
  score: 0.4661998748779297
  projected_partitions: 1
  projected_files_at_target: 3
  ddl: 
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 688de3b676ba9835
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
```

Identical ranking to run 8. The 20 percent check still uses `identity(grp)` —
rank 2 here, 20 partitions, 20 projected files.

## skewed — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.skewed', target_file_size_bytes =>
524288)` — 10 rows, verbatim top three:

```text
rank 1
  candidate: years(ts)
  score: 0.0
  projected_partitions: 2
  projected_files_at_target: 4
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD years(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: ead03c4be9f31557
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 2
  candidate: unpartitioned
  score: 0.4661998748779297
  projected_partitions: 1
  projected_files_at_target: 3
  ddl: 
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: b499c35ae5dc3a71
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 3
  candidate: months(ts)
  score: 0.8349990844726562
  projected_partitions: 24
  projected_files_at_target: 24
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD months(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: aa4c0d6fd1a0dc91
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
```

`identity(grp)` is again rank 4: score 7.2718505859375, 20 partitions, 21
projected files — cell for cell run 8.

## The 20 percent check — projection vs live rewrite actuals under one codec

The rewrite re-ran on fresh copies (`uniform_orun`, `skewed_orun`) built from the
same seeds. Pre-rewrite each copy matched the plan beds cell for cell: 206
files, 3 074 844 bytes, footer codec ZSTD, ratio 0.376076. Statements, verbatim:

```text
ALTER TABLE ap.ns.<bed>_orun ADD PARTITION FIELD identity(grp)
CALL ap.system.rewrite_data_files(table => 'ns.<bed>_orun')
```

`run_adaptpart.py` still has no rewrite flag; the driver was a throwaway
`/tmp/ap1r-orun.py` (not committed) against the same release module. CALL
frames, both beds: `rewritten_data_files_count=206`,
`added_data_files_count=20`, `rewritten_bytes_count=3074844`,
`failed_data_files_count=0`, `removed_delete_files_count=0`. Live data-file
bytes are the current-snapshot `files` metadata sum of `file_size_in_bytes`
where `content = 0`; the live files are the 20 parquet files under the new
`data/grp=<value>/` partition directories, whose sizes sum to the metadata total
exactly (the 206 superseded inputs remain at `data/` top level until expire).

**The live output footers are now zstd** — every column chunk of all 20 files on
both beds reads `ZSTD` (asserted per chunk, not sampled). One footer line,
verbatim, per bed:

```text
uniform_orun compacted-00006-01a09473-96a6-7fc2-b577-2d7702c3ffb5.parquet rg0 col0 codec=ZSTD compressed=160259 uncompressed=197592
skewed_orun  compacted-00013-01a09473-d422-7a03-9681-79b03d6423ed.parquet rg0 col0 codec=ZSTD compressed=79938 uncompressed=97572
```

Run 8's mechanism (uncompressed compaction output) is gone. The outputs'
own footer ratios are 0.562968 (uniform) and 0.537664 (skewed) — larger than the
inputs' 0.376076, because 206 two-thousand-row files chunk differently than 20
four-hundred-thousand-row files.

| Bed | Stored bytes (Σ `file_size`) | Σ uncompressed | byte_ratio | Proj (stored × ratio) | Proj (uncompressed × ratio) | Live actual | Δ stored-proj | Δ uncompressed-proj |
|---|---|---|---|---|---|---|---|---|
| futures | 26 729 684 | 57 643 097 | 0.462675 | 12 367 156 | 26 670 018 | n/a (no rewrite) | n/a | n/a |
| uniform | 3 074 844 | 7 529 566 | 0.376076 | 1 156 376 | 2 831 692 | **4 474 081** (20 files, zstd) | **−74.2 %** | −36.7 % |
| skewed | 3 074 844 | 7 529 566 | 0.376076 | 1 156 376 | 2 831 692 | **4 136 632** (20 files, zstd) | **−72.0 %** | −31.5 % |

Post-rewrite layout: 20 live files, one per `grp` value. Uniform min 219 186 /
median 223 781 / max 226 767. Skewed min 107 283 / median 108 511 / max
1 968 321. `rewrite_data_files` still carries no `target-file-size-bytes`; the
`g00` file did not split at 512 KiB (projected_files_at_target was 21 on
skewed). No timing is claimed.

## What this means for AP-1-R-001 — verdict as measured (D-2)

The check the residue names is `Σ file_size × byte_ratio` against the live
rewrite actual. Under one codec it now reads **−74.2 % uniform, −72.0 % skewed**
— still outside 20 % on both beds. **AP-1-R-001 stays OPEN.** The gap narrowed
from −85.4 % / −84.9 % only because the actual shrank (uncompressed 7.93 MB /
7.67 MB → zstd 4.47 MB / 4.14 MB); the projection's stored×ratio side is
unchanged at 1 156 376.

For the orchestrator's deferred Q-1 ruling (D-3, not decided here): the
alternative projection `Σ uncompressed × byte_ratio` — which equals the footer
Σ compressed, 2 831 692 — reads −36.7 % / −31.5 % against the same actuals,
closer but still outside 20 %, because the rewrite's own output ratio
(0.563 / 0.538) is not the inputs' stored ratio (0.376). Both projections sit in
the table above; the formula was not tuned (D-4).

## Reproduce

```text
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release && cd ../..
rm -rf /tmp/ap1r-bed /tmp/ap1r-orun
.venv/bin/python python/repark-parity/bench/adaptpart/run_adaptpart.py \
  --scratch /tmp/ap1r-bed --futures-parquet /tmp/ap1r-src/test_futures.parquet --plan
```

`--plan` issues the CALL per bed after the AP-0 scoring tables print and dumps
every frame row field by field; the blocks above are that output verbatim. The
independent ratio check is `pyarrow.parquet` over the warehouse path named
above. The rewrite leg is the two statements in the 20 percent section, on a
fresh INSERT-grown copy per synthetic bed (scratch `/tmp/ap1r-orun`); the live
files are the `data/grp=*/` parquet files whose sizes sum to the
current-snapshot `files` total. Related:
[adapt-part-ap1-2026-09-11.md](adapt-part-ap1-2026-09-11.md);
[adapt-part-ap1-remeasure-2026-09-11.md](adapt-part-ap1-remeasure-2026-09-11.md);
[ap-0-partition-candidates-2026-09-10.md](ap-0-partition-candidates-2026-09-10.md);
ledgers `task/ledgers/staging/rp-17-ledger.md` and
`task/ledgers/completed/ap-1-remeasure-ledger.md`.

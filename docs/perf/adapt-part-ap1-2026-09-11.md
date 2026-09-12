# AP-1 step 2 — `plan_partitioning` on the AP-0 beds (2026-09-11)

ADAPT-PART unit AP-1 step 2 (measure + document): run `CALL
ap.system.plan_partitioning(table => 'ns.<bed>', target_file_size_bytes => 524288)`
against the three AP-0 beds on the release module, record the top three frame rows
per bed and the footer-measured `byte_ratio`, and read the projection against AP-0's
O-run actuals. Step 2's change under measurement: `projected_files_at_target` now
folds each partition value's byte share by `byte_ratio` — Σ
`total_compressed_size` / Σ `total_uncompressed_size` over every column chunk of
every live data file's parquet footer, read as ranged metadata through the table's
`FileIO`, never row-group data — instead of projecting the raw pre-rewrite sum.

Environment: Linux 6.8.0-138-generic, release `maturin develop --release` module at
commit `094fc75b`, 1-minute load 2.85 during the run. The beds are the AP-0 builds,
rebuilt fresh under `/tmp/ap1-bed` (never committed): futures — 3 files,
26 729 684 bytes, CTAS-written; uniform — 206 files, 7 773 590 bytes, INSERT-grown;
skewed — 206 files, 7 773 590 bytes, INSERT-grown.

## Measured byte ratios

The ratio in each frame's `notes` (`byte_ratio=<r> (footers)`) recomputed
independently with pyarrow over the bed's on-disk parquet files:

| Bed | Codec the files carry | Σ compressed | Σ uncompressed | byte_ratio | Frame says |
|---|---|---|---|---|---|
| futures | zstd (CTAS path, `writer_properties_for`) | 26 670 018 | 57 643 097 | 0.462675 | `byte_ratio=0.46 (footers)` |
| uniform | uncompressed (`INSERT` via the fork's task writer) | 7 529 966 | 7 529 966 | 1.000000 | `byte_ratio=1.00 (footers)` |
| skewed | uncompressed (same) | 7 529 966 | 7 529 966 | 1.000000 | `byte_ratio=1.00 (footers)` |

The codec split is a write-path fact, not a data property: `INSERT INTO` passes
through the fork's `iceberg-datafusion` task writer at parquet-rs's uncompressed
default, while CTAS goes through `repark-iceberg`'s `writer_properties_for`
(default zstd). On an INSERT-grown table the footers measure 1.0 and the step-2
correction is a no-op — that is what the synthetic beds show below.

## futures — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.futures', target_file_size_bytes =>
524288)` — 39 rows, verbatim top three (all columns):

```text
rank 1
  candidate: months(event_date)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 46c4d534690ee2b3
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 2
  candidate: months(event_date_utc)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date_utc)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 091e2c4e8e6202cc
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 3
  candidate: months(event_timestamp)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_timestamp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: da0d79f2ed8d8406
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
```

Against AP-0: the same six `months()` columns still score 0.0 over 36 partitions,
but their projected file count drops from 72 to 36 (each month carries ~743 KB raw,
~343 KB at 0.462675), so the months block now out-ranks the `bucket(16, *)` row set
on the projected-count tie-break — AP-0's leader `bucket(16, down_ticks)` stays at
64 projected files. `unpartitioned` projects 24 files
(`26 729 684 × 0.462675 / 524 288 → 23.59 → 24`) where AP-0's raw sum projected 51.
AP-0 ran no rewrite on this bed, so no actual exists to compare.

## uniform — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.uniform', target_file_size_bytes =>
524288)` — 10 rows, verbatim top three:

```text
rank 1
  candidate: identity(grp)
  score: 0.0
  projected_partitions: 20
  projected_files_at_target: 20
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD identity(grp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 82e4b89f197db95e
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=1.00 (footers)
rank 2
  candidate: months(ts)
  score: 0.0
  projected_partitions: 24
  projected_files_at_target: 24
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD months(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 0d03473f27043f89
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=1.00 (footers)
rank 3
  candidate: years(ts)
  score: 1.7067365646362305
  projected_partitions: 2
  projected_files_at_target: 16
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD years(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: fbe45570967968f1
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=1.00 (footers)
```

The frame is cell-for-cell AP-0's ranked table, because `byte_ratio=1.00` makes the
step-2 correction a no-op on uncompressed inputs.

## skewed — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.skewed', target_file_size_bytes =>
524288)` — 10 rows, verbatim top three:

```text
rank 1
  candidate: months(ts)
  score: 0.0
  projected_partitions: 24
  projected_files_at_target: 24
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD months(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: 7305be44f4353697
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=1.00 (footers)
rank 2
  candidate: identity(grp)
  score: 0.8533515930175781
  projected_partitions: 20
  projected_files_at_target: 27
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD identity(grp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: 3cd1edbdf884a931
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=1.00 (footers)
rank 3
  candidate: years(ts)
  score: 1.7067365646362305
  projected_partitions: 2
  projected_files_at_target: 16
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD years(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: af52c4cc8c9ff47b
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=1.00 (footers)
```

Likewise identical to AP-0's table.

## The 20 percent check — residue AP-1-R-001

AP-0's O-run rewrote `identity(grp)` on the two synthetic beds and measured actual
post-rewrite bytes. The step-2 projection applied to the same candidate:

| Bed | Projected bytes (`Σ file_size × byte_ratio`) | O-run actual bytes | Byte error | AP-0's error |
|---|---|---|---|---|
| uniform | 7 773 590 × 1.00 = 7 773 590 | 4 413 222 | **+76.2 %** | +76.2 % |
| skewed | 7 773 590 × 1.00 = 7 773 590 | 4 134 457 | **+88.0 %** | +88.0 % |

**The projection is still more than 20 percent wrong on both beds — residue
AP-1-R-001 is filed in the ledger.** The measurement wins; the formula was not
tuned.

The miss has a mechanism worth naming. The O-run rewrite landed zstd files at
0.53–0.57× the input bytes, but these beds' input footers measure 1.0 because their
files are uncompressed — `byte_ratio` measures the *input* codec's stored ratio,
and on an uncompressed table it cannot predict the shrink a codec-changing rewrite
produces. Where input and rewrite share the codec (the futures bed's zstd files, or
any table grown through the CTAS/`rewrite_data_files` path), the footer ratio is a
real signal: 0.46 on futures turns the projection from 51 raw-sum files into 24.
The fallback constant 0.55 only fires on unreadable footers; it never fired on
these beds. A projection that accounts for the *rewrite's* codec — not just the
input's — is the follow-up this residue points at; it is AP-1's out-of-scope call,
recorded for the card's reader, not decided here.

RP-16 re-measure (2026-09-11): the INSERT path now writes zstd (fork `#276`); the same 20 percent check on rebuilt beds is in [adapt-part-ap1-remeasure-2026-09-11.md](adapt-part-ap1-remeasure-2026-09-11.md).

RP-17 re-measure (2026-09-12): the rewrite writers now carry the codec too (fork `#278`); the third run's live actuals are zstd and the check reads −74.2 % / −72.0 % — AP-1-R-001 still OPEN. [adapt-part-ap1-remeasure-2-2026-09-12.md](adapt-part-ap1-remeasure-2-2026-09-12.md).

## Reproduce

```text
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release && cd ../..
rm -rf /tmp/ap1-bed
.venv/bin/python python/repark-parity/bench/adaptpart/run_adaptpart.py \
  --scratch /tmp/ap1-bed --futures-parquet /tmp/ap1-src/test_futures.parquet --plan
```

`--plan` issues the CALL per bed after the AP-0 scoring tables print and dumps
every frame row field by field; the blocks above are that output verbatim. The
independent ratio check is `pyarrow.parquet` over
`/tmp/ap1-bed/warehouse/repark_ctas/ap/ns/<bed>/data/*.parquet`, summing
`total_compressed_size` / `total_uncompressed_size` across row groups and columns.
Related: [ap-0-partition-candidates-2026-09-10.md](ap-0-partition-candidates-2026-09-10.md);
ledger `task/ledgers/staging/ap-1-ledger.md`.

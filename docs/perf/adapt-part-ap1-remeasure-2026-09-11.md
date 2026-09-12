# AP-1 RP-16 re-measure — `plan_partitioning` after INSERT zstd (2026-09-11)

ADAPT-PART residue AP-1-R-001 re-measure on the RP-16 fork pin (`090bc821`,
fork `#276` F-WRITE-COMPRESS-1). AP-1 step 2 measured
`projected_files_at_target` +76.2 % / +88.0 % high on the two synthetic beds
because those beds were INSERT-grown, the fork's task writer wrote them
uncompressed, and `byte_ratio` was therefore 1.000000. RP-16 makes `INSERT INTO`
data files carry the table's `write.parquet.compression-codec` (default zstd).
This round rebuilds the three AP-0 beds on the release module, re-runs
`CALL plan_partitioning`, independently recomputes codecs and footer ratios, and
re-reads the 20 percent check. No Rust or Python source changed.

Environment: Linux 6.8.0-138-generic, Linux Mint 22.2, 64 threads, release
`maturin develop --release` module at commit `864e3483` with
`repark._native.__debug_assertions__ == False` before any measurement. 1-minute
load 12.57 at bed-run start and 8.80 at end. A `java` PID (1310622) was already
on the box at start; this session did not start a JVM and did not set
`REPARK_PARITY_LIVE`. The beds live under `/tmp/ap1r-bed` (never committed).
All three beds ran. The futures source is `/tmp/ap1r-src/test_futures.parquet`,
byte-identical to `~/CodeRepos/myTemp/reTest/test_futures.parquet` (the AP-0
documented source; `/tmp/ap1-src/test_futures.parquet` is gone).

Build walls on this run: futures CTAS 0.3 s, uniform 14.9 s, skewed 14.9 s.

## Measured codecs and byte ratios

Independent recompute with `pyarrow.parquet` over
`/tmp/ap1r-bed/warehouse/repark_ctas/ap/ns/<bed>/data/*.parquet`, summing
`total_compressed_size` / `total_uncompressed_size` across every row group and
column. Every column chunk on every bed reads **ZSTD**.

| Bed | Codec | Files | On-disk bytes (Iceberg `file_size`) | Σ compressed | Σ uncompressed | byte_ratio | Frame says | AP-1 step 2 |
|---|---|---|---|---|---|---|---|---|
| futures | zstd (CTAS, unchanged) | 3 | 26 729 684 | 26 670 018 | 57 643 097 | 0.462675 | `byte_ratio=0.46 (footers)` | zstd, 0.462675, `0.46` |
| uniform | **zstd** (`INSERT`) | 206 | 3 074 844 | 2 831 692 | 7 529 566 | 0.376076 | `byte_ratio=0.38 (footers)` | uncompressed, 1.000000, `1.00` |
| skewed | **zstd** (`INSERT`) | 206 | 3 074 844 | 2 831 692 | 7 529 566 | 0.376076 | `byte_ratio=0.38 (footers)` | uncompressed, 1.000000, `1.00` |

The uniform and skewed INSERT files now carry zstd. That is the direct pin that
fork `#276` reached RePark's INSERT path. On-disk bytes on those beds drop from
AP-1's 7 773 590 to 3 074 844. Column-chunk uncompressed sums stay 7 529 566
(AP-1 step 2 recorded 7 529 966 uncompressed on the uncompressed files). The
0.55 fallback never fired.

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
  plan_id: fa6a69fc97ba7e96
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 2
  candidate: months(event_date_utc)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date_utc)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: d76d53184fb5bb93
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 3
  candidate: months(event_timestamp)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 36
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_timestamp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: c6935c6d7f5aa93a
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.46 (footers)
```

The futures bed is still CTAS-written zstd. Ratio, file count, on-disk bytes and
the months() top-three shape match AP-1 step 2. `unpartitioned` still projects 24
files (`26 729 684 × 0.462675 / 524 288 → 23.59 → 24`). AP-0 ran no rewrite on
this bed, so no actual exists to compare.

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
  plan_id: a842566965c64476
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 2
  candidate: identity(grp)
  score: 0.0
  projected_partitions: 20
  projected_files_at_target: 20
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD identity(grp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: c422253712a77087
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 3
  candidate: unpartitioned
  score: 0.4661998748779297
  projected_partitions: 1
  projected_files_at_target: 3
  ddl: 
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 96092272ad4178b7
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
```

Ranking moved because stored bytes dropped. AP-1 step 2 (ratio 1.00) ranked
`identity(grp)` / `months(ts)` / `years(ts)`. The 20 percent check still uses
`identity(grp)` — the candidate AP-0's O-run rewrote — not the new rank-1 row.
That row still projects 20 partitions and 20 files.

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
  plan_id: 899db36f5516a840
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 2
  candidate: unpartitioned
  score: 0.4661998748779297
  projected_partitions: 1
  projected_files_at_target: 3
  ddl: 
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: 347466f35d5f7af3
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 3
  candidate: months(ts)
  score: 0.8349990844726562
  projected_partitions: 24
  projected_files_at_target: 24
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD months(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: 9ece2d1b7cbc9d98
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the pre-rewrite file bytes (the raw sum measured 76-88% high on the AP-0 beds); projected_partitions measured exact; byte_ratio=0.38 (footers)
```

`identity(grp)` is now rank 4: score 7.2718505859375, 20 partitions, 21 projected
files (AP-1 step 2: rank 2, score 0.853, 27 files). Ranking moved for the same
stored-byte drop.

## The 20 percent check — projection vs live rewrite actuals

AP-0's O-run actuals (uniform 4 413 222, skewed 4 134 457) rewrote
**uncompressed** INSERT files with a partitioned CTAS. They are the wrong
generation of input once INSERT writes zstd. This round re-ran the rewrite on
fresh copies of the zstd INSERT beds (`uniform_orun`, `skewed_orun`), built
from the same seeds as the plan beds (pre-rewrite 206 files, 3 074 844 bytes,
codec ZSTD, footer ratio 0.376076 — cell for cell the plan-bed numbers).

Statements, verbatim, per copy:

```text
ALTER TABLE ap.ns.<bed>_orun ADD PARTITION FIELD identity(grp)
CALL ap.system.rewrite_data_files(table => 'ns.<bed>_orun')
```

`run_adaptpart.py` still has no rewrite flag; the driver was a throwaway
`/tmp/ap1r-orun.py` (not committed) against the same release module. CALL
frames: both beds `rewritten_data_files_count=206`, `added_data_files_count=20`,
`rewritten_bytes_count=3074844`, `failed_data_files_count=0`. Live data-file
bytes are the current-snapshot `files` metadata sum of `file_size_in_bytes`
where `content = 0`. Live footer codecs are the 20 current-snapshot parquet
files (the data directory still holds the 206 superseded zstd inputs until
expire; those are not live).

| Bed | Projected bytes (`Σ file_size × byte_ratio`) | **New actual** (this rewrite) | Error vs new actual | AP-0 stale actual | Error vs stale |
|---|---|---|---|---|---|
| uniform | 3 074 844 × 0.376076 = 1 156 376 | **7 928 680** (20 files, UNCOMPRESSED) | **-85.4 %** | 4 413 222 | −73.8 % |
| skewed | 3 074 844 × 0.376076 = 1 156 376 | **7 672 169** (20 files, UNCOMPRESSED) | **-84.9 %** | 4 134 457 | −72.0 % |

**The honest comparison is the new actual.** It rewrites the same zstd INSERT
beds the projection read. AP-0's actuals stay in the table for continuity; they
are a different write path (partitioned CTAS of uncompressed inputs) and a
different codec (those outputs were zstd at 0.53–0.57×). No timing is claimed:
1-minute load was 7.37 at O-run start and 10.13 at end.

Post-rewrite layout: 20 live files (one per `grp` value). Uniform every file
396 434 bytes. Skewed min 196 394 / median 196 394 / max 3 745 643. Live
column-chunk codecs are **UNCOMPRESSED** on all 20 files, footer ratio 1.0.
`rewrite_data_files` did not carry `target-file-size-bytes`; it did not split
the 3.75 MB `g00` file at 512 KiB (projected_files_at_target was 21 on skewed).

**The projection is still more than 20 percent wrong on both beds against the
new actuals.** Residue AP-1-R-001 stays OPEN. The formula was not tuned.

## What this means for AP-1-R-001

INSERT on these beds is now zstd (the RP-16 pin took). The 20 percent bar is
still missed, and the gap is larger against the live rewrite than against the
stale CTAS actuals, because `rewrite_data_files` wrote **uncompressed** files
(7.93 MB / 7.67 MB) while the projection multiplied already-zstd stored bytes
by the stored zstd ratio (3 074 844 × 0.376076 = 1 156 376). Input codec and
rewrite codec still do not match.

The uncompressed rewrite output has a named cause: fork residue
**F-WRITE-COMPRESS-1-R-002** (`crates/iceberg/src/maintenance/rewrite_data_files_write.rs:69`
builds `WriterProperties::builder().build()`, parquet-rs's UNCOMPRESSED default). Fork `#276`
fixed the INSERT path only and listed the four other writer sites as residue; this measurement is
the first downstream evidence that one of them inflates a real table — compaction of a 3.07 MB
zstd bed produced 7.93 MB of live files. A fork follow-up (F-WRITE-COMPRESS-2) is the fix; it is
not this measurement's to make.

Owner question Q-1, not decided here: should the ratio multiply the footers'
*uncompressed* sum rather than the stored file bytes, i.e. predict the rewrite's
own codec?

## Reproduce

```text
cd python/repark && VIRTUAL_ENV=$PWD/../../.venv uvx maturin@1.14.1 develop --release && cd ../..
rm -rf /tmp/ap1r-bed
.venv/bin/python python/repark-parity/bench/adaptpart/run_adaptpart.py \
  --scratch /tmp/ap1r-bed --futures-parquet /tmp/ap1r-src/test_futures.parquet --plan
```

`--plan` issues the CALL per bed after the AP-0 scoring tables print and dumps
every frame row field by field; the blocks above are that output verbatim. The
independent ratio check is `pyarrow.parquet` over the warehouse path named
above. The O-run rewrite is the two statements in the 20 percent section, on a
fresh INSERT-grown copy per synthetic bed (scratch `/tmp/ap1r-orun`). Related:
[adapt-part-ap1-2026-09-11.md](adapt-part-ap1-2026-09-11.md);
[ap-0-partition-candidates-2026-09-10.md](ap-0-partition-candidates-2026-09-10.md);
ledger `task/ledgers/staging/ap-1-remeasure-ledger.md`.

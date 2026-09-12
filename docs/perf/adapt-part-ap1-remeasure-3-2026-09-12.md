# AP-1 RP-18 re-measure — `plan_partitioning` with the AP-3 projection under one-pass compression (2026-09-12)

ADAPT-PART residue AP-1-R-001 fourth re-measure on the RP-18 fork pin
(`9e3522e3`, fork `#280` F-REWRITE-SIZE-1 step 2). Run 9's re-measure
([adapt-part-ap1-remeasure-2-2026-09-12.md](adapt-part-ap1-remeasure-2-2026-09-12.md))
put the rewrite under one codec but left the dead dictionary pages: the outputs
read 4 474 081 / 4 136 632 against the inputs' compressed sum 2 831 692 — the
measured 1.47× S2-24 inflation. `#280` makes the maintenance rewrite disable the
dictionary per column from the input files' footers and makes an unset
`write.parquet.compression-level` mean zstd 3 like Java. RePark's AP-3 (`#526`)
moved `projected_files_at_target` onto the uncompressed footer sum ×
`byte_ratio`, counting compression once. This round rebuilds the three AP-0 beds
with run 8's exact commands, re-runs `CALL plan_partitioning`, re-runs the
`identity(grp)` rewrite on fresh copies, and reads the 20 percent check against
live actuals under one codec and one compression pass. No Rust or Python source
changed.

Environment: Linux 6.8.0-138-generic, Linux Mint 22.2, 64 threads, release
`maturin develop --release` module at commit `9b1b81da` (the pin bump) with
`repark._native.__debug_assertions__ == False` verified before any measurement.
1-minute load 8.39 at bed-run start. No `java` PID started by this session and
`REPARK_PARITY_LIVE` unset. The beds live under `/tmp/ap1r-bed` and the rewrite
copies under `/tmp/ap1r-orun` (never committed). The futures source is
`/tmp/ap1r-src/test_futures.parquet`, md5-identical to
`~/CodeRepos/myTemp/reTest/test_futures.parquet` (the AP-0 documented source).

Build walls on this run: futures CTAS 0.3 s, uniform 15.0 s, skewed 14.9 s;
rewrite-copy builds 15.1 s / 15.5 s.

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
column. Every column chunk on every bed reads **ZSTD**.

| Bed | Codec | Files | On-disk bytes (Iceberg `file_size`) | Σ compressed | Σ uncompressed | byte_ratio | Frame says | Run 9 |
|---|---|---|---|---|---|---|---|---|
| futures | zstd (CTAS, unchanged) | 3 | 26 729 684 | 26 670 018 | 57 643 097 | 0.462675 | `byte_ratio=0.46 (footers)` | identical |
| uniform | zstd (`INSERT`) | 206 | 3 083 824 | 2 840 672 | 7 529 566 | 0.377269 | `byte_ratio=0.38 (footers)` | 3 074 844 / 2 831 692 / 0.376076 |
| skewed | zstd (`INSERT`) | 206 | 3 083 824 | 2 840 672 | 7 529 566 | 0.377269 | `byte_ratio=0.38 (footers)` | 3 074 844 / 2 831 692 / 0.376076 |

The synthetic beds drifted a little from run 9's cell-for-cell numbers because
the unset-level zstd 3 default reaches the INSERT writers too: stored bytes
3 074 844 → 3 083 824, compressed sum 2 831 692 → 2 840 672, ratio 0.376076 →
0.377269. The uncompressed sum is unchanged at 7 529 566 — same rows, same
codec, different level. Every plan-bed column still carries a dictionary page
(`ts`, `grp`, `id` all `dict=True`); the dictionary change is the rewrite's, not
the INSERT path's. The 0.55 fallback never fired.

## futures — top three frame rows

`CALL ap.system.plan_partitioning(table => 'ns.futures', target_file_size_bytes =>
524288)` — verbatim top three (all columns):

```text
rank 1
  candidate: months(event_date)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 72
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 3f4cfbe95b6acad9
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 2
  candidate: months(event_date_utc)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 72
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_date_utc)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 42d987612d813e3a
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.46 (footers)
rank 3
  candidate: months(event_timestamp)
  score: 0.0
  projected_partitions: 36
  projected_files_at_target: 72
  ddl: ALTER TABLE ns.futures ADD PARTITION FIELD months(event_timestamp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.futures'); CALL ap.system.rewrite_manifests(table => 'ns.futures'); CALL ap.system.expire_snapshots(table => 'ns.futures')
  plan_id: 0eb54cacd18b56ae
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.46 (footers)
```

The months() block still leads at 36 projected partitions but the AP-3
uncompressed basis projects 72 files where the stored-byte basis projected 36.
No rewrite was run on this bed (no `identity(grp)` column), so no actual exists
to compare.

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
  plan_id: a3f533d335bff221
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 2
  candidate: months(ts)
  score: 0.0
  projected_partitions: 24
  projected_files_at_target: 24
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD months(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 13ff52b3c4512b33
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 3
  candidate: years(ts)
  score: 1.590376853942871
  projected_partitions: 2
  projected_files_at_target: 6
  ddl: ALTER TABLE ns.uniform ADD PARTITION FIELD years(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.uniform'); CALL ap.system.rewrite_manifests(table => 'ns.uniform'); CALL ap.system.expire_snapshots(table => 'ns.uniform')
  plan_id: 2064655631e92c0e
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.38 (footers)
```

`identity(grp)` is now rank 1 (was rank 2 under the stored-byte basis) — the
score bands uncompressed bytes now, so the candidate that collapses 206 files
into 20 scores 0. The 20 percent check still uses it: 20 partitions, 20
projected files.

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
  plan_id: ac302ba01be7a341
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 2
  candidate: identity(grp)
  score: 0.7951693534851074
  projected_partitions: 20
  projected_files_at_target: 22
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD identity(grp)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: 789f19b86c785933
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.38 (footers)
rank 3
  candidate: years(ts)
  score: 1.590376853942871
  projected_partitions: 2
  projected_files_at_target: 6
  ddl: ALTER TABLE ns.skewed ADD PARTITION FIELD years(ts)
  calls: CALL ap.system.rewrite_data_files(table => 'ns.skewed'); CALL ap.system.rewrite_manifests(table => 'ns.skewed'); CALL ap.system.expire_snapshots(table => 'ns.skewed')
  plan_id: a72d0683bb47f44f
  notes: AP-0-R-001: projected_files_at_target applies byte_ratio to the footers' uncompressed byte sum, counting compression once (the stored-byte form measured -74%/-72% low against the RP-17 same-codec rewrite; the remaining gap is S2-24); projected_partitions measured exact; byte_ratio=0.38 (footers)
```

`identity(grp)` is now rank 2 (was rank 4): score 0.7951693534851074, 20
partitions, 22 projected files — g00's uncompressed share projects 3 files at
the 512 KiB target where the stored-byte basis projected 2.

## The 20 percent check — projection vs live rewrite actuals, compression counted once

The rewrite re-ran on fresh copies (`uniform_orun`, `skewed_orun`) built from
the same seeds. Pre-rewrite each copy matched the plan beds cell for cell: 206
files, 3 083 824 bytes, footer codec ZSTD, ratio 0.377269. Statements, verbatim:

```text
ALTER TABLE ap.ns.<bed>_orun ADD PARTITION FIELD identity(grp)
CALL ap.system.rewrite_data_files(table => 'ns.<bed>_orun')
```

`run_adaptpart.py` still has no rewrite flag; the driver was a throwaway
`/tmp/ap1r-orun.py` (not committed) against the same release module. CALL
frames, both beds: `rewritten_data_files_count=206`,
`added_data_files_count=20`, `rewritten_bytes_count=3083824`,
`failed_data_files_count=0`, `removed_delete_files_count=0`. Live data-file
bytes are the current-snapshot `files` metadata sum of `file_size_in_bytes`
where `content = 0`; the live files are the 20 parquet files under the new
`data/grp=<value>/` partition directories, whose sizes sum to the metadata total
exactly (the 206 superseded inputs remain at `data/` top level until expire).

**The live output footers are zstd and the dictionary split is asserted** —
every column chunk of all 20 files on both beds reads `ZSTD`; the near-unique
columns `ts` and `id` carry **no** dictionary page while the low-cardinality
`grp` still does (asserted per chunk on every file, not sampled). One footer
line per kind, verbatim:

```text
uniform_orun compacted-00019-01a0959f-3b07-7a71-83c3-393a618a1bcf.parquet rg0 ts  codec=ZSTD dict=False compressed=64505  uncompressed=160031
uniform_orun compacted-00019-01a0959f-3b07-7a71-83c3-393a618a1bcf.parquet rg0 grp codec=ZSTD dict=True  compressed=70     uncompressed=52
skewed_orun  compacted-00001-01a0959f-3e15-7760-8dbd-b3faf8ee5f86.parquet rg0 ts  codec=ZSTD dict=False compressed=645328 uncompressed=1600310
skewed_orun  compacted-00001-01a0959f-3e15-7760-8dbd-b3faf8ee5f86.parquet rg0 grp codec=ZSTD dict=True  compressed=430    uncompressed=331
```

Run 9's dead-dictionary mechanism is gone, and the outputs' own footer ratios
fell below the inputs': 0.283603 (uniform) and 0.270393 (skewed) against the
inputs' 0.377269 — the rewrite now recompresses the same rows *better* than 206
small files did, so the inputs' compressed sum is no longer the floor the
previous rounds assumed.

| Bed | Stored bytes (Σ `file_size`) | Σ uncompressed | byte_ratio | Proj (stored × ratio) | Proj (uncompressed × ratio, the AP-3 basis) | Live actual | Δ stored-proj | Δ uncompressed-proj |
|---|---|---|---|---|---|---|---|---|
| futures | 26 729 684 | 57 643 097 | 0.462675 | 12 367 156 | 26 670 018 | n/a (no rewrite) | n/a | n/a |
| uniform | 3 083 824 | 7 529 566 | 0.377269 | 1 163 431 | 2 840 672 | **1 839 168** (20 files, zstd) | −36.7 % | **+54.5 %** |
| skewed | 3 083 824 | 7 529 566 | 0.377269 | 1 163 431 | 2 840 672 | **1 755 749** (20 files, zstd) | −33.7 % | **+61.8 %** |

Post-rewrite layout: 20 live files, one per `grp` value. Uniform min 87 669 /
median 92 421 / max 94 270. Skewed min 43 657 / median 45 511 / max 860 663.
`rewrite_data_files` still carries no `target-file-size-bytes`; the `g00` file
did not split at 512 KiB (projected_files_at_target was 22 on skewed, 20 files
written). No timing is claimed.

## What this means for AP-1-R-001 — verdict as measured (D-2)

The check the residue names is `Σ file_size × byte_ratio` against the live
rewrite actual, and the projection is now the AP-3 one — the uncompressed sum ×
`byte_ratio` (D-3), the old stored × ratio figure kept beside it for the record.
Under one codec and one compression pass the AP-3 projection reads **+54.5 %
uniform, +61.8 % skewed** — outside 20 % on both beds, so **AP-1-R-001 stays
OPEN**. (The recorded stored×ratio figure reads −36.7 % / −33.7 %, also
outside.)

The sign flipped: the projection no longer under-reads. The dead dictionary
pages are gone and the rewrite's own output ratio (0.284 / 0.270) beats the
inputs' stored ratio (0.377), so the live actual landed *below* the projection's
2 840 672 — the inputs' compressed sum is not the rewrite floor on these beds.
The remaining gap is a real residual, not a codec or double-compression
artifact: an input's footer ratio is measured over 2 000-row files while the
rewrite's output row groups hold ~20 000 rows of one partition each. The formula
was not tuned (D-4).

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
[adapt-part-ap1-remeasure-2-2026-09-12.md](adapt-part-ap1-remeasure-2-2026-09-12.md);
[ap-0-partition-candidates-2026-09-10.md](ap-0-partition-candidates-2026-09-10.md);
ledgers `task/ledgers/staging/rp-18-ledger.md` and
`task/ledgers/completed/ap-1-remeasure-ledger.md`.

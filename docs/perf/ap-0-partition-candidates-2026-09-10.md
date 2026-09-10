# AP-0 partition candidates (2026-09-10)

ADAPT-PART unit AP-0 (measure): given a target file size, profile three real tables from
their Iceberg manifests, generate the P-2 candidate partition specs, and score them with
the P-3 model. No engine changes — one script, three beds, three ranked tables. The
20 percent prediction check needs a real rewrite and is not claimed here; it is the
orchestrator's O-run, open as ledger clause C-005.

## Beds and build

All three beds live in one local memory-catalog warehouse under the `--scratch` root
(never committed) and are unpartitioned, so every `partitions` table carries the single
aggregate census row with no `partition` column (fork 194 Java parity) and every
projection below comes from file bound ranges. Target file size for all three beds:
**524288 bytes** (512 KiB). One-column distinct samples: `LIMIT 200000` (see Samples).

1. **futures** — `ap.ns.futures`, CTAS unpartitioned from
   `~/CodeRepos/myTemp/reTest/test_futures.parquet` (20 MB on disk, 1 000 000 rows, 21
   columns: timestamps, symbols, prices, counts). CTAS wall 5.4 s. The table lands in
   **3 files, 26 729 684 bytes**. Measured layout: the source frame is year-clustered,
   so each file holds exactly one calendar year — 2008 (337 483 rows), 2016 (349 088),
   2020 (313 429). The `partitions` census agrees cell for cell (rows 1000000,
   file_count 3, bytes 26729684). The global month envelope is 156 months wide but only
   36 months carry bytes.
2. **synthetic-uniform** — `ap.ns.uniform`, generated in the script: `ts` timestamp
   spread over 2023-01-01..2024-12-31, `grp` string over 20 values `g00..g19`,
   `id` int64 sequential 0..399999. 200 batches of 2000 rows, batch `b` to group
   `g{b%20}`, written as one seed parquet, then `CREATE TABLE` plus one `INSERT` per
   batch so the writer lands one narrow file per commit. Build wall 129.4 s. The table
   holds **400 000 rows in 206 files, 7 773 590 bytes** (200 appends, six net extra
   files where an append split across two). The `partitions` census agrees
   (file_count 206).
3. **synthetic-skewed** — `ap.ns.skewed`, same schema and row count, same batching:
   even batches go to `g00` (200 000 rows, 50 percent of the bed), odd batches cycle
   `g01..g19` (`g01..g05` land 12 000 rows each, `g06..g19` land 10 000 each). Build
   wall 126.6 s. **400 000 rows in 206 files, 7 773 590 bytes**; `partitions` agrees.

## Method, one paragraph

Per P-1 the script reads `SELECT * FROM <table>.files` (per-file `record_count`,
`file_size_in_bytes`, and the `readable_metrics` lower/upper bound per column) and
`SELECT * FROM <table>.partitions`, never the data — except one bounded
single-column head sample per string/int column to estimate distinct counts (P-2 names
the sample as the distinct-count source; bounds-proven constants take no sample).
Per P-2 every timestamp/date column gets `year`/`month`/`day`/`hour`, every
string/int column at or below 1 000 sampled distinct values (or proven constant from
bounds) gets `identity`, every high-cardinality string/int column gets `bucket(N)`
for N in 8, 16, 32, 64, 128, plus `none`; the three two-field specs pair the best
single spec of the top three fields. Per P-3 a file whose bound range spans k values
contributes 1/k of its bytes to each, and each projected partition value scores its
shortfall below 0.25x target plus its overshoot above 4x target; lower is better,
ties break by projected file count then name. A file with missing bounds, or an
identity file whose range holds no sampled value, spreads uniformly and is flagged in
the note column. Bucket candidates assume a uniform hash spread; pairs assume a
per-file cross-product spread. Projected files per value are `ceil(bytes/target)`,
summed. Naive bound timestamps read as UTC.

## Ranked tables

Score is the P-3 band penalty (best 0). Partitions counts values with bytes above
zero. Ranks 1..N are best first.

### futures

```text
rows 1000000 files 3 bytes 26729684
target 524288 sample_rows 200000
partitions table: rows 1000000
partitions table: file_count 3
partitions table: bytes 26729684
partitions table: partition_column False
constant from bounds (no sample): interval_minutes, open_interest, ticker
sampled: contract_symbol distinct=3; ticker_epoch distinct=200000; total_volume distinct=14029; total_ticks distinct=5883; up_ticks distinct=3512; down_ticks distinct=3507; up_volume distinct=8869; down_volume distinct=8915
nulls observed: none
note: ticker_epoch: 200000 sampled distinct values exceed 1000, bucket branch
note: total_volume: 14029 sampled distinct values exceed 1000, bucket branch
note: total_ticks: 5883 sampled distinct values exceed 1000, bucket branch
note: up_ticks: 3512 sampled distinct values exceed 1000, bucket branch
note: down_ticks: 3507 sampled distinct values exceed 1000, bucket branch
note: up_volume: 8869 sampled distinct values exceed 1000, bucket branch
note: down_volume: 8915 sampled distinct values exceed 1000, bucket branch
rank candidate                                               score partitions proj_files note
   1 bucket(16, down_ticks)                                  0.000         16         64 uniform-hash spread over N buckets
   2 bucket(16, down_volume)                                 0.000         16         64 uniform-hash spread over N buckets
   3 bucket(16, ticker_epoch)                                0.000         16         64 uniform-hash spread over N buckets
   4 bucket(16, total_ticks)                                 0.000         16         64 uniform-hash spread over N buckets
   5 bucket(16, total_volume)                                0.000         16         64 uniform-hash spread over N buckets
   6 bucket(16, up_ticks)                                    0.000         16         64 uniform-hash spread over N buckets
   7 bucket(16, up_volume)                                   0.000         16         64 uniform-hash spread over N buckets
   8 bucket(32, down_ticks)                                  0.000         32         64 uniform-hash spread over N buckets
   9 bucket(32, down_volume)                                 0.000         32         64 uniform-hash spread over N buckets
  10 bucket(32, ticker_epoch)                                0.000         32         64 uniform-hash spread over N buckets
  11 bucket(32, total_ticks)                                 0.000         32         64 uniform-hash spread over N buckets
  12 bucket(32, total_volume)                                0.000         32         64 uniform-hash spread over N buckets
  13 bucket(32, up_ticks)                                    0.000         32         64 uniform-hash spread over N buckets
  14 bucket(32, up_volume)                                   0.000         32         64 uniform-hash spread over N buckets
  15 bucket(64, down_ticks)                                  0.000         64         64 uniform-hash spread over N buckets
  16 bucket(64, down_volume)                                 0.000         64         64 uniform-hash spread over N buckets
  17 bucket(64, ticker_epoch)                                0.000         64         64 uniform-hash spread over N buckets
  18 bucket(64, total_ticks)                                 0.000         64         64 uniform-hash spread over N buckets
  19 bucket(64, total_volume)                                0.000         64         64 uniform-hash spread over N buckets
  20 bucket(64, up_ticks)                                    0.000         64         64 uniform-hash spread over N buckets
  21 bucket(64, up_volume)                                   0.000         64         64 uniform-hash spread over N buckets
  22 month(event_date)                                       0.000         36         72
  23 month(event_date_utc)                                   0.000         36         72
  24 month(event_timestamp)                                  0.000         36         72
  25 month(event_timestamp_utc)                              0.000         36         72
  26 month(roll_in_date)                                     0.000         36         72
  27 month(roll_out_date)                                    0.000         36         72
  28 bucket(128, down_ticks)                                 0.000        128        128 uniform-hash spread over N buckets
  29 bucket(128, down_volume)                                0.000        128        128 uniform-hash spread over N buckets
  30 bucket(128, ticker_epoch)                               0.000        128        128 uniform-hash spread over N buckets
  31 bucket(128, total_ticks)                                0.000        128        128 uniform-hash spread over N buckets
  32 bucket(128, total_volume)                               0.000        128        128 uniform-hash spread over N buckets
  33 bucket(128, up_ticks)                                   0.000        128        128 uniform-hash spread over N buckets
  34 bucket(128, up_volume)                                  0.000        128        128 uniform-hash spread over N buckets
  35 bucket(8, down_ticks)                                   4.746          8         56 uniform-hash spread over N buckets
  36 bucket(8, down_volume)                                  4.746          8         56 uniform-hash spread over N buckets
  37 bucket(8, ticker_epoch)                                 4.746          8         56 uniform-hash spread over N buckets
  38 bucket(8, total_ticks)                                  4.746          8         56 uniform-hash spread over N buckets
  39 bucket(8, total_volume)                                 4.746          8         56 uniform-hash spread over N buckets
  40 bucket(8, up_ticks)                                     4.746          8         56 uniform-hash spread over N buckets
  41 bucket(8, up_volume)                                    4.746          8         56 uniform-hash spread over N buckets
  42 year(roll_in_date)                                      6.746          6         54
  43 year(roll_out_date)                                     7.746          5         54
  44 identity(contract_symbol)                               9.746          3         52
  45 year(event_date)                                        9.746          3         53
  46 year(event_date_utc)                                    9.746          3         53
  47 year(event_timestamp)                                   9.746          3         53
  48 year(event_timestamp_utc)                               9.746          3         53
  49 identity(interval_minutes)                             11.746          1         51
  50 identity(open_interest)                                11.746          1         51
  51 identity(ticker)                                       11.746          1         51
  52 none                                                   11.746          1         51
  53 day(roll_in_date)                                     800.069       1004       1004
  54 day(roll_out_date)                                    800.069       1004       1004
  55 day(event_date)                                       872.069       1076       1076
  56 day(event_timestamp)                                  872.069       1076       1076
  57 day(event_date_utc)                                   873.069       1077       1077
  58 day(event_timestamp_utc)                              873.069       1077       1077
  59 bucket(128, down_ticks)+month(event_date)            4404.069       4608       4608 cross-product spread
  60 bucket(128, down_volume)+month(event_date)           4404.069       4608       4608 cross-product spread
  61 bucket(128, down_ticks)+bucket(128, down_volume)    16180.069      16384      16384 cross-product spread
  62 hour(roll_in_date)                                  23823.069      24027      24027
  63 hour(roll_out_date)                                 23823.069      24027      24027
  64 hour(event_date)                                    25551.069      25755      25755
  65 hour(event_timestamp)                               25565.069      25769      25769
  66 hour(event_timestamp_utc)                           25565.069      25769      25769
  67 hour(event_date_utc)                                25575.069      25779      25779
```

### synthetic-uniform

```text
rows 400000 files 206 bytes 7773590
target 524288 sample_rows 200000
partitions table: rows 400000
partitions table: file_count 206
partitions table: bytes 7773590
partitions table: partition_column False
constant from bounds (no sample): none
sampled: grp distinct=20; id distinct=200000
nulls observed: none
note: id: 200000 sampled distinct values exceed 1000, bucket branch
rank candidate                           score partitions proj_files note
   1 bucket(16, id)                      0.000         16         16 uniform-hash spread over N buckets
   2 bucket(8, id)                       0.000          8         16 uniform-hash spread over N buckets
   3 identity(grp)                       0.000         20         20
   4 month(ts)                           0.000         24         24
   5 bucket(32, id)                      0.000         32         32 uniform-hash spread over N buckets
   6 year(ts)                            1.707          2         16
   7 none                                2.707          1         15
   8 bucket(64, id)                      4.692         64         64 uniform-hash spread over N buckets
   9 bucket(128, id)                    68.692        128        128 uniform-hash spread over N buckets
  10 identity(grp)+month(ts)           162.692        222        222 cross-product spread
  11 identity(grp)+bucket(16, id)      260.692        320        320 cross-product spread
  12 bucket(16, id)+month(ts)          324.692        384        384 cross-product spread
  13 day(ts)                           670.692        730        730
  14 hour(ts)                        17460.692      17520      17520
```

### synthetic-skewed

```text
rows 400000 files 206 bytes 7773590
target 524288 sample_rows 200000
partitions table: rows 400000
partitions table: file_count 206
partitions table: bytes 7773590
partitions table: partition_column False
constant from bounds (no sample): none
sampled: grp distinct=20; id distinct=200000
nulls observed: none
note: id: 200000 sampled distinct values exceed 1000, bucket branch
rank candidate                           score partitions proj_files note
   1 bucket(16, id)                      0.000         16         16 uniform-hash spread over N buckets
   2 bucket(8, id)                       0.000          8         16 uniform-hash spread over N buckets
   3 month(ts)                           0.000         24         24
   4 bucket(32, id)                      0.000         32         32 uniform-hash spread over N buckets
   5 identity(grp)                       0.853         20         27
   6 year(ts)                            1.707          2         16
   7 none                                2.707          1         15
   8 bucket(64, id)                      4.692         64         64 uniform-hash spread over N buckets
   9 bucket(128, id)                    68.692        128        128 uniform-hash spread over N buckets
  10 month(ts)+identity(grp)            80.346        134        134 cross-product spread
  11 bucket(16, id)+identity(grp)      274.346        320        320 cross-product spread
  12 bucket(16, id)+month(ts)          324.692        384        384 cross-product spread
  13 day(ts)                           670.692        730        730
  14 hour(ts)                        17460.692      17520      17520
```

## Columns that needed a data sample

No candidate column needed a sample for its bounds: every `readable_metrics`
lower/upper pair the scorer touched was present, so the uniform-spread fallback never
fired on missing bounds. Samples served only the P-2 distinct-count branch:

- futures: `contract_symbol` (3 distinct in sample), `ticker_epoch` (200000),
  `total_volume` (14029), `total_ticks` (5883), `up_ticks` (3512), `down_ticks`
  (3507), `up_volume` (8869), `down_volume` (8915). `ticker`, `interval_minutes`
  and `open_interest` are bounds-proven constants and took no sample. Temporal
  columns never sample.
- synthetic-uniform and synthetic-skewed: `grp` (20 distinct, complete — the batch
  interleave puts every group inside the head sample) and `id` (200000, bucket
  branch). No constants.

Two blind spots, both measured. First, the futures sample saw 3 of the true 14
`contract_symbol` values (a dev-time full `COUNT DISTINCT` found 14; the source
frame is time-ordered, so the head sample covers only the 2008 file's era). The
ranked `identity(contract_symbol)` row therefore projects 3 partitions, not 14.
Second, string range containment is lexicographic: the 3 sampled symbols sit inside
all three files' `[ESH.., ESZ..]` ranges, so each file spreads evenly and no
fallback fired. Had the synthetic beds interleaved groups inside files instead of
clustering one group per file, the skewed and uniform beds would score identically —
the skew is visible only because the layout keeps group ranges narrow.

## What this says

The P-3 ordering looks plausible against the file-size facts the metadata reports:

- `month()` beats `day()`, `hour()` and `year()` on every bed with time content.
  On futures the monthly slices land near 740 KB, inside the 128 KB..2 MB band
  (score 0.000, 36 partitions), while daily slices near 25 KB fall below it (near
  872), hourly slices near 1 KB far below it (near 25 000), and yearly slices near
  9 MB above it (near 9.7). The band, not the grain name, does the ranking.
- `none` and the single-value identities sit at 11.746 on futures: 26.7 MB in one
  value against a 2 MB cap. The model calls the status quo — three files averaging
  8.9 MB, seventeen times the target — the worst single-field shape short of
  `day`/`hour`. That matches the census.
- The designed contrast landed: uniform `identity(grp)` scores 0.000 over 20
  partitions and 20 files, skewed scores 0.853 over 20 partitions and 27 files.
  That row is the only difference between the two synthetic tables, and it isolates
  the skew penalty: `g00` carries near 3.9 MB against the 2 MB cap while the
  nineteen small groups sit inside the band.
- The `bucket(N)` gradient on the sequential `id` is monotone in the right
  direction: 8/16/32 score 0.000 (per-bucket bytes inside the band), 64 scores
  4.692, 128 scores 68.692 as per-bucket bytes slide under the 128 KB floor.
- Two-field specs rank below their parents on all three beds (futures pairs at
  4404 and above; synthetics at 80 and above). The cross-product spread fragments
  bytes across hundreds of combos, so the model reports the fragmentation cost of
  a second field honestly; whether queries repay it is AP-1's decision, not the
  score's.
- The ties at 0.000 on futures (all of `bucket(16/32/64)` plus `month()`) break by
  projected file count then name. That order carries no signal beyond file count
  and is disclosed as a tie-break, not a ranking.

Not claimed: whether these projections predict real rewritten file sizes within
20 percent. That needs the manual rewrite — the orchestrator's O-run, open as
ledger clause C-005.

## Reproduce

One command reproduces every row above, on a fresh scratch root (about five minutes;
builds dominate). A rerun needs a fresh root: clear it first and run again.

```text
.venv/bin/python python/repark-parity/bench/adaptpart/run_adaptpart.py --scratch /tmp/ap0-bed
```

Defaults used above: `--target-file-size-bytes 524288 --sample-rows 200000
--batches 200 --batch-rows 2000`. The script is
[python/repark-parity/bench/adaptpart/run_adaptpart.py](../../python/repark-parity/bench/adaptpart/run_adaptpart.py);
its clause table is
[task/ledgers/staging/ap-0-ledger.md](../../task/ledgers/staging/ap-0-ledger.md).

# PROFILES-1 step 2 — the configuration sweep, measured (2026-09-12)

Step 2 of card PROFILES-1
(`task/roadmap/mid-term/cheap-tier-slate-2026-09-08.md`): the sweep and the
measurements, one table per knob. This document is measurements only — no profile
is declared, no guide page changed, no `repark.toml` example written (step 3).
Every number traces to a CSV row committed beside this document under
[config-profiles-2026-09-12/](config-profiles-2026-09-12/map.md).

## Method

- **Build:** `maturin develop --release`; `python/repark/src/repark/_native.abi3.so`
  is byte-identical to `target/release/lib_native.so` (166,874,448 B, built
  2026-09-11 23:23). No debug module measured.
- **Box:** AMD Ryzen Threadripper 3970X (32 cores / 64 threads, so
  `target_partitions` defaults to 64), 125 GB RAM, kernel 6.8.0-138-generic.
  The quiet-box guard (`pgrep -f java` before every timed repetition) held for
  every cell — no JVM ran. Background desktop load (load average ~15 of 64
  threads) is recorded honestly; this is the same box every `docs/perf/`
  baseline is measured on.
- **Datasets (D-2):** the owner's futures parquet
  (`~/CodeRepos/myTemp/reTest/test_futures.parquet`, 20 MB, read in place), TPC-H
  SF10 generated once under the scratch root through the shared dbgen path
  (`bench/tpch/datagen.py`, duckdb `CALL dbgen(sf=10)`), and a memory-catalog
  Iceberg v2 table `PARTITIONED BY (g)` rebuilt to 200 files (5,000 rows each)
  before every timed write repetition.
- **Repetitions:** three per cell, median reported (D-3). Repetition 1 is cold
  (parquet footers, OS cache); the median dampens it.
- **Cells:** every sweep runs the full bed — seven read cells (scan+filter and
  group-by on futures and tpch, hash join and sort-merge join on tpch, window on
  futures) and three write cells (append 8 files, `INSERT OVERWRITE` one
  partition, `MERGE` 10 % updates) — so a read knob's write cells and a write
  knob's read cells are the flat controls that make "no effect" a measurement,
  not an assumption.
- **Values (D-2):** `@default` is the knob unset — the honest default, not the
  default value typed back in. Booleans sweep `@default` plus both ways; integers
  sweep `@default`, half and double of the default (batch sizes ×¼ and ×4 per
  D-2's wider band); enumerations sweep `@default` plus every documented member.
- **`write.*` knobs are table properties,** not session conf — the write path
  reads them from the table's metadata (`append.rs`, `distribution.rs`), and the
  step-0 probe showed the session only stores them. `run_profiles.py` therefore
  lands them on the rebuilt bed table with `ALTER TABLE … SET TBLPROPERTIES`
  before the timed write (the harness fix, pinned); the bed itself is built
  identical across cells.
- **argmax:** the non-`@default` cell with the lowest ratio to that cell's
  `@default` over all ten cells — the biggest improvement observed, per knob
  not per combination (D-4). An argmax above 1.0 means every non-default value
  was slower everywhere; an argmax on a cell the knob cannot reach is noise,
  and the no-effect rule (not the argmax) is the classification.
- **Affected cells:** the card's "its query" reads as the cells the knob can
  reach — a writer-only knob cannot move a parquet read, so its read cells are
  controls, not evidence. Read-side parquet knobs (scan repartitioning,
  pushdown, page index, bloom filters) reach every read cell plus
  `merge_updates` (the merge target scan is a parquet read); join knobs reach
  the two join cells plus `merge_updates` (the MERGE match is a join);
  `repartition_aggregations` reaches the cells carrying a GROUP BY;
  `target_partitions` and both batch-size knobs reach all ten cells; the two
  merge knobs and `repark.scan.concurrency_limit` reach `merge_updates` alone;
  every writer-only knob reaches the three write cells.
- **Control spellings:** a spelled value identical to the engine default is a
  control, not a change — `true` for the boolean knobs whose default is `true`
  (and `false` for `pushdown_filters` and `bloom_filter_on_write`), `zstd(3)`
  for `compression`, and `hash`/`range` for `write.distribution-mode` (the
  engine's `distribution_is_none` treats absent, `hash` and `range`
  identically — only `none` changes behaviour).
- **No effect measured:** a knob lands on the no-effect list when every
  affected cell with a non-control spelling stays within 5 % of its `@default`
  cell — the card's 5 % rule applied where the knob can act. A regression
  counts: a knob that only makes things worse has an effect, not "no effect".
- **Not swept:** `datafusion.execution.coalesce_batches` — refused loud at
  `.config()` since CONF-UNREAD-1 (DataFusion 54.1.0 defines the option but no
  engine path reads it); listed as "not read (R-16)". `lzo` is not a documented
  member of the compression enumeration in this build (the writer's own error
  names the valid set: `uncompressed, snappy, gzip(level), brotli(level), lz4,
  zstd(level), lz4_raw`); leveled codecs were swept at the parquet crate's
  default level (`gzip(6)`, `brotli(1)`, `zstd(3)` — the last identical to the
  engine default).
- **CONF-UNREAD-1 re-check (D-1):** each of the four keys the step-0 probe left
  `ACCEPTED BUT UNREAD` was re-probed this round before sweeping:
  `enable_page_index`, `bloom_filter_on_read` and `write_batch_size` set and read
  back (they pass through since CONF-UNREAD-1 — swept below); `coalesce_batches`
  refuses (above).

Reproduce one row:

```bash
.venv/bin/python python/repark-parity/bench/profiles/run_profiles.py \
  --scratch <scratch> --knob <key> --values <v1,v2,v3> --repeats 3 \
  --out docs/perf/config-profiles-2026-09-12/<key>.csv
```

## Swept values chosen per knob (D-2)

| knob | default | values swept |
|---|---|---|
| `datafusion.optimizer.prefer_hash_join` | `true` | `@default,true,false` |
| `datafusion.execution.target_partitions` | `64` | `@default,32,128` |
| `datafusion.execution.batch_size` | `65536` (RePark session default) | `@default,16384,262144` |
| `datafusion.optimizer.repartition_joins` | `true` | `@default,true,false` |
| `datafusion.optimizer.repartition_aggregations` | `true` | `@default,true,false` |
| `datafusion.optimizer.repartition_file_scans` | `true` | `@default,true,false` |
| `datafusion.execution.parquet.pushdown_filters` | `false` | `@default,true,false` |
| `datafusion.execution.parquet.enable_page_index` | `true` | `@default,true,false` |
| `datafusion.execution.parquet.bloom_filter_on_read` | `true` | `@default,true,false` |
| `repark.scan.concurrency_limit` | unset (unlimited) | `@default,4,16` — optional key, no numeric default; two limits bracket the 200-file scan |
| `repark.batch.size` | `65536` | `@default,16384,262144` |
| `datafusion.execution.parquet.compression` | `zstd(3)` | `@default,uncompressed,snappy,gzip(6),brotli(1),lz4,zstd(3),lz4_raw` |
| `datafusion.execution.parquet.max_row_group_size` | `1048576` | `@default,524288,2097152` |
| `datafusion.execution.parquet.bloom_filter_on_write` | `false` | `@default,true,false` |
| `datafusion.execution.parquet.write_batch_size` | `1024` | `@default,512,2048` |
| `write.target-file-size-bytes` | `536870912` (512 MiB, fork default) | `@default,268435456,1073741824` |
| `write.distribution-mode` | unset (behaves as `hash`/`range`) | `@default,none,hash,range` |
| `repark.merge.file_scoped_rewrite` | `true` | `@default,true,false` |
| `repark.merge.scan_pruning` | `true` | `@default,true,false` |
| `datafusion.execution.coalesce_batches` | — | not swept — refused loud (R-16) |

## Baseline

| dataset | query | median s |
|---|---|---|
| futures | scan_filter | 0.014 |
| tpch | scan_filter | 0.387 |
| futures | group_by | 0.040 |
| tpch | group_by | 0.526 |
| tpch | hash_join | 1.124 |
| tpch | sort_merge_join | 5.352 |
| futures | window | 0.676 |
| iceberg | append_files | 0.835 |
| iceberg | overwrite_partition | 0.172 |
| iceberg | merge_updates | 0.621 |

## Read knobs

## `datafusion.optimizer.prefer_hash_join`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.267 | 1.0000 |
| @default | futures | group_by | 0.030 | 1.0000 |
| @default | tpch | group_by | 0.528 | 1.0000 |
| @default | tpch | hash_join | 1.082 | 1.0000 |
| @default | tpch | sort_merge_join | 5.254 | 1.0000 |
| @default | futures | window | 0.668 | 1.0000 |
| @default | iceberg | append_files | 0.772 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.186 | 1.0000 |
| @default | iceberg | merge_updates | 0.593 | 1.0000 |
| true | futures | scan_filter | 0.012 | 0.9889 |
| true | tpch | scan_filter | 0.276 | 1.0337 |
| true | futures | group_by | 0.075 | 2.4850 |
| true | tpch | group_by | 0.534 | 1.0113 |
| true | tpch | hash_join | 1.145 | 1.0584 |
| true | tpch | sort_merge_join | 5.539 | 1.0542 |
| true | futures | window | 0.745 | 1.1143 |
| true | iceberg | append_files | 0.785 | 1.0171 |
| true | iceberg | overwrite_partition | 0.171 | 0.9163 |
| true | iceberg | merge_updates | 0.619 | 1.0430 |
| false | futures | scan_filter | 0.010 | 0.8233 |
| false | tpch | scan_filter | 0.274 | 1.0264 |
| false | futures | group_by | 0.038 | 1.2521 |
| false | tpch | group_by | 0.522 | 0.9882 |
| false | tpch | hash_join | 11.038 | 10.2064 |
| false | tpch | sort_merge_join | 18.688 | 3.5567 |
| false | futures | window | 0.669 | 1.0020 |
| false | iceberg | append_files | 0.796 | 1.0309 |
| false | iceberg | overwrite_partition | 0.189 | 1.0126 |
| false | iceberg | merge_updates | 0.602 | 1.0142 |

**argmax:** `false` on futures/scan_filter (ratio 0.8233)

## `datafusion.execution.target_partitions`

Default `64`; values swept per D-2: half / double of default.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.013 | 1.0000 |
| @default | tpch | scan_filter | 0.278 | 1.0000 |
| @default | futures | group_by | 0.028 | 1.0000 |
| @default | tpch | group_by | 0.535 | 1.0000 |
| @default | tpch | hash_join | 1.125 | 1.0000 |
| @default | tpch | sort_merge_join | 5.233 | 1.0000 |
| @default | futures | window | 0.668 | 1.0000 |
| @default | iceberg | append_files | 0.778 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.167 | 1.0000 |
| @default | iceberg | merge_updates | 0.623 | 1.0000 |
| 32 | futures | scan_filter | 0.007 | 0.5542 |
| 32 | tpch | scan_filter | 0.227 | 0.8178 |
| 32 | futures | group_by | 0.032 | 1.1222 |
| 32 | tpch | group_by | 0.417 | 0.7800 |
| 32 | tpch | hash_join | 1.111 | 0.9876 |
| 32 | tpch | sort_merge_join | 4.800 | 0.9173 |
| 32 | futures | window | 0.667 | 0.9982 |
| 32 | iceberg | append_files | 0.766 | 0.9855 |
| 32 | iceberg | overwrite_partition | 0.168 | 1.0042 |
| 32 | iceberg | merge_updates | 0.606 | 0.9726 |
| 128 | futures | scan_filter | 0.020 | 1.6358 |
| 128 | tpch | scan_filter | 0.325 | 1.1705 |
| 128 | futures | group_by | 0.054 | 1.8953 |
| 128 | tpch | group_by | 0.571 | 1.0688 |
| 128 | tpch | hash_join | 1.041 | 0.9252 |
| 128 | tpch | sort_merge_join | 6.001 | 1.1468 |
| 128 | futures | window | 0.816 | 1.2212 |
| 128 | iceberg | append_files | 0.951 | 1.2230 |
| 128 | iceberg | overwrite_partition | 0.189 | 1.1287 |
| 128 | iceberg | merge_updates | 0.651 | 1.0454 |

**argmax:** `32` on futures/scan_filter (ratio 0.5542)

## `datafusion.execution.batch_size`

Default `65536`; values swept per D-2: batch sizes x1/4, x1, x4.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.013 | 1.0000 |
| @default | tpch | scan_filter | 0.280 | 1.0000 |
| @default | futures | group_by | 0.032 | 1.0000 |
| @default | tpch | group_by | 0.537 | 1.0000 |
| @default | tpch | hash_join | 1.063 | 1.0000 |
| @default | tpch | sort_merge_join | 5.294 | 1.0000 |
| @default | futures | window | 0.782 | 1.0000 |
| @default | iceberg | append_files | 0.833 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.170 | 1.0000 |
| @default | iceberg | merge_updates | 0.603 | 1.0000 |
| 16384 | futures | scan_filter | 0.012 | 0.9395 |
| 16384 | tpch | scan_filter | 0.170 | 0.6055 |
| 16384 | futures | group_by | 0.047 | 1.4792 |
| 16384 | tpch | group_by | 0.169 | 0.3148 |
| 16384 | tpch | hash_join | 0.920 | 0.8653 |
| 16384 | tpch | sort_merge_join | 5.198 | 0.9820 |
| 16384 | futures | window | 0.697 | 0.8914 |
| 16384 | iceberg | append_files | 0.801 | 0.9608 |
| 16384 | iceberg | overwrite_partition | 0.167 | 0.9817 |
| 16384 | iceberg | merge_updates | 0.611 | 1.0138 |
| 262144 | futures | scan_filter | 0.011 | 0.8795 |
| 262144 | tpch | scan_filter | 0.342 | 1.2200 |
| 262144 | futures | group_by | 0.045 | 1.4202 |
| 262144 | tpch | group_by | 0.658 | 1.2242 |
| 262144 | tpch | hash_join | 1.236 | 1.1628 |
| 262144 | tpch | sort_merge_join | 5.889 | 1.1124 |
| 262144 | futures | window | 0.720 | 0.9205 |
| 262144 | iceberg | append_files | 0.783 | 0.9391 |
| 262144 | iceberg | overwrite_partition | 0.188 | 1.1099 |
| 262144 | iceberg | merge_updates | 0.636 | 1.0543 |

**argmax:** `16384` on tpch/group_by (ratio 0.3148)

## `datafusion.optimizer.repartition_joins`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.273 | 1.0000 |
| @default | futures | group_by | 0.032 | 1.0000 |
| @default | tpch | group_by | 0.523 | 1.0000 |
| @default | tpch | hash_join | 1.129 | 1.0000 |
| @default | tpch | sort_merge_join | 5.247 | 1.0000 |
| @default | futures | window | 0.673 | 1.0000 |
| @default | iceberg | append_files | 0.790 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.168 | 1.0000 |
| @default | iceberg | merge_updates | 0.603 | 1.0000 |
| true | futures | scan_filter | 0.010 | 0.8275 |
| true | tpch | scan_filter | 0.278 | 1.0187 |
| true | futures | group_by | 0.085 | 2.6288 |
| true | tpch | group_by | 0.528 | 1.0101 |
| true | tpch | hash_join | 1.094 | 0.9697 |
| true | tpch | sort_merge_join | 5.475 | 1.0436 |
| true | futures | window | 0.651 | 0.9680 |
| true | iceberg | append_files | 0.816 | 1.0327 |
| true | iceberg | overwrite_partition | 0.175 | 1.0421 |
| true | iceberg | merge_updates | 0.634 | 1.0502 |
| false | futures | scan_filter | 0.014 | 1.1154 |
| false | tpch | scan_filter | 0.291 | 1.0660 |
| false | futures | group_by | 0.037 | 1.1404 |
| false | tpch | group_by | 0.524 | 1.0012 |
| false | tpch | hash_join | 1.064 | 0.9426 |
| false | tpch | sort_merge_join | 3.944 | 0.7517 |
| false | futures | window | 0.657 | 0.9758 |
| false | iceberg | append_files | 0.815 | 1.0320 |
| false | iceberg | overwrite_partition | 0.174 | 1.0383 |
| false | iceberg | merge_updates | 0.582 | 0.9647 |

**argmax:** `false` on tpch/sort_merge_join (ratio 0.7517)

## `datafusion.optimizer.repartition_aggregations`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.011 | 1.0000 |
| @default | tpch | scan_filter | 0.265 | 1.0000 |
| @default | futures | group_by | 0.029 | 1.0000 |
| @default | tpch | group_by | 0.534 | 1.0000 |
| @default | tpch | hash_join | 1.105 | 1.0000 |
| @default | tpch | sort_merge_join | 5.265 | 1.0000 |
| @default | futures | window | 0.672 | 1.0000 |
| @default | iceberg | append_files | 0.787 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.166 | 1.0000 |
| @default | iceberg | merge_updates | 0.610 | 1.0000 |
| true | futures | scan_filter | 0.011 | 0.9889 |
| true | tpch | scan_filter | 0.284 | 1.0715 |
| true | futures | group_by | 0.037 | 1.2794 |
| true | tpch | group_by | 0.529 | 0.9914 |
| true | tpch | hash_join | 1.043 | 0.9441 |
| true | tpch | sort_merge_join | 5.382 | 1.0221 |
| true | futures | window | 0.663 | 0.9870 |
| true | iceberg | append_files | 0.789 | 1.0016 |
| true | iceberg | overwrite_partition | 0.190 | 1.1415 |
| true | iceberg | merge_updates | 0.621 | 1.0172 |
| false | futures | scan_filter | 0.010 | 0.9413 |
| false | tpch | scan_filter | 0.285 | 1.0752 |
| false | futures | group_by | 0.030 | 1.0312 |
| false | tpch | group_by | 0.534 | 1.0011 |
| false | tpch | hash_join | 4.688 | 4.2430 |
| false | tpch | sort_merge_join | 5.610 | 1.0655 |
| false | futures | window | 0.667 | 0.9931 |
| false | iceberg | append_files | 0.806 | 1.0232 |
| false | iceberg | overwrite_partition | 0.173 | 1.0384 |
| false | iceberg | merge_updates | 0.623 | 1.0209 |

**argmax:** `false` on futures/scan_filter (ratio 0.9413)

## `datafusion.optimizer.repartition_file_scans`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.013 | 1.0000 |
| @default | tpch | scan_filter | 0.272 | 1.0000 |
| @default | futures | group_by | 0.029 | 1.0000 |
| @default | tpch | group_by | 0.535 | 1.0000 |
| @default | tpch | hash_join | 1.066 | 1.0000 |
| @default | tpch | sort_merge_join | 5.231 | 1.0000 |
| @default | futures | window | 0.715 | 1.0000 |
| @default | iceberg | append_files | 0.780 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.166 | 1.0000 |
| @default | iceberg | merge_updates | 0.611 | 1.0000 |
| true | futures | scan_filter | 0.012 | 0.9447 |
| true | tpch | scan_filter | 0.300 | 1.1010 |
| true | futures | group_by | 0.050 | 1.7273 |
| true | tpch | group_by | 0.525 | 0.9814 |
| true | tpch | hash_join | 0.998 | 0.9359 |
| true | tpch | sort_merge_join | 5.443 | 1.0404 |
| true | futures | window | 0.660 | 0.9225 |
| true | iceberg | append_files | 0.788 | 1.0104 |
| true | iceberg | overwrite_partition | 0.167 | 1.0020 |
| true | iceberg | merge_updates | 0.607 | 0.9946 |
| false | futures | scan_filter | 0.004 | 0.3331 |
| false | tpch | scan_filter | 1.825 | 6.7024 |
| false | futures | group_by | 0.061 | 2.1203 |
| false | tpch | group_by | 1.924 | 3.5942 |
| false | tpch | hash_join | 3.115 | 2.9218 |
| false | tpch | sort_merge_join | 8.116 | 1.5514 |
| false | futures | window | 0.753 | 1.0528 |
| false | iceberg | append_files | 0.785 | 1.0065 |
| false | iceberg | overwrite_partition | 0.174 | 1.0475 |
| false | iceberg | merge_updates | 0.617 | 1.0101 |

**argmax:** `false` on futures/scan_filter (ratio 0.3331)

## `datafusion.execution.parquet.pushdown_filters`

Default `false`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.268 | 1.0000 |
| @default | futures | group_by | 0.027 | 1.0000 |
| @default | tpch | group_by | 0.516 | 1.0000 |
| @default | tpch | hash_join | 1.107 | 1.0000 |
| @default | tpch | sort_merge_join | 5.026 | 1.0000 |
| @default | futures | window | 0.726 | 1.0000 |
| @default | iceberg | append_files | 0.779 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.169 | 1.0000 |
| @default | iceberg | merge_updates | 0.627 | 1.0000 |
| true | futures | scan_filter | 0.012 | 0.9912 |
| true | tpch | scan_filter | 0.309 | 1.1551 |
| true | futures | group_by | 0.034 | 1.2667 |
| true | tpch | group_by | 0.525 | 1.0177 |
| true | tpch | hash_join | 1.937 | 1.7493 |
| true | tpch | sort_merge_join | 6.399 | 1.2731 |
| true | futures | window | 0.714 | 0.9842 |
| true | iceberg | append_files | 0.787 | 1.0102 |
| true | iceberg | overwrite_partition | 0.168 | 0.9974 |
| true | iceberg | merge_updates | 0.608 | 0.9690 |
| false | futures | scan_filter | 0.012 | 0.9827 |
| false | tpch | scan_filter | 0.281 | 1.0488 |
| false | futures | group_by | 0.071 | 2.6465 |
| false | tpch | group_by | 0.536 | 1.0393 |
| false | tpch | hash_join | 1.052 | 0.9499 |
| false | tpch | sort_merge_join | 5.558 | 1.1058 |
| false | futures | window | 0.759 | 1.0456 |
| false | iceberg | append_files | 0.802 | 1.0290 |
| false | iceberg | overwrite_partition | 0.170 | 1.0091 |
| false | iceberg | merge_updates | 0.604 | 0.9625 |

**argmax:** `false` on tpch/hash_join (ratio 0.9499)

## `datafusion.execution.parquet.enable_page_index`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.269 | 1.0000 |
| @default | futures | group_by | 0.027 | 1.0000 |
| @default | tpch | group_by | 0.531 | 1.0000 |
| @default | tpch | hash_join | 1.097 | 1.0000 |
| @default | tpch | sort_merge_join | 5.263 | 1.0000 |
| @default | futures | window | 0.679 | 1.0000 |
| @default | iceberg | append_files | 0.792 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.169 | 1.0000 |
| @default | iceberg | merge_updates | 0.592 | 1.0000 |
| true | futures | scan_filter | 0.011 | 0.9263 |
| true | tpch | scan_filter | 0.277 | 1.0307 |
| true | futures | group_by | 0.101 | 3.7156 |
| true | tpch | group_by | 0.524 | 0.9869 |
| true | tpch | hash_join | 1.108 | 1.0107 |
| true | tpch | sort_merge_join | 5.384 | 1.0229 |
| true | futures | window | 0.678 | 0.9995 |
| true | iceberg | append_files | 0.800 | 1.0105 |
| true | iceberg | overwrite_partition | 0.167 | 0.9871 |
| true | iceberg | merge_updates | 0.594 | 1.0038 |
| false | futures | scan_filter | 0.012 | 1.0281 |
| false | tpch | scan_filter | 0.263 | 0.9763 |
| false | futures | group_by | 0.035 | 1.2763 |
| false | tpch | group_by | 0.520 | 0.9798 |
| false | tpch | hash_join | 1.078 | 0.9828 |
| false | tpch | sort_merge_join | 5.705 | 1.0839 |
| false | futures | window | 0.659 | 0.9713 |
| false | iceberg | append_files | 0.789 | 0.9961 |
| false | iceberg | overwrite_partition | 0.171 | 1.0066 |
| false | iceberg | merge_updates | 0.611 | 1.0327 |

**argmax:** `true` on futures/scan_filter (ratio 0.9263)

## `datafusion.execution.parquet.bloom_filter_on_read`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.014 | 1.0000 |
| @default | tpch | scan_filter | 0.278 | 1.0000 |
| @default | futures | group_by | 0.030 | 1.0000 |
| @default | tpch | group_by | 0.538 | 1.0000 |
| @default | tpch | hash_join | 1.140 | 1.0000 |
| @default | tpch | sort_merge_join | 5.179 | 1.0000 |
| @default | futures | window | 0.656 | 1.0000 |
| @default | iceberg | append_files | 0.790 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.169 | 1.0000 |
| @default | iceberg | merge_updates | 0.591 | 1.0000 |
| true | futures | scan_filter | 0.011 | 0.7865 |
| true | tpch | scan_filter | 0.283 | 1.0200 |
| true | futures | group_by | 0.059 | 2.0143 |
| true | tpch | group_by | 0.524 | 0.9734 |
| true | tpch | hash_join | 1.024 | 0.8982 |
| true | tpch | sort_merge_join | 5.451 | 1.0525 |
| true | futures | window | 0.659 | 1.0048 |
| true | iceberg | append_files | 0.801 | 1.0142 |
| true | iceberg | overwrite_partition | 0.175 | 1.0409 |
| true | iceberg | merge_updates | 0.618 | 1.0456 |
| false | futures | scan_filter | 0.013 | 0.9085 |
| false | tpch | scan_filter | 0.315 | 1.1319 |
| false | futures | group_by | 0.067 | 2.2571 |
| false | tpch | group_by | 0.525 | 0.9751 |
| false | tpch | hash_join | 1.135 | 0.9955 |
| false | tpch | sort_merge_join | 5.841 | 1.1279 |
| false | futures | window | 0.800 | 1.2182 |
| false | iceberg | append_files | 0.787 | 0.9967 |
| false | iceberg | overwrite_partition | 0.171 | 1.0137 |
| false | iceberg | merge_updates | 0.603 | 1.0191 |

**argmax:** `true` on futures/scan_filter (ratio 0.7865)

## `repark.scan.concurrency_limit`

Default `unset (unlimited)`; values swept per D-2: unset plus two limits.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.011 | 1.0000 |
| @default | tpch | scan_filter | 0.265 | 1.0000 |
| @default | futures | group_by | 0.029 | 1.0000 |
| @default | tpch | group_by | 0.536 | 1.0000 |
| @default | tpch | hash_join | 1.146 | 1.0000 |
| @default | tpch | sort_merge_join | 5.323 | 1.0000 |
| @default | futures | window | 0.685 | 1.0000 |
| @default | iceberg | append_files | 0.775 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.166 | 1.0000 |
| @default | iceberg | merge_updates | 0.592 | 1.0000 |
| 4 | futures | scan_filter | 0.012 | 1.0450 |
| 4 | tpch | scan_filter | 0.304 | 1.1509 |
| 4 | futures | group_by | 0.060 | 2.0762 |
| 4 | tpch | group_by | 0.543 | 1.0117 |
| 4 | tpch | hash_join | 1.054 | 0.9198 |
| 4 | tpch | sort_merge_join | 5.453 | 1.0245 |
| 4 | futures | window | 0.643 | 0.9390 |
| 4 | iceberg | append_files | 0.815 | 1.0510 |
| 4 | iceberg | overwrite_partition | 0.170 | 1.0207 |
| 4 | iceberg | merge_updates | 0.602 | 1.0178 |
| 16 | futures | scan_filter | 0.015 | 1.3138 |
| 16 | tpch | scan_filter | 0.299 | 1.1289 |
| 16 | futures | group_by | 0.032 | 1.1077 |
| 16 | tpch | group_by | 0.538 | 1.0037 |
| 16 | tpch | hash_join | 1.147 | 1.0007 |
| 16 | tpch | sort_merge_join | 5.533 | 1.0395 |
| 16 | futures | window | 0.663 | 0.9674 |
| 16 | iceberg | append_files | 0.795 | 1.0252 |
| 16 | iceberg | overwrite_partition | 0.177 | 1.0648 |
| 16 | iceberg | merge_updates | 0.612 | 1.0345 |

**argmax:** `4` on tpch/hash_join (ratio 0.9198)

## `repark.batch.size`

Default `65536`; values swept per D-2: batch sizes x1/4, x1, x4.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.014 | 1.0000 |
| @default | tpch | scan_filter | 0.265 | 1.0000 |
| @default | futures | group_by | 0.031 | 1.0000 |
| @default | tpch | group_by | 0.526 | 1.0000 |
| @default | tpch | hash_join | 1.092 | 1.0000 |
| @default | tpch | sort_merge_join | 5.254 | 1.0000 |
| @default | futures | window | 0.669 | 1.0000 |
| @default | iceberg | append_files | 0.799 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.169 | 1.0000 |
| @default | iceberg | merge_updates | 0.608 | 1.0000 |
| 16384 | futures | scan_filter | 0.012 | 0.8676 |
| 16384 | tpch | scan_filter | 0.150 | 0.5666 |
| 16384 | futures | group_by | 0.031 | 1.0170 |
| 16384 | tpch | group_by | 0.175 | 0.3323 |
| 16384 | tpch | hash_join | 0.888 | 0.8139 |
| 16384 | tpch | sort_merge_join | 5.152 | 0.9805 |
| 16384 | futures | window | 0.667 | 0.9973 |
| 16384 | iceberg | append_files | 0.794 | 0.9931 |
| 16384 | iceberg | overwrite_partition | 0.169 | 1.0017 |
| 16384 | iceberg | merge_updates | 0.614 | 1.0096 |
| 262144 | futures | scan_filter | 0.012 | 0.8806 |
| 262144 | tpch | scan_filter | 0.328 | 1.2378 |
| 262144 | futures | group_by | 0.031 | 1.0117 |
| 262144 | tpch | group_by | 0.622 | 1.1832 |
| 262144 | tpch | hash_join | 1.187 | 1.0878 |
| 262144 | tpch | sort_merge_join | 5.724 | 1.0895 |
| 262144 | futures | window | 0.732 | 1.0951 |
| 262144 | iceberg | append_files | 0.788 | 0.9854 |
| 262144 | iceberg | overwrite_partition | 0.172 | 1.0169 |
| 262144 | iceberg | merge_updates | 0.640 | 1.0537 |

**argmax:** `16384` on tpch/group_by (ratio 0.3323)

## Write knobs

## `datafusion.execution.parquet.compression`

Default `zstd(3)`; values swept per D-2: every documented member.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.013 | 1.0000 |
| @default | tpch | scan_filter | 0.269 | 1.0000 |
| @default | futures | group_by | 0.030 | 1.0000 |
| @default | tpch | group_by | 0.531 | 1.0000 |
| @default | tpch | hash_join | 1.117 | 1.0000 |
| @default | tpch | sort_merge_join | 5.397 | 1.0000 |
| @default | futures | window | 0.705 | 1.0000 |
| @default | iceberg | append_files | 0.784 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.167 | 1.0000 |
| @default | iceberg | merge_updates | 0.599 | 1.0000 |
| uncompressed | futures | scan_filter | 0.013 | 0.9732 |
| uncompressed | tpch | scan_filter | 0.293 | 1.0906 |
| uncompressed | futures | group_by | 0.059 | 1.9543 |
| uncompressed | tpch | group_by | 0.545 | 1.0252 |
| uncompressed | tpch | hash_join | 1.070 | 0.9582 |
| uncompressed | tpch | sort_merge_join | 5.512 | 1.0213 |
| uncompressed | futures | window | 0.673 | 0.9545 |
| uncompressed | iceberg | append_files | 0.792 | 1.0109 |
| uncompressed | iceberg | overwrite_partition | 0.169 | 1.0152 |
| uncompressed | iceberg | merge_updates | 0.617 | 1.0299 |
| snappy | futures | scan_filter | 0.011 | 0.8684 |
| snappy | tpch | scan_filter | 0.296 | 1.1004 |
| snappy | futures | group_by | 0.026 | 0.8692 |
| snappy | tpch | group_by | 0.526 | 0.9909 |
| snappy | tpch | hash_join | 1.139 | 1.0199 |
| snappy | tpch | sort_merge_join | 5.685 | 1.0532 |
| snappy | futures | window | 0.760 | 1.0774 |
| snappy | iceberg | append_files | 0.852 | 1.0870 |
| snappy | iceberg | overwrite_partition | 0.171 | 1.0237 |
| snappy | iceberg | merge_updates | 0.601 | 1.0036 |
| gzip(6) | futures | scan_filter | 0.014 | 1.0846 |
| gzip(6) | tpch | scan_filter | 0.284 | 1.0579 |
| gzip(6) | futures | group_by | 0.085 | 2.8272 |
| gzip(6) | tpch | group_by | 0.524 | 0.9865 |
| gzip(6) | tpch | hash_join | 1.138 | 1.0187 |
| gzip(6) | tpch | sort_merge_join | 5.684 | 1.0531 |
| gzip(6) | futures | window | 0.671 | 0.9519 |
| gzip(6) | iceberg | append_files | 0.789 | 1.0074 |
| gzip(6) | iceberg | overwrite_partition | 0.176 | 1.0550 |
| gzip(6) | iceberg | merge_updates | 0.602 | 1.0042 |
| brotli(1) | futures | scan_filter | 0.013 | 0.9665 |
| brotli(1) | tpch | scan_filter | 0.320 | 1.1925 |
| brotli(1) | futures | group_by | 0.056 | 1.8646 |
| brotli(1) | tpch | group_by | 0.530 | 0.9977 |
| brotli(1) | tpch | hash_join | 1.103 | 0.9881 |
| brotli(1) | tpch | sort_merge_join | 5.728 | 1.0613 |
| brotli(1) | futures | window | 0.663 | 0.9395 |
| brotli(1) | iceberg | append_files | 0.791 | 1.0094 |
| brotli(1) | iceberg | overwrite_partition | 0.168 | 1.0052 |
| brotli(1) | iceberg | merge_updates | 0.599 | 1.0005 |
| lz4 | futures | scan_filter | 0.011 | 0.8788 |
| lz4 | tpch | scan_filter | 0.279 | 1.0396 |
| lz4 | futures | group_by | 0.032 | 1.0690 |
| lz4 | tpch | group_by | 0.536 | 1.0083 |
| lz4 | tpch | hash_join | 1.085 | 0.9712 |
| lz4 | tpch | sort_merge_join | 5.761 | 1.0673 |
| lz4 | futures | window | 0.675 | 0.9569 |
| lz4 | iceberg | append_files | 0.818 | 1.0437 |
| lz4 | iceberg | overwrite_partition | 0.170 | 1.0169 |
| lz4 | iceberg | merge_updates | 0.611 | 1.0204 |
| zstd(3) | futures | scan_filter | 0.011 | 0.8656 |
| zstd(3) | tpch | scan_filter | 0.290 | 1.0803 |
| zstd(3) | futures | group_by | 0.059 | 1.9683 |
| zstd(3) | tpch | group_by | 0.526 | 0.9898 |
| zstd(3) | tpch | hash_join | 1.082 | 0.9693 |
| zstd(3) | tpch | sort_merge_join | 5.751 | 1.0655 |
| zstd(3) | futures | window | 0.701 | 0.9941 |
| zstd(3) | iceberg | append_files | 0.793 | 1.0123 |
| zstd(3) | iceberg | overwrite_partition | 0.170 | 1.0217 |
| zstd(3) | iceberg | merge_updates | 0.604 | 1.0079 |
| lz4_raw | futures | scan_filter | 0.012 | 0.9163 |
| lz4_raw | tpch | scan_filter | 0.296 | 1.1003 |
| lz4_raw | futures | group_by | 0.052 | 1.7216 |
| lz4_raw | tpch | group_by | 0.543 | 1.0220 |
| lz4_raw | tpch | hash_join | 1.198 | 1.0726 |
| lz4_raw | tpch | sort_merge_join | 5.712 | 1.0582 |
| lz4_raw | futures | window | 0.685 | 0.9709 |
| lz4_raw | iceberg | append_files | 0.813 | 1.0375 |
| lz4_raw | iceberg | overwrite_partition | 0.168 | 1.0050 |
| lz4_raw | iceberg | merge_updates | 0.609 | 1.0164 |

**argmax:** `zstd(3)` on futures/scan_filter (ratio 0.8656)

## `datafusion.execution.parquet.max_row_group_size`

Default `1048576`; values swept per D-2: half / double of default.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.271 | 1.0000 |
| @default | futures | group_by | 0.030 | 1.0000 |
| @default | tpch | group_by | 0.540 | 1.0000 |
| @default | tpch | hash_join | 1.104 | 1.0000 |
| @default | tpch | sort_merge_join | 5.207 | 1.0000 |
| @default | futures | window | 0.668 | 1.0000 |
| @default | iceberg | append_files | 0.784 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.169 | 1.0000 |
| @default | iceberg | merge_updates | 0.617 | 1.0000 |
| 524288 | futures | scan_filter | 0.011 | 0.9152 |
| 524288 | tpch | scan_filter | 0.281 | 1.0369 |
| 524288 | futures | group_by | 0.068 | 2.2730 |
| 524288 | tpch | group_by | 0.564 | 1.0444 |
| 524288 | tpch | hash_join | 1.050 | 0.9509 |
| 524288 | tpch | sort_merge_join | 5.437 | 1.0441 |
| 524288 | futures | window | 0.661 | 0.9890 |
| 524288 | iceberg | append_files | 0.783 | 0.9988 |
| 524288 | iceberg | overwrite_partition | 0.172 | 1.0180 |
| 524288 | iceberg | merge_updates | 0.592 | 0.9601 |
| 2097152 | futures | scan_filter | 0.013 | 1.0909 |
| 2097152 | tpch | scan_filter | 0.291 | 1.0768 |
| 2097152 | futures | group_by | 0.064 | 2.1424 |
| 2097152 | tpch | group_by | 0.545 | 1.0084 |
| 2097152 | tpch | hash_join | 1.080 | 0.9786 |
| 2097152 | tpch | sort_merge_join | 5.762 | 1.1065 |
| 2097152 | futures | window | 0.667 | 0.9991 |
| 2097152 | iceberg | append_files | 0.795 | 1.0137 |
| 2097152 | iceberg | overwrite_partition | 0.171 | 1.0145 |
| 2097152 | iceberg | merge_updates | 0.608 | 0.9848 |

**argmax:** `524288` on futures/scan_filter (ratio 0.9152)

## `datafusion.execution.parquet.bloom_filter_on_write`

Default `false`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.266 | 1.0000 |
| @default | futures | group_by | 0.031 | 1.0000 |
| @default | tpch | group_by | 0.529 | 1.0000 |
| @default | tpch | hash_join | 1.121 | 1.0000 |
| @default | tpch | sort_merge_join | 5.176 | 1.0000 |
| @default | futures | window | 0.667 | 1.0000 |
| @default | iceberg | append_files | 0.773 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.166 | 1.0000 |
| @default | iceberg | merge_updates | 0.598 | 1.0000 |
| true | futures | scan_filter | 0.011 | 0.9136 |
| true | tpch | scan_filter | 0.306 | 1.1471 |
| true | futures | group_by | 0.034 | 1.0860 |
| true | tpch | group_by | 0.530 | 1.0006 |
| true | tpch | hash_join | 1.008 | 0.8994 |
| true | tpch | sort_merge_join | 5.467 | 1.0563 |
| true | futures | window | 0.723 | 1.0837 |
| true | iceberg | append_files | 0.784 | 1.0132 |
| true | iceberg | overwrite_partition | 0.169 | 1.0153 |
| true | iceberg | merge_updates | 0.591 | 0.9890 |
| false | futures | scan_filter | 0.011 | 0.8433 |
| false | tpch | scan_filter | 0.297 | 1.1134 |
| false | futures | group_by | 0.031 | 0.9980 |
| false | tpch | group_by | 0.538 | 1.0171 |
| false | tpch | hash_join | 1.067 | 0.9516 |
| false | tpch | sort_merge_join | 5.688 | 1.0988 |
| false | futures | window | 0.659 | 0.9882 |
| false | iceberg | append_files | 0.794 | 1.0260 |
| false | iceberg | overwrite_partition | 0.169 | 1.0167 |
| false | iceberg | merge_updates | 0.611 | 1.0230 |

**argmax:** `false` on futures/scan_filter (ratio 0.8433)

## `datafusion.execution.parquet.write_batch_size`

Default `1024`; values swept per D-2: half / double of default.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.014 | 1.0000 |
| @default | tpch | scan_filter | 0.267 | 1.0000 |
| @default | futures | group_by | 0.026 | 1.0000 |
| @default | tpch | group_by | 0.528 | 1.0000 |
| @default | tpch | hash_join | 1.078 | 1.0000 |
| @default | tpch | sort_merge_join | 5.276 | 1.0000 |
| @default | futures | window | 0.756 | 1.0000 |
| @default | iceberg | append_files | 0.796 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.169 | 1.0000 |
| @default | iceberg | merge_updates | 0.589 | 1.0000 |
| 512 | futures | scan_filter | 0.010 | 0.7410 |
| 512 | tpch | scan_filter | 0.288 | 1.0808 |
| 512 | futures | group_by | 0.037 | 1.4453 |
| 512 | tpch | group_by | 0.528 | 1.0012 |
| 512 | tpch | hash_join | 1.049 | 0.9737 |
| 512 | tpch | sort_merge_join | 5.370 | 1.0179 |
| 512 | futures | window | 0.679 | 0.8980 |
| 512 | iceberg | append_files | 0.810 | 1.0181 |
| 512 | iceberg | overwrite_partition | 0.170 | 1.0062 |
| 512 | iceberg | merge_updates | 0.600 | 1.0195 |
| 2048 | futures | scan_filter | 0.012 | 0.9021 |
| 2048 | tpch | scan_filter | 0.304 | 1.1399 |
| 2048 | futures | group_by | 0.058 | 2.2205 |
| 2048 | tpch | group_by | 0.541 | 1.0247 |
| 2048 | tpch | hash_join | 1.062 | 0.9856 |
| 2048 | tpch | sort_merge_join | 5.773 | 1.0944 |
| 2048 | futures | window | 0.656 | 0.8673 |
| 2048 | iceberg | append_files | 0.794 | 0.9979 |
| 2048 | iceberg | overwrite_partition | 0.168 | 0.9975 |
| 2048 | iceberg | merge_updates | 0.614 | 1.0438 |

**argmax:** `512` on futures/scan_filter (ratio 0.7410)

## `write.target-file-size-bytes`

Default `536870912`; values swept per D-2: half / double of default.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.277 | 1.0000 |
| @default | futures | group_by | 0.027 | 1.0000 |
| @default | tpch | group_by | 0.523 | 1.0000 |
| @default | tpch | hash_join | 1.074 | 1.0000 |
| @default | tpch | sort_merge_join | 5.145 | 1.0000 |
| @default | futures | window | 0.692 | 1.0000 |
| @default | iceberg | append_files | 0.795 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.167 | 1.0000 |
| @default | iceberg | merge_updates | 0.596 | 1.0000 |
| 268435456 | futures | scan_filter | 0.011 | 0.9360 |
| 268435456 | tpch | scan_filter | 0.289 | 1.0464 |
| 268435456 | futures | group_by | 0.069 | 2.5227 |
| 268435456 | tpch | group_by | 0.535 | 1.0230 |
| 268435456 | tpch | hash_join | 1.124 | 1.0471 |
| 268435456 | tpch | sort_merge_join | 5.388 | 1.0473 |
| 268435456 | futures | window | 0.671 | 0.9706 |
| 268435456 | iceberg | append_files | 0.792 | 0.9966 |
| 268435456 | iceberg | overwrite_partition | 0.170 | 1.0128 |
| 268435456 | iceberg | merge_updates | 0.602 | 1.0100 |
| 1073741824 | futures | scan_filter | 0.011 | 0.9444 |
| 1073741824 | tpch | scan_filter | 0.276 | 0.9970 |
| 1073741824 | futures | group_by | 0.060 | 2.2011 |
| 1073741824 | tpch | group_by | 0.543 | 1.0382 |
| 1073741824 | tpch | hash_join | 1.109 | 1.0329 |
| 1073741824 | tpch | sort_merge_join | 5.898 | 1.1464 |
| 1073741824 | futures | window | 0.668 | 0.9661 |
| 1073741824 | iceberg | append_files | 0.796 | 1.0014 |
| 1073741824 | iceberg | overwrite_partition | 0.168 | 1.0014 |
| 1073741824 | iceberg | merge_updates | 0.621 | 1.0417 |

**argmax:** `268435456` on futures/scan_filter (ratio 0.9360)

## `write.distribution-mode`

Default `unset (behaves as hash/range)`; values swept per D-2: every documented member.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.013 | 1.0000 |
| @default | tpch | scan_filter | 0.282 | 1.0000 |
| @default | futures | group_by | 0.030 | 1.0000 |
| @default | tpch | group_by | 0.523 | 1.0000 |
| @default | tpch | hash_join | 1.104 | 1.0000 |
| @default | tpch | sort_merge_join | 5.441 | 1.0000 |
| @default | futures | window | 0.710 | 1.0000 |
| @default | iceberg | append_files | 0.772 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.181 | 1.0000 |
| @default | iceberg | merge_updates | 0.629 | 1.0000 |
| none | futures | scan_filter | 0.011 | 0.8737 |
| none | tpch | scan_filter | 0.304 | 1.0776 |
| none | futures | group_by | 0.044 | 1.4785 |
| none | tpch | group_by | 0.537 | 1.0258 |
| none | tpch | hash_join | 1.109 | 1.0045 |
| none | tpch | sort_merge_join | 5.646 | 1.0377 |
| none | futures | window | 0.678 | 0.9552 |
| none | iceberg | append_files | 0.800 | 1.0356 |
| none | iceberg | overwrite_partition | 0.169 | 0.9333 |
| none | iceberg | merge_updates | 0.769 | 1.2217 |
| hash | futures | scan_filter | 0.012 | 0.9381 |
| hash | tpch | scan_filter | 0.285 | 1.0107 |
| hash | futures | group_by | 0.034 | 1.1646 |
| hash | tpch | group_by | 0.531 | 1.0149 |
| hash | tpch | hash_join | 1.014 | 0.9188 |
| hash | tpch | sort_merge_join | 5.739 | 1.0548 |
| hash | futures | window | 0.747 | 1.0525 |
| hash | iceberg | append_files | 0.796 | 1.0301 |
| hash | iceberg | overwrite_partition | 0.172 | 0.9526 |
| hash | iceberg | merge_updates | 0.611 | 0.9708 |
| range | futures | scan_filter | 0.011 | 0.8680 |
| range | tpch | scan_filter | 0.314 | 1.1139 |
| range | futures | group_by | 0.084 | 2.8360 |
| range | tpch | group_by | 0.529 | 1.0105 |
| range | tpch | hash_join | 1.107 | 1.0023 |
| range | tpch | sort_merge_join | 5.677 | 1.0435 |
| range | futures | window | 0.759 | 1.0690 |
| range | iceberg | append_files | 0.788 | 1.0200 |
| range | iceberg | overwrite_partition | 0.180 | 0.9956 |
| range | iceberg | merge_updates | 0.710 | 1.1286 |

**argmax:** `range` on futures/scan_filter (ratio 0.8680)

## `repark.merge.file_scoped_rewrite`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.012 | 1.0000 |
| @default | tpch | scan_filter | 0.312 | 1.0000 |
| @default | futures | group_by | 0.036 | 1.0000 |
| @default | tpch | group_by | 0.566 | 1.0000 |
| @default | tpch | hash_join | 1.107 | 1.0000 |
| @default | tpch | sort_merge_join | 5.122 | 1.0000 |
| @default | futures | window | 0.666 | 1.0000 |
| @default | iceberg | append_files | 0.787 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.168 | 1.0000 |
| @default | iceberg | merge_updates | 0.598 | 1.0000 |
| true | futures | scan_filter | 0.012 | 1.0583 |
| true | tpch | scan_filter | 0.306 | 0.9828 |
| true | futures | group_by | 0.039 | 1.0923 |
| true | tpch | group_by | 0.538 | 0.9507 |
| true | tpch | hash_join | 1.123 | 1.0149 |
| true | tpch | sort_merge_join | 5.698 | 1.1123 |
| true | futures | window | 0.668 | 1.0030 |
| true | iceberg | append_files | 0.783 | 0.9950 |
| true | iceberg | overwrite_partition | 0.166 | 0.9924 |
| true | iceberg | merge_updates | 0.593 | 0.9915 |
| false | futures | scan_filter | 0.011 | 0.9829 |
| false | tpch | scan_filter | 0.299 | 0.9590 |
| false | futures | group_by | 0.059 | 1.6411 |
| false | tpch | group_by | 0.520 | 0.9186 |
| false | tpch | hash_join | 1.323 | 1.1948 |
| false | tpch | sort_merge_join | 5.723 | 1.1173 |
| false | futures | window | 0.690 | 1.0347 |
| false | iceberg | append_files | 0.799 | 1.0152 |
| false | iceberg | overwrite_partition | 0.170 | 1.0161 |
| false | iceberg | merge_updates | 1.106 | 1.8483 |

**argmax:** `false` on tpch/group_by (ratio 0.9186)

## `repark.merge.scan_pruning`

Default `true`; values swept per D-2: booleans both ways.

| value | dataset | query | median s | ratio to @default |
|---|---|---|---|---|
| @default | futures | scan_filter | 0.014 | 1.0000 |
| @default | tpch | scan_filter | 0.270 | 1.0000 |
| @default | futures | group_by | 0.025 | 1.0000 |
| @default | tpch | group_by | 0.528 | 1.0000 |
| @default | tpch | hash_join | 1.149 | 1.0000 |
| @default | tpch | sort_merge_join | 5.225 | 1.0000 |
| @default | futures | window | 0.712 | 1.0000 |
| @default | iceberg | append_files | 0.782 | 1.0000 |
| @default | iceberg | overwrite_partition | 0.167 | 1.0000 |
| @default | iceberg | merge_updates | 0.594 | 1.0000 |
| true | futures | scan_filter | 0.013 | 0.9147 |
| true | tpch | scan_filter | 0.276 | 1.0205 |
| true | futures | group_by | 0.087 | 3.5080 |
| true | tpch | group_by | 0.553 | 1.0468 |
| true | tpch | hash_join | 1.033 | 0.8995 |
| true | tpch | sort_merge_join | 5.416 | 1.0364 |
| true | futures | window | 0.710 | 0.9972 |
| true | iceberg | append_files | 0.794 | 1.0156 |
| true | iceberg | overwrite_partition | 0.172 | 1.0334 |
| true | iceberg | merge_updates | 0.625 | 1.0524 |
| false | futures | scan_filter | 0.012 | 0.8708 |
| false | tpch | scan_filter | 0.293 | 1.0851 |
| false | futures | group_by | 0.076 | 3.0728 |
| false | tpch | group_by | 0.538 | 1.0190 |
| false | tpch | hash_join | 1.039 | 0.9047 |
| false | tpch | sort_merge_join | 5.867 | 1.1227 |
| false | futures | window | 0.717 | 1.0070 |
| false | iceberg | append_files | 0.793 | 1.0142 |
| false | iceberg | overwrite_partition | 0.169 | 1.0116 |
| false | iceberg | merge_updates | 0.869 | 1.4638 |

**argmax:** `false` on futures/scan_filter (ratio 0.8708)

## No effect measured

A knob lands here when every cell the knob can reach stays within 5 % of its `@default` cell — the card's 5 % rule applied to the knob's affected cells (see Method). This table is step 3's input.

| knob | max |ratio − 1| on affected cells |
|---|---|
| `repark.scan.concurrency_limit` | 0.0345 |
| `datafusion.execution.parquet.max_row_group_size` | 0.0399 |
| `datafusion.execution.parquet.bloom_filter_on_write` | 0.0153 |
| `datafusion.execution.parquet.write_batch_size` | 0.0438 |
| `write.target-file-size-bytes` | 0.0417 |

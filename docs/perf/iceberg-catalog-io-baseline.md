# Iceberg catalog-IO baseline (PERF-ICE-CATALOG-IO-1)

Measured 2026-09-05 on the lane `$HOME/repark-lanes/lanes/oc-catio` (branch
`perf/ice-catalog-io-1`, base `origin/main` `6eaccd5e`). Cites
[engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md) §2 rows 6 and 11,
§5 item 7, §7.4 and §7.6, and re-runs their shapes before and after.

pins: perf-ice-catalog-io-1/C-001, C-005, C-006

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X, 64 threads |
| ram | 125.7 GiB · governor `schedutil` · kernel 6.8.0-138 |
| native | `_native.abi3.so` 163,488,736 B, `__debug_assertions__ is False` (every probe refuses otherwise) |
| build | `CARGO_BUILD_JOBS=8 maturin develop --release` |
| repark / DataFusion / arrow | 1.0.1 / 54.1.0 / 58.4 |
| fork pin | `189a73ed` (unchanged; the fork-gated rows below name their temporary path override) |
| threads | `spark.sql.shuffle.partitions = 8` |
| iterations | 5 timed after 1 warm-up; median, min, spread per cell; 1-minute load at start and end |

**Not a quiet box.** Every timing run below carries its own 1-minute load and its own re-measured
floor, and a cost is only ever read against the floor of the run it came from. A sibling lane's
`cargo` build was live throughout (load 7–12).

## 1. The file-open census (`strace -f -e trace=openat`, ENOENT excluded)

One statement per marker pair, memory catalog on a local-FS warehouse, format-v3 table, 20 k
rows. `metaR` counts `metadata.json` opened for **reading**; `metaW` counts the commit writing its
own new pointer, which no cache can remove. Analysis §7.6 is the `before` column and it reproduced
exactly.

| statement | metaR before | metaR after | metaW after | manifest-list | manifest | parquet |
|---|---:|---:|---:|---:|---:|---:|
| `SELECT count(*)` | 2 | **0** | 0 | 1 | 1 | 1 |
| `SELECT count(*)` (repeat) | 2 | **0** | 0 | 1 | 1 | 1 |
| `SELECT … WHERE part = 3 AND vi < 10` | 2 | **0** | 0 | 1 | 1 | 1 |
| `INSERT … SELECT` | 4 | **0** | 1 | 2 | 1 | 0 |
| `SELECT count(*)` after the insert | 2 | **0** | 0 | 1 | 2 | 2 |
| `DELETE` MoR v3 | 5 | **0** | 1 | 4 | 8 | 4 |
| `UPDATE` MoR v3 | 6 | **0** | 1 | 5 | 15 | 8 |
| `MERGE` MoR v3 | 3 | **0** | 1 | 4 | 8 | 3 |
| `SELECT count(*)` tail | 2 | **0** | 0 | 1 | 2 | 4 |

Targets were SELECT ≤ 1 and DML ≤ 2; the measured result is **0 metadata reads on every
statement**. Manifest-list and manifest columns are unchanged, which is the point of §2 below.

**What this is on a real catalog.** The cache removes the S3 GET of the metadata document, not the
catalog round trip that learns where the document is. Counting by the same census method, a Glue
catalog pays, per statement:

| statement | `GetTable` before | `GetTable` after | S3 GET of `metadata.json` before | after |
|---|---:|---:|---:|---:|
| SELECT | 2 | 2 | 2 | **0** |
| INSERT | 5 | 5 | 4 | **0** |
| DELETE | 6 | 6 | 5 | **0** |
| UPDATE | 7 | 7 | 6 | **0** |
| MERGE | 4 | 4 | 3 | **0** |

The `GetTable` column is unchanged because cutting it is part 1, which is fork-gated (§3). The
AWS legs of `python/repark/tests/test_perf_ice_catalog_io_1.py` are written and SKIP naming that
reason; the wall-clock evidence for Glue latency stays the recorded suite walls in
[../tier2-aws.md](../tier2-aws.md), since this unit measures no AWS.

## 2. The manifest cells (`t_many` vs `t_many_merged`, 1e6 rows)

`t_many` is 208 data files across **193** manifests; `t_many_merged` is the same 208 files after
`rewrite_manifests`, at **1** manifest. Both cells are the SECOND statement on the table, which is
the one the shared manifest cache is supposed to make free. Both columns are the same release
module in back-to-back runs; the only variable is `repark.iceberg.metadataCache`.

| cell | cache off (ms) | cache on (ms) | floor (ms) |
|---|---:|---:|---:|
| `t_many/count_id/stmt1` | 123.73 | 119.27 | — |
| `t_many/count_id/stmt2` | **120.35** | **120.01** | 1.10 / 0.30 |
| `t_many/point/stmt2` | 118.49 | 121.16 | — |
| `t_many_merged/count_id/stmt2` | 17.63 | 14.91 | — |
| `t_many_merged/point/stmt2` | 20.52 | 17.72 | — |

Load at the cache-off run 10.1 → 9.7; at the cache-on run 9.7 → 7.0.

**The metadata cache does not move this cell, and was never going to.** The 192-manifest delta is
~103 ms of manifest reads through a fresh `ObjectCache` per `Table`, and a metadata-location cache
removes two small `metadata.json` reads. The `t_many_merged` cells drop 17.6 → 14.9 and 20.5 →
17.7, which is those two reads and is at the edge of the run's floor. The ≤ 20 ms target for
`t_many` needs part 3.

## 3. Parts 1 and 3: measured through a temporary path override, not shipped

Both need the fork, and this unit does not bump the pin (`git diff origin/main -- Cargo.toml
Cargo.lock` is empty). They were implemented in the fork lane
`$HOME/repark-lanes/lanes/catio-fork` at `fork/catio-io` (base `189a73ed`) and consumed through a
`[patch.crates-io]` path override that was reverted after measurement.

- **`F-CATIO-A` — one load per planning round.** `IcebergSchemaProvider::resolve_table` hands the
  `Table` it just loaded to the provider it returns, and `TableProvider::scan` reuses it.
  `try_new` is untouched, so only the catalog-resolved path (one per planning round) reuses; a
  user holding a provider still reloads on every scan. This is also a latent correctness fix: the
  provider's `schema` is fixed at construction and DataFusion stores ordinals against it, so a
  schema change between `try_new` and `scan` currently plans one snapshot against another's
  ordinals.
- **`F-CATIO-B` — a shared, bounded, path-keyed manifest cache.** `TableBuilder::object_cache`
  injects an `Arc<ObjectCache>` and `MemoryCatalogBuilder::with_shared_object_cache_bytes` builds
  one per catalog, so manifests and manifest lists survive across `Table` instances instead of
  dying with each `load_table`. Safe by construction: `ObjectCache` is already keyed by
  `(path, format_version, schema_id)` for a manifest list and `(path, schema_id)` for a manifest,
  and both objects are immutable at their path — a `rewrite_manifests` or `expire_snapshots`
  writes NEW paths, and a deleted path is never asked for again. It is already bounded: moka
  weighted eviction, default 32 MiB, sized here by the catalog builder.
- **`F-CATIO-AWS` — the Glue and S3 Tables metadata cache.** Not implemented. Their builders take
  no `with_table_metadata_cache` at `189a73ed`, so §1's AWS table's `after` column is the memory
  catalog's shape, argued by the census method, not measured on AWS.

Measured numbers for A and B are in §4 of this file's "after the override" table once the pin bump
lands; the fork lane carries the change and its tests
(`cargo test -p iceberg --lib` 3,612 passed; `cargo test -p iceberg-datafusion` all green).

## 4. Commands

```bash
cd $HOME/repark-lanes/lanes/oc-catio/python/repark && \
  VIRTUAL_ENV=$HOME/repark-lanes/lanes/oc-catio/.venv CARGO_BUILD_JOBS=8 \
  $HOME/repark-lanes/lanes/oc-catio/.venv/bin/maturin develop --release
cd $HOME/repark-lanes/lanes/oc-catio
strace -f -e trace=openat -o scratch/strace_after.txt .venv/bin/python scratch/probes/probe_calls.py
.venv/bin/python scratch/probes/count_calls.py scratch/strace_after.txt scratch/census_after.json
.venv/bin/python scratch/probes/probe_manifest.py cacheoff
.venv/bin/python scratch/probes/probe_manifest.py cacheon
.venv/bin/python -m pytest python/repark/tests/test_perf_ice_catalog_io_1.py -q
```

Probe sources live under `scratch/probes/` (gitignored via `.git/info/exclude`, never committed;
they carry no comments). The census probe marks every statement with a
`scratch/marks/REPARKMARK_<label>_{begin,end}` file so the tracer can be cut into statements.

## Pointers

- Up: [map.md](map.md)
- The analysis this unit cites: [engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md)
- The knobs: [../../crates/repark-iceberg/src/catalog/map.md](../../crates/repark-iceberg/src/catalog/map.md)

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

One statement per marker pair, **memory catalog** on a local-FS warehouse, format-v3 table, 20 k
rows. `metaR` counts `metadata.json` opened for **reading**; `metaW` counts the commit writing its
own new pointer, which no cache can remove. Both columns are the same release module, one run per
knob setting, so the only variable is `repark.iceberg.metadataCache`.

Analysis §7.6 reports TOTAL `metadata.json` opens per statement (SELECT 2, INSERT 5, DELETE 6,
UPDATE 7, MERGE 4, CTAS 3). This table splits that into reads and the commit's own write, so its
`before` column is **reads only** and is smaller than §7.6's numbers by exactly the `metaW`
column. Reads plus writes reproduce §7.6 exactly.

| statement | metaR off | metaR on | metaW | manifest-list | manifest | parquet |
|---|---:|---:|---:|---:|---:|---:|
| `CREATE TABLE` | 1 | **1** | 1 | 0 | 0 | 0 |
| `CREATE TABLE … AS SELECT` | 1 | **1** | 2 | 0 | 0 | 0 |
| `SELECT count(*)` | 2 | **0** | 0 | 1 | 1 | 1 |
| `SELECT count(*)` (repeat) | 2 | **0** | 0 | 1 | 1 | 1 |
| `SELECT … WHERE part = 3 AND vi < 10` | 2 | **0** | 0 | 1 | 1 | 1 |
| `INSERT … SELECT` | 4 | **0** | 1 | 2 | 1 | 0 |
| `SELECT count(*)` after the insert | 2 | **0** | 0 | 1 | 2 | 2 |
| `DELETE` MoR v3 | 5 | **0** | 1 | 4 | 8 | 4 |
| `UPDATE` MoR v3 | 6 | **0** | 1 | 5 | 15 | 8 |
| `MERGE` MoR v3 | 3 | **0** | 1 | 4 | 8 | 3 |
| `SELECT count(*)` tail | 2 | **0** | 0 | 1 | 2 | 4 |
| `SELECT count(*)` tail (repeat) | 2 | **0** | 0 | 1 | 2 | 4 |

Targets were SELECT ≤ 1 and DML ≤ 2. The measured result is **0 metadata reads on every statement
that reads an existing table**, and **1 on a statement that creates one, with the cache on and
off alike**: `CREATE TABLE` and CTAS read back the document they have just written, because the
catalog proves the metadata is reachable before it claims the pointer. Creation cannot be cached
and this unit does not claim it is. Manifest-list and manifest columns are unchanged, which is the
point of §2.

**What this is on a real catalog — and what it is not.** Only `memory_catalog_cached` takes a
`CatalogCaches`. `glue_catalog` and `s3tables_catalog` are **unchanged**: the fork's
`GlueCatalogBuilder` / `S3TablesCatalogBuilder` have no `with_table_metadata_cache` at pin
`189a73ed`, so no cache reaches them and nothing in this unit changes what they pay. Counting by
the same census method, per statement:

| statement | Glue `GetTable` today | S3 GET of `metadata.json` today | after this unit |
|---|---:|---:|---|
| SELECT | 2 | 2 | unchanged (both) |
| INSERT | 5 | 4 | unchanged (both) |
| DELETE | 6 | 5 | unchanged (both) |
| UPDATE | 7 | 6 | unchanged (both) |
| MERGE | 4 | 3 | unchanged (both) |

Two separate asks stand between that table and a zero. `F-CATIO-AWS`
(`PERF-CATALOG-AWS-CACHE-1`) would let a Glue or S3 Tables catalog take the cache, which is what
would move the S3-GET column. `F-CATIO-A` (`PERF-CATALOG-LOADS-1`) would cut the `GetTable`
column, which no cache can touch: the count of round trips per statement is the count of
`load_table` calls, and on the memory catalog those are now **cache hits with the same count** —
2 per SELECT, 3–6 per DML. The AWS legs of
`python/repark/tests/test_perf_ice_catalog_io_1.py` are written and SKIP naming `F-CATIO-AWS`.
The wall-clock evidence for Glue latency stays the recorded suite walls in
[../tier2-aws.md](../tier2-aws.md); this unit measures no AWS.

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

Both are test-green in the fork lane: `cargo test -p iceberg --lib` 3,612 passed / 0 failed and
`cargo test -p iceberg-datafusion` green including doctests, at the fork lane's own toolchain. A
RePark facade run against the override reached 50 % of `python/repark/tests` with no failure
before the run was cut short; a complete facade run against the override is part of the pin bump,
not of this unit, and this note claims nothing more than the two fork suites and the cells below.

### 3.1 What A and B measure (release module, path override, same box, same hour)

| cell | shipped (ms) | with A + B (ms) | target |
|---|---:|---:|---|
| `t_many/count_id/stmt2` (193 manifests) | 120.01 | **11.33** | ≤ 20 |
| `t_many/point/stmt2` | 121.16 | **14.49** | — |
| `t_many_merged/count_id/stmt2` (1 manifest) | 14.91 | **10.56** | — |
| `t_many_merged/point/stmt2` | 17.72 | **13.19** | — |

Floor 0.87 ms, load 10.3 → 9.9. Two things separate: the 193-manifest table stops paying for its
manifests at all (120.0 → 11.3, and it now sits within 0.8 ms of its one-manifest twin, so the
manifest penalty is **gone**, not reduced), and the one-manifest twin still drops 14.9 → 10.6,
which is part 1 — the planning round that used to load the table twice now loads it once.

### 3.2 The census with A + B

| statement | manifest-list shipped → override | manifest shipped → override |
|---|---:|---:|
| `SELECT count(*)` (first) | 1 → 1 | 1 → 1 |
| `SELECT count(*)` (repeat) | 1 → **0** | 1 → **0** |
| `SELECT … WHERE …` | 1 → **0** | 1 → **0** |
| `DELETE` MoR v3 | 4 → 3 | 8 → **6** |
| `UPDATE` MoR v3 | 5 → 4 | 15 → **12** |
| `SELECT count(*)` tail (repeat) | 1 → **0** | 2 → **0** |

`metadata.json` reads stay 0 everywhere, which is the shipped half doing its job. A repeated read
now opens **nothing at all** except the parquet it must decode.

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

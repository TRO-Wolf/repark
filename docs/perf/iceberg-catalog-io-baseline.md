# Iceberg catalog-IO baseline (PERF-ICE-CATALOG-IO-1)

Measured 2026-09-05 on the lane `$HOME/repark-lanes/lanes/oc-catio` (branch
`perf/ice-catalog-io-1`, base `origin/main` `6eaccd5e`). Cites
[engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md) §2 rows 6 and 11,
§5 item 7, §7.4 and §7.6, and re-runs their shapes before and after.

pins: perf-ice-catalog-io-1/C-001, C-005, C-006
pins: perf-ice-catalog-io-2/C-006
pins: perf-ice-catalog-io-3/C-006

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
`load_table` calls, and those are unchanged — the cache turns them into hits, not into fewer
calls. Measured through the census counter (`hits + misses` per statement, cache on): **SELECT 2,
SELECT-with-filter 2, INSERT 4, DELETE 5, UPDATE 6, MERGE 3** — the same numbers as the knob-off
metadata-read column above, which is exactly the point. The AWS legs of
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

## 5. Part 3 re-measured on the real pin (PERF-ICE-CATALOG-IO-2, 2026-09-05)

Measured 2026-09-05 on the lane `$HOME/repark-lanes/lanes/oc-catio2` (branch
`perf/ice-catalog-io-2`, base `origin/main` `7bef4afd`, fork pin `79119643`, no path
override). Same box as §1–§4 (Threadripper 3970X, 64 threads, 125.7 GiB, governor
`schedutil`, kernel 6.8.0-138), same versions (repark 1.0.1 / DataFusion 54.1.0 / arrow
58.4), same shapes (the §1 20 k-row census table, the §2 1e6-row 208-file tables at 193
vs 1 manifests — the counts reproduce exactly), same method (5 timed after 1 warm-up;
median, min, spread; floor per run; `strace -f -e trace=openat`, ENOENT excluded). The
native module is `_native.abi3.so` 163,517,296 B with `__debug_assertions__ is False`
(every probe refuses otherwise). The only variable between the two columns is
`repark.iceberg.manifestCacheBytes`: `0` (off — the shipped default) vs `33554432`
(on, set explicitly). The metadata
cache is ON in both columns, so the `off` column is IO-1's shipped `on` column re-run on
the new pin — and it reproduces it cell for cell.

**Not a quiet box, again.** Load 14.5–15.9 through the timing runs (a sibling lane's
build was live). Every cell carries its run's floor and load; a cost is only ever read
against its own run.

### 5.1 The timing cells

| cell | manifest off = default (ms) | manifest on, knob = 32 MiB, set explicitly (ms) | override §3.1 (ms) | target |
|---|---:|---:|---:|---|
| `t_many/count_id/stmt1` | 123.02 (spread 9.52) | 10.73 (spread 2.17) | — | — |
| `t_many/count_id/stmt2` | **115.81** (spread 23.00) | **10.95** (spread 1.22) | 11.33 | ≤ 20 |
| `t_many/point/stmt2` | 124.75 (spread 18.87) | 14.75 (spread 1.18) | 14.49 | — |
| `t_many_merged/count_id/stmt2` | 14.37 (spread 2.00) | 10.49 (spread 1.29) | 10.56 | — |
| `t_many_merged/point/stmt2` | 17.85 (spread 4.77) | 13.20 (spread 0.69) | 13.19 | — |

Floors 0.31 (off) / 1.13 (on); load 14.5 → 14.5 (off) and 15.9 → 15.9 (on). The
`t_many` second statement clears the target at roughly half of it, and it now sits
within 0.5 ms of its one-manifest twin (10.95 vs 10.49): the manifest penalty is gone,
not reduced. The on-column reproduces the override's §3.1 cells within 0.4 ms on all
four rows — the temporary override measured the real thing. The `off` second statement
(115.81) sits 4 ms under IO-1's shipped 120.01; the run's own spread is 23 ms, so that
delta is noise, not a claim.

### 5.2 The census with the cache on

| statement | manifest-list off → on (knob = 32 MiB, set explicitly) | manifest off → on (knob = 32 MiB, set explicitly) |
|---|---:|---:|
| `SELECT count(*)` (first) | 1 → 1 | 1 → 1 |
| `SELECT count(*)` (repeat) | 1 → **0** | 1 → **0** |
| `SELECT … WHERE …` | 1 → **0** | 1 → **0** |
| `INSERT … SELECT` | 2 → 2 | 1 → 1 |
| `SELECT count(*)` after the insert | 1 → 1 | 2 → **1** |
| `DELETE` MoR v3 | 4 → 3 | 8 → **6** |
| `UPDATE` MoR v3 | 5 → 4 | 15 → **12** |
| `MERGE` MoR v3 | 4 → 4 | 8 → 8 |
| `SELECT count(*)` tail | 1 → 1 | 2 → 2 |
| `SELECT count(*)` tail (repeat) | 1 → **0** | 2 → **0** |

`metadata.json` reads stay 0 on every statement that reads an existing table (1 on the
two creators), which is IO-1's half doing its job. A repeated read now opens **nothing
at all** except the parquet it must decode.

The DML rows need their reason, because the naive reading ("the cache misses half the
DML opens") is wrong in an instructive way. The fork's **scan** path consults the
shared cache, but its **transaction, maintenance and inspect** paths load manifests
straight from `FileIO`: at pin `79119643`, `transaction/` holds 0 cached reads against
166 direct `load_manifest*` calls, and `maintenance/`, `inspect/` and
`delete_vector_lookup.rs` hold 0. So a DML statement saves exactly its read-side
repeats and keeps its commit-side opens: DELETE and UPDATE each drop one full
read-side cycle (the same 4 → 3 / 8 → 6 and 5 → 4 / 15 → 12 the override's §3.2 table
showed), the post-insert SELECT skips the one manifest it already holds, and INSERT
and MERGE are unchanged because every manifest they touch is commit-side — MERGE opens
the same new list and the same two new manifests four times each, which is four direct
loads, not four misses. Per-path strace dumps (which statement opens which named file,
first-seen marked) are in the unit ledger's evidence. This is filed as
`PERF-CATALOG-COMMIT-CACHE-1` / fork ask `F-CATIO-COMMIT`, not fixed here: a bypassing
path re-reads, it never serves stale, and the fork owns the fix.

### 5.3 Commands

```bash
cd $HOME/repark-lanes/lanes/oc-catio2/python/repark && \
  VIRTUAL_ENV=$HOME/repark-lanes/lanes/oc-catio2/.venv CARGO_BUILD_JOBS=8 \
  $HOME/repark-lanes/lanes/oc-catio2/.venv/bin/maturin develop --release
cd $HOME/repark-lanes/lanes/oc-catio2
.venv/bin/python scratch/probes/probe_manifest.py manoff
.venv/bin/python scratch/probes/probe_manifest.py manon
strace -f -e trace=openat -o scratch/strace_manoff.txt .venv/bin/python scratch/probes/probe_calls.py manoff
.venv/bin/python scratch/probes/count_calls.py scratch/strace_manoff.txt scratch/census_manoff.json
strace -f -e trace=openat -o scratch/strace_manon.txt .venv/bin/python scratch/probes/probe_calls.py manon
.venv/bin/python scratch/probes/count_calls.py scratch/strace_manon.txt scratch/census_manon.json
```

Probe sources live under `scratch/probes/` (gitignored, never committed; they carry no
comments). `fixtures.py` / `probe_calls.py` / `probe_manifest.py` are the IO-1 probes
with the lane path moved and the isolated variable changed from `metadataCache` to
`manifestCacheBytes` (`0`, the default, vs `33554432`, set explicitly); `harness.py` /
`count_calls.py` are byte-identical
to IO-1's. The JSON number files (`scratch/numbers_manifest_manoff.json`,
`scratch/numbers_manifest_manon.json`, `scratch/census_manoff.json`,
`scratch/census_manon.json`) carry every sample, min, spread, floor repeat and load.

## 6. The default-ON flip re-measured on the default session (PERF-ICE-CATALOG-IO-3, 2026-09-05)

Measured 2026-09-05 on the lane `$HOME/repark-lanes/lanes/oc-catio3` (branch
`perf/ice-catalog-io-3`, base `origin/main` `b4af56d0`, fork pin `2ed39cb0`, no path
override). Same box as §1–§5 (Threadripper 3970X, 64 threads, 125.7 GiB, governor
`schedutil`, kernel 6.8.0-138), same versions (repark 1.0.1 / DataFusion 54.1.0 / arrow
58.4), same shapes (the §1 20 k-row census table, the §2 1e6-row 208-file tables at 193
vs 1 manifests — the counts reproduce exactly), same method (5 timed after 1 warm-up;
median, min, spread; floor per run; `strace -f -e trace=openat`, ENOENT excluded). The
native module is `_native.abi3.so` 164,313,144 B with `__debug_assertions__ is False`
(every probe refuses otherwise). The only variable between the two columns is the
session: the default (no knob — the cache is ON at 32 MiB) vs
`repark.iceberg.manifestCacheBytes = "0"` set explicitly. The metadata cache is ON in
both columns.

**Not a quiet box, again.** Load 8.7 through the default timing run and 6.7 through the
explicit-`0` run (sibling lane builds live). Every cell carries its run's floor and load;
a cost is only ever read against its own run.

### 6.1 The timing cells

| cell | default = ON, no knob (ms) | explicit `0` (ms) | IO-2 §5.1 on, knob set (ms) | target |
|---|---:|---:|---:|---|
| `t_many/count_id/stmt1` | 11.95 (spread 1.56) | 121.45 (spread 8.04) | 10.73 | — |
| `t_many/count_id/stmt2` | **11.27** (spread 3.56) | **123.47** (spread 31.52) | 10.95 | ≤ 20 |
| `t_many/point/stmt2` | 14.67 (spread 1.97) | 125.16 (spread 31.92) | 14.75 | — |
| `t_many_merged/count_id/stmt2` | 10.33 (spread 0.55) | 15.52 (spread 1.66) | 10.49 | — |
| `t_many_merged/point/stmt2` | 12.71 (spread 0.81) | 18.03 (spread 4.41) | 13.20 | — |

Floors 0.46 (default) / 0.21 (off); load 8.7 → 8.7 (default) and 6.7 → 6.7 (off). The
default second statement clears the target at roughly half of it, and it now sits
within 1.0 ms of its one-manifest twin (11.27 vs 10.33): the manifest penalty is gone,
not reduced. The default column reproduces IO-2's explicit-knob column within 0.4 ms on
all four rows — the flip serves the measured win by default, and the win was never the
knob. The explicit-`0` second statement (123.47) sits 8 ms over IO-2's off column
(115.81); both runs' spreads are ~30 ms, so that delta is noise, not a claim.

### 6.2 The census on the default session

| statement | manifest-list `0` → default | manifest `0` → default |
|---|---:|---:|
| `SELECT count(*)` (first) | 1 → 1 | 1 → 1 |
| `SELECT count(*)` (repeat) | 1 → **0** | 1 → **0** |
| `SELECT … WHERE …` | 1 → **0** | 1 → **0** |
| `INSERT … SELECT` | 2 → 2 | 1 → 1 |
| `SELECT count(*)` after the insert | 1 → 1 | 2 → **1** |
| `DELETE` MoR v3 | 4 → 3 | 8 → **6** |
| `UPDATE` MoR v3 | 5 → 4 | 15 → **12** |
| `MERGE` MoR v3 | 4 → 4 | 8 → 8 |
| `SELECT count(*)` tail | 1 → 1 | 2 → 2 |
| `SELECT count(*)` tail (repeat) | 1 → **0** | 2 → **0** |

`metadata.json` reads stay 0 on every statement that reads an existing table (1 on the
two creators), which is IO-1's half doing its job. The table reproduces IO-2's §5.2
on-column cell for cell with no knob set: a repeated read opens **nothing at all**
except the parquet it must decode. The DML scope is unchanged — the commit-path bypass
persists at pin `2ed39cb0` (`transaction/` still loads straight from `FileIO`), so
`PERF-CATALOG-COMMIT-CACHE-1` / `F-CATIO-COMMIT` stays open and DML still saves
read-side repeats only.

### 6.3 Memory: 500 small tables, default versus explicit `0`

Each column is a fresh subprocess touching 500 CTAS tables (4 rows each) and reading
every one back; the number is peak `ru_maxrss`:

| column | peak RSS (MB) | rows |
|---|---:|---:|
| default (no knob) | 332.2 | 2000 / 2000 |
| explicit `0` | 323.9 | 2000 / 2000 |
| delta | **8.3** | — |

The committed leg (`test_peak_rss_over_five_hundred_tables_stays_within_the_default_cache_budget`)
pins delta ≤ 64 MB on every build — twice the 32 MiB budget, so any regression that
retains outside the cache reds while moka bookkeeping and allocator variance (a
debug-module back-to-back of identical configs varied by 12 MB) stay green.

**Round 2 (2026-09-05): the 32 MiB budget binds estimated weight, not resident
bytes.** The same single-driver shape re-run at 2,000 and 8,000 tables (same release
module, loads 6–21, sibling builds live throughout):

| tables | default peak (MB) | explicit-`0` peak (MB) | delta (MB) | charged (MB) |
|---|---:|---:|---:|---:|
| 500 | 332.2 | 323.9 | 8.3 | 0.5 |
| 2,000 | 340.1 | 340.1 | ~0 | 2.0 |
| 8,000 | 370.2 | 322.6 | 47.6 | 8.0 |
| 32,768 | 602.9 | 352.3 | 250.6 | 32.0 |

Every pair is row-correct in both columns (2,000 / 8,000 / 32,000 rows). But the
explicit-`0` peak is non-monotonic across table counts (323.9 / 340.1 / 322.6), and a
2,000-table replication pair lands in the same band — the CTAS-phase peak and
allocator mood move the single-driver number more than the retained cache does below
~8,000 tables, so no resident-vs-charged ratio is read off peak RSS.

The discriminating shape samples VmRSS at the phase boundary instead: after the CTAS
loop and after the read loop, in the same fresh subprocess. The read-phase growth
(read RSS minus CTAS RSS) cancels the CTAS peak, and the delta of growths is the
retained cache plus allocator wobble (the 2,000-table pair is replicated: two default
and three off samples):

| tables | default growth (MB) | explicit-`0` growth (MB) | delta (MB) | charged (MB) |
|---|---:|---:|---:|---:|
| 500 | 175.2 | 149.8 | 25.4 | 0.5 |
| 2,000 | 161.8 / 164.9 | 147.9 / 152.4 / 145.0 | ~15 | 2.0 |
| 8,000 | 194.9 | 135.6 | 59.3 | 8.0 |
| 32,768 | 416.9 | 138.5 | 278.4 | 32.0 |

Both columns carry an N-flat ~140–150 MB read-phase level (allocator arena growth
under the plan+execute pattern, not retained entries — it is there with the cache
off). Above it the default column retains ~7.5 KB per table at 2,000–8,000 tables
(~15 MB / 59.3 MB against 2.0 / 8.0 MB charged — ~7.5× the fork's estimate),
a noisy per-run figure (independent 2,000-table samples measured growth deltas from 4.6 to 47.2 MB); the ratio rests on the file-bytes floor (5.1× charged, deterministic) and the 32,768-table at-bound run (~8×); the 500-table delta (25 MB on 0.5 MB charged) is
fixed-level wobble, not retention, and is disclosed as such. The estimate under-counts
before any parsed-form overhead is even considered: one table's manifest list is
1,604 B and its manifest 3,466 B on disk (5,070 B of file bytes charged as 1,024 B).
Registry `PERF-CATALOG-CACHE-WEIGHT-1` / fork ask `F-CATIO-WEIGHT` carries the fix;
the red-when-fixed pin is
`test_a_budget_sized_to_the_charged_weight_retains_every_table` (256 tables fit a
280000 budget at charged weight, so the coldest table still hits after every manifest
is deleted; true weights evict it and the leg reds).

At 8,000 tables the cache holds 8 MB of an estimated 32 MB budget (25 %) while the
session peaks at 370 MB total. The 32,768-table at-bound run (32.0 MB charged — 100 %
of budget) holds **617.5 MB** resident after the read pass (602.9 MB process peak)
against 338.8 MB with the cache off: ~265–278 MB of cache entries above a ~340–352 MB
non-cache base, ~8× the charged weight. That crosses the brief's 512 MB line, so the
32 MiB default is not changed in-lane — the at-bound numbers go to the orchestrator,
who picks the default (unit ledger, round-2 section). Method footnote: the at-bound
peak (602.9 MB) reads BELOW the direct VmRSS sample (617.5 MB) — `ru_maxrss`
under-reports a direct sample by ~15 MB here, more reason no ratio is read off peak
RSS.

### 6.4 A token budget churns without benefit

Same release module, same loads. Each column builds 2,000 tables, reads every table
back once, then times a second full pass:

| column | second pass (s) | rows |
|---|---:|---:|
| default (no knob) | 5.6 | 8000 / 8000 |
| explicit `0` | 8.2 | 8000 / 8000 |
| `131072` (128 KiB) | 8.1 | 8000 / 8000 |

A 128 KiB budget over a 2 MB working set evicts continuously, so the second pass
costs what explicit `0` costs while paying insert/evict work that never pays off —
the mechanism is pinned structurally by
`test_a_sub_megabyte_byte_budget_churns_cold_tables_while_hot_tables_hit` (at 128 KiB
over 256 tables with every manifest deleted, cold tables miss while hot tables still
hit). No wall-clock assertion is attached: the cells above are
documented numbers. The critic round reported 347 s for this cell; this lane's
re-runs do not reproduce it (8.1 s here), the cause of that cell is unexplained, and
the structural pin carries the thrash claim instead. A single-entry version of this
pin (coldest raises, hottest answers) passed 17 of 18 samples across probe and file
runs and was replaced by the aggregate count: asserting one entry's fate under
TinyLFU admission plus async eviction is inherently a few percent flaky in either
direction, while all-miss or all-hit across 256 tables is absurd in both.

### 6.5 Commands

```bash
cd $HOME/repark-lanes/lanes/oc-catio3/python/repark && \
  VIRTUAL_ENV=$HOME/repark-lanes/lanes/oc-catio3/.venv CARGO_BUILD_JOBS=8 \
  uvx maturin@1.14.1 develop --release
cd $HOME/repark-lanes/lanes/oc-catio3
.venv/bin/python scratch/probes/probe_manifest.py default
.venv/bin/python scratch/probes/probe_manifest.py off
strace -f -e trace=openat -o scratch/strace_default.txt .venv/bin/python scratch/probes/probe_calls.py default
.venv/bin/python scratch/probes/count_calls.py scratch/strace_default.txt scratch/census_default.json
strace -f -e trace=openat -o scratch/strace_off.txt .venv/bin/python scratch/probes/probe_calls.py off
.venv/bin/python scratch/probes/count_calls.py scratch/strace_off.txt scratch/census_off.json
.venv/bin/python -m pytest python/repark/tests/test_perf_ice_catalog_io_1.py -q -k "peak_rss or second_statement"
```

Probe sources live under `scratch/probes/` (gitignored, never committed; they carry no
comments). `fixtures.py`, `harness.py` and `count_calls.py` are byte-identical to IO-2's;
`probe_manifest.py` / `probe_calls.py` are the IO-2 probes with the lane path moved and
the labels changed from `manon` / `manoff` to `default` / `off` (the isolated variable
is now the default session versus explicit `0`). The JSON number files
(`scratch/numbers_manifest_default.json`, `scratch/numbers_manifest_off.json`,
`scratch/census_default.json`, `scratch/census_off.json`) carry every sample, min,
spread, floor repeat and load.

Round-2 re-runs (same module, no rebuild; each line is one fresh subprocess; the
32,768-table pair is the at-bound run):

```bash
cd $HOME/repark-lanes/lanes/oc-catio3
.venv/bin/python scratch/probes/rss_peak.py 2000 scratch/wh_peak2000_default 4
.venv/bin/python scratch/probes/rss_peak.py 2000 scratch/wh_peak2000_off 4 repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_peak.py 8000 scratch/wh_peak8000_default 4
.venv/bin/python scratch/probes/rss_peak.py 8000 scratch/wh_peak8000_off 4 repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_growth.py 500 scratch/wh_growth500_default 4
.venv/bin/python scratch/probes/rss_growth.py 500 scratch/wh_growth500_off 4 repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_growth.py 2000 scratch/wh_growth2000_default 4
.venv/bin/python scratch/probes/rss_growth.py 2000 scratch/wh_growth2000_off 4 repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_growth.py 8000 scratch/wh_growth8000_default 4
.venv/bin/python scratch/probes/rss_growth.py 8000 scratch/wh_growth8000_off 4 repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_growth.py 32768 scratch/wh_growth32768_default 4
.venv/bin/python scratch/probes/rss_growth.py 32768 scratch/wh_growth32768_off 4 repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_second_pass.py 2000 scratch/wh_thrash_default
.venv/bin/python scratch/probes/rss_second_pass.py 2000 scratch/wh_thrash_off repark.iceberg.manifestCacheBytes=0
.venv/bin/python scratch/probes/rss_second_pass.py 2000 scratch/wh_thrash_tiny repark.iceberg.manifestCacheBytes=131072
.venv/bin/python -m pytest python/repark/tests/test_perf_ice_catalog_io_1.py -q -k "charged_weight or sub_megabyte"
```

`rss_peak.py` is the committed RSS leg's driver with the table count lifted to argv;
`rss_growth.py` adds the two VmRSS samples (peak still printed, line 3);
`rss_second_pass.py` warms every table once and times one repeat pass. All three
carry no comments. The lane's runs used `/tmp` warehouses; the shape is identical.

## Pointers

- Up: [map.md](map.md)
- The analysis this unit cites: [engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md)
- The knobs: [../../crates/repark-iceberg/src/catalog/map.md](../../crates/repark-iceberg/src/catalog/map.md)

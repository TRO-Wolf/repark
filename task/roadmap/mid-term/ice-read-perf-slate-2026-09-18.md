# Iceberg read performance on S3 Tables and Glue — slate for v1.5.0

**Filed:** 2026-09-18 by the orchestrating session on the owner's instruction ("go ahead and do that, let's set that
for a minor release on 1.5"). **Target:** the v1.5.0 minor — no 1.5 tag exists yet (latest tag `v1.4.2`, workspace
version `1.4.2`). **Starts after** the Iceberg remediation residue closes (runs 19–21, rating of 2026-09-16); the
two campaigns do not interleave, except that fork PRs may share a pin bump.

**Source.** An owner-supplied source review of S3 Tables read performance (2026-09-18; RePark main `e7857f9f`, fork
pin `8fb44a3`; no benchmark, no AWS call), then verified line by line by the orchestrating session against the same
pin. Every claim below cites the line that carries it. **Nothing here is measured yet** — unit 0 exists so that every
later unit is ranked by a number, not by this document.

## What the verification found

| Claim | Verdict | Where |
|---|---|---|
| Page-level pruning is off on the DataFusion scan path | confirmed — `stream_partition_work(file_io, &work, concurrency, batch_size, true, false)`; the last argument is `row_selection_enabled` | fork `crates/integrations/datafusion/src/physical_plan/scan.rs:324`; default `false` at `crates/iceberg/src/arrow/reader.rs:171` |
| Enabling it costs unfiltered scans nothing | confirmed — the page index is loaded only when `row_selection_enabled && task.predicate.is_some()`, or when the task has deletes (already today) | fork `reader.rs:432-433` |
| The S3 Tables catalog gets no shared cache | confirmed, **and it is fork work**: `with_table_metadata_cache` and `with_shared_object_cache_bytes` exist only on the memory catalog builder; `S3TablesCatalogBuilder` has no cache surface | RePark `crates/repark-iceberg/src/catalog/builders.rs:51-55` (memory) against `:106-107` (S3 Tables); fork `crates/iceberg/src/catalog/memory/{catalog.rs:75,caches.rs:28}` |
| **Glue has the same gap** (not in the source review) | confirmed — `glue_catalog` is a bare `.load("glue", …)` | RePark `builders.rs:81` |
| Every S3 Tables load re-reads and re-parses the metadata document | confirmed — `TableMetadata::read_from` then a fresh `Table::builder()` | fork `crates/catalog/s3tables/src/catalog.rs:361-363` |
| The manifest cache default is 32 MiB and raising it does not add sharing | confirmed | RePark `crates/repark-iceberg/src/catalog/caches.rs:22` |
| A prefetched-footer hook exists and nothing feeds it | confirmed — `with_prefetched_parquet_metadata` is `pub(crate)`, so this too starts in the fork | fork `reader.rs:228` |
| A footer read is one request of up to 512 KiB | confirmed — the size hint defaults to 512 KiB, per file, per partition reader, per query | fork `reader.rs:99` |
| Aggregate file concurrency may exceed its nominal budget | confirmed, and intended: "With N > L this gives P = 1, and the total may exceed L" | fork `scan.rs:195` |
| Scan statistics are unknown except an exact row count under conditions | confirmed | fork `scan.rs:285-291` |

## Wave 1 — the v1.5.0 units, in order

### 0. ICE-READ-PERF-0 — instrumentation and the benchmark bed

No product behaviour changes. A counting layer over the fork's `FileIO` (requests, bytes, per operation kind) exposed
through the scan's metrics, and a replayable bench with three modes — cold, warm, four concurrent queries — that
records planning time, time to first batch, total time, requests, bytes transferred, cache hits and peak RSS.
Two beds: **local** (a Hadoop-layout table on local disk; reader-level units rank here, release native on the
default profile) and **AWS** (an S3 Tables and a Glue table through a dispatch-only leg of `aws-acceptance.yml`;
catalog-level units rank here). The AWS leg makes real calls and costs money: approved under ruling R-3, with its 3 GB
table-size flag as a hard clause of this unit. The baseline table of this unit is the "before" of every unit below.

### 1. ICE-PAGE-PRUNE-1 — predicate-driven page selection on the DataFusion path

Fork: the scan passes `row_selection_enabled = true` (a scan knob beside the existing two, default on once the pins
hold). No new API. The work is correctness, red-first, both doors, v2 and v3, RePark- and Spark-written files:

- **Row lineage.** `_row_id` derives from row position; a skipped page must not shift it. Pin `_row_id` and
  `_last_updated_sequence_number` on a filtered scan of a multi-page file against the unfiltered scan's values.
- Position deletes, equality deletes and deletion vectors on a file whose surviving pages are a strict subset.
- Schema evolution (ADD / RENAME / DROP), type promotion (the #673 shapes), nulls and all-null pages, NaN bounds,
  truncated string bounds.
- A clause that the writers emit page indexes at all (RePark's and Spark's), or the unit measures nothing.

Measured on unit 0's local bed with selective timestamp, identifier and range filters over large files.

### 2. F-CATALOG-CACHE-1 (fork) → pin bump → ICE-CATALOG-CACHE-1 (RePark)

Fork: the S3 Tables and Glue catalogs accept the same two shared handles the memory catalog does. The catalog
pointer check stays on **every** load — external writers and AWS-managed compaction must remain visible — and the
parsed `TableMetadata` is reused when the metadata location is unchanged; immutable manifest bytes are reused across
table loads. Bounded admission, concurrent-miss deduplication (one fetch per key under contention), and cache
identity that separates catalogs and credential contexts (table bucket ARN or Glue catalog id + warehouse, never the
table name alone). RePark: pass `caches` into `glue_catalog` and `s3tables_catalog`, and surface hit / miss / eviction
through the existing `iceberg_metadata_cache_stats`. Pins: a commit by a second catalog handle is visible on the next
load; an eviction under pressure never returns stale metadata; two sessions with different credentials never share
an entry.

### 3. F-FOOTER-CACHE-1 (fork) → ICE-FOOTER-CACHE-1 (RePark)

A bounded, shared `ParquetMetaData` cache keyed by storage context + data-file path + the manifest's
`file_size_in_bytes` (Iceberg data files are immutable and uniquely named, so the key is trustworthy), feeding the
existing prefetched-metadata map; an entry loaded without the page index is upgraded, not duplicated, when a filtered
scan needs the index. Ranks on "many files, repeated scans, split-file reads": 512 KiB × files × partition readers
per query is the ceiling it removes.

### 4. The re-measure gate

Unit 0's bench again, all three modes, both beds. Wave 2 is ordered by this table. v1.5.0 does not wait on wave 2.

## Wave 2 — cards, not scheduled

- **ICE-SCAN-SCHED-1** — assign scan groups by estimated bytes instead of round-robin; one in-flight budget (requests
  and bytes) across partitions, which also respects the box's worker CPU and memory caps; reader tuning exposed on
  the DataFusion path.
- **ICE-SCAN-STATS-1** — column null counts, conservative bounds and byte sizes as **inexact** statistics for join
  ordering only. `Precision::Exact` stays where it is provable today: an Exact that is wrong under deletes, missing
  metrics, truncated bounds or schema evolution is a silent wrong answer, the class this project refuses. Metadata-only
  `COUNT(column)` extends the existing `COUNT(*)` rule with the same conditions, or not at all.
- **ICE-LAYOUT-GUIDE-1** — RePark already ships `CALL plan_partitioning()`, `apply_partitioning()` and the
  maintenance policy; AWS-managed S3 Tables compaction (binpack / sort / z-order, applies row-level deletes) can
  rewrite the same files. A policy decision first (question Q-3), then diagnostics: delete density, file-size spread
  and sort-order coverage per table.
- **ICE-RUNTIME-FILTER-1** and early `LIMIT` cancellation — the scan plans all partition work before execution, so
  both need deeper integration; a later minor.

## Rules that bind every unit

The three fork rules ([docs/fork-sync.md](../../../docs/fork-sync.md)): table-format and reader behaviour is fork
work, the pin moves only in its own PR, fork main stays green. Rust only — no Python decision, no `unsafe`. The
comment ban on every actor, enforced mechanically before the PR and before the merge. Local gates run only what CI
cannot (release native, the unit's tests, the Spark-gated and live cells); numbers in a ledger come from the default
release profile. A performance claim is a before/after pair from unit 0's bed at a named head.

## Owner rulings (2026-09-18)

- **R-4 (2026-09-19) — v1.5.0 also waits for full Spark–Iceberg parity.** The owner amended R-1: the tag waits for
  wave 1 below **and** for the parity slate, measured as zero non-EQUAL cells that Spark answers
  ([ice-parity-inventory-2026-09-19.md §0](ice-parity-inventory-2026-09-19.md#0-owner-ruling-2026-09-19--this-slate-gates-v150)).
- **R-1 (was Q-2) — v1.5.0 waits for ALL of wave 1.** Units 0 through 4 land, measured, before the tag. Wave 2 stays
  unscheduled.
- **R-2 (was Q-3) — on S3 Tables, AWS-managed maintenance owns compaction by default; RePark's maintenance is an
  option.** RePark's `rewrite_data_files` / maintenance policy on an S3 Tables table is opt-in per call or per table
  (the switch and its name are ICE-LAYOUT-GUIDE-1's first clause); without the opt-in RePark refuses the rewrite with
  a typed error naming the switch, so two compactors never rewrite the same files unannounced. Glue and Hadoop tables
  keep RePark's policy as today.
- **R-3 (was Q-1) — the AWS bench spend is approved (owner, 2026-09-18: "that's fine, just implement a flag if the
  table sizes get anywhere above 3 GB").** The size flag is a clause of unit 0, not a convention: before any read, the
  bench sums `file_size_in_bytes` over each bench table's `files` metadata table (data and delete files, every
  snapshot's live set), prints the total, and **if any table exceeds 3 GB (3 × 1024³ bytes) it raises the flag —
  the dispatch stops before the first scan, the job fails with the measured size in its summary, and one Slack note
  goes to the owner.** The same check runs right after the tables are written. Raising the threshold is an owner
  decision, never a bench option. The estimate behind the approval: Estimate, from AWS list prices as the orchestrating session recalls them
  (verify on the pricing page before the first dispatch): the bench tables are fixed at **≤ 2 GB** each (about 200
  files, one S3 Tables table and one Glue table), written once. A dispatch reads roughly 12 GB (cold scan, warm scan,
  four concurrent scans, selective queries) out to a GitHub-hosted runner, so **data transfer out at about $0.09/GB
  is the dominant line — about $1 per dispatch**, and AWS's 100 GB/month free transfer tier may absorb all of it.
  Requests are tens of thousands of GETs (cents), storage is about $0.05 per table-month, Glue catalog calls sit
  inside the free million. Six dispatches (baseline, units 1–3, the re-measure gate, one retry) come to **about
  $5–10; cap $25**. Guards: dispatch-only (never on the nightly schedule), one dispatch per unit plus the baseline,
  AWS-managed compaction **disabled on the bench tables** so the file layout is identical across dispatches, and the
  bench tables dropped when the re-measure gate closes. An AWS Budgets alert on the account is the owner's to set.

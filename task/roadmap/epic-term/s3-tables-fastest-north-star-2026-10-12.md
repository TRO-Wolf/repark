# The S3 Tables performance goal — the fastest known query engine on S3 Tables (2026-10-12)

**Date:** 2026-10-12 · **Filed by:** the owner's delegate, at the owner's request · **Status:**
**FUTURE GOAL, UNSCHEDULED** — filed so the claim is defined before any lane runs on it; no unit
starts on this path until the owner schedules it. **Set by the owner** the same day: "I want RePark
to be the fastest known query engine on S3 Tables", filed as a future goal the same evening. This
card turns that sentence into a claim that can be measured, won, published and lost. It sets no
number; every number lives in the card of the unit that measures it.

## 1. The claim, precisely

RePark is "the fastest known query engine on S3 Tables" when, on the published benchmark page this
card defines, its median wall per query is lower than every engine in the comparison set, run the
same way on the same day, over the whole workload set. The claim carries its date, its comparison
set and its workload set; it is re-run at every release; and when another engine measures faster
it is retired on the page, not defended in prose. "Known" means the comparison set, named below
and extended whenever a single-node engine gains S3 Tables support.

## 2. Scope

- **One node.** One EC2 instance of a named class, in the region of the S3 Tables bucket. Cluster
  engines are compared single-node; Athena is reported as the serverless reference with wall and
  cost, not as a like-for-like row.
- **S3 Tables as catalog and store**, through its service-managed endpoint, the path a user's
  configuration takes (the `s3tables` block the read bench already registers).
- **Workload set, three classes.** (a) TPC-H at two scale factors loaded into S3 Tables as Iceberg
  tables; (b) pipeline-shaped statements from the operator's workloads: incremental `MERGE`,
  time-window scans, `count(*)` and `count(col)`, wide `SELECT` over many small files; (c) a
  metadata-heavy class, many manifests and many files, where catalog and manifest round trips
  dominate.
- **Cold and warm.** A cold run is a fresh session; a warm run keeps the session caches. Both are
  published; the claim is held on both or on neither.

## 3. The comparison set

Engines that query S3 Tables from a single node, each run from the same instance through its
documented S3 Tables path: DuckDB (Iceberg extension over the S3 Tables REST catalog), Apache Spark
single-node with the Iceberg runtime, Trino single-node, Daft; Amazon Athena as the serverless
reference. An engine that cannot be run the same way is listed as not comparable, with the reason.

## 4. Metrics and publication

Per query: median wall over interleaved repetitions, S3 request count, bytes transferred, catalog
round trips; per engine: setup cost and, for Athena, cost per query. The page carries the instance
class, region, software versions, the generator seeds, the scripts and the raw JSON, so anyone can
re-run it. It lives under `docs/perf/` with a dated name and is re-published at every release.

## 5. Where RePark stands on main at this date

Honest starting line, from the committed record:

- No AWS performance measurement is recorded. The Glue and S3 Tables metadata caches were wired on
  2026-09-19 and never measured; the AWS legs of the catalog I/O test skip on an IAM grant.
- Locally, a full Iceberg scan lands 1.8–3.6x the equivalent Parquet scan, with an unexplained
  fixed cost per query and about 2x per byte.
- DML still loads manifests from file I/O on the commit path; whether manifest reads are concurrent
  is unmeasured.
- No manifest-derived statistics reach the planner beyond the exact `count(*)` fold.
- Windows run on one partition outside the TA single-series path.

The claim is aspirational today and says so in PROJECT.md.

## 6. The path, each row its own card, measure first

1. **PERF-S3T-0 — the AWS baseline.** Clear the IAM grant; run the read bench against S3 Tables
   cold, warm, concurrent and concurrent-cold; run the catalog I/O legs; record request and byte
   counts per statement. Nothing else starts before this row is on main.
2. **PERF-S3T-1 — catalog and manifest I/O.** `GetTable` and REST round trips per statement, the
   manifest cache on S3, concurrent manifest reads, the commit path's manifest loads.
3. **PERF-S3T-2 — bytes.** Filter pushdown and late materialisation re-measured where bytes cost
   money, range coalescing and prefetch in the object store layer, parallel file reads.
4. **PERF-S3T-3 — statistics.** Row counts, null counts and bounds from manifests into planner
   statistics, only where the spec makes them exact; the scan path stays.
5. **PERF-S3T-4 — the scan residue.** The fixed per-query cost and the per-byte factor, with the
   fork scan instrumented first.
6. **PERF-S3T-5 — the comparison harness and the first page.** The engines of §3 scripted on the
   same instance, the first publication.
7. **The DataFusion family bump** is a performance row on this path, rehearsed on a scratch clone
   before it is taken.

## 7. Rules

- Every row measures before it builds and publishes interleaved like-for-like medians; a row that
  cannot show a gain on S3 Tables does not merge on local gains alone.
- The per-card regression ceilings on every other path still apply; a gain on S3 Tables does not
  buy a loss elsewhere.
- Data safety and correctness against the oracle still win inside a unit; this north star decides
  which lanes run, not what a unit may trade.
- No number from this card enters PROJECT.md.

## 8. Open owner decisions

- The instance class and region.
- The two TPC-H scale factors.
- The comparison set of §3, confirmed or changed.
- The AWS budget per benchmark run and per release.
- When this goal is scheduled, and which release publishes the first page.

# Card AWS-EPHEMERAL-RUNS-1: every aws-acceptance run creates its own tables, cross-reads them, and removes them; one build job, parallel leg jobs; the S3 Tables Spark session on the REST catalog

**Date:** 2026-10-24. **Filed by:** Claude Fable 5.1 (`claude-fable-5-1`), orchestrator, from the owner's rulings of
the same morning (walkthrough artifact `FmUcKaesjdAtvPBPK6mqhK`).

**Status:** ruled, not scheduled. Two owner actions stand in front of step 1 (§ The owner's actions). Ledger C-060 is
superseded by C-066 and C-067 (REJECTED, measured 2026-10-24, owner's word the same day); C-068 and C-069 hold this
card's obligations OPEN and C-070 carries run 3's pre-streaming stall.

**Built, awaiting the first dispatched run (2026-10-24, Claude Sonnet 5.5):** the local half is on branch
`feat/aws-ephemeral-runs-1`: the run prefix `testing_r<run_id>_` at every table-creating site (step 1), the cleanup
script `scripts/aws_acceptance_cleanup.py` (step 2) and the Spark REST catalog block (step 4). The job split
(step 1b, 3 in the lane order) is not built: it waits on two questions in the
[ledger](../../ledgers/staging/aws-ephemeral-runs-1-ledger.md), C-012 (one shared run prefix lets a leg's cleanup
and cross-read reach other legs' tables) and C-014 (`boto3` is not in `uv.lock`). Census:
[step0-census.md](../../wo/aws-ephemeral-runs-1/step0-census.md).

**Retires:** when one dispatched `aws-acceptance` run on `main` proves all of: every leg job green inside its own cap,
each leg's cross-read of its own tables EQUAL (or a named Spark error that is not the catalog-init failure), the
scratch namespaces holding no table of that run after it exits, and the S3 Tables cross-read reading through Spark's
REST catalog.

## Why

The nightly of 2026-10-24 (run 40079497082, main `e4f8d36b`) is the first with per-table streaming and per-table
error text in the cross-read step. It shows two problems, neither a hang:

- **Glue: a namespace walk that cannot fit its budget.** The Glue test discovered 2288 scratch tables and compared
  249 before its 600 s whole-test deadline, every one EQUAL, at 1.98 / 2.20 / 2.66 / 8.64 s (min / median / p90 /
  max) per table. The whole walk needs about 84 min. Run 6 (03:31 UTC, `e765f897`) showed the same: 402 EQUAL of
  2257 before the deadline. The native samples recorded on 2026-10-23/24 land mid-stride in one of the five stages
  of the loop (Spark socket wait, S3 `HeadObject`, a Rust condition variable inside a RePark collect); they are
  snapshots of a working loop. Run 3 of 2026-10-23 (a `metadata_log_entries` stall with no progress, before
  streaming existed) is not explained by this and keeps its own row.
- **S3 Tables: Spark has never opened the catalog.** Every S3 Tables outcome in every run is
  `IllegalArgumentException: Cannot initialize Catalog implementation
  org.apache.iceberg.aws.s3tables.S3TablesCatalog: Cannot find constructor for interface
  org.apache.iceberg.catalog.Catalog` (53 of 53 today, 548 of 548 in run 6). The two jars the harness puts on
  Spark's classpath, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` and `iceberg-aws-bundle-1.11.0.jar`, were listed
  on 2026-10-24: the runtime carries `GlueCatalog` and `DynamoDbCatalog`, the bundle carries no Iceberg catalog
  class at all. The class name is RePark's own suffix convention (`crates/repark-core/src/catalog_kind.rs` maps any
  `*S3TablesCatalog` to the S3 Tables builder), so the RePark half of every S3 Tables test has always worked. The
  test was wired into the nightly by #1163 on 2026-10-23 and had not met a real Spark session before.
- **The namespaces grow every night.** Glue 2257 → 2288 and S3 Tables 1725 → 1748 between runs 6 and 7. The
  acceptance role has no table delete by design (`docs/tier2-aws.md` §2, OD-3 never-teardown) and the dbt gold leg
  creates a six-to-seven-table family under a fresh UUID per run; among the first 250 Glue names alone are 29 such
  families. The documented by-hand expiry has not run.
- **The job's time is already spent before cross-read.** Setup 32 min (checkout and toolchains 0.5, `uv sync` plus
  the native Rust build 24.7, dbt and the previous-release venv 6.7), acceptance 11.7, dbt 1.7, integrity U 50.6,
  integrity X 12.8: cross-read started with about 11 min of the 120-min job cap. Leg U grew 31 → 36 → 51 min across
  runs 4, 6 and 7 with all 96 tests passing each time; the cause is not established (runner variance or the listing
  cost of the grown namespace are the candidates).

## The rulings (owner, 2026-10-24)

1. **Every run is self-contained.** A run creates what it needs under its own prefix, proves what it proves,
   cross-reads exactly the tables it wrote, and removes them before it exits. Every table a run creates is
   ephemeral, fixed-name ones included (`testing_cu_*`, `testing_appointment`); leg X installs the 1.5.5 wheel inside
   the same job, so no leg needs a prior run's table. The ICE-READ-PERF bench table (dispatch-only, carries a
   maintenance configuration) is the one persistent exception and stays out of every sweep.
2. **Each run owns a prefix**, `testing_<run-id>_…`, so the nightly and a dispatch can overlap and a run deletes only
   its own. Cleanup runs with `if: always()`. A cap-killed job can still skip it, so a backstop removes prefixes older
   than 24 h; that backstop is the only broad delete and is bounded by age and prefix.
3. **One build job, parallel leg jobs.** The build job (no credentials) compiles once and uploads the wheel as a
   workflow artifact. Leg jobs download it, `uv sync`, mint their own credential, run, cross-read their own tables and
   clean up: acceptance together with dbt gold (dbt reads the silver table acceptance creates), integrity U alone,
   integrity X alone (the previous-release venv installs here only). The backstop runs last with `if: always()`.
   Cross-read needs no job of its own. Measured basis: the compile is 77 % of the setup; the `aws-acceptance`
   environment has a branch policy and zero required reviewers, so parallel jobs mint without a human gate; the
   repository is public, so standard-runner minutes are not billed; each credential session is the one-hour default.
4. **The S3 Tables Spark session uses Iceberg's REST catalog** against the S3 Tables Iceberg endpoint with SigV4,
   the table bucket ARN as the warehouse and `S3FileIO`: `type=rest`,
   `uri=https://s3tables.<region>.amazonaws.com/iceberg`, `rest.sigv4-enabled=true`, `rest.signing-name=s3tables`,
   `rest.signing-region=<region>`. No new jar. The Spark block is a **separate** config function from the block
   RePark consumes: RePark classifies a catalog by the `catalog-impl` suffix and has no `type=rest` mapping. The
   region comes from the ARN or the workflow's region variable, never a committed literal.
5. **The scoped delete grant** reverses OD-3's never-teardown for scratch tables under the run prefix and nothing
   else: `glue:DeleteTable` and `glue:BatchDeleteTable` on `table/<scratch-db>/testing_*` (with the catalog and
   database ARNs Glue requires), never `glue:DeleteDatabase`; `s3tables:DeleteTable` under the existing
   `s3tables:namespace = testing_repark_acceptance` condition, never `DeleteNamespace` or `DeleteTableBucket`;
   object delete on the warehouse scratch prefix is already held. After the change a compromised job could drop
   `testing_*` tables in that one namespace and could not touch bronze, the production database or the namespaces.

## The owner's actions, in front of step 1

- The IAM policy edit of ruling 5, in the owner's account.
- The one-time prune of the roughly 4,000 tables there today, by hand through the owner's IAM user, or by the
  owner explicitly authorizing the backstop's first run to take everything. No agent-built step performs that first
  sweep on its own (ruling 76(a): a hint never names a data-deleting action; no agent names or performs a scratch
  delete before the grant exists and the owner has ruled it).
- Whether a lifecycle rule on the warehouse scratch prefix exists today; if it does, it is kept consistent with the
  24 h backstop so no catalog entry outlives its files.

## Unit shape

- **Step 0 (verifier, read-only):** confirm no leg reads a table it did not create in the same run; name the bench
  table exclusion; list every table-creating site per leg.
- **Step 1:** the run-id prefix on every created table; a cleanup step per job with `if: always()`; the age-bounded
  backstop. `docs/tier2-aws.md` §2 and §3 updated to the new property in the same PR.
- **Step 1b:** the job split of ruling 3.
- **Step 2:** cross-read over the run's own tables, and the S3 Tables Spark session on the REST catalog (ruling 4).
  Proof is one dispatched run (ruling 86: the first dispatched run with a change proves it); the first S3 Tables table
  alone (1800 rows on the RePark side today) settles the route.

Tier: Sonnet 5.5 or Muse for the build, Opus 5.5 and Muse MAX as the two verifiers (owner 2026-10-23). The
workflow (`.github/`) is committed by the orchestrator, never by a Muse lane. Expected effect beyond the fix: leg
U's growth is tested (a small namespace either brings it back toward 31 min or rules the listing cost out), and the
job's wall time falls from the full two hours to about the build plus the longest leg.

# Tier-2 live-AWS setup — operator runbook

The `aws-acceptance` workflow (phase-3 PR-6; design `docs/design/python-facade.md` §7.4) runs the
acceptance module against real AWS nightly + on dispatch, merged code only, via OIDC. Everything on
this page is **operator-side, one-time setup**; no agent performs any of it, and no value on this
page is ever committed — variable/secret NAMES only.

## 1. GitHub environment — this is the branch gate

Create environment **`aws-acceptance`** (Settings → Environments):

- **Required reviewers**: add yourself. A dispatch waits for your approval before any credential
  mints. At approval time your job is to check the run's **ref and commit** (the approval UI shows
  the environment name, not a diff) — approval is the one human control in the credential path.
- **Deployment branches and tags**: set to **Selected branches and tags** with a single rule
  `main`. This — NOT the in-workflow ref guard and NOT the IAM sub — is what makes "merged code
  only" a platform fact. GitHub's default is *All branches*; leaving it there would let anyone with
  push access dispatch a modified workflow from a topic branch (the `environment:` line, hence the
  OIDC sub, is unchanged), and the only remaining gate would be the reviewer.

## 2. IAM role (OIDC trust: one environment sub; permissions: create-only tables, scoped object-delete)

**Trust policy.** GitHub sends exactly ONE `sub` claim per run, and because the job sets
`environment: aws-acceptance` the sub is the *environment* form (never the ref form). This repo was
created after 2026-07-15, so it uses GitHub's **immutable subject format** (owner + repo IDs). Use a
single `StringEquals` on the one presented sub:

```json
{
  "Effect": "Allow",
  "Principal": { "Federated": "arn:aws:iam::<ACCOUNT>:oidc-provider/token.actions.githubusercontent.com" },
  "Action": "sts:AssumeRoleWithWebIdentity",
  "Condition": {
    "StringEquals": {
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      "token.actions.githubusercontent.com:sub": "repo:TRO-Wolf@64240326/repark@1325259325:environment:aws-acceptance"
    }
  }
}
```

Do NOT also add the `...:ref:refs/heads/main` form: only one sub is presented, so ANDing two exact
values never matches, and ORing them (a JSON array) is strictly *weaker* — the ref form would let a
job that omits `environment:` (bypassing the approval gate) assume the role. Branch binding comes
from §1's deployment-branch policy, not from the sub. Confirm the exact sub with
`gh api repos/TRO-Wolf/repark --jq '{id, owner_id: .owner.id}'`, or copy it verbatim from the
`AssumeRoleWithWebIdentity` CloudTrail event of a first (failing) dispatch.

**Permission policy** — least-privilege over the scratch surface only, scoped by explicit
**resource ARN** (never `Resource: "*"`). Glue tables still cannot be deleted. Object-delete is
the OD-3 exception on the warehouse scratch prefix only (MW-4 compact + expire):

- `s3:GetObject` / `s3:ListBucket` on the **bronze** bucket + prefix the harness reads
  (`REPARK_ACCEPT_BRONZE_BUCKET`) — read-only.
- `s3:PutObject` / `s3:GetObject` / `s3:ListBucket` / **`s3:DeleteObject`** on the **warehouse**
  scratch prefix only (`REPARK_ACCEPT_WAREHOUSE` + the `testing_repark_acceptance/` namespace).
  `s3:DeleteObject` is **OD-3** (2026-08-21, owner-executed 2026-08-23): `expire_snapshots` and
  rewrite procedures remove expired snapshot files. It is not table teardown. Bronze stays
  read-only. Never `Resource: "*"`.
- `glue:CreateDatabase` / `glue:GetDatabase` / `glue:CreateTable` / `glue:GetTable` /
  `glue:UpdateTable` on the scratch database resource ARN only — never the production database.
  **No `glue:DeleteTable` / `glue:DeleteDatabase`.**
- `glue:GetDatabases` / `glue:GetTables` — a **separate, catalog-wide, read-only** statement over
  the three ARN forms `arn:aws:glue:<REGION>:<ACCOUNT>:catalog`, `…:database/*` and
  `…:table/*/*`. These two are the plural LIST actions, and unlike everything above them they
  **cannot be scratch-scoped**: catalog registration builds a provider snapshot by enumerating
  every database in the account and then listing each one's tables, so a scope naming only the
  scratch database denies the walk the moment the account holds any other database — which is to
  say, in any populated account. Keep the statement read-only and keep it separate from the write
  statement, so widening the read scope never widens the write scope: creates and updates stay
  scratch-scoped. (The eager enumeration is filed separately as an engine observation; this
  runbook records what the current engine requires and promises no engine change.)
- S3 Tables (the v3 live legs — **OD-3b, owner ruling 2026-08-25: in v1.0; applied by the owner
  2026-08-28**; the first measurement is unit MW-10): one statement on
  the **table** ARN, scratch-scoped by the namespace condition key —
  `s3tables:GetTable`, `s3tables:GetTableMetadataLocation`,
  `s3tables:UpdateTableMetadataLocation`, `s3tables:GetTableData`, `s3tables:PutTableData` on
  `arn:aws:s3tables:<REGION>:<ACCOUNT>:bucket/<TABLE-BUCKET>/table/*` with
  `"Condition": {"StringEquals": {"s3tables:namespace": "testing_repark_acceptance"}}`, plus
  `s3tables:GetTableBucket`, `s3tables:CreateNamespace`, `s3tables:GetNamespace`,
  `s3tables:CreateTable`, `s3tables:ListTables` on the bucket ARN. **Still no `s3tables:DeleteTable` / `DeleteNamespace` / `DeleteTableBucket`.**
  AWS's published object-API mapping lists `PutObject` + the multipart operations under
  `PutTableData` and names no action for `DeleteObject` on table storage: whether
  `expire_snapshots` can remove files there is measured by the first S3 Tables maintenance
  unit — a denial is a stop, not a design; do not widen pre-emptively.
  Measured result (2026-08-30, [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383) — the owner's first `aws-acceptance` dispatch on merged `main` `afe3b807`): **allow**. `test_mor_merge_compact_expire_against_s3tables` ran green (`4 passed in 145.15s`): the expired CTAS snapshot's files were removed from table storage under this exact policy, the live row set was unchanged, and no denial signature appeared — `s3tables:PutTableData` does authorize the removal; nothing was widened.
  S3 Tables' automatic
  snapshot management (keep 1 / 120 h, then permanent removal of noncurrent objects) fails for
  a whole table that carries any user-defined branch or tag or a `history.expire.*` property —
  the refs leg disables it on its scratch tables or expects that failure.
- **The ICE-READ-PERF bench (§8) needs two more actions, which the role does NOT have today:**
  `s3tables:PutTableMaintenanceConfiguration` and `s3tables:GetTableMaintenanceConfiguration`,
  scoped to the one bench table
  `arn:aws:s3tables:<REGION>:<ACCOUNT>:bucket/<TABLE-BUCKET>/table/*` with the same
  `s3tables:namespace = testing_repark_acceptance` condition (or the bench table's own ARN once
  it exists). The bench disables AWS-managed compaction on its table and reads the setting back
  before it writes a byte, so the file layout stays identical across dispatches (slate R-2 / R-3
  guard). Until the owner grants them, the bench job stops at that step, before any write, and
  its summary names the two actions. The acceptance module does not need them.

Never-teardown of **tables** is still a PERMISSIONS FACT: the harness creates into the scratch
namespace with a scratch table prefix, has no DROP TABLE path, and the role still has no
`glue:DeleteTable` / `glue:DeleteDatabase`. A compromised job can remove objects under the
warehouse scratch prefix (OD-3) and cannot drop Glue tables or touch bronze.

## 3. Scratch expiry (outside the workflow)

Configure an S3 lifecycle expiry rule on the warehouse scratch prefix (e.g. 14 days) once, by hand.
Review the scratch Glue database at a documented cadence (monthly is fine). This is deliberately
neither a delete path in CI nor a human remembering to reap.

## 4. Variables + secrets — prefer the environment scope

Both scopes work and **the names are identical either way**, so nothing in the workflow changes
with the choice. Prefer **environment-scoped** values (Settings → Environments → `aws-acceptance`)
over repository-level ones: an environment secret is readable only by a job that declares
`environment: aws-acceptance`, and such a job has already passed §1's required-reviewer approval
and `main`-only deployment-branch rule. Repository-level values are readable by any job in any
workflow on any branch, which puts them one workflow edit away from a topic-branch read. The
gating you configured in §1 is worth more when the values sit behind it.

**Variables** (environment → Variables, or Settings → Secrets and variables → Actions → Variables)
— none account-identifying, so plain variables are fine:
`AWS_ACCEPTANCE_REGION` (**= the table bucket's region**, e.g. `us-east-2`),
`REPARK_ACCEPT_DS`, `REPARK_ACCEPT_ENTITY`, `REPARK_ACCEPT_ID_COL`,
`REPARK_ACCEPT_BRONZE_BUCKET` (a bucket **you own** — the committed default is a synthetic
placeholder the harness refuses to sign requests against), `REPARK_ACCEPT_WAREHOUSE` (likewise,
e.g. `s3://<your-warehouse>/`).

**Secrets** — account-identifying, so masked: `AWS_ACCEPTANCE_ROLE_ARN` (the role ARN embeds the
account id), `TABLE_BUCKET_ARN` (absent ⇒ the S3 Tables leg SKIPs, exactly as the test module
behaves locally).

## 5. First-run acceptance

**Pre-check the scratch namespace before the first dispatch.** If `testing_repark_acceptance`
already exists in Glue from earlier hand testing, it carries the `LocationUri` it was created with
— and the harness's create is *idempotent*, so it adopts the existing database silently rather
than failing. Every table write then lands under the OLD warehouse, and the only thing standing
between you and writes outside the prefix you scoped in §2 is that scratch-scoped IAM (Glue
writes and OD-3 `s3:DeleteObject` stay on that prefix; bronze is read-only). A denial is a
stop, not a design. Check first, with owner credentials:

```bash
aws glue get-database --name testing_repark_acceptance   # inspect LocationUri
aws glue get-tables    --database-name testing_repark_acceptance   # expect only testing_* names
```

Read the `LocationUri`: it must be the `REPARK_ACCEPT_WAREHOUSE` prefix you configured in §4
(`s3://<your-warehouse>/…`), not an older one. If it is stale — or `get-tables` shows anything
that is not a `testing_*` scratch table — delete the database with **owner** credentials before
the first dispatch, never from CI (the role has no `glue:DeleteTable` /
`glue:DeleteDatabase`, by design — §2):

```bash
aws glue delete-database --name testing_repark_acceptance
```

If `get-database` returns `EntityNotFoundException`, there is no stale state and nothing to do.

Runner behaviour (OIDC exchange, environment gating, cron) cannot be proven locally. After setup,
trigger one `workflow_dispatch` and confirm: the environment approval gate fires; the role assumes;
the module runs green (or the S3 Tables leg skips if `TABLE_BUCKET_ARN` is unset). That dispatch is
the acceptance step the design's §11 assigns to the operator. The parity-live workflow's first
dispatch is its own separate acceptance step (no AWS involved there). Whether that dispatch has
happened, and how it went, is current state: it is recorded in [../STATUS.md](../STATUS.md), never
restated here.

## 6. The legs this workflow runs

One row per test in `python/repark/tests/test_aws_acceptance.py`. Whether a leg has run, and how
it went, is current state and lives in [../STATUS.md](../STATUS.md) — never here.

| Leg | Catalog | Extra gate | What the run answers |
|---|---|---|---|
| `test_process_silver_acceptance_against_glue` | Glue | — | the source publish path: bronze `s3a` read → dedup → CTAS-or-MERGE → an idempotent second MERGE |
| `test_process_silver_acceptance_against_s3tables` | S3 Tables | `TABLE_BUCKET_ARN` | the same publish path against a table bucket (namespace carries no `location`) |
| `test_mor_merge_compact_expire_against_glue` | Glue | — | MW-4: v2 merge-on-read MERGEs → `rewrite_position_delete_files` + `rewrite_data_files` + `expire_snapshots`, rows unchanged, the CTAS snapshot gone |
| `test_mor_merge_compact_expire_against_s3tables` | S3 Tables | `TABLE_BUCKET_ARN` | MW-10 / OD-3b: whether `s3tables:PutTableData` authorizes expire's file removal on table storage |
| `test_v3_dv_dml_maintenance_against_glue` | Glue | — | LIVE-v3, answered 2026-09-02: Glue reproduces the local v3 numbers exactly — opt-in v3 MoR CTAS partitioned by identity `part`, 1 Puffin DV after the row `DELETE`, 2 after the `MERGE`, `rewrite_data_files` 12/2/2 leaving 0 DVs, `expire_snapshots` 14 → 1, then `register_table` of the final metadata location on a second session |
| `test_v3_dv_dml_maintenance_against_s3tables` | S3 Tables | `TABLE_BUCKET_ARN` | LIVE-v3 / `S3T-V3-1`, answered 2026-09-02: S3 Tables accepts `format-version = 3` at CREATE, so the leg runs the accepted branch — the same assertions with service-commit counts relaxed; the refusal branch (no table left behind, masked refusal text recorded, leg passes) stays wired and unused. `register_table` is not attempted (`S3T-1` / fork R126) |
| `test_create_or_replace_twice_against_glue` | Glue | — | ICE-GOLD-TWICE-1 (2026-09-14): a `testing_replace2_<uuid>` table — plain CTAS seed (3 rows), then `CREATE OR REPLACE TABLE … AS` twice (4 rows, then 5); rows equal the last SELECT, `id: int32`/`name: string`, `.snapshots` grows exactly one `append` per replace, and every earlier snapshot id is still present after each replace (history retained, not truncated). This is the live cell of the F-GLUE-REPLACE-1 publish path (RP-20, fork `edc38c6a`) |
| `test_create_or_replace_twice_against_s3tables` | S3 Tables | `TABLE_BUCKET_ARN` | the Glue leg's twin on a table bucket (namespace without `location`); only the absolute snapshot counts are relaxed for the service's own commits — rows, types, `append` ops on the three current ids, and the history-retention check are identical, so a truncated replace still fails even when the service adds a snapshot |

The two v3 legs need **no new IAM action and no new workflow variable**: they create and update
tables in the same scratch namespace, write and remove objects under the same warehouse scratch
prefix, and read metadata the role can already read. The Glue leg's `register_table` creates one
extra scratch table per run (`…_adopted`) under `glue:CreateTable`, which never-teardown already
allows.

The two replace legs need nothing new either: `CREATE OR REPLACE TABLE … AS` publishes through
`glue:UpdateTable` / `s3tables:UpdateTableMetadataLocation`, both already granted on the scratch
resources.

## 7. The dbt gold module

A second pytest step runs `python/dbt-repark/tests/test_aws_acceptance_gold.py`
(`test_gold_stage_on_glue`, ICE-GOLD-TWICE-1) **after** the silver module, on the same
`REPARK_AWS_ACCEPTANCE=1` gate and the same env block. The step installs nothing at run time —
the `sync + build native module` step already installs the Makefile `DBT_PINS`
(`dbt-core==1.9.11`, `dbt-spark==1.9.3`) before credentials are minted. The test builds a dbt
project on a per-run `testing_dbt1_<uuid8>` stem in the scratch namespace, runs `dbt run`, checks
the S6 fact and aggregate rows, records each gold model's snapshot count, runs `dbt run` a second
time — the in-place `create or replace` publish must leave the same rows and grow each model's
history by exactly one snapshot — then `dbt test` (ten blocks). It needs no new IAM action and no
new variable: `glue:UpdateTable` and the warehouse scratch prefix are already granted, and it
drops nothing.

## 8. The ICE-READ-PERF bench leg (dispatch-only)

The read-performance slate's AWS bed (task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md,
unit 0 and ruling R-3) runs as a second job of the same workflow, `ice-read-perf-bench`. It runs
**only** on a manual dispatch that asks for it, never on the nightly schedule. The nightly (and a
plain dispatch, whose `leg` defaults to `acceptance`) runs only `live-aws`, exactly as before.

```bash
gh workflow run aws-acceptance.yml -f leg=ice-read-perf-bench -f purpose=<unit, e.g. ice-read-perf-0-baseline>
```

**The owner approves each dispatch at the `aws-acceptance` environment gate (§1)**: the job
declares the same environment, so no credential mints before that approval. Check the ref and
the commit, then approve.

What the job does, in order (the same pinned actions as `live-aws`, the same ref guard,
`persist-credentials: false`, `id-token: write` scoped to the job):

1. Checkout, the Rust toolchain and the cache, then `cargo bench -p repark-spark --bench
   ice_read_perf --no-run`: the bench is built **before** any credential exists.
2. It refuses an unset or placeholder `REPARK_ACCEPT_WAREHOUSE` and an unset `TABLE_BUCKET_ARN`
   before signing any request. Both legs are required, and neither is skipped.
3. It mints the credentials (session name `repark-ice-read-perf-bench`, account id masked).
4. **S3 Tables.** `setup --phase create` on `testing_repark_acceptance.ice_read_perf_bench`
   (idempotent: the namespace without a location, the empty table with the bed schema, or a
   report that it exists). Then `put-table-maintenance-configuration --type icebergCompaction`
   with status `disabled`, and `get-table-maintenance-configuration`. The job fails before any
   write unless compaction reads `disabled`. Then `setup --phase write`: it writes the 200 files
   only into an empty table, skips a table that already holds exactly the bed, and fails loud on
   any other state.
5. **Glue.** The same `create` / `write` on `ice_read_perf_bench` in the Glue scratch namespace
   `testing_repark_acceptance` (the namespace at `<REPARK_ACCEPT_WAREHOUSE>/testing_repark_acceptance`,
   verified as §5 requires). Glue's compaction optimizer is off unless someone enables it on the
   table. The job makes no optimizer call and says so in its summary.
6. For each catalog, `run --mode cold`, `warm`, `concurrent`, `concurrent-cold` with `--repeat 1
   --out <json>` (`BENCH_REPEAT`).
7. The job summary gets one line per run with the bytes it read (`run_io_total.bytes` of the
   run's JSON: every request of every session the run opened) and the dispatch total. The eight
   JSONs upload as the `ice-read-perf-bench` artifact (30 days).

**R-3 on every step.** Every `setup` phase ends with the table-size check, and every `run` makes
it before its first scan. Above 3 × 1024³ bytes the bench prints `R3-SIZE-FLAG …`, appends it to
the job summary and exits 3. No step sets `continue-on-error`, so the job fails at once. Raising
the limit is an owner decision, never an option. The one Slack note to the owner that the slate asks
for is sent by whoever dispatched the run, on reading the failed job's summary.

**Cost.** The slate's R-3 estimate was about 12 GB read per dispatch, so about $1 of data transfer
out at about $0.09/GB, plus cents of requests and about $0.05 per table-month of storage, and
$5–10 for six dispatches (cap $25). From the 20-file local smoke scaled to 200 files (see the unit
ledger), one pass of Q1–Q7 reads about 2.7 GB and one round of the four concurrent queries about
2.4 GB. The job runs every mode at `BENCH_REPEAT` = `"1"` (ruling Q-24a-1, 2026-09-19): cold one
pass, warm two (one warm-up), concurrent two rounds (one warm-up), concurrent-cold one round, so
**about 31 GB per dispatch, about $2.8** before AWS's 100 GB/month free transfer tier, and about
$17 for six dispatches before the free tier (about $8 after it), inside the $25 cap. At `"3"` a
dispatch would read about 70 GB and six would pass the cap, so raising `BENCH_REPEAT` is an owner
decision. The writes (two tables of about 1.4 GB each, written once) are inbound and free. The
summary's dispatch total is the measured figure to check against this estimate.

**The scratch lifecycle rule (§3) and the Glue bench table.** The Glue bench table's files live
under the warehouse scratch prefix (`<REPARK_ACCEPT_WAREHOUSE>/testing_repark_acceptance/ice_read_perf_bench/`).
The slate reuses that table from the baseline through the re-measure gate, which is weeks. A
14-day expiry rule over the whole scratch prefix would delete its data files under a live
table. The next dispatch would then find the table "exact" by metadata, skip the write, and fail
its first scan. Before the baseline dispatch the owner either scopes the lifecycle rule away from
that one table prefix or accepts that such a failure means dropping the table and writing it
again. The S3 Tables table is not under that rule; its storage is the table bucket.

**A wedged bench table.** If a `write` phase dies part-way (a network error at file 57 of 200),
the next dispatch refuses the table (neither empty nor exact). The role cannot drop tables, by
design (§2). The owner drops it with owner credentials (`aws s3tables delete-table …` /
`aws glue delete-table …`) and dispatches again. The slate drops both bench tables when its
re-measure gate closes. That too is an owner action.

**The `baseline` input (the before half of a pair on one head).** The dispatch takes
`-f baseline=true` (default `false`). When true, all eight `run --mode` invocations carry
`--baseline`: every session they open runs with page-index row selection off and no shared
caches, and each JSON carries `"baseline": true` with its `baseline_switches` while the job
summary names `baseline=true` beside the purpose. Dispatch the pair on the same head — once
without the input, once with it — and compare the two artifacts. What the baseline cannot
switch off (the timestamp predicate pushdown, the `count(*)` fold) stays on; see the
`ice-bench-baseline-1` ledger for what the number means.

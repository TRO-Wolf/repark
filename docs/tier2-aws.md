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

## 2. IAM role (OIDC trust: one environment sub; permissions: create-only, no delete)

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
**resource ARN** (never `Resource: "*"`), and **no delete of any kind**:

- `s3:GetObject` / `s3:ListBucket` on the **bronze** bucket + prefix the harness reads
  (`REPARK_ACCEPT_BRONZE_BUCKET`) — read-only.
- `s3:PutObject` / `s3:GetObject` / `s3:ListBucket` on the **warehouse** scratch prefix only
  (`REPARK_ACCEPT_WAREHOUSE` + the `testing_repark_acceptance/` namespace). **No `s3:DeleteObject`.**
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
- S3 Tables (optional leg): create/get on the one table bucket ARN only. **No delete actions.**

Never-teardown is thereby a PERMISSIONS FACT: the harness is create-only into the scratch namespace
with a scratch table prefix and has no teardown path, and a compromised or buggy job cannot delete
regardless of what it runs.

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
between you and writes outside the prefix you scoped in §2 is the create-only role's denial. A
denial is a stop, not a design. Check first, with owner credentials:

```bash
aws glue get-database --name testing_repark_acceptance   # inspect LocationUri
aws glue get-tables    --database-name testing_repark_acceptance   # expect only testing_* names
```

Read the `LocationUri`: it must be the `REPARK_ACCEPT_WAREHOUSE` prefix you configured in §4
(`s3://<your-warehouse>/…`), not an older one. If it is stale — or `get-tables` shows anything
that is not a `testing_*` scratch table — delete the database with **owner** credentials before
the first dispatch, never from CI (the role has no delete actions, by design — §2):

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

# Tier-2 live-AWS setup — operator runbook

The `aws-acceptance` workflow (phase-3 PR-6; design `docs/design/python-facade.md` §7.4) runs the
acceptance module against real AWS nightly + on dispatch, merged code only, via OIDC. Everything on
this page is **operator-side, one-time setup**; no agent performs any of it, and no value on this
page is ever committed — variable/secret NAMES only.

## 1. GitHub environment

Create environment **`aws-acceptance`** (Settings → Environments) with a deployment-protection
required reviewer (yourself). The workflow names it; a manual dispatch waits for approval before
any credential is minted.

## 2. IAM role (trust: repo + branch + environment; permissions: create-only)

Trust policy: the GitHub OIDC provider, with the subject constrained to BOTH forms this workflow
presents — `repo:<owner>/<repo>:ref:refs/heads/main` AND
`repo:<owner>/<repo>:environment:aws-acceptance`. Never a wildcard repo or ref.

Permission policy — least-privilege over the scratch surface only, and **no delete of any kind**:
- Glue: create/get database + create/get/update table on the scratch database only.
- S3 (warehouse prefix): Put/Get/List on the scratch prefix only. **No `s3:DeleteObject`.**
- S3 Tables (optional leg): create/get on the one table bucket. **No delete actions.**

Never-teardown is thereby a PERMISSIONS FACT, not a docstring promise: the harness is create-only
into the scratch namespace with a scratch table prefix and has no teardown path, and a compromised
or buggy job cannot delete regardless of what it runs.

## 3. Scratch expiry (outside the workflow)

Configure an S3 lifecycle expiry rule on the scratch prefix (e.g. 14 days) once, by hand. Review
the scratch Glue database at a documented cadence (monthly is fine). This is deliberately neither
a delete path in CI nor a human remembering to reap.

## 4. Repository variables + secret

Variables (Settings → Secrets and variables → Actions → Variables): `AWS_ACCEPTANCE_ROLE_ARN`,
`AWS_ACCEPTANCE_REGION`, `REPARK_ACCEPT_DS`, `REPARK_ACCEPT_ENTITY`, `REPARK_ACCEPT_ID_COL`.
Secret: `TABLE_BUCKET_ARN` (account-identifying — this repository is public; absent ⇒ the S3
Tables leg SKIPs, exactly as the test module behaves locally).

## 5. First-run acceptance

Runner behaviour (OIDC exchange, environment gating, cron) cannot be proven locally. After setup,
trigger one `workflow_dispatch` and confirm: the environment approval gate fires; the role assumes;
the module runs green (or the S3 Tables leg skips if the secret is unset). That dispatch is the
acceptance step the design's §11 assigns to the operator. The parity-live workflow's first
dispatch is its own separate acceptance step (no AWS involved there).

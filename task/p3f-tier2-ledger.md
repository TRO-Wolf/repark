# Unit ledger — P3F: tier-2 CI (parity-live armed + net-new aws-acceptance)

**Unit:** phase-3 PR-6 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-6" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) §7.4 · **Port-Source:** v1
`main` @ `fc3f48102` (parity-live only; aws-acceptance is NET-NEW) · **Status:** IN FLIGHT

## Scope

Both artifacts are `.github/` carve-outs — this unit is ORCHESTRATOR-BUILT by the standing rules
(delegated agents never touch workflow files) and panel-verified.

- `parity-live.yml` ported and ARMED: nightly 07:17 + dispatch; the port source's paused-cron
  block removed and its `pull_request` paths trigger DROPPED BY DESIGN (tier-2 = merged code
  only, docs/testing.md) — not a pause carried over. `timeout-minutes: 45` added (the design's
  §7.4 explicit-wall rule; the ported tier-1 workflows keep their pin-era shape).
- `aws-acceptance.yml` net-new per design §7.4, every clause implemented: environment gate with
  human approval; job-scoped `id-token: write`; role/region from repository VARIABLES and the
  account-identifying ARN as a SECRET; mechanical non-main ref refusal (a dispatch's
  `contents: read` does not constrain the ref); acceptance MODULE only; `concurrency
  cancel-in-progress: false` (never cancel a live-AWS run mid-commit); SHA-pinned
  `aws-actions/configure-aws-credentials` v6.2.3; the `--no-project` + `VIRTUAL_ENV` run form so
  the maturin develop install is authoritative.
- `docs/tier2-aws.md` operator runbook: trust-policy subject constrained to BOTH
  `ref:refs/heads/main` AND `environment:aws-acceptance`; never-teardown as a PERMISSIONS fact
  (no delete actions of any kind); S3 lifecycle expiry outside the workflow; variable/secret
  NAMES only; first-dispatch acceptance steps.
- Makefile: `parity-live` target (mirrors the workflow step for step; `PARITY_LIVE_JAVA_HOME`
  overridable, default zulu-17). map.md lockstep: workflows map (two rows), docs map, this
  ledger, task map.

Out of scope: any AWS-side action (IAM/lifecycle/variables/secret = operator, runbook §1-§4);
the first dispatch runs (operator acceptance, design §11); required-check changes (neither
workflow is ever required).

## Census obligation

None — workflow + doc files only; no test names created, moved, or removed. `cargo test --
--list` and `pytest --collect-only` unchanged by construction.

## Gate results

Recorded at push time: workflows parse (11 files), zizmor clean (`make workflows-lint`),
`make ci` + `make test` green, both hygiene passes 0 (added-lines content semantics).
Honest disclosure: neither workflow's runtime behaviour is locally provable — the first
`workflow_dispatch` of each is the operator's post-merge acceptance step.

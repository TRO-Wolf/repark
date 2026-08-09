# Unit ledger — P3F: tier-2 CI (parity-live armed + net-new aws-acceptance)

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

**Unit:** phase-3 PR-6 · **Brief:**
[phase-3-python-facade.md](phase-3-python-facade.md) §1 "PR-6" · **Design:**
[docs/design/python-facade.md](../../design/python-facade.md) §7.4 · **Port-Source:** v1
`main` @ `fc3f48102` (parity-live only; aws-acceptance is NET-NEW) · **Status:** DELIVERED — merged
2026-08-08 (#22); archived 2026-08-09

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

The security-panel fix (below) required a source guard in the facade tree, so this unit is NOT
census-neutral after all: two always-run pins were ADDED to `test_acceptance_helpers.py`
(`test_placeholder_buckets_refuse_a_real_aws_run`, `test_operator_buckets_pass_the_guard`).
They are declared v2-only facade-census additions in `task/port/added-python-tests.txt`
(the facade collect-only count goes 2,497 → 2,499); the reconciliation identity becomes
`(v2_collected − added) ∪ deferred = pin_collected`, and PR-7's comparator subtracts the
additions list from the candidate side. Rust `-- --list` unchanged.

## Gate results

Recorded at push time: workflows parse (11 files), zizmor clean (`make workflows-lint`),
`make ci` + `make test` green, both hygiene passes 0 (added-lines content semantics).
Honest disclosure: neither workflow's runtime behaviour is locally provable — the first
`workflow_dispatch` of each is the operator's post-merge acceptance step.


## Security-panel fixes (orchestrator, post-verification)

The three-lens panel (SECURITY lens mandated) returned 4 HIGH + 4 MED on the net-new AWS
workflow — every one real, every one fixed:

- **Credentials mint LAST** (HIGH): `configure-aws-credentials` moved to immediately before the
  pytest step, after checkout/toolchains/uv-sync/maturin — no third-party build script or PyPI
  download runs inside the credentialed window. The rust-cache restore is likewise pre-credential
  now (subsumes the "cache inside credentialed job" LOW).
- **Placeholder-bucket refusal** (HIGH ×2, security+design): the EC-9-scrubbed `example-*` bucket
  constants in `_acceptance.py` are now `os.environ`-overridable (defaulting to the placeholders,
  so local runs are unaffected) and a new `assert_real_buckets_configured()` FAILS LOUD when a
  real-AWS run still targets a placeholder — a signed request to a squattable global name would
  disclose the assumed-role ARN + account id. Wired into both gated tests + two always-run pins;
  operator supplies `REPARK_ACCEPT_BRONZE_BUCKET`/`_WAREHOUSE` as repository variables.
- **OIDC sub corrected** (HIGH): GitHub sends ONE sub per run; with `environment:` set it is the
  environment form, and this repo uses the immutable subject format — the runbook now prescribes a
  single `StringEquals` on `repo:TRO-Wolf@64240326/repark@1325259325:environment:aws-acceptance`
  (the old ref+environment "AND" was unmatchable, and its "OR" workaround weaker). design §7.4 +
  the workflow header corrected to match.
- **Branch binding is the environment's deployment-branch policy** (HIGH): runbook §1 now
  instructs Selected-branches → `main` (the in-file ref guard is defence-in-depth only; the IAM
  sub can't pin the branch).
- **`mask-aws-account-id: true`** (MED) + role ARN promoted to a SECRET (public run logs).
- **IAM policy given resource-ARN shapes** (MED): explicit per-service ARNs, `Resource: "*"` banned,
  the bronze-read grant the harness actually needs, all delete actions banned.
- **pyspark pinned** (design HIGH, PR-4-owned file fixed here): `record = ["pyspark==4.1.2"]` +
  re-lock (was `>=3.5` resolving 4.2.0 — the drift detector's own oracle was unpinned); the
  parity-live/Makefile/map "4.1.2 pinned in uv.lock" claims are now true.
- Maps: parent `.github/map.md` + workflows-map tiering paragraphs updated (tier-2 landed here).
- Makefile `PARITY_LIVE_JAVA_HOME` defaults to `$(JAVA_HOME)` (no machine-specific committed path);
  parity-live timeout removed (fidelity to the verbatim port).

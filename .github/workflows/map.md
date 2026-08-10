# map — .github/workflows/

## Purpose

The tier-1 CI gates: every PR, no secrets, GitHub-hosted runners, read-only token. The main gate
(`ci.yml`) mirrors `make ci`; the rest are supply-chain and security scans. **Everything here
runs locally via `make preflight`** — tool versions are pinned identically in the Makefile and
these workflows (bump both in the same change).

Action-pinning convention (carried from the private v1 repository): **every** third-party action
is pinned to a **commit SHA** with a trailing `# vX` tag comment (Dependabot can still bump
SHAs). `dtolnay/rust-toolchain` is SHA-pinned because its refs are branches, not tags. Checkouts
set `persist-credentials: false`. CI cargo steps use `--locked`. `cargo-deny` / `cargo-audit` /
`uv` / ruff / taplo / typos / zizmor are version-pinned identically to the Makefile. zizmor is
**blocking**: any finding not suppressed in [../zizmor.yml](../zizmor.yml) fails the job.
Dependabot entries carry a 7-day `cooldown`. No `pull_request_target` anywhere; top-level
`permissions: contents: read` in every workflow.

> **Tiering (updated PR-6):** the PR gate is tier 1 (`ci.yml` + the always-run siblings —
> every PR, no secrets, GitHub-hosted, read-only token). Tier 2 (`parity-live.yml`,
> `aws-acceptance.yml`) is **merged-code-only** — nightly cron + manual dispatch, never
> `pull_request`, never a required check; `aws-acceptance.yml` is the ONE workflow that touches
> real AWS (OIDC, environment-gated, no secrets in tier 1). Not everything here runs locally.

## Contents

| Workflow | What |
|---|---|
| `ci.yml` | **Split rust jobs (phase-3 arming):** `rust-lint` (fmt + clippy `-D warnings` + panic-ban + check, cache prefix `v2-df54`) and `rust-test` (`cargo test --locked --workspace`, cache prefix `v2-df54-test`) — one prefix each, fresh disk each (free-disk step, no debuginfo/incremental), setup-python 3.12 on both so the PyO3 cdylib links from phase 3 on; run through the Makefile targets (`make rust-fmt-check` … `rust-test`) so the clippy/panic-ban split applies identically local and CI; prefix-key + shared-key pairs must match `cache-warm.yml` — bump both files in the same change. Repo guards (`scripts/check_map_md.sh` + `scripts/check_workflows_parse.py` + `scripts/check_crate_dag.sh` + `scripts/check_lib_rs.sh` + `scripts/check_manifest.sh` — the structural-manifest guard, added at FD-3; the guards **job name is deliberately unchanged**, since a rename would have to move the branch-protection required context in the same change); Python — renamed from "Python (ruff)" at PR-4 — (ruff check + format, `check_lib_py` thinness guard, `uv lock --locked` freshness gate, parity-harness pytest). Always-on. The v1 diff-classifier (`detect`) that path-filtered the heavy jobs stays deferred — it returns when rust-test exceeds ~3 min. |
| `cache-warm.yml` | Swatinem rust-cache pre-warm OFF the PR critical path: every push to main (+ weekly cron safety net) builds lint (`v2-df54`/`shared-key: lint`) and test (`v2-df54-test`/`shared-key: test`) artifacts under the same prefix-key + shared-key pairs the `ci.yml` `rust-lint` / `rust-test` jobs restore (shared-key is load-bearing — without it rust-cache mixes the job id into the key and warm saves are never restored), so PR jobs start hot even right after a dependency-family bump. |
| `cargo-deny.yml` | Rust license / banned / duplicate checks ([../../deny.toml](../../deny.toml)). **Always-run on PRs** — required check; see the zizmor row. |
| `audit.yml` | cargo-audit RustSec CVE scan over the Cargo dependency tree; weekly schedule + Cargo.toml/Cargo.lock path triggers; pin matches Makefile `CARGO_AUDIT_VERSION`. |
| `wheels.yml` | Two paths (ported at PR-5): `smoke` (PRs + main; **REQUIRED check**, deliberately UN-path-filtered) = host debug wheel via maturin-action + venv import smoke + the FULL facade suite against the packaged wheel (the real-artifact rule's CI home); `release-wheels` (tags only) = manylinux `--release` + artifact upload. **No rust-cache anywhere** (tag-triggered ⇒ zizmor cache-poisoning); wheels install by EXPLICIT file path, never bare `repark` (a PyPI name-reservation package outversions local 0.0.0 wheels). |
| `parity-live.yml` | Live PySpark oracle tier (ported + ARMED at PR-6): nightly cron 07:17 UTC + `workflow_dispatch`, NEVER `pull_request` (tier-2 = merged code only, docs/testing.md); Temurin 17 + setup-python 3.12 + rust + uv, `uv sync --locked --extra record --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark` + maturin develop, then the full facade suite with `REPARK_PARITY_LIVE=1` via `uv run --locked --no-sync`. **The sync flags are load-bearing** (2026-08-10 fix): `uv sync` is exact, so the bare `--extra record` spelling it carried before dropped the four facade extras (docs/port/census.md §4) and every polars/ML test skipped silently, and without `--locked` a run could rewrite the checked-in `uv.lock`; `--no-install-package repark` / `--no-sync` keep `maturin develop` authoritative. Mirrors `make parity-live` step for step — change one, change the other. Not a required check; the flag change has no PR-visible check, so its post-merge validation is the next nightly cron. |
| `aws-acceptance.yml` | Tier-2 live AWS (NET-NEW at PR-6, design §7.4): nightly 08:43 UTC + dispatch, `environment: aws-acceptance` (human approval), OIDC (`id-token: write` job-scoped, role/region from repo VARIABLES, `TABLE_BUCKET_ARN` as SECRET), mechanical non-main ref refusal, acceptance MODULE only, create-only + no-delete IAM posture (docs/tier2-aws.md). Not a required check. |
| `pip-audit.yml` | Python dependency CVE scan (ported at PR-4): `uv export --frozen --no-emit-workspace` → `uvx pip-audit --strict`; weekly cron + pyproject/uv.lock path triggers. **Path-filtered ⇒ must never be a required check** (task/lessons.md). |
| `typos.yml` | Spell-check ([../../.typos.toml](../../.typos.toml)); uvx-pinned, same version as `make spell-check`. |
| `taplo.yml` | TOML format + lint ([../../.taplo.toml](../../.taplo.toml)); uvx-pinned, same version as `make toml-check`. **Always-run on PRs** — required check; see the zizmor row. |
| `zizmor.yml` | Workflow security analysis; **blocking** — fails on any finding not suppressed in [../zizmor.yml](../zizmor.yml) (currently none); uploads SARIF as an artifact (`if: always()`). uvx-pinned, same version as `make workflows-lint`. **Always-run on PRs** (no `paths:` filter on `pull_request`): it is a required status check, and a path-filtered required check deadlocks every PR that doesn't match the filter (lesson 2026-08-07). |

**Not re-homed** (each returns with a concrete driver; the v1 assets are the templates):

- The `ci.yml` diff-classifier (`detect`) — deferred; **trigger: returns when rust-test exceeds
  ~3 min** (phase-1 decision, recorded here per the execution brief).

- `benches.yml`, `tpch-sf1.yml` — tier-3 benchmark ratio gates; slated for the V2 Engine
  Hardening campaign's performance-baseline work.
- `codeql.yml` — CodeQL security-extended matrix; later phases.
- `release.yml` — lands at the first tagged release, not before; see
  [../../docs/release.md](../../docs/release.md).

## I want to...

| ...do this | go to |
|---|---|
| Change the main PR gate | `ci.yml` (and mirror the change in the Makefile — dual wiring) |
| Bump a pinned tool version | the workflow AND the Makefile pin, same change |
| Suppress a zizmor finding | [../zizmor.yml](../zizmor.yml) (documented rationale required) |
| Understand release publishing | [../../docs/release.md](../../docs/release.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../Makefile](../../Makefile) (local equivalents);
  [../../docs/release.md](../../docs/release.md) (release engineering).

## Debug

| Symptom | First check |
|---|---|
| `cargo test` link failure in CI | `--workspace` must be used, never the all-features spelling (PyO3 extension-module; applies from phase 3, banned from day one) |
| taplo/typos red | `make toml-check` / `make spell-check` — same pinned tool as CI |
| cargo-deny / cargo-audit red | `make audit` locally — usually a newly published advisory, not the diff |
| map.md guard red | `bash scripts/check_map_md.sh` — a touched directory's `map.md` lags the change |
| crate-DAG guard red | `make check-crate-dag` — the named edge is undeclared, carries a kind the policy forbids, points up a tier, or a new crate is unclassified (SSOT: `scripts/check_crate_dag.py`) |
| repo-manifest guard red | `make check-manifest` — `repo-manifest.toml` disagrees with the workspace, a declared doc, a make target, STATUS.md, or a crate-root `map.md` (SSOT: `repo-manifest.toml` + `scripts/check_manifest.py`) |
| lib.rs thinness guard red | `make check-lib-rs` — inline test module or a root over its ceiling (SSOT: `scripts/check_lib_rs.py`) |
| workflow parse guard red | `make workflows-parse` — zizmor skips unparseable YAML, so this guard blocks it |
| zizmor red | `make workflows-lint` — same pinned zizmor; fix the workflow or suppress with rationale in [../zizmor.yml](../zizmor.yml) |
| `parity-live` green but every polars / ML facade test SKIPped | an `--extra` was dropped from the `uv sync` step, or a bare `uv run` re-synced over it. `uv sync` is EXACT: it uninstalls anything the named extras do not cover. Both the workflow and `make parity-live` must carry `--locked --extra record --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark`, and the pytest step must be `uv run --locked --no-sync` |
| PR BLOCKED with every check green | a *required* check never ran — usually a path-filtered or renamed workflow job vs. the branch-protection contexts (`gh api repos/{owner}/{repo}/branches/main/protection/required_status_checks`); required workflows must be always-run, and job renames must update the contexts in the same change (task/lessons.md 2026-08-07) |

First checks: reproduce with `make preflight` (the full CI surface; `make ci` for the core gate).
Note `ci.yml`'s Rust job calls the individual Makefile targets (never `make ci` wholesale); the
guards-job steps are raw commands — a new gate still needs **dual** Makefile + ci.yml wiring.
Escalate to: [../map.md#debug](../map.md).

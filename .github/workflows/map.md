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

## Contents

| Workflow | What |
|---|---|
| `ci.yml` | Rust (fmt + clippy `-D warnings` + panic-ban + check + `cargo test --locked --workspace`), run through the Makefile targets (`make rust-fmt-check` … `rust-test`) so the clippy/panic-ban split applies identically local and CI; the rust job restores the Swatinem rust-cache under the family prefix-keys `v2-df54` / `v2-df54-test` (must match `cache-warm.yml` — bump both in the same change); repo guards (`scripts/check_map_md.sh` + `scripts/check_workflows_parse.py` + `scripts/check_crate_dag.sh` + `scripts/check_lib_rs.sh`); Python (ruff check + format). Always-on. The v1 diff-classifier (`detect`) that path-filtered the heavy jobs stays deferred — it returns when rust-test exceeds ~3 min. |
| `cache-warm.yml` | Swatinem rust-cache pre-warm OFF the PR critical path: every push to main (+ weekly cron safety net) builds lint (`v2-df54`) and test (`v2-df54-test`) artifacts under the same prefix-keys the `ci.yml` rust job restores, so PR jobs start hot even right after a dependency-family bump. |
| `cargo-deny.yml` | Rust license / banned / duplicate checks ([../../deny.toml](../../deny.toml)). |
| `audit.yml` | cargo-audit RustSec CVE scan over the Cargo dependency tree; weekly schedule + Cargo.toml/Cargo.lock path triggers; pin matches Makefile `CARGO_AUDIT_VERSION`. |
| `typos.yml` | Spell-check ([../../.typos.toml](../../.typos.toml)); uvx-pinned, same version as `make spell-check`. |
| `taplo.yml` | TOML format + lint ([../../.taplo.toml](../../.taplo.toml)); uvx-pinned, same version as `make toml-check`. |
| `zizmor.yml` | Workflow security analysis; **blocking** — fails on any finding not suppressed in [../zizmor.yml](../zizmor.yml) (currently none); uploads SARIF as an artifact (`if: always()`). uvx-pinned, same version as `make workflows-lint`. **Always-run on PRs** (no `paths:` filter on `pull_request`): it is a required status check, and a path-filtered required check deadlocks every PR that doesn't match the filter (lesson 2026-08-07). |

**Not ported yet** (return in later phases; the v1 assets are the templates):

- The `ci.yml` diff-classifier (`detect`) — deferred; **trigger: returns when rust-test exceeds
  ~3 min** (phase-1 decision, recorded here per the execution brief).
- `pip-audit.yml` — Python CVE scan; returns with the Python packages (phase 3).
- `parity-live.yml` — live PySpark oracle tier (needs a JVM); phase 3.
- `wheels.yml` — wheel build + import smoke + packaged-wheel facade suite; phase 3.
- `benches.yml`, `tpch-sf1.yml` — tier-3 benchmark ratio gates; later phases.
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
| crate-DAG guard red | `make check-crate-dag` — the named edge points up a tier, or a new crate is unclassified (SSOT: `scripts/check_crate_dag.py`) |
| lib.rs thinness guard red | `make check-lib-rs` — inline test module or a root over its ceiling (SSOT: `scripts/check_lib_rs.py`) |
| workflow parse guard red | `make workflows-parse` — zizmor skips unparseable YAML, so this guard blocks it |
| zizmor red | `make workflows-lint` — same pinned zizmor; fix the workflow or suppress with rationale in [../zizmor.yml](../zizmor.yml) |
| PR BLOCKED with every check green | a *required* check never ran — usually a path-filtered or renamed workflow job vs. the branch-protection contexts (`gh api repos/{owner}/{repo}/branches/main/protection/required_status_checks`); required workflows must be always-run, and job renames must update the contexts in the same change (task/lessons.md 2026-08-07) |

First checks: reproduce with `make preflight` (the full CI surface; `make ci` for the core gate).
Note `ci.yml`'s Rust job calls the individual Makefile targets (never `make ci` wholesale); the
guards-job steps are raw commands — a new gate still needs **dual** Makefile + ci.yml wiring.
Escalate to: [../map.md#debug](../map.md).

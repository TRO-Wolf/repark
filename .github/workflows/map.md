# map — .github/workflows/

## Purpose

The tier-1 CI gates: every PR, no secrets, GitHub-hosted runners, read-only token. The main gate
(`ci.yml`) mirrors `make ci`; the rest are supply-chain and security scans. **Everything here
runs locally via `make preflight`** — tool versions are pinned identically in the Makefile and
these workflows (bump both in the same change).

Action-pinning convention (carried from the private v1 repository): **every** third-party action
is pinned to a **commit SHA** with a trailing `# vX` tag comment (Dependabot can still bump
SHAs). `dtolnay/rust-toolchain` is SHA-pinned because its refs are branches, not tags. Checkouts
set `persist-credentials: false`. CI cargo steps use `--locked`. `cargo-deny` / `uv` / ruff /
taplo / typos / zizmor are version-pinned identically to the Makefile. zizmor is **blocking**:
any finding not suppressed in [../zizmor.yml](../zizmor.yml) fails the job. Dependabot entries
carry a 7-day `cooldown`. No `pull_request_target` anywhere; top-level
`permissions: contents: read` in every workflow.

## Contents

| Workflow | What |
|---|---|
| `ci.yml` | Rust (fmt + clippy `-D warnings` + panic-ban + check + `cargo test --locked --workspace`), run through the guarded Makefile targets (`make rust-fmt-check` … `rust-test`) so the phase-0 empty-workspace guard and the clippy/panic-ban split apply identically local and CI; repo guards (`scripts/check_map_md.sh` + `scripts/check_workflows_parse.py`); Python (ruff check + format). Always-on: the phase-0 workspace is empty, so every job is cheap. v1's diff-classifier path filtering for heavy jobs returns with phase 1. |
| `cargo-deny.yml` | Rust license / banned / duplicate checks ([../../deny.toml](../../deny.toml)); carries the same phase-0 empty-workspace guard as `make rust-deny`. |
| `typos.yml` | Spell-check ([../../.typos.toml](../../.typos.toml)); uvx-pinned, same version as `make spell-check`. |
| `taplo.yml` | TOML format + lint ([../../.taplo.toml](../../.taplo.toml)); uvx-pinned, same version as `make toml-check`. |
| `zizmor.yml` | Workflow security analysis; **blocking** — fails on any finding not suppressed in [../zizmor.yml](../zizmor.yml) (currently none); uploads SARIF as an artifact (`if: always()`). uvx-pinned, same version as `make workflows-lint`. |

**Not ported yet** (return in later phases; the v1 assets are the templates):

- `audit.yml` / `pip-audit.yml` — Rust/Python CVE scans; return when there are dependencies to
  scan (phase 1 / phase 3).
- `parity-live.yml` — live PySpark oracle tier (needs a JVM); phase 3.
- `wheels.yml` — wheel build + import smoke + packaged-wheel facade suite; phase 3.
- `benches.yml`, `tpch-sf1.yml` — tier-3 benchmark ratio gates; later phases.
- `codeql.yml` — CodeQL security-extended matrix; later phases.
- `cache-warm.yml` — rust-cache pre-warm on main pushes; returns when CI builds are heavy
  enough to cache (phase 1).
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
| cargo-deny red | `make audit` locally — usually a newly published advisory, not the diff |
| map.md guard red | `bash scripts/check_map_md.sh` — a touched directory's `map.md` lags the change |
| workflow parse guard red | `make workflows-parse` — zizmor skips unparseable YAML, so this guard blocks it |
| zizmor red | `make workflows-lint` — same pinned zizmor; fix the workflow or suppress with rationale in [../zizmor.yml](../zizmor.yml) |

First checks: reproduce with `make preflight` (the full CI surface; `make ci` for the core gate).
Note `ci.yml`'s Rust job calls the individual Makefile targets (never `make ci` wholesale); the
non-Rust steps are raw commands — a new gate still needs **dual** Makefile + ci.yml wiring.
Escalate to: [../map.md#debug](../map.md).

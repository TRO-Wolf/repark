# map — repository root

## Purpose

RePark: a pure-Rust, no-JVM data engine over DataFusion + Arrow + the owned iceberg-rust fork,
with two SQL doors (native ANSI/Trino-style and a near-drop-in PySpark facade). This is the
front-door navigation map. See [README.md](README.md) for the overview and
[AGENTS.md](AGENTS.md) for the agent contract. The repo is at
**phase 1** of the port: the engine core is arriving — the workspace carries
`crates/repark-common` (error seed), `crates/repark-iceberg` (catalog + write over the owned
iceberg-rust fork, `[patch.crates-io]`-pinned), and `crates/repark-core` (the `ReparkSession`
engine API, landing commit-by-commit in PR-C).

## Contents

- `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rustfmt.toml`, `clippy.toml`, `deny.toml`,
  `.cargo/` — Rust workspace + tooling. `[workspace.dependencies]` is the single version table;
  workspace lints (`unsafe_code = "forbid"`) and the clippy `disallowed-methods` panic/spawn bans
  are in force.
- `crates/` — the Cargo workspace members (the engine). See [crates/map.md](crates/map.md).
- `pyproject.toml`, `.python-version` — Python tooling config (Ruff, line 100). The uv workspace
  member list arrives with phase 3.
- `Makefile` — developer command surface (`make help`). `make ci` is the canonical gate;
  `make verify` = ci + test; `make preflight` mirrors the full CI surface. Tool pins match the
  workflow pins.
- `.typos.toml`, `.taplo.toml`, `.pre-commit-config.yaml`, `.gitignore`, `scripts/` —
  tooling/config and the mechanical guards (`scripts/check_map_md.sh` is the map.md lockstep
  oracle; `make install-hooks` wires it).
- `CODEOWNERS` — maintainer ownership. `LICENSE`, `README.md` — repo front matter.
- `docs/` — contracts, ADRs, the port plan, and per-tier manuals. `task/` — todo + lessons
  trackers. `briefs/` — versioned delegated-agent slate briefs. `skills/` — the SEPMO control
  plane. `.github/` — tier-1 CI + Dependabot. `PROJECT.md` — north-star charter. `CLAUDE.md` — session
  orientation. `AGENTS.md` — the authoritative agent contract. `CONTRIBUTING.md` /
  `SECURITY.md` — public-repo policy.

## I want to...

| ...do this | go to |
|---|---|
| Understand the project intent / north star | [PROJECT.md](PROJECT.md) |
| Follow the agent rules | [AGENTS.md](AGENTS.md) |
| Understand the port plan / what arrives when | [docs/port/PLAN.md](docs/port/PLAN.md) |
| Read/extend the testing contract | [docs/testing.md](docs/testing.md) |
| Understand why a load-bearing decision was made | [docs/adr/map.md](docs/adr/map.md) |
| Operate under the SEPMO control plane | [skills/map.md](skills/map.md) |
| Read the manual for your model tier | [docs/skills/map.md](docs/skills/map.md) |
| See in-flight work / lessons | [task/map.md](task/map.md) |
| Touch CI | [.github/map.md](.github/map.md) |
| Read the phase briefs | [briefs/map.md](briefs/map.md) |
| Navigate the engine crates | [crates/map.md](crates/map.md) |
| Run the canonical gate | `make ci` (see `make help`) |
| Understand the mechanical guards | [scripts/map.md](scripts/map.md) |
| Understand the cargo tooling config | [.cargo/map.md](.cargo/map.md) |

## Pointers

- Up: — (repository root)
- Related: the private v1 repository is the port source; this repo is the public V2 target.

## Debug

First checks: `make ci`, then `make help` for the full target list. CI mirrors `make ci`.

| Symptom | First check |
|---|---|
| A cargo target loudly no-ops | Should no longer happen — the workspace has members; see the Makefile header |
| Pre-commit hook rejects a commit | `bash scripts/check_map_md.sh` — the touched directory's map.md must be staged in the same commit |
| A gate is unclear | `make help`; [docs/testing.md](docs/testing.md) and [AGENTS.md](AGENTS.md) are authoritative |

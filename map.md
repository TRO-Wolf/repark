# map — repository root

## Purpose

RePark: a pure-Rust, no-JVM data engine over DataFusion + Arrow + the owned iceberg-rust fork,
with two SQL doors (native ANSI/Trino-style and a near-drop-in PySpark facade). This is the
front-door navigation map. See [README.md](README.md) for the overview and
[AGENTS.md](AGENTS.md) for the agent contract. The repo is at
**phase 3** of the port (Python binding + facade + parity = milestone one; design
[docs/design/python-facade.md](docs/design/python-facade.md), slate in
[task/todo.md](task/todo.md)). Phases 1–2 are complete — the workspace carries
`crates/repark-common` (error seed + the surface-matrix registry), `crates/repark-iceberg`
(catalog + write over the owned iceberg-rust fork, `[patch.crates-io]`-pinned),
`crates/repark-core` (the `ReparkSession` engine API + the frozen `SqlDialect` /
`SessionExtension` seams), `crates/repark-functions` (Spark-semantics scalar/aggregate function
shims, tier 3), `crates/repark-spark` (the Spark-SQL door: router + `SparkDialect` +
`SparkExtension`), `crates/repark-ta` (bit-exact TA-Lib kernels + the optional window-UDF
layer, tier 3), and `crates/repark-sql` (the ANSI/Trino-flavoured door: `AnsiDialect` + guard
set + wrong-door sniff + the curated `WITH (…)` vocabulary). Phase 3 has landed
`crates/repark-ml` (native ML estimator kernels, tier 3, ported verbatim in PR-2) and
`crates/repark-python` (the PyO3 cdylib, **tier 4 "bindings"**, ported in PR-3 under design §3's
edit classes) and `python/repark-parity` (the parity harness + the census machinery + the NEW
report comparator that is the port's acceptance gate, PR-4) and `python/repark` (the PySpark
facade wheel — 53 source modules and the 127-file suite, ported verbatim under design §3's
EC-4/EC-7/EC-9 in PR-5). A wheel is buildable from PR-5 onward; it is not yet tagged.

## Contents

- `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rustfmt.toml`, `clippy.toml`, `deny.toml`,
  `.cargo/` — Rust workspace + tooling. `[workspace.dependencies]` is the single version table;
  workspace lints (`unsafe_code = "forbid"`) and the clippy `disallowed-methods` panic/spawn bans
  are in force.
- `crates/` — the Cargo workspace members (the engine). See [crates/map.md](crates/map.md).
- `pyproject.toml`, `.python-version`, `uv.lock` — the **uv workspace root** (virtual — not
  itself a package): the member list, the `dev` dependency group, and the Ruff config (line 100).
  Both members are declared (`python/repark`, `python/repark-parity`); the three facade
  per-file-ignore blocks (`ml/**`, `session/**`, `dataframe/**`) are **load-bearing**, not style —
  they are how the r26 region splits keep their pre-split import paths (design §2.3).
  `uv.lock` is checked in from phase 3 on and is validated, never rewritten, by `uv lock --locked`.
- `Makefile` — developer command surface (`make help`). `make ci` is the canonical gate;
  `make verify` = ci + test; `make preflight` mirrors the full CI surface. Tool pins match the
  workflow pins.
- `.typos.toml`, `.taplo.toml`, `.pre-commit-config.yaml`, `.gitignore`, `scripts/` —
  tooling/config and the mechanical guards (`scripts/check_map_md.sh` is the map.md lockstep
  oracle; `make install-hooks` wires it). `.typos.toml`'s `extend-words` carries the domain
  vocabulary the checker would otherwise "correct" — including the TA-Lib indicator names
  (`TEMA`, `CMO`) that arrived with `crates/repark-ta`; the lines are carried from the
  port-source pin's own config, never invented to silence a real misspelling.
- `CODEOWNERS` — maintainer ownership. `LICENSE`, `README.md` — repo front matter.
- `python/` — the uv workspace members: the facade wheel (`repark`) and the parity harness
  (`repark-parity`). See [python/map.md](python/map.md).
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
| Navigate the Python tree | [python/map.md](python/map.md) |
| Build the wheel / run the facade suite | [python/repark/map.md](python/repark/map.md) |
| Run or compare a census | [docs/port/census.md](docs/port/census.md) |
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

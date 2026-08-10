# map — repository root

## Purpose

RePark: a pure-Rust, no-JVM data engine over DataFusion + Arrow + the owned iceberg-rust fork,
with two SQL doors (native ANSI/Trino-style and a near-drop-in PySpark facade). This is the
front-door navigation map. See [README.md](README.md) for the overview,
[AGENTS.md](AGENTS.md) for the agent contract, and **[STATUS.md](STATUS.md) for current state**
(release state, delivery, active workstreams — the single source of truth; do not restate it here).

The workspace carries nine delivered crates: `crates/repark-common` (error seed + the
surface-matrix registry), `crates/repark-iceberg` (catalog + write over the owned iceberg-rust
fork, `[patch.crates-io]`-pinned), `crates/repark-core` (the `ReparkSession` engine API + the
frozen `SqlDialect` / `SessionExtension` seams), `crates/repark-functions` (Spark-semantics
scalar/aggregate function shims, tier 3), `crates/repark-spark` (the Spark-SQL door: router +
`SparkDialect` + `SparkExtension`), `crates/repark-ta` (bit-exact TA-Lib kernels + the optional
window-UDF layer, tier 3), `crates/repark-sql` (the ANSI/Trino-flavoured door: `AnsiDialect` +
guard set + wrong-door sniff + the curated `WITH (…)` vocabulary), `crates/repark-ml` (native ML
estimator kernels, tier 3), and `crates/repark-python` (the PyO3 cdylib, **tier 4 "bindings"**).
The Python tree ships `python/repark-parity` (the parity harness + census machinery + report
comparator) and `python/repark` (the PySpark facade wheel). A wheel is buildable; it is not yet
tagged (see [STATUS.md](STATUS.md) "Release state").

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
- `repo-manifest.toml` — the **machine-readable structural facts**: the component inventory
  (path / layer / status for every crate, delivered and planned), the current phase, the
  canonical gate commands, and the documentation index. It is a validated MIRROR, never a second
  source of truth — `scripts/check_manifest.py` (`make check-manifest`, in `make ci`) checks
  every field against the Cargo workspace, the Makefile, STATUS.md, the declared documents and
  the crate-root `map.md` files, and cross-checks each `layer` against the dependency-policy
  SSOT in `scripts/check_crate_dag.py`. Structural drift is a red gate, not a stale sentence.
- `.typos.toml`, `.taplo.toml`, `.pre-commit-config.yaml`, `.gitignore`, `scripts/` —
  tooling/config and the mechanical guards (`scripts/check_map_md.sh` is the map.md lockstep
  oracle; `make install-hooks` wires it). `.typos.toml`'s `extend-words` carries the domain
  vocabulary the checker would otherwise "correct" — including the TA-Lib indicator names
  (`TEMA`, `CMO`) that arrived with `crates/repark-ta`; the lines are carried from the
  port-source pin's own config, never invented to silence a real misspelling.
- `CODEOWNERS` — maintainer ownership. `LICENSE`, `README.md` — repo front matter.
- `python/` — the uv workspace members: the facade wheel (`repark`) and the parity harness
  (`repark-parity`). See [python/map.md](python/map.md).
- `docs/` — contracts, ADRs, the port plan, per-tier manuals, and `docs/history/` (the archive of
  closed campaigns — the v1 → v2 port and the Agent-Agnostic Front-Door campaign, both off the
  normal read path; see [docs/history/map.md](docs/history/map.md)).
  `task/` — the rules in force (`lessons.md`), the process metrics ledger (`metrics.md`), the
  ledger of each unit in flight, and the live acceptance inputs (`task/port/`, `task/census/`); the
  backlog itself lives in [STATUS.md](STATUS.md). `briefs/` — slate briefs for campaigns that are
  still running (holding only its `map.md` between campaigns; a closed campaign's slate is
  archived with it).
  `skills/` — the SEPMO control plane. `.github/` — tier-1 CI + Dependabot. `PROJECT.md` — north-star charter. `STATUS.md` — the
  single source of truth for current state (release state, delivery, active workstreams, deferred
  work). `AGENTS.md` — **the single authoritative contributor contract** (holds the precedence
  chain, invariants, safety boundaries; written for any human or agent, names no tool).
  `ARCHITECTURE.md` — component boundaries, the crate DAG, and the three runtime flows.
  `DEVELOPMENT.md` — build / test / verify, the `make` targets, the CI surface, troubleshooting.
  `CLAUDE.md` — the **Claude adapter** (tool mechanics only; zero authoritative facts).
  `CONTRIBUTING.md` / `SECURITY.md` — public-repo policy.
- `.agent/` — tool-neutral + per-tool agent adapters (`common.md` + `claude.md` + `codex.md` /
  `cursor.md` stubs); each is a thin pointer into the spine, carrying no authoritative facts. See
  [.agent/map.md](.agent/map.md).

## I want to...

| ...do this | go to |
|---|---|
| Know the current state (release / delivery / what's next) | [STATUS.md](STATUS.md) |
| Understand the project intent / north star | [PROJECT.md](PROJECT.md) |
| Follow the authoritative contributor contract | [AGENTS.md](AGENTS.md) |
| Understand the architecture / crate DAG / runtime flows | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Build / test / verify locally (setup, `make` targets, CI) | [DEVELOPMENT.md](DEVELOPMENT.md) |
| Onboard as an agent (any tool) | [.agent/map.md](.agent/map.md) |
| Understand the port plan / what arrives when | [docs/port/PLAN.md](docs/port/PLAN.md) |
| Read/extend the testing contract | [docs/testing.md](docs/testing.md) |
| Understand why a load-bearing decision was made | [docs/adr/map.md](docs/adr/map.md) |
| Operate under the SEPMO control plane | [skills/map.md](skills/map.md) |
| Read the manual for your model tier | [docs/skills/map.md](docs/skills/map.md) |
| See in-flight work / lessons | [task/map.md](task/map.md) |
| Read how the engine got here (the archived port record) | [docs/history/port-v2/README.md](docs/history/port-v2/README.md) |
| Read how the front door got here (the archived campaign record) | [docs/history/frontdoor/README.md](docs/history/frontdoor/README.md) |
| See what a closed campaign cost, caught and missed | [task/metrics.md](task/metrics.md) |
| Touch CI | [.github/map.md](.github/map.md) |
| Read a running campaign's slate brief | [briefs/map.md](briefs/map.md) |
| Navigate the engine crates | [crates/map.md](crates/map.md) |
| Navigate the Python tree | [python/map.md](python/map.md) |
| Build the wheel / run the facade suite | [python/repark/map.md](python/repark/map.md) |
| Run or compare a census | [docs/port/census.md](docs/port/census.md) |
| Run the canonical gate | `make ci` (see `make help`) |
| Declare a new crate / doc / gate command structurally | [repo-manifest.toml](repo-manifest.toml) (validated by `make check-manifest`) |
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
| `manifest: FAIL …` | `bash scripts/check_manifest.sh` — [repo-manifest.toml](repo-manifest.toml) disagrees with the workspace, a doc, a make target, STATUS.md, or a crate map ([scripts/map.md#debug](scripts/map.md) has the per-message table) |

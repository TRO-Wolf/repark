# CLAUDE.md

Orientation for Claude sessions working in the **repark** repo. This file exists so a new session
can act correctly on turn 1. [AGENTS.md](AGENTS.md) is the authoritative project contract;
[README.md](README.md) is the overview. CLAUDE.md and AGENTS.md must not drift — when a rule changes,
update both.

> **A note on the XML tags.** A few sections are wrapped in semantic tags (`<read_order>`,
> `<map_md_navigation>`, `<non_negotiable_invariants>`, `<testing_discipline>`, `<subagent_policy>`).
> They mark must-not-skip / must-not-violate regions so an agent can locate and obey them
> unambiguously; they carry no meaning beyond "this bounded region is load-bearing."

## What repark is

A pure-Rust, **no-JVM** single-node data engine with first-class Apache Iceberg support.
Positioning: *"Trino's SQL, DuckDB's deployment model, deepest Iceberg support."* Built **on**
Apache DataFusion + Arrow + our **owned iceberg-rust fork** (all pure-Rust), with native PyO3
bindings. Compute runs in Rust; data crosses the Python boundary as Apache Arrow, zero-copy.

Two user-facing **doors**, no blended parser:

- **Native door** — a lazy DataFrame API plus `repark.sql()` speaking an ANSI/Trino-style dialect.
- **Spark facade door** — a near-drop-in PySpark facade whose `.sql()` keeps the Spark dialect, so
  existing PySpark pipelines migrate with only the import line changed.

The Iceberg machinery (commit semantics, MERGE, snapshots, evolution) is shared beneath both doors.

**Current state: phase 0 of the port** — governance, testing contract, mechanical gates, and tier-1
CI are in place on an **empty Cargo workspace**. No crates exist yet. The crate skeleton below is
the *target*; code arrives by porting the private v1 repository phase by phase — see
[docs/port/PLAN.md](docs/port/PLAN.md).

<read_order>

## Read order (every session)

1. **This file (CLAUDE.md)** — repo intent and constraints.
2. **The operating manual for your model tier** in [docs/skills/](docs/skills/):
   [Opus.md](docs/skills/Opus.md) (canonical full contract), [Sonnet.md](docs/skills/Sonnet.md), or
   [Haiku.md](docs/skills/Haiku.md). Read the one matching the model you are running as.
3. **[docs/testing.md](docs/testing.md)** — the mandatory testing-discipline contract. Tests-with-code
   is a hard block. Read it before any code change.
4. **[AGENTS.md](AGENTS.md)** — the authoritative project contract (target crate map, PyO3 build notes,
   the version-pin contract, what's out of scope).
5. **[skills/sepmo/SKILL.md](skills/sepmo/SKILL.md)** — the SEPMO v2 control plane (lifecycle/orchestration:
   proposition-ledger scope audit → adversarial Actor–Critic → per-PR delivery → retrospective; the spine
   routes to per-phase [references](skills/sepmo/references/map.md)). Binds to this repo via
   [skills/sepmo/binding-manifest.md](skills/sepmo/binding-manifest.md); it cedes the engineering contract
   to the files above (see `## Precedence`).
6. **[task/todo.md](task/todo.md)** (in-flight work) and **[task/lessons.md](task/lessons.md)** (DO /
   DO-NOT rules in force; append date-stamped).
7. The `map.md` of every directory your task will touch (see below).

</read_order>

## Precedence

The authority chain on any conflict (highest first). **This is the single home for the chain** — other
files ([AGENTS.md](AGENTS.md), [skills/sepmo/binding-manifest.md](skills/sepmo/binding-manifest.md)) point
here, never restate it.

> [CLAUDE.md](CLAUDE.md) = [AGENTS.md](AGENTS.md) (the two authoritative contracts, kept in sync) **>**
> [PROJECT.md](PROJECT.md) (north-star intent) **>** Status SSOT (PROJECT.md "Current state" /
> [task/todo.md](task/todo.md)) **>** engineering contract ([docs/skills/Opus.md](docs/skills/Opus.md) +
> [docs/testing.md](docs/testing.md)) **>** SEPMO ([skills/sepmo/SKILL.md](skills/sepmo/SKILL.md) —
> lifecycle/orchestration only).

SEPMO governs *how work flows* (scope audit → Actor–Critic → PR → delivery → retrospective); it never
overrides an engineering rule. When SEPMO and a contract appear to conflict, the contract wins and the
conflict becomes a clarifying question (SEPMO doctrine D1).

<map_md_navigation>

## `map.md` navigation — mandatory

**Every directory** carries a single `map.md` — strictly, no exceptions, including container dirs that
hold only subdirectories. The only exclusions are version-control metadata (`.git/`) and gitignored /
vendored trees (`target/`, `.venv/`, caches). Each `map.md` documents `Purpose`, `Contents`, an
`I want to... → go to` table, `Pointers`, and a `## Debug` section.

**Before editing any file:** read the `map.md` of every directory your task will touch.

**Lockstep update rule (hard):** whenever code is created, changed, moved, or deleted, update that
directory's `map.md` in the *same change*. New directory → create its `map.md` in the same change. A
code change is not "done" until the touched directories' `map.md` files reflect it. The
`scripts/check_map_md.sh` pre-commit guard enforces this; `make install-hooks` wires it.

</map_md_navigation>

<non_negotiable_invariants>

## Non-negotiable invariants

The locked decisions a new session is most likely to accidentally violate. Rationale lives in
[docs/adr/](docs/adr/) and [task/lessons.md](task/lessons.md).

- **iceberg-rust is forked & owned; DataFusion is built ON (not forked).** The `TRO-Wolf/iceberg-rust`
  fork is a **sibling sub-project we own** (1:1 Java `iceberg-core` parity); it stays a **separate
  repo, never vendored**. The engine-agnostic table-format work (write actions, schema/partition
  evolution, snapshot management, views, maintenance) lives **in the fork**, not here. When the
  workspace gains crates (phase 1), `[patch.crates-io]` sources the whole `iceberg*` family from the
  fork, rev-pinned. The fork's `iceberg-datafusion` is consumed as a **supported product surface**;
  MERGE stays RePark-owned. Fork capability status lives ONLY in the fork's
  `docs/parity/GAP_MATRIX.md`, and the engine-facing recipes in its `docs/ENGINE_CONTRACT.md` —
  link them, never restate them here. DataFusion stays a normal upstream dep — do not fork it.
  See [docs/adr/0001-own-iceberg-fork.md](docs/adr/0001-own-iceberg-fork.md).
- **Two honest SQL doors, no blended parser.** Native `repark.sql()` = ANSI/Trino-style; the facade's
  `.sql()` = Spark dialect. Each door declares its dialect; guessing which dialect a string meant is
  banned. New SQL surface lands with both spellings + one test row per door. See
  [docs/adr/0002-two-sql-doors.md](docs/adr/0002-two-sql-doors.md).
- **Server-prep disciplines from day one:** everything-through-Session (no global mutable state, no
  env reads at query time) and bindings-as-thin-adapter (one internal engine API; PyO3 and a future
  Flight SQL handler are both thin adapters). See
  [docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md).
- **No PyIceberg — in any form.** Not the Apache `pyiceberg` Python lib, not the `pyiceberg_core` crate.
  Iceberg is reached only through the Rust `iceberg-rust` crates + our own PyO3 bindings.
- **No Sail / pysail dependency.** Own-the-stack was chosen; Sail is reference/prior-art only.
- **Distribution is deferred** behind the `ExecutionBackend` seam. Single-node DataFusion is the
  target; the posture is fleet-parallel → server mode → distributed only if a query outgrows one box.
- **`unsafe_code = "forbid"` workspace-wide, EXCEPT the future `crates/repark-python`** (PyO3 macros
  expand to `unsafe`; that crate will set a local `allow`). Do not introduce `unsafe` anywhere else.
- **Test with `cargo test --workspace`, NEVER the all-features flag.** Enabling every feature turns
  on the cdylib's `extension-module`, which tells PyO3 not to link libpython and breaks a standalone
  test binary (applies from phase 3; the rule is in force now so it is never re-litigated). See
  [AGENTS.md](AGENTS.md) "PyO3 build notes".
- **Pin one DataFusion version** across `datafusion` + `datafusion-spark` + `iceberg*`. Bump together
  with `rust-toolchain.toml`. `Cargo.lock` is checked in.
- **Tier-2 CI (live AWS) never runs against unmerged code.** Nightly on `main` + manual dispatch
  only, via OIDC role assumption. No self-hosted runners in any tier.
- **The Spark facade is near-drop-in, not drop-in.** Only the import line changes; SQL strings stay
  identical. We are not implementing the Spark Connect protocol.

</non_negotiable_invariants>

<testing_discipline>

## Testing discipline — mandatory

Full contract in [docs/testing.md](docs/testing.md). Two non-negotiable rules:

1. **Hard block: tests land in the same commit/PR as the code being tested.** No "later". The only
   exempt changes are ones with no testable surface (pure docs, comment-only edits, renames,
   lockfile-only bumps, behaviourless config/stub scaffolding).
2. **Test-per-change, not coverage %.** Every behavior gets a test; every spec invariant gets ≥1 test.
   The **entry-point matrix** is the central structure: native DataFrame, ANSI SQL, and the Spark
   facade are each a row for every behavior and divergence class. A **divergence-class claim**
   ("Spark parity", "fixed #n") pins *every* class it names, per user entry point, on the Arrow path
   (`collect`/`to_arrow`, value AND type — never only `show`); one representative case is not the
   claim (docs/testing.md "Divergence-class claims").

"Calibration-sensitive" is reframed per domain: decimal128 bit-exact for DECIMAL arithmetic, row-order
fixtures for null/sort/window semantics, `f64::to_bits` only for float aggregation across partitions,
schema-equality for evolution. Forbidden without a linked tracking issue: `#[ignore]`, commented-out
tests, `// TODO: add test`, `assert!(is_ok())` as the whole body, `--skip` in CI, bypassing hooks.

</testing_discipline>

## Tech stack

- **Language**: Rust (edition 2024, toolchain pinned in `rust-toolchain.toml`).
- **Engine**: Apache DataFusion + iceberg-rust + iceberg-datafusion; `datafusion-spark` for
  Spark-compatible functions behind the facade door.
- **Data format**: Apache Arrow throughout; zero-copy across the PyO3 boundary (Arrow C Data Interface).
- **Python**: PyO3 cdylib built by **maturin** (arrives phase 3); native lazy API + PySpark facade.
- **Catalogs**: AWS Glue (primary) + S3 Tables (secondary) via iceberg-rust native catalogs; AWS SDK
  credential chain (SigV4 automatic).
- **Tooling**: Ruff (lint+format, line 100), Rustfmt (`max_width=100`), Clippy `all`+`pedantic`
  `-D warnings`. `make ci` is the canonical gate; `make preflight`
  mirrors the full CI surface — run it before opening a PR. Tool versions are pinned identically in
  the Makefile and the workflows.

## Repo layout

- [README.md](README.md) — overview. [AGENTS.md](AGENTS.md) — authoritative agent contract.
- `crates/` — the Cargo workspace (arrives phase 1; empty members list today). Target skeleton in
  [AGENTS.md](AGENTS.md) "Target crate map".
- `python/` — the `repark` package (arrives phase 3).
- [docs/](docs/) — [testing.md](docs/testing.md), [port/PLAN.md](docs/port/PLAN.md),
  [adr/](docs/adr/), per-tier manuals in [docs/skills/](docs/skills/),
  [release.md](docs/release.md).
- [task/](task/) — `todo.md` + `lessons.md` + per-unit ledgers. [briefs/](briefs/) — versioned
  delegated-agent slate briefs (standing rules live in [AGENTS.md](AGENTS.md)
  "Delegated-agent standing rules"). [.github/](.github/) — CI + Dependabot. `scripts/` — hooks.
- Each directory carries a `map.md` (navigation + `## Debug`).

## Rust conventions

Rustfmt (`max_width=100`, `edition=2024`) + Clippy `all`+`pedantic`, `-D warnings`, `unsafe_code=forbid`
(except the future `repark-python`). `thiserror` for libs, `anyhow` for binaries; `tracing` for logs;
no panics in prod — no `unwrap`/`expect` (`with_context()?` / `.ok_or_else(…)?`). The panic/async-spawn
bans are mechanical (`clippy.toml` `disallowed-methods`); further structure gates (crate layering,
crate-root manifests, Python thinness) return with the code they gate in phase 1+ — see
[docs/port/PLAN.md](docs/port/PLAN.md). **House style:** section-function `///` doc blocks
banner-wrapped with `///` + space + **91** `=` characters (95 cols); ONE blank line between top-level
items; banners are hand-authored (rustfmt preserves but never generates them). `Cargo.lock` checked in.

## Python conventions

Type hints on every signature; Pydantic v2 for structured config (not dataclasses); `pathlib`;
`logging` not `print`; f-strings; never bare `except`; Ruff `line-length=100`. uv workspace members
(from phase 3) add a `pyproject.toml` + an entry in the root `[tool.uv.workspace] members`.

## Destructive / outward-facing operations

The engine will touch AWS (Glue, S3 Tables, S3). **Never drop/delete a Glue table, an S3 Tables table,
or S3 data, and never mutate IAM, without explicit user action** — if such an operation seems needed,
stop and ask. AWS writes go only through the engine's sanctioned catalog/write paths (the future
`repark-iceberg`). Commit or push only when the user asks.

<subagent_policy>

## Agent orchestration policy

Single agent by default — do the work in the main thread. Do **not** spawn sub-agents / Workflow
fan-out unless the user asks for it. When the user does ask, default delegated agents to **Sonnet** or
**Haiku** (pass the tier explicitly). **Opus sub-agents require a direct, explicit command from the
user naming Opus.** Relax this section by editing it and noting the change in
[task/lessons.md](task/lessons.md).

</subagent_policy>

## Working conventions

- **AGENTS.md and the approved plan are load-bearing.** When a decision changes, update them (and this
  file). Ask before silently reconciling a spec/code conflict.
- **Keep `map.md` in lockstep with code** — always in scope for any code change.
- **Verify before "done":** `make verify`; before opening a PR, `make preflight` (verify + the
  security/workflow gates CI also runs). Follow the per-tier manual in
  [docs/skills/](docs/skills/) for everything not covered here; CLAUDE.md wins on any conflict.

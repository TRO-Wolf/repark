# AGENTS.md — the authoritative contributor contract

This is the **single authoritative project contract** for repark, written for **any contributor —
human or automated agent**, naming no tool or model. It holds the precedence chain, the
architectural invariants, the change-location guide, required verification, and the safety
boundaries. When a rule changes, it changes **here**; other files point at this contract, they do
not restate it.

Tool-specific onboarding lives in clearly-labelled **adapter** files that carry no authoritative
facts (so they cannot drift): [CLAUDE.md](CLAUDE.md) and [.agents/](.agents/map.md). Deleting any
adapter loses no project knowledge.

## Read first

The read path before touching code — the same for a human and an agent:

1. [README.md](README.md) — what repark is, one screen.
2. [STATUS.md](STATUS.md) — current state (release, delivered crates, active/deferred work); the
   status source of truth.
3. [ARCHITECTURE.md](ARCHITECTURE.md) — component boundaries, the crate DAG, the three runtime
   flows.
4. [DEVELOPMENT.md](DEVELOPMENT.md) — local setup, the `make` targets, the CI surface,
   troubleshooting.
5. **This contract (AGENTS.md)** — the rules that govern any change.
6. [docs/testing.md](docs/testing.md) — the mandatory testing-discipline contract; read it before
   any code change.

Then the `map.md` of every directory your task will touch (see "`map.md` in every directory" below).

## Precedence

The authority chain on any conflict, highest first. **This section is the chain's single home** —
every other file ([CLAUDE.md](CLAUDE.md), [PROJECT.md](PROJECT.md),
[skills/sepmo/binding-manifest.md](skills/sepmo/binding-manifest.md)) points here, never restates
it.

> **[AGENTS.md](AGENTS.md)** (the authoritative contract) **>** [PROJECT.md](PROJECT.md) (north-star
> intent) **>** [STATUS.md](STATUS.md) (status SSOT) **>** engineering conventions
> ([DEVELOPMENT.md](DEVELOPMENT.md) + [docs/testing.md](docs/testing.md); the per-tier manuals in
> [docs/skills/](docs/skills/) are one model family's view of these same conventions) **>** SEPMO
> ([skills/sepmo/SKILL.md](skills/sepmo/SKILL.md) — lifecycle/orchestration only).

SEPMO governs *how work flows* (scope audit → Actor–Critic → PR → delivery → retrospective); it
never overrides an engineering rule. When SEPMO and this contract appear to conflict, the contract
wins and the conflict becomes a clarifying question (SEPMO doctrine D1).

## What repark is

A pure-Rust, no-JVM single-node data engine with first-class Apache Iceberg support — *"Trino's SQL,
DuckDB's deployment model, deepest Iceberg support."* Two doors: a **native** lazy DataFrame API +
ANSI/Trino-style `repark.sql()`, and a **near-drop-in PySpark facade** whose `.sql()` keeps the Spark
dialect. Built on Apache DataFusion + Arrow + our **owned `iceberg-rust` fork** (see the hard rules
below), with native PyO3 Python bindings. See [README.md](README.md).

**Current state lives in [STATUS.md](STATUS.md)** — the single source of truth for release state,
delivered crates, active workstreams, and deferred work. The port that stood this repository up
(copy-then-re-home, four phases) is recorded in [docs/port/PLAN.md](docs/port/PLAN.md). Do not
restate status here; point at STATUS.md.

## Crate map — where a change will go

The change-location guide: which home owns which kind of change. `Status: delivered` homes exist
today; `Status: deferred` homes do **not** exist yet and are extracted only when their code arrives
— do not create one ahead of its driver. The nine delivered crates (including `repark-common`,
`repark-functions`, `repark-ta`, which are not change-destination rows here) are inventoried in the
live workspace `Cargo.toml` (the authoritative list) and mirrored — path, layer and delivery status,
mechanically kept honest by `make check-manifest` — in [repo-manifest.toml](repo-manifest.toml); see
[STATUS.md](STATUS.md) for delivery state and [crates/map.md](crates/map.md) for navigation. The three
`crates/` `deferred` rows below are the manifest's `planned` components: the gate reds if anything
appears at their path while they are still declared planned.

| You will want to change… | Home | Status |
|---|---|---|
| Lazy-frame IR, planning, optimizer hooks, `Session` | `crates/repark-core` | delivered |
| Execution config, spill, out-of-core | `crates/repark-exec` | deferred — extracted when its code arrives |
| Inference readers (CSV, Excel, JSON) | `crates/repark-io` | deferred — extracted when its code arrives |
| Catalogs (Glue, S3 Tables) + Iceberg DML + maintenance; adapter over the owned fork | `crates/repark-iceberg` | delivered |
| Postgres / MSSQL connectivity | `crates/repark-connect` | deferred |
| ANSI SQL front end (native dialect) | `crates/repark-sql` | delivered |
| Spark semantics: function shims, Spark SQL dialect, the parity surface | `crates/repark-spark` | delivered |
| ML: native estimator kernels (Cholesky/OLS/IRLS/Lloyd) | `crates/repark-ml` | delivered |
| PyO3 bindings: thin adapter over the internal engine API | `crates/repark-python` | delivered |
| Native lazy API + `repark.sql()` (Python) | `python/repark` | delivered |
| The PySpark facade | `python/repark/spark` | delivered |
| The dbt adapter | `dbt-repark` (separate package) | deferred (parked lane) |

v1 crates re-home rather than rewrite (catalog + write → `repark-iceberg`; the Spark parts of the v1
functions/sql crates → `repark-spark`; the smart CSV reader → `repark-io`). DataFusion remains the
engine under everything.

*Correction (2026-08-06):* `repark-exec` and `repark-io` were originally listed as phase 1. The
settled phase-1 design ([docs/design/session-api.md](docs/design/session-api.md) §1) deliberately
does not create them — no v1 code exists for either (execution config is ~40 lines inside the
Session builder). Each is extracted later, when its code actually arrives.

## Verify before "done"

`make verify` — a change is not done until lint, format, clippy, **Rust** tests, and the touched
directories' `map.md` files are all current. `verify` is Rust-only on purpose (inner-loop speed);
it does **not** build the native module. See [docs/testing.md](docs/testing.md). Before opening a
PR, run `make preflight` — `verify` plus the facade suite (`make py-test-facade`, which carries
the live-mirror gate) plus the security/workflow gates CI also runs. `make ci` is the canonical
fast gate. Tool versions are pinned identically in the Makefile and the workflows, and CI-enforced
tools never silently skip locally (uvx provisions the pinned tool on demand).

## Hard rules (non-negotiable)

- **iceberg-rust is forked & owned** (`TRO-Wolf/iceberg-rust`, a sibling sub-project, 1:1 Java
  `iceberg-core` parity). RePark builds on the fork; the table-format engine (write actions,
  evolution, snapshots, views, maintenance) lives **in the fork**. The fork stays a **separate repo,
  never vendored**. `[patch.crates-io]` sources the `iceberg*` family from the owned fork, rev-pinned.
  The fork's `iceberg-datafusion` is a **supported product surface** RePark consumes
  (DELETE/UPDATE/INSERT via its `TableProvider`); MERGE stays RePark-owned. Fork capability status
  lives ONLY in the fork's `docs/parity/GAP_MATRIX.md` + `docs/ENGINE_CONTRACT.md` — link, never
  restate. DataFusion is a normal upstream dep (do not fork it). See
  [docs/adr/0001-own-iceberg-fork.md](docs/adr/0001-own-iceberg-fork.md).
- **Two honest SQL doors, no blended parser.** Native = ANSI/Trino-style; facade = Spark dialect;
  shared Iceberg machinery beneath both; new SQL surface lands with both spellings + one test row per
  door. See [docs/adr/0002-two-sql-doors.md](docs/adr/0002-two-sql-doors.md).
- **Server-prep disciplines:** everything-through-Session (no global mutable state, no env reads at
  query time) and bindings-as-thin-adapter (one internal engine API; PyO3 and a future Flight SQL
  handler are both thin adapters). See
  [docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md).
- **Tests in the same commit as code.** No "later". The **entry-point matrix** (native DataFrame /
  ANSI SQL / Spark facade as rows) is the central testing structure. A divergence-class claim
  ("Spark parity", "fixed #n") pins *every* class it names, per user entry point, on the Arrow path
  (`collect`/`to_arrow`, value AND type — never only `show`); one representative case is not the
  claim. Full contract: [docs/testing.md](docs/testing.md).
- **`map.md` in every directory, updated in the same change.** Enforced by
  `scripts/check_map_md.sh` (pre-commit). New directory → new `map.md`, no judgment call. Maps
  are **hand-written**: there is no generator, and the one piece of `map.md` automation
  (`check_manifest.py`'s crate-root consistency rule) only *checks* that a map exists and agrees
  with `repo-manifest.toml` — it never writes, scaffolds, or rewrites one.
- **Rust house style:** 91-`=` banner doc blocks on section fns; one blank line between top-level
  items; `max_width=100`, `edition=2024`; clippy `all`+`pedantic`, `-D warnings`; `thiserror`
  (libs) / `anyhow` (bins); `tracing`; no panics in prod — no `unwrap`/`expect`
  (`with_context()?` / `.ok_or_else(…)?`).
- **Mechanical structure gates** — enforced, not conventions; each has a script/list SSOT that prose
  must point at, never restate:
  - *Panic + async bans*: `clippy.toml` `disallowed-methods` (unwrap/expect +
    `tokio::spawn`/`spawn_blocking`). Escape = per-call-site
    `#[expect(clippy::disallowed_methods, reason = …)]` stating the lifecycle; never a
    file/crate-wide allow. One recorded module-scoped `#![expect]` exists — the binding's
    exception-taxonomy module (`crates/repark-python/src/lib.rs`), because a per-call-site
    `#[expect]` cannot reach inside `pyo3::create_exception!`'s macro expansion (proven both ways —
    [docs/history/port-v2/p3c-binding-ledger.md](docs/history/port-v2/p3c-binding-ledger.md)
    P-4/P-5); the lint stays live for the rest of that crate.
  - *Crate dependency policy* (`scripts/check_crate_dag.py` — the SSOT for the tier map, the
    crate roles, and the explicit allowed-edge table: every internal edge with the dependency
    kinds it may take and why it exists; an undeclared edge, a promoted kind, or a forbidden
    shape — door↔door, anything→bindings, foundation→internal, capability→door — is red, and the
    rules re-apply to the declaration itself so writing a forbidden edge down cannot legalize it)
    and *crate-root manifests*
    (`scripts/check_lib_rs.py` — ceilings + EXCEPTIONS SSOT) are **armed** since phase-1 PR-A,
    dual-wired Makefile + ci.yml.
  - *Facade `.py` thinness* (`scripts/check_lib_py.py` — the Python sibling of `check_lib_rs` and
    the SSOT for facade file size: per-file line ceilings with an EXCEPTIONS-with-reason table that
    **ratchets DOWN only**, plus the no-stub rule — a re-export-only module must open its docstring
    with the exact substring `re-export binding`) is **armed** since phase-3 PR-5 over
    `python/repark/src/repark/**`, dual-wired `make check-lib-py` (in the `make ci` chain) +
    ci.yml's `python` job, and in both pre-commit paths (`make install-hooks` +
    `.pre-commit-config.yaml`).
  - *Rust file-size* (`scripts/check_rust_file_size.py` — the general companion of `check_lib_rs`
    and the SSOT for per-file `crates/**/*.rs` line ceilings: a default ceiling with an
    EXCEPTIONS-with-reason table that **ratchets DOWN only**) is **armed** since G-8, dual-wired
    `make check-rust-file-size` (in the `make ci` chain) + the ci.yml `guards` job, and in both
    pre-commit paths (`make install-hooks` + `.pre-commit-config.yaml`). Prose points at the
    script; ceilings are never restated here.
  - *Python conventions* (`scripts/check_python_conventions.py` — the SSOT for the two Python
    rules Ruff cannot express: the **nested-`def` ban**, with an inline `# nested-def: <reason>`
    pragma for the three sanctioned cases and a per-file ceiling table that **ratchets DOWN only**,
    and the **`dataclasses`/`attrs` ban**, with an exceptions table and no inline pragma on
    purpose) is **armed** since PYC over `python/repark/src`, `python/repark-parity` and
    `scripts/`, dual-wired `make check-python-conventions` (in the `make ci` chain) + ci.yml's
    `python` job. **Not** on the pre-commit hook as of PYC-5: n=5 median 0.996 s (max 1.011 s)
    over 164 files, at the sub-second budget line, with the max already over it. The other two
    Python rules are held elsewhere and are deliberately not duplicated: type coverage is
    Ruff's `ANN` rule set, naming is a review
    duty. Prose points at the script; the tables are never restated here. Rationale and the
    method for arming a convention: [.agents/skills/code-quality/SKILL.md](.agents/skills/code-quality/SKILL.md).
  - *Structural truth* (`repo-manifest.toml` + `scripts/check_manifest.py`): the component
    inventory, phase, canonical gate commands and documentation index are machine-readable and
    validated against the workspace, the Makefile, STATUS.md, the declared documents and the
    crate-root `map.md` files. Adding a Cargo member without declaring it, letting a declared
    document or `make` target rot, or moving the milestone in STATUS.md alone is a red gate. The
    manifest is a MIRROR — its `layer` values are checked against the crate-DAG SSOT, never the
    other way round — and its `map.md` rule **checks** hand-written maps; nothing generates one.
  - *parity-live dual-wire* (`scripts/check_parity_live_dual_wire.py` — the SSOT): `make parity-live`
    and `.github/workflows/parity-live.yml` are compared to **each other** on their load-bearing
    tokens (`uv sync` flag/extra set, `--no-install-package`, maturin pin + `develop`,
    `uv run --locked --no-sync` + pytest path, `REPARK_PARITY_LIVE` / `SPARK_LOCAL_IP` env pins).
    There is no third hand-maintained expected-flags list. Fail-closed on a parse miss. Dual-wired
    `make check-parity-live-dual-wire` (in the `make ci` chain) + the ci.yml `guards` job.
  - The few v1 helper scripts not yet re-homed are listed in [scripts/map.md](scripts/map.md)
    "Not re-homed"; each **returns only with a concrete driver** (named per script there). Do not
    re-invent one ahead of its driver; re-home v1's script.
- **Rust module layout is the default one** — `mod foo;` resolved by `foo.rs`, `foo/mod.rs`, or
  `foo/*.rs`. `#[path = "…"]` is not a module-inclusion mechanism here: move the file into the
  canonical tree instead. A generated-code, FFI, or test-fixture case that genuinely cannot sit in
  the tree keeps the attribute local to that one item and states in a comment why the canonical
  layout cannot work.
- **`unsafe_code = "forbid"` everywhere except `crates/repark-python`** (landed phase-3 PR-3), which sets a
  local `unsafe_code = "allow"` because PyO3 macros expand to `unsafe`. Do not add `unsafe` elsewhere.
- **Python:** type hints on every parameter, every return and every public attribute; Pydantic v2
  `BaseModel` for all structured data, never `dataclasses` or `attrs`; define functions at module
  or class level rather than nested inside another function; name a function for the work it does,
  as a verb phrase; `pathlib`; `logging`; f-strings; never bare `except`; Ruff `line-length=100`.
- **Spell things out** — no casual abbreviations (`config` not `cfg`, `index` not `idx`).
- **Tier-2 CI (live AWS) never runs against unmerged code**; nightly on `main` + manual dispatch via
  OIDC only. No self-hosted runners in any tier. No secrets in tier-1 workflows.

## Change discipline

How a change is shaped, independent of where it lands.

- **Fixes stay narrow.** Implement the requested behaviour and stop. A semantic-adjacent rewrite is
  a separate change with its own review — never a passenger on a fix, and never at all while
  touching a sensitive path (the write/commit path, the catalog, the exception taxonomy, anything
  under "Safety" below).
- **Do not refactor existing code only to make it easier to unit test.** Test the code as it
  stands, or argue the refactor on its own merits as its own change.
- **The smallest readable design wins.** Reach for an existing abstraction before adding one.
  Parallel managers, factories, adapters, and wrappers introduced to make a design *look*
  extensible are defects; extensibility is earned by a second real caller, not anticipated.
- **Comments carry the non-obvious reason, the assumption, or the invariant** — in the shortest
  complete form. Length follows the invariant: `SAFETY`, lock ordering, durability, and
  compatibility contracts may need a short list of conditions. A comment never narrates the next
  line, restates a signature, or records change history. Durable design rationale goes to
  [ARCHITECTURE.md](ARCHITECTURE.md), a `map.md`, or [docs/adr/](docs/adr/map.md) — not inline.

## Markdown document lifecycle

Every markdown document here belongs to **exactly one class**, and the class decides how it is
amended, what retires it, and where its record goes afterwards. Classification is not decoration:
it is what keeps a live document from silently accumulating a closed campaign's detail.

| Class | Members | Lifecycle |
|---|---|---|
| **contract** | [AGENTS.md](AGENTS.md), [docs/testing.md](docs/testing.md), [PROJECT.md](PROJECT.md) | permanent; amended deliberately, never as a passenger on another change |
| **state** | [STATUS.md](STATUS.md), [ARCHITECTURE.md](ARCHITECTURE.md), [DEVELOPMENT.md](DEVELOPMENT.md) | trued up at every unit close **and** at pickup; git is their history, so they carry no changelog section |
| **navigation** | every `map.md` | lockstep with the directory's content, in the same commit |
| **campaign** | [briefs/](briefs/map.md), [docs/design/](docs/design/map.md) | amended in place, dated; frozen and archived to [docs/history/](docs/history/map.md) when the campaign closes |
| **ledger** | `task/<unit>-ledger.md` | append-only while the unit runs; frozen at merge; archived with its campaign |
| **skill** | [.agents/skills/](.agents/skills/map.md) | versioned with the procedure it records; a rule measured and **declined** is written down so nobody re-litigates it |

The rules that bind all six:

- **A document names the event that retires it at birth.** "This file closes when X merges" belongs
  in its first commit, not in a later discovery. If nothing can retire a document, it is a contract
  or a state document — or it should not have been created.
- **Truth moves, it is never deleted.** Compaction is archival, not removal: a closed campaign's
  record goes to `docs/history/`. The only deletable documents are working notes that produced no
  decision.
- **A claim that can go stale carries its date.** Measurements, counts, timings, "not yet", "planned",
  and phase words are dated where they are written, so a reader can tell rot from truth.
- **An archived document is corrected only by a dated errata note at its top**, never rewritten. The
  archive's value is that it says what was believed at the time.
- **Every fact is single-homed**; every other mention is a pointer (the rule in
  [`## Precedence`](#precedence) applied to documents).

The **executor** is the [compact-context-docs](.agents/skills/compact-context-docs/SKILL.md) skill —
the pickup ritual at the start of a unit and the truth-up after one lands. This section states the
classes and the rules; the skill states the procedure and is not restated here. The navigation class
has a mechanical half: `make check-map-sync` (`scripts/sync_map_md.py` — the SSOT) fails a map whose
relative links no longer resolve, with the coverage rule available behind `--strict`.

## Working style and communication

- **Stop gathering once you can act.** Redundant file reads, repeated commands, and exploratory
  work past the point of sufficient context are waste — and in a delegated unit they are the main
  way a context budget is lost.
- **Answer in the language the requester used.** Source code, comments, identifiers, commit
  messages, and PR titles and bodies stay **English** regardless.
- **Be concise.** No sycophantic openers, no closing filler, no narrated status. Plain words over
  ceremony: say what changed, what it cost, and what is still open.

## PyO3 build notes

The bindings crate (`crates/repark-python`) is delivered; these rules are recorded here so they are
never re-litigated.

- The cdylib's `extension-module` feature is **off by default** so `cargo test`/`check` build
  without needing it; it is enabled only when maturin builds the wheel.
- Therefore the test command is **`cargo test --workspace`**, never the flag that enables every
  feature — that would turn on `extension-module`, which tells PyO3 not to link libpython and breaks
  a standalone test binary. CI, the Makefile, and this rule all agree.
- PyO3's build script needs an interpreter; linking `cargo test` needs `libpython3.x.so` present.

## Version-pin contract

Pin **one** DataFusion version across `datafusion` + `datafusion-spark` + `iceberg*`, recorded in the
workspace `Cargo.toml`. **The live `Cargo.toml` is the SSOT for every
pin** — prose tables are orientation only; verify against `Cargo.toml` before ANY family bump. The
`iceberg*` family comes from the **owned fork**, so a "bump" is re-pinning the `[patch.crates-io]`
rev to a newer fork commit, together with `datafusion` + `datafusion-spark` + `arrow*`/`parquet` +
`rust-toolchain.toml`, re-resolving with `cargo update`. `Cargo.lock` is checked in. Upstream family
bumps (e.g. a Dependabot DataFusion major) are **skipped** until the fork moves its base — record a
dated take/skip decision per release. Never merge a Dependabot PR that bundles a safe bump with a
DataFusion/arrow family major — split it.

**Every fork repin also re-verifies what we built on top of the old rev** — a fork fix silently
makes a local workaround dead code, and a fork trait gaining a defaulted method silently reopens a
swallowed-override bug:

- Re-check the **metadata-projection shim**'s removal criterion: it goes only when a fork rev's
  metadata-table `scan` honors `projection`, including the empty-projection case.
- **Re-enumerate the wrapped catalog's trait surface** — a `Catalog` method that newly falls to a
  trait default is a new latent gap, not a no-op (the both-sides trait-wrapping audit).
- Both duties, and the defects that motivate them, are recorded against the crate that owns the
  code: [crates/repark-iceberg/map.md](crates/repark-iceberg/map.md) "Known limitations".

## Explicitly out of scope (do not reintroduce without a decision)

- **PyIceberg** — neither the Python lib nor the `pyiceberg_core` crate. Iceberg = `iceberg-rust` only.
- **Sail / pysail** — own-the-stack was chosen; no dependency on it.
- **Distributed cluster** — the `ExecutionBackend` seam marks the boundary, with its honest scope
  documented (see [ARCHITECTURE.md](ARCHITECTURE.md) "what the seam is, honestly"); posture is
  fleet-parallel → server mode → distributed only if a query outgrows one box. Do not build
  Ballista-for-writes (it cannot serialize Iceberg write/commit plan nodes).
- **External code PRs** — not accepted while the engine is pre-alpha; see
  [CONTRIBUTING.md](CONTRIBUTING.md).

## Upstream contribution policy

The table-format engine is built in the **owned `TRO-Wolf/iceberg-rust` fork**. Upstream-mergeability
with `apache/iceberg-rust` is **not a constraint** — upstreaming a primitive is
**optional/opportunistic**, not an obligation. We still **cherry-pick upstream improvements** into
the fork when useful.

## Resource discipline — disk and artifacts

Builds, test runs, coverage, and per-unit worktrees are the largest consumers of disk here, and a
disk that fills halts a campaign mid-unit.

- **Check free space before you spend it** — before creating a worktree, and before any dependency
  download, build, test run, coverage run, or other artifact-heavy command.
- **Re-check at phase boundaries** on long-running or artifact-heavy work, and again before broad
  validation. If the remaining space may not safely carry the next command, stop and reclaim
  task-owned artifacts before continuing — never start the command hoping it fits.
- **Cleanup is scoped.** Remove the generated build, test, and coverage artifacts and temporary
  files *this* task created, once they are no longer needed. **Never delete another task's
  worktree, and never delete uncommitted files** — a dirty tree that is not yours is someone
  else's unit in flight, not garbage.
- **Share caches instead of duplicating them.** Where a tool supports a shared dependency or build
  cache, point worktrees at it rather than giving each one its own copy of the same large artifact.
- **Report it at handoff:** the disk checks you ran, the cleanup you performed, and any worktree or
  artifact you deliberately kept, with the reason it is still needed.

## Safety — destructive / outward-facing operations

The engine touches AWS (Glue, S3 Tables, S3). **Never drop or delete a Glue table, an S3 Tables
table, or S3 data, and never mutate IAM, without explicit user action** — if such an operation
seems needed, stop and ask. AWS writes go only through the engine's sanctioned catalog/write paths
(`repark-iceberg`). **Commit or push only when the user asks.** These approval boundaries bind every
contributor and every delegated work unit; a delegated unit may narrow them, never relax them.

## Delegated-agent standing rules

The rules every delegated work unit (slates, sub-agent briefs) inherits — briefs in
[briefs/](briefs/) reference this section instead of restating it; a brief may narrow these,
never relax them.

- **The Rust rule:** Python builds plans; Python never touches rows. Engine-missing
  functions get Rust shims or LOUD unsupported errors — never Python compute. (Deliberate
  exception: user-supplied UDF code, engine-driven over Arrow batches.)
- **Workspace validity:** commits MUST pass the installed hooks; verify hooks fire before
  the first commit in any worktree. A hook bypass or non-firing-hook workspace is a
  slate-failing violation.
- **Gates:** every unit gates (`make verify`), including STOP / report-only units; check REAL
  exit codes (never a pipe's); lint only via the Makefile's pinned toolchain targets. Before a
  PR: `make preflight` (verify + `py-test-facade` + audit + workflow lint).
- **Ledgers:** one `task/<unit>-ledger.md` per unit, linked from `task/map.md` in the same
  commit. Ledger presence is a gate item.
- **Oracles:** oracle/differential test files are NAMED deliverables per unit; live-oracle
  output recorded verbatim; hand-computed expectations are not an oracle. Divergences get
  honest `_divergence` pins, never silent absorption.
- **Mechanical gates bite every commit** (see "Hard rules"): a red gate is never worked
  around — the sanctioned outs (per-site `#[expect]` with reason / SSOT-table edit with reason)
  are visible diffs, reviewed like code.
- **Test relocations follow docs/testing.md "Relocation discipline":** move-only = identity-diff
  gate (`--list` / `--collect-only` empty); path-changing regroups are declared-rename units that
  ship alone with an explicit name map.
- **Disk:** the pre-spend checks, the scoped cleanup, and the handoff report in
  ["Resource discipline"](#resource-discipline--disk-and-artifacts) bind every unit.
- **Never:** AWS credentials/envs, `Cargo.toml [patch]` changes, `.github/` changes,
  secrets in any output. Clean STOP states only — a dirty worktree is not a delivered unit.

## Delegated work

Single-agent-in-the-main-thread is the default. The orchestrating agent owns architecture and
assembly; delegated fan-out is for **search, mechanical edits, and narrow, well-scoped
implementation**, never for architectural judgement. Every delegated unit inherits the standing
rules above and the approval boundaries in "Destructive / outward-facing operations" — a delegated
unit may narrow those, never relax them. Any capability-tier choices for delegated agents (which
model does what, when a stronger tier needs an explicit request) are **tool mechanics**, recorded in
the relevant tool adapter ([CLAUDE.md](CLAUDE.md) / [.agents/](.agents/map.md)), not here.

## Process governance (SEPMO)

Lifecycle/orchestration for non-trivial work runs under the **SEPMO control plane**
([skills/sepmo/SKILL.md](skills/sepmo/SKILL.md)): scope audit (proposition-ledger gate: every clause
`PROVEN`, zero `OPEN`/`REJECTED`) → adversarial Actor–Critic →
per-PR delivery → retrospective. SEPMO governs **only how work flows** and **cedes every
engineering decision to this contract** — on any conflict the precedence chain in
[`## Precedence`](#precedence) above wins. SEPMO's abstract roles bind to this repo through
[skills/sepmo/binding-manifest.md](skills/sepmo/binding-manifest.md). Its Actor–Critic runs
**single-session by default** (sequential phases); sub-agent fan-out follows "Delegated work" above.

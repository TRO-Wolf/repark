# AGENTS.md — the authoritative contributor contract

# OWNER RULING (2026-08-26) — NO CODE COMMENTS FROM ANTHROPIC MODELS

**EFFECTIVE IMMEDIATELY. ALL ANTHROPIC MODELS ARE HEREBY BANNED FROM MAKING COMMENTS IN THE CODE. THIS INCLUDES, FABLE, OPUS, SONNET AND HAIKU**

**AGAIN, IF YOU ARE FABLE, OPUS, SONNET OR HAIKU, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE FABLE, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE OPUS, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE SONNET, DO NOT WRITE ANY COMMENTS**<br>
**IF YOU ARE HAIKU, DO NOT WRITE ANY COMMENTS**<br>

**THIS INCLUDES ANY MODEL VERSION, EXAMPLE BEING OPUS 4.8 OR OPUS 5, EITHER ONE IS BANNED, IT DOESN'T MATTER**

*Adjustment (owner, 2026-08-26, same day):* the ban is on comments **in code** — Rust, Python, shell,
TOML, YAML and every other source file. **Markdown files may carry comments and explanatory prose**;
that is where a reason, a design note or a `pins: <unit>/C-NNN` citation now lives — the
directory's `map.md` (the ledger-grammar gate reads every tracked file under `crates/`,
`python/`, `scripts/`, so a citation in a `map.md` there counts). Condensation is held by review,
not by a gate: the `check-comment-density` ratchet was dropped before #247 merged; the file-size
ratchets are the mechanical gates, and a new file carries no comments.

Authorship is undetectable; review holds this rule. The gate preserves bytes. Required
docstrings, Rust banners, and invariant comments remain. No sweep.

This is the **single authoritative project contract** for repark, written for **any contributor —
human or automated agent**, naming no tool or model: the precedence chain, the architectural
invariants, the change-location guide, required verification, and the safety boundaries. When a
rule changes, it changes **here**; other files point at this contract, they do not restate it.

Tool-specific onboarding lives in clearly-labelled **adapter** files that carry no authoritative
facts (so they cannot drift): [CLAUDE.md](CLAUDE.md) and [.agents/](.agents/map.md). Deleting any
adapter loses no project knowledge.

## Read first

Load only what your role needs. Each row is exhaustive for its role: what it does not name, you
do not read. Adapters point here and add no second roster.

| Role | Load, in order | Explicitly NOT required |
|---|---|---|
| Orchestrator / governing session | [README.md](README.md) → [STATUS.md](STATUS.md) → this contract → [ARCHITECTURE.md](ARCHITECTURE.md) → the SEPMO `## Procedure` section → [task/lessons.md](task/lessons.md) in full | [DEVELOPMENT.md](DEVELOPMENT.md), [docs/testing.md](docs/testing.md), skill `## Reference` sections, critic-critic-critic |
| Executor (a unit or work order) | This contract's "Hard rules" + "Verify before done" → the work order → the `map.md` of every directory touched → [docs/testing.md](docs/testing.md) → the engineering-method `## Procedure` section → the lessons entries for subsystems you touch (`grep -n <subsystem> task/lessons.md`) | [STATUS.md](STATUS.md) beyond the order's base, [PROJECT.md](PROJECT.md), [ARCHITECTURE.md](ARCHITECTURE.md), [DEVELOPMENT.md](DEVELOPMENT.md) unless setup blocks you, SEPMO, critic-critic-critic, lessons in full |
| Critic / reviewer | This contract's "Hard rules" → the critic-critic-critic `## Procedure` section → the evidence packet | Everything else |
| Lookup / status question | [STATUS.md](STATUS.md) only | Everything else |

## Precedence

The authority chain on any conflict, highest first. **This section is the chain's single home** —
[CLAUDE.md](CLAUDE.md), [PROJECT.md](PROJECT.md), and
[.agents/skills/sepmo/binding-manifest.md](.agents/skills/sepmo/binding-manifest.md) point here, never restate it.

> **[AGENTS.md](AGENTS.md)** (the authoritative contract) **>** [PROJECT.md](PROJECT.md) (north-star
> intent) **>** [STATUS.md](STATUS.md) (status SSOT) **>** engineering conventions
> ([DEVELOPMENT.md](DEVELOPMENT.md) + [docs/testing.md](docs/testing.md) + the portable working
> method in [.agents/skills/engineering-method/SKILL.md](.agents/skills/engineering-method/SKILL.md))
> **>** SEPMO ([.agents/skills/sepmo/SKILL.md](.agents/skills/sepmo/SKILL.md) — lifecycle/orchestration only).

SEPMO governs *how work flows* (scope audit → Actor–Critic → PR → delivery → retrospective); it
never overrides an engineering rule. When SEPMO and this contract appear to conflict, the contract
wins and the conflict becomes a clarifying question (SEPMO doctrine D1).

## What repark is

A pure-Rust, no-JVM single-node data engine with first-class Apache Iceberg support — two SQL
doors (native ANSI/Trino-style `repark.sql()`, Spark-dialect facade), DataFusion + Arrow, and an
**owned `iceberg-rust` fork**. Product intent: [README.md](README.md) / [PROJECT.md](PROJECT.md).
Current state: [STATUS.md](STATUS.md) — do not restate it here.

## Crate map — where a change will go

Which home owns which kind of change. `deferred` homes do **not** exist yet: they are the
`planned` rows of [repo-manifest.toml](repo-manifest.toml) (`make check-manifest` reds if anything
appears at their path) with tiers and edges pre-declared in `scripts/check_crate_dag.py`; never
create one ahead of its first unit. Delivery state:
[STATUS.md](STATUS.md); navigation: [crates/map.md](crates/map.md).

| You will want to change… | Home | Status |
|---|---|---|
| Lazy-frame IR, planning, optimizer hooks, `Session` | `crates/repark-core` | delivered |
| Inference readers (CSV, Excel, JSON) | `crates/repark-io` | deferred — extracted when its code arrives |
| Catalogs (Glue, S3 Tables) + Iceberg DML + maintenance; adapter over the owned fork | `crates/repark-iceberg` | delivered |
| Postgres / MSSQL connectivity | `crates/repark-connect` | deferred — 1.6 |
| Native snapshot + CDC capture into Bronze | `crates/repark-cdc` | deferred — 1.7 |
| Discovery runs, proposed specs | `crates/repark-crawler` | deferred — 1.7 |
| The Spark SQL grammar | `crates/repark-spark-dialect` | deferred — tidy window |
| ANSI SQL front end (native dialect) | `crates/repark-sql` | delivered |
| Spark semantics: function shims, Spark SQL dialect, the parity surface | `crates/repark-spark` | delivered |
| ML: native estimator kernels (Cholesky/OLS/IRLS/Lloyd) | `crates/repark-ml` | delivered |
| PyO3 bindings: thin adapter over the internal engine API | `crates/repark-python` | delivered |
| Native lazy API + `repark.sql()` (Python) | `python/repark` | delivered |
| The PySpark facade | `python/repark/spark` | delivered |
| The dbt adapter | `python/dbt-repark` | delivered (DBT-1) |

DataFusion remains the engine under everything. Layout of record:
[task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md](task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md).

## Verify before "done"

`make verify` — a change is not done until lint, format, clippy, **Rust** tests, and the touched
directories' `map.md` files are all current. `verify` is Rust-only on purpose (inner-loop speed);
it does **not** build the native module. See [docs/testing.md](docs/testing.md). Before opening a
PR, run `make preflight` — `verify` plus the facade suite (`make py-test-facade`, which carries
the live-mirror gate) plus the security/workflow gates CI also runs. `make ci` is the canonical
fast gate. Tool versions are pinned identically in the Makefile and the workflows, and CI-enforced
tools never silently skip locally (uvx provisions the pinned tool on demand).

## Hard rules (non-negotiable)

- **iceberg-rust is forked & owned** (`TRO-Wolf/iceberg-rust`, 1:1 Java `iceberg-core` parity);
  the table-format engine lives **in the fork**, which stays a **separate repo, never vendored**
  (`[patch.crates-io]`, rev-pinned). The fork's `iceberg-datafusion` is a **supported product
  surface** (DELETE/UPDATE/INSERT via its `TableProvider`); MERGE stays RePark-owned. Fork
  capability status lives ONLY in the fork's `docs/parity/GAP_MATRIX.md` + `docs/ENGINE_CONTRACT.md`
  — link, never restate. DataFusion is a normal upstream dep (do not fork it). See
  [docs/adr/0001-own-iceberg-fork.md](docs/adr/0001-own-iceberg-fork.md).
- **Two honest SQL doors, no blended parser.** Native = ANSI/Trino-style; facade = Spark dialect;
  shared Iceberg machinery beneath both; new SQL surface lands with both spellings + one test row per
  door. See [docs/adr/0002-two-sql-doors.md](docs/adr/0002-two-sql-doors.md).
- **Server-prep disciplines:** everything-through-Session (no global mutable state, no env reads at
  query time) and bindings-as-thin-adapter (one internal engine API; PyO3 and a future Flight SQL
  handler are both thin adapters). See
  [docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md).
- **Tests in the same commit as code.** No "later". The **entry-point matrix** (native DataFrame /
  ANSI SQL / Spark facade) is the testing structure. A divergence-class claim pins *every* class it
  names, per entry point, on the Arrow path (`collect`/`to_arrow`, value AND type — never only
  `show`). Full contract: [docs/testing.md](docs/testing.md).
- **`map.md` in every directory, updated in the same change** (`scripts/check_map_md.sh`; the hook
  warns). New directory → new `map.md`, no judgment call. Maps are **hand-written**: no generator
  exists, and the one piece of `map.md` automation only *checks* — it never writes one.
- **Rust house style:** one-line comments; `// === name ===` stay; one blank line between top-level
  items; `max_width=100`, `edition=2024`; clippy `all`+`pedantic`, `-D warnings`; `thiserror`
  (libs) / `anyhow` (bins); `tracing`; no panics in prod — no `unwrap`/`expect`
  (`with_context()?` / `.ok_or_else(…)?`).
- **Mechanical structure gates** — enforced, not conventions; each has a script/list SSOT that
  prose must point at, never restate. Dual-wired `make ci` + ci.yml unless a row says otherwise.
  - *Panic + async bans*: `clippy.toml` `disallowed-methods`. Escape = per-call-site
    `#[expect(clippy::disallowed_methods, reason = …)]` stating the lifecycle; never a
    file/crate-wide allow. One
    recorded module-scoped `#![expect]` exists — the binding's exception-taxonomy module
    (a per-call-site `#[expect]` cannot reach inside `pyo3::create_exception!`;
    [proof](docs/history/port-v2/p3c-binding-ledger.md) P-4/P-5).
  - *Crate dependency policy* (`scripts/check_crate_dag.py`) — SSOT for the tier map, crate
    roles, and allowed-edge table. An undeclared edge, a promoted kind, or a forbidden shape is
    red; writing the edge down cannot legalize it. *Crate-root manifests*
    (`scripts/check_lib_rs.py`) — ceilings + EXCEPTIONS.
  - *Python source file-size + facade thinness* (`scripts/check_lib_py.py`) — exact-baseline
    ceilings cover Python source under `python/` and `scripts/`; the facade-only no-stub rule
    requires a re-export-only module to open its docstring with `re-export binding`.
  - *Rust file-size* (`scripts/check_rust_file_size.py`) — default ceiling + EXCEPTIONS, ratchet
    DOWN only. Both source-size gates fail on growth and on an unrecorded shrink so exception
    baselines remain exact. Ceilings are never restated here.
  - *Python conventions* (`scripts/check_python_conventions.py`) — nested-`def` ban and
    `dataclasses`/`attrs` ban. **Not** on the pre-commit hook as of PYC-5 (measured over the
    sub-second budget). Type coverage is Ruff `ANN`; naming is review. Rationale:
    [.agents/skills/code-quality/SKILL.md](.agents/skills/code-quality/SKILL.md).
  - *Public-docstring presence* (`scripts/check_docstring_presence.py`) — Ruff
    `D101`/`D102`/`D103`/`D105`/`D107`; style `D` declined (facade mirrors PySpark).
  - *Structural truth* (`repo-manifest.toml` + `scripts/check_manifest.py`) — a MIRROR of the
    crate-DAG SSOT; it checks hand-written maps and never writes one.
  - *parity-live dual-wire* (`scripts/check_parity_live_dual_wire.py`) — the `make` target and
    the workflow compared on load-bearing tokens; fail-closed on a parse miss.
  - *`map.md` content* (`scripts/sync_map_md.py`) — link validity armed (`make check-map-sync`);
    coverage behind `--strict`. Policy: ["Markdown document lifecycle"](#markdown-document-lifecycle).
  - v1 helper scripts not yet re-homed: [scripts/map.md](scripts/map.md) "Not re-homed". Each
    returns only with a concrete driver named there.
- **Rust module layout is the default one** — `mod foo;` resolved by `foo.rs`, `foo/mod.rs`, or
  `foo/*.rs`. `#[path = "…"]` is not module inclusion: move the file into the canonical tree.
  A case that genuinely cannot sit in the tree keeps the attribute local to that one item and
  states why in a comment.
- **`unsafe_code = "forbid"` everywhere except `crates/repark-python`**, which sets a local
  `unsafe_code = "allow"` because PyO3 macros expand to `unsafe`. Do not add `unsafe` elsewhere.
- **Python:** hints on every parameter, return, and public attribute; Pydantic v2 `BaseModel`
  for structured data, never `dataclasses`/`attrs`; module- or class-level `def`s, never nested;
  verb-phrase names; a docstring on every public function, class, method, `__init__`, and magic
  method (presence held by `scripts/check_docstring_presence.py`; style `D` declined); `pathlib`;
  `logging`; f-strings; never bare
  `except`; Ruff `line-length=100`.
- **Spell things out** — no casual abbreviations (`config` not `cfg`, `index` not `idx`).
- **Tier-2 CI (live AWS) never runs against unmerged code**; nightly on `main` + manual dispatch via
  OIDC only. No self-hosted runners in any tier. No secrets in tier-1 workflows.

## Change discipline

How a change is shaped, independent of where it lands.

- **Fixes stay narrow.** Implement the requested behaviour and stop. A semantic-adjacent rewrite is
  a separate change — never a passenger on a fix, and never at all on a sensitive path (the
  write/commit path, the catalog, the exception taxonomy, anything under "Safety" below).
- **Do not refactor only to ease unit testing.** Test the code as it stands, or argue the
  refactor on its own merits as its own change.
- **The smallest readable design wins.** Reach for an existing abstraction before adding one.
  Speculative managers, factories, adapters, and wrappers are defects; extensibility is earned by
  a second real caller, not anticipated.
- **Comments carry the non-obvious reason, the assumption, or the invariant** — full rule, table,
  and docstring contract:
  [Write for the eventual reader](#write-for-the-eventual-reader--comments-docstrings-and-prose).

## Write for the eventual reader — comments, docstrings, and prose

Owner-stated 2026-08-23. Write every paragraph, comment, doc, PR description, and issue for
its **eventual reader**, not the current agent conversation. Work out the reader's needs
privately; do not add an audience-analysis section to the artifact itself.

**Do not over-comment.** Three rules, applied to every comment and docstring:

1. **Comment the WHY, never the WHAT.** A clear name plus type hints documents the WHAT. What
   earns a comment: a race you prevent, an ordering invariant, a cross-cutting contract, a
   deliberate loud failure, defensive code that looks dead but is not, and the reason you did
   not do the obvious thing.
2. **Use the shortest form that carries the reason.** Keep the constraint and the failure mode.
   Safety, lock-ordering, durability, and compatibility contracts may need a short condition
   list. A comment never narrates the next line, restates a signature, or records history.
   Durable rationale goes to [ARCHITECTURE.md](ARCHITECTURE.md), a `map.md`, or
   [docs/adr/](docs/adr/map.md) — not inline.
3. **Write comments and docstrings in ASD-STE100 Simplified Technical English.**

   | Do | Not |
   |---|---|
   | One idea per sentence. Max ~20 words. | Multi-clause sentences. |
   | Active voice: "the writer commits the batch". | Passive: "the batch is committed". |
   | Present tense: "the retry fails". | "the retry would have failed". |
   | One word, one meaning. | Rotating synonyms for one thing. |
   | Plain verbs: "use", "read", "fail", "retry". | "leverage", "utilize", "surface", "orchestrate". |
   | Say the thing. | Hedging, apology, or self-narration. |

   Good: "Two writers can interleave here. The write is idempotent, so a retry is safe."

When in doubt: delete what a competent reader derives from the code; keep what they cannot;
then cut the keeper by half and check it still says the same thing.

**Docstrings.** Every function has one: what it does, its inputs, its outputs. Non-trivial
functions use Google-style sections (`Args:` / `Returns:` / `Raises:` / `Notes:` — `Notes:` is
for caller-needed invariants that fit none of the other three). Exception: facade docstrings
mirror PySpark's where PySpark has one.

**The 91-`=` banner keeps its form; its body obeys rules 1–2.** Consolidating existing long
comments is chartered sweep work, never a passenger on a fix.

**Held by:** review — except docstring **presence**, which
`scripts/check_docstring_presence.py` holds. Style `D` declined (facade mirrors PySpark).

## Markdown document lifecycle

Every markdown document here belongs to **exactly one class**, and the class decides how it is
amended, what retires it, and where its record goes afterwards.

| Class | Members | Lifecycle |
|---|---|---|
| **contract** | [AGENTS.md](AGENTS.md), [docs/testing.md](docs/testing.md), [PROJECT.md](PROJECT.md) | permanent; amended deliberately, never as a passenger |
| **state** | [STATUS.md](STATUS.md), [ARCHITECTURE.md](ARCHITECTURE.md), [DEVELOPMENT.md](DEVELOPMENT.md) | trued up at every unit close **and** at pickup; git is their history, so no changelog section |
| **navigation** | every `map.md` | lockstep with the directory's content, in the same commit |
| **campaign** | [briefs/](briefs/map.md), [docs/design/](docs/design/map.md) | amended in place, dated; frozen and archived to [docs/history/](docs/history/map.md) at close |
| **ledger** | [task/ledgers/](task/ledgers/map.md): `staging/<unit>-ledger.md` → `completed/` → `archive/yyyy-mm/yyyy-mm-dd-<unit>-ledger.md` | the directory is the status. Append-only in `staging/`; `move`d to `completed/` in the unit's last commit and frozen; filed to `archive/` by `make ledger-archive` at the next pickup, immutable. A campaign's `docs/history/` folder links to its ledgers in the monthly archive; pre-2026-08-23 folders keep theirs |
| **skill** | [.agents/skills/](.agents/skills/map.md) | versioned with the procedure it records; a measured-and-**declined** rule is written down so nobody re-litigates it |

The rules that bind all six:

- **A document names the event that retires it at birth.** "This file closes when X merges" belongs
  in its first commit. If nothing can retire a document, it is a contract or a state document —
  or it should not have been created.
- **Truth moves, it is never deleted.** Compaction is archival, not removal: a closed campaign's
  record goes to `docs/history/`. Only working notes that produced no decision may be deleted.
- **A live document carries no obituary.** `STATUS.md` "Active workstreams" and the slate carry
  block markers (`scripts/doc_blocks.py`) so `scripts/ledger_lifecycle.py compact` makes both leave
  mechanically; closure is declared in the marker under an owner ruling, never inferred (DL-4,
  2026-08-25).
- **A claim that can go stale carries its date.** Measurements, counts, timings, "not yet", "planned",
  and phase words are dated where they are written.
- **An archived document is corrected only by a dated errata note at its top**, never rewritten.
- **Every fact is single-homed**; every other mention is a pointer (the rule in
  [`## Precedence`](#precedence) applied to documents).

The **executor** is [.agents/skills/compact-context-docs/SKILL.md](.agents/skills/compact-context-docs/SKILL.md)
— this section is the policy, the skill is the procedure. Mechanical halves:
`make check-map-sync`, `make check-ledgers`, `make check-ledger-grammar`,
`make check-docs-compaction`.

## Working style and communication

- **Stop gathering once you can act.** Redundant reads and exploratory work past sufficient
  context are waste — in a delegated unit they are the main way a context budget is lost.
- **Answer in the language the requester used.** Source code, comments, identifiers, commit
  messages, and PR titles and bodies stay **English** regardless.
- **Be concise.** No sycophantic openers, no closing filler, no narrated status.

## PyO3 build notes

- The cdylib's `extension-module` feature is **off by default**; it is enabled only when
  maturin builds the wheel.
- Therefore the test command is **`cargo test --workspace`**, never the flag that enables every
  feature — that would turn on `extension-module`, which tells PyO3 not to link libpython and
  breaks a standalone test binary. CI, the Makefile, and this rule all agree.
- PyO3's build script needs an interpreter; linking `cargo test` needs `libpython3.x.so` present.

## Version-pin contract

Pin **one** DataFusion version across `datafusion` + `datafusion-spark` + `iceberg*`.
**The live `Cargo.toml` is the SSOT for every pin** — prose tables are orientation only; verify against
`Cargo.toml` before ANY family bump. The `iceberg*` family comes from the **owned fork**, so a
"bump" is re-pinning the `[patch.crates-io]` rev plus `datafusion` + `datafusion-spark` +
`arrow*`/`parquet` + `rust-toolchain.toml`, re-resolving with `cargo update`. `Cargo.lock` is
checked in. Upstream family bumps are **skipped** until the fork moves its base — record a dated
take/skip decision per release. Never merge a Dependabot PR that bundles a safe bump with a
DataFusion/arrow family major — split it.

**Every fork repin also re-verifies what we built on top of the old rev.** The duties (and the
defects that motivate them) live on the crate that owns the code:
[crates/repark-iceberg/map.md](crates/repark-iceberg/map.md) "Known limitations".

## Explicitly out of scope (do not reintroduce without a decision)

- **PyIceberg** — neither the Python lib nor the `pyiceberg_core` crate. Iceberg = `iceberg-rust` only.
- **Sail / pysail** — own-the-stack was chosen; no dependency on it.
- **Distributed cluster** — the `ExecutionBackend` seam marks the boundary (see
  [ARCHITECTURE.md](ARCHITECTURE.md) "what the seam is, honestly"). Do not build
  Ballista-for-writes (it cannot serialize Iceberg write/commit plan nodes).
- **External code PRs** — not accepted while the engine is pre-alpha; see
  [CONTRIBUTING.md](CONTRIBUTING.md).

## Upstream contribution policy

Upstream-mergeability with `apache/iceberg-rust` is **not a constraint** — upstreaming a primitive
is **optional/opportunistic**, not an obligation. We still **cherry-pick upstream improvements**
into the fork when useful.

## Resource discipline — disk and artifacts

Builds, test runs, coverage, and per-unit worktrees are the largest consumers of disk here, and a
disk that fills halts a campaign mid-unit.

- **Check free space before you spend it** — before creating a worktree, and before any
  artifact-heavy command.
- **Re-check at phase boundaries** and before broad validation. If the remaining space may not
  safely carry the next command, stop and reclaim task-owned artifacts — never start the command
  hoping it fits.
- **Cleanup is scoped.** Remove the generated artifacts *this* task created, once unneeded.
  **Never delete another task's worktree, and never delete uncommitted files.**
- **Share caches instead of duplicating them** across worktrees where the tool allows it.
- **Report it at handoff:** the disk checks you ran, the cleanup you performed, and any worktree or
  artifact you deliberately kept, with the reason it is still needed.

Procedure: [.agents/skills/check-disk-headroom/SKILL.md](.agents/skills/check-disk-headroom/SKILL.md).

## Safety — destructive / outward-facing operations

The engine touches AWS (Glue, S3 Tables, S3). **Never drop or delete a Glue table, an S3 Tables
table, or S3 data, and never mutate IAM, without explicit user action** — if such an operation
seems needed, stop and ask. AWS writes go only through the engine's sanctioned catalog/write paths
(`repark-iceberg`). **Commit or push only when the user asks.** These approval boundaries bind every
contributor and every unit; a unit may narrow them, never relax them.

## Delegated-agent standing rules

The rules every delegated work unit inherits — briefs in [briefs/](briefs/map.md) reference this
section instead of restating it; a brief may narrow these, never relax them.

- **The Rust rule:** Python builds plans; Python never touches rows. Engine-missing
  functions get Rust shims or LOUD unsupported errors — never Python compute. (Deliberate
  exception: user-supplied UDF code, engine-driven over Arrow batches.)
- **Workspace validity:** commits MUST pass the installed hooks; verify hooks fire before
  the first commit in any worktree. A hook bypass or non-firing-hook workspace is a
  slate-failing violation.
- **Gates:** every unit gates, including STOP / report-only units: the scoped local gate (comment
  gate, release native, `cargo test -p` touched crates, own pytests) plus CI's required checks —
  never the whole-workspace suite locally (owner, 2026-09-18); REAL exit codes; Makefile-pinned lint.
- **Ledgers:** one `task/ledgers/staging/<unit>-ledger.md` per unit, linked from that
  directory's `map.md` in the same commit; `move`d to `completed/` in the unit's last commit.
  Ledger presence is a gate item.
- **Oracles:** oracle/differential test files are NAMED deliverables per unit; live-oracle
  output recorded verbatim; hand-computed expectations are not an oracle. Divergences get
  honest `_divergence` pins, never silent absorption.
- **Mechanical gates bite every commit** (see "Hard rules"): a red gate is never worked
  around — the sanctioned outs are visible diffs, reviewed like code.
- **Test relocations follow docs/testing.md "Relocation discipline":** move-only = identity-diff
  gate; path-changing regroups are declared-rename units that ship alone with a name map.
- **Disk:** the pre-spend checks, scoped cleanup, and handoff report in
  ["Resource discipline"](#resource-discipline--disk-and-artifacts) bind every unit.
- **Never:** AWS credentials/envs, `Cargo.toml [patch]` changes, `.github/` changes,
  secrets in any output. Clean STOP states only — a dirty worktree is not a delivered unit.

## Delegated work

Single-agent-in-the-main-thread is the default. The orchestrating agent owns architecture and
assembly; delegated fan-out is for **search, mechanical edits, and narrow, well-scoped
implementation**, never for architectural judgement. Every unit inherits the standing rules above
and the "Safety" approval boundaries — it may narrow those, never relax them. Tier policy:
owner-approved executor tiers (owner, 2026-09-20); launch mechanics live in [CLAUDE.md](CLAUDE.md).

## Process governance (SEPMO)

Non-trivial work runs under the **SEPMO control plane**
([.agents/skills/sepmo/SKILL.md](.agents/skills/sepmo/SKILL.md)): scope audit (proposition-ledger gate:
every clause `PROVEN`, zero `OPEN`/`REJECTED`) → adversarial Actor–Critic → per-PR delivery →
retrospective. SEPMO governs **only how work flows** and **cedes every engineering decision to this
contract** — on conflict the chain in [`## Precedence`](#precedence) wins. Roles bind via
[.agents/skills/sepmo/binding-manifest.md](.agents/skills/sepmo/binding-manifest.md). Actor–Critic runs
**single-session by default**; sub-agent fan-out follows "Delegated work" above.

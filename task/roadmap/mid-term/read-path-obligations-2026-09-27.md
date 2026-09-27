# Read-path obligations inventory — 2026-09-27

Proof artifact for WO READ-PATH-1 (owner go 2026-09-27 on efficiency-review items 3 and 4).
Every imperative or binding sentence in the five compaction sources, one row each: the
obligation, its old home, its new home after the read-path split, and the loading trigger
(the role whose read path loads it). A row with no new home is a HALT, not a deletion.

This file closes when the READ-PATH-1 PR merges; the merged PR is its record.

Trigger key: **O** = orchestrator / governing session, **E** = executor (unit / work order),
**C** = critic / reviewer, **L** = lookup / status question (STATUS.md only; no row loads on L).
"All" = O+E+C. New-home shorthands: **A** = [AGENTS.md](../../../AGENTS.md),
**C** = [CLAUDE.md](../../../CLAUDE.md), **EM** = engineering-method SKILL.md,
**S** = sepmo SKILL.md, **CCC** = critic-critic-critic SKILL.md; **P** = the skill's new
`## Procedure` section, **R** = its `## Reference` section, **H** = its new `history.md`.

## AGENTS.md (A-001…)

| ID | Obligation | Old home | New home | Trigger |
|----|-----------|----------|----------|---------|
| A-001 | No code comments from Anthropic models (owner ruling, verbatim). | A top | A top (unchanged) | All |
| A-002 | Markdown may carry explanatory prose; reasons live in `map.md`. | A ruling adjustment | A top (unchanged) | All |
| A-003 | Review holds the comment rule; required docstrings, banners, invariant comments remain; no sweep. | A boundary | A top (unchanged) | All |
| A-004 | When a rule changes, it changes in AGENTS.md; other files point, never restate. | A intro | A intro (kept) | All |
| A-005 | Adapters carry no authoritative facts; deleting one loses no knowledge. | A intro | A intro (kept) | All |
| A-006 | Load only your role's read path (role table); roles state what they do NOT load. | A Read first (universal list) | A Read first (role table) | All |
| A-007 | Authority chain AGENTS.md > PROJECT.md > STATUS.md > conventions > SEPMO; this section is the chain's single home; others point, never restate. | A Precedence | A Precedence (kept) | All |
| A-008 | SEPMO never overrides an engineering rule; a conflict is a clarifying question (D1), contract wins. | A Precedence | A Precedence (kept) | O, E |
| A-009 | Product intent lives in README / PROJECT.md; current state in STATUS.md; do not restate state here. | A What repark is | A What repark is (kept) | O |
| A-010 | Do not create a deferred crate home ahead of its driver code. | A Crate map | A Crate map (kept) | O, E |
| A-011 | The live workspace Cargo.toml is the authoritative crate list; repo-manifest.toml mirrors it via `make check-manifest`. | A Crate map | A Crate map (kept) | O, E |
| A-012 | Nothing appears at a `planned` component path while declared planned (gate reds). | A Crate map | A Crate map (kept) | O, E |
| A-013 | `make verify` before done: lint, format, clippy, Rust tests, touched `map.md` current. | A Verify before done | A Verify before done (kept) | E |
| A-014 | `verify` is Rust-only; it does not build the native module. | A Verify before done | A Verify before done (kept) | E |
| A-015 | Run `make preflight` before opening a PR. | A Verify before done | A Verify before done (kept) | E |
| A-016 | `make ci` is the canonical fast gate. | A Verify before done | A Verify before done (kept) | E |
| A-017 | Tool versions pinned identically in Makefile and workflows; CI-enforced tools never silently skip locally. | A Verify before done | A Verify before done (kept) | E |
| A-018 | iceberg-rust is forked and owned; the table-format engine lives in the fork. | A Hard rules | A Hard rules (kept) | O, E, C |
| A-019 | The fork stays a separate repo, never vendored; `[patch.crates-io]` rev-pinned. | A Hard rules | A Hard rules (kept) | O, E |
| A-020 | The fork's `iceberg-datafusion` is a supported surface (DELETE/UPDATE/INSERT); MERGE stays RePark-owned. | A Hard rules | A Hard rules (kept) | O, E |
| A-021 | Fork capability status lives ONLY in the fork's GAP_MATRIX.md + ENGINE_CONTRACT.md; link, never restate. | A Hard rules | A Hard rules (kept) | O, E |
| A-022 | DataFusion is a normal upstream dep; do not fork it. | A Hard rules | A Hard rules (kept) | O, E |
| A-023 | Two honest SQL doors, no blended parser; new SQL surface lands with both spellings + one test row per door. | A Hard rules | A Hard rules (kept) | O, E, C |
| A-024 | Everything-through-Session: no global mutable state, no env reads at query time. | A Hard rules | A Hard rules (kept) | E, C |
| A-025 | Bindings-as-thin-adapter: one internal engine API; PyO3 and Flight SQL are thin adapters. | A Hard rules | A Hard rules (kept) | E, C |
| A-026 | Tests in the same commit as code; no "later". | A Hard rules | A Hard rules (kept) | E, C |
| A-027 | Entry-point matrix (native / ANSI / facade) is the testing structure; divergence-class claims pin every class per entry point on the Arrow path, value AND type. | A Hard rules | A Hard rules (kept) | E, C |
| A-028 | `map.md` in every directory, updated in the same change; new directory → new `map.md`. | A Hard rules | A Hard rules (kept) | E, C |
| A-029 | Maps are hand-written; no generator; automation only checks. | A Hard rules | A Hard rules (kept) | E |
| A-030 | Rust house style (one-line comments, banners stay, blank lines, max_width=100, edition=2024, clippy all+pedantic -D warnings, thiserror/anyhow, tracing, no panics). | A Hard rules | A Hard rules (kept) | E, C |
| A-031 | Mechanical gates are enforced; each has a script/list SSOT; prose points, never restates; dual-wired `make ci` + ci.yml. | A Hard rules | A Hard rules (kept) | E |
| A-032 | Panic + async bans via clippy.toml; escape is per-call-site `#[expect]` with reason; never file/crate-wide allow; one recorded module exception (binding taxonomy). | A Hard rules | A Hard rules (kept) | E, C |
| A-033 | Crate DAG policy: undeclared edge, promoted kind, or forbidden shape is red; writing an edge down cannot legalize it. | A Hard rules | A Hard rules (kept) | E |
| A-034 | Python source file-size + facade thinness: exact-baseline ceilings; re-export-only modules open with `re-export binding`. | A Hard rules | A Hard rules (kept) | E |
| A-035 | Rust file-size: default ceiling + EXCEPTIONS, ratchet DOWN only; size gates fail on growth and unrecorded shrink; ceilings never restated here. | A Hard rules | A Hard rules (kept) | E |
| A-036 | Python conventions: nested-`def` ban, `dataclasses`/`attrs` ban; not on the pre-commit hook as of PYC-5 (sub-second budget); Ruff ANN; naming is review. | A Hard rules | A Hard rules (kept) | E, C |
| A-037 | Public-docstring presence D101/D102/D103/D105/D107; style D declined (facade mirrors PySpark). | A Hard rules | A Hard rules (kept) | E |
| A-038 | Structural truth: repo-manifest.toml mirrors the DAG SSOT; checks maps, never writes one. | A Hard rules | A Hard rules (kept) | O, E |
| A-039 | parity-live dual-wire; fail-closed on a parse miss. | A Hard rules | A Hard rules (kept) | E |
| A-040 | `map.md` content: link validity armed; coverage behind `--strict`. | A Hard rules | A Hard rules (kept) | E |
| A-041 | v1 helper scripts return only with a concrete driver named in scripts/map.md. | A Hard rules | A Hard rules (kept) | O, E |
| A-042 | Rust default module layout; `#[path]` is not module inclusion; genuine exceptions stay local with a reason. | A Hard rules | A Hard rules (kept) | E, C |
| A-043 | `unsafe_code = "forbid"` except `crates/repark-python`; do not add `unsafe` elsewhere. | A Hard rules | A Hard rules (kept) | E, C |
| A-044 | Python: hints everywhere, Pydantic v2, module/class-level defs, verb names, docstrings, pathlib, logging, f-strings, no bare except, Ruff 100. | A Hard rules | A Hard rules (kept) | E, C |
| A-045 | Spell things out; no casual abbreviations. | A Hard rules | A Hard rules (kept) | E, C |
| A-046 | Tier-2 CI never runs against unmerged code; nightly on main + manual OIDC; no self-hosted runners; no secrets in tier-1. | A Hard rules | A Hard rules (kept) | O, E |
| A-047 | Fixes stay narrow; semantic-adjacent rewrites ship separately, never on a sensitive path. | A Change discipline | A Change discipline (kept) | E, C |
| A-048 | Do not refactor only to ease unit testing; test as-is or argue the refactor alone. | A Change discipline | A Change discipline (kept) | E, C |
| A-049 | Smallest readable design wins; no speculative managers/factories/adapters; extensibility needs a second caller. | A Change discipline | A Change discipline (kept) | E, C |
| A-050 | Comments carry the non-obvious reason, assumption, or invariant (rule in next section). | A Change discipline | A Change discipline (kept) | E, C |
| A-051 | Write for the eventual reader; no audience-analysis section in the artifact. | A Eventual reader | A Eventual reader (kept) | E |
| A-052 | Comment the WHY, never the WHAT. | A Eventual reader | A Eventual reader (kept) | E, C |
| A-053 | Shortest form that carries the reason; rationale goes to ARCHITECTURE / map.md / ADR, not inline. | A Eventual reader | A Eventual reader (kept) | E |
| A-054 | Comments and docstrings use ASD-STE100 Simplified Technical English. | A Eventual reader | A Eventual reader (kept) | E |
| A-055 | Every function has a docstring (what, inputs, outputs); non-trivial use Google sections; facade mirrors PySpark. | A Eventual reader | A Eventual reader (kept) | E, C |
| A-056 | The 91-`=` banner keeps its form; no narration, no signature restatement, no unreachable-case walks. | A Eventual reader | A Eventual reader (kept) | E |
| A-057 | Comment consolidation is chartered sweep work, never a passenger on a fix. | A Eventual reader | A Eventual reader (kept) | E |
| A-058 | Prose style held by review; docstring presence by gate; style D declined. | A Eventual reader | A Eventual reader (kept) | E |
| A-059 | Every markdown document belongs to exactly one lifecycle class. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-060 | Contract/state/navigation/campaign/ledger/skill lifecycles per the class table. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-061 | A document names its retiring event at birth. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-062 | Truth moves, never deleted; compaction is archival to docs/history/. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-063 | A live document carries no obituary; closure declared in block markers, never inferred. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-064 | Stale-able claims carry their date. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-065 | Archived documents corrected only by a dated errata note at top, never rewritten. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-066 | Every fact single-homed; other mentions are pointers. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-067 | compact-context-docs is the lifecycle executor; mechanical halves are the four `make check-*` gates. | A Doc lifecycle | A Doc lifecycle (kept) | O, E |
| A-068 | Stop gathering once you can act; no redundant reads past sufficient context. | A Working style | A Working style (kept) | All |
| A-069 | Answer in the requester's language; code/identifiers/commits/PRs stay English. | A Working style | A Working style (kept) | All |
| A-070 | Be concise; no openers, filler, or narrated status. | A Working style | A Working style (kept) | All |
| A-071 | cdylib `extension-module` off by default; on only for maturin wheel builds. | A PyO3 notes | A PyO3 notes (kept) | E |
| A-072 | Test with `cargo test --workspace`, never the all-features flag. | A PyO3 notes | A PyO3 notes (kept) | E |
| A-073 | PyO3 build needs an interpreter; linking needs libpython present. | A PyO3 notes | A PyO3 notes (kept) | E |
| A-074 | Pin one DataFusion version across datafusion + datafusion-spark + iceberg*; live Cargo.toml is the SSOT. | A Version-pin | A Version-pin (kept) | O, E |
| A-075 | A bump re-pins the patch rev + family + toolchain and re-resolves; Cargo.lock checked in. | A Version-pin | A Version-pin (kept) | O, E |
| A-076 | Upstream family majors skipped until the fork moves base; dated take/skip per release; never merge bundled Dependabot PRs — split. | A Version-pin | A Version-pin (kept) | O |
| A-077 | Every fork repin re-verifies local workarounds and swallowed overrides (duties in repark-iceberg map). | A Version-pin | A Version-pin (kept) | O, E |
| A-078 | Out of scope: PyIceberg, Sail/pysail, distributed cluster (no Ballista-for-writes), external code PRs. | A Out of scope | A Out of scope (kept) | O, E |
| A-079 | Upstreaming is optional; cherry-pick useful upstream improvements into the fork. | A Upstream policy | A Upstream policy (kept) | O |
| A-080 | Check free disk before spending it; re-check at phase boundaries; reclaim before continuing. | A Resource discipline | A Resource discipline (kept) | O, E |
| A-081 | Cleanup is scoped; never delete another task's worktree or uncommitted files. | A Resource discipline | A Resource discipline (kept) | O, E |
| A-082 | Share dependency/build caches across worktrees. | A Resource discipline | A Resource discipline (kept) | O, E |
| A-083 | Report disk checks, cleanup, and kept artifacts at handoff (procedure skill). | A Resource discipline | A Resource discipline (kept) | O, E |
| A-084 | Never drop/delete a Glue table, S3 Tables table, or S3 data, never mutate IAM, without explicit user action; stop and ask. | A Safety | A Safety (kept) | All |
| A-085 | AWS writes go only through the engine's sanctioned catalog/write paths. | A Safety | A Safety (kept) | E |
| A-086 | Commit or push only when the user asks. | A Safety | A Safety (kept) | All |
| A-087 | Approval boundaries bind every unit; a unit may narrow them, never relax them. | A Safety | A Safety (kept) | O, E |
| A-088 | The Rust rule: Python builds plans, never touches rows; missing engine functions get Rust shims or LOUD errors (UDF exception: user code, engine-driven). | A Standing rules | A Standing rules (kept) | O, E, C |
| A-089 | Workspace validity: commits pass installed hooks; verify hooks fire before the first commit. | A Standing rules | A Standing rules (kept) | E |
| A-090 | Every unit gates (scoped local + CI required); never the whole suite locally; REAL exit codes; Makefile-pinned lint. | A Standing rules | A Standing rules (kept) | E |
| A-091 | One staging ledger per unit, linked from its map.md; moved to completed/ in the last commit; presence is a gate item. | A Standing rules | A Standing rules (kept) | E |
| A-092 | Oracles are NAMED deliverables; live-oracle output verbatim; no hand-computed expectations; divergences get honest `_divergence` pins. | A Standing rules | A Standing rules (kept) | E, C |
| A-093 | Mechanical gates bite every commit; sanctioned outs are visible diffs. | A Standing rules | A Standing rules (kept) | E |
| A-094 | Test relocations follow the Relocation discipline (identity-diff gate; renames ship alone with a name map). | A Standing rules | A Standing rules (kept) | E |
| A-095 | Disk checks, scoped cleanup, and handoff report bind every unit. | A Standing rules | A Standing rules (kept) | E |
| A-096 | Never: AWS credentials/envs, Cargo `[patch]` changes, `.github/` changes, secrets in output; clean STOP states only. | A Standing rules | A Standing rules (kept) | E |
| A-097 | Single-agent-in-main-thread is the default; orchestrator owns architecture and assembly. | A Delegated work | A Delegated work (kept) | O |
| A-098 | Delegated fan-out is for search, mechanical edits, and narrow implementation — never architectural judgement. | A Delegated work | A Delegated work (kept) | O |
| A-099 | Executor tiers are owner-approved; launch mechanics live in CLAUDE.md. | A Delegated work | A Delegated work (kept) | O |
| A-100 | Non-trivial work runs under SEPMO (scope audit → Actor–Critic → PR → delivery → retrospective). | A Process governance | A Process governance (kept) | O |
| A-101 | SEPMO cedes every engineering decision to this contract; roles bind via the manifest; Actor–Critic runs single-session by default. | A Process governance | A Process governance (kept) | O |

## CLAUDE.md (C-001…)

| ID | Obligation | Old home | New home | Trigger |
|----|-----------|----------|----------|---------|
| C-001 | Owner ruling block VERBATIM (owner text; never reword, shorten, or move). | CLAUDE.md top | CLAUDE.md top (unchanged) | All |
| C-002 | STOP: the authoritative contract is AGENTS.md; read it first; this adapter defines no project rules. | CLAUDE.md intro | CLAUDE.md intro (kept) | All |
| C-003 | Follow the pointer table to authoritative homes (read path, precedence, state, DAG, setup, gates, testing, intent, maps). | CLAUDE.md pointers | CLAUDE.md pointers (kept, trimmed) | All |
| C-004 | Read by role via the AGENTS.md role table (replaces the universal read order). | CLAUDE.md read order | CLAUDE.md role pointer | All |
| C-005 | Keep this filename so auto-loading tooling lands on AGENTS.md on turn 1. | CLAUDE.md read order | CLAUDE.md role pointer (kept) | All |
| C-006 | `.claude/skills` symlinks `.agents/skills`; skills load natively and are invocable by name; roster in skills map. | CLAUDE.md mechanics | CLAUDE.md mechanics (kept) | O |
| C-007 | SEPMO is invocable by name; discoverable is not auto-run; invoke deliberately for non-trivial work. | CLAUDE.md mechanics | CLAUDE.md role pointer (kept) | O |
| C-008 | Governed sessions run the repo's SEPMO + bound CCC engine; no user-level variant overrides; repo copy wins. | CLAUDE.md mechanics | CLAUDE.md mechanics (kept) | O, C |
| C-009 | A governed unit starts from the unit-runbook.md checklist. | CLAUDE.md mechanics | CLAUDE.md mechanics (kept) | O |
| C-010 | CCC role shapes map to Claude mechanics per the spawn table (fresh Explore, Fixer general-purpose, scratch clone, tier). | CLAUDE.md spawn table | CLAUDE.md spawn table (kept) | O, C |
| C-011 | Opus orchestrates; fan-out runs on AGENTS.md-allowed tiers with an explicit tier and posture; tiers share one method. | CLAUDE.md tiers | CLAUDE.md tiers (kept) | O |
| C-012 | Do not spawn Opus sub-agents without a direct, explicit request naming Opus. | CLAUDE.md tiers | CLAUDE.md tiers (kept) | O |
| C-013 | Single agent in the main thread is the default; do not fan out unless the user asks. | CLAUDE.md tiers | CLAUDE.md tiers (kept) | O |
| C-014 | Relaxations of the tier section are recorded in task/lessons.md. | CLAUDE.md tiers | CLAUDE.md tiers (kept) | O |

## engineering-method SKILL.md (E-001…)

| ID | Obligation | Old home | New home | Trigger |
|----|-----------|----------|----------|---------|
| E-001 | Operate as a senior Rust/Python engineer; boring, obvious solutions over clever ones. | EM Identity | EM top intro (kept) | E, C |
| E-002 | Priority order: correctness → clarity → production-readiness. | EM Identity | EM top intro (kept) | E, C |
| E-003 | Read AGENTS.md before this skill; AGENTS.md wins on any conflict; do not load for navigation/Q&A/status. | EM Identity | EM P router + top intro (kept) | E, C |
| E-004 | Non-negotiables: no destructive SQL/AWS/IAM; tests same commit; no prod panics; parity case per new op; plan files only; no dependency edits unapproved. | EM Non-Negotiables | EM P (kept, locator) | E, C |
| E-005 | Interactive mode: reason, record the plan, check in before complex implementation (§1), confirm scope changes. | EM Mode Handling | EM R Mode Handling (kept) | E |
| E-006 | Delegated mode: never block on approval; surface blockers/assumptions/decisions in the final report; ambiguity that changes outcome still stops. | EM Mode Handling | EM R Mode Handling (kept) | E |
| E-007 | Delegated §1 becomes document-plan-proceed-flag; reviewer corrections go to lessons per §2. | EM Mode Handling | EM R Mode Handling (kept) | E |
| E-008 | Plan in the unit ledger (or task/todo.md for quick work); lessons in task/lessons.md; pickup via STATUS + ledger. | EM Workflow Storage | EM P (kept) | E |
| E-009 | Read the lessons entries for subsystems you touch (`grep -n <subsystem> task/lessons.md`); in full only when orchestrating. | EM Workflow Storage + §2 + Pre-Flight | EM P (scoped: storage, §2, Pre-Flight) | O (full), E (scoped) |
| E-010 | Ask "what can go wrong with what I build?" at design, implementation, and test time. | EM Risk-First | EM P (kept) + R detail | E, C |
| E-011 | Design risk pass: preconditions, failing deps, invariants, partial failure, silent-bug cost, edge validation, double-execution. | EM Risk-First | EM R Risk-First (kept) | E |
| E-012 | Implementation risk pass: unwrap/expect, bare except, TOCTOU, off-by-one, overflow/NaN, concurrency, destructive paths. | EM Risk-First | EM R Risk-First (kept) | E |
| E-013 | Testing risk pass: every test names its risk; negative per happy path; named numeric regressions; guards tested shut; races tested directly. | EM Risk-First | EM R Risk-First (kept) | E |
| E-014 | Keep the project risk surface (parity, numerics, snapshot atomicity, destructive SQL, map drift) in front of mind. | EM Risk-First | EM R Risk-First (kept) | E |
| E-015 | Risk-First is naming failure modes, not defensive programming. | EM Risk-First | EM R Risk-First (kept) | E |
| E-016 | Single-agent default; delegated fan-out only for search/mechanical/narrow work; tiers live in the tool adapter. | EM Workflow intro | EM P Workflow intro (kept) | O, E |
| E-017 | §1: reason before acting on non-trivial tasks (contract, edges, simplest approach, prep-refactor, plan in tracker, check in). | EM §1 | EM P §1 (kept) | E |
| E-018 | §1 while working: re-read plan + scoped lessons per step; sub-bullets on complexity; STOP and re-plan on surprise; flip boxes; record why. | EM §1 | EM P §1 (kept) | E |
| E-019 | §2: after ANY user correction, append a dated DO/DO NOT lesson immediately; iterate; supersede, never mutate. | EM §2 | EM P §2 (kept) | E |
| E-020 | §2: review lessons before each implementation step; never use code placeholders — write complete functions. | EM §2 | EM P §2 (kept) | E |
| E-021 | §3: re-read any file before editing and after editing; re-read on long conversations; never trust memory of file state. | EM §3 | EM P §3 (kept) | E |
| E-022 | §4: read docs/testing.md before any code change; tests-with-code is a hard block; names are specifications; parity + f64::to_bits regressions. | EM §4 | EM P §4 done gate (kept) | E |
| E-023 | §4 done gate: all boxes checked (tests, names, risks, happy+negative, fail-without-change, parity, compile, green, schema, nulls, logs, imports, verify commands). | EM §4 | EM P §4 done gate (kept) | E |
| E-024 | §5: pause for elegance on non-trivial changes; correct and clear first; profiled bottlenecks only; right complexity up front. | EM §5 | EM P §5 (kept) | E |
| E-025 | §6 scope boundaries: plan files only; no drive-by cleanup/renames/features/signature changes; unexpected file → STOP/report; flag out-of-scope finds. | EM §6 | EM P §6 (kept) | E |
| E-026 | §7: verify external library APIs current; record corrected usage in lessons; Arrow for the long term; exact signatures; no dependency edits unapproved. | EM §7 | EM P §7 (kept) | E |
| E-027 | §8 debugging protocol in order: read error, reproduce, isolate, hypothesize, fix smallest, verify, regression-check. | EM §8 | EM P §8 (kept) | E |
| E-028 | §8: no unrelated refactors; one change at a time; two failed fixes → re-read from disk; consult map.md#debug first. | EM §8 | EM P §8 (kept) | E |
| E-029 | §9 quality gates: no magic numbers; docstrings per AGENTS.md; actionable errors; explicit types; unrepresentable illegal states; immutable-first; rule of three; delete dead code; <100-line functions. | EM §9 | EM P §9 (kept) | E |
| E-030 | Navigation: read touched map.md first; use I-want-to/Pointers; code beats map (fix map same change); Debug before §8. | EM Navigation | EM P Navigation (kept) | E |
| E-031 | Naming: spell it out; allowed acronyms only; no casual abbreviations; no single letters; boolean questions; verbs/nouns/plurals. | EM Naming | EM R Naming (kept) | E |
| E-032 | Verification commands canonical list (Rust make verify + cargo roster; Python ruff + pytest + conventions check). | EM Language Rules | EM R Language Rules (kept) | E |
| E-033 | Rust panic-ban how-to: with_context/?, ok_or_else/?, log-and-exit only in main; tests prefer .expect("context"). | EM Language Rules | EM R Language Rules (kept) | E |
| E-034 | Library thiserror enums, binaries anyhow, Error::source, tracing without secrets; try_into over `as`; iterators; FFI validation at boundary; lock-order and async rules. | EM Language Rules | EM R Language Rules (kept) | E |
| E-035 | Python how-to: polars default, frozen models, noqa with rule + reason. | EM Language Rules | EM R Language Rules (kept) | E |
| E-036 | Function length/recursion: <100 lines, extract on name-worthiness; recursion only for recursive data + bounded + clearer; max_depth or Vec stack on user-influenced input. | EM Func Length | EM R Func Length (kept) | E |
| E-037 | Pre-Flight checklist before starting; §4 done gate before declaring complete. | EM Pre-Flight | EM P Pre-Flight (kept) | E |
| E-038 | Core principles TL;DR (simplicity, read-before-write, no assumptions, risk-first, names, no panics, iterate, small functions, types, edges, measure, minimal impact). | EM Core Principles | EM R Core Principles (kept) | E |

## sepmo SKILL.md (S-001…)

| ID | Obligation | Old home | New home | Trigger |
|----|-----------|----------|----------|---------|
| S-001 | Every gate is a checkable artifact, not a self-report. | S spine intro | S P (kept) | O |
| S-002 | Load the reference file for your phase before acting; the spine routes, the reference instructs. | S spine intro | S P (kept) | O |
| S-003 | Frontier model on every critical-path step; tier/governance resolves via the binding manifest; this repo runs single-agent default with procedural context break. | S Model assumption | S R Model assumption (kept) | O |
| S-004 | Every sequence of work converges on a PR; carve by logical coherence, never bundle or split a logical change. | S PR unit | S P (pointer) + R (rule) | O |
| S-005 | Severity scale S0–S3 is global vocabulary; default floor S1; manifest may raise, never lower. | S Severity | S P (floor) + R scale | O, C |
| S-006 | Iron state machine in order; backward transitions allowed, skipping forward forbidden; the transition table is normative. | S State machine | S P (map) + R table | O |
| S-007 | Proposition ledger with PROVEN/OPEN/REJECTED verdicts; quantified clauses carry the enumeration obligation. | S Ledger gate | S P (kept) + R detail | O |
| S-008 | Gate passes iff zero OPEN/REJECTED and the user explicitly confirms; LOGIC_SCORE is the ratio only; a score without a ledger is an audit failure. | S Ledger gate | S P (kept) | O |
| S-009 | PRE_EXECUTION_REVIEW: one one-time whole-plan review owned by the Orchestrator; gaps route backward, never patch inline. | S PRE_EXECUTION_REVIEW | S P (kept) + R detail | O |
| S-010 | Invariant V is a standing invariant from gate pass to retrospective; it owns the T8 drift alarm. | S Invariant V | S R Invariant V (kept) | O |
| S-011 | Escaped defects and lifecycle-machinery incidents trigger an immediate incident retrospective with asymmetric feed-forward. | S Incidents | S R Incidents (kept) | O |
| S-012 | Per-PR sub-machine stages with owners and exit guards (scoping → build → SLR → break → Critic → remediate → convergence → readiness → assemble). | S Sub-machine | S P (order) + R stages | O |
| S-013 | R1: minimum one sequential frontier–frontier cycle per PR unit. | S R1–R13 | S P (minimum) + R (rule) | O |
| S-014 | R2: green exit (build, tests, static checks per manifest); every clause pinned; quantified clauses pinned per enumerated element; domain growth inherits the obligation same-unit. | S R1–R13 | S P (gloss) + R (rule) | O, E |
| S-015 | R3: context break before every Critic (restricted inputs, findings before self-review, artifact evidence, fresh context preferred; procedural break compensated by novel fresh execution on silently-wrong claims). | S R1–R13 | S P (gloss) + R (rule) | O, C |
| S-016 | R4: convergence is complete coverage attestation + no open/sustained-disputed findings at/above floor; the Critic's call, never the Actor's. | S R1–R13 | S P (gloss) + R (rule) | O, C |
| S-017 | R5: remediation needs regression proof (failed-before/pass-after test) or a one-line justification; "fixed" without proof is OPEN. | S R1–R13 | S P (gloss) + R (rule) | O, E |
| S-018 | R6: disputes terminate (WITHDRAWN/sustained); sustained at/above floor halts the unit; below floor ships ACCEPTED_FLAGGED in PR + retro; nothing silently dropped. | S R1–R13 | S P (gloss) + R (rule) | O |
| S-019 | R7: readiness audit is light but real; mergeable means CI green; unit gate + pre-merge gate bound; CI-only exceptions name residual gaps; silent skip is a binding defect. | S R1–R13 | S P (gloss) + R (rule) | O |
| S-020 | R8: the PR embeds trace, attestation summary, findings ledger, and shipped flags. | S R1–R13 | S P (gloss) + R (rule) | O |
| S-021 | R9: DELIVERY is per-PR. | S R1–R13 | S R rules (kept) | O |
| S-022 | R10: environment drift proven by the base-ref reproduction test; base-red is its own unit; recorded as environment_drift_events. | S R1–R13 | S R rules (kept) | O |
| S-023 | R11: contingencies must be executable by their trigger role (additive by construction or pre-authorized destructive). | S R1–R13 | S R rules (kept) | O |
| S-024 | R12: every unit ends CONVERGED/REMOVED/REMANDED; unsettled dispositions block the line even when logged. | S R1–R13 | S R rules (kept) | O |
| S-025 | R13: remand is explicit with enumerated findings; downstream proceeds only on recorded disjoint scope; closing authority dispositions item by item; user decisions are named merge gates. | S R1–R13 | S R rules (kept) | O |
| S-026 | Proportionality: LIGHT only when all six rubric criteria hold (else STANDARD); bar never scales, only process amount. | S Proportionality | S P (rubric) + R detail | O |
| S-027 | Doctrines D1–D5 bind every agent in every state via their canonical homes (pointers route, never restate); D6 adversarial-by-construction with executed attack. | S Doctrines | S P (pointer) + R (rule) | O |
| S-028 | Stricter interpretation wins on doctrine conflict; doctrines never trade against velocity. | S Doctrines | S R Doctrines (kept) | O |
| S-029 | Agent roster: one job and boundary per agent; Orchestrator holds the whole picture. | S Roster | S P (pointer) + R roster | O |
| S-030 | How-to-use: locate state, load reference, run SLRs, honor gates as artifacts, think in PRs, fall back without shame. | S How-to-use | S P (kept) | O |
| S-031 | Global conventions: frozen charter, PR delivery, addressable outputs, global severity, machine-readable verdicts, escalate-never-guess, mandatory metrics, versioned canon + manifest binding, manifest-bound navigation. | S Conventions | S R Conventions (kept) | O |
| S-032 | Reference map: each reference is the canonical home of its instruments. | S Reference map | S P (kept) | O |
| S-033 | Canon changelog v2.0–v2.3 (historical rationale for each amendment). | S Changelog | S history.md (moved) | O |

## critic-critic-critic SKILL.md (G-001…)

| ID | Obligation | Old home | New home | Trigger |
|----|-----------|----------|----------|---------|
| G-001 | CCC is the taxonomy home; binders load this file + role references and do not restate them. | CCC intro | CCC intro + P (pointer) | C |
| G-002 | Spawning is a tool mechanic; it lives in the tool adapter, never here. | CCC intro | CCC intro + P (pointer) | O, C |
| G-003 | No Actor build phase by default; never merge roles into one pass. | CCC intro | CCC intro + P (pointer) | C |
| G-004 | review-only runs Critics in parallel with peer reports withheld; review-and-fix runs sequential until each Critic is CLEAN; merge order 1→2→3→4. | CCC intro | CCC intro + P (pointer) | C |
| G-005 | Four phases (quality+crates, safety/security, logic, claims/record); Critic-4 default-on for ledger-bearing units, opt-out only explicit. | CCC phase table | CCC intro (kept) | C |
| G-006 | Role prompts live in references/01–04 and are loaded per phase. | CCC references | CCC intro + P (pointer) | C |
| G-007 | Doctrines: context-break opening line, artifact evidence, no building, null-report coverage, concrete findings, evidenced rebuttals, gates still pass, honest labels, red-on-revert pins, specialization with HANDOFF-only glare. | CCC doctrines | CCC intro + P (pointer) | C |
| G-008 | Parse parameters (task, repo, dependency_repos, mode, max_cycles=2, severity_floor=S1, risk_tier, claims_critic, verify); ask only if ambiguous. | CCC Parameters | CCC P (pointer) + R (rule) | C |
| G-009 | Risk tiers (exempt/mechanical/standard/high) set CCC intensity; auto-detect from riskiest file; behavior-affecting ≥ standard; multi-step publish/commit → high. | CCC Risk tiers | CCC P (pointer) + R (rule) | C |
| G-010 | Severity scale S0–S3. | CCC Severity | CCC P (pointer) + R (rule) | C |
| G-011 | Absolute rules 1–13 (distinct phases, context break, parallel/sequential, evidenced findings/rebuttals, no secrets, repo contracts win, never weaken gates, mutation-proof tests, green≠convergence, dependency scope, crates contract, spawn contract). | CCC Absolute rules | CCC P (pointer) + R (rule) | C |
| G-012 | Spawn contract: Critics read+shell, never edit, always fresh; Fixer edits but never converges; shell for git/verify; scratch copy never live tree; role instructions in the prompt; commits carry repo identity. | CCC Spawn contract | CCC P (pointer) + R (rule) | O, C |
| G-013 | Crates/library attack contract in Critic-1 for library roots (thiserror, typed errors, locks, recursion, casts, tests, async); handoffs to Critic-2/3 at the boundary. | CCC Crates contract | CCC P (pointer) + R (rule) | C |
| G-014 | Convergence labels: CCC-CONVERGED vs TEST-GATED vs HALTED; never relabel TEST-GATED. | CCC Convergence | CCC P (pointer) + R (rule) | C |
| G-015 | Workflow: setup (params, contracts, dependency_repos, baseline, tier, slice charter); stop on ambiguous scope or exempt. | CCC Workflow | CCC P (pointer) + R (rule) | C |
| G-016 | Phases 1–3: context break, load reference, fresh subagent on the current diff, taxonomy + attestation + findings, skeptic/span/nulls, verdict; review-and-fix gates the next phase on CLEAN. | CCC Workflow | CCC P (pointer) + R (rule) | C |
| G-017 | Convergence checklist (artifacts, floor, dispositions, green, mutation-proof, enumeration, dependency_repos, logic + claims attestations); Critic-4 with CL-IDENTITY via %ae when on. | CCC Workflow | CCC P (pointer) + R (rule) | C |
| G-018 | Finding schema (id, severity, category, claim, evidence, disposition, rebuttal). | CCC Finding schema | CCC P (pointer) + R (rule) | C |
| G-019 | Final user report in the required shape. | CCC Final report | CCC P (pointer) + R (rule) | C |
| G-020 | Subagent guidance: role-shape table; adapter owns agent-type mapping; sequential hat-switches only when unspawnable, named as weaker. | CCC Subagent | CCC P (pointer) + R (rule) | O, C |
| G-021 | High tier prefers real subagents per Critic; no invented swarm. | CCC Subagent | CCC P (pointer) + R (rule) | O, C |
| G-022 | As the SEPMO Critic engine: CCC-CONVERGED never Delivery (maps into ledger + R7 runs); LIGHT never selects it; taxonomy mapping in the manifest; tunables bind in the manifest row. | CCC SEPMO engine | CCC R engine (kept) | O, C |
| G-023 | Anti-patterns (merged Critics, skipped crates contract, taxonomy re-runs, mislabeled convergence, hollow pins, weakened gates, bare pass, "unlikely", primary-only scope, shipping open S0/S1). | CCC Anti-patterns | CCC R anti-patterns (kept) | C |
| G-024 | Quick-start examples. | CCC Quick start | CCC P (pointer) + R (rule) | C |
| G-025 | Provenance (SEPMO derivation, crates contract source, 2026-08-12/25 history). | CCC Provenance | CCC history.md (moved) | C |

## Counts

101 AGENTS.md rows + 14 CLAUDE.md rows + 38 engineering-method rows + 33 SEPMO rows + 25 CCC
rows = **211 rows**, every one with a new home. Zero rows without a home: no HALT.

Trued up 2026-09-27 (round c1): E-001/002/003/009/016–030/037 and S-004/013–020/027/029 now
name the final Procedure/Reference/history homes; all other rows verified unchanged.

Trued up 2026-09-27 (round c2): G-001–021/024 name the final intro/Procedure/Reference homes
(G-005/022/023/025 verified unchanged). All 211 rows homed; zero unhomed.


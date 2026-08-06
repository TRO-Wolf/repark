# Phase-0 bootstrap brief — RePark V2

**Date:** 2026-08-06 · **Target:** the public repo `TRO-Wolf/repark` · **Status:** in execution

Phase 0 of the V2 port: **gates before code.** Every process asset that governs how work lands —
the testing contract, mechanical gates, map.md discipline, the agent contracts, SEPMO, tier-1
CI — is ported and running green *before* the first crate is ported. When phase 0 merges, the
repo enforces the same discipline as the private v1 repo, on an empty workspace.

Delivered as one branch (`phase-0/bootstrap`) with a five-commit series, verified by a review
panel, then pushed and PR'd by the orchestrator. Post-merge (not in this brief's scope): branch
protection with required checks; registry-side trusted-publisher configuration (maintainer
action, documented in `docs/release.md`).

## Context

- **V2 is a new engine, not "v1 plus features."** Native lazy DataFrame API + ANSI/Trino-style
  SQL door + a near-drop-in PySpark facade as a second door, over DataFusion + Arrow + the owned
  iceberg-rust fork. Positioning: *"Trino's SQL, DuckDB's deployment model, deepest Iceberg
  support."*
- **Port plan:** copy-then-re-home in four phases (0 bootstrap, 1 engine core, 2 the two SQL
  doors, 3 Python facade + parity = milestone one). v1 freezes to bugfix-only at milestone one.
  Public ≠ released: the API-forever clock starts at the first tagged PyPI release.
- **CI tiers:** tier 1 = every PR, no secrets, GitHub-hosted, read-only token (this brief);
  tier 2 = live AWS on merged code only, OIDC role, nightly + manual (later); tier 3 =
  benchmarks, no self-hosted runners (later).
- **Contribution policy:** source-open, contribution-gated. Issues welcome; external code PRs
  not accepted during the port. SEPMO / map.md / briefs fully enforced maintainer-side.

## Hard rules — every agent, no exceptions

1. **The v1 repo is READ-ONLY.** Read any file; never write, never `git switch`/`stash`/`commit`
   there, never run build/test commands that mutate its tree state beyond `target/`.
2. **No AWS.** Never run AWS CLI/SDK commands. Never set the AWS-acceptance or live-database
   env vars that arm gated test tiers.
3. **Forbidden content** in any staged or committed file: AWS account IDs, bucket ARNs or bucket
   names, connection strings/DSNs/credentials, brokerage or account identifiers, the operator's
   employer names, personal names/emails, absolute local paths, agent session IDs or session
   URLs. The exact grep list is in the brief's execution-local appendix (which itself never
   enters the repo).
4. **Attribution:** every commit message ends with exactly
   `Authored-By: Claude (claude-fable-5) <noreply@anthropic.com>` — nothing else. No co-author
   trailers, no session trailers, no session links in commits or the PR body.
5. **`cargo test --workspace`, NEVER `--all-features`. Never `--no-verify`.** These strings must
   also not appear in any ported Makefile/CI/doc as a recommended invocation.
6. **Adapt, don't invent.** When porting an asset, preserve v1's content and structure; change
   only what the V2 delta requires (repo name, paths, crate list, phase-0 reality, the deltas
   this brief names). If you are unsure whether something should change, keep v1's version and
   record the question in your `deviations` report. Do not improvise policy.
7. Staged files must be **complete and final** — no `TODO(port)`, no stub sections, no
   commented-out blocks.

## Repo-shape rules (bind everywhere)

- **Every directory gets a `map.md`** (`Purpose`, `Contents`, `I want to... → go to` table,
  `Pointers`, `## Debug`) — same contract as v1. `scripts/check_map_md.sh` is the oracle.
- The V2 delta that governance docs must reflect, beyond v1's rules:
  - **Two honest SQL doors, no blended parser.** Native `repark.sql()` = ANSI/Trino-style;
    facade `.sql()` = Spark dialect; Iceberg machinery shared beneath both; new SQL surface
    lands with both spellings + one test row per door.
  - **Entry-point matrix** as the central testing structure: native DataFrame, ANSI SQL, Spark
    facade are each a row for every behavior/divergence class.
  - **Server-prep disciplines from day one:** everything-through-Session (no global mutable
    state, no env reads at query time) and bindings-as-thin-adapter (one internal engine API;
    PyO3 and a future Flight SQL handler are both thin adapters).
  - **Tier-2 CI never runs against unmerged code.** No self-hosted runners.
  - Carried v1 invariants, verbatim in spirit: iceberg-rust fork owned (separate repo, never
    vendored, rev-pin + `[patch]`), DataFusion never forked; no PyIceberg in any form; no
    Sail/pysail; `unsafe_code = "forbid"` workspace-wide except `repark-python`; one DataFusion
    version pinned across the family; distribution deferred behind the `ExecutionBackend` seam.
- Target crate skeleton (documented as *target*, not yet present): `repark-core`, `repark-exec`,
  `repark-io`, `repark-iceberg`, `repark-connect`, `repark-sql`, `repark-spark`, `repark-ml`,
  `repark-python`; `python/repark` + `python/repark/spark`; `dbt-repark` as a separate package.

## Workstreams

Each workstream writes its deliverables under its own staging directory, mirroring repo-relative
paths. Read source assets from the v1 repo. Return a report: `files` (repo-relative), `notes`,
`deviations`.

### WS1 — toolchain, workspace scaffolding, mechanical gates

- `.gitignore` — extend the existing one (keep `.vscode/`): `target/`, `.venv/`, `__pycache__/`,
  `dist/`, `.ruff_cache/`, `.pytest_cache/`, plus whatever v1's ignores that applies. Staged file
  replaces the repo's current one.
- Port from v1 root, adapting only where needed: `rust-toolchain.toml` (verbatim),
  `rustfmt.toml`, `clippy.toml` (keep the disallowed-methods panic/spawn bans), `deny.toml`,
  `.taplo.toml`, `.typos.toml`, `.cargo/` config if present and portable (drop anything
  machine-local), `.python-version`.
- `Cargo.toml` — a workspace manifest with `members = []` (empty until phase 1), carrying v1's
  workspace lints (`unsafe_code = "forbid"` etc.), resolver, and profile sections that still
  make sense. `cargo test --workspace`, `cargo clippy`, `cargo fmt --check` must all run green
  on it. Generate `Cargo.lock`.
- Root `pyproject.toml` — carry v1's Ruff configuration (line 100 etc.); defer the uv workspace
  member list to phase 3 (no members yet). Only include tooling config that works today.
- `Makefile` — port from v1 **keeping v1's target names and tool-version pins**. Keep only
  targets whose subject exists in phase 0 (fmt/fmt-check, clippy/lint, test, `ci`, `verify`,
  `preflight`, `install-hooks`, the map.md check, taplo/typos/deny/zizmor/actionlint/workflow
  checks as v1 has them). Delete targets whose subject doesn't exist yet (census, parity,
  maturin/wheel, profiling, crate-DAG, lib-rs/lib-py checks) — no dead references anywhere.
  **Every kept target must run green in the V2 clone today** (empty workspace). `make ci` stays
  the canonical gate; `make verify` and `make preflight` keep their v1 meaning at phase-0 scope.
- `.pre-commit-config.yaml` + `scripts/`: port `check_map_md.sh` and `check_workflows_parse.py`;
  wire `make install-hooks` as in v1. Not ported now (phase 1+): `check_crate_dag.*`,
  `check_lib_rs.*`, `check_lib_py.*`, `run_census.sh`, `test_lock_gate.sh`,
  `generate_excel_fixtures.py` — record them as pointers in `scripts/map.md`.
- `CODEOWNERS` — one line: `* @TRO-Wolf`.
- `scripts/map.md`.
- Self-test: materialize your staging into a throwaway dir (`git init`; copy files), run
  `make ci` and each kept target there; fix until green. The integrator re-runs in the real
  clone.

### WS2 — governance contracts and ADRs

- `CLAUDE.md` + `AGENTS.md` — adapted from v1's pair (keep the semantic XML-tag regions device,
  the read-order, the single-home precedence chain in CLAUDE.md, the map.md mandate, testing
  pointer, the destructive-AWS-operations section, the sub-agent policy verbatim, and v1
  AGENTS.md's delegated-agent standing rules). Rewrite the identity/what-is sections for V2
  (two doors, crate skeleton as target, port phases with a pointer to `docs/port/PLAN.md`).
  PyO3 build notes carry over marked as applying from phase 3. The two files must not drift —
  same rules, AGENTS.md authoritative, CLAUDE.md the session-orientation view. Reference only
  make targets WS1 keeps (`make ci`, `make verify`, `make preflight`, `make install-hooks`).
- `PROJECT.md` — the V2 north star, sanitized for public: positioning line, goals (state the
  never-OOM goal honestly as pending a spill-coverage spike), differentiators, target crate
  skeleton, two doors, server-prep disciplines, distributed posture (fleet-parallel → server
  mode → distributed-if-needed), year-one priorities described as "the operator's production
  data-engineering workloads (Airflow + Iceberg + dbt)" — **never employer names** — and
  current state (phase 0).
- `CONTRIBUTING.md` — source-open, contribution-gated; issues/bug reports welcome; external code
  PRs not accepted during the port; maintainer-side process is SEPMO + briefs + map.md lockstep;
  point to SECURITY.md.
- `SECURITY.md` — adapt v1's: private vulnerability reporting via GitHub security advisories;
  pre-alpha, no supported versions yet.
- `docs/adr/` + `docs/adr/map.md`, four short ADRs (Context/Decision/Consequences, ~a page each):
  - `0001-own-iceberg-fork.md` — the fork is owned, separate, rev-pinned via `[patch]`; its
    `iceberg-datafusion` consumed as product surface; MERGE stays RePark-owned; DataFusion never
    forked. (Condenses v1 ADR-0002/0003; link the fork's ENGINE_CONTRACT.md by repo path, don't
    restate it.)
  - `0002-two-sql-doors.md` — ANSI/Trino-style native dialect + Spark-dialect facade; no blended
    parser; shared Iceberg machinery; dual-spelling rule for new surface.
  - `0003-copy-then-rehome-port.md` — the four phases, census multiset acceptance gate, v1
    freeze at milestone one, public ≠ released.
  - `0004-server-prep-disciplines.md` — everything-through-Session + bindings-as-thin-adapter;
    the three deferred server problems (credential vending, Python UDFs, resource policy);
    distribution deferred behind `ExecutionBackend`.

### WS3 — testing contract, port plan, task ledgers, this brief

- `docs/testing.md` — port v1's contract keeping every rule (tests-with-code hard block,
  test-per-change, divergence-class claims, calibration-per-domain, the forbidden list,
  relocation discipline — the port depends on it). Generalize v1-unit-specific examples only
  where they'd confuse; add the **entry-point matrix** (native DataFrame / ANSI SQL / Spark
  facade as rows for every behavior) as the day-one structure.
- `docs/port/PLAN.md` + `docs/port/map.md` — the port plan: copy-then-re-home rules (literal
  copy re-homed commit by commit; every intermediate state runnable), the four phases with
  their contents, the acceptance gate (census multiset byte-flat across repos: classic 135/345,
  expand 42/171, expand2 41/167, plus the full-extras facade count; tests port with their
  names), v1-freeze trigger, public ≠ released, and the cutover open item (single-writer-per-
  table during parallel-run).
- `briefs/phase-0-bootstrap.md` + `briefs/map.md` — this brief **with everything from the
  Appendix strip-marker down removed**, so the in-repo copy carries no local paths or literal
  grep patterns.
- `task/todo.md` — the phase-0 unit (this brief) plus the phase 1–3 backlog skeleton.
  `task/lessons.md` — seed with sanitized, date-stamped (2026-08-06) lessons carried from v1:
  tests-with-code is a hard block; `--all-features` breaks the PyO3 test binary (ban); Dependabot
  cargo PRs can bundle a safe bump with a breaking DataFusion-family bump — always split; map.md
  lockstep is part of every change; attribution format (rule 4). `task/map.md`.

### WS4 — SEPMO control plane and per-tier manuals

- `skills/sepmo/` — copy v1's tree (SKILL.md, `binding-manifest.template.md`, `references/`
  01–08, all map.md files) content-preserving; adapt only file paths/repo references.
  `binding-manifest.md` is **rewritten** to bind SEPMO to V2's files (CLAUDE.md, AGENTS.md,
  `docs/testing.md`, `task/todo.md`, `task/lessons.md`, `briefs/`). SEPMO cedes the engineering
  contract to those files, as in v1.
- `docs/skills/` + `docs/skills/map.md` — port `Opus.md`, `Sonnet.md`, `Haiku.md` adapting
  paths/repo names only; content preserved. `trait-wrapping.md` is code-specific — not ported
  now; record as a pointer (returns with phase 1).
- Scrub check: these files are the likeliest to carry v1-local references — verify none of the
  appendix-forbidden strings and no v1-only paths survive in your staged copies.

### WS5 — tier-1 CI and release-engineering docs

- `.github/workflows/` + its `map.md` and `.github/map.md`: adapt from v1 —
  - `ci.yml` — tier-1: rustfmt check, clippy `-D warnings`, `cargo test --workspace`, the
    map.md guard, workflow-parse check, Ruff if v1's ci runs it. Top-level
    `permissions: contents: read`; every action SHA-pinned (carry v1's pins); concurrency
    group; triggers `pull_request` + `push` to `main`. **No `pull_request_target` anywhere.**
  - `zizmor.yml`, `typos.yml`, `taplo.yml`, `cargo-deny.yml` — port, adapted to the empty
    workspace.
  - NOT ported now: `parity-live.yml`, `benches.yml`, `tpch-sf1.yml`, `wheels.yml`,
    `codeql.yml`, `pip-audit.yml`, `cache-warm.yml` (tier-2/3 and later phases) — record as
    pointers in `workflows/map.md`.
  - Every ported workflow must pass on the phase-0 tree. No secrets referenced by any of them.
- `.github/dependabot.yml` — port v1's; add a comment noting the DataFusion-family rule (never
  merge a bundled DF/arrow major bump; split it).
- `docs/release.md` — release engineering, documentation only this phase: PyPI trusted
  publishing setup (project `repark`, owner TRO-Wolf, the exact "add a publisher" steps, with
  the release workflow named `release.yml` for when it exists), crates.io Trusted Publishing
  equivalent, the note that the two bootstrap upload tokens are to be revoked, and that
  `release.yml` itself lands at the first tagged release (not before). Open items listed: wheel
  matrix, abi3, Python floor, cadence, signing.

## Assembly (integrator)

1. Preconditions: the V2 clone is clean, on `main` at `906b66b`. Abort with a deviation report
   if not. Create `git switch -c phase-0/bootstrap`.
2. Copy the workstream staging directories into the clone (union). Any path collision between
   workstreams is an error — resolve only if trivial, otherwise report and stop.
3. Author the shared maps: root `map.md`, `docs/map.md`, and any directory still missing one.
4. Run the gates in the clone: `make ci`, `make verify`, `make preflight`,
   `bash scripts/check_map_md.sh`, `make install-hooks`. Fix mechanical breakage (broken links,
   missing map sections, formatting); anything substantive goes in `deviations`.
5. Commit series — five commits, staged so each passes the pre-commit guard, root `map.md`
   created in commit 1 and updated in later commits as directories appear, each message ending
   with the rule-4 trailer:
   1. `chore: toolchain, workspace scaffolding, and mechanical gates` (WS1)
   2. `docs: governance contracts and ADRs` (WS2)
   3. `docs: testing contract, port plan, task ledgers, phase-0 brief` (WS3)
   4. `docs: SEPMO control plane and per-tier manuals` (WS4)
   5. `ci: tier-1 workflows and dependabot; release-engineering docs` (WS5)
6. **Do not push. Do not open a PR.** The orchestrator does both after verification.

## Verification (panel of four, on the assembled branch)

- **V-A consistency.** CLAUDE.md ↔ AGENTS.md carry the same rules; the precedence chain has one
  home; every intra-repo link resolves to a real file; every make target / script referenced in
  any doc or workflow exists; Makefile tool pins match workflow pins; no v1-only leakage (v1
  repo name, crates asserted as present, dead references to unported assets).
- **V-B security.** Run the appendix grep list over the working tree AND `git log -p main..HEAD`
  (zero hits required); every action SHA-pinned; workflow permissions read-only; no
  `pull_request_target`; no secrets referenced; zizmor clean; dependabot config sane.
- **V-C gates execute.** In the clone: run every target the Makefile advertises, plus `make ci`,
  `make verify`, `make preflight`, `bash scripts/check_map_md.sh`, hook installation — all
  green. Confirm by grep that no file recommends `--all-features` or `--no-verify`.
- **V-D completeness.** Diff the tree against this brief's deliverable lists — anything missing
  or extra; `map.md` present in every directory with the required sections; the five commits
  match the plan with exact trailers and nothing else (no co-author trailers, no session lines);
  the in-repo brief copy contains nothing from the appendix down; `task/` seeds present.

Findings come back as `{file, summary, failure_scenario, severity}`. A fixer agent applies
confirmed findings, re-runs `make ci` + the map guard, and lands one additional commit
(`chore: address phase-0 verification findings`) with the rule-4 trailer. Rejected findings are
reported with reasons, not silently dropped.

## Out of scope — do not build in this phase

Crate code of any kind; census machinery; `check_crate_dag`/`check_lib_rs`/`check_lib_py`;
tier-2/tier-3 workflows; `release.yml`; wheels; CodeQL; issue templates; branch protection
(orchestrator, post-merge); registry-side trusted-publisher configuration (maintainer action).

## Acceptance

All gates green locally on `phase-0/bootstrap`; panel findings fixed or explicitly rejected
with reasons; commit series clean; branch ready for the orchestrator to push and PR.

# todo

In-flight work and the port backlog. Check items off as they complete; verify against source
before scoping (checkboxes can go stale — see `lessons.md`). The port's authoritative phase
definitions and acceptance gate live in [../docs/port/PLAN.md](../docs/port/PLAN.md); this file
tracks execution state only.

## Phase 0 — bootstrap (IN FLIGHT, brief: [../briefs/phase-0-bootstrap.md](../briefs/phase-0-bootstrap.md))

Gates before code: process assets ported and green on an empty workspace.

- [x] Repo bootstrap (public repo live, `main` default, Apache-2.0, README + .gitignore,
      security settings verified: secret scanning + push protection, fork-PR approval,
      read-only default workflow token) — done 2026-08-06, pre-brief.
- [ ] WS1 — toolchain, workspace scaffolding, mechanical gates (empty-workspace `Cargo.toml`,
      Makefile with kept v1 targets green today, `check_map_md.sh` + pre-commit, CODEOWNERS).
- [ ] WS2 — governance contracts (CLAUDE.md / AGENTS.md / PROJECT.md / CONTRIBUTING.md /
      SECURITY.md) + ADRs 0001–0004.
- [ ] WS3 — testing contract, port plan, task ledgers, in-repo brief.
- [ ] WS4 — SEPMO control plane + per-tier manuals, binding-manifest rewritten for V2.
- [ ] WS5 — tier-1 CI workflows + dependabot + `docs/release.md`.
- [ ] Assembly: five-commit series on `phase-0/bootstrap`; all gates green; panel verification;
      findings fixed or rejected with reasons.
- [ ] Post-merge (orchestrator/maintainer, outside the brief): branch protection with required
      checks; registry-side trusted-publisher configuration per `docs/release.md`.

## Phase 1 — engine core (BACKLOG)

- [ ] Literal copy of v1 engine crates; re-home commit by commit (every intermediate state
      runnable; relocation discipline per `docs/testing.md`).
- [ ] `repark-core` — the Session-centric internal engine API (the one deliberate design pass):
      everything-through-Session, bindings-as-thin-adapter seams.
- [ ] `repark-iceberg` — from v1's catalog + write crates; fork rev-pin via `[patch]`.
- [ ] Rust unit-test tier ports green (`cargo test --workspace`; MemoryCatalog, no AWS).
- [ ] Re-arm the phase-1+ mechanical gates not ported in phase 0: crate-DAG guard, lib.rs/lib.py
      thinness checks, `trait-wrapping.md` manual.

## Phase 2 — the two SQL doors (BACKLOG)

- [ ] `repark-spark` — Spark-dialect door, ported.
- [ ] `repark-sql` — ANSI/Trino-style native dialect; Iceberg DDL design pass.
- [ ] Dual-spelling rule live: new SQL surface lands with both spellings + one test row per door.
- [ ] `dbt-repark` may start in parallel once this phase lands (separate package).

## Phase 3 — Python facade + parity = milestone one (BACKLOG)

- [ ] `repark-python` thin adapter + PySpark facade; PyO3/maturin build surface returns
      (boundary real-artifact test rule arms — `docs/testing.md`).
- [ ] Parity harness + census machinery port; uv workspace members land.
- [ ] Acceptance gate: census multiset byte-flat across repos (classic 135/345, expand 42/171,
      expand2 41/167, plus the full-extras facade count) — see `docs/port/PLAN.md`.
- [ ] Tier-2 CI (live AWS, merged code only, OIDC) + live oracle tier.
- [ ] v1 freezes to bugfix-only at acceptance; first tagged PyPI release gated on milestone one
      (`docs/release.md`).

## Open items

- [ ] Cutover sequencing during parallel-run (single-writer-per-table) — settle before
      milestone one (`docs/port/PLAN.md` "Open item: cutover").
- [ ] Never-OOM goal pending a spill-coverage spike (PROJECT.md).

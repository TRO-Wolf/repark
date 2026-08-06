# todo

In-flight work and the port backlog. Check items off as they complete; verify against source
before scoping (checkboxes can go stale — see `lessons.md`). The port's authoritative phase
definitions and acceptance gate live in [../docs/port/PLAN.md](../docs/port/PLAN.md); this file
tracks execution state only.

## Phase 0 — bootstrap (DONE 2026-08-06, brief: [../briefs/phase-0-bootstrap.md](../briefs/phase-0-bootstrap.md))

Gates before code: process assets ported and green on an empty workspace.

- [x] Repo bootstrap (public repo live, `main` default, Apache-2.0, README + .gitignore,
      security settings verified: secret scanning + push protection, fork-PR approval,
      read-only default workflow token) — done 2026-08-06, pre-brief.
- [x] WS1 — toolchain, workspace scaffolding, mechanical gates (empty-workspace `Cargo.toml`,
      Makefile with kept v1 targets green today, `check_map_md.sh` + pre-commit, CODEOWNERS).
- [x] WS2 — governance contracts (CLAUDE.md / AGENTS.md / PROJECT.md / CONTRIBUTING.md /
      SECURITY.md) + ADRs 0001–0004.
- [x] WS3 — testing contract, port plan, task ledgers, in-repo brief.
- [x] WS4 — SEPMO control plane + per-tier manuals, binding-manifest rewritten for V2.
- [x] WS5 — tier-1 CI workflows + dependabot + `docs/release.md`.
- [x] Assembly: five-commit series on `phase-0/bootstrap`; all gates green; panel verification;
      findings fixed or rejected with reasons — merged 2026-08-06.
- [x] Post-merge (orchestrator/maintainer): branch protection with required checks live on
      `main`. Registry-side trusted-publisher configuration stays deferred to the first release
      (`docs/release.md`).

## Phase 1 — engine core (IN FLIGHT, brief: [../briefs/phase-1-engine-core.md](../briefs/phase-1-engine-core.md), design: [../docs/design/session-api.md](../docs/design/session-api.md))

Design settled + port-source pinned 2026-08-06 (v1 `main` @ `fc3f48102`). Three sequential PRs,
copy-then-re-home, every commit green; deferred tests tracked in
[port/deferred-tests.md](port/deferred-tests.md).

- [ ] **PR-A — workspace arming + repark-common + gates (IN FLIGHT,
      ledger: [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md))**:
      `[workspace.dependencies]` pins, `crates/repark-common` (error seed, 2 tests),
      CARGO_EMPTY guard removal, crate-DAG + lib-rs gates with provocation proofs,
      audit.yml workflow returns (cache-warm.yml deferred to PR-B together with the ci.yml
      rust-cache restore steps — see `.github/workflows/map.md`), design doc + this slate's
      docs in-repo.
- [ ] **PR-B — `repark-iceberg` (IN FLIGHT — assembled 2026-08-06, branch `phase-1/pr-b`,
      ledger: [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md))**: fork
      `[patch.crates-io]` pin + fork-pin proof test; v1 catalog → `src/catalog/`, v1 write →
      `src/write/`; declared-rename unit, 241 ported tests (catalog 50 + write 191; corrected
      from the brief's grep-based 243 — `--list` at the pin is ground truth) under the
      generated rename map, diff empty; forced-edit class 6 shared tracing harness; awaiting
      panel verification + orchestrator carve-outs (cache-warm.yml / ci.yml cache steps).
- [ ] PR-C — `repark-core` (QUEUED, after PR-B): v1 repark-session re-homed (Session,
      builder, two-phase lifecycle), the three repark-sql hoists, the `SqlDialect` /
      `SessionExtension` seams + seam tests, the four forced edits (E-2, dialect inversion,
      extension inversion, E-4), session-test audit + deferred manifest reconciliation.
- [ ] Phase close: acceptance per the brief §4 (gates armed + provocation proofs recorded,
      census subset reconciles, omissions ledger in place); dated retrospective note here per
      SEPMO.

Note (2026-08-06): the earlier "re-arm the phase-1+ mechanical gates" line item mislabeled
`check_lib_py` as phase 1 — the Python-thinness gate returns with the Python surface in
**phase 3**. Phase 1 re-arms the Rust gates only (crate-DAG guard, `check_lib_rs`,
`trait-wrapping.md` manual).

## Phase 2 — the two SQL doors (BACKLOG)

- [ ] `repark-spark` — Spark-dialect door, ported (implements `SqlDialect` + the
      `SessionExtension` with v1's registration code).
- [ ] `repark-sql` — ANSI/Trino-style native dialect; Iceberg DDL design pass.
- [ ] Dual-spelling rule live: new SQL surface lands with both spellings + one test row per door.
- [ ] `dbt-repark` may start in parallel once this phase lands (separate package).

## Phase 3 — Python facade + parity = milestone one (BACKLOG)

- [ ] `repark-python` thin adapter + PySpark facade; PyO3/maturin build surface returns
      (boundary real-artifact test rule arms — `docs/testing.md`); `check_lib_py` gate returns
      with it.
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
- [ ] ci.yml detect classifier deferred until rust jobs are actually slow — returns when
      rust-test exceeds ~3 min (recorded in `.github/workflows/map.md`).

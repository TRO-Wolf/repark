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

## Phase 1 — engine core (DONE 2026-08-07, brief: [../briefs/phase-1-engine-core.md](../briefs/phase-1-engine-core.md), design: [../docs/design/session-api.md](../docs/design/session-api.md))

Design settled + port-source pinned 2026-08-06 (v1 `main` @ `fc3f48102`). Three sequential PRs,
copy-then-re-home, every commit green; deferred tests tracked in
[port/deferred-tests.md](port/deferred-tests.md).

- [x] **PR-A — workspace arming + repark-common + gates (MERGED 2026-08-07, PR #3 `5eba40a`,
      ledger: [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md))**:
      `[workspace.dependencies]` pins, `crates/repark-common` (error seed, 2 tests),
      CARGO_EMPTY guard removal, crate-DAG + lib-rs gates with provocation proofs,
      audit.yml workflow returns (cache-warm.yml deferred to PR-B together with the ci.yml
      rust-cache restore steps — see `.github/workflows/map.md`), design doc + this slate's
      docs in-repo.
- [x] **PR-B — `repark-iceberg` (MERGED 2026-08-07, PR #4 `4e3887b`,
      ledger: [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md))**: fork
      `[patch.crates-io]` pin + fork-pin proof test; v1 catalog → `src/catalog/`, v1 write →
      `src/write/`; declared-rename unit, 241 ported tests (catalog 50 + write 191; corrected
      from the brief's grep-based 243 — `--list` at the pin is ground truth) under the
      generated rename map, diff empty; forced-edit class 6 shared tracing harness; orchestrator
      carve-outs LANDED on the branch as `340211a` (cache-warm.yml + ci.yml rust-cache
      restore steps); panel-verified, merged.
- [x] **PR-C — `repark-core` (MERGED 2026-08-07, PR #6 `c05bc31` — resubmission of #5, which
      GitHub auto-closed when the `phase-1/pr-b` base branch was deleted at #4's merge;
      ledger: [p1c-repark-core-ledger.md](p1c-repark-core-ledger.md))**: v1 repark-session
      re-homed (Session, builder, two-phase lifecycle), the three repark-sql hoists, the
      `SqlDialect` / `SessionExtension` seams + seam tests, the four forced edits (E-2,
      dialect inversion, extension hooks, E-4 `TempFallbackAllowed { root }`); session-test
      audit landed: 68 port-now + 18 deferred (= 86 at the pin, manifest reconciled),
      workspace `--list` 321 (244 PR-B + 68 + 2 hoisted + 7 new seam/gate tests), zero
      `#[ignore]`; panel-verified, merged.
- [x] Phase close: acceptance per the brief §4 (gates armed + provocation proofs recorded,
      census subset reconciles, omissions ledger in place); retrospective below.

### Retrospective (2026-08-07, per SEPMO)

Three PRs, all merged 2026-08-07: #3 (`5eba40a`), #4 (`4e3887b`), #6 (`c05bc31`). Full
workspace green at close: 322 tests, zero `#[ignore]`; the census discipline held end-to-end —
the rename map was generated from `cargo test --list` at the pin, and every PR's name-by-name
sorted diff came back empty. The 18 deferred session tests are named with their phase-2
blockers in [port/deferred-tests.md](port/deferred-tests.md) (phase 2's completeness
checklist). **What worked:** verification tiers caught real defects at every level — the
assembly STOP found the two-global-tracing-subscriber collision (ruled forced-edit class 6,
shared `cfg(test)` harness), the PR-A panel caught stale phase-0 governance claims, and the
PR-C design-conformance lens caught the missing `#[doc(hidden)]` on the `testing_` seams and a
missing E-2 signal class on the late-catalog path. Stacked branches + pre-staged assembly
carried the work through a ~7-hour GitHub Actions outage with zero rework. The
`cache-warm.yml` / `ci.yml` rust-cache pairing proved out: Rust job 8m02s cold → 55s warm.
**What hurt** (rules now in [lessons.md](lessons.md) 2026-08-07): a CI job rename silently
broke branch protection's required contexts (#3 blocked green); path-filtered required checks
made PRs structurally unmergeable — zizmor blocked #6, then cargo-deny + taplo blocked the
close-out #7 itself, the first docs-only diff (fixed in this close-out: all three are now
always-run on PRs); deleting a stacked PR's base branch auto-closed the
dependent PR unrecoverably (#5 → resubmitted as #6).

Note (2026-08-06): the earlier "re-arm the phase-1+ mechanical gates" line item mislabeled
`check_lib_py` as phase 1 — the Python-thinness gate returns with the Python surface in
**phase 3**. Phase 1 re-arms the Rust gates only (crate-DAG guard, `check_lib_rs`,
`trait-wrapping.md` manual).

## Phase 2 — the two SQL doors (IN FLIGHT, brief: [../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md), design: [../docs/design/sql-doors.md](../docs/design/sql-doors.md))

Design settled 2026-08-07 (delegate-first, no shared-lowering crate); port-source pin unchanged
(v1 `main` @ `fc3f48102`). Seven PRs; deferred-test obligations close per
[port/deferred-tests.md](port/deferred-tests.md).

- [ ] **PR-1 — repark-functions + docs (IN FLIGHT)**: verbatim port (crate name kept, 62-test
      battery, identity census map), DAG TIERS rows pre-declared for all four new crates,
      design doc + brief in-repo (ledger: [p2a-functions-ledger.md](p2a-functions-ledger.md)).
- [ ] **PR-2 — repark-spark skeleton (IN FLIGHT)**: router spine + guards + time-travel
      scanner + `SparkDialect` + `SparkExtension`; DF-54.1 guard hoist rides (G8 — the guard
      sits in core `build()` since PR-C; PR-2 adds the bare-Session pin). Unblocks deferred #1
      (ledger: [p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md)).
- [ ] **PR-3a — repark-spark DDL (IN FLIGHT)**: ctas, create_table, namespace_ddl,
      catalog_ops, local_fs_ddl, alter. Unblocks the CTAS-blocked deferred rows (#2, #4–#7)
      (ledger: [p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md)).
- [ ] **PR-3b — repark-spark DML + refs (IN FLIGHT)**: merge, insert_overwrite, ref_ddl, call
      + MoR-valve hoist; census closed (334 ported names empty sorted-diff under
      `repark_sql::` → `repark_spark::`; 342 − 6 postgres_p11 − 2 phase-1 time-travel hoists).
      Deferred #3 landed (ledger: [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md)).
- [ ] **PR-4 — repark-ta (IN FLIGHT)**: kernels + goldens (148 `.bin`) ported verbatim +
      NEW `TaExtension`; `SparkExtension` composes it at v1's registration position (p2b rider
      #1 DISCHARGED). TA census generated at the pin and empty-diff (146/146 default features;
      178→180 with `--features datafusion`, +2 = the door-native `TaExtension` tests).
      Deferred #8–#14 landed as `repark-spark/tests/ta_window.rs` (7/7 green) — the deferred
      manifest is now exactly 4 rows, all post-milestone-one. Ledger:
      [p2e-ta-ledger.md](p2e-ta-ledger.md). Rider: the ANSI TA smoke + non-literal-period
      refuse rows (design Q11 toll) land PR-6 — `repark-sql` does not exist yet.
- [ ] **PR-5 — repark-sql ANSI M1**: `AnsiDialect` delegation core, guard set, wrong-door
      sniff, CTAS `WITH (…)` vocab + `extra_properties` + Q15 routing, schema DDL, surfaces
      registry + matrix seeded; R1/R2 spikes day 1. May start once PR-1 merges.
- [ ] **PR-6 — repark-sql ANSI M2**: ALTER evolution, MERGE lowering, `FOR … AS OF` scanner +
      pin set, branch/tag ALTER DDL, full refuse set, cross-door two-session equivalence rows,
      matrix completion, session-api.md seam-freeze edits.
- [ ] Phase close: acceptance per the brief §3; `dbt-repark` may start after PR-6
      (separate package).

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

## Post-milestone-one (BACKLOG)

- [ ] `repark-postgres` + `repark-excel` — read connectors (v1 `read_postgres` / `read_excel`
      surfaces); explicitly scheduled post-milestone-one (decision 2026-08-07, recorded in
      [../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) §4). The 4 deferred-test
      manifest rows re-point here ([port/deferred-tests.md](port/deferred-tests.md)).

## Open items

- [ ] Cutover sequencing during parallel-run (single-writer-per-table) — settle before
      milestone one (`docs/port/PLAN.md` "Open item: cutover").
- [ ] Never-OOM goal pending a spill-coverage spike (PROJECT.md).
- [ ] ci.yml detect classifier deferred until rust jobs are actually slow — returns when
      rust-test exceeds ~3 min (recorded in `.github/workflows/map.md`).

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

## Phase 2 — the two SQL doors (DONE 2026-08-08, brief: [../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md), design: [../docs/design/sql-doors.md](../docs/design/sql-doors.md))

Design settled 2026-08-07 (delegate-first, no shared-lowering crate); port-source pin unchanged
(v1 `main` @ `fc3f48102`). Seven PRs; deferred-test obligations close per
[port/deferred-tests.md](port/deferred-tests.md).

- [x] **PR-1 — repark-functions + docs (MERGED #8)**: verbatim port (crate name kept, 62-test
      battery, identity census map), DAG TIERS rows pre-declared for all four new crates,
      design doc + brief in-repo (ledger: [p2a-functions-ledger.md](p2a-functions-ledger.md)).
- [x] **PR-2 — repark-spark skeleton (MERGED #9)**: router spine + guards + time-travel
      scanner + `SparkDialect` + `SparkExtension`; DF-54.1 guard hoist rides (G8 — the guard
      sits in core `build()` since PR-C; PR-2 adds the bare-Session pin). Unblocks deferred #1
      (ledger: [p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md)).
- [x] **PR-3a — repark-spark DDL (MERGED #10)**: ctas, create_table, namespace_ddl,
      catalog_ops, local_fs_ddl, alter. Unblocks the CTAS-blocked deferred rows (#2, #4–#7)
      (ledger: [p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md)).
- [x] **PR-3b — repark-spark DML + refs (MERGED #11)**: merge, insert_overwrite, ref_ddl, call
      + MoR-valve hoist; census closed (334 ported names empty sorted-diff under
      `repark_sql::` → `repark_spark::`; 342 − 6 postgres_p11 − 2 phase-1 time-travel hoists).
      Deferred #3 landed (ledger: [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md)).
- [x] **PR-4 — repark-ta (MERGED #12)**: kernels + goldens (148 `.bin`) ported verbatim +
      NEW `TaExtension`; `SparkExtension` composes it at v1's registration position (p2b rider
      #1 DISCHARGED). TA census generated at the pin and empty-diff (146/146 default features;
      178→180 with `--features datafusion`, +2 = the door-native `TaExtension` tests).
      Deferred #8–#14 landed as `repark-spark/tests/ta_window.rs` (7/7 green) — the deferred
      manifest is now exactly 4 rows, all post-milestone-one. Ledger:
      [p2e-ta-ledger.md](p2e-ta-ledger.md). Rider: the ANSI TA smoke + non-literal-period
      refuse rows (design Q11 toll) land PR-6 — `repark-sql` does not exist yet. The Spark
      door's `TA_FUNCTIONS` matrix row flipped `DeliberatelyAbsent` → `Tested` in PR-5's
      sync merge (the row rode PR-5's matrix machinery).
- [x] **PR-5 — repark-sql ANSI M1 (MERGED #13)**: `AnsiDialect` delegation core, guard set
      (multi-statement FIRST, P11, SEC-02, write-to-branch), wrong-door sniff, CTAS `WITH (…)`
      vocab + `extra_properties` + Q15 loud-refuse routing, schema DDL; `repark_common::surfaces`
      (43 capability IDs) + `matrix.rs` in BOTH doors with the compile-run audit — Spark 39
      tested / 4 absent (TA_FUNCTIONS flipped in the sync), ANSI M1 29 tested / 14 absent (delegated `INSERT`/`DELETE`/`UPDATE` ship
      with M1 because the delegation core ships them, each with a round-trip row and the BUG-001
      MoR valve wired over them); R1/R2 spikes recorded day 1
      (ledger: [p2f-ansi-m1-ledger.md](p2f-ansi-m1-ledger.md)). **R2 filed a core gap —
      RESOLVED in PR-6:** `ReparkSession` could not enable `information_schema` (the builder
      config map never reached `SessionConfig`), so `SHOW TABLES`/`DESCRIBE` were dead in BOTH
      doors and Q8's "delegate" delegated to nothing. Fixed core-side by
      `repark_core::session::apply_datafusion_config_keys` (every `datafusion.*` builder key now
      reaches `SessionConfig`; unknown key = loud `Error::Config`); Q8 is delivered, with the
      `$`-metadata-table filtering question carried forward as an open fork/core rider — see
      [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md).
- [x] **PR-6 — repark-sql ANSI M2 (MERGED #14)**: ALTER evolution +
      `SET PROPERTIES` + `RENAME TO`, MERGE lowering, `FOR … AS OF` scanner + the double-quote
      pin set, branch/tag ALTER DDL (Q6 — NOT deferred; the SCOPE contingency never fired), the
      full refuse set (`INSERT OVERWRITE` / `CALL` / `ALTER … EXECUTE` / `TRUNCATE`), the Q11 TA
      toll, Q8 introspection (unblocked by the R2 core fix above), and the Q13/G5 **two-session**
      cross-door rows (CTAS, INSERT, ALTER, MERGE, time travel, case folding — each running a
      native ANSI session against a Spark-extended Spark session, compared on the Arrow path).
      Matrix: ANSI 39 tested / 4 absent (ten rows flipped; the `M2` const deleted — every
      remaining absence is a standing ruling: Q3, Q9, TRUNCATE, Q7), Spark 40 tested / 3 absent
      (`CROSS_DOOR_EQUIVALENCE` flipped). Plus the `session-api.md` seam freeze (UNSTABLE →
      FROZEN + the extensions-are-session-scoped line) and the ADR-0002 design-pass discharge
      note. Ledger: [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md).
- [x] Phase close: acceptance per the brief §3 met (program census reconciled below; matrix
      audits green both doors; manifest at 4 post-milestone-one rows; `make preflight` green);
      `dbt-repark` may now start (separate package). Retrospective below.

### Retrospective (2026-08-08, per SEPMO)

Seven PRs (#8–#14), all merged 2026-08-07 → 2026-08-08. Workspace at close: **1170 tests, 0
failed, 0 ignored** across seven crates. **Program census reconciles and foots** (brief §3
identity): v1 pin names 342 (repark-sql) + 62 (repark-functions) + 146 (repark-ta, default
features) = 550; ported 334 (repark-spark) + 62 + 146 + 2 (time-travel parser pins, attributed
to repark-core where phase 1 hoisted them) = 544; unported = exactly the 6 `postgres_p11`
names, scheduled with their crate post-milestone-one. Surface matrices: ANSI 39 Tested / 4
Absent, Spark 40 / 3 — every absence a standing ruling with its design/ADR citation, every
cited test mechanically verified against `--list`. Both seams FROZEN (session-api.md), the
ADR-0002 design-pass obligation discharged.
**What worked:** the recon → design-competition → synthesis pipeline priced the architecture
correctly (delegate-first kept every port census empty-diff while the ANSI door stayed ~thin);
verification tiers matched risk — slim (builder + one adversarial verifier) sufficed for the
three mechanical ports, while the full 4-lens panel caught HIGH defects on ALL THREE new-code
PRs (the `$`-passthrough silent-MemTable hole, false-absence matrix rows over live delegated
DML with the MoR valve unwired, the EXECUTE-recognizer false refusals, branch-DDL trailing-
token acceptance, and the time-travel temp-view leak — each empirically reproduced before
fixing, each fix reproduction-pinned); sibling-PR parallelism (PR-4 ∥ PR-5) worked because the
crate footprints were disjoint by design; the two sync recipes (stacked-after-squash vs
sibling-union) held. Day-1 spikes validated their existence: R2 found a real core gap
(`information_schema` unreachable) early enough to fix in-phase.
**What hurt** (rules in [lessons.md](lessons.md) 2026-08-08): typed absence rows can be false
— the audit proves absence is *recorded*, not *true*; ephemeral provider lifecycle (the
time-travel view leak — ANSI fixed, Spark-door inherits v1's copy as tracked debt below).

## Phase 3 — Python facade + parity = milestone one (IN PROGRESS 2026-08-08)

Design SETTLED 2026-08-08 (competition-synthesized): `docs/design/python-facade.md`; brief:
`briefs/phase-3-python-facade.md`. The seven-PR slate (order 1 → 2 → (3 ∥ 4) → 5 → 6 → 7):

- [ ] PR-1 arming — design + brief in-repo; crate-DAG tier rows (`repark-ml` 3, `repark-python`
      4 "bindings") pre-declared; rust CI job split (lint/test, setup-python, free-disk);
      testing.md row-2 spelling note; dialect doc rider. Ledger: `task/p3a-arming-ledger.md`.
- [ ] PR-2 `repark-ml` — verbatim, identity census. Ledger: `task/p3b-ml-ledger.md`.
- [ ] PR-3 `crates/repark-python` — door wiring + dep collapse + refuse-arms + EngineRuntime +
      edit classes EC-1/2/3/5/6/10. Ledger: `task/p3c-binding-ledger.md`.
- [ ] PR-4 `python/repark-parity` + census foundation — comparator, additive `--classic`,
      v1-pin baseline + stability self-diff. Ledger: `task/p3d-parity-ledger.md`.
- [ ] PR-5 `python/repark` facade + suite + wheels.yml (real-artifact rule discharges here);
      EC-4 deferral ledger + EC-9 hygiene ledger. Ledger: `task/p3e-facade-ledger.md`.
- [ ] PR-6 tier-2 CI — parity-live armed + net-new `aws-acceptance.yml` (OIDC, env-gated,
      no-delete IAM). Ledger: `task/p3f-tier2-ledger.md`.
- [ ] PR-7 phase close — v2 census run, comparator ×4, reconciliation, PLAN re-baseline,
      retrospective, operator cutover note. Ledger: `task/p3g-close-ledger.md`.

Standing acceptance rows (discharged across the slate):

- [ ] `repark-python` thin adapter + PySpark facade; PyO3/maturin build surface returns
      (boundary real-artifact test rule arms — `docs/testing.md`); `check_lib_py` gate returns
      with it.
- [ ] Parity harness + census machinery port; uv workspace members land.
- [ ] Acceptance gate: census multiset byte-flat across repos — baselines re-recorded at the
      pin by the PR-4 procedure (the historical PLAN.md numbers were stale; see
      `docs/design/python-facade.md` §5 F2) — plus the full-extras facade cohort, defined in
      the design §6.3.
- [ ] Tier-2 CI (live AWS, merged code only, OIDC) + live oracle tier.
- [ ] v1 freezes to bugfix-only at acceptance; first tagged PyPI release gated on milestone one
      (`docs/release.md`).

## Post-milestone-one (BACKLOG)

- [ ] `repark-postgres` + `repark-excel` — read connectors (v1 `read_postgres` / `read_excel`
      surfaces); explicitly scheduled post-milestone-one (decision 2026-08-07, recorded in
      [../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) §4). The 4 deferred-test
      manifest rows re-point here ([port/deferred-tests.md](port/deferred-tests.md)).

## Open items

- [ ] **Spark-door time-travel temp-view leak (inherited from v1)** — `repark-spark`'s
      time_travel apply-half registers pinned views and never deregisters (v1's own behavior at
      the pin). The ANSI door fixed its copy in PR-6 (`PinnedViews` released on every exit
      path); the Spark door's fix is a **declared divergence-with-issue** (port fidelity says
      never fix silently): apply the same release idiom + a divergence note, ideally alongside
      the matching v1 bugfix. Found by the PR-6 verify panel (p2g ledger).
- [ ] **`$`-metadata-table filtering in introspection** — carried forward as an open fork/core
      rider from PR-6's Q8 delivery (see p2g ledger).

- [ ] Cutover sequencing during parallel-run (single-writer-per-table) — settle before
      milestone one (`docs/port/PLAN.md` "Open item: cutover").
- [ ] Never-OOM goal pending a spill-coverage spike (PROJECT.md).
- [ ] ci.yml detect classifier deferred until rust jobs are actually slow — returns when
      rust-test exceeds ~3 min (recorded in `.github/workflows/map.md`).

# Promotion ledger — what every archived file still binds, and where that rule lives now

**Date:** 2026-08-09 · **Unit:** Front-Door FD-4 · **Design:**
[../frontdoor/agent-agnostic-frontdoor.md](../frontdoor/agent-agnostic-frontdoor.md) §7 ·
**Slate:** [../frontdoor/frontdoor-campaign.md](../frontdoor/frontdoor-campaign.md) "FD-4"

This is the audit that made the archival in this directory **lossless**. Every file moved here was
read end to end; every rule, constraint, standing decision and lesson it carries was classified; and
anything still in force that had **no** home outside the archived file was promoted into a current
document **before** the move.

## The reconciliation identity (design §7)

> Every rule R in an archived source either (a) already lives in an authoritative current document,
> (b) was promoted into one by this unit, (c) has been superseded by a later rule that does, or
> (d) is a dated historical claim that binds nothing going forward.
>
> **No active rule is reachable only through an archived file.**

## Dispositions used below

| Disposition | Meaning |
|---|---|
| **HOMED** | Already stated in a current authoritative document; the archived copy is a duplicate record, not the source. |
| **PROMOTED** | Was reachable only from the archived file; this unit copied it into the named current document (see [§ Promotions landed](#promotions-landed-in-this-unit)). |
| **SUPERSEDED** | Replaced by a later rule or decision, which is named. The archived text stays as the record of what was true then. |
| **HISTORICAL** | A record of what happened — counts, commit series, one-time deviations, panel findings already discharged. Binds nothing going forward. |

**Counts:** 126 classified rows — 84 HOMED · 15 PROMOTED · 7 SUPERSEDED · 20 HISTORICAL — counted
mechanically from the disposition column of the tables below (regenerate, never hand-edit). The 15
PROMOTED rows resolve to **13 distinct landed promotions**: one promotion spans two rows, and one
row back-references another.

---

## Execution briefs

### [phase-0-bootstrap.md](phase-0-bootstrap.md) — gates before code

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| The port source (private v1) is READ-ONLY; copies via `git show` at the pin | SUPERSEDED | The port is complete; v1 is bugfix-only and is not a source of copies — [STATUS.md](../../../STATUS.md) "Current milestone" |
| No AWS calls; never set the acceptance / live-database gate variables | HOMED | [AGENTS.md](../../../AGENTS.md) "Safety — destructive / outward-facing operations" + "Delegated-agent standing rules" |
| Forbidden content in any staged file (account ids, ARNs, bucket names, credentials, personal identifiers, absolute local paths, session identifiers) | HOMED | [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules" (`Never: … secrets in any output`) + [briefs/map.md](../../../briefs/map.md) "Import gate" |
| Commit attribution trailer, nothing else; no session identifiers | SUPERSEDED | [task/lessons.md](../../../task/lessons.md) 2026-08-09 (name the model actually running; same shape, no session identifiers) |
| `cargo test --workspace`, never the all-features flag; never skip hooks | HOMED | [AGENTS.md](../../../AGENTS.md) "PyO3 build notes" + [DEVELOPMENT.md](../../../DEVELOPMENT.md) "Test-command discipline" + [task/lessons.md](../../../task/lessons.md) 2026-08-06 |
| Adapt, don't invent: preserve the source's content/structure; record the question rather than improvising policy | SUPERSEDED | Port-era fidelity rule. The general form — surface a conflict as a clarifying question instead of silently reconciling — is in [AGENTS.md](../../../AGENTS.md) "Process governance (SEPMO)" (doctrine D1) |
| Staged files must be complete and final — no `TODO(port)`, no stub sections, no commented-out blocks | HOMED | [docs/testing.md](../../testing.md) "Forbidden patterns" |
| Every directory carries a `map.md` with the five sections | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" (`map.md` in every directory) |
| Two honest SQL doors, no blended parser; new SQL surface lands with both spellings + one row per door | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" + [docs/adr/0002-two-sql-doors.md](../../adr/0002-two-sql-doors.md) |
| The entry-point matrix is the central testing structure | HOMED | [docs/testing.md](../../testing.md) "The entry-point matrix" |
| Server-prep disciplines (everything-through-Session; bindings-as-thin-adapter) | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" + [docs/adr/0004-server-prep-disciplines.md](../../adr/0004-server-prep-disciplines.md) |
| Tier-2 CI never runs against unmerged code; no self-hosted runners | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" + [DEVELOPMENT.md](../../../DEVELOPMENT.md) "The CI surface" |
| Carried invariants: owned fork never vendored; DataFusion never forked; no PyIceberg; no Sail; `unsafe_code = "forbid"` except the binding; one DataFusion pin; distribution deferred behind `ExecutionBackend` | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" / "Version-pin contract" / "Explicitly out of scope" |
| Target crate skeleton (which crate owns what, and which are not extracted yet) | HOMED | [AGENTS.md](../../../AGENTS.md) "Crate map" + [repo-manifest.toml](../../../repo-manifest.toml) (`planned` rows, gated by `make check-manifest`) |
| Per-workstream deliverable lists, the five-commit series, the four-lens verification panel | HISTORICAL | — (the record of how phase 0 was executed) |

### [phase-1-engine-core.md](phase-1-engine-core.md) — the engine core

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Port-source pin `fc3f48102`; all copies from `git show` at that SHA | HISTORICAL | Recorded in [README.md](README.md) and in [STATUS.md](../../../STATUS.md) "Delivered capabilities" as the census baseline |
| Copy-then-re-home: every intermediate commit builds and passes `make ci` | HOMED | [docs/port/PLAN.md](../../port/PLAN.md) "The shape: copy-then-re-home" (kept live as the port's plan of record) |
| Relocation discipline: generated old→new name map, never hand-written; `--list` diff empty | HOMED | [docs/testing.md](../../testing.md) "Relocation discipline" |
| Deferred tests go in the checked-in manifest, never in comments; zero `#[ignore]` | HOMED | [docs/testing.md](../../testing.md) "Forbidden patterns" + [task/port/deferred-tests.md](../../../task/port/deferred-tests.md) (live) |
| `.github/`, the contracts and Makefile guards are orchestrator-scoped for delegated units | HOMED | [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules" (`Never: … .github/ changes`) |
| Every new mechanical gate ships with provocation proofs in its unit ledger | HOMED | [docs/testing.md](../../testing.md) "Gate provocation proofs" |
| The three-PR slate, its acceptance list, the fleet plan | HISTORICAL | — |

### [phase-2-sql-doors.md](phase-2-sql-doors.md) — the two SQL doors

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Decision (2026-08-07): `repark-postgres` / `repark-excel` are post-milestone-one, and the four deferred-test rows re-point there | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Deferred capabilities" now names both connectors and links the manifest; [task/port/deferred-tests.md](../../../task/port/deferred-tests.md) keeps the rows |
| Decision (2026-08-07): `repark-ta` is phase-2 scope | HISTORICAL | The crate is delivered — [repo-manifest.toml](../../../repo-manifest.toml) |
| Census ground truth is regenerated from `cargo test -- --list` at the pin, never hand-written | HOMED | [docs/testing.md](../../testing.md) "Relocation discipline" + [docs/port/census.md](../../port/census.md) |
| Standing rules bind every delegated unit (no AWS, hygiene greps, read-only source, never all-features, carve-outs) | HOMED | [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules" |
| Deliverables, hoists, PR ordering, acceptance arithmetic | HISTORICAL | Delivered surface is described in [ARCHITECTURE.md](../../../ARCHITECTURE.md) + the crate maps |

### [phase-3-python-facade.md](phase-3-python-facade.md) — facade, parity, milestone one

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Every gating count is **generated**, never hand-written | HOMED | [docs/testing.md](../../testing.md) "Relocation discipline" + [docs/port/census.md](../../port/census.md) §2–§5 |
| Census baseline generation and the acceptance run are local operator procedures — never CI-wired, never delegated to an agent with environment-variable access | HOMED | [docs/port/census.md](../../port/census.md) "Who runs this" |
| Parity ships before the facade; the wheel workflow ships with the wheel, never before | HISTORICAL | Both landed; CI shape is described in [.github/workflows/map.md](../../../.github/workflows/map.md) |
| Delegated-agent standing rules (gate variables, read-only source, attribution, carve-outs) | HOMED | [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules" |
| Which model tier acts and which criticizes | SUPERSEDED | Deliberately removed from the authoritative surface by this campaign — capability-tier choices are tool mechanics ([AGENTS.md](../../../AGENTS.md) "Delegated work" → the tool adapters) |
| The seven-PR slate, verification tiers, decisions record | HISTORICAL | — |

---

## Phase-1 unit ledgers

### [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Gate provocation proofs for `check_crate_dag` / `check_lib_rs` (both directions) | HOMED | [docs/testing.md](../../testing.md) "Gate provocation proofs"; the gates' rules are their own scripts (`scripts/check_crate_dag.py`, `scripts/check_lib_rs.py`) |
| D-1/D-2/D-3 deviations (cache-warm deferral, two sanitization edits, five-commit series) | HISTORICAL | — |

### [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| **Metadata-projection shim removal criterion** — delete the shim only when a fork rev's metadata-table `scan` honors `projection` (including the empty-projection case); **re-verify at every fork repin** | **PROMOTED** | [AGENTS.md](../../../AGENTS.md) "Version-pin contract" (repin re-verification duties) + [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) "Known limitations" |
| **`NamespaceScopedCatalog` defaulted-method gap** — 16 `Catalog` methods fall to trait defaults with no omission comments (`publish_replace_table` HIGH: the default swallows real catalog overrides); **on every fork repin, re-enumerate the trait surface** | **PROMOTED** | [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) "Known limitations" + [AGENTS.md](../../../AGENTS.md) "Version-pin contract" |
| Fork-docs gap: the metadata-projection gap is not in the fork's `GAP_MATRIX.md`; file it there and land the fork-side fix | **PROMOTED** | [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) "Known limitations" (the fork owns capability status — [AGENTS.md](../../../AGENTS.md) "Hard rules") |
| Trait-wrapping both-sides audit (the wrapped and the defaulted half each get a pin) | HOMED | Live in code and its maps — `crates/repark-ta/src/extension.rs`, `crates/repark-core/src/extension.rs`, [docs/design/session-api.md](../../design/session-api.md) |
| Forced-edit class 6 (one shared `cfg(test)` tracing harness per test binary) | HOMED | `crates/repark-iceberg/src/test_tracing.rs` + its map; the general rule is the fidelity-port record, not a standing rule |
| Fork-pin proof test (a test naming a fork-only public symbol, so a silent registry fallback cannot compile) | HOMED | `crates/repark-iceberg/src/fork_pin_tests.rs` + [docs/adr/0001-own-iceberg-fork.md](../../adr/0001-own-iceberg-fork.md) |
| Census corrections (241 = 50 + 191), commit series, deny/audit restorations, reflow sites | HISTORICAL | — |

### [p1c-repark-core-ledger.md](p1c-repark-core-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Every commit compiles and passes `make ci` (the staged-then-wired carry is the sanctioned way to keep that true) | HOMED | [AGENTS.md](../../../AGENTS.md) "Verify before done" + [docs/port/PLAN.md](../../port/PLAN.md) |
| `#[doc(hidden)]` on the `testing_` seams | HOMED | Live in `crates/repark-core` + [docs/design/session-api.md](../../design/session-api.md) §3 |
| Module split, forced-edit classes, census arithmetic (322 tests), deviations | HISTORICAL | — |

---

## Phase-2 unit ledgers

### [p2a-functions-ledger.md](p2a-functions-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Declared, bounded edit classes; anything outside them is a STOP | HOMED | [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules" (clean STOP states; declared edits) |
| Rider 1 — stale v1 crate references inside the ported crate's doc comments | **PROMOTED** | Not fully discharged — the p2e "discharged at PR-2/3b" claim was second-hand and wrong; 12 sites were live at archival. The four `map.md` sites were corrected at FD-4; the eight `Cargo.toml`/Rust comment sites are named in [STATUS.md](../../../STATUS.md) "Deferred capabilities" (the doc-pointer sweep) |
| Rider 2 — reword the benches map's v1-internal round jargon when the benches workflow is ported | HOMED | [crates/repark-functions/benches/map.md](../../../crates/repark-functions/benches/map.md) already flags the un-ported workflow in its "I want to…" row; cosmetic, no standing rule |
| The empty gate table / unfilled retrospective | HISTORICAL | — (the unit merged as PR #8; the phase-2 close record is [port-execution-log.md](port-execution-log.md)) |

### [p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md) · [p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md) · [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Temporary refuse arms are declared, one refuse test each, and are restored verbatim by a named PR | HISTORICAL | Every arm was restored (p2c/p2d restoration checklists); no refuse arm of this class remains |
| Riders WS1 #5–#9 (write-to-branch sniff, `catalog_ops`/`namespace_ddl` partials, normalize bypass, lib.rs re-export trim) | SUPERSEDED | All DISCHARGED in p2c/p2d, verified against the delivered crate |
| `EngineContext::new` is the one sanctioned downstream constructor; seam growth stays field-additions | HOMED | [docs/design/session-api.md](../../design/session-api.md) "Seam freeze" + `crates/repark-core/src/dialect.rs` |
| Extensions install at v1's construction positions; write knobs live in core `build()`, not in the extension hook | HOMED | `crates/repark-spark/src/extension.rs` + [docs/design/session-api.md](../../design/session-api.md) |
| The MoR (BUG-001) valve lives beside the position-delete path it gates; doors call it | HOMED | `crates/repark-iceberg/src/write/position_delete.rs` + [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) "Known limitations" |
| The six `postgres_p11` census names are the only unported repark-sql names | HOMED | [crates/repark-spark/src/map.md](../../../crates/repark-spark/src/map.md) (the count) + [STATUS.md](../../../STATUS.md) "Deferred capabilities" (the bucket); the six names themselves are archive-only — [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md) |
| Census arithmetic (334 / 342 / 344), commit series, per-PR gate tables | HISTORICAL | — |

### [p2e-ta-ledger.md](p2e-ta-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| The TA set is door-neutral: the owning crate exposes the extension; a door **composes** it rather than re-registering | HOMED | [docs/design/sql-doors.md](../../design/sql-doors.md) Q11 + `crates/repark-ta/src/extension.rs` + [crates/repark-ta/map.md](../../../crates/repark-ta/map.md) |
| `check_lib_rs` ceiling exceptions ratchet **down** only, with a reason and a ratchet trigger | HOMED | `scripts/check_lib_rs.py` `EXCEPTIONS` (the SSOT, quoted by its own error message) |
| A feature-gated surface needs its own census pass — a default-feature `--list` does not see it | HOMED | [docs/testing.md](../../testing.md) "Relocation discipline" (identity is per-invocation) |
| Riders 1–4 (ANSI TA toll, non-default feature, two tests not one, goldens path no-op) | SUPERSEDED | Rider 1 delivered in PR-6 ([p2g](p2g-ansi-m2-ledger.md) "Q11 TA toll"); the rest are recorded outcomes |

### [p2f-ansi-m1-ledger.md](p2f-ansi-m1-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| The surface matrix is the structural backstop: every capability ID is mapped `Tested` (with a test name) or `DeliberatelyAbsent` (with a reason/ADR), audited by a compile-run test | HOMED | `crates/repark-common/src/surfaces.rs` + both doors' `matrix.rs` + [docs/design/sql-doors.md](../../design/sql-doors.md) Q13 |
| An absence row proves an absence is **recorded**, not that it is **true** — probe absence claims empirically | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 2) |
| **The matrix cannot verify that a cited test NAME still exists** (a Rust test binary cannot enumerate itself); the check needs a harness-level gate — `cargo test -- --list` diffed against the matrices | **PROMOTED** | [crates/repark-common/map.md](../../../crates/repark-common/map.md) "Known limitations" (the mechanism's own component contract) |
| ANSI evidence may never be gathered on a Spark-extended session (extensions are session-scoped) | HOMED | [docs/design/session-api.md](../../design/session-api.md) "Seam freeze" + the ANSI matrix's own audit test |
| SEC-02 scope: the local-filesystem refusal covers delegated plans; an intercepted create is governed by the catalog's `LocationPolicy` | HOMED | `crates/repark-sql/src/guards.rs` + `crates/repark-sql/src/map.md` |
| Collision note (43-ID vs 28-ID vocabulary), day-1 spike transcripts, verify-panel fixes 1–6 | HISTORICAL | The spikes' outcomes are live as tests (`parser_productions.rs`, the R2 fix in `repark-core`) |

### [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| `datafusion.*` builder keys reach `SessionConfig`; an unknown key fails loud naming the key; `repark.*` / `spark.*` keep their tolerant consumers | HOMED | `crates/repark-core/src/session.rs` + its map + the seven named pins |
| **OPEN rider:** whether the fork's `$`-suffixed metadata tables should be filtered out of `SHOW TABLES` / `information_schema.tables` (Trino hides them; we do not) — pinned as current behavior in two tests | HOMED | [STATUS.md](../../../STATUS.md) "Known correctness issues" (the `$`-metadata introspection rider) |
| **OPEN rider:** identifier case folding diverges from Apache Spark (quoted identifiers are case-**sensitive** here, through both doors, inherited from stock DataFusion resolution) | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Known correctness issues" |
| ~~**OPEN rider:**~~ **CLOSED by H-1b (2026-08-11)** — the Spark door had the same time-travel pinned-view leak the ANSI door fixed (and the ANSI door's own fix turned out to be half-done; both closed in that unit) | HOMED, then CLOSED | [task/h1b-ledger.md](../hardening-h1/h1b-ledger.md); the STATUS entry was deleted by the fixing unit, per that section's own rule |
| **OPEN rider:** the cited-test-name gate (carried from PR-5) | **PROMOTED** | see the p2f row above — [crates/repark-common/map.md](../../../crates/repark-common/map.md) "Known limitations" |
| Release ephemeral providers on every `?` / `return` path (`PinnedViews` + release after planning; wording corrected 2026-08-11 by H-1b) | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 2, corrected 2026-08-11) + `crates/repark-sql/src/time_travel.rs` |
| Seam freeze: `SqlDialect` / `SessionExtension` are FROZEN (2026-08-08); `EngineContext` stays `#[non_exhaustive]` | HOMED | [docs/design/session-api.md](../../design/session-api.md) "Seam freeze" |
| ADR-0002's design-pass obligation is discharged; the maintenance-as-callable-ops pin keeps its trigger | HOMED | [docs/adr/0002-two-sql-doors.md](../../adr/0002-two-sql-doors.md) |
| Dev-dependencies may cross the door boundary; nothing in `src/` may name the other door | HOMED | `scripts/check_crate_dag.py` `ALLOWED_EDGES` (the SSOT, with the dev-only edge declared as `dev`) |
| Matrix counts, deviations, verify-panel findings 1–3 | HISTORICAL | Each fix is live with its regression pin |

---

## Phase-3 unit ledgers

### [p3a-arming-ledger.md](p3a-arming-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| A CI job split must move the branch-protection required contexts in the **same** update (add the new AND remove the old) | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-07 |
| `rust-cache` needs an explicit `shared-key` or the warm job's key never matches the PR job's | HOMED | [.github/workflows/map.md](../../../.github/workflows/map.md) (`ci.yml` row: prefix-key + shared-key pairs must match `cache-warm.yml`) |
| The `repark.sql()` row-2 spelling note and its release gate | HOMED | [docs/testing.md](../../testing.md) "Row-2 spelling note" + [docs/release.md](../../release.md) "Hard blockers" |

### [p3b-ml-ledger.md](p3b-ml-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| A ported `map.md` is rewritten to the true tree rather than carried stale (dead links violate the hard map-accuracy rule) | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" (`map.md` in every directory, hand-written, updated in the same change) |
| When the acceptance oracle and reality disagree, amend the oracle deliberately and record it — never hedge the claim | HOMED | [docs/testing.md](../../testing.md) (a pin is valid only if reverting the fix turns it red; claims are mechanical) |
| F-1 / F-2 findings, the identity census (34 = 34), the LOW dispositions | HISTORICAL | F-2's dead pointer was discharged in PR-3 |

### [p3c-binding-ledger.md](p3c-binding-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| The panic/async ban's only sanctioned module-scoped escape is the binding's exception-taxonomy module (a per-call-site `#[expect]` cannot reach inside the macro expansion — proven both ways) | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" → *Panic + async bans* (which cites this ledger for the proof) + `clippy.toml` |
| The deferred readers refuse at the **Rust binding** and nowhere else; the refusal names the surface, the schedule and its tracking row | HOMED | `crates/repark-python/src/session.rs` (the refusal text) + [crates/repark-python/map.md](../../../crates/repark-python/map.md); the tracking row now resolves through [task/todo.md](../../../task/todo.md) → [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| A refusal must never echo a connection URL **or** its properties (both are credential vectors) | HOMED | `crates/repark-python/src/session.rs` + its two sentinel pins |
| `EngineRuntime`: the type lives in core, the process-wide instance in the binding; core never constructs a runtime and never blocks on its own behalf | HOMED | `crates/repark-core/src/runtime.rs` + its map |
| Nothing may depend on the bindings crate; the binding names no `repark-sql` / `repark-iceberg` edge | HOMED | `scripts/check_crate_dag.py` (roles + `ALLOWED_EDGES`) + [crates/map.md](../../../crates/map.md) |
| Green-on-a-clean-tree proves nothing about detection — a gate without a recorded provocation is unproven | HOMED | [docs/testing.md](../../testing.md) "Gate provocation proofs" |
| Edit classes EC-1/2/3/5/6/10 as applied, the census diffs, the nine verify-panel findings | HISTORICAL | Each finding is closed (see the ledger's own orchestrator pass) |

### [p3d-parity-ledger.md](p3d-parity-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| The comparator's contract: manifests first, ledger subtraction echoed, sorted-rendering byte comparison, both denominators re-asserted, non-zero exit on any difference | HOMED | [docs/port/census.md](../../port/census.md) §5 (live) + `python/repark-parity/compat/compare_reports.py` and its battery |
| The checked-in ledger files are the ONLY subtraction inputs (no flag, no environment variable) | HOMED | [docs/port/census.md](../../port/census.md) §5 + the provocation test in the comparator's suite |
| An external manifest may **augment**, never overwrite, a report's own manifest | HOMED | [docs/port/census.md](../../port/census.md) §5 |
| Redact evidence artifacts **through each format's parser**, never `sed` | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 3) + `python/repark-parity/compat/redact.py` |
| Census evidence is never hand-edited; a re-run replaces the whole directory in one commit | HOMED | [docs/port/census.md](../../port/census.md) §3 + §7 (the runs, evicted to history 2026-08-23) |
| Local wheels install by **explicit file path** only (a reserved PyPI name outversions a local 0.0.0 wheel) | HOMED | [python/repark/map.md](../../../python/repark/map.md) "Debug" + `.github/workflows/wheels.yml` header + [.github/workflows/map.md](../../../.github/workflows/map.md) |
| A stale orientation count in a design/brief is corrected against the generated number, not the other way round | HOMED | [docs/testing.md](../../testing.md) (generated counts gate; prose is orientation) |
| `docs/release.md`'s PyPI trusted-publisher wording needs the existing-project correction before the release PR (carried as an open note) | **PROMOTED** | Landed at FD-4: [docs/release.md](../../release.md) "PyPI — Trusted Publishing" now states the existing-project flow (the name is reserved; pending publishers are only for names that do not exist) |
| The six judgement calls, the 26 fixer additions, the baseline regeneration record | HISTORICAL | The regenerated baseline is the committed evidence under `task/census/baseline-fc3f48102/` |

### [p3e-facade-ledger.md](p3e-facade-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Deferral decisions are generated **empirically**, by where the exception is raised, against a built artifact | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 3) + [task/port/deferred-tests.md](../../../task/port/deferred-tests.md) |
| A ledger that can drift from the gate it feeds is not a ledger — the prose half and the machine-readable half are bound by a test | HOMED | [task/port/map.md](../../../task/port/map.md) + `python/repark-parity/tests/test_deferred_ledger.py` |
| Unexplained pass/fail movement is stop-and-report; root-cause at the source with a named test | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 3) |
| The full-extras facade cohort definition (extras by name, pyspark/duckdb absent, no JVM, gate variables unset, pandas major recorded) | HOMED | [docs/port/census.md](../../port/census.md) §4 |
| The real-artifact rule is discharged through a wheel installed into a bare interpreter outside the workspace | HOMED | [docs/testing.md](../../testing.md) "Boundary changes need a real-artifact test" |
| Hygiene content passes measure **added lines only** | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 3) |
| A generated run output is not a record — gitignore it rather than tracking it | HOMED | `.gitignore` + [task/map.md](../../../task/map.md) "Debug" |
| **OPEN:** nine ported facade **source** files still reference a v1-only design path (`docs/ml-design.md`) in comments/docstrings, one inside a runtime f-string; a doc-pointer sweep was deferred to post-milestone-one | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| **OPEN (owner decision):** the pre-scrub literal remains reachable in already-published history; recommendation was accept + delete the stale merged phase branches on the remote | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Current milestone" (owner-side housekeeping) |
| Orchestrator hand-offs 1, 2 and 4 (relative `file://` wheel URL; smoke installing one extra; `check-lib-py` / `py-lock-check` missing from `make ci`) | SUPERSEDED | All three are closed on `main` today: `wheels.yml` installs an absolute path with all four extras, and `make ci` runs both guards ([DEVELOPMENT.md](../../../DEVELOPMENT.md) "The commands that matter") |
| Orchestrator hand-off 3 (a required context is matched by the job's **display name**, not its id) | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-07 (twice); the display names themselves are `jobs.<id>.name` in `.github/workflows/*.yml` |
| EC-9 scrub table, EC-4 adjudication, the census/wheel proofs, finding B-1 | HISTORICAL | B-1 was fixed before merge, with pins in `repark-core` |

### [p3f-tier2-ledger.md](p3f-tier2-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| Mint cloud credentials LAST (after checkout/build), never before third-party code runs | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 3) + [docs/tier2-aws.md](../../tier2-aws.md) |
| A real-cloud run must refuse placeholder bucket names (a signed request to a squattable global name discloses the caller) | HOMED | [docs/tier2-aws.md](../../tier2-aws.md) + `python/repark/tests/_acceptance.py`'s guard and its two always-run pins |
| One OIDC subject per run; bind the branch at the environment's deployment policy, not in the workflow file | HOMED | [docs/tier2-aws.md](../../tier2-aws.md) §1 + [task/lessons.md](../../../task/lessons.md) 2026-08-08 |
| Create-only / no-delete IAM posture; resource-scoped ARNs; the account-identifying ARN is a secret | HOMED | [docs/tier2-aws.md](../../tier2-aws.md) |
| Tier-2 workflows are never required checks and never run on unmerged code | HOMED | [AGENTS.md](../../../AGENTS.md) "Hard rules" + [.github/workflows/map.md](../../../.github/workflows/map.md) |
| The two added facade tests are declared census additions | HOMED | [task/port/added-python-tests.txt](../../../task/port/added-python-tests.txt) + [task/port/map.md](../../../task/port/map.md) |

### [p3g-close-ledger.md](p3g-close-ledger.md)

| Rule / constraint carried | Disposition | Authoritative home today |
|---|---|---|
| The acceptance identity `(v2_collected − added) ∪ deferred = pin_collected`, byte-flat on all four cohorts | HOMED | [STATUS.md](../../../STATUS.md) "Delivered capabilities" (the result) + [docs/port/census.md](../../port/census.md) (the procedure) + `task/census/**` (the evidence) |
| Milestone-one declaration items (v1 bugfix-only; cutover sequencing; first tagged release; first tier-2 dispatches) | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Current milestone" — v1 bugfix-only is now a **standing decision**, and the remaining items are the named forward sequence |
| The `--added` ledger is the mirror of `--deferred`, subtracted from the candidate side | HOMED | [task/lessons.md](../../../task/lessons.md) 2026-08-08 (phase 3) + [task/port/map.md](../../../task/port/map.md) |

---

## [port-execution-log.md](port-execution-log.md) — the former `task/todo.md`

The live-backlog condensation. Every unchecked item at the time of archival, and where it went:

| Item (unchecked at archival) | Disposition | Authoritative home today |
|---|---|---|
| `repark-postgres` + `repark-excel` read connectors — post-milestone-one | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| Spark-door time-travel temp-view leak (declared divergence, fix paired with the v1 bugfix) — **CLOSED by H-1b (2026-08-11)**, and NOT as a divergence: a fixed defect gets no registry row | HOMED, then CLOSED | [task/h1b-ledger.md](../hardening-h1/h1b-ledger.md); the STATUS entry was deleted by the fixing unit |
| `$`-metadata-table filtering in introspection | HOMED | [STATUS.md](../../../STATUS.md) "Known correctness issues" |
| Cutover sequencing during parallel-run (single-writer-per-table) | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Current milestone" (the production-pipeline cutover inventory) |
| Never-OOM goal pending a spill-coverage spike | **PROMOTED** | [STATUS.md](../../../STATUS.md) "Deferred capabilities" (scoped into the V2 Engine Hardening campaign) |
| `ci.yml` `detect` classifier deferred until the Rust jobs are slow (trigger: rust-test > ~3 min) | HOMED | [.github/workflows/map.md](../../../.github/workflows/map.md) (`ci.yml` row) |
| The three SEPMO retrospectives and every checked phase item | HISTORICAL | Kept verbatim in this file |

---

## Promotions landed in this unit

Thirteen distinct promotions, each the smallest correct edit to the right current document.
Promotions 1–11 landed **before** the `git mv`; 12–13 landed at the unit's adversarial-review
stage, which found them:

| # | Promoted rule | Landed in |
|---|---|---|
| 1 | Fork-repin re-verification duties (metadata-projection shim removal criterion; re-enumerate the wrapped catalog's trait surface) | [AGENTS.md](../../../AGENTS.md) "Version-pin contract" |
| 2 | The wrapped-catalog defaulted-method gap + the fork `GAP_MATRIX` filing, as a named component limitation | [crates/repark-iceberg/map.md](../../../crates/repark-iceberg/map.md) "Known limitations" |
| 3 | The surface matrix cannot verify a cited test **name** still exists; the closing gate is `cargo test -- --list` diffed against the matrices | [crates/repark-common/map.md](../../../crates/repark-common/map.md) "Known limitations" |
| 4 | Identifier case folding diverges from Apache Spark (quoted identifiers), engine-wide | [STATUS.md](../../../STATUS.md) "Known correctness issues" |
| 5 | `repark-postgres` / `repark-excel` read connectors are the named post-milestone-one deferral | [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| 6 | The never-OOM goal is gated on a spill-coverage spike | [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| 7 | The facade-source dead doc-pointer sweep (nine files, one runtime string) | [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| 8 | Cutover sequencing (single-writer-per-table) as the production-pipeline cutover inventory | [STATUS.md](../../../STATUS.md) "Current milestone" |
| 9 | v1 is bugfix-only and this repository is the sole forward target (standing decision, not a checklist item) | [STATUS.md](../../../STATUS.md) "Current milestone" |
| 10 | The remaining milestone-one declaration items as the named forward sequence | [STATUS.md](../../../STATUS.md) "Current milestone" |
| 11 | The owner-side repository housekeeping left open by the forward-scrub | [STATUS.md](../../../STATUS.md) "Current milestone" |
| 12 | The p2a Rider-1 residue (stale v1 crate references in the ported functions crate): four `map.md` sites corrected outright; the eight `Cargo.toml`/Rust comment sites named for the sweep | [crates/repark-functions/map.md](../../../crates/repark-functions/map.md) + [crates/repark-functions/src/map.md](../../../crates/repark-functions/src/map.md) + [STATUS.md](../../../STATUS.md) "Deferred capabilities" |
| 13 | The PyPI trusted-publisher existing-project correction (the open note carried by the parity ledger) | [docs/release.md](../../release.md) "PyPI — Trusted Publishing" |

## Known residue (recorded, not hidden)

Nine plain-text mentions of a now-archived ledger path survive in files this unit is not permitted to
edit (Rust sources, `Makefile`, `clippy.toml` — FD-4 is docs-and-moves only). They are prose
citations, not links, and every one of them resolves through
[task/map.md](../../../task/map.md) "Where the port ledgers went" or through this directory's
[README.md](README.md):

`Makefile`, `clippy.toml`, `crates/repark-common/src/surfaces/tests.rs`,
`crates/repark-spark/src/extension.rs`, `crates/repark-spark/src/matrix.rs`,
`crates/repark-sql/src/guards.rs`, `crates/repark-sql/src/matrix.rs`,
`crates/repark-sql/tests/introspection.rs`, `crates/repark-sql/tests/parser_productions.rs`.

(`crates/repark-iceberg/src/write/merge/mod.rs` cites a v1-only ledger that never existed in this
repository — pre-existing, unrelated to this move.) A comment-only sweep is the natural rider for the
next unit that edits those files.

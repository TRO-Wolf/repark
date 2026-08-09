# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agent/](.agent/map.md) as thin tool adapters that carry no authoritative facts). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-08-09._

## Release state

Pre-alpha. **No tagged release exists yet.** The PyPI `repark` name is reserved with a placeholder
`0.0.1`; nothing functional has been published. The "API is forever" clock starts at the **first
tagged PyPI release** — that release is now *unblocked* (milestone one is reached) but is a
**user-side action that has not yet been taken**. Public ≠ released: the repository is public, the
engine is not yet distributed. Release mechanics: [docs/release.md](docs/release.md).

## Delivered capabilities

**Milestone one — the private-v1 → public-v2 port — is COMPLETE and merged to `main`
(2026-08-08)** (PRs #16, #18–#23). The port ran copy-then-re-home in four phases; all four are
delivered:

| Phase | Scope | State |
|---|---|---|
| Phase 0 | Bootstrap: governance, testing contract, mechanical gates, map.md discipline, tier-1 CI | **DONE (2026-08-06)** |
| Phase 1 | Engine core: `repark-common`, `repark-iceberg`, `repark-core` | **DONE (2026-08-07)** |
| Phase 2 | The two SQL doors: `repark-functions`, `repark-ta`, `repark-spark`, `repark-sql` | **DONE (2026-08-07)** |
| Phase 3 | Python facade + parity: `repark-ml`, `repark-python`, the wheel + parity harness | **DONE (2026-08-08)** |

**Nine crates are delivered** (workspace SSOT: root `Cargo.toml`; navigation:
[crates/map.md](crates/map.md)): `repark-common`, `repark-core`, `repark-iceberg`,
`repark-functions`, `repark-spark`, `repark-sql`, `repark-ta`, `repark-ml`, `repark-python`. The
Python tree ships `python/repark` (the PySpark facade wheel) and `python/repark-parity` (the
differential harness); a wheel is buildable but not yet tagged.

**Acceptance:** the v2 test census is byte-flat against the port-source pin baseline
`fc3f48102`, exit 0 on all four cohorts — classic `142/345`, expand `44/171`, expand2 `87/167`,
and the facade cohort `(2,499 − 2 added) ∪ 12 deferred = pin 2,509`. Census procedure:
[docs/port/census.md](docs/port/census.md); evidence:
[task/census/baseline-fc3f48102](task/census/) and [task/census/v2-a5be8a7](task/census/); deferred
and added acceptance inputs (live ledgers, still consumed by the comparator):
[task/port/](task/port/). The port's full record — the four phase briefs, the seventeen unit
ledgers, the retrospectives — is archived at
[docs/history/port-v2/](docs/history/port-v2/README.md).

## Current milestone

**Milestone one is COMPLETE.** There is no in-flight *port* work; the delivered record — briefs,
unit ledgers, retrospectives — is archived at
[docs/history/port-v2/](docs/history/port-v2/README.md).

**Standing decision: the private v1 predecessor is bugfix-only, and this repository is the sole
forward target.** New engine work happens here. v1 receives fixes only, and a defect both engines
share is fixed there and re-ported rather than patched only here.

What happens next, in order:

1. **Finish the Agent-Agnostic Front-Door campaign** — in flight; see Active workstreams below.
2. **V2 Engine Hardening** — the next campaign: full optimization *and* the verification that
   proves it, across the native door, the Spark facade, and the write path. Not yet drawn up;
   nothing in it is in flight. The engineering items parked below (spill coverage, the
   `ReparkSession` decomposition trigger, the `ExecutionBackend` seam) are its natural inputs.
3. **Production-pipeline cutover inventory** — enumerate which production workloads move, in what
   order, under **single-writer-per-table** (an Iceberg table is written by v1 or by V2, never
   both), with the rollback story for each. Carried from the port
   ([docs/port/PLAN.md](docs/port/PLAN.md) "Open item: cutover").
4. **The first tagged release** — **held by the owner**, not blocked by engineering. It starts the
   "API is forever" clock; mechanics and hard blockers: [docs/release.md](docs/release.md).

Owner-side actions that ride this sequence rather than gate it: the first `workflow_dispatch` of the
parity-live and aws-acceptance (tier-2, live-AWS) workflows. On repository housekeeping, none
remains: the stale merged `phase-2/*` branches that once carried easy-to-find copies of pre-scrub
content are already gone from the remote. Per the forward-scrub rule (fix content in a new commit,
never rewrite published history), pre-scrub content remains reachable in already-published history —
including `main`'s own — an exposure reviewed and **accepted by explicit decision** rather than by
history-rewrite; provenance and the options weighed:
[docs/history/port-v2/p3e-facade-ledger.md](docs/history/port-v2/p3e-facade-ledger.md)
("the B-2 literal is already published").

## Active workstreams

- **The Agent-Agnostic Front-Door campaign** (in flight) — documentation + mechanical-gate work
  only, no engine-behavior change: a single neutral contributor interface, one status source of
  truth (this file), a machine-readable structural manifest, and reduced active-doc weight. Design:
  [docs/design/agent-agnostic-frontdoor.md](docs/design/agent-agnostic-frontdoor.md); slate:
  [briefs/frontdoor-campaign.md](briefs/frontdoor-campaign.md).

Parked lanes (drawn up, not started; they conflict with nothing and can interleave):

- **`repark.sql` re-home** — the deferred native-door `repark.sql()` relocation, gated on
  release-prep (design ruling in [docs/design/python-facade.md](docs/design/python-facade.md) §4).
- **dbt-repark** — the dbt adapter (separate Python package, dbt-duckdb precedent), a year-one
  load-bearing surface per [PROJECT.md](PROJECT.md).

## Known correctness issues

Carried debt from the port; each is a real defect, honestly tracked, not a blocker for the state
above.

- **Spark-door time-travel view leak** — a declared divergence inherited verbatim from v1; the
  v1 source wants the same bugfix, so it is fixed there and re-ported, not patched only here. The
  ANSI door's fix (`PinnedViews`, released on every exit path) is the template.
- **The `$`-metadata introspection rider** — the fork's `$`-suffixed metadata tables enumerate as
  ordinary tables in `SHOW TABLES` / `information_schema.tables`, where Trino hides them. Whether
  `repark_iceberg::catalog`'s `SchemaProvider::table_names` should filter them is a fork/core
  decision, not a door parser; current behavior is pinned by tests on the ANSI door and on the
  bare-session core path (`crates/repark-sql/tests/introspection.rs`,
  `crates/repark-core/src/session/tests.rs`), so changing it reds a test on purpose.
- **Identifier case folding diverges from Apache Spark.** Both doors resolve a *quoted* identifier
  case-**sensitively** (stock DataFusion resolution); Spark resolves `` `ID` `` case-insensitively
  by default. Unquoted identifiers agree. This is inherited engine-wide, not introduced by either
  door, and is pinned by the cross-door case-folding test; fixing it would be a deliberate
  Spark-door resolution decision, not a bug fix.

## Architectural risks

Design-honesty items — accurate today, with a scheduled correction where noted.

- **`ExecutionBackend` exposes a concrete DataFusion `SessionContext`.** The seam is documented as
  more abstract than it currently is; an honest-doc correction is scheduled (Front-Door FD-5,
  doc/comment only — no signature change).
- **`ReparkSession` is a growing internal policy object.** It accretes session policy; a principled
  internal decomposition is deferred and driver-gated (see below).

## Deferred capabilities

Recorded, not built. Each names the trigger that would start it.

- **Internal `ReparkSession` decomposition** — driver-gated: executed only when a concrete driver
  arrives (PyO3 pressure, a second `ExecutionBackend`, cancellation, or server-protocol needs), not
  on a schedule.
- **`repark-postgres` + `repark-excel` read connectors** — the v1 `read_postgres` / `read_excel`
  surfaces. Scheduled post-milestone-one by explicit decision (2026-08-07). The Python binding
  answers all three entry points (`read_excel`, `excel_sheet_names`, `read_postgres`) with a loud
  refusal naming the surface and this schedule; the withheld tests are the 4 Rust rows + 12 facade
  node ids in [task/port/deferred-tests.md](task/port/deferred-tests.md). The `postgres_p11`
  connectivity count (6 names, same bucket) is tracked in
  [crates/repark-spark/src/map.md](crates/repark-spark/src/map.md); the names themselves live in
  the archived [p2d ledger](docs/history/port-v2/p2d-spark-dml-ledger.md).
- **Never-OOM (spill coverage)** — the goal in [PROJECT.md](PROJECT.md) is stated honestly as
  *pending a spill-coverage spike*; the spike is a natural V2 Engine Hardening input.
- **Dead doc-pointer sweep in ported sources** — eight ported `python/repark/src` modules (nine
  citations, one inside a runtime f-string) still cite a v1-only design path, and eight comment
  sites in `crates/repark-functions` (its `Cargo.toml` and Rust sources) still cite the v1 crate
  name `repark-session`. Left byte-identical during the port to protect the census identity; a
  comment-only sweep can land any time now that the census is closed. (The four `map.md` sites of
  the same class were corrected at FD-4 — maps are live navigation, not port fidelity surface.)

## Release blockers

**None technical.** The engine, tests, and gates are green on `main`. The first tagged release is a
**user-side action, held by the owner** (step 4 of the sequence in "Current milestone"), not an
engineering blocker. The release-side hard blockers — the ones that fail a tag rather than delay
one — are in [docs/release.md](docs/release.md).

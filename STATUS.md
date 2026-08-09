# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) + [CLAUDE.md](CLAUDE.md). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-08-08._

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
acceptance inputs: [task/port/](task/port/).

## Current milestone

**Milestone one is COMPLETE.** There is no in-flight *port* work; the phase ledgers under
[task/](task/) (`p1*`…`p3*`) are the delivered record. What comes next is a **user-side
milestone-one declaration**, then the post-milestone campaign below. The declaration checklist:

- Declare v1 (the private source) **bugfix-only** from this milestone.
- Settle the single-writer-per-table cutover.
- Cut the **first tagged PyPI release** (starts the API-forever clock).
- First `workflow_dispatch` of the parity-live and aws-acceptance (tier-2 live-AWS) workflows.

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
  v1 source wants the same bugfix, so it is fixed there and re-ported, not patched only here.
- **The `$`-metadata introspection rider** — the metadata-column introspection behavior carried
  as a known rider on the Spark door.

## Architectural risks

Design-honesty items — accurate today, with a scheduled correction where noted.

- **`ExecutionBackend` exposes a concrete DataFusion `SessionContext`.** The seam is documented as
  more abstract than it currently is; an honest-doc correction is scheduled (Front-Door FD-5,
  doc/comment only — no signature change).
- **`ReparkSession` is a growing internal policy object.** It accretes session policy; a principled
  internal decomposition is deferred and driver-gated (see below).

## Deferred capabilities

- **Internal `ReparkSession` decomposition** — driver-gated: executed only when a concrete driver
  arrives (PyO3 pressure, a second `ExecutionBackend`, cancellation, or server-protocol needs), not
  on a schedule. Recorded, not built.
- **`postgres_p11` connectivity** — 6 census names, deferred to post-milestone-one.

## Release blockers

**None technical.** The engine, tests, and gates are green on `main`. The first tagged release is a
**user-side action** (the milestone-one declaration checklist above), not an engineering blocker.

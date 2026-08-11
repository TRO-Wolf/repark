# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agent/](.agent/map.md) as thin tool adapters that carry no authoritative facts). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-08-10._

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

1. **Finish the Agent-Agnostic Front-Door campaign** — **DONE (2026-08-10).** All five units
   merged 2026-08-09 (#24, #25, #26, #28, #29); the two acceptance items still unmet at that point
   were closed at the campaign's close-out. Its whole record — design, slate, unit ledger and
   retrospective — is archived at
   [docs/history/frontdoor/](docs/history/frontdoor/README.md), off the normal read path; the
   process metrics are in [task/metrics.md](task/metrics.md).
2. **V2 Engine Hardening** — the next campaign, and the active one: full optimization *and* the
   verification that proves it, across the native door, the Spark facade, and the write path.
   Reconnaissance is complete, and the campaign's design and slate are in-repo
   ([docs/design/v2-engine-hardening.md](docs/design/v2-engine-hardening.md),
   [briefs/v2-engine-hardening.md](briefs/v2-engine-hardening.md)). One preparatory
   sweep has already landed from it (#30, 2026-08-10 — the dead doc-pointer sweep in ported
   sources, which closed the deferral of the same name). The engineering items parked below (spill
   coverage, the `ReparkSession` decomposition trigger, the `ExecutionBackend` seam) are its
   natural inputs.
3. **Production-pipeline cutover inventory** — enumerate which production workloads move, in what
   order, under **single-writer-per-table** (an Iceberg table is written by v1 or by V2, never
   both), with the rollback story for each. Carried from the port
   ([docs/port/PLAN.md](docs/port/PLAN.md) "Open item: cutover").
4. **The first tagged release** — **held by the owner**, not blocked by engineering. It starts the
   "API is forever" clock; mechanics and hard blockers: [docs/release.md](docs/release.md).

Owner-side actions that rode this sequence rather than gating it are **DISCHARGED — no owner-side
tier-2 action remains.** The aws-acceptance (tier-2, live-AWS) workflow's first dispatch ran
**green on 2026-08-10**, with **both catalog legs — Glue and S3 Tables — passing** under the
create-only OIDC role; its AWS-side configuration (OIDC role, variables/secrets per
[docs/tier2-aws.md](docs/tier2-aws.md)) is in place, and what that bring-up taught is folded back
into that runbook (the catalog-wide Glue LIST statement that registration's provider walk
requires, the environment-scoped secret preference, the stale-namespace pre-check). The
parity-live half was **discharged** earlier: the armed nightly has run green on merged `main`
(first runs 2026-08-09/10), so the live-oracle first-run evidence exists without a manual
dispatch. On repository housekeeping, none remains: the stale merged `phase-2/*` branches that
once carried easy-to-find copies of pre-scrub
content are already gone from the remote. Per the forward-scrub rule (fix content in a new commit,
never rewrite published history), pre-scrub content remains reachable in already-published history —
including `main`'s own — an exposure reviewed and **accepted by explicit decision** rather than by
history-rewrite; provenance and the options weighed:
[docs/history/port-v2/p3e-facade-ledger.md](docs/history/port-v2/p3e-facade-ledger.md)
("the B-2 literal is already published").

## Active workstreams

- **V2 Engine Hardening** (active; recon complete, design and slate landed) — the first campaign to
  touch engine code since the port: optimization across the native door, the Spark facade and the
  write path, together with the verification that proves each improvement. Its design is
  [docs/design/v2-engine-hardening.md](docs/design/v2-engine-hardening.md) (goal, the six phases
  H-0…H-5, the dated decisions) and its execution slate is
  [briefs/v2-engine-hardening.md](briefs/v2-engine-hardening.md) (the per-unit definitions and
  acceptance gates). One unit has already merged ahead of it
  (#30, the dead doc-pointer sweep in ported sources).

The **Agent-Agnostic Front-Door campaign** closed on 2026-08-10 — five units merged 2026-08-09,
its two remaining acceptance items discharged at close-out. It is no longer a workstream; its
record is [docs/history/frontdoor/](docs/history/frontdoor/README.md) and its process metrics are
[task/metrics.md](task/metrics.md).

Parked lanes (drawn up, not started; they conflict with nothing and can interleave):

- **`repark.sql` re-home** — the deferred native-door `repark.sql()` relocation, gated on
  release-prep (design ruling in [docs/design/python-facade.md](docs/design/python-facade.md) §4).
- **dbt-repark** — the dbt adapter (separate Python package, dbt-duckdb precedent), a year-one
  load-bearing surface per [PROJECT.md](PROJECT.md).

## Known correctness issues

Carried debt from the port; each is a real defect, honestly tracked, not a blocker for the state
above.

**Where each fact lives.** This section is the authoritative home for an issue that has **no
disposition yet** — its state *and* enough description to be understood. Once an issue is *disposed
of* as a **divergence** — DECLARED (a permanent difference) or BACKLOG (a difference we intend to
close) — its semantics move to the divergence registry,
[docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md), and this file keeps one line
of state plus a link. A known **defect with its fix scheduled** is not a divergence and gets no
row: it stays described here until the fix lands, and the fixing unit deletes the entry rather than
moving it. Nothing is described in both places.

- **Spark-door time-travel view leak** — a known **defect**, not a declared divergence: the fix is
  scheduled (campaign decision D1, unit H-1b) and it is inherited verbatim from v1, so under the
  v1-first rule it is fixed in the v1 source and re-ported rather than patched only here. The ANSI
  door's fix (`PinnedViews`, released on every exit path) is the template. It has no registry row
  and no pin today; H-1b's re-port lands both the fix and the pin, and retires this entry.
- **Identifier case folding diverges from Apache Spark** — **DECLARED (2026-08-10)**, not open. It
  is the divergence registry's first declared row, with its behavior, its rationale and its pin:
  [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md) §3 row ID-1. It stays listed
  here because it remains a real difference a migrating workload can hit; it is not scheduled for a
  fix, and revisiting it needs a new dated decision.
- **Timestamp extraction ignores the session timezone** — **PARTIALLY FIXED (2026-08-10), and the
  remainder is named.** H-1a split A delivered the conf surface, the non-UTC oracle scenarios and
  the recorded disclosure corpus; split B landed the extraction fix. What is **closed** is the
  instant-typed half: `year` / `month` / `dayofmonth` / `hour` / `date_trunc` / `date_format`, and
  this repo's `trunc` / `add_months`, over a TIMESTAMP that already carries the right instant now
  resolve in `spark.sql.session.timeZone` at all four entry points (SQL door, ANSI door, native
  `DataFrame` API, facade — the last pinned at both `sql()` and `df.select(F...)`). Registry row
  TZ-1 was CLOSED IN PART and CONVERTED rather than retired, because two halves are still wrong and
  a reader arriving from a wrong wall clock must land on one of them:
  * **[TZ-7](docs/spark-sql-iceberg-parity.md)** — a **zoneless** TIMESTAMP input (a
    `TIMESTAMP '…'` literal, a zoneless `to_timestamp`, `CAST(str AS TIMESTAMP)`, a
    naive-`datetime` column) is read as UTC rather than as a wall clock in the session zone, so its
    instant is wrong before any extractor sees it. These shapes **agreed with Spark before the fix
    and diverge after it** — the disclosed, forced price of reading every TIMESTAMP as an instant.
  * **[TZ-8](docs/spark-sql-iceberg-parity.md)** — `to_date` / `CAST(ts AS DATE)` / `datediff` still
    take the date in the stored zone (`last_day` / `date_add` over a TIMESTAMP do not plan at all).
    Not a regression; a completeness gap, and `CAST(ts AS DATE)` is the commonest partition-key
    derivation in a migrated job.

  Two further rows carry the type half: **[TZ-4](docs/spark-sql-iceberg-parity.md)** — the tz-naive
  TIMESTAMP Arrow export, which split off because it is the timestamp *representation* rather than
  the extractor path — and **TZ-6**, that repark has no `TIMESTAMP_NTZ` distinct from `TIMESTAMP`
  (re-recorded from the live oracle in the same change). TZ-4's unit is the one that retires TZ-6
  and TZ-7 with it.
- **`CAST(TIMESTAMP AS BIGINT)` returns nanoseconds, not seconds** — **BACKLOG, open
  (2026-08-10)**: a silently-wrong-result class (a 10⁹ factor on every timestamp→integer cast),
  found while authoring the timezone corpus; not a zone bug, and it gets its own unit rather than
  a fold into the extraction fix. Semantics + pin: registry §7 row TZ-5.
- **decimal128 semantics diverge from Apache Spark across nine classes** — **BACKLOG, open
  (2026-08-11)**: bare-literal inference, division precision/scale, the 38-digit result-type
  clamp (and its plan-refuse face), `avg`/`INT*DECIMAL` promotion, ANSI overflow and
  divide-by-zero, and arithmetic nullability — recorded against live PySpark 4.1.2 by the G-7
  differential corpus (hardening gaps G2 + G13). Semantics + pins: registry §7 rows DEC-1 …
  DEC-9; the corpus classifies any silent convergence CONVERGED-flip-don't-delete.

**Closed out of this section.** The `$`-metadata introspection rider was fixed in unit H-1c on
**2026-08-10** — see
[docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md).
Deleted at the campaign's close-out.

## Architectural risks

Design-honesty items — accurate today; each says where the honest description now lives.

- **`ExecutionBackend` exposes a concrete DataFusion `SessionContext`.** The risk is unchanged —
  callers reach single-node DataFusion facilities through the seam, so a distributed backend would
  require widening the surface, not merely a second `impl`. **The docs now say so** (2026-08-09):
  the trait, module, and crate doc-comments in `crates/repark-core` match
  [ARCHITECTURE.md](ARCHITECTURE.md) "`ExecutionBackend` — what the seam is, honestly". No
  correction is outstanding; distribution stays deferred by decision
  ([docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md)).
- **`ReparkSession` is a growing internal policy object.** It accretes session policy; a principled
  internal decomposition is deferred and driver-gated —
  [docs/adr/0005-defer-session-decomposition.md](docs/adr/0005-defer-session-decomposition.md)
  records the intended shape, the exact triggers, and the discharge-note requirement (see also
  Deferred capabilities below).

## Deferred capabilities

Recorded, not built. Each names the trigger that would start it.

- **Internal `ReparkSession` decomposition** — driver-gated: executed only when a concrete driver
  arrives (PyO3 pressure, a second `ExecutionBackend`, cancellation / per-query resource policy, or
  server-protocol needs), not on a schedule. Recorded as
  [docs/adr/0005-defer-session-decomposition.md](docs/adr/0005-defer-session-decomposition.md)
  (status **Deferred**, 2026-08-09) — the intended internal services, the precise trigger
  conditions, and the rule that the unit appends a discharge note naming the driver that fired.
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

## Release blockers

**None technical.** The engine, tests, and gates are green on `main`. The first tagged release is a
**user-side action, held by the owner** (step 4 of the sequence in "Current milestone"), not an
engineering blocker. The release-side hard blockers — the ones that fail a tag rather than delay
one — are in [docs/release.md](docs/release.md).

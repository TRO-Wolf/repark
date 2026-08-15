# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agent/](.agent/map.md) as thin tool adapters that carry no authoritative facts). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-08-15._

## Release state

Pre-alpha, **with v0.1.0 published to PyPI (2026-08-15)** — the first functional release: tag-triggered
`release.yml`, PyPI trusted publishing (the bootstrap token is revoked), `cp312-abi3` manylinux
wheel. The first-tag shakeout (an unresolvable `setup-python` pin, #112) was fixed and the tag
re-cut; the release machinery is proven end-to-end — `release.yml` (tag-triggered, PyPI trusted publishing, wheel-only;
crates.io publishing is structurally deferred, see docs/release.md) with the version SSOT at the
Cargo workspace (`0.1.0`). What remains is **owner-side**: the one-time PyPI trusted-publisher
setup, then the tag — which starts the "API is forever" clock. Public ≠ released: the repository
is public, the engine is not yet distributed. Release mechanics: [docs/release.md](docs/release.md).

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
   sources, which closed the deferral of the same name). **H-1 phase record archived
   2026-08-11** at [docs/history/hardening-h1/](docs/history/hardening-h1/README.md) (mid-campaign
   promotion G-9 — ten unit ledgers + `g4-artifacts/` through the H-1 close gate #35–#46; campaign
   continues into **H-2**). **H-2 seed+tail progress (2026-08-12):** landed G1/G16, G2/G13,
   G3 guard-half, G4+G4b, G5+G5b, G6, G7, G12, G17, G18, G9-partial; **TZ-5 closed by #64**.
   Still open: G8 (deliberately last), G10 (now unblocked by the X-5 comparator), G11/G15
   (owner rulings), TZ-4 (design pass required), G3-E8 FIX, G5b-R. The engineering items
   parked below (spill coverage, the `ReparkSession` decomposition trigger, the
   `ExecutionBackend` seam) are its natural inputs.
   **2026-08-13 — Y wave landed; Z wave in flight.** Y-wave PRs **#66–#72** are on `main`
   (kickoff SHA `9b2dce3`). Closed as code: G4b-R1 rename (#66), G11 ANSI-door
   correctness-not-parity (#67), G10 boundary-shape corpus (#68), `getDatabase` real
   `locationUri` (#69), G4b-R2 origin-map (#70), G15 collation refuse-loud (#71), G5b-R2
   and Spark-door G5b-R3 empty-frame (#72). Still OPEN and **not** claimed closed by this
   increment: G5b-R1 / R4 / R5, G3-E8 FIX, TZ-4 implementation, DEC-1…9, G8, G10 follow-on,
   F-Y10-1 integer wrap (DEC U5 / G13). Semantics of each landed or still-open class: the
   divergence registry
   [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md); completeness
   table: [task/z5-landing-increment-ledger.md](task/z5-landing-increment-ledger.md).
   Z-wave (Z-1…Z-5) is in flight on that frozen SHA.
   **2026-08-13 — Z wave landed ×5; W wave in flight.** Z-wave PRs **#75–#79** are on
   `main` (this increment's base `c7e6589` / `#79` tip; `#73` also on `main`). Closed
   as code: Y-wave §6 landing (#75), facade `avg(DECIMAL)` Spark `(p+4,s+4)` (#76 /
   registry DEC-4 / campaign DEC-5), `F.abs` after semi/anti origin-thread (#77),
   uncorrelated `DELETE … IN` both doors (#78), TZ-4 PR-1 instant-producer
   `timestamp[us, tz=UTC]` + Spark-door Iceberg `timestamptz` (#79). Still OPEN and
   **not** claimed closed by this increment: G3-E8 residual spellings (W-3 in flight),
   TZ-6 / TZ-7 (W-1; **not** retired), TZ-4 residues (B-TZ-4, ANSI column-def
   `timestamp_ns`, Python `TimestampType` mapping), DEC-1 (W-2 in flight) and
   DEC-2/3/5–9, TY-3 still DECLARED, G5b-R1 / R4 / R5 (W-4 in flight), G8, G10
   follow-on, F-Y10-1. Completeness table:
   [task/w5-z-landing-ledger.md](task/w5-z-landing-ledger.md).
   **2026-08-13 — W wave landed ×5; V wave in flight (night 1 of the 48-hour push).**
   W-wave PRs **#81–#85** are on `main` (this increment's base `8d325d4` / `#85` tip;
   `#80` also on `main`). Closed as code: Z-wave §6 landing (#81), G5b-R1/R5 + Q-002
   origin-thread (#82), uncorrelated `DELETE … NOT IN` + NULL 3VL both doors (#83),
   Spark-door `parse_float_as_decimal` / DEC-1 (#84), TZ-4 PR-2 zoneless LTZ
   localization + NTZ distinction; TZ-6/TZ-7 FIXED notes (#85). Still OPEN and **not**
   claimed closed by this increment: G3-E8 residual EXISTS ± correlation (V-1; dbt
   gate not met), TZ-4 residues (B-TZ-4, ANSI column-def `timestamp_ns`), DEC-2/3/5–9,
   TY-3 still DECLARED (U3 revisit rides with V-2), G5b-R4, G8, G10 follow-on,
   F-Y10-1. Completeness table:
   [task/v5-w-landing-ledger.md](task/v5-w-landing-ledger.md).
   **2026-08-14 — V wave landed ×5; S wave in flight (night 2 of the 48-hour push).**
   V-wave PRs **#87–#91** are on `main` (this increment's base `d9a7391` / `#91` tip;
   `#86` also on `main`). Closed as code: W-wave §6 landing (#87), write-path
   partition-value audit (#88), `[NOT] EXISTS` ± correlation both doors (#89; dbt-upgrade
   gate MET, family not fixed), B-TZ-4 string-cast (#90), U3 fromLiteral + U4a
   add/sub/mul clamp (#91; `/` EXCEPTED as U4b). Still OPEN and **not** claimed closed
   by this increment: G3-E8 residual UPDATE IN + correlated IN/ANY/ALL, TZ-8 date-cast,
   DEC-2 `/` (U4b), registry DEC-8 plan-refuse, DEC-6/7/9, TY-3 still DECLARED,
   F-V4-1/2 fork-wave, G5b-R4, G8, the `repark.sql` re-home. Completeness table:
   [task/s5-v-landing-ledger.md](task/s5-v-landing-ledger.md).
3. **Production-pipeline cutover inventory** — enumerate which production workloads move, in what
   order, under **single-writer-per-table** (an Iceberg table is written by v1 or by V2, never
   both), with the rollback story for each. Carried from the port
   ([docs/port/PLAN.md](docs/port/PLAN.md) "Open item: cutover").
4. **The first tagged release** — **held by the owner**, and still blocked by the
   `repark.sql` re-home ([docs/release.md](docs/release.md) "Hard blockers"). It starts the
   "API is forever" clock.

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

- **V2 Engine Hardening** (active; recon complete, design and slate landed; **H-1 phase archived
  mid-campaign 2026-08-11** at [docs/history/hardening-h1/](docs/history/hardening-h1/README.md);
  campaign continues into H-2) — the first campaign to touch engine code since the port:
  optimization across the native door, the Spark facade and the write path, together with the
  verification that proves each improvement. Its design is
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
  This is also the **hard blocker for the first tag**.

**dbt-repark is no longer parked.** M0–M2a merged on the sibling repo (append, delete+insert,
insert_overwrite, merge). M0b/M1b/M2b AWS gates are owner-scheduled; do not claim M0/M1/M2
done until those gates run.

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
  TZ-1 was CLOSED IN PART and CONVERTED rather than retired. Two successor rows carried
  the remainder; #85 closed the zoneless-input / NTZ half:
  * **[TZ-7](docs/spark-sql-iceberg-parity.md)** — **FIXED (2026-08-13, #85).** Zoneless
    LTZ inputs localize in the session zone. Residual: extractor nullability on
    `TIMESTAMP` literals; `F.lit(tz-aware)` under a non-UTC session. Semantics: registry
    TZ-7 FIXED note.
  * **[TZ-8](docs/spark-sql-iceberg-parity.md)** — `to_date` / `CAST(ts AS DATE)` / `datediff` still
    take the date in the stored zone (`last_day` / `date_add` over a TIMESTAMP do not plan at all).
    Not a regression; a completeness gap, and `CAST(ts AS DATE)` is the commonest partition-key
    derivation in a migrated job.

  Two further rows carry the type half: **[TZ-6](docs/spark-sql-iceberg-parity.md)** —
  **FIXED (2026-08-13, #85).** `TIMESTAMP` vs `TIMESTAMP_NTZ` are distinct. Residual:
  `spark.sql.timestampType` is not implemented. **[TZ-4](docs/spark-sql-iceberg-parity.md)** —
  **PROGRESS (2026-08-13, #79 + #85 + #90), not retired.** Instant-typed producers export
  `timestamp[us, tz=UTC]`; Spark-door DDL `TIMESTAMP` stores Iceberg `timestamptz`;
  zoneless localization + NTZ distinction landed in PR-2; B-TZ-4 string-cast **FIXED
  (#90)**. Residue: ANSI column-def `timestamp_ns`.
  **[F-V4-1](docs/spark-sql-iceberg-parity.md) / [F-V4-2](docs/spark-sql-iceberg-parity.md)** —
  **DECLARED (2026-08-14), fork-wave-routed.** Timestamptz identity meta projection
  refuses; Arrow annotation is `+00:00` vs Spark `UTC`.
- **`CAST(TIMESTAMP AS <numeric>)` returns epoch seconds** — **FIXED (2026-08-12, #64).**
  The 10⁹ nanoseconds-vs-seconds class is closed, including INT/SMALLINT un-refusal and
  floor semantics. Residual: TIMESTAMP→INT **nullability only** (registry G6-4). Semantics of
  the closed class: registry TZ-5 (FIXED note).
- **decimal128 semantics diverge from Apache Spark across nine classes** — **BACKLOG,
  still open except DEC-1, DEC-3, DEC-4, and DEC-5 width.** Registry DEC-4 / campaign
  DEC-5 `avg(DECIMAL)` is **FIXED (2026-08-13, #76)** — facade now Spark `(p+4,s+4)`.
  DEC-1 (literal inference) is **FIXED (2026-08-13, #84)** — Spark-door
  `parse_float_as_decimal=true`. DEC-3 (38-digit add/sub/mul clamp) is **FIXED
  (2026-08-13, #91 / U4a)**; `/` is **EXCEPTED** as U4b (DEC-2 stays BACKLOG).
  Campaign DEC-8 / U3 integer-literal min-precision closed DEC-5 **width** (#91);
  DEC-5 **nullability** stays BACKLOG (DEC-9). Registry DEC-8 (`(38,20)*(38,20)`
  plan-refuse) stays BACKLOG (ExprPlanner). DEC-2/6/7/9 remain photographed, not
  fixed. TY-3 stays DECLARED (U3 landed; residual is UNION `forType(INT)` —
  `(21,1)` nullable vs Spark `(11,1)` non-null). Semantics + pins: registry §7
  DEC-1 … DEC-9 (DEC-1, DEC-3, DEC-4 are dated FIXED notes).
- **Negative temporal-RANGE `count(*)` = -1 in release wheels** — **FIXED on the Spark
  door / facade `.sql()` (2026-08-12, Y-1 / #72).** Kind-or-magnitude invert is Spark's
  empty frame (`count(*)` 0, `sum` NULL) or a loud refuse; wrap is gone there.
  **G5b-R1 / R5 FIXED (2026-08-13, #82).** G5b-R4 still OPEN (FOLLOWING-to-FOLLOWING
  120 vs 90; DF 54.1 range-search). ANSI-door wrap is a named residual (no pin).
  Semantics: registry G5b-R3 / R1 / R5 FIXED notes +
  [G5b-R4](docs/spark-sql-iceberg-parity.md).
- **DELETE/UPDATE subquery predicates** — **PARTIALLY FIXED (2026-08-13, #78 + #83 + #89).**
  Uncorrelated `DELETE … WHERE col IN (SELECT col FROM …)`,
  `DELETE … WHERE col NOT IN (SELECT col FROM …)` (including the NULL 3VL trap), and
  `DELETE … WHERE [NOT] EXISTS (SELECT …)` ± correlation execute on both doors and
  match Spark. IN + NOT IN + `[NOT] EXISTS` ± correlation all execute both doors —
  the dbt-upgrade gate is MET. The family is **not** closed: UPDATE IN + correlated
  IN / ANY / ALL stay valved. G3-E8-NULL DELETE half matches Spark; UPDATE half stays
  refused. Semantics + pins: registry §7 rows G3-E8 / G3-E8-NULL.
- **`repark.sql` re-home** — **blocks the first tagged release** (not a correctness defect).
  See [docs/release.md](docs/release.md) "Hard blockers" and Deferred capabilities.

**Closed out of this section.** The `$`-metadata introspection rider was fixed in unit H-1c on
**2026-08-10** — see
[docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md).
The Spark-door time-travel view leak was fixed in unit H-1b on **2026-08-11** — see
[docs/history/hardening-h1/h1b-ledger.md](docs/history/hardening-h1/h1b-ledger.md).
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

**None.** v0.1.0 shipped 2026-08-15. Future tags follow [docs/release.md](docs/release.md)
(version SSOT at the Cargo workspace; wheel-only; crates.io publishing structurally deferred).

## 2026-08-15 hardening increment (conductor-13)

Twelve PRs merged in one wave: MERGE OCC hardening (`write.merge.isolation-level` honored
(#117, audit M13), conflict batteries + M14/M15/M20 characterization pins (#121), evolved-spec
position-delete `spec_id` stamping fixed (#118, M16)); functions surface 253 -> 291 across
FN-C/D/E/F (#115/#119/#122/#125); the TA lane (fusion pins #116, `ta.with_indicators` #120,
volume goldens #123, volume kernels `ad`/`adosc`/`obv`/`mfi` #127); and the pre-authorized
stretch pair (`spark.sql.timestampType` LTZ/NTZ #124, ANSI-door nanosecond CREATE reject #126).
Remaining MERGE divergences are registered as DML-4/DML-5 and BL-3/BL-4/BL-5 in
[the divergence registry](docs/spark-sql-iceberg-parity.md); TA oracle divergences stay
documented in-crate (`crates/repark-ta`), their authoritative home.

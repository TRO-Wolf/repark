# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agents/](.agents/map.md) as thin tool adapters that carry no authoritative facts). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-08-25._

## Release state

Pre-alpha, **with v0.5.0 published to PyPI (2026-08-20)** — the seventh tag on proven machinery
(v0.1.0 / v0.2.0: 2026-08-15; v0.3.0–v0.3.2: 2026-08-16; v0.4.0: 2026-08-19): tag-triggered
`release.yml`, PyPI trusted publishing (the bootstrap token is revoked), `cp312-abi3` manylinux
wheel, wheel-only (crates.io publishing is structurally deferred, see docs/release.md), version
SSOT at the Cargo workspace (`0.5.0`). v0.5.0 is a feature minor with a single engine payload:
the native `dynamicFlatten` plan rewrite (#183 — DF1). The Python planner loop moves into
`repark_core::dynamic_flatten` (structs first, lists one at a time, null-safe `get_field`
Project, unqualified Unnest) behind a thin PyO3 bind; the facade keeps only type-gates and
`_spawn`, and `plan_collapse.py` / `_dynamic_flatten_unnest_structs` are deleted. Three octo
remediation cycles pinned it fail-closed: list-of-map and `ListView`/`LargeListView` refuse
LOUD, Dictionary and `LargeList`/`FixedSizeList` list unwrap before Unnest, mixed-plan inner
errors stay loud, plus a `ScanForbidden` plan-build spy and the `df_guard_tests.rs` fence. Also
in the tag: the `.agents/skills` runbooks (#184 — publish-pypi, compact-context-docs).
Pre-alpha still means the API can move between tags. Release mechanics:
[docs/release.md](docs/release.md).

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
differential harness). The published wheel is in [Release state](#release-state) above.

**Acceptance:** the v2 test census is byte-flat against the port-source pin baseline
`fc3f48102`, exit 0 on all four cohorts — classic `142/345`, expand `44/171`, expand2 `87/167`,
and the facade cohort `(2,499 − 2 added) ∪ 12 deferred = pin 2,509`. Census procedure:
[docs/port/census.md](docs/port/census.md); evidence:
`task/census/baseline-fc3f48102/` and `task/census/v2-a5be8a7/`, evicted from the tree by DL-1 on
2026-08-23 and reachable at `main` `b13b22c` ([docs/port/census.md](docs/port/census.md) §7) —
except the baseline's [facade cohort](task/census/baseline-fc3f48102/facade/map.md), which the
deferred-ledger tests read; deferred
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
   Still open: G8 (deliberately last), G10 (now unblocked by the X-5 comparator), TZ-4
   (design pass required), G3-E8 FIX, G5b-R. The engineering items
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
   table: [task/z5-landing-increment-ledger.md](task/ledgers/archive/2026-08/2026-08-13-z5-landing-increment-ledger.md).
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
   [task/w5-z-landing-ledger.md](task/ledgers/archive/2026-08/2026-08-13-w5-z-landing-ledger.md).
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
   [task/v5-w-landing-ledger.md](task/ledgers/archive/2026-08/2026-08-13-v5-w-landing-ledger.md).
   **2026-08-14 — V wave landed ×5; S wave in flight (night 2 of the 48-hour push).**
   V-wave PRs **#87–#91** are on `main` (this increment's base `d9a7391` / `#91` tip;
   `#86` also on `main`). Closed as code: W-wave §6 landing (#87), write-path
   partition-value audit (#88), `[NOT] EXISTS` ± correlation both doors (#89; dbt-upgrade
   gate MET, family not fixed), B-TZ-4 string-cast (#90), U3 fromLiteral + U4a
   add/sub/mul clamp (#91; `/` EXCEPTED as U4b). Still OPEN and **not** claimed closed
   by this increment: G3-E8 residual UPDATE IN + correlated IN/ANY/ALL, TZ-8 date-cast,
   DEC-2 `/` (U4b), registry DEC-8 plan-refuse, DEC-6/7/9, TY-3 still DECLARED,
   F-V4-1/2 fork-wave, G5b-R4, G8, the `repark.sql` re-home. Completeness table:
   [task/s5-v-landing-ledger.md](task/ledgers/archive/2026-08/2026-08-13-s5-v-landing-ledger.md).
3. **Production-pipeline cutover inventory** — enumerate which production workloads move, in what
   order, under **single-writer-per-table** (an Iceberg table is written by v1 or by V2, never
   both), with the rollback story for each. Carried from the port
   ([docs/port/PLAN.md](docs/port/PLAN.md) "Open item: cutover").
4. **The first tagged release** — **DONE**: v0.1.0 shipped 2026-08-15 (v0.5.0 current; see
   Release state above), unblocked by the `repark.sql` re-home landing 2026-08-14
   ([docs/release.md](docs/release.md) "RESOLVED", #95). Pre-alpha still means the API can
   move between tags (the design ruling that the API-forever clock starts at the first tag —
   [docs/design/python-facade.md](docs/design/python-facade.md) §4 — is enforced at the v1.0
   north-star API review).

Owner-side actions that rode this sequence rather than gating it are **DISCHARGED — no owner-side
tier-2 action remains.** The aws-acceptance (tier-2, live-AWS) workflow's first dispatch ran
**green on 2026-08-10**, with **both catalog legs — Glue and S3 Tables — passing** under the
create-only OIDC role; its AWS-side configuration (OIDC role, variables/secrets per
[docs/tier2-aws.md](docs/tier2-aws.md)) is in place, and what that bring-up taught is folded back
into that runbook (the catalog-wide Glue LIST statement that registration's provider walk
requires, the environment-scoped secret preference, the stale-namespace pre-check). The
parity-live half was **discharged** on first-run evidence (green on merged `main`
2026-08-09/10). From 2026-08-14 through 2026-08-22 the armed nightly was red on three
stale always-PASS Apache smoke pins — G15/Y-7
[#71](https://github.com/TRO-Wolf/repark/pull/71) collation refuse and FA-4
[#164](https://github.com/TRO-Wolf/repark/pull/164) nested-dict-as-struct —
disposed divergences whose pin list was never updated. Those three are now
known-FAIL meta pins; the nightly is a live signal again. On repository housekeeping, none remains: the stale merged `phase-2/*` branches that
once carried easy-to-find copies of pre-scrub
content are already gone from the remote. Per the forward-scrub rule (fix content in a new commit,
never rewrite published history), pre-scrub content remains reachable in already-published history —
including `main`'s own — an exposure reviewed and **accepted by explicit decision** rather than by
history-rewrite; provenance and the options weighed:
[docs/history/port-v2/p3e-facade-ledger.md](docs/history/port-v2/p3e-facade-ledger.md)
("the B-2 literal is already published").

## Active workstreams

**The ordered queue across the open tracks is [briefs/next-sequence.md](briefs/next-sequence.md)**
(rolling, opened 2026-08-21). It states sequence and reasoning; the per-track state stays here.

<!-- ws id=dl ledgers=dl- state=open -->
- **Document lifecycle (DL)** (chartered 2026-08-23 by the owner; DL-1..DL-4 delivered). Unit ledgers live in [task/ledgers/](task/ledgers/map.md) by state — `staging/` →
  `completed/` (the unit's last commit) → `archive/yyyy-mm/` (the script's move at pickup,
  immutable) — and `scripts/ledger_lifecycle.py` is the only thing that moves them, rewriting
  every link as it goes. Three gates in `make ci` (dual-wired into `ci.yml` by
  [#223](https://github.com/TRO-Wolf/repark/pull/223)) hold the class: `check-ledgers` — bins,
  archive names, every ledger link, the frozen rule (DL-1,
  [#221](https://github.com/TRO-Wolf/repark/pull/221)); `check-ledger-grammar` — clause rows,
  `pins:` citations, the Critic's attestation (DL-2,
  [#222](https://github.com/TRO-Wolf/repark/pull/222)); and, since DL-4, `check-docs-compaction`
  — no closed campaign in this section, no merged unit on the slate, every workstream marked,
  byte ceilings. Archive month maps are one line per ledger (DL-3,
  [#225](https://github.com/TRO-Wolf/repark/pull/225)). Records: the DL-1/2/3 charters in
  [task/ledgers/archive/2026-08/](task/ledgers/archive/2026-08/map.md); DL-4 (delivered
  2026-08-25: STATUS.md 65.9 → 30.1 kB, the slate 26.7 → 5.6 kB):
  [the DL-4 ledger](task/ledgers/archive/2026-08/2026-08-25-dl-4-live-doc-compaction-charter-ledger.md).
<!-- /ws -->

<!-- ws id=sem ledgers=sem- state=held -->
- **The Spark semantics fixes (SEM)** (chartered 2026-08-21, gate ruled the same day; **held**).
  Delivered as [#192](https://github.com/TRO-Wolf/repark/pull/192) and
  [#193](https://github.com/TRO-Wolf/repark/pull/193): SEM-1/3/4/5/6 — `RE-1` and `RE-3` closed
  (both rows retired from the registry), the regexp refusals carry Spark's `REGEX_GROUP_INDEX`
  condition, the string-`idx` regression fixed. **SEM-2 (`LOG-1`) is tabled, not dropped** —
  owner ruling 2026-08-21 — with `F.log`'s two-argument overload tabled alongside it (the only
  kernel available lacks Spark's null-guard). The measured scope for both stays in the charter:
  [task/ledgers/staging/sem-0-charter-ledger.md](task/ledgers/staging/sem-0-charter-ledger.md).
  This campaign changes what a working query returns, deliberately — the reason the LRS
  registered `RE-1` rather than fixing it.
<!-- /ws -->

<!-- ws id=v3 ledgers=v3-,v3e- state=open -->
- **Format-v3 track** — **the v1.0 north star (owner ruling 2026-08-23): full production-grade
  format-v3.** Definition and gate:
  [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md);
  design: [docs/design/format-v3-track.md](docs/design/format-v3-track.md); audit:
  [task/ledgers/staging/v3-0-charter-ledger.md](task/ledgers/staging/v3-0-charter-ledger.md).
  - **Measured true (V3-0, [#199](https://github.com/TRO-Wolf/repark/pull/199)):** reading
    Spark-written v3 with Puffin deletion vectors and appending with row lineage are correct,
    round-tripped through Spark. **Guarded:** `rewrite_data_files` reassigned `_row_id` on v3 —
    registry `V3-LINEAGE-1`, stricter than Spark on purpose, reversible in one line; the fix is
    fork work (F-7). Queued: `V3-DANGLE-1`, `V3-ROWID-1` (V3-4).
  - **Delivered:** V3-1 `register_table` + the checked-in v3 fixture
    ([#203](https://github.com/TRO-Wolf/repark/pull/203)); V3-2 CREATE/CTAS `format-version = 3`
    behind `repark.sql.allowCreateFormatVersion3` (default false), ALTER refused
    ([#232](https://github.com/TRO-Wolf/repark/pull/232)); V3E-1 + V3E-2 — measured adopted-v3
    COW DML committing the right rows while **reassigning** lineage (Spark preserves `_row_id`),
    `ENC-1` DECLARED, the v3 maintenance oracle is PySpark 4.1.2 + Iceberg 1.11.0
    ([#235](https://github.com/TRO-Wolf/repark/pull/235)); V3E-3 — partitioned-DV and
    equality-delete v3 fixtures CI-runnable, live rows Spark-exact on all three doors,
    `.delete_files` content 1/2 ([#236](https://github.com/TRO-Wolf/repark/pull/236)); V3R-1 —
    **the 2026-08-25 owner rulings:** COW DML on a v3 table is **guarded** (registry `V3-COW-1`
    refuses on both doors before any write — with MOR refused too, a v3 table is append-only
    here until fork F-7), `geometry`/`geography` DECLARED out of v1.0 (`V3-GEO-1`),
    shredded-Parquet `variant` DECLARED out (queued `V3-VARIANT-SHRED-1`), the S3 Tables live
    legs are **in** (OD-3b; the scoped IAM statement is in `docs/tier2-aws.md` §2, owner-executed),
    and the v2→v3 in-place upgrade is built behind the create opt-in after V3-3. Ledgers in
    [task/ledgers/archive/2026-08/](task/ledgers/archive/2026-08/map.md).
  - **Next:** V3E-4 (refs + time travel; expiry/orphans with real work), on
    [briefs/next-sequence.md](briefs/next-sequence.md). V3-3 (DV writes) is owner-sequenced,
    gated on fork F-13.
<!-- /ws -->

<!-- ws id=perf ledgers=perf- state=open -->
- **Performance campaign — TA parity with `polars_talib` (chartered 2026-08-15; measure-first).**
  Goal added to [PROJECT.md](PROJECT.md) Goals. Phase 0 is the recorded benchmark baseline (the
  perf note's §8 battery: kernel race, many-symbols scaling, wide serving SELECT, batch-size
  sweep, null_lookback cost, last-row collect; plus flamegraph/heaptrack and a bench-only
  safe-vs-unchecked ceiling microbench). Implementation slates (multi-slot cache, null-free
  borrow, single Arrow write, short-partition early-out) are GATED on those numbers; the perf
  note's §7 do-not list (no math reordering, goldens bit-exact) is binding; `unsafe` remains
  workspace-forbidden.
<!-- /ws -->

<!-- ws id=fnp ledgers=fnp- state=open -->
- **Spark function parity campaign** (active, chartered 2026-08-20; first tranche merged as
  [#190](https://github.com/TRO-Wolf/repark/pull/190)). Close the `pyspark.sql.functions` gap and
  move the semantics behind every name out of Python into Rust. Design:
  [docs/design/spark-function-parity.md](docs/design/spark-function-parity.md) (§7 carries the
  unit table and the recommended order); slate:
  [briefs/spark-function-parity.md](briefs/spark-function-parity.md); gate (12/12 `PROVEN`):
  [task/ledgers/staging/fnp-0-charter-ledger.md](task/ledgers/staging/fnp-0-charter-ledger.md);
  evidence: [task/fnp-0-census/](task/fnp-0-census/map.md).
  **Delivered:** `__all__` 333 → 360, 41 names from refusing-or-absent to working (FNP-1..6c);
  thirty-six needed no new kernel — that seam is exhausted. **Next, in order (revised
  2026-08-20 on measured evidence):** FNP-15 + FNP-16 (refusing stubs + registry sections for
  the 62 names that will not be built — closes charter C-009), FNP-4c (the higher-order
  kernels), then F-Y10-1 integer wrap (measured: `CAST(2147483647 AS INT) + 1` returns
  `2147483648` where Spark raises; blocks FNP-7b), FNP-7a / 9 / 10, FNP-8 (repatriation), FNP-11
  / 12, FNP-Z close-out. Deferred with reasons in the design: FNP-6d, FNP-13, FNP-14, FNP-4b.
<!-- /ws -->

<!-- ws id=h2 ledgers=h-,h2- state=open -->
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
<!-- /ws -->

Parked lanes: **none.** The `repark.sql` re-home lane closed 2026-08-14 (#95 —
[docs/release.md](docs/release.md) "RESOLVED"; design ruling
[docs/design/python-facade.md](docs/design/python-facade.md) §4).

<!-- ws id=dbt ledgers=dbt- state=open -->
- **dbt-repark is no longer parked.** M0–M2a merged on the sibling repo (append, delete+insert,
  insert_overwrite, merge). M0b/M1b/M2b AWS gates are owner-scheduled; do not claim M0/M1/M2
  done until those gates run.
<!-- /ws -->

**Closed campaigns** — each record is in [docs/history/](docs/history/map.md); the rows below are
written by `scripts/ledger_lifecycle.py compact` when a workstream's marker says `state=closed`:
<!-- closed-campaigns -->
- **Agent-Agnostic Front-Door campaign** — closed 2026-08-10 (five units merged 2026-08-09, the two
  remaining acceptance items discharged at close-out); record:
  [docs/history/frontdoor/README.md](docs/history/frontdoor/README.md); metrics:
  [task/metrics.md](task/metrics.md)
- **Python convention conformance (PYC)** — closed 2026-08-22 by #216; record: [docs/history/pyc/status-record.md](docs/history/pyc/status-record.md)
- **Low-risk sweep (LRS)** — closed 2026-08-21 by #191; record: [docs/history/lrs/status-record.md](docs/history/lrs/status-record.md)
- **Iceberg maintenance wave (MW)** — closed 2026-08-23 by #224; record: [docs/history/iceberg-maintenance-wave/status-record.md](docs/history/iceberg-maintenance-wave/status-record.md)

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

- **`F.log` two-argument form** — tabled with SEM-2 (owner ruling 2026-08-21); scope in
  [task/ledgers/staging/sem-0-charter-ledger.md](task/ledgers/staging/sem-0-charter-ledger.md).
- **Identifier case folding** — **DECLARED (2026-08-10)**: registry
  [ID-1](docs/spark-sql-iceberg-parity.md); revisiting it needs a new dated decision.
- **The session-timezone family** — TZ-1 converted; TZ-6 / TZ-7 FIXED (#85); **TZ-8 open**
  (`to_date` / `CAST(ts AS DATE)` / `datediff` read the stored zone — a completeness gap, and the
  commonest partition-key derivation in a migrated job); TZ-4 in progress (residue: ANSI
  column-def `timestamp_ns`); F-V4-1 / F-V4-2 DECLARED, fork-routed; TIMESTAMP→INT nullability
  BACKLOG (G6-4; the epoch-seconds class itself FIXED, #64). Semantics + pins: the registry's TZ
  rows, [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md).
- **decimal128** — BACKLOG on DEC-2 / DEC-6 / DEC-7 / DEC-8 / DEC-9 (DEC-5 nullability); DEC-1 /
  DEC-3 / DEC-4 / DEC-5 width FIXED; TY-3 DECLARED. Registry §7 DEC-1 … DEC-9.
- **Temporal-RANGE frames** — G5b-R4 OPEN (FOLLOWING-to-FOLLOWING; DF 54.1 range-search); R1 /
  R3 / R5 FIXED; the ANSI-door wrap is a named residual with no pin. Registry G5b rows.
- **DELETE/UPDATE subquery predicates** — the dbt-upgrade gate is MET (IN / NOT IN / `[NOT]
  EXISTS` ± correlation execute on both doors); UPDATE IN and correlated IN / ANY / ALL stay
  valved; G3-E8-NULL's UPDATE half stays refused. Registry §7 G3-E8 / G3-E8-NULL.
- **`bin` / `rint` BOOLEAN over-accept** — **BACKLOG (2026-08-19)**: registry
  [§7 BL-6](docs/spark-sql-iceberg-parity.md).
- **`bit_length` / `octet_length` DOUBLE stringify (Infinity / E-notation)** — **BACKLOG
  (2026-08-19)**: registry [§7 BL-7](docs/spark-sql-iceberg-parity.md).
- **Regexp match-counting residual families (GT1-FIX #180, no disposition yet)** — the owned
  `regexp_count` / `regexp_instr` kernel reproduces Java `Matcher.find()` for the mainstream
  corpus (97%+ agreement over a 4118-case fuzz vs real OpenJDK) but four narrow families still
  diverge, each live-verified against PySpark 4.1.2 on 2026-08-19: (1) Java can match at a
  mid-surrogate UTF-16 index with no preceding empty match (`'\B'` on `ab🐈cd`: Spark 3,
  repark 2) — not fixable without a UTF-16 code-unit matcher; (2) Java non-MULTILINE `$` also
  matches before a final line terminator (`'a\n'`, `'$'`: Spark 2, repark 1); (3) Java `(?m)^`
  never matches at end-of-input and its line terminators include `\r`, `\r\n`, U+0085, U+2028,
  U+2029 (regex-crate multiline is `\n`-only); (4) ANSI-off conditional semantics
  (`legacySizeOfNull` −1; string-idx CAST NULL) are not modeled — the kernels hardcode the
  ANSI-ON default. Descriptions with examples:
  [task/fn-gt1-ledger.md](task/ledgers/archive/2026-08/2026-08-19-fn-gt1-ledger.md) Residuals.
- **Numeric implicit-cast breadth on string-function arguments (no disposition yet)** — Spark
  implicitly casts numeric→string first args (`regexp_count(123,'2')` = 1) and non-integer
  numerics for `regexp_instr` idx / `split_part` partNum (`split_part(…, 2.0)` = `'b'`); repark
  plan-refuses both doors (fail-loud direction, pre-existing class). Also `split_part` with a
  NULL str and a non-foldable `partNum` 0 errors where Spark short-circuits to NULL. Live-verified
  2026-08-19; [task/fn-gt1-ledger.md](task/ledgers/archive/2026-08/2026-08-19-fn-gt1-ledger.md) Residuals.
- **SQL string literals do not process backslash escapes (no disposition yet)** — the SQL door
  parses `'\d'` as two characters where Spark's parser processes the escape to one
  (`length('\d')`: Spark 1, repark 2). Found 2026-08-19 during the GT1-FIX review; affects every
  SQL-door string literal containing a backslash (regex patterns most visibly — a pattern spelled
  `'\\d'` reaches the engine as `\d` on Spark but as `\\d` here). Engine parser level, undisposed.
- **`CAST(x AS BINARY)` unimplemented on the SQL door (no disposition yet)** — plan-time
  `Unsupported SQL type BINARY`; the facade `.cast("binary")` path is unaffected. Found
  2026-08-19 during the GT1-FIX review.

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

**None.** v0.5.0 shipped 2026-08-20 (v0.4.0: 2026-08-19; v0.3.0–v0.3.2: 2026-08-16; v0.1.0 / v0.2.0:
2026-08-15). Future tags follow
[docs/release.md](docs/release.md) (version SSOT at the Cargo workspace; wheel-only; crates.io
publishing structurally deferred).

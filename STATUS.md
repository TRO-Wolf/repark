# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agents/](.agents/map.md) as thin tool adapters that carry no authoritative facts). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-08-23._

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

- **Document lifecycle (DL)** (chartered 2026-08-23 by the owner; **DL-1 merged as
  [#221](https://github.com/TRO-Wolf/repark/pull/221)**). Unit ledgers live in
  [task/ledgers/](task/ledgers/map.md) by state — `staging/` → `completed/` (the unit's last
  commit) → `archive/yyyy-mm/` (the script's move at pickup, immutable) — and
  `scripts/ledger_lifecycle.py` is the only thing that moves them, rewriting every link as it
  goes; `make check-ledgers` (in `make ci`) holds the bins, the archive names, every ledger
  link in the repository and the frozen rule. Backfilled the same day: 122 ledgers to
  `archive/2026-08/`, four open charters (MW-0, SEM-0, FNP-0, V3-0) in `staging/`. The roadmap
  has two bins by horizon, [task/roadmap/](task/roadmap/map.md) `mid-term/` and `epic-term/`
  (short-term stays the slate). `task/census/` is evicted from the tree (reachable at
  `b13b22c`, [docs/port/census.md](docs/port/census.md) §7). Charter and execution record:
  [task/ledgers/archive/2026-08/2026-08-23-dl-1-ledger-lifecycle-charter-ledger.md](task/ledgers/archive/2026-08/2026-08-23-dl-1-ledger-lifecycle-charter-ledger.md).
  **DL-2 merged as [#222](https://github.com/TRO-Wolf/repark/pull/222)** (stacked on DL-1): the
  ledger *grammar* — `scripts/check_ledger_grammar.py` / `make check-ledger-grammar` (in
  `make ci`) holds the shape of every live ledger: clause rows (`C-NNN`, one verdict,
  evidence), the `pins: <unit>/C-NNN` citation that binds a test to the clause it discharges
  ([docs/testing.md](docs/testing.md) "Pinning a charter clause"), and the Critic's
  `COVERAGE_ATTESTATION` in ref 05's shape, required once no clause is `OPEN`. Measured floor
  seeded (31 unpinned `PROVEN` clauses across three live charters; ratchets down only). XML as
  the ledger carrier was measured and declined. Bound in
  [skills/sepmo/binding-manifest.md](skills/sepmo/binding-manifest.md). Record:
  [task/ledgers/archive/2026-08/2026-08-23-dl-2-ledger-grammar-charter-ledger.md](task/ledgers/archive/2026-08/2026-08-23-dl-2-ledger-grammar-charter-ledger.md).
  **DL-3 merged as [#225](https://github.com/TRO-Wolf/repark/pull/225):** archive month maps are
  an index, not a book — one line per ledger (owner ruling 2026-08-23: the record is the ledger;
  git history keeps the long rows), `_condense_row` in the lifecycle script, the 2026-08 map
  55.5 kB → 29.3 kB, and the maps now say they are off the normal read path. Record:
  [task/ledgers/completed/dl-3-archive-map-compaction-charter-ledger.md](task/ledgers/archive/2026-08/2026-08-23-dl-3-archive-map-compaction-charter-ledger.md).
  **[#223](https://github.com/TRO-Wolf/repark/pull/223)** (owner-granted, not a slate unit)
  dual-wired the map-link, ledger-lifecycle, and ledger-grammar guards into `ci.yml`'s
  `guards` job (`fetch-depth: 0` so the frozen-bin diff has `origin/main`).

- **Python convention conformance (PYC)** (chartered 2026-08-21 by the owner; **PYC-1
  merged as [#204](https://github.com/TRO-Wolf/repark/pull/204)**; **PYC-2
  merged as [#207](https://github.com/TRO-Wolf/repark/pull/207)**; **PYC-3
  merged as [#208](https://github.com/TRO-Wolf/repark/pull/208)**; **PYC-4
  merged as [#209](https://github.com/TRO-Wolf/repark/pull/209)**; **PYC-5
  merged as [#211](https://github.com/TRO-Wolf/repark/pull/211)** / `b966c9b`). Four Python rules the owner stated, now written into the contract:
  types on everything; Pydantic v2 `BaseModel` rather than `dataclasses`/`attrs`; no function
  defined inside another function; functions named as verb phrases for the work they do. The rules
  themselves landed in [AGENTS.md](AGENTS.md) "Python" and in all three tier manuals under
  [docs/skills/](docs/skills/map.md) with the guard that holds two of them, **merged as
  [#201](https://github.com/TRO-Wolf/repark/pull/201)** / `5f05d8c`; the conformance work is what
  remains.
  - **Measured debt (AST scan at guard arming 2026-08-21, not an estimate):**
    - *Types.* The shipped package is already clean — 2,170 functions, **zero** missing a return
      annotation, because Ruff's `ANN` rules are selected in [pyproject.toml](pyproject.toml) and
      gate CI. **PYC-4** split the tests glob so `python/repark-parity/tests/**` no longer
      inherits ANN201/ANN202 and annotated the ten returns in `test_compare.py`. **PYC-5**
      dropped unearned facade `ANN201` (isolated count 0); `ANN202` stays for private
      helpers. `scripts/` has zero unannotated returns.
    - *Pydantic.* **PYC-3** converted `spark/merge.py` and `spark/_csv_smart.py` to
      Pydantic v2 `BaseModel` (and added `pydantic>=2.10,<3` as the wheel's second hard
      runtime dep). **PYC-4** converted the 20 `python/repark-parity` dataclass files
      (`pydantic>=2.10,<3` on that package too). Remaining sanctioned row:
      `scripts/check_parity_live_dual_wire.py` (runs as bare `python3`, no venv pydantic).
    - *Nested functions.* **66** nested `def`s in 21 files at arming. **PYC-1** lifted
      the 35 the gate counted in `spark/dataframe/core.py` (23) and
      `spark/dataframe/plan_collapse.py` (12), plus `_emit_side` under `try:`.
      **PYC-2** lifts or pragmas the remaining 14 shipped nested defs across 10 files
      (plus `session_core.probe` under `if`); those ten EXCEPTIONS rows are deleted,
      not zeroed. **PYC-4** emptied `NESTED_DEF_EXCEPTIONS`: walkers/factories/flush/
      execute lifted; signal handlers, shrink predicate, spy, and dual-wire comparator
      ended as `# nested-def:` pragmas. Dataclass remaining after **PYC-4**: 1 row.
    - *Docstrings.* **PYC-6** armed presence-only (`D101`/`D102`/`D103`/`D105`/`D107`)
      over the same SCAN_ROOTS as the conventions guard, tests excluded. Seeded
      2026-08-22 at **136** findings across **39** files (the slate's ~266 included
      tests). Style `D` (`D401`/`D202`/`D205`/`D413` and the rest) declined
      permanently — facade docstrings mirror PySpark. `PL` / `A` / `print()` stay
      declined with the reasons recorded in [briefs/next-sequence.md](briefs/next-sequence.md)
      (the declined-armings record; PYC-6 left the rolling queue).
    - *Names.* Not machine-countable; it rides along with whatever the other three touch.
  - **The guard is armed** (owner ruled 2026-08-21). Ruff has no check for a nested `def` and none
    for "Pydantic rather than `dataclass`", so those two rules now live in
    `scripts/check_python_conventions.py`, dual-wired `make check-python-conventions` (in the
    `make ci` chain) + ci.yml's `python` job. **Not** on the pre-commit hook as of PYC-5. The
    measured debt above is seeded into its two EXCEPTIONS tables, so the tree is green today and
    **cannot get worse**: a new nested `def` or a new `dataclass` import is red on `make ci` / CI.
    PYC is now the burn-down of those tables rather than a rule nobody can enforce. **PYC-5**
    re-measured the hook at n=5 median **0.996 s** (max 1.011 s) over **164** files — at the
    sub-second budget line, with the max already over it — and dropped it from pre-commit; it
    stays dual-wired in `make ci` + CI. **PYC-6** added `scripts/check_docstring_presence.py`
    for public-docstring presence, dual-wired `make check-docstring-presence` + ci.yml's
    `python` job, and on the pre-commit hook (n=5 median **0.13 s**). Ruff `ANN` still
    holds types; naming stays review.
  - **The nested-`def` rule ships with an inline pragma**, `# nested-def: <reason>`, for the three
    cases the contract sanctions: a decorator closing over its own arguments, a callback whose
    closure over local state is the point, and a `functools.wraps` wrapper. An empty reason does
    not pass. Seeded rows that ended as pragmas: PYC-2 `udtf._build` and `types.py` `verifier`;
    PYC-4 the TPC-H/TPC-DS and census SIGALRM handlers, the fuzz shrink predicate, the
    harness spy, and the dual-wire `field` comparator.
  - **Sequenced** in [briefs/next-sequence.md](briefs/next-sequence.md) as PYC-1 (merged,
    the two DataFrame modules), PYC-2 (merged: remaining shipped nested defs; the
    `udtf` builder and `types.py` verifier ended as pragmas, not lifts), PYC-3
    (merged as [#208](https://github.com/TRO-Wolf/repark/pull/208): the two shipped
    `dataclass` containers → `BaseModel`; accepted-input set pinned; pydantic
    becomes a wheel hard dep), PYC-4 (merged as [#209](https://github.com/TRO-Wolf/repark/pull/209):
    the parity harness and `scripts/`, plus narrowing the `ANN` per-file ignores),
    and PYC-5 (merged as [#211](https://github.com/TRO-Wolf/repark/pull/211): close —
    hook off pre-commit, unearned facade ANN201 dropped, dual-wire dataclass row
    stays the sanctioned leftover), and PYC-6 (merged as
    [#216](https://github.com/TRO-Wolf/repark/pull/216): public-docstring
    presence `D101`/`D102`/`D103`/`D105`/`D107` armed with a seeded ratchet;
    style `D` declined permanently). No further chartered PYC unit. The dual-wire
    dataclass leftover and the D-presence EXCEPTIONS table are remaining debt,
    not sequenced work.
  - **Rationale and the arming method are a portable skill**,
    [.agents/skills/code-quality/SKILL.md](.agents/skills/code-quality/SKILL.md): each rule with the failure it
    prevents and whether it is held by a linter, a gate, or review, plus the ratchet pattern for
    arming a convention against a codebase that already violates it.
  - **The risk this campaign carries is that it is a pure refactor of working code.** The facade
    suite at arming was 3,639 passing tests, none of them about where a `def` sits; PYC-1
    and PYC-2 add layout pins for that, PYC-3 pins the accepted-input set of the two
    shipped containers, PYC-4 pins EXCEPTIONS identity plus the CensusRow type check
    (`test_id: str`; dummy denominator ids are strings). Lifting a closure changes what
    it can see; converting a `dataclass` to a `BaseModel` adds validation that was not
    running before and can reject input the old container accepted. The invariant to
    hold is the LRS one: no query that worked before returns a different value.

- **Low-risk sweep (LRS)** (chartered 2026-08-20, delivered; branch `fix/low-risk-sweep`,
  eleven commits, **merged as [#191](https://github.com/TRO-Wolf/repark/pull/191)** / `8c660f6`).
  Chartered off `feat/spark-function-parity` @ `8a28057`; rebased onto `main` on 2026-08-21 when
  that campaign squash-merged as `65bacdf`, tree-identical both before and after. Works the
  sub-floor remainder the two Critic rounds forwarded. Design: [docs/design/low-risk-sweep.md](docs/design/low-risk-sweep.md);
  slate: [briefs/low-risk-sweep.md](briefs/low-risk-sweep.md); approval gate (10/10 `PROVEN`):
  [task/lrs-0-charter-ledger.md](task/ledgers/archive/2026-08/2026-08-21-lrs-0-charter-ledger.md).
  - **Delivered:** LRS-5 (canonical Rust module layout — all six `#[path]` sites gone), LRS-1
    (four facade paths refuse a higher-order column instead of leaking a DataFusion internal),
    LRS-2 (argument contracts matched to Spark), LRS-7 (a window with no `ORDER BY` frames the
    whole partition), LRS-3 (registry rows `RAND-1` / `BL-8` landed with pins; `randstr` batch
    bound; the SQL door learned `approx_count_distinct`), LRS-6 and LRS-4 (measurement +
    registration).
  - **The campaign's invariant held:** no query that worked before returns a different value.
    Every change turns a failure into a better failure, or registers something already decided.
  - **A live PySpark 4.1.2 + JVM oracle** is installed on this machine and was used to scope every
    unit. It refuted **three** of the Critic round's suggested fixes and one of my own — see
    design §7. It is not a build dependency and CI cannot reach it; every answer it gave is
    transcribed into the ledger that used it.
  - **Two silently wrong answers found and registered, not fixed** (each changes what a working
    query returns, which the charter forbids): `RE-1` — `regexp_extract_all(str, regexp)` returns
    capture group 0 where Spark returns group 1, on both doors; `LOG-1` — `SELECT log(x)` through
    the SQL door returns DataFusion's base-10 answer where Spark returns the natural log. Both are
    ordinary calls on common functions. **Both went to the owner, who ruled on 2026-08-21:**
    `RE-1` closes (SEM-1, below), `LOG-1` is **tabled** and keeps its row.

- **The Spark semantics fixes (SEM)** (chartered 2026-08-21, **gate ruled the same day**). First
  four units **MERGED** as [#192](https://github.com/TRO-Wolf/repark/pull/192) / `f3eaa9d`; SEM-6
  followed as [#193](https://github.com/TRO-Wolf/repark/pull/193) / `a547905`. Charter and
  measured scope: [task/sem-0-charter-ledger.md](task/ledgers/staging/sem-0-charter-ledger.md).
  - **The owner's rulings:** 2026-08-21 — `RE-1` closes, **`LOG-1` is TABLED** and keeps its
    BACKLOG row, the adjacent defects and the message work go ahead. Then, after `RE-3` was
    registered: close that one too.
  - **Delivered:** SEM-4 (the regexp refusals carry Spark's `REGEX_GROUP_INDEX` condition, and the
    four regexp kernels stop naming each other in their own planning errors), SEM-1 (`RE-1` closed
    — the two-argument `regexp_extract_all` defaults to capture group 1 on both doors), SEM-3 (the
    string-`idx` regression), SEM-5 (`RE-3` split out of `RE-2`), SEM-6 (**`RE-3` closed** —
    `regexp_substr` returns NULL for a zero-width match). Both closed rows are retired from the
    registry; `RE-2` keeps only the surrogate-bound count divergence it was always about.
  - **SEM-2 (`LOG-1`) is tabled, not dropped.** Its measured scope stays in the charter: a new
    dual-arity, null-guarded `log` kernel over DataFusion's `LogFunc` (`datafusion-spark` 54.1.0
    ships no `log`), which would also move the C-012 ratchet from 24 rows to 23. **`F.log`'s
    missing two-argument overload is tabled with it** — the only kernel available for it today is
    the one without Spark's null-guard, so shipping the overload alone would trade a crash for an
    answer that is silently wrong on six edges.
  - **This campaign changes what a working query returns**, deliberately and for the first time
    since the port. That is the whole reason the LRS registered `RE-1` rather than fixing it.
  - **`RE-3` was registered by SEM-5 and closed by SEM-6**, in that order and on purpose: it is a
    value change, so it was measured and written down before it was fixed, and the owner ruled on a
    row rather than on a proposal. It exists at all because a **draft SEM-1 assertion read repark's
    own answer back as if it were Spark's** and was checked against the oracle before the pin was
    committed — the exact failure `docs/testing.md` names, caught one step before it would have
    been pinned as truth.

- **Iceberg maintenance wave (MW)** (chartered 2026-08-21; **closed by MW-5**). Merge-on-read
  was production-grade as a *write* path and fenced off as an *operational* one: the maintenance
  procedures refused on exactly the catalogs holding production data. Design:
  Design and slate:
  [docs/history/iceberg-maintenance-wave/](docs/history/iceberg-maintenance-wave/README.md)
  (archived 2026-08-23). Charter:
  [task/ledgers/completed/mw-0-charter-ledger.md](task/ledgers/archive/2026-08/2026-08-23-mw-0-charter-ledger.md).
  - **Delivered:** MW-0 the measured charter ([#195](https://github.com/TRO-Wolf/repark/pull/195)),
    MW-1 the fence lifted for both catalog policies plus Spark's six-column `expire_snapshots`
    ([#196](https://github.com/TRO-Wolf/repark/pull/196)), MW-2 `rewrite_position_delete_files`
    and Spark's fifth `rewrite_data_files` column
    ([#197](https://github.com/TRO-Wolf/repark/pull/197)), MW-3 `remove_orphan_files`
    ([#198](https://github.com/TRO-Wolf/repark/pull/198)), MW-4 Glue live MOR compact+expire
    ([#218](https://github.com/TRO-Wolf/repark/pull/218)), MW-4b Glue dotted metadata-table
    rewrite ([#219](https://github.com/TRO-Wolf/repark/pull/219)), MW-6 `rewrite_manifests`
    (post-campaign, owner-chartered 2026-08-23). **Six maintenance
    procedures** run through `CALL`; no procedure omits a Spark column. V3-1 adds
    `register_table` (adoption, not maintenance).
  - **Scorecard.** The MW-0 growth demo reproduces: ten sequential MERGEs into a 1,000-row v2
    merge-on-read table, each touching the same 200 ids, grow position-delete files **1→10**.
    After `rewrite_position_delete_files` + `rewrite_data_files` + `expire_snapshots`, delete
    files are **10→1** and data files are **1**. Pin:
    `python/repark/tests/test_mw5_baseline_delta.py::test_mw0_demo_delete_files_grow_then_compact_reclaims`
    (`assert` 10 then 1 deletes; `assert` 1 data file after rewrite+expire). `COUNT(*)` stays
    **1,000** (`int64`) on the Arrow path. Data-file count *before* compact was **41** on this
    host (2026-08-23, logged, not asserted). Wall-clock on this host (not a CI pin): merge 2
    **56.1 ms**, merge 10 **131.3 ms** (**2.3×**, MW-0 was 60.1→127.9 ms / 2.1×), warmed
    post-maintenance **96.6 ms**.
    Scan cost still tracks delete-file growth; compact reclaims the files. It does not restore
    merge-2 wall-clock on this machine, and MW-5 does not claim a timing SLA.
  - **Live Glue proof.** Post-#219 `aws-acceptance` dispatch
    [32640855145](https://github.com/TRO-Wolf/repark/actions/runs/32640855145) on `d3c248c`
    (2026-08-23 12:56Z) is green. OD-3 is owner-executed `s3:DeleteObject` on the warehouse
    scratch prefix. Glue tables still cannot be dropped. **S3 Tables MOR compact+expire is
    out of this campaign** (OD-3 is the Glue warehouse prefix). The 2026-08-23 intake's
    "MW-4b" candidate (S3 Tables MOR leg, needs OD-3b) is a **different id** from campaign
    MW-4b (#219) and is not sequenced.
  - **Divergences that remain rows**, not closed here — `MOR-2`, `ORPHAN-1`,
    `ORPHAN-2`, `B-MOR-3`, and MW-6's `MANIFEST-1` (delete manifests are not rewritten; Spark
    rewrites them in a second leg), `MANIFEST-2` (`spec_id` refuses; `use_caching` is an
    accepted no-op and takes a boolean literal where Spark also casts a string) and
    `MANIFEST-3` (above `commit.manifest.target-size-bytes` the two engines write a different
    number of manifests, so `added_manifests_count` diverges; the rewritten count matches), and
    MW-7's `RDF-1` (a correctly sized data file whose rows are all deleted is never a
    `rewrite_data_files` candidate, so its dead rows and the delete file covering it are
    retained without bound; Spark reclaims both) in
    [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md). `MOR-1` retired at
    RP-1 (fork F-1, floor 5). The two result-schema
    gaps the charter queued for MW-5 were **closed in MW-1/MW-2**, not registered. Two of the
    remaining rows (`ORPHAN-1` required `older_than`, `ORPHAN-2` dry-run by default) invert
    Spark's defaults on the one procedure with no undo, under owner decision **OD-2**.
  - **A13** (merged [#217](https://github.com/TRO-Wolf/repark/pull/217)) set
    `register_memory_catalog`'s fallback root to the supplied warehouse. MW-3 still refuses
    orphan cleanup of that fallback tree.
  - **MW-7 scale scorecard (measured 2026-08-24, this host — ratios, not absolutes).**
    1e7-row partitioned v2 table, 50 MERGEs of 200,000 ids each (2 %), 8 partitions, a
    merge-on-read leg and a copy-on-write leg. **The charter said 100 merges; the measured
    projection put 1e7 × 100 at 3.72 h on top of 0.75 h already spent against a ~4 h budget,
    so it ran 1e7 × 50** — the arithmetic is §1 of the ledger. Run wall 2:09:29, **peak RSS
    4,461 MiB** (`getrusage` and `/usr/bin/time -v` agree).
    Merge-on-read grows exactly linearly per merge: **+8 position-delete files (one per
    partition — registry `MOR-2`), +200,000 delete records, +32 data files, +2 manifests,
    +478 manifest-list bytes**. Its predicate scans reach **4.18×** (point) and **4.58×**
    (partition) the copy-on-write control by merge 50, crossing **2× at 19.6 merges**. The
    copy-on-write control is **flat** over the same 50 merges (1.08× / 1.18×). The gap is the
    delete files **plus the data-file fan-out** merge-on-read leaves behind — at merge 50 it
    also carries 16.3× the control's data files and 1.83× its live bytes, because every MERGE
    appends rather than rewrites; this unit does not separate the two. Copy-on-write pays on
    write:
    MERGE plateaus at **~113 s** against merge-on-read's **~28 s** (4.1×), and its warehouse
    held **14,782 MB for a 342 MB table (43×)** until `expire_snapshots` ran.
    The full maintenance sequence took **142.4 s** on the merge-on-read leg (delete files
    400→8, data files 1,696→170, manifest list 25,665→3,659 B) and **21.2 s** on the
    copy-on-write leg. **It does not close the gap:** 8 delete files holding
    10,000,000 records survive and the table still reads at **2.45× / 2.02×** the control while
    holding 1.90× its live bytes. They are **not dangling** — they name live data files.
    `rewrite_data_files` never selects a delete-laden file, because the fork at `5e7b2e4` defers
    Java's `tooHighDeleteRatio` clause (`DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`) and defaults the
    delete-count threshold to `usize::MAX`, so a correctly sized 100 %-dead file is invisible to
    compaction and its dead rows are retained without bound. Spark ends the same sequence at
    **zero** delete files at both `write.delete.granularity` settings with
    `remove-dangling-deletes` off. Registry row **`RDF-1`**, fork ask **F-16**, ledger finding
    F-MW7-1 (OPEN), pinned by `test_delete_laden_in_band_file_survives_the_runbook`. Driver: `python/repark-parity/bench/mw7/`; machinery pin:
    `python/repark/tests/test_mw7_scale_smoke.py`. Ledger:
    [task/ledgers/completed/mw-7-scale-measurement-ledger.md](task/ledgers/completed/mw-7-scale-measurement-ledger.md).
    **The verdict the charter asked for: MW-9 is urgent** — the point probe goes
    **858 → 3,878 ms** for a predicate returning 0.02 % of the rows, because partition
    granularity forces open every delete file in every partition it touches (400 files,
    10,000,000 records, for 2,000 rows returned). MW-8's defaults follow from §6 there: run the
    sequence every 10 merges, with merge 20 the ceiling that already measures 2.05×.
  - **Sequenced remainder (owner-chartered 2026-08-23):** RP-1, MW-6 and MW-7 are delivered;
    MW-8 runbook → V3-2 create-v3 opt-in remain.
    Order and reasoning: [briefs/next-sequence.md](briefs/next-sequence.md). MW-9 is
    **unsequenced but no longer ungated** — MW-7's numbers answered its gating question
    "yes" on 2026-08-24; entering it in the queue is an owner call. The intake S3 Tables
    MOR leg stays unsequenced.

- **Format-v3 track** (roadmap **A12** in
  [task/roadmap-intake-2026-08-21.md](task/roadmap/mid-term/roadmap-intake-2026-08-21.md), owner-scheduled
  2026-08-21; V3-0 audit merged; **V3-1 merged** as
  [#203](https://github.com/TRO-Wolf/repark/pull/203) / `d3152b1`).
  **The owner set the v1.0 north star on 2026-08-23: full production-grade format-v3.**
  Definition and acceptance gate:
  [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md).
  Design: [docs/design/format-v3-track.md](docs/design/format-v3-track.md); audit:
  [task/v3-0-charter-ledger.md](task/ledgers/staging/v3-0-charter-ledger.md).
  - **V3-0** ([#199](https://github.com/TRO-Wolf/repark/pull/199)) ran the surfaces A12 had only
    read. Two claims were too pessimistic: **reading** a Spark-written v3 table with Puffin
    deletion vectors is already correct (857 rows, `sum(id) = 428429` — Spark's numbers exactly),
    and **appending** is correct including the row lineage v3 mandates, round-tripped through
    Spark. Neither had been claimed.
  - **One surface was wrong and is now guarded.** `rewrite_data_files` had no format-version check
    and reassigned every row's `_row_id` while returning correct rows, where Spark carries lineage
    through unchanged. Reachable on a v3 table already sitting in a Glue catalog, which is the
    drop-in case. Registry row `V3-LINEAGE-1` — **stricter than Spark on purpose, reversible in
    one line** if the owner would rather match it. The underlying fix is fork work.
  - **Queued, not forced:** `V3-DANGLE-1` (made unreachable by the guard), `V3-ROWID-1` (V3-4
    owns row lineage).
  - **V3-1 delivered:** `CALL system.register_table` is wired (Spark's two arguments and
    three nullable BIGINT columns, measured from the 1.10.0 jar); a Spark-written format-v3
    fixture is checked in so CI can load Puffin vectors with no JVM; `B-MOR-3` and
    `V3-ADOPT-1` are admitted rows. S3 Tables still refuses `register_table` in the fork
    (`FeatureUnsupported`); this engine does not swallow that. **MW is closed**; V3-2 is
    sequenced on [briefs/next-sequence.md](briefs/next-sequence.md) (RP-1 lands with this
    change).

- **Performance campaign — TA parity with `polars_talib` (chartered 2026-08-15; measure-first).**
  Goal added to [PROJECT.md](PROJECT.md) Goals. Phase 0 is the recorded benchmark baseline (the
  perf note's §8 battery: kernel race, many-symbols scaling, wide serving SELECT, batch-size
  sweep, null_lookback cost, last-row collect; plus flamegraph/heaptrack and a bench-only
  safe-vs-unchecked ceiling microbench). Implementation slates (multi-slot cache, null-free
  borrow, single Arrow write, short-partition early-out) are GATED on those numbers; the perf
  note's §7 do-not list (no math reordering, goldens bit-exact) is binding; `unsafe` remains
  workspace-forbidden.

- **Spark function parity campaign** (active, chartered 2026-08-20; first tranche **MERGED
  2026-08-21** as [#190](https://github.com/TRO-Wolf/repark/pull/190) / `65bacdf` — thirteen
  commits squashed into one, two adversarial Critic rounds). Close the `pyspark.sql.functions`
  gap and move the semantics behind every name out of Python into Rust. Design:
  [docs/design/spark-function-parity.md](docs/design/spark-function-parity.md); slate:
  [briefs/spark-function-parity.md](briefs/spark-function-parity.md); approval gate (12/12
  `PROVEN`): [task/fnp-0-charter-ledger.md](task/ledgers/staging/fnp-0-charter-ledger.md); measured evidence:
  [task/fnp-0-census/](task/fnp-0-census/map.md).

  **Delivered so far** — `__all__` 333 → 360, **41 names** moved from refusing-or-absent to
  working: two-door kernel parity (FNP-1), the null-ordering corners and five free names (FNP-2),
  eleven de-stubs whose kernel already shipped (FNP-3), the higher-order/lambda seam with `exists`
  (FNP-4a), thirteen aggregates the facade could not reach (FNP-5), and six new kernels
  (FNP-6a/b/c). Thirty-six of the 41 needed **no new kernel** — the engine was already more
  capable than the facade could reach, and that seam is now exhausted.

  **Remaining work, in the recommended order** (design §7 carries the full unit table; this is the
  sequence, revised 2026-08-20 on measured evidence):

  1. **FNP-15 + FNP-16 — register what will not be built (62 names).** Moved EARLIER than the
     original plan. These need refusing stubs and divergence-registry sections, not kernels, and
     they turn `AttributeError` (reads as "repark is broken") into a stated limit. Largest honesty
     gain per unit of work in the campaign, and what closes charter clause C-009.
  2. **FNP-4c — the eight higher-order kernels** (`transform`, `filter`, `aggregate`, `zip_with`,
     the four map forms) plus `forall` and `reduce`. The family users notice most; the seam it
     needs already shipped in FNP-4a.
  3. **F-Y10-1 (integer wrap) — not this campaign's unit, but its next dependency.** Measured
     2026-08-20: `CAST(2147483647 AS INT) + 1` returns `2147483648`, where Spark raises
     `ARITHMETIC_OVERFLOW`. A wrong answer on ordinary addition outranks any missing function, and
     it is what blocks FNP-7b.
  4. **FNP-7a** (8 `try_*` names whose raising path exists), **FNP-9** (collections/generators),
     **FNP-10** (JSON).
  5. **FNP-8 — repatriation of the 55 non-compliant facade functions.** The strategic goal, and
     deliberately late: no user sees a difference the day it lands, it prevents future defects
     rather than fixing current ones.
  6. **FNP-11** (timestamp/TIME — needs a design pass first; entangled with the open TZ rows),
     **FNP-12** (remaining aggregates + numeric formatting).
  7. **Deferred with reasons, not dropped:** FNP-6d (three `bitmap_*_agg` — UDAFs needing Spark's
     exact 4096-bit layout, unverifiable without a live Spark, least-used names in the gap);
     FNP-13 (collation / G15 retirement); FNP-14 (crypto — needs a new cipher dependency for four
     names); FNP-4b (the Spark-door dialect — blocked on making the engine's own generated SQL
     dialect-independent, which is a write-path change).
  8. **FNP-Z — close-out:** `__all__` completion, census re-run, STATUS truth-up, the
     dispatch-table module split, and the registry rows handed forward by FNP-3 (`arrays_zip`,
     `json_tuple`) and FNP-6c (the UTF-8 value-representation difference). The `#[path]` conversion
     is **done** (LRS-5); FNP-6a's empty-pattern residual is decided and scheduled as LRS-6; the
     `randstr` cap is disposed of as registry row
     [RAND-1](docs/spark-sql-iceberg-parity.md#rand-1--randstr-refuses-a-length-spark-accepts). The FNP-5 unsigned-count row is
     **closed at the facade** and the door half is disposed of as registry row
     [BL-8](docs/spark-sql-iceberg-parity.md#bl-8--sql-door-count-like-aggregates-return-uint64),
     which is where its semantics now live. The `approx_count_distinct` door-name gap is
     **closed** (LRS-3).

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

Parked lanes: **none.** The `repark.sql` re-home lane closed 2026-08-14 (#95 —
[docs/release.md](docs/release.md) "RESOLVED"; design ruling
[docs/design/python-facade.md](docs/design/python-facade.md) §4).

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

- **`F.regexp_extract_all` rejects a string `idx` that every other door accepts** — measured
  2026-08-21. `F.regexp_extract_all(s, pattern, "1")` raises `AnalysisException: No field named
  "1"` — the string is read as a column name. Spark accepts it (`['a','b']`), repark's own SQL door
  accepts it, and repark's own sibling `F.regexp_instr(s, pattern, "0")` accepts it. A **regression
  from the FNP-6a critic remediation**: `task/fnp-6a-regexp-ledger.md` records the wrapper as having
  carried `lit_indices={1, 2}`, and the F-FNP6A-1 fix stripped `lit_indices` entirely instead of
  narrowing it to `{2}`. Defect with a scheduled fix, so it stays here rather than becoming a
  registry row. Fix it with, or immediately after,
  `RE-1` — same function, same remediation window, and its test pass is the cheapest place to catch
  it. Scheduled as SEM-3: [task/sem-0-charter-ledger.md](task/ledgers/staging/sem-0-charter-ledger.md).

- **`F.log` has no two-argument form** — measured 2026-08-21. PySpark's signature is
  `log(arg1, arg2=None)`, where the two-argument form is `log(base, x)`; repark's is
  `log(col)` only (`python/repark/src/repark/spark/functions_expr.py`), and
  `crates/repark-python/src/column/function_dispatch.rs`'s `"log" | "ln"` arm has no two-argument
  case at all, so `F.log(2.0, col)` fails in Python before reaching Rust. Distinct from
  [LOG-1](docs/spark-sql-iceberg-parity.md), which is about the SQL door's base; this is a missing
  facade overload. Natural to land in the same unit — a Spark-semantics `log` kernel is what both
  need. Scoped into SEM-2: [task/sem-0-charter-ledger.md](task/ledgers/staging/sem-0-charter-ledger.md).

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

## 2026-08-15 night increment (conductor-15 + Opus work group 2)

Eight more merged PRs. Engine: S-1 "spill truth" (#143 — runtime `memory_limit` now installs
a FairSpillPool so the "one truth" claim is TRUE; `temp_directory` refuses loud; RAM-relative
default `clamp(0.6 x detected, floor, 8 GiB)`; the spill regression battery landed), the M11
cardinality exemption (#140 — BL-3 retired; the last MERGE-audit divergence with a fix path
is closed), and the WI-1 store-assignment gate (#142 — INSERT OVERWRITE + append paths now
refuse un-assignable types; the four plain-INSERT doors need the WI-2 analyzer seam, named).
Debt service: column.rs -> column/ (ceiling 2200 -> 1850, #139) and udf.rs -> udf/ per-family
(2200 -> 2100, #141) — both ratcheted DOWN; the CDL Int32 prerequisite is now unblocked.
Perf: iterator-form rsi/sma, bit-exact (#138). Fork: Java battery increment 2 (#199, entries
+ readable_metrics; F-1 leaf-doc finding recorded). Recon discharged: dbt-repark (P0 session
lifetime found), fork Partitioning unification (PT-0 positional-walk corruption finding),
G6-3/G6-5 cast design.

## 2026-08-15 evening increment (conductor-14 + Opus work group)

Six more merged PRs: the five deferred window functions land (`lag`/`lead`/`nth_value`/
`percent_rank`/`cume_dist`, #133 — functions surface 291 -> 296; `column.rs` is now at its
2200 ceiling, operator-group extraction due before any further growth); the BL-4 UPDATE-path
store-assignment gate (#135) and the BL-5 abort-path cleanup (#134, with a
`CommitStateUnknown` carve-out so ambiguous commits are never corrupted) — both registry rows
retired; the M11 Spark golden RECORDED (#131, answering the audit's open question: Spark
deletes, repark refuses — the fix unit is now unblocked); and the perf baseline batteries
(#132 criterion kernels, #136 the six pipeline benches). Orchestrator profiling: TA kernels
measured at ~5% of engine time (window-exec 37% / sort 34% / arrow glue 21%) and the
unsafe-Rust ceiling measured at <=0.4% and rejected — slate priority is plan-shape work.
Also discharged by the Opus group: the spill-coverage spike (unit S-1 chartered) and the
CDL Int32-lane design.

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

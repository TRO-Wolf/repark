# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> what is delivered, what is in flight, and what is deferred. Intent and the "why" live in
> [PROJECT.md](PROJECT.md) (product charter) and [docs/adr/](docs/adr/) (load-bearing decisions);
> the day-to-day contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agents/](.agents/map.md) as thin tool adapters that carry no authoritative facts). When a current-state
> fact changes, it changes **here** — other files point at this file, they do not restate it.

_Last updated: 2026-09-03._

## Release state

**v1.0.0 shipped (2026-09-03)** — the first stable tag, the ninth on proven machinery (v0.6.0:
2026-08-31; v0.5.0: 2026-08-20; v0.4.0: 2026-08-19; v0.3.0–v0.3.2: 2026-08-16; v0.1.0 / v0.2.0:
2026-08-15): tag-triggered `release.yml`, PyPI trusted publishing, `cp312-abi3` manylinux wheel,
wheel-only (crates.io publishing structurally deferred, see docs/release.md), version SSOT at the
Cargo workspace (`1.0.0`). v1.0.0 is the format-v3 north star at its gate: all twenty §3 rows of
[the north star](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md) ✅ or dated DECLARED
(V1-GATE #320, V3-COV #321), the legacy-delete chain closed end to end (V3-12 → fork F-21 /
F-22 → RP-8), every unit oracle-measured against live PySpark 4.1.2 + Iceberg 1.11.0 before
it was built. From this tag the API freeze binds:
additive-only within the major for every frozen row of
[v1-0-api-freeze.json](docs/design/v1-0-api-freeze.json) (owner ruling 2026-09-03: cut at the
gate, ahead of the 0.x ladder). Release mechanics: [docs/release.md](docs/release.md).

## Delivered capabilities

**Milestone one — the private-v1 → public-v2 port — is COMPLETE and merged to `main`
(2026-08-08)** (PRs #16, #18–#23). Four phases delivered 2026-08-06/08: bootstrap (governance,
testing contract, mechanical gates, tier-1 CI), engine core, the two SQL doors, and the Python
facade + parity harness. The full record — phase briefs, the seventeen unit ledgers, the
retrospectives — is archived at [docs/history/port-v2/](docs/history/port-v2/README.md).

**Nine crates are delivered** (workspace SSOT: root `Cargo.toml`; navigation:
[crates/map.md](crates/map.md)): `repark-common`, `repark-core`, `repark-iceberg`,
`repark-functions`, `repark-spark`, `repark-sql`, `repark-ta`, `repark-ml`, `repark-python`. The
Python tree ships `python/repark` (the PySpark facade wheel) and `python/repark-parity` (the
differential harness). The published wheel is in [Release state](#release-state) above.

**Acceptance:** the v2 test census is byte-flat against the port-source pin baseline
`fc3f48102`, exit 0 on all four cohorts. The per-cohort counts are
[docs/history/port-v2/](docs/history/port-v2/README.md) "Result at acceptance"; the procedure and
the DL-1 eviction of the evidence trees (reachable at `main` `b13b22c`) are
[docs/port/census.md](docs/port/census.md) §7; the baseline's
[facade cohort](task/census/baseline-fc3f48102/facade/map.md) stays in the tree because the
deferred-ledger tests read it, and the deferred and added acceptance inputs are
[task/port/](task/port/).

## Current milestone

**Milestone one is COMPLETE** and there is no in-flight *port* work; its record is under
[Delivered capabilities](#delivered-capabilities) above.

**Standing decision: the private v1 predecessor is bugfix-only, and this repository is the sole
forward target.** New engine work happens here. v1 receives fixes only, and a defect both engines
share is fixed there and re-ported rather than patched only here.

What happens next, in order:

1. **Agent-Agnostic Front-Door** — **DONE (2026-08-10).** Record:
   [docs/history/frontdoor/](docs/history/frontdoor/README.md); metrics:
   [task/metrics.md](task/metrics.md).
2. **V2 Engine Hardening** — the active engine campaign; its state is the H-2 entry in
   [Active workstreams](#active-workstreams). Wave landing records (Y/Z/W/V/S, 2026-08-13/14)
   are the `z5` / `w5` / `v5` / `s5` increment ledgers indexed in
   [task/ledgers/archive/2026-08/map.md](task/ledgers/archive/2026-08/map.md).
3. **Production-pipeline cutover inventory** — which workloads move, in what order, under
   **single-writer-per-table**, with each rollback story. Carried from
   [docs/port/PLAN.md](docs/port/PLAN.md) "Open item: cutover".
4. **The first tagged release** — **DONE**: see [Release state](#release-state). API review
   answered 2026-09-02 (`R0 yes`, every row decided at its recommendation); freeze pinned — 888
   names in [docs/design/v1-0-api-freeze.json](docs/design/v1-0-api-freeze.json), policy in
   [docs/release.md](docs/release.md) "Versioning policy"; v1.0.0 cut 2026-09-03 on the
   north-star gate line V1-GATE wrote the same day.

Owner-side actions that rode this sequence are **DISCHARGED — no owner-side tier-2 action
remains** (aws-acceptance green 2026-08-10; the parity-live half on first-run evidence; three
stale always-PASS Apache smoke pins are known-FAIL meta pins). Pre-scrub content stays reachable
in published history by explicit decision:
[p3e-facade-ledger.md](docs/history/port-v2/p3e-facade-ledger.md).

## Active workstreams

**The ordered queue across the open tracks is [briefs/next-sequence.md](briefs/next-sequence.md)**
(rolling, opened 2026-08-21). It states sequence and reasoning; the per-track state stays here.

<!-- ws id=dl ledgers=dl- state=open -->
- **Document lifecycle (DL)** (chartered 2026-08-23; DL-1..DL-5 delivered). Unit ledgers live in
  [task/ledgers/](task/ledgers/map.md) by state; `scripts/ledger_lifecycle.py` is the only mover.
  Three gates in `make ci` hold the class: `check-ledgers`, `check-ledger-grammar`,
  `check-docs-compaction`. Policy: [AGENTS.md](AGENTS.md) "Markdown document lifecycle".
  Records: [task/ledgers/archive/2026-08/](task/ledgers/archive/2026-08/map.md).
<!-- /ws -->

<!-- ws id=sem ledgers=sem- state=open -->
- **The Spark semantics fixes (SEM)** (chartered 2026-08-21). #192/#193 delivered SEM-1/3/4/5/6
  (`RE-1`/`RE-3` retired, `REGEX_GROUP_INDEX`, string-`idx`). Owner ruling 2026-08-31: both
  silently-wrong answers fix to Spark. This unit closes `LOG-1` (Spark-door natural `log`,
  dual-arity null-guard, `F.log` two-arg) and re-measures RE-1. `F.log` is an accept-more
  superset of PySpark's (a column base accepted, keyword names differ) — ledger C-006,
  oracle note under C-010.
  Ledger: [task/ledgers/archive/2026-09/2026-09-02-sem-1-spark-answer-parity-ledger.md](task/ledgers/archive/2026-09/2026-09-02-sem-1-spark-answer-parity-ledger.md).
<!-- /ws -->

<!-- ws id=v3 ledgers=v3-,v3e- state=open -->
- **Format-v3 track** — **the v1.0 north star (owner ruling 2026-08-23): full production-grade
  format-v3.** Definition and gate:
  [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md);
  design: [docs/design/format-v3-track.md](docs/design/format-v3-track.md); audit:
  [task/ledgers/staging/v3-0-charter-ledger.md](task/ledgers/staging/v3-0-charter-ledger.md).
  - **Measured true (V3-0, [#199](https://github.com/TRO-Wolf/repark/pull/199)):** v3 DV reads
    and lineage appends round-trip through Spark. **V3-5:** `V3-DANGLE-1` FIXED.
  - **Delivered** — the north-star §3 matrix is the per-row home; this is the merge record.
    V3-1 `register_table` + the v3 fixture ([#203](https://github.com/TRO-Wolf/repark/pull/203));
    V3-2 opt-in CREATE/CTAS `format-version = 3`
    ([#232](https://github.com/TRO-Wolf/repark/pull/232)); V3E-1 + V3E-2 adopted-v3 COW DML,
    `ENC-1` DECLARED ([#235](https://github.com/TRO-Wolf/repark/pull/235)); V3E-3 partitioned-DV
    and equality-delete fixtures ([#236](https://github.com/TRO-Wolf/repark/pull/236)); V3R-1
    recorded the 2026-08-25 owner rulings, every one now discharged; V3E-4 measured refs,
    `VERSION AS OF` over DVs, expire dual-probe, orphan floor.
    V3E-5 added the nightly v3 live-oracle leg
    ([#253](https://github.com/TRO-Wolf/repark/pull/253)); first green nightly 2026-09-02.
    RP-2 / RP-3 / RP-4 took the fork repins (`V3-ADOPT-1`, `V3-LINEAGE-1` FIXED); RP-6
    (2026-09-01) lifts UPDATE. V3-7 / V3-8 (2026-09-02) carry MERGE and subquery-`WHERE` COW
    `_row_id` and delete the refusal seat — `V3-COW-1` **FIXED**.
    **V3-4 (2026-08-31):** `_row_id` / `_last_updated_sequence_number` Spark-equal on
    single-table v3 reads (`V3-ROWID-1` FIXED, `V3-ROWID-2` refuses the rest).
    **V3-6 (2026-09-01):** opt-in v3 CREATE takes the fork's `timestamp_ns` types, append fills
    from a schema-carried `write_default`, and DEFAULT DDL / `unknown` / binary `variant`
    refuse Spark-equal (`V3-VARIANT-SHRED-1`).
    **V3-9 (2026-09-02):** predicate DML's V2-only gate is lifted — MoR `DELETE`/`UPDATE …
    WHERE` on v3 write file-scoped Puffin DVs on three doors, created and adopted, Spark-equal
    (`V3-MOR-1` FIXED). **V3-10 (2026-09-02):** the in-place v2→v3 upgrade lands on three doors
    (`V3-UPGRADE-1` FIXED). **RP-7 (2026-09-02):** the fork repin to `ff4764d3` (F-18) makes the shared-Puffin
    container close Spark-equal — `V3-DV-1` **FIXED**.
    **LIVE-v3 (2026-09-02):** both live v3 legs green on `aws-acceptance` run 33635288918
    (`S3T-V3-1`), re-dispatched 2026-09-03 (run 33699342417) under V3-11's exact `_row_id`
    assertion. **V3-11 (2026-09-02):** one commit's data files reach the manifest in ascending
    partition order, so the MoR MERGE insert's `_row_id` is deterministic and Spark-equal
    (`V3-ROWID-3` FIXED); Spark's order is a Java `HashMap` bucket artefact, so wider
    partition sets differ (`V3-FILEORDER-1` DECLARED).
    **V3-12 (2026-09-02):** a legacy position delete merges into the new DV; the close reads
    its branch (`V3-UPGRADE-DV-1` FIXED, `V3-DV-BRANCH-1`). **RP-8 (2026-09-03):** repin to
    `c1d6c9de` (F-19..F-22) — the close owns that merge in one manifest pass and `FanoutWriter`
    drains ascending, so RePark's 493-line walk goes and `V3-UPGRADE-DV-PLAIN-1`,
    `V3-UPGRADE-DV-PART-1`, `V3-COV-3`, `F-v3-10-partition-file-order` are **FIXED**.
    **SCALE-v3 (2026-09-02):** the MW-7 `1e7 x 50` workload re-measured on v3 at the same knobs
    — **96 delete files against v2's 400** and 496 data files against 1,696, because a v3 delete
    is one Puffin vector per data file rewritten in place; the maintenance sequence ends at
    **zero delete files and zero delete records** where v2 kept 8 files and 10,000,000 records,
    and `rewrite_data_files` reclaims all 96 alone. The point probe reads at 0.64x v2 on a cell
    whose copy-on-write control moved 1.00x; every write-side ratio is cross-run and
    uncontrolled. Numbers:
    [scale-v3-mw7-ledger.md](task/ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md) §3.
  - **The gate is audited (V1-GATE, 2026-09-03) and §2 pillar 4 is discharged (**V3-COV**,
    2026-09-03):** all twenty north-star §3 rows are ✅ or carry a dated DECLARED residual with a
    pin (§3.1), and the statement matrix is measured — 81 programs, 267 cells, 72 EQUAL, 8 rows
    filed, 2 FIXED ([v3-statement-coverage.md](docs/design/v3-statement-coverage.md)). `B-MOR-3`
    FIXED 2026-09-03 (owner ruling: build); floor residue `B-MOR-3-FLOOR-1`.
  - **Next:** lineage carry and merge-on-read are complete on every served DML shape
    (`V3-COW-1`, `V3-MOR-1`, `V3-DV-1`, `V3-ROWID-3`, `V3-UPGRADE-DV-1`,
    `V3-UPGRADE-DV-PLAIN-1`, `V3-UPGRADE-DV-PART-1`, `V3-COV-3`,
    `F-v3-10-partition-file-order` FIXED); open v3 residuals are `V3-FILEORDER-1`,
    `V3-COV-4` / `V3-COV-5` / `V3-COV-6`, `V3-UPGRADE-V4-1`, `G3-E8`.
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
  [#190](https://github.com/TRO-Wolf/repark/pull/190),
  [#191](https://github.com/TRO-Wolf/repark/pull/191),
  [#192](https://github.com/TRO-Wolf/repark/pull/192), and
  [#193](https://github.com/TRO-Wolf/repark/pull/193)). Close the `pyspark.sql.functions` gap and
  move the semantics behind every name out of Python into Rust. Design:
  [docs/design/spark-function-parity.md](docs/design/spark-function-parity.md) (§7 carries the
  unit table and the recommended order); slate:
  [briefs/spark-function-parity.md](briefs/spark-function-parity.md); gate (12/12 `PROVEN`):
  [task/ledgers/staging/fnp-0-charter-ledger.md](task/ledgers/staging/fnp-0-charter-ledger.md);
  evidence: [task/fnp-0-census/](task/fnp-0-census/map.md).
  **Delivered:** `__all__` 333 → 360, 41 names from refusing-or-absent to working (FNP-1..6c);
  thirty-six needed no new kernel — that seam is exhausted. **F-Y10-1 (2026-08-30):** integer
  `+` / `-` / `*` raise `ARITHMETIC_OVERFLOW` where Spark raises; FNP-7b is unblocked.
  **FNP-4c (2026-08-31):** ten higher-order names on the FNP-4a seam. **FNP-7a/7b (2026-08-31):**
  twelve `try_*` inversions (NULL instead of raise). Remaining work ships as one coherent
  PR per unit or tightly coupled pair.
  **LOG1P-1 (2026-09-02):** `log1p` / `expm1` move to the precise kernels on both SQL doors and
  the facade, Spark-equal at the tiny-argument edge (`BL-15` FIXED).
  **Next, in order (revised 2026-08-31):** FNP-9/10 → FNP-8 → FNP-11/12 → FNP-Z.
  Deferred with reasons in the design: FNP-4b, FNP-6d, FNP-13, FNP-14. This campaign and TA
  performance consume no F-17 surface and may use fork-wait windows; neither gates v1.0.
<!-- /ws -->

<!-- ws id=h2 ledgers=h-,h2- state=open -->
- **V2 Engine Hardening** (active; recon complete; **H-1 archived mid-campaign 2026-08-11** at
  [docs/history/hardening-h1/](docs/history/hardening-h1/README.md); continues into H-2) —
  optimization across the native door, the Spark facade and the write path with the verification
  that proves each improvement. Design (goal, phases H-0…H-5, dated decisions):
  [docs/design/v2-engine-hardening.md](docs/design/v2-engine-hardening.md); slate:
  [briefs/v2-engine-hardening.md](briefs/v2-engine-hardening.md). **DFP-1 (2026-08-31):**
  preserve-null Unnest removes redundant projections; adjacent candidates stay
  measurement-gated. #30 (the dead doc-pointer sweep) merged.
<!-- /ws -->

<!-- ws id=dml ledgers=dml-,maint- state=open -->
- **Iceberg DML remainder (v0.6)** — DML-B / DML-C / DML-A / MAINT delivered 2026-08-30/31; the
  four units are described in [Release state](#release-state) above and in
  [the registry](docs/spark-sql-iceberg-parity.md).
  **REF (2026-09-01, RP-5):** writes to `t.branch_<name>` land (`REF-1` FIXED). WAP publish
  procedures and `spark.wap.*` stay BACKLOG (`REF-3`). Reads were already Spark-equal (`REF-4`).
  **RDF-1 (2026-09-02):** the position-delete writer stamps exact `file_path` lower/upper
  bounds, so the fork's delete-ratio clause sees a partition-granularity delete and
  `rewrite_data_files` selects the delete-laden file (registry `RDF-1`; residue `F-RDF1-1`).
  Ledger: [rp-5-fork-repin-ledger.md](task/ledgers/archive/2026-09/2026-09-01-rp-5-fork-repin-ledger.md).
<!-- /ws -->

Parked lanes: **none** (the `repark.sql` re-home lane closed 2026-08-14, #95 —
[docs/release.md](docs/release.md) "RESOLVED").

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

- **Identifier case folding** — **DECLARED (2026-08-10)**: registry
  [ID-1](docs/spark-sql-iceberg-parity.md); revisiting it needs a new dated decision.
- **The session-timezone family** — TZ-1 converted; TZ-6 / TZ-7 FIXED (#85); **TZ-8** partially
  FIXED (#100): `CAST(ts AS DATE)` / `to_date` / `datediff` read the session zone now; only
  `last_day` / `date_add` over a TIMESTAMP (+ B-TZ-3) stay BACKLOG; TZ-4 in progress
  (residue: ANSI column-def `timestamp_ns`); F-V4-1 / F-V4-2 DECLARED, fork-routed; TIMESTAMP→INT
  nullability BACKLOG (G6-4; the epoch-seconds class itself FIXED, #64). Semantics + pins: the
  registry's TZ rows, [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md).
- **decimal128** — DEC-2 / DEC-6 / DEC-7 / DEC-8 FIXED (#94 / #99); DEC-1 / DEC-3 / DEC-4 / DEC-5
  width FIXED; DEC-9 (and DEC-5 nullability) stay BACKLOG; TY-3 DECLARED. Registry §7 DEC-1 … DEC-9 in
  [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md).
- **Temporal-RANGE frames** — G5b-R4 OPEN (FOLLOWING-to-FOLLOWING; DF 54.1 range-search); R1 /
  R3 / R5 FIXED; the ANSI-door wrap is a named residual with no pin. Registry G5b rows.
- **DELETE/UPDATE subquery predicates** — the dbt-upgrade gate is MET (IN / NOT IN / `[NOT]
  EXISTS` ± correlation execute on both doors, plus uncorrelated identity UPDATE IN); `UPDATE NOT
  IN` / `[NOT] EXISTS` / correlated UPDATE IN / ANY / ALL stay valved;
  G3-E8-NULL's UPDATE half stays refused. Registry §7 G3-E8 / G3-E8-NULL.
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

**None.** v1.0.0 shipped 2026-09-03; the tag history is in [Release state](#release-state).
Future tags follow [docs/release.md](docs/release.md) (version SSOT at the Cargo workspace;
wheel-only; crates.io publishing structurally deferred).

# STATUS.md — current state of RePark

> **This file is the single source of truth for RePark's *present* state** — release state,
> delivery, in-flight and deferred work. Intent lives in [PROJECT.md](PROJECT.md) and
> [docs/adr/](docs/adr/); the contract is [AGENTS.md](AGENTS.md) (with [CLAUDE.md](CLAUDE.md) and
> [.agents/](.agents/map.md) as thin adapters). A current-state fact changes **here**; other files point here.

_Last updated: 2026-11-22._

## Now

As of 2026-11-22.

- **Shipped:** v1.5.7 ([Release state](#release-state)).
- **S1s in the 1.5.6 wheel, fixed in v1.5.7:** struct comparisons, native-door DataFrame writes, dynamic partitioned overwrites, casts over Parquet statistics, pre-1970 `unix_timestamp` ([Known correctness issues](#known-correctness-issues)).
- **Labels:** format-v2 WITHDRAWN; format-v3 core HARDENED-NARROW; nanosecond columns and struct writes EXPERIMENTAL ([Iceberg maturity](#iceberg-maturity-2026-10-27)).
- **In flight:** SIGNED-ZERO-NORMALIZE-1 part B part 2 (the nested-float write conditions), which blocks only the full v2 label (owner, 2026-11-17); parts A and B part 1 are on main as #1298 and #1326. The `UPDATE … WHERE v IN (w)` residue is carded as SIGNED-ZERO-UPDATE-IN-LIST-1. C-116 and C-118 closed.
- **Next:** the fixes for v1.5.8 ([Known correctness issues](#known-correctness-issues)).

## Release state

**v1.5.7 (2026-11-22):** the data-safety fixes for 1.5.6, listed in the [notes](task/roadmap/mid-term/v1-5-7-release-notes-2026-11-22.md). Tagged `v1.5.7` at `450a0dfe`; on PyPI with five wheels. Qualification: [record](task/roadmap/mid-term/v1-5-7-qualification-record.md).

**v1.5.6 (2026-11-08):** the empty-DML guard (EMPTY-DML-SHARED-GUARD-1, #1277: a DELETE or UPDATE whose condition folds to no rows writes nothing and reports zero rows), and the rest of the cycle, listed in the [notes](task/roadmap/mid-term/v1-5-6-release-notes-2026-11-08.md). Tagged `v1.5.6` at `ae60c359`; on PyPI with five wheels. The v2 production label is not part of this release (ruling A, 2026-11-08); the v3 label moves to a later release. Qualification: [record](task/roadmap/mid-term/v1-5-6-qualification-record.md).

**v1.5.5 (2026-10-13):** R-008 (every write door stores INSERT's instant into `timestamptz_ns`), the frame door (R-019), `current_date` in the session zone, the Postgres write core and routing, the merge module split. Matrix 705/132/5/0 (2026-10-13; also v1.5.1 to v1.5.4). Tagged `v1.5.5` at `7fb5f6b2` (re-tag, #1127). [Notes](task/roadmap/mid-term/v1-5-5-release-notes-2026-10-13.md). The re-tag also ships U-1, U-3, U-5, U-6, U-7 (reconciled 2026-10-22).

**v1.5.4 (2026-10-10):** R-007, ENC-1, the pyarrow floor. Tagged `v1.5.4` at `f4e424c1`. [Notes](task/roadmap/mid-term/v1-5-4-release-notes-2026-10-10.md).

**v1.5.3 (2026-10-07):** credential redaction, ATTR-ID-1, TA slices. Tagged `v1.5.3` at `7a8fcf1a`. [Notes](task/roadmap/mid-term/v1-5-3-release-notes-2026-10-07.md).

**v1.5.2 (2026-10-03):** TA-CHAIN-1, POLARS-IS-DUPLICATED-1. [Notes](task/roadmap/mid-term/v1-5-2-release-notes-2026-10-03.md).

**v1.5.1 (2026-09-29):** S3 path writes (U12), CASESENS-1, INTDIV-1. Tagged `v1.5.1` at `db3a1f37`. [Notes](task/roadmap/mid-term/v1-5-1-release-notes-2026-09-29.md).

**v1.5.0 (2026-09-27) — the Spark–Iceberg parity minor:** the gate is met on the 842-cell matrix, **705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT** (fresh-build rerun, main `f7422565`), with dated owner carve-outs C-1 to C-5. [Notes](task/roadmap/mid-term/v1-5-0-release-notes-draft-2026-09-27.md).

**v1.4.2 (2026-09-15):** DECIMAL-CACHE-1 (#632): decimal overflow past 38 digits types like PySpark 4.1.2 and materializes through the cache paths.

**v1.4.1 (2026-09-14):** the five-leg wheel set on PyPI; PLATFORM-2..4 stay slated for 1.6.

**v1.4.0 (2026-09-12) — the maintenance minor on 1.3**, additive: adaptive partitioning ([policy](docs/guide/maintenance-policy.md)), compaction net-size wins, measured profiles ([repark-toml.md](docs/guide/repark-toml.md)), `describe`/`summary`/`colRegex` as Spark 4.1.2.

**v1.3.0 (2026-09-11) — the Never-OOM minor:** spill-coverage matrix, 24 of 27 cells stable ([matrix](docs/perf/spill-coverage-matrix-2026-09-11.md)).

**History:** v1.1.1 (09-06, RDF-SCHEMA-EVO-1, WRITE-DISTRIBUTION-2, CSV-INFER-PERF-1); v1.1.0 (09-06, the first minor); v1.0.1 (09-04); **v1.0.0 (2026-09-03, the first stable tag)**, the format-v3 north star at its gate ([all twenty §3 rows](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md) ✅ or dated DECLARED); the API freeze binds from there, additive within the major ([v1-0-api-freeze.json](docs/design/v1-0-api-freeze.json)); v0.1.0–v0.6.0 (2026-08-15 → 08-31). Release mechanics: [docs/release.md](docs/release.md).

## Iceberg maturity (2026-10-27)

Levels: the [2026-10-13 re-rating](docs/artifacts/iceberg-rerating-2026-10-13.html)'s, against the [production-grade definition](docs/production-grade.md); evidence by pointer, main `105d4625`.

- **Format-v2 reads and writes: WITHDRAWN 2026-10-29; the 2026-11-05 [re-audit](docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md) ruled LABEL DOES NOT STAND (C-097): five findings block it (AUD-021 to AUD-025; AUD-024 unless the owner rules it oracle-class).** AUD-015 and AUD-016 are fixed (#1233, #1232; coverage check C-110), RP-64 is #1228, and the id-less positional card closed with #1246, its residue open as RP-65 C-010. Filed: the [third audit](docs/audits/iceberg-v2-label-targeted-reaudit-2026-11-01.md), the [second](docs/audits/iceberg-v2-label-independent-reaudit-2026-10-29.md) and the [first](docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md); the [label order](task/wo/ice-v2-label-1/order.md) records it. Evidence (signed 2026-10-27, PR #1195): the [parity registry](docs/spark-sql-iceberg-parity.md) (R-011, ICE-COMMIT-UNKNOWN-1, ICE-LOCAL-COMMIT-POINT-1), the closure ledgers [nested-literal-seam-1](task/ledgers/staging/nested-literal-seam-1-ledger.md) and [ice-replace-kill-doors-1](task/ledgers/archive/2026-10/2026-10-30-ice-replace-kill-doors-1-ledger.md), the integrity ledger [ice-integrity-pins-1](task/ledgers/staging/ice-integrity-pins-1-ledger.md), the [bound record](docs/perf/resource-bound-2026-10-26.md), and the cross-read leg of [aws-acceptance](.github/workflows/aws-acceptance.yml) (run 40295982749). Carried on the label: the hint walk, NESTED-LITERAL-SEAM-1, ICE-REPLACE-KILL-DOORS-1, and the replace residuals as declared. Carve-outs: ENC-1 items 1 and 5, ENC-CTAS-REPLACE-1 and NESTED-TEXT-LEAF-STORE-1 (fix 1.5.7). Owner 2026-11-02: Avro and ORC deletes excluded from this label; CORPUS-1 after RP-65.
- **Format-v3 core: HARDENED-NARROW** until V3-LABEL-1.
- **Nanosecond columns and struct writes: EXPERIMENTAL** (rated 2026-10-13; the owner's rating stands). Its 2026-10-13 reasons are partly superseded: U-1, U-3, U-4, U-6 and NS-TRUTHUP have landed (U-6 pins struct pairing in `python/repark/tests/writes/test_ice_struct_pairing_1.py`), and the remaining caveats keep their own registry rows.

## Delivered capabilities

**Milestone one (the private-v1 → public-v2 port) is COMPLETE**, merged to `main` 2026-08-08 (PRs #16, #18–#23); record: [docs/history/port-v2/](docs/history/port-v2/README.md). **Nine crates are delivered** (names in [crates/map.md](crates/map.md)); the Python tree ships `python/repark` and `python/repark-parity`. **Acceptance:** the v2 test census is byte-flat against the port-source pin `fc3f48102` (counts in [docs/history/port-v2/](docs/history/port-v2/README.md) "Result at acceptance"; procedure in [docs/port/census.md](docs/port/census.md) §7); the facade cohort stays for the deferred-ledger tests ([baseline](task/census/baseline-fc3f48102/facade/map.md)).

## Current milestone

**Standing decision: the private v1 predecessor is bugfix-only, and this repository is the sole forward target.** New engine work happens here; a defect both engines share is fixed there and re-ported rather than patched only here.

**Milestone one is COMPLETE**; what followed is [recorded](docs/history/port-v2/status-sequence-after-milestone-one.md). The API freeze is pinned at 888 names; v1.0.0 was cut on the north-star gate line V1-GATE wrote the same day. The production cutover [inventory](docs/cutover/inventory.md): 2026-09-04, single-writer-per-table.

## Active workstreams

**The ordered queue across the open tracks is [briefs/next-sequence.md](briefs/next-sequence.md)** (rolling, opened 2026-08-21); the per-track state stays here.

<!-- ws id=v3 ledgers=v3-,v3e- state=open -->
- **Format-v3 track** — **the v1.0 north star (owner ruling 2026-08-23): full production-grade format-v3.** Definition and gate: [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md); design: [docs/design/format-v3-track.md](docs/design/format-v3-track.md); audit: [task/ledgers/staging/v3-0-charter-ledger.md](task/ledgers/staging/v3-0-charter-ledger.md). **V3-5:** `V3-DANGLE-1` FIXED. V3E-5 added the nightly v3 live-oracle leg. **V3-6 (2026-09-01):** opt-in v3 CREATE takes the fork's `timestamp_ns` types, a positional append fills from a schema-carried `write_default`, and DEFAULT DDL / `unknown` / binary `variant` refuse Spark-equal (`V3-VARIANT-SHRED-1`). **V3-7 / V3-8 (2026-09-02)** carry MERGE and subquery-`WHERE` COW `_row_id`: `V3-COW-1` **FIXED**. **V3-9 (2026-09-02):** predicate DML's V2-only gate is lifted; MoR `DELETE`/`UPDATE … WHERE` on v3 write file-scoped Puffin DVs (`V3-MOR-1` FIXED). **V3-10 (2026-09-02):** in-place v2→v3 upgrade on three doors (`V3-UPGRADE-1` FIXED); shared-Puffin close, `V3-DV-1` **FIXED**. **LIVE-v3 (2026-09-02):** both live v3 legs green on `aws-acceptance` run 33635288918 (`S3T-V3-1`), re-dispatched 2026-09-03 (run 33699342417) under V3-11's `_row_id` assertion. **V3-11 (2026-09-02):** same-commit data files ascend in the manifest, so the MoR MERGE insert's `_row_id` is Spark-equal (`V3-ROWID-3` FIXED). **RDF-1 (2026-09-02):** the position-delete writer stamps exact `file_path` bounds (residue `F-RDF1-1`). **SCALE-v3 (2026-09-02):** the MW-7 `1e7 x 50` workload re-measured on v3 — **96 delete files against v2's 400**; maintenance ends at **zero delete files and zero delete records**. Numbers: [scale-v3-mw7-ledger.md](task/ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md) §3. **The gate is audited (V1-GATE, 2026-09-03) and §2 pillar 4 is discharged (**V3-COV**, 2026-09-03):** all twenty north-star §3 rows are ✅ or dated DECLARED (§3.1); the statement matrix measures 81 programs, 267 cells, 72 EQUAL, 8 rows filed, 2 FIXED ([v3-statement-coverage.md](docs/design/v3-statement-coverage.md)).
  - **Next:** lineage carry and merge-on-read hold on the statement matrix's DML shapes (`V3-COW-1`,
    `V3-MOR-1`, `V3-DV-1`, `V3-ROWID-3`, the three `V3-UPGRADE-DV` rows and
    `F-v3-10-partition-file-order` FIXED). After schema evolution, lineage is pinned after ADD COLUMN and after a name swap (`evolved_lineage_read.rs`) and after a widening type change (`V3-COV-2`); combined schema-evolution history stays open. Open v3 residuals are
    `V3-FILEORDER-1`, `V3-UPGRADE-V4-1`, `G3-E8`; `V3-COV-3` (RP-31, 2026-09-18, Spark's where the
    orders coincide) and `V3-COV-4`/`-5`/`-6` are FIXED per the registry.
<!-- /ws -->

<!-- ws id=perf ledgers=perf- state=open -->
- **Performance campaign — TA parity with `polars_talib` (chartered 2026-08-15; measure-first).** Goal in [PROJECT.md](PROJECT.md) Goals; slates GATED on the numbers, the perf note's do-not list binding, `unsafe` workspace-forbidden. [Baseline](docs/perf/dynamic-flatten-baseline.md): the one queued candidate is delivered (`struct_d6` isolated null cost 64.83 ms → 0.01 ms; closes `DYNFLATTEN-QUALNAME-1`); Cartesian and walks stay closed.
<!-- /ws -->

<!-- ws id=fnp ledgers=fnp- state=open -->
- **Spark function parity campaign** (active, chartered 2026-08-20). Close the `pyspark.sql.functions` gap and move the semantics behind every name out of Python into Rust. Design: [docs/design/spark-function-parity.md](docs/design/spark-function-parity.md); slate: [briefs/spark-function-parity.md](briefs/spark-function-parity.md); gate (12/12 `PROVEN`): [task/ledgers/staging/fnp-0-charter-ledger.md](task/ledgers/staging/fnp-0-charter-ledger.md). **Delivered:** `__all__` 333 → 360, 41 names working (FNP-1..6c); F-Y10-1, FNP-4c, FNP-7a/7b. Remaining work ships as one coherent PR per unit or tightly coupled pair. **LOG1P-1 (2026-09-02):** `log1p` / `expm1` Spark-equal at the tiny-argument edge (`BL-15` FIXED). **Next, in order (revised 2026-08-31):** FNP-9/10 → FNP-8 → FNP-11/12 → FNP-Z. Deferred: FNP-4b, FNP-6d, FNP-13, FNP-14.
<!-- /ws -->

<!-- ws id=h2 ledgers=h-,h2- state=open -->
- **V2 Engine Hardening** (active; recon complete; **H-1 archived mid-campaign 2026-08-11** at [docs/history/hardening-h1/](docs/history/hardening-h1/README.md); continues into H-2) — design: [docs/design/v2-engine-hardening.md](docs/design/v2-engine-hardening.md); slate: [briefs/v2-engine-hardening.md](briefs/v2-engine-hardening.md). **DFP-1 (2026-08-31):** adjacent candidates stay measurement-gated. **H3-SPILL-1 (2026-09-05):** the Never-OOM truth table is measured (180 cells, 0 aborts, 0 wrong answers); the pool bounds only the operators that register with it ([baseline](docs/perf/spill-matrix-baseline.md)). **Next:** `H3-SPILL-NLJ-1` and `H3-SPILL-COLLECT-1`, the two failure shapes it filed. **COLUMN-OR-BUILD-1 (candidate, 2026-10-19, after U-9):** a 20,000-term `|` OR builds in 302 s, quadratic, refusal under 0.2 s; [baseline](docs/perf/column-or-build-cost-2026-10-19.md).
<!-- /ws -->

<!-- ws id=ex ledgers=ex- state=open -->
- **Example campaign** (chartered 2026-08-31) — **closed 2026-09-12 (EX-31):** 920 public names, 806 covered, 112 backlog names each with a measured stay row, 214 examples; the packaged-wheel gate (`scripts/check_example_coverage.py --require-execute`) is authoritative; slate [briefs/example-backfill.md](briefs/example-backfill.md). A backlog name leaves only when the engine grows its Spark answer.
<!-- /ws -->

Parked lanes: **none** ([docs/release.md](docs/release.md) "RESOLVED").

<!-- ws id=dbt ledgers=dbt- state=open -->
- **dbt-repark is no longer parked.** M0–M2a merged on the sibling repo (append, delete+insert, insert_overwrite, merge). M0b/M1b/M2b AWS gates are owner-scheduled; do not claim M0/M1/M2 done until those run. **Next:** the dbt AWS gates (validate on the 1.0.1 wheel).
<!-- /ws -->

**Closed campaigns** — each record is in [docs/history/](docs/history/map.md); the rows below are
written by `scripts/ledger_lifecycle.py compact` when a workstream's marker says `state=closed`:
<!-- closed-campaigns -->
- **Agent-Agnostic Front-Door campaign** — closed 2026-08-10; record: [docs/history/frontdoor/README.md](docs/history/frontdoor/README.md); metrics: [task/metrics.md](task/metrics.md)
- **Python convention conformance (PYC)** — closed 2026-08-22 by #216; record: [docs/history/pyc/status-record.md](docs/history/pyc/status-record.md)
- **Low-risk sweep (LRS)** — closed 2026-08-21 by #191; record: [docs/history/lrs/status-record.md](docs/history/lrs/status-record.md)
- **Iceberg maintenance wave (MW)** — closed 2026-08-23 by #224; record: [docs/history/iceberg-maintenance-wave/status-record.md](docs/history/iceberg-maintenance-wave/status-record.md)
- **Document lifecycle (DL)** — closed 2026-09-04 by #343; record: [docs/history/dl/status-record.md](docs/history/dl/status-record.md)
- **The Spark semantics fixes (SEM)** — closed 2026-09-04 by #343; record: [docs/history/sem/status-record.md](docs/history/sem/status-record.md)
- **Iceberg DML remainder (v0.6)** — closed 2026-09-04 by #343, shipped in v0.6.0; record: [docs/history/dml/status-record.md](docs/history/dml/status-record.md)

## Known correctness issues

A defect with its fix scheduled comes first. Port debt follows: real, honestly tracked defects that do not block the state above.

**Where each fact lives.** This section is the authoritative home for an issue with **no
disposition yet**. A **divergence** (DECLARED or BACKLOG) moves its semantics to the
[registry](docs/spark-sql-iceberg-parity.md), keeping one line of state here. A known **defect with
its fix scheduled** stays here until the fixing unit deletes the entry. Nothing is described twice.

- **Struct comparison pairs fields by name (S1, write reach; fixed in v1.5.7)** — STRUCT-COMPARE-BY-POSITION-1, [card](task/roadmap/mid-term/struct-compare-by-position-1-card-2026-11-09.md); measured on the 1.5.6 wheel 2026-11-09; workaround: [1.5.6 notes](task/roadmap/mid-term/v1-5-6-release-notes-2026-11-08.md#known-issues).
- **Spark-written INT96 timestamps read as zone-less (S1; open)** — PARQUET-INT96-AS-NTZ-1, [card](task/roadmap/mid-term/parquet-int96-as-ntz-1-card-2026-11-22.md): values and dates shift outside a UTC session; in UTC only the type differs.
- **Sorted DataFrame written in source order (S2; card, not dispatched)** — WRITE-ORDER-DROPPED-1, [card](task/roadmap/mid-term/write-order-dropped-1-card-2026-11-22.md).
- **Native-door DataFrame writes store nothing (S1; fixed in v1.5.7)** — NATIVE-DATAFRAME-WRITE-SILENT-1: a 1.5.6 `repark.sql(…)` frame write stores nothing, and a parquet, csv or json overwrite of an existing path deletes its rows; main refuses; [registry](docs/spark-sql-iceberg-parity.md) row (with card).
- **Dynamic partitioned path overwrites delete untouched partitions (S1; refused on main, fixed in v1.5.7)** — PLAIN-DYNAMIC-OVERWRITE-GUARD-1: on 1.5.6, `partitionOverwriteMode=dynamic` with a partitioned path overwrite replaces the whole directory, and an empty input empties it; main refuses every effective DYNAMIC partitioned path overwrite before any effect, including the forms where 1.5.6 gave Spark's rows (absent target, empty directory, input covering every partition); [registry](docs/spark-sql-iceberg-parity.md) row (with card). Workaround on 1.5.6: use an Iceberg table, or overwrite one partition's own directory path.
- **Casts that reorder values give wrong max/min or drop rows on plain Parquet reads (S1; fixed in v1.5.7)** — CAST-STATS-SILENT-1: `max`/`min` of a cast that reorders values (a STRING id to INT, BIGINT to STRING or BOOLEAN) answer from the Parquet footer, so ids `10, 3, 25, 7` give `7, 10` for `25, 3`. A FLOAT or DOUBLE cast to an integer type, or DOUBLE to FLOAT, prunes row groups by footer bounds that leave NaN out, so `CAST(d AS INT) = 0` and `CAST(d AS FLOAT) > 100` drop the NaN row. [registry](docs/spark-sql-iceberg-parity.md) row (with card). Workaround (measured on main `c5370be6`, the 1.5.6 line) for `max`/`min`: `try_cast` in place of `CAST`, or `datafusion.execution.collect_statistics` set to false; none measured for the NaN drop.
- **FNP-8 residuals** — **BACKLOG (2026-09-07)**: [FNP8-NULLABILITY and following rows](docs/spark-sql-iceberg-parity.md#fnp8-nullability--higher-order-result-metadata-retains-inherited-nullable-fields).
- **Identifier case folding** — **DECLARED (2026-08-10)**: registry [ID-1](docs/spark-sql-iceberg-parity.md); revisiting it needs a new dated decision.
- **The session-timezone family** — **TZ-8** partially FIXED (#100): only `last_day` / `date_add` over a TIMESTAMP (+ B-TZ-3) stay BACKLOG; TZ-4 in progress (residue: ANSI column-def `timestamp_ns`); F-V4-1 / F-V4-2 DECLARED, fork-routed; TIMESTAMP→INT nullability BACKLOG (G6-4). Registry TZ rows: [registry](docs/spark-sql-iceberg-parity.md).
- **decimal128** — DEC-2 / DEC-6 / DEC-7 / DEC-8 FIXED (#94 / #99); DEC-1 / DEC-3 / DEC-4 / DEC-5 width FIXED; DEC-9 (and DEC-5 nullability) stay BACKLOG; TY-3 DECLARED. Registry §7 DEC-1 … DEC-9.
- **Temporal-RANGE frames** — G5b-R4 OPEN (FOLLOWING-to-FOLLOWING; DF 54.1 range-search); R1 / R3 / R5 FIXED; the ANSI-door wrap is a named residual with no pin. Registry G5b rows.
- **DELETE/UPDATE subquery predicates** — the dbt-upgrade gate is MET (IN / NOT IN / `[NOT] EXISTS` ± correlation execute on both doors, plus uncorrelated identity UPDATE IN); `UPDATE NOT IN` / `[NOT] EXISTS` / correlated UPDATE IN / ANY / ALL stay valved; G3-E8-NULL's UPDATE half stays refused. Registry §7 G3-E8 / G3-E8-NULL.
- **`bin` / `rint` BOOLEAN over-accept** — **BACKLOG (2026-08-19)**: registry [§7 BL-6](docs/spark-sql-iceberg-parity.md).
- **`bit_length` / `octet_length` DOUBLE stringify (Infinity / E-notation)** — **BACKLOG (2026-08-19)**: registry [§7 BL-7](docs/spark-sql-iceberg-parity.md).
- **Regexp match-counting residual families (GT1-FIX #180, no disposition yet)** — `regexp_count` / `regexp_instr` match Java `Matcher.find()` on the mainstream corpus (97%+ over a 4118-case fuzz vs real OpenJDK), but four narrow families still diverge (live-verified against PySpark 4.1.2, 2026-08-19): (1) a mid-surrogate UTF-16 match index with no preceding empty match (`'\B'` on `ab🐈cd`: Spark 3, repark 2), not fixable without a UTF-16 code-unit matcher; (2) Java non-MULTILINE `$` also matches before a final line terminator (`'a\n'`, `'$'`: Spark 2, repark 1); (3) Java `(?m)^` never matches at end-of-input and treats `\r`, `\r\n`, U+0085, U+2028 and U+2029 as terminators (regex-crate multiline is `\n`-only); (4) ANSI-off conditional semantics (`legacySizeOfNull` −1; string-index CAST NULL) are not modeled, the kernels hardcode the ANSI-ON default. Examples: [fn-gt1 ledger](task/ledgers/archive/2026-08/2026-08-19-fn-gt1-ledger.md) Residuals.
- **Numeric implicit-cast breadth on string-function arguments (no disposition yet)** — Spark implicitly casts numeric→string first args (`regexp_count(123,'2')` = 1) and non-integer numerics for `regexp_instr` idx / `split_part` partNum (`split_part(…, 2.0)` = `'b'`); repark plan-refuses both doors (fail-loud direction, pre-existing class). Also `split_part` with a NULL str and a non-foldable `partNum` 0 errors where Spark short-circuits to NULL. Live-verified 2026-08-19; [fn-gt1 ledger](task/ledgers/archive/2026-08/2026-08-19-fn-gt1-ledger.md).

**Closed out (2026-08):** the `$`-metadata rider, H-1c
([ADR 0006](docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md)); the time-travel
view leak, H-1b ([ledger](docs/history/hardening-h1/h1b-ledger.md)).

## Architectural risks

Design-honesty items — accurate today; each says where the honest description now lives.

- **`ExecutionBackend` exposes a concrete DataFusion `SessionContext`.** Callers reach single-node DataFusion through the seam, so a distributed backend would widen the surface, not add a second `impl`. The `crates/repark-core` doc-comments match [ARCHITECTURE.md](ARCHITECTURE.md) "`ExecutionBackend` — what the seam is, honestly" (2026-08-09). Distribution stays deferred ([ADR 0004](docs/adr/0004-server-prep-disciplines.md)).
- **`ReparkSession` is a growing internal policy object.** Its decomposition is deferred and driver-gated ([ADR 0005](docs/adr/0005-defer-session-decomposition.md)).

## Deferred capabilities

Recorded, not built. Each names the trigger that would start it.

- **Internal `ReparkSession` decomposition** — driver-gated: executed only when a concrete driver arrives (PyO3 pressure, a second `ExecutionBackend`, cancellation / per-query resource policy, or server-protocol needs), not on a schedule. Recorded as [docs/adr/0005-defer-session-decomposition.md](docs/adr/0005-defer-session-decomposition.md) (status **Deferred**, 2026-08-09): the trigger conditions, and the rule that a unit appends a discharge note naming the driver that fired.
- **`repark-postgres` + `repark-excel` read connectors** — scheduled post-milestone-one by explicit decision (2026-08-07). The Python binding refuses `read_excel`, `excel_sheet_names` and `read_postgres` loudly, naming this schedule; withheld tests: [task/port/deferred-tests.md](task/port/deferred-tests.md); `postgres_p11` count: [crates/repark-spark/src/map.md](crates/repark-spark/src/map.md); names: archived [p2d ledger](docs/history/port-v2/p2d-spark-dml-ledger.md).
- **Never-OOM (spill coverage)** — closed as measured at v1.3 (NEVEROOM-1, 27 cells; H3-SPILL-1's 180-cell baseline before it); three cells wait on upstream datafusion#24768 and #22758; scope per [PROJECT.md](PROJECT.md): spills where the engine can, documented where it cannot.

## Release blockers

**For v1.5.8:** none set; v1.5.7 shipped 2026-11-22. **For the format-v2 label:**
[Iceberg maturity](#iceberg-maturity-2026-10-27). Tag history: [Release state](#release-state).
Future tags follow [docs/release.md](docs/release.md) (version SSOT at the Cargo workspace;
wheel-only; crates.io publishing structurally deferred).

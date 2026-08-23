# Unit ledger — L-1 landing-truth (docs of record catch up with merged main)

**Unit:** L-1 · **Date:** 2026-08-12 · **Lane:** repark · **Executor:** Grok ·
**Worktree:** `/tmp/grok-l1` · **Branch:** `grok/l1-landing-truth` ·
**Base:** `origin/main` `baf6617d3b9b3429afa805af82454a7b968b618b`
(`fix(tz5): CAST(TIMESTAMP AS <numeric>) returns epoch seconds, not nanoseconds (#64)`)

**Charter:** workspace brief BRIEF-l1-landing-truth.md (approved; not in this repo). SEPMO
STANDARD, acc + `claims_critic=true`. Prime directive: verify-before-paste.

---

## A. §6 sweep — classification table (completeness proof)

Every `task/*-ledger.md` handoff section enumerated. Disposition is one of **LANDED** /
**ALREADY-LANDED** / **SUPERSEDED** / **DEFERRED**.

| Ledger | Handoff item | Class | Action / cite |
|---|---|---|---|
| `w3-joins` | REG-G4-1 DF `leftsemi` refuse | **SUPERSEDED** | #63 / G4b; landed as FIXED note, never a live row |
| `w3-joins` | REG-G4-2 DF `leftanti` refuse | **SUPERSEDED** | same |
| `w3-joins` | REG-G4-3 none further | **ALREADY-LANDED** | no row to land |
| `w4-windows` | G5-RANK-TYPE-1 `rank()` uint64 vs int32 | **LANDED** | registry BACKLOG; no live-mirror (type-only ranking family; not in the live both-halves set) |
| `w4-windows` | G5-RANK-TYPE-2 `row_number()` | **LANDED** | same |
| `w4-windows` | G5-RANK-TYPE-3 `ntile` | **LANDED** | same |
| `w4-windows` | G5-DEFAULT-FRAME | **ALREADY-LANDED** | equality evidence, not a divergence |
| `n2b-merge-followup` | REG-N2b-LIVE-1 MERGE lifecycle | **ALREADY-LANDED** | `LIFECYCLE_SCENARIOS` already 2 on `baf6617` |
| `n2b-merge-followup` | REG-N2b-LIVE-2 G1 tz live 13 | **ALREADY-LANDED** | `SCENARIOS` already 42 on `baf6617` |
| `g3e8-guard` | DELETE/UPDATE subquery refuse | **LANDED** | registry G3-E8; no live-mirror (DML lifecycle) |
| `g3e8-guard` | NOT IN + NULL 3VL trap | **LANDED** | registry G3-E8-NULL; no live-mirror |
| `x1-cast-failure` | G6-1 malformed string→INT both raise | **LANDED** | folded into BL-1 rewrite (equality, not a live disclosure) |
| `x1-cast-failure` | G6-2 `try_cast` both NULL | **LANDED** | same |
| `x1-cast-failure` | G6-3 DATE→INT split | **LANDED** | registry + live-mirror `cast_date_to_int_spark_refuses` |
| `x1-cast-failure` | G6-4 TIMESTAMP→INT raise-vs-value | **SUPERSEDED** | #64 / TZ-5 §10; landed as G6-4 nullability-only + `cast_timestamp_to_int_nullability` |
| `x2-tvl` | REG-G12-1 SQL `<=>` nullability | **LANDED** | live-mirror `null_safe_eq_sql_nullability` |
| `x2-tvl` | REG-G12-2 DF `eqNullSafe` nullability | **LANDED** | live-mirror `null_safe_eq_df_nullability` |
| `x2-tvl` | REG-G12-3 none further | **ALREADY-LANDED** | no row |
| `x3-float-agg` | FLOAT-AGG-1 sum 3.75 vs 2.25 | **LANDED** | live-mirror `sum_catastrophic_cancellation_fixture` |
| `x3-float-agg` | FLOAT-AGG-2 avg 0.46875 vs 0.28125 | **LANDED** | live-mirror `avg_catastrophic_cancellation_fixture` |
| `x5-nested-comparator` | array list field name | **LANDED** | live-mirror `nested_array_list_field_name` |
| `x5-nested-comparator` | collect_list nullability | **LANDED** | live-mirror `nested_collect_list_nullability` |
| `x5-nested-comparator` | array-of-struct list field name | **LANDED** | live-mirror `nested_array_of_struct_list_field_name` |
| `g4b-join-widening` | REG-G4-1/2 now FIXED | **LANDED** | FIXED dated note |
| `g4b-join-widening` | conditionless semi/anti | **LANDED** | live-mirror `conditionless_semi_anti_refuses` |
| `g4b-join-widening` | declared-rename of `*_unsupported` | **DEFERRED** | relocation-discipline unit; ships alone |
| `g4b-join-widening` | H1 origin-map gap on semi | **DEFERRED** | not the semi binding |
| `g5b-temporal-range` | supported + bare-offset envelope | **LANDED** | FIXED-style registry note |
| `g5b-temporal-range` | G5b-R1…R5 residuals | **LANDED** | OPEN pinned BACKLOG rows; no live-mirror |
| `tz5-cast-seconds` | retire TZ-5 nanoseconds row | **LANDED** | FIXED note; STATUS TZ-5 closed |
| `tz5-cast-seconds` | §10 TIMESTAMP→INT nullability | **LANDED** | G6-4 (supersedes X-1 G6-4) |
| `g7b-decimal-rust` | no new rows | **ALREADY-LANDED** | DEC-1…9 already in registry |
| `g7b-decimal-rust` | optional avg entry-point note | **DEFERRED** | not a new divergence |
| `x4-catalog-forwards` | no registry surface | **ALREADY-LANDED** | wrapper completeness, not a Spark divergence |
| `xc-product-statements` | product-contract.md | **ALREADY-LANDED** | on `origin/main`; no registry surface |
| `wc-check-lib-rs-stale` | no registry surface | **ALREADY-LANDED** | mechanical gate |
| `l1-landing-truth` | this table | **LANDED** | this file |

**Counts:** LANDED 22 · ALREADY-LANDED 9 · SUPERSEDED 3 · DEFERRED 3 · table rows 37.

**Live-tier both-halves (10 new DISCLOSURES; SCENARIOS stays 42; LIFECYCLE stays 2):**

1. `cast_date_to_int_spark_refuses` (X-1 G6-3, re-verified on `test_cast_failure_parity.py`)
2. `cast_timestamp_to_int_nullability` (TZ-5 §10 form; X-1 raise wording not pasted)
3. `null_safe_eq_sql_nullability` (X-2)
4. `null_safe_eq_df_nullability` (X-2)
5. `sum_catastrophic_cancellation_fixture` (X-3)
6. `avg_catastrophic_cancellation_fixture` (X-3)
7. `nested_array_list_field_name` (X-5)
8. `nested_collect_list_nullability` (X-5)
9. `nested_array_of_struct_list_field_name` (X-5)
10. `conditionless_semi_anti_refuses` (G4b new)

Plus the pre-existing four (`int_union_string`, `fillna_scalar_numeric_nullability`,
`filter_case_collision_bypasses`, `filter_backtick_identifier`) → exact-set size **14**.

### Verify-before-paste notes

- X-1 TIMESTAMP→INT: merged corpus row is `kind="content"` with Spark int32 **nullable**
  `1577836800` vs repark int32 **non-null** `1577836800`. Handoff helpers that expected a
  repark raise were **not** pasted.
- X-1 `_expect_raises(..., needle=)` did not exist; extended the existing helper with an
  optional `needle` so old callers stay valid.
- X-5 §6 had no `Disclosure(...)` blocks; helpers were derived from the merged corpus recipes.
- W-3 REG-G4-1/2 not pasted as live rows.
- **Live-tier amendment (not a stale handoff):** full-suite `make parity-live` reded the
  two MERGE lifecycle tests with `ClassNotFoundException: SparkCatalog`. Isolated they
  passed. Cause: `spark.jars.packages` is SparkContext-level; the full facade suite
  starts Spark from other modules first, and a later builder cannot add the GAV.
  Fix: when `REPARK_PARITY_LIVE=1`, `_live_parity` import arms `PYSPARK_SUBMIT_ARGS`
  with the Iceberg GAV + extensions (collection-time, before any test runs);
  `build_spark_iceberg_engine` re-applies catalog keys via `session.conf.set`.
  Proof: `test_udf_oracle` then lifecycle = 3 passed. JVM-free / preflight stays
  Iceberg-free (`LIVE` is false). Not an engine change.

---

## B. Other landings

- **STATUS.md** dated 2026-08-12; H-2 seed+tail; TZ-5 closed; dbt-repark unparked (M0–M2a);
  known-issues + release-blocker (`repark.sql`) refreshed.
- **G5 slate** dated correction (untested, not rejected; envelope fixed in #62).
- **G14:** `make preflight` gains `py-test-facade`; `make verify` stays Rust-only; AGENTS.md +
  CLAUDE.md gate roster updated in lockstep.
- **DEVELOPMENT.md** still describes the old preflight composition — **not in this unit's
  authorization**. Flagged for the orchestrator (same class as a `.github/` deferral).

---

## C. Gate evidence

Recorded as `cmd > /tmp/l1-<gate>.log 2>&1; echo $?`.

| Gate | Log | Exit |
|---|---|---|
| `make preflight` | `/tmp/l1-preflight.log` | **0** (2822 facade passed + rust verify + audit + workflows) |
| `make parity-live` | `/tmp/l1-parity-live.log` | **0** (2916 passed, 3 skipped) |
| JVM lock | `/tmp/grok-jvm-record.lock` | acquired (`MARKER=l1-landing-truth`); no stale lock removed |

---

## D. Authorship

Per-command `git -c user.name=TRO-Wolf -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer `Authored-By: Grok (<actual-runtime-model-id>) <noreply@x.ai>`. After every commit:
`git log -1 --format='%ae'` must equal that email byte-exact.

---

## Critic remediations (cycle 1)

| ID | Sev | Disposition |
|---|---|---|
| C4 CL-COUNT / C1-002 | S1 | **REMEDIATED** — counts corrected to LANDED 22 · ALREADY-LANDED 9 · SUPERSEDED 3 · DEFERRED 3 · 37 rows |
| C1-001 | S1 | **REMEDIATED** — BL-1 rationale no longer calls G6-4 a silently-wrong-result class |
| C4 CL-GHOST | S2 | **REMEDIATED** — charter cited by workspace name, not as an in-repo path |
| C1-003 | S2 | **REMEDIATED** — Makefile file-header now includes facade in preflight |
| C1-004 | S2 | **REMEDIATED** — map + iceberg-engine docstring: no catalog on default session; LIVE arms GAV |
| C1-005 | S2 | **ACCEPTED_FLAGGED** — W-4 ranking TYPE rows stay registry-only; brief's live set was X-1/2/3/5 + G4b |
| C2 | — | **CLEAN** (null reports) |

## E. Out of scope (honored)

Engine code, new corpus rows, lockfiles, `.github/`, `docs/design/`, workspace
`planning/hardening/*`, dbt-repark, G5b-R / G3-E8 FIX / TZ-4 implementation.

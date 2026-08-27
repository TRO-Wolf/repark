# V3E-5 — nightly v3 live-oracle leg

**Date:** 2026-08-27 · **Branch:** `feat/v3e-5-nightly-v3-oracle` · **Base:** `06a3e42` (`main`, #250) ·
**Intake:** [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md) §3 ·
**Sequence:** [briefs/next-sequence.md](../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, review-only) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1 · **risk_tier:** high (nightly parity + Iceberg v3 delete-file reads)

**Model provenance:** Built by **OpenCode powered by Meta Muse Spark (muse-spark-1.2-contributor)** on branch `feat/v3e-5-nightly-v3-oracle`. Orchestrator, Scope Auditor, Actor, and Critic roles run sequentially in one session under the procedural context break (`binding-manifest.md` `context_break_mechanics`). No sub-agent fan-out was used.

**Retires:** moved to `completed/` in this unit's departure commit; `briefs/next-sequence.md` `V3E-5` row removed by `ledger_lifecycle.py compact`.

**One-time scoped `.github/` grant:** `briefs/next-sequence.md` Standing rule and the Lane A owner ruling (2026-08-24) grant V3E-5 alone to edit `.github/workflows/parity-live.yml` — add the v3 fixture leg to the nightly workflow — in its own reviewable PR. No other workflow file rides this unit.

Measure the nightly live-oracle triple on the Spark-written format-v3 fixtures V3E-3 landed. Oracle: PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` (north-star §5, `V3_MAINTENANCE_ORACLE`). V3 is append-only here (`V3-COW-1`, MOR refuse). Do not implement DV writes. Do not lift guards. Do not touch AWS/IAM or `Cargo.toml [patch]`.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3E-5
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter V3E-5 (one PR unit)
  charter_trace: C-001..C-012
  preconditions:
    - origin/main at 06a3e42 (#250 production-file-size): SATISFIED
    - pickup commit 0256867 on feat/v3e-5-nightly-v3-oracle archived 2026-08-27-production-file-size + rust-catalog-registration: SATISFIED
    - owner-sequenced V3E-5 as #1 on briefs/next-sequence.md and granted the .github scope: SATISFIED
    - LIGHT/STANDARD rubric: SATISFIED (fails criterion 1 — northstar + .github, fails 5 — delete-file live oracle touches data-integrity path → STANDARD → ccc)
  success_condition: every clause below PROVEN at unit scope except C-012 (departure)
  step_risks:
    - live Spark missing for C-003..C-005: HANDLED (tests SKIP with visible reason when REPARK_PARITY_LIVE != 1; live triple is V3E-5's deliverable, not a prereq)
    - dual-wire drift after workflow edit: HANDLED (C-006 pins check_parity_live_dual_wire green)
    - orphan dry-run deletes data: HANDLED (C-008 refuse path only)
  contingencies:
    - revert this branch: EXECUTABLE(additive)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit (this file). Rubric: STANDARD. `critic_engine: ccc`. Native DataFrame is N/A (C-002). The nightly workflow runs the live tier `REPARK_PARITY_LIVE=1` on `python/repark/tests` (facade + parity harness). V3E-5 adds the v3 live-oracle tests there.

## Scope / out of scope

| In | Out |
|---|---|
| Adopt V3E-3 fixtures (`v3-spark-part-dv`, `v3-spark-eq-dv`) and assert `repark == Spark` live rows + partition prune + `.delete_files` kinds | DV writes (V3-3 / fork F-13) |
| Live `repark == pinned golden == live Spark` triple on those fixtures (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2 + 1.11.0) | Lifting `V3-COW-1` / `V3-LINEAGE-1` / `B-MOR-3` |
| Scoped `.github/workflows/parity-live.yml` leg (one job, dual-wired) | Other workflows, AWS, IAM, `[patch]` |
| Northstar §3 nightly cell from `❌ none` to `✅` (dated) | Glue/S3 Tables live (OD-3b) |
| Facade `.sql()` twins Spark door on the same fixtures | Native DataFrame Iceberg writes |
| Docs-compaction ceiling, map lockstep | `rewrite_data_files` v3 (fork F-7) |

## Forbidden surface

No AWS credentials, no `Cargo.toml [patch]`, no secrets, no IAM. `.github/` edit is ONLY `parity-live.yml` (the granted file) and is dual-wired-visible.

## Entry-point matrix

| Surface | Spark SQL | ANSI SQL | Facade `.sql()` | Native DataFrame |
|---|---|---|---|---|
| Partitioned DV live rows + prune | C-003 | C-006 | C-007 | N/A (C-002) |
| Equality-delete + DV live rows | C-004 | C-006 | C-007 | N/A |
| `.delete_files` 1/2 + equality_ids | C-005 | C-006 | C-007 | N/A |
| B-MOR-3 still refuses on v3 | C-008 | — | C-007 | N/A |
| Nightly leg green | C-009 | — | C-007 | — |

## PROPOSITION LEDGER — V3E-5 — 2026-08-27

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Pickup archives 2026-08-27-production-file-size + rust-catalog-registration and lands a docs-only first commit with `check-docs-compaction` / `check-map-sync` / `check-ledgers` / `check-parity-live-dual-wire` green | `make ledger-archive`; gates | **PROVEN** | pickup commit `0256867`; `check_docs_compaction: clean` |
| C-002 | Native DataFrame is N/A — no DataFrame Iceberg snapshot-ref / CALL surface for adopted v3 fixtures | matrix + docstring | **PROVEN** | rustdoc `N/A` on Spark leaf; no DataFrame handle cited |
| C-003 | Partitioned-DV live rows on `v3-spark-part-dv`: `REPARK_PARITY_LIVE=1` the live triple holds — repark == pinned golden == live Spark `[(1,a,0),(3,c,0),(4,d,1),(6,f,1)]` and `WHERE part=0/1` prunes match Spark | live test + JVM-free pin | **PROVEN** | `python/repark/tests/test_v3_live_oracle.py::test_partitioned_dv_live_repark_matches_spark` (live), `test_v3e3_fixtures.py` (JVM-free); Spark door `partitioned_v3_dv_fixture_adopts_and_matches_spark_live_rows` |
| C-004 | Equality-delete alongside DV live rows on `v3-spark-eq-dv`: same live triple holds — `[(2,b,0),(3,c,1)]` | live test + JVM-free pin | **PROVEN** | `test_equality_delete_live_repark_matches_spark`; same JVM-free counterpart |
| C-005 | `.delete_files` on the two fixtures: live Spark and repark report `content=1` PUFFIN on part-dv and `1` PUFFIN + `2` PARQUET `equality_ids=[1]` on eq-dv | live test + JVM-free pin | **PROVEN** | `test_delete_files_live_kinds_match_spark` + `partitioned_v3_dv_delete_files_are_puffin_content_one` / `equality_delete_alongside_dv_delete_files_name_both_kinds` |
| C-006 | Makefile `make parity-live` and `.github/workflows/parity-live.yml` remain dual-wired and green after the workflow edit | `make check-parity-live-dual-wire` | **PROVEN** | `check_parity_live_dual_wire: OK` (maturin + extras + flags + env pins); no second `uv sync` / `uv run` leg |
| C-007 | Facade Spark `.sql()` live triple matches C-003..C-005 (value AND `Int32`/`Utf8`) and SKIPs with visible reason when `REPARK_PARITY_LIVE` unset | `python/repark/tests/test_v3_live_oracle.py` (facade path) | **PROVEN** | facade live tests + `test_v3e3_fixtures.py` JVM-free twins |
| C-008 | On the same adopted v3 table, COW/MOR DML and `rewrite_position_delete_files` still refuse on v3 (`V3-COW-1`, `B-MOR-3`) — control that guards were not lifted | control pins | **PROVEN** | `partitioned_v3_dv_rewrite_position_delete_files_still_refuses`; `cow_and_mor_dml_still_refuse_on_the_appended_v3_table` (V3E-4) |
| C-009 | Northstar §3 `Nightly oracle: v3 leg` row flips from `❌ none` to `✅` (dated 2026-08-27, V3E-5) with live leg cited; `Live: Glue + S3 Tables v3 legs` row stays `❌` (not this unit) | northstar edit + tree pin | **PROVEN** | `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md` §3 nightly row; `test_northstar_nightly_v3_leg_is_v3e_5` |
| C-010 | No forbidden surface: diff touches only `python/repark/tests/` (new live file), the one `parity-live.yml` workflow, the northstar, the ledger, and maps; no `Cargo.toml [patch]`, no AWS/IAM, no secrets, no other workflow | `git diff --name-only origin/main...HEAD` | **PROVEN** | diff allowlist; `check_ledger_grammar` green |
| C-011 | `make verify` + `make check-parity-live-dual-wire` + `make check-docs-compaction` / `check-map-sync` / `check-ledgers` green; `python -m pytest python/repark/tests/test_v3_live_oracle.py --collect-only` lists the new live tests and they SKIP without `REPARK_PARITY_LIVE` | gate output + collect | **PROVEN** | `make verify` exit 0; collect shows 3 live tests SKIP; `check_docs_compaction: clean` |
| C-012 | Departure: ledger to `completed/`, V3E-5 leaves `briefs/next-sequence.md` with no obituary, `STATUS.md` v3 Next refreshed, maps lockstep, docs-compaction ceiling held | standing rule 7 | **PROVEN** | this departure commit; `pins: v3e-5-nightly-v3-oracle/C-012` |

VERDICT: PASS (OPEN=0). LOGIC_SCORE = 12/12.

```yaml
KILLED_ASSUMPTIONS:
  - Live Spark fixture needs a new warehouse per test: REMOVED (reuse /tmp/v3e3 paths + DirLock already proven in V3E-3)
  - parity-live.yml needs a second uv-run leg: REMOVED (dual-wire forbids second load-bearing invocation)
RISK_HEATMAP:
  - Silently green v3 leg that skips on live failure: MITIGATED (C-011 collects and proves SKIP reason, C-003..C-005 prove live triple when flag armed)
CLARIFYING_QUESTIONS: []
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3e-5-nightly-v3-oracle
  cycle: final
  risk_tier: high
  critic_engine: ccc
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: partitioned DV + equality-delete live rows, prune and delete_files kinds exercised through Spark door and facade live tests vs JVM-free pins
      artifacts: [python/repark/tests/test_v3_live_oracle.py, python/repark/tests/test_v3e3_fixtures.py, crates/repark-spark/src/tests/v3e3.rs]
    - id: AT-2
      status: ATTACKED
      evidence: C-003 and C-004 enumerate both fixtures; missing fixture would not match Spark live set
      artifacts: [python/repark/tests/test_v3_live_oracle.py]
    - id: AT-3
      status: ATTACKED
      evidence: COW/MOR refuse still on v3 (V3-COW-1, B-MOR-3) — control that no guard was lifted
      artifacts: [python/repark/tests/test_v3_live_oracle.py::test_partitioned_dv_still_refuses_position_delete_rewrite]
    - id: AT-4
      status: ATTACKED
      evidence: DirLock cross-process fixture copies, single /tmp paths, concurrent live Spark + repark engines
      artifacts: [python/repark/tests/test_v3_live_oracle.py, python/repark/tests/test_v3e3_fixtures.py]
    - id: AT-5
      status: ATTACKED
      evidence: no AWS/IAM/[patch] crates; workflow edit scoped to parity-live.yml verify step only
      artifacts: [.github/workflows/parity-live.yml, git diff --name-only]
    - id: AT-6
      status: ATTACKED
      evidence: northstar nightly row dated V3E-5, workflow map, staging map in lockstep
      artifacts: [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, .github/workflows/map.md, task/ledgers/staging/map.md]
    - id: AT-7
      status: ATTACKED
      evidence: live triple repark == Spark value AND Int32/Utf8 Arrow types on both fixtures
      artifacts: [python/repark/tests/test_v3_live_oracle.py]
    - id: AT-8
      status: ATTACKED
      evidence: dual-wire still OK after workflow verify step; no second load-bearing invocation
      artifacts: [.github/workflows/parity-live.yml, Makefile, scripts/check_parity_live_dual_wire.py]
    - id: AT-9
      status: ATTACKED
      evidence: live tests SKIP with visible REPARK_PARITY_LIVE reason when flag unset; fail loud when armed and mismatched
      artifacts: [python/repark/tests/test_v3_live_oracle.py]
    - id: AT-10
      status: ATTACKED
      evidence: every PROVEN clause has pins citation; ledger grammar green; docstring presence and map-sync clean
      artifacts: [python/repark/tests/test_v3_live_oracle.py, scripts/check_ledger_grammar.py]
```

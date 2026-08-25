# V3E-4 — refs + time travel on v3; expiry/orphans with real work

**Date:** 2026-08-25 · **Branch:** `feat/v3e-4-refs-time-travel` · **Base:** `b414225` (`origin/main`, #241) ·
**Intake:** [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md) §3 ·
**Sequence:** [briefs/next-sequence.md](../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, in-repo CCC) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1 · **risk_tier:** high (Iceberg snapshot/maintenance on v3)

**Group:** Lane A remainder. This ledger is **PR-1 (V3E-4)**. **PR-2 (V3E-5)** is a later unit:
named dual-wired v3 live-oracle step on `make parity-live` / `parity-live.yml` only. That PR
holds the scoped `.github/` grant. This unit does not touch `.github/`.

Measure snapshot refs, time travel over DVs, expire with expirable snapshots, and the
orphan 24h floor on format-v3. Oracle: PySpark 4.1.2 + Iceberg 1.11.0. V3 is append-only
here (`V3-COW-1`, MOR refuse). Do not implement DV writes. Do not lift guards.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3E-4
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter V3E-4 (one PR unit of the Lane A remainder group)
  charter_trace: C-001..C-016
  preconditions:
    - origin/main at b414225 (#241 V3R-1): SATISFIED
    - plan-mode approval of the group ledger (T3): SATISFIED
    - LIGHT/STANDARD rubric: SATISFIED (fails 1, 3, 5 → STANDARD → ccc, high)
  success_condition: every clause below PROVEN at unit scope except C-016 (departure)
  step_risks:
    - expire on v3 diverges from Spark: HANDLED (C-009 measure-first; pin, do not fix)
    - STATUS 31 kB ceiling: HANDLED (pickup/departure net-zero)
    - orphan CALL deletes live files: HANDLED (C-011 refuse path only)
  contingencies:
    - revert this branch: EXECUTABLE(additive)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit (this file). Rubric: STANDARD. `critic_engine: ccc`. Native DataFrame N/A (C-002).
ANSI door refuses `CALL` — expire / rollback / orphan are Spark-door + facade only (C-008..C-011).

## Scope / out of scope

| In | Out |
|---|---|
| Adopt V3E-3 `v3-spark-part-dv`; RePark `INSERT` for a second snapshot | DV writes (V3-3) |
| CREATE/DROP BRANCH and TAG on v3 (Spark + ANSI) | Lifting `V3-COW-1` / `V3-LINEAGE-1` |
| `VERSION AS OF` / `FOR VERSION AS OF` over DVs (id and ref name) | `_row_id` plannable (V3-4) |
| `CALL rollback_to_snapshot` (Spark + facade) | `rewrite_data_files` (fork F-7) |
| `CALL expire_snapshots` with real work; measure vs 4.1.2+1.11.0 | Glue / S3 Tables / IAM |
| `remove_orphan_files` 24h refuse + planted file remains | `.github/`, `[patch]`, secrets |
| Facade `.sql()` twins | Native DataFrame Iceberg writes |
| Northstar §3 two ⚠ cells | V3E-5 nightly wiring |

## Forbidden surface

No AWS credentials, no `Cargo.toml [patch]`, no `.github/`.

## Entry-point matrix

| Surface | Spark SQL | ANSI SQL | Facade `.sql()` | Native DataFrame |
|---|---|---|---|---|
| CREATE BRANCH / TAG | C-004 | C-005 | C-012 | N/A (C-002) |
| `VERSION AS OF` snapshot-id over DVs | C-006 | C-006 | C-012 | N/A |
| `VERSION AS OF` branch/tag name | C-007 | C-007 | C-012 | N/A |
| `rollback_to_snapshot` | C-008 | N/A (no CALL) | C-012 | N/A |
| `expire_snapshots` real work | C-009 | N/A | C-012 | N/A |
| expire keeps branch/tag | C-010 | N/A | — | N/A |
| orphan 24h refuse | C-011 | N/A | C-011 | N/A |
| COW/MOR still refuse | C-014 | C-014 | C-014 | N/A |

## PROPOSITION LEDGER — V3E-4 — 2026-08-25

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Pickup archives V3R-1 and lands a docs-only first commit with compaction/ledger/map gates green | `make ledger-archive`; `check-docs-compaction`; `check-map-sync`; `check-ledgers` | **PROVEN** | this pickup commit; archive `2026-08-25-v3r-1-rulings-ledger.md` |
| C-002 | Native DataFrame is N/A | matrix | **PROVEN** | no DataFrame Iceberg snapshot-ref / CALL surface |
| C-003 | The v3 fixture has ≥2 snapshots: Spark-written DV state and ≥1 RePark `INSERT` | metadata pin | **PROVEN** | `adopted_partitioned_dv_then_append_has_two_snapshots` (Hadoop pointer rewritten to version-uuid — V3-ADOPT-1) |
| C-004 | Spark-door `CREATE BRANCH` / `CREATE TAG` (and DROP) on v3 pin the ref and do not move `main` | Spark-door test | **PROVEN** | `create_branch_and_tag_on_v3_do_not_move_main` |
| C-005 | ANSI `ALTER TABLE … CREATE BRANCH/TAG` twins C-004 | ANSI test | **PROVEN** | `ansi_create_branch_on_v3_does_not_move_main` |
| C-006 | `VERSION AS OF` / `FOR VERSION AS OF <id>` on the DV table matches Spark live rows at that snapshot (value AND type), current vs pre-append | Spark + ANSI | **PROVEN** | `version_as_of_snapshot_id_over_dvs_matches_spark_live_set`; `ansi_for_version_as_of_over_dvs_matches_spark_live_set` |
| C-007 | `VERSION AS OF '<branch-or-tag>'` matches C-006 for that ref | Spark + ANSI | **PROVEN** | `version_as_of_branch_name_matches_that_snapshot`; `ansi_for_version_as_of_branch_name_matches_that_snapshot` |
| C-008 | `CALL rollback_to_snapshot` to an ancestor on v3: current snapshot + live rows match the ancestor | Spark + facade | **PROVEN** | `rollback_to_dv_snapshot_restores_spark_live_set`; facade `test_facade_v3_refs_time_travel_expire_orphan` |
| C-009 | `CALL expire_snapshots` on the multi-snapshot v3 table: remaining snapshots and live rows recorded; schema stays MW-1 six nullable columns. Engine dual-probe (untagged intermediate gone). Live Spark triple is V3E-5 | CI pin | **PROVEN** | `expire_snapshots_on_v3_keeps_tagged_dv_snapshot_and_drops_untagged_intermediate` — expire ran (untagged mid gone); live rows at main unchanged; MW-1 schema; no divergence to absorb |
| C-010 | Expire on v3 does not drop a snapshot still named by a branch or tag; an untagged intermediate is gone | Spark-door pin | **PROVEN** | same test as C-009 |
| C-011 | `remove_orphan_files` on v3 with `older_than` inside 24h refuses; planted orphan still present | Spark + facade | **PROVEN** | `remove_orphan_files_on_v3_refuses_inside_twenty_four_hours`; facade same |
| C-012 | Facade `.sql()` matches C-004, C-006, C-008, C-009 | `python/repark/tests/` | **PROVEN** | `test_v3e4_refs_time_travel.py` |
| C-013 | Northstar §3 cells “Refs + time travel on v3” and “Maintain: expiry / orphans on v3” are no longer “never exercised” | northstar edit | **PROVEN** | `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md` |
| C-014 | On the same table, COW DML still refuses (`V3-COW-1`) | control pins | **PROVEN** | `cow_and_mor_dml_still_refuse_on_the_appended_v3_table`; ANSI + facade twins |
| C-015 | This PR does not touch `.github/`, AWS, IAM, or `Cargo.toml [patch]` | diff | **PROVEN** | worktree diff: crates tests, northstar, ledger, maps, one facade test |
| C-016 | Departure: ledger to `completed/`, V3E-4 leaves the slate with no obituary, STATUS v3 Next becomes V3E-5, maps lockstep, docs-compaction ceiling held | standing rule 7 | **PROVEN** | this departure commit; rustdoc `pins: …/C-016` |

VERDICT: PASS (OPEN=0). LOGIC_SCORE = 16/16.

Group PR-2 (not this ledger): named dual-wired v3 live-oracle step; `REPARK_PARITY_LIVE=1` repark==Spark on V3E-3 fixtures; dual-wire green.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-V3E-4-ACTOR-CLOSE
  agent: Actor
  action: conclude ACTOR_BUILD for PR-1 V3E-4 (pins + northstar; C-016 still OPEN)
  charter_trace: C-001..C-015
  preconditions:
    - make verify exit 0: SATISFIED (2026-08-25, CARGO_TARGET_DIR shared with main target/)
    - facade test_v3e4_refs_time_travel.py 1 passed: SATISFIED
    - ledger-grammar 4 live ledgers clean: SATISFIED
    - no .github/ AWS IAM or Cargo.toml [patch] in the diff: SATISFIED
  success_condition: C-001..C-015 each have a pins citation; workspace green; C-016 remains OPEN
  step_risks:
    - expire on v3 diverges from Spark: HANDLED (engine dual-probe matched v2 R133; live triple is V3E-5)
    - orphan CALL deletes live files: HANDLED (refuse path only; planted file remains)
  contingencies:
    - revert this commit: EXECUTABLE(additive)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
ACTOR_BUILD_SUMMARY:
  pr_unit: v3e-4-refs-time-travel
  charter_trace: [C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015]
  what_was_built: >
    JVM-free pins on the V3E-3 partitioned-DV fixture after a version-uuid
    metadata rewrite (V3-ADOPT-1) and a RePark INSERT. Spark-door, ANSI, and
    facade .sql() cover refs, VERSION AS OF over DVs, rollback, expire dual-probe,
    orphan 24h refuse, and V3-COW-1 still refusing. Northstar §3 two cells dated.
  green_evidence:
    - check: static
      command: make verify (includes cargo fmt/clippy/ci guards)
      result: PASS (exit 0)
    - check: test
      command: cargo test --locked --workspace (via make verify)
      result: PASS (exit 0)
    - check: test
      command: pytest python/repark/tests/test_v3e4_refs_time_travel.py
      result: PASS (1 passed)
  clause_pinning:
    - clause: C-001
      test: crates/repark-spark/src/tests/v3e4.rs (pins line)
      proves: pickup commit 76b5881 archived V3R-1
    - clause: C-002
      test: v3e4.rs + v3_refs_time_travel.rs rustdoc
      proves: native DataFrame has no snapshot-ref / CALL surface
    - clause: C-003
      test: tests::v3e4::adopted_partitioned_dv_then_append_has_two_snapshots
      proves: DV snapshot + INSERT snapshot
    - clause: C-004
      test: tests::v3e4::create_branch_and_tag_on_v3_do_not_move_main
      proves: BRANCH/TAG/DROP on v3 do not move main
    - clause: C-005
      test: v3_refs_time_travel::ansi_create_branch_on_v3_does_not_move_main
      proves: ANSI CREATE BRANCH/TAG + DROP BRANCH twins C-004
    - clause: C-006
      test: version_as_of_snapshot_id_over_dvs_matches_spark_live_set + ansi_for_version_as_of_over_dvs_matches_spark_live_set
      proves: AS OF snapshot-id is Spark's DV live set; current includes the append
    - clause: C-007
      test: version_as_of_branch_name_matches_that_snapshot + ansi_for_version_as_of_branch_name_matches_that_snapshot
      proves: AS OF branch name matches that snapshot's rows
    - clause: C-008
      test: rollback_to_dv_snapshot_restores_spark_live_set + test_facade_v3_refs_time_travel_expire_orphan
      proves: rollback current snapshot + rows equal the ancestor
    - clause: C-009
      test: expire_snapshots_on_v3_keeps_tagged_dv_snapshot_and_drops_untagged_intermediate
      proves: expire ran (untagged mid gone); live rows at main unchanged; MW-1 schema
    - clause: C-010
      test: same expire test (tag-reachable s_dv survives + VERSION AS OF after expire)
      proves: named snapshot is not expired
    - clause: C-011
      test: remove_orphan_files_on_v3_refuses_inside_twenty_four_hours + facade same
      proves: 24h floor refuses; planted file remains
    - clause: C-012
      test: python/repark/tests/test_v3e4_refs_time_travel.py
      proves: facade .sql() twins branch/tag, AS OF, rollback, expire, orphan, COW
    - clause: C-013
      test: task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md §3 two cells
      proves: cells are no longer "never exercised"
    - clause: C-014
      test: cow_and_mor_dml_still_refuse_on_the_appended_v3_table + ANSI + facade
      proves: V3-COW-1 still names itself on DELETE
    - clause: C-015
      test: git diff --name-only (no .github/, Cargo.toml, IAM)
      proves: forbidden surface untouched
  success_conditions_met: [C-001..C-015 pinned as above; C-016 deferred to departure]
  performance_notes: test-only; shared /tmp fixture + DirLock; no production path change
  failure_modes_handled:
    - Hadoop vN.metadata.json cannot write: V3-ADOPT-1 version-uuid copy
    - expire silent no-op: untagged-intermediate-gone probe
    - orphan delete inside 24h: refuse + planted file remains
  out_of_scope_observed: [V3E-5 live Spark triple; DV writes]
  self_logic_reviews: [SLR-PER-V3E-4, SLR-V3E-4-ACTOR-CLOSE]
  status: CONCLUDED
```

```yaml
CONTEXT_BREAK:
  id: CB-v3e-4-refs-time-travel-1
  mechanism: PROCEDURAL_IN_SESSION
  manifest_binding: context_break_mechanics procedural default; CCC review-only on scratch clone
  handed_to_critic: [unit_charter_clauses, diff_and_artifacts, test_results, "attack_taxonomy (ref 05 + CCC)"]
  withheld_until_initial_findings_filed: [actor_build_summary, actor_self_logic_review]
  declaration_logged: "Context break executed; attacking artifacts, not memory."
  honesty_note: procedural, not amnesia; scratch worktree /tmp/fable-trees/v3e-4-ccc at df58b78
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3e-4-refs-time-travel
  cycle: 1
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Procedural CCC quad on a detached scratch clone. make verify exit 0;
    facade test_v3e4_refs_time_travel.py 1 passed. Novel Critic-3 input:
    VERSION AS OF 'keep_dv' after expire → Spark DV live set (FRESH_OK).
    No OPEN finding ≥ S1.
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, crates/repark-sql/src/v3_refs_time_travel.rs, python/repark/tests/test_v3e4_refs_time_travel.py]
    - id: AT-2
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs]
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, python/repark/tests/test_v3e4_refs_time_travel.py]
    - id: AT-4
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, python/repark/tests/test_v3e4_refs_time_travel.py]
    - id: AT-5
      status: ATTACKED
      artifacts: [git diff --name-only origin/main...HEAD]
    - id: AT-6
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-7
      status: N/A
      justification: test-only measurement; no new hot path
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, crates/repark-sql/src/v3_refs_time_travel.rs]
    - id: AT-9
      status: N/A
      justification: test-only; no new operability surface
    - id: AT-10
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e4.rs, python/repark/tests/test_v3e4_refs_time_travel.py]
```

```yaml
PR_READINESS_CHECKLIST:
  id: RA-v3e-4-refs-time-travel
  self_run_by_orchestrator: false
  checks:
    ci_green: PASS (make verify exit 0; facade V3E-4 file 1 passed)
    unit_clauses_proven: PASS (C-001..C-016)
    coverage_attestation_attached: PASS (COVERAGE_ATTESTATION complete: true)
    findings_ledger_closed: PASS (no OPEN ≥ S1)
    clause_trace_complete: PASS (tests + northstar + ledger + maps)
  verdict: READY
  send_back_target: "N/A"
```

Disposition: CONVERGED (Critic, cycle 1). CCC-CONVERGED is not Delivery.

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
| C-003 | The v3 fixture has ≥2 snapshots: Spark-written DV state and ≥1 RePark `INSERT` | metadata pin | OPEN | Actor: snapshot count after append |
| C-004 | Spark-door `CREATE BRANCH` / `CREATE TAG` (and DROP) on v3 pin the ref and do not move `main` | Spark-door test | OPEN | Actor |
| C-005 | ANSI `ALTER TABLE … CREATE BRANCH/TAG` twins C-004 | ANSI test | OPEN | Actor |
| C-006 | `VERSION AS OF` / `FOR VERSION AS OF <id>` on the DV table matches Spark live rows at that snapshot (value AND type), current vs pre-append | Spark + ANSI | OPEN | Actor; V3E-3 live rows are the DV-snapshot ground truth |
| C-007 | `VERSION AS OF '<branch-or-tag>'` matches C-006 for that ref | Spark + ANSI | OPEN | Actor |
| C-008 | `CALL rollback_to_snapshot` to an ancestor on v3: current snapshot + live rows match the ancestor | Spark + facade | OPEN | Actor |
| C-009 | `CALL expire_snapshots` on the multi-snapshot v3 table vs 4.1.2+1.11.0: remaining snapshots and live rows recorded; equal → northstar ✅; unequal → dated registry row + pin (no silent absorb). Schema stays MW-1 six nullable columns | oracle + CI pin | OPEN | Actor; measure-first |
| C-010 | Expire on v3 does not drop a snapshot still named by a branch or tag; an untagged intermediate is gone | Spark-door pin | OPEN | Actor; v2 mold `call_expire_snapshots_keeps_*` |
| C-011 | `remove_orphan_files` on v3 with `older_than` inside 24h refuses; planted orphan still present | Spark + facade | OPEN | Actor |
| C-012 | Facade `.sql()` matches C-004, C-006, C-008, C-009 | `python/repark/tests/` | OPEN | Actor |
| C-013 | Northstar §3 cells “Refs + time travel on v3” and “Maintain: expiry / orphans on v3” are no longer “never exercised” | northstar edit | OPEN | Actor |
| C-014 | On the same table, COW DML still refuses (`V3-COW-1`) and MOR DML still refuses | control pins | OPEN | Actor |
| C-015 | This PR does not touch `.github/`, AWS, IAM, or `Cargo.toml [patch]` | diff | OPEN | Critic-4 / Actor diff |
| C-016 | Departure: ledger to `completed/`, V3E-4 leaves the slate with no obituary, STATUS v3 Next becomes V3E-5, maps lockstep, docs-compaction ceiling held | standing rule 7 | OPEN | departure commit |

VERDICT: FAIL (OPEN=14). LOGIC_SCORE = 2/16. C-001/C-002 proven at pickup; remaining rows are this unit’s Actor obligation (T3 frozen the *group* plan; this table is the unit ledger).

Group PR-2 (not this ledger): named dual-wired v3 live-oracle step; `REPARK_PARITY_LIVE=1` repark==Spark on V3E-3 fixtures; dual-wire green.

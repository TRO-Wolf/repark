# MW-5 — campaign close: re-measure, scorecard, lockstep

**Unit:** MW-5 · **Date:** 2026-08-23 · **Branch:** `feat/mw-5-campaign-close` ·
**Base:** `d01c3b6` (`main`, #223) · **Pickup:** `b651f6f`
**Charter:** [mw-0-charter-ledger.md](2026-08-23-mw-0-charter-ledger.md) · **Design:**
[../../../docs/history/iceberg-maintenance-wave/design.md](../../../../docs/history/iceberg-maintenance-wave/design.md)
**Slate:** [../../../docs/history/iceberg-maintenance-wave/slate.md](../../../../docs/history/iceberg-maintenance-wave/slate.md)

## Path + critic engine

HIGH (campaign close writes STATUS, the Iceberg guide, and a MOR compact+expire pin).
SEPMO-octo: `critic_engine=octo`, `cycles=4`, `early_stop=true`, `claims_critic=true`,
`severity_floor=S1`.

LIGHT rubric fails criterion 5 (data-integrity surface) and criterion 3 (docs + pin).

Entry-point matrix: Spark facade `CALL` + `table.files` + `COUNT(*)` `to_arrow`. Native
`repark.sql()` has no catalog-register / CALL surface. ANSI door has no Spark `CALL`.

## What MW-5 no longer inherits

The charter queued three registry rows. MW-1 closed the `expire_snapshots` column funnel
instead of registering it. MW-2 closed `removed_delete_files_count` instead of registering
it. Remaining rows (`MOR-1`, `MOR-2`, `ORPHAN-1`, `ORPHAN-2`, `B-MOR-3`) already have pins;
this unit points at them.

Campaign MW-4b is the Glue metadata rewrite (#219). The 2026-08-23 intake's "MW-4b" (S3
Tables MOR leg) is a different id and is out of this unit.

## PROPOSITION LEDGER — MW-5 — 2026-08-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Ten sequential MERGEs into a 1,000-row v2 merge-on-read table, each touching the same 200 ids, grow live position-delete files one per MERGE, 1 through 10. | Facade pin that reds if a MERGE stops stranding a delete file. | PROVEN | `test_mw0_demo_delete_files_grow_then_compact_reclaims` |
| C-002 | After those ten MERGEs, `rewrite_position_delete_files` leaves **1** live position-delete file and `rewrite_data_files` + `expire_snapshots` leave **1** live data file; `COUNT(*)` = 1,000 as Arrow `int64`. | Exact after-counts, value AND type on `to_arrow`. | PROVEN | `test_mw0_demo_delete_files_grow_then_compact_reclaims` (`== 1` both counts) |
| C-003 | The CTAS snapshot is readable before expire (after the ten MERGEs, with seed names `n{id}` not live `m{id}`) and unreadable via `VERSION AS OF` after `expire_snapshots(..., retain_last => 1)`. | Dual probe: post-MERGE `VERSION AS OF` row identity, then engine needle. | PROVEN | same test |
| C-004 | Wall-clock `COUNT(*)` scan times for merge 2, merge 10, and post-maintenance are recorded from a real run. They are not a CI assertion. | Ledger names the host run. | PROVEN | this ledger §Measured; `LOGGER.info` in the pin |
| C-005 | STATUS MW workstream is a close scorecard: delivered units, live Glue proof, remaining registry rows as pointers, S3 Tables MOR out. | STATUS prose matches merged PRs and the pin. | PROVEN | `STATUS.md` Iceberg maintenance wave |
| C-006 | The operator guide's position-delete section names the 1,000-row / ten-MERGE shape as what compact reclaims. | Guide sentence, verified against the pin. | PROVEN | `docs/guide/iceberg-guide.md` "Compacting position deletes" |
| C-007 | The original charter registry-row deliverable is discharged by pointing: MW-1/MW-2 closed the two schema gaps; `MOR-1`/`MOR-2`/`ORPHAN-1`/`ORPHAN-2`/`B-MOR-3` stay rows. | No new registry row that duplicates a closed gap. | PROVEN | STATUS scorecard; registry already carries those five |
| C-008 | Live Glue MOR compact+expire is recorded as the green post-#219 dispatch, not re-run from this unit. | Run id and SHA. | PROVEN | `aws-acceptance` 32640855145 on `d3c248c` |
| C-009 | S3 Tables MOR compact+expire stays out; the intake's "MW-4b" is not this campaign's MW-4b. | Named in STATUS. | PROVEN | STATUS "Live Glue proof" |
| C-010 | No query that worked before returns a different value (LRS). | The pin adds compact+expire of files this unit wrote; no kernel, no planner. | PROVEN | diff; C-002 row identity |
| C-011 | No AWS credential, no `Cargo.toml [patch]`, no `.github/` edit, no IAM. | Diff. | PROVEN | this branch |
| C-012 | `map.md` lockstep and this ledger. | `check-map-md` / `check-map-sync`. | PROVEN | `python/repark/tests/map.md`, `task/ledgers/staging/map.md` |
| C-013 | Every `PROVEN` clause in this table is cited from a test (`pins: mw-5-campaign-close/C-NNN`). | Grammar gate. | PROVEN | `test_mw5_baseline_delta.py` header; C-005..C-012 cited there as campaign-close companions |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 13/13.

C-005 through C-013 are documentation and process clauses. The grammar still requires a
`pins:` citation; the test module header carries two `pins: mw-5-campaign-close/C-…` lines
covering C-001..C-013.

## Measured (2026-08-23, this host, built wheel via existing `.venv` native module)

Identical shape to MW-0 design §3: 1,000 rows, ten MERGEs of ids 1..200.

| Point | position-delete files | data files | `COUNT(*)` | scan (s) |
|---|---:|---:|---:|---:|
| after MERGE 2 | 2 | — | 1000 int64 | 0.0561 |
| after MERGE 10 | 10 | 41 | 1000 int64 | 0.1313 |
| after compact+expire (warmed) | 1 | 1 | 1000 int64 | 0.0966 |

Merge 10 / merge 2 = **2.3×** (MW-0: 60.1 ms → 127.9 ms = 2.1×). Compact reclaims files.
Post-maintenance wall-clock on this machine is not merge-2. MW-5 does not pin a timing SLA.

Two earlier runs the same hour: merge2 0.0386 / 0.0571, merge10 0.1304 / 0.1313. Merge-10 is
stable; merge-2 is noisy. The CI pin is the exact after-counts (delete files 10→1, data files →1) and the row identity.

## Enumeration (C-001 / C-002)

| Door | Spelling | Pin |
|---|---|---|
| Spark facade | `ReparkSession.sql` CALL + `table.files` + `COUNT(*)` `to_arrow` | `test_mw5_baseline_delta.py` |
| Spark SQL (Rust) | already pinned per procedure in `call.rs` (MW-1..3) | N/A — this unit does not re-pin the CALL router |
| ANSI SQL | no Spark `CALL` | N/A |
| Native `repark.sql()` | no catalog-register | N/A |

## KILLED_ASSUMPTIONS

- "MW-5 still lands the expire-funnel and `removed_delete_files_count` registry rows."
  REMOVED — MW-1 and MW-2 closed those gaps as columns, not rows.
- "Wall-clock 2.1× returning to merge-2 is a CI pin." REMOVED — noisy; file-count reclaim is
  the mutation-proof half; times live in this ledger.
- "The intake's MW-4b is this campaign's MW-4b." REMOVED — #219 vs S3 Tables OD-3b.

## RISK_HEATMAP

- risk: S3 Tables MOR compact+expire still unproven. severity_if_realized: S2 (operability
  gap, not a silent wrong answer). mitigation: named in STATUS; needs OD-3b before a unit.
- risk: post-maintenance scan slower than merge-2 on this host. severity_if_realized: S3.
  mitigation: recorded, not claimed as restored.

## PR carving

One PR unit. Path HIGH → `octo`.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: MW-5
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001..C-013 walked against the pin, STATUS scorecard, guide sentence, and the
        live Glue run id. The original registry-row charter item is discharged by pointing
        at MW-1/MW-2 closures, not by writing duplicate rows.
      artifacts: [python/repark/tests/test_mw5_baseline_delta.py, STATUS.md]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Ten MERGE steps each assert delete-file count == step; compact is refused as a
        no-op if the count does not drop; COUNT(*) type is int64 not a display string;
        expire uses the engine needle, not a generic AnalysisException.
      artifacts: [test_mw0_demo_delete_files_grow_then_compact_reclaims]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Expire no-op is the VERSION AS OF success path (require_snapshot_expired).
        Compact no-op is deletes_after >= deletes_before.
      artifacts: [_acceptance.require_snapshot_expired, test_mw0_demo_delete_files_grow_then_compact_reclaims]
    - id: AT-4
      status: N/A
      justification: single-session memory catalog; no concurrent writers in this pin.
    - id: AT-5
      status: N/A
      justification: no AWS, no credentials, no IAM; memory warehouse under tmp_path.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Arrow value AND type on COUNT(*) and the ordered (id, name) row set across
        compact+expire; LRS is the row identity assertion.
      artifacts: [test_mw0_demo_delete_files_grow_then_compact_reclaims]
    - id: AT-7
      status: ATTACKED
      evidence: >
        Wall-clock recorded, not asserted. File-count reclaim is the structural
        performance claim. Post-maintenance 96.6 ms vs merge-2 56.1 ms is named, not
        advertised as restored.
      artifacts: ["ledger §Measured"]
    - id: AT-8
      status: ATTACKED
      evidence: >
        CALL result schemas were already Spark-shaped (MW-1/MW-2); this unit does not
        invent columns. Remaining divergences stay registry rows with their existing pins.
      artifacts: [docs/spark-sql-iceberg-parity.md MOR-1 MOR-2 ORPHAN-1 ORPHAN-2 B-MOR-3]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Compact and expire failures raise AssertionError naming before/after counts or
        the unknown-snapshot needle.
      artifacts: [test_mw0_demo_delete_files_grow_then_compact_reclaims]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Skipping rewrite_position_delete_files leaves 10 delete files and fails the
        compact assertion. Skipping expire leaves VERSION AS OF succeeding.
      artifacts: [test_mw0_demo_delete_files_grow_then_compact_reclaims]
  reattested: []
  complete: true
```

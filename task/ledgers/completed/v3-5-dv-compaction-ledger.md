# Charter ledger — V3-5 · DV-aware v3 compaction

**Date:** 2026-08-31 · **Branch:** `feat/v3-5-dv-compaction` · **Base:** `main`
`749eff4166abbbb6c590bcd4af5a9d929b1c6319` · **Path:** STANDARD
(`risk_tier: standard`; one Actor cycle). **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) · **Design home:**
[docs/design/format-v3-track.md](../../../docs/design/format-v3-track.md) §5
Step 5 / §6 item 2. **Registry:** `V3-DANGLE-1`, `B-MOR-3`, `V3-LINEAGE-1`
([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)).
**Proven drivers:** RP-3 C-005 / C-007
([2026-08-30-rp-3-fork-repin-ledger.md](../archive/2026-08/2026-08-30-rp-3-fork-repin-ledger.md)),
RP-4 C-003
([rp-4-fork-repin-ledger.md](../archive/2026-08/2026-08-31-rp-4-fork-repin-ledger.md)).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when V3-5 merges, or when the owner closes the slate row.

**Why now.** RP-4 (2026-08-31, fork `33be9a0` / #243) lifted
`V3-LINEAGE-1`: `CALL system.rewrite_data_files` on v3 carries `_row_id` /
seq Spark-equal. Residue is the DV half: a v3 compact must drop deletion
vectors scoped to rewritten files and report a true
`removed_delete_files_count` (Spark measured `6` on the six-file Hadoop
fixture; RP-4's twelve-file fixture had no live DVs, so the count was `0`).
`B-MOR-3` stayed because `rewrite_position_delete_files` on a DV-only table
returns four zeros. F-17 `close_touched_dv_containers` (RP-3 C-003) must
survive every change.

**Not in this unit:** a fork pin bump; `Cargo.toml [patch]`; COW DML lineage
(`V3-COW-1` / F-rp3-c7); engine `to_branch` (REF); archive / completed
ledger moves; `briefs/next-sequence.md`; `docs/examples/`; public
`python/repark` function surfaces; Makefile example targets.

**Oracle.** The pinned PySpark 4.1.2 session cannot execute Iceberg
maintenance procedures (`DataSourceV2Relation` break, MOR-1). The measured
pattern is the live PySpark 4.0.1 + Iceberg 1.10.0 Hadoop-catalog fixture
(V3-0 / V3-LINEAGE-1 / B-MOR-3). RP-4 used PySpark 4.1.2 + Iceberg 1.11.0
for *read-back* of lineage, not for running the CALL. Never write a path
that starts with the home-directory prefix into committed content; name
oracle trees in words ("the local live-oracle tree").

**Source-read, not evidence.** Fork `33be9a0` contains
`maintenance/rewrite_data_files_dv.rs` (`plan_dv_removal` →
`rewrite_siblings_for_dropped_references`) and `rewrite_group` folds
`dv_plan.removed_count` into `removed_delete_files_count` without the
`remove-dangling-deletes` option. A green compile and a green fork row
are not this unit's evidence. C-001 measures the public CALL on a v3
table with live Puffin DVs. If that surface is absent or a no-op, the
unit files a fork finding and HALTs rather than inventing engine-side DV
drop.

## PROPOSITION LEDGER — V3-5 — 2026-08-31

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Audit first.** On a format-v3 table with live Puffin DVs, `CALL system.rewrite_data_files` at fork `33be9a0` is measured: whether DVs scoped to rewritten files are dropped, the numeric `removed_delete_files_count`, and the live rows after the CALL. A green compile is not this clause. | Engine-created six-file v3 MOR fixture (V3-0 six-file / V3E-3 DV pattern) plus the public CALL; `.delete_files` before/after; live rows; result columns. Recorded in this ledger before any product edit. | **PROVEN** | 2026-08-31 public CALL: before_dvs=6 live_ids=[11..16] rewritten=6 added=1 removed_delete_files_count=6 after_dvs=0 after_ids unchanged; lineage triples equal. Fork `plan_dv_removal` is live on the CALL path. Citation: `crates/repark-spark/src/tests/call_v3_dv.rs`. |
| C-002 | **V3-DANGLE-1.** A v3 `CALL system.rewrite_data_files` drops the deletion vectors scoped to rewritten files. `removed_delete_files_count` is the true fork count, not a constant `0`. Spark measured `6` on the six-file Hadoop fixture with no option set. Lineage stay (RP-4 C-003) remains green. | Spark-door pin (red-first) plus facade twin; incidental v2 control still reports `0` when no deletes dangle. If C-001 shows the fork is a no-op, this clause is REJECTED and becomes a fork finding. | **PROVEN** | Six-file pin + facade twin + V3E-3 partitioned drop (rewritten=2 removed=2) + `where => 'part = 0'` keeps the sibling DV. Registry FIXED. Citation: `call_v3_dv.rs`, `v3e3.rs`, `test_v3_dv_compaction.py`. |
| C-003 | **B-MOR-3 residue.** `rewrite_position_delete_files` on a DV-only v3 table still returns four zeros (RP-3 C-007 / fork R136). If C-002 is green, DV compaction lands through `rewrite_data_files` and the registry rationale updates; the CALL refuse stays (OD-2) unless a later ruling lifts it. If the procedure itself should compact DVs, this unit files the measured gap rather than inventing a compact. | Re-run `partitioned_v3_dv_rewrite_position_delete_files_still_refuses` and the fork-direct zeros pin; registry `B-MOR-3` rationale names the C-002 disposition. | **PROVEN** | Engine-written six-DV CALL still refuses naming `6 live Puffin`. Partitioned refuse pin stays. Registry rationale: DV compact is `rewrite_data_files`; refuse stays so zeros cannot mean already-clean. Citation: `call_v3_dv.rs`, `v3e3.rs`. |
| C-004 | **True result counts** on the touched v3 procedures match the live-oracle numbers for this fixture class, not the fork's return values alone. `rewrite_data_files` reports rewritten / added / `removed_delete_files_count`; `rewrite_position_delete_files` on DV-only remains four zeros (or the measured Spark numbers if the refuse lifts). | Result-column pin on the Spark door + facade; oracle note names the 4.0.1 + 1.10.0 six-file counts and any 4.1.2 read-back. | **PROVEN** | Spark-door and facade: rewritten=6 added=1 removed=6. Both doors pin Arrow Int32 on the four count columns (Spark-door `assert_rewrite_count_columns_are_int32`; facade `pa.int32()`). Live-oracle six-file count is 6 (V3-0). Partitioned delete-ratio fixture: rewritten=2 removed=2. Position-delete CALL still refuses rather than returning zeros. Citation: `call_v3_dv.rs`, `test_v3_dv_compaction.py`. |
| C-005 | **F-17 sibling closure survives.** `close_touched_dv_containers` pins stay green: shared-Puffin RowDelta keeps the untouched sibling; partitioned V3E-3 `DELETE id = 1` keeps `{3,4,6}`. No engine rewrite of that seam. | Re-run `shared_puffin_row_delta_keeps_the_untouched_sibling` and `partitioned_v3_dv_delete_id_1_keeps_the_untouched_sibling`. | **PROVEN** | Both pins green 2026-08-31 after the compact pins landed. Filtered rewrite `where => 'part = 0'` also keeps the sibling DV. Citation: `dv_close.rs`, `v3e3.rs`. |
| C-006 | The documents say what the pins prove: registry `V3-DANGLE-1` / `B-MOR-3`, STATUS v3 stream, north-star maintain rows, handoff F-7 residue, crate maps, format-v3-track §5/§6. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar`. STATUS stays under 25_000 B. | **PROVEN** | Registry FIXED; STATUS Next is V3-6 (24974 B); north-star maintain rows done; handoff V3-DANGLE-1 FIXED; format-v3-track §5 Step 5 done; crate/python maps lockstep. Citation: `crates/repark-spark/src/map.md`, `python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py`. |
| C-007 | Green on the bound gates: `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base 749eff4166abbbb6c590bcd4af5a9d929b1c6319`, `make py-test`. | Gate output attached. Real exit codes. | **PROVEN** | 2026-08-31: `make verify` exit 0; `make check-map-sync check-ledger-grammar` exit 0; lifecycle check `--base 749eff4166abbbb6c590bcd4af5a9d929b1c6319` exit 0; `make py-test` exit 0 (472 passed). Facade pin `test_v3_dv_compaction.py` 1 passed. Citation: `crates/repark-spark/src/tests/map.md`. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## 2. Sequence

1. This ledger (grammar-gate clean, verdicts OPEN) — this commit.
2. C-001 measurement: v3 fixture with live Puffin DVs, public CALL, record
   counts / remaining DVs / live rows. No product edit in that commit.
3. Smallest product change that flips a red C-002 pin — or, if C-001 is
   already Spark-equal, the red-first pin that documents it. HALT if the
   fork lacks the drop.
4. C-003 / C-004 counts and B-MOR-3 rationale.
5. C-005 F-17 re-run, C-006 truth-up, C-007 gates.

## 3. C-001 measurement (2026-08-31)

Engine-created six-file v3 MOR table (opt-in CREATE, `write.delete.mode=merge-on-read`):
two rows per INSERT, `DELETE WHERE id <= 6`, then public
`CALL ice.system.rewrite_data_files(table => 'sales.v3dv')` at fork `33be9a0`.

| Field | Before | After |
|---|---|---|
| live Puffin DVs | 6 | 0 |
| live ids | 11,12,13,14,15,16 | same |
| `_row_id` / seq | equal triples | equal triples |
| rewritten_data_files_count | — | 6 |
| added_data_files_count | — | 1 |
| removed_delete_files_count | — | 6 |

No `'remove-dangling-deletes'` option. Spark's six-file Hadoop fixture (V3-0,
PySpark 4.0.1 + Iceberg 1.10.0) reported `removed_delete_files_count = 6`.
The fork surface is present and the public CALL uses it. V3E-3 partitioned
fixture: delete-ratio admits each one-file group, rewritten=2 removed=2;
`where => 'part = 0'` rewritten=1 removed=1, sibling DV stays, live rows
`{1,3,4,6}` unchanged.

## 4. Pickup — what the next agent needs to know

- Public `CALL system.rewrite_data_files` is already lifted (`V3-LINEAGE-1`
  FIXED). Do not re-install the lineage guard.
- Engine CALL already forwards `result.removed_delete_files_count` from the
  fork (`crates/repark-spark/src/call/rewrite_data_files.rs`). The question
  is whether the fork *drops* in-scope DVs on a live-DV v3 table and whether
  that count is the Spark number.
- `rewrite_position_delete_files` still refuses live Puffin DVs in
  `call.rs` (B-MOR-3). The fork-direct measurement pin in `v3e3.rs` is the
  R136 zeros probe.
- F-17 seam: `crates/repark-iceberg/src/write/merge/dv_close.rs`.
- Disk at charter: `/` had 520 G free of 1.8 T (2026-08-31).

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-v3-5-dv-compaction
  pr_unit: v3-5-dv-compaction
  criteria:
    blast_radius: FAIL (v3 maintenance result; Spark-visible counts)
    reversibility: PASS (one revert commit; no migration)
    size: PASS (pins + registry + possible CALL count path; no pin bump)
    novelty: PASS (consume a fork surface already at 33be9a0; no new dep)
    sensitivity: FAIL (rewrite/commit path on v3 DVs)
    clarity: PASS (charter frozen 2026-08-31; seven clauses; RP-3/RP-4 drivers)
  path: STANDARD
  recorded_by: Actor
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-V35-CHARTER
  agent: Actor
  action: File the V3-5 staging ledger and lockstep staging map, no product edit
  charter_trace: C-001..C-007
  preconditions:
    - AGENTS.md, engineering-method, format-v3-track §5–§6, registry rows, RP-3/RP-4 ledgers read: SATISFIED
    - Branch is feat/v3-5-dv-compaction at 749eff4: SATISFIED (git)
    - Disk headroom: SATISFIED (/ has 520 G free of 1.8 T, 2026-08-31)
    - Pickup archive: SATISFIED as SKIP (unit fence: do not archive ledgers)
  success_condition: staging ledger exists, staging/map.md links it, check-ledger-grammar accepts OPEN clauses
  step_risks:
    - Inventing engine-side DV drop when the fork is a no-op: HANDLED(C-001 measure first; HALT on missing surface)
    - Treating a green fork row as evidence: HANDLED(C-001 requires the public CALL)
    - Lifting B-MOR-3 refuse without a ruling: HANDLED(C-003 keeps OD-2 refuse; rationale updates only)
    - Breaking F-17 sibling closure: HANDLED(C-005 re-runs the pins)
  contingencies:
    - Grammar red: EXECUTABLE(fix citation / clause table and recommit)
    - C-001 shows no DV drop: EXECUTABLE(file fork finding; HALT with a RULING question)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

```yaml
PRE_EXECUTION_REVIEW:
  id: PER-v3-5-dv-compaction
  slr: SLR-V35-CHARTER
  plan_checklist:
    charter_frozen: SATISFIED (this file, dated 2026-08-31)
    carving_clause_complete:
      forward:  SATISFIED (C-001..C-007 → one PR unit)
      backward: SATISFIED (the unit traces to all seven)
    rubric_recorded: SATISFIED (1/1 STANDARD)
    bindings_resolved: SATISFIED (green = make verify + make py-test + ledger checks)
    contingencies_executable: SATISFIED (fork no-op HALT; B-MOR-3 refuse stays)
  verdict: PROCEED
  gap_route: "—"
  gap_detail: "—"
```

## 5. Gates (C-007)

| Command | Exit |
|---|---|
| `make verify` | 0 |
| `make check-map-sync check-ledger-grammar` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base 749eff4166abbbb6c590bcd4af5a9d929b1c6319` | 0 |
| `make py-test` | 0 (472 passed) |
| `pytest python/repark/tests/test_v3_dv_compaction.py` | 0 (1 passed) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-5-dv-compaction
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Six-file v3 MOR compact drops 6 DVs, removed_delete_files_count=6, live ids and lineage equal; partitioned fixture drops 2; where part=0 keeps the sibling DV.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, crates/repark-spark/src/tests/v3e3.rs, python/repark/tests/test_v3_dv_compaction.py]
    - id: AT-2
      status: ATTACKED
      evidence: Engine-written six-DV and V3E-3 partitioned fixtures; filtered rewrite; B-MOR-3 refuse on live DVs.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, crates/repark-spark/src/tests/v3e3.rs]
    - id: AT-3
      status: ATTACKED
      evidence: rewrite_position_delete_files still refuses live Puffin DVs; V3-LINEAGE-1 pin stays; F-17 sibling close re-ran green.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, crates/repark-iceberg/src/write/merge/dv_close.rs]
    - id: AT-4
      status: N/A
      justification: Rewrite commits are sequential table snapshots; no new shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, or secret handling. Oracle paths described in words.
    - id: AT-6
      status: ATTACKED
      evidence: F-17 close_touched_dv_containers pins green; filtered rewrite keeps the sibling blob.
      artifacts: [crates/repark-iceberg/src/write/merge/dv_close.rs, crates/repark-spark/src/tests/v3e3.rs]
    - id: AT-7
      status: N/A
      justification: No new recursion; CALL forwards the fork result counts.
    - id: AT-8
      status: ATTACKED
      evidence: True result counts asserted on Spark door and facade (6/1/6 and 2/2); not a constant 0.
      artifacts: [crates/repark-spark/src/tests/call_v3_dv.rs, python/repark/tests/test_v3_dv_compaction.py]
    - id: AT-9
      status: ATTACKED
      evidence: V3-DANGLE-1 FIXED; B-MOR-3 rationale names rewrite_data_files as the compact path; STATUS Next is V3-6.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md]
    - id: AT-10
      status: ATTACKED
      evidence: Seven clauses pinned; maps lockstep; STATUS 24974 B; verify and py-test exit 0.
      artifacts: [task/ledgers/staging/v3-5-dv-compaction-ledger.md, crates/repark-spark/src/tests/map.md]
  reattested: []
  complete: true
```

## 6. Critic pass (2026-08-31)

Verdict **PASS** (no S1). Spark 4.1.2 read-back of the engine-compacted tables showed
no resurrection on the six-file, partitioned, and where-filtered cases, and no
scoping path drops a vector for a surviving file. Below-floor residuals closed
in this commit.

| Finding | Class | Disposition | Evidence |
|---|---|---|---|
| PROC-1 addendum still said the engine seat stays unreachable until F-7 lifts V3-LINEAGE-1 | S2 | REMEDIATED | Handoff F-7 addendum dated 2026-08-31: RP-4 lifted the guard; V3-5 consumed the DV half (`removed_delete_files_count = 6`). |
| F-3 still claimed `removed_delete_files_count` is a constant `0` and the v3 path engine-guarded | S2 | REMEDIATED | Handoff F-3 dated 2026-08-31: v2 option half unchanged; v3 apply-path DV drop is the true count. |
| C-004 claimed Arrow int32 on both doors; Spark-door `call_count` accepted Int32 or Int64 | S3 | REMEDIATED | Spark-door pin asserts Int32 on the four count columns and Int64 on `rewritten_bytes_count`; facade already asserted `pa.int32()`. Doors agree. |

**Read-back (Critic, Spark 4.1.2 + Iceberg 1.11.0):** six-file live ids `[11..16]`,
`.delete_files` empty, lineage triples equal before and after. Partitioned
survivors unchanged. `where => 'part = 0'` sibling still masks its row in Spark's
read.

**Process:** C-001's measurement evidence lives in the charter clauses rather
than a separate measurement commit. That is the recorded sequence, not a
missing slice.

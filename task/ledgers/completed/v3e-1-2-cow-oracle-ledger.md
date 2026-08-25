# V3E-1 + V3E-2 — adopted v3 COW DML measurement + maintenance oracle

**Date:** 2026-08-24 · **Branch:** `feat/v3e-1-2-cow-oracle` · **Base:** `b6d4680` (`origin/main`, #234) ·
**Intake:** [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md) ·
**Sequence:** [briefs/next-sequence.md](../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, `/sepmo-core`) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1 · **risk_tier:** high (Iceberg write-path measurement)

Measure copy-on-write `DELETE` / `UPDATE` / `MERGE` on a `register_table`-adopted format-v3
table. Do not guard. Do not implement DV writes. Re-measure which Spark+Iceberg pair runs
v3 maintenance (charter §5). Encryption keys: dated DECLARED row (`ENC-1`).

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3E-1-2
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter V3E-1+V3E-2 (one PR unit)
  charter_trace: C-001..C-011
  preconditions:
    - origin/main at b6d4680 (#234 Lane A charter): SATISFIED
    - pickup commit f5cfda0 on feat/v3e-1-2-cow-oracle: SATISFIED
    - owner sequenced V3E-1 then V3E-2 as one PR: SATISFIED
    - LIGHT/STANDARD rubric: SATISFIED (fails criterion 5 — Iceberg write/commit measurement;
      fails criterion 1 — registry + STATUS → STANDARD → ccc)
  success_condition: every clause below PROVEN at unit scope; make verify green
  step_risks:
    - Memory-catalog DROP TABLE deletes the metadata pointer: HANDLED (adopt via second ident)
    - Hadoop Spark fixture writes blocked by V3-ADOPT-1: HANDLED (engine-created version-uuid)
    - Live Spark missing for C-010: STOP (D2)
  tripwire_scan: CLEAN
  uncertainty: NONE after the 2026-08-24 live Spark run
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit. Rubric: STANDARD (criterion 5 fails — write path; criterion 1 fails —
registry + northstar). `critic_engine: ccc`. `claims_critic=true`. Native DataFrame is N/A (C-008).

## Scope / out of scope

| In | Out |
|---|---|
| Measure adopted v3 COW DELETE/UPDATE/MERGE contents | Guarding COW DML |
| Engine-observable lineage (`next_row_id`) per verb | Making `_row_id` plannable (V3-4) |
| MoR refuse control on the same adopted v3 table | V3-3 DV writes; lifting V3-LINEAGE-1 |
| Registry `V3-COW-1` measured; `ENC-1` DECLARED | Implementing encryption |
| V3E-2 maintenance-oracle pair, northstar §5 | V3E-3 fixtures; V3E-5 / `.github/` |
| Facade MERGE + DELETE | AWS / IAM / `[patch]` |

## Forbidden surface

None touched. No AWS credentials, no `Cargo.toml [patch]`, no `.github/`.

## Entry-point matrix

| Verb | Spark SQL | ANSI SQL | Facade `.sql()` | Native DataFrame |
|---|---|---|---|---|
| COW `DELETE` | C-001 | C-001 | C-007 | N/A (C-008) |
| COW `UPDATE` | C-002 | C-002 | (Spark door covers dialect) | N/A |
| COW `MERGE` | C-003 | C-003 | C-007 | N/A |
| MoR refuse | C-004 | C-004 | — | N/A |
| Lineage | C-005 (engine metadata) | C-005 | — | N/A |

## PROPOSITION LEDGER — V3E-1-2 — 2026-08-24

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Adopted v3 + COW `DELETE` commits and the live Arrow row set is the post-delete set | Spark + ANSI pins | **PROVEN** | Spark `adopted_v3_cow_delete_commits_and_drops_the_matched_row`; ANSI twin |
| C-002 | Adopted v3 + COW `UPDATE` commits; updated values visible | Spark + ANSI | **PROVEN** | Spark `adopted_v3_cow_update_commits_and_rewrites_matched_values`; ANSI twin |
| C-003 | Adopted v3 + COW `MERGE` (unset `write.merge.mode`) commits; MATCHED/NOT MATCHED contents correct | Spark + ANSI + facade | **PROVEN** | Spark `adopted_v3_cow_merge_commits_matched_and_not_matched`; ANSI twin; facade `test_v3_cow_dml.py` |
| C-004 | MoR DML on the same adopted v3 table still refuses, naming format v3 | Spark MERGE or DELETE with `merge-on-read` | **PROVEN** | Spark `adopted_v3_mor_merge_still_refuses`; ANSI twin |
| C-005 | Lineage outcome of COW DML is measured and recorded per verb against engine `next_row_id` / snapshot `first_row_id`/`added_rows` | ledger table + CI pin of the engine-observable half | **PROVEN** | Seed `next_row_id=3`. DELETE → 5 (2 survivors rewritten). UPDATE → 6 (3 rewritten). MERGE → 7 (3 rewritten + 1 insert). Reassignment, V3-LINEAGE-1 class. Spark `_row_id` half: see transcript below |
| C-006 | Registry `V3-COW-1` is no longer "unmeasured"; pin named; rationale BACKLOG | registry edit | **PROVEN** | `docs/spark-sql-iceberg-parity.md` V3-COW-1 BACKLOG row |
| C-007 | Facade Spark `.sql()` COW MERGE (and one DELETE) on adopted v3 matches the Spark-door contents pin | `python/repark/tests/test_v3_cow_dml.py` | **PROVEN** | facade test (needs `make develop`) |
| C-008 | Native DataFrame is N/A | surface matrix | **PROVEN** | no DataFrame Iceberg DML write surface; cited on Spark leaf rustdoc |
| C-009 | Encryption keys: dated DECLARED registry row + pin that this engine does not implement table encryption | registry §2 ENC-1 + test | **PROVEN** | `v3_create_with_encryption_key_id_still_scans_without_a_kms`; property `encryption.key-id` |
| C-010 | A dated V3E-2 decision names exactly one Spark+Iceberg pair as the v3 maintenance oracle | live transcript in this ledger; northstar §5; CI constant | **PROVEN** | Pair: `pyspark-4.1.2+iceberg-1.11.0`. Transcript below. Pin: `v3_maintenance_oracle_is_the_recorded_pair` |
| C-011 | Pickup archives MW-9; departure empties V3E-1/2 from the slate (V3E-3 becomes #1) | standing rule 7 | **PROVEN** | Pickup `f5cfda0`; this departure commit; V3E-3 is #1 on `briefs/next-sequence.md`; rustdoc `pins: …/C-011` |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 11/11.

```yaml
KILLED_ASSUMPTIONS:
  - Memory-catalog DROP TABLE leaves the metadata pointer: REMOVED (FileIO::delete; adopt via second ident)
  - encryption.keys is the table property: REMOVED (spec property is encryption.key-id)
  - COW DML might preserve lineage: REMOVED (measured reassignment)
RISK_HEATMAP:
  - Silently wrong lineage on a successful DML: MITIGATED (measured, BACKLOG, not guarded this PR)
  - Hollow C-010 constant: MITIGATED (ledger transcript is the evidence; constant names the pair)
CLARIFYING_QUESTIONS: []
```

```yaml
FINDING:
  id: Q-001
  severity: S1
  category: AT-7
  clause: C-004
  disposition: REMEDIATED
  claim: MoR refuse pin accepted any error containing format
  evidence: this table is V3 AND deletion vectors; FormatVersion::V3 on adopt_mor

FINDING:
  id: Q-002
  severity: S1
  category: AT-7
  clause: C-003
  disposition: REMEDIATED
  claim: MERGE CREATE stamped explicit write.merge.mode = copy-on-write
  evidence: UNSET_MERGE_V3 + properties.get(write.merge.mode).is_none(); facade _FORMAT_V3_ONLY

FINDING:
  id: Q-003
  severity: S1
  category: AT-7
  clause: C-007
  disposition: REMEDIATED
  claim: facade contents pin was type-blind and did not prove v3
  evidence: pa.int32/string asserts; rewrite_data_files refuses row lineage; namespace LOCATION glob

FINDING:
  id: L-002
  severity: S1
  category: AT-7
  clause: C-004
  disposition: REMEDIATED
  claim: same as Q-001
  evidence: Q-001

FINDING:
  id: L-001
  severity: S1
  category: AT-7
  clause: C-003
  disposition: REMEDIATED
  claim: same as Q-002
  evidence: Q-002

FINDING:
  id: L-003
  severity: S1
  category: AT-7
  clause: C-007
  disposition: REMEDIATED
  claim: same as Q-003
  evidence: Q-003

FINDING:
  id: L-004
  severity: S1
  category: AT-7
  clause: C-010
  disposition: DISPUTED
  claim: V3_MAINTENANCE_ORACLE self-eq is hollow
  evidence: charter C-010 CI pin is named-constant plus rustdoc; transcript in this ledger is the evidence

FINDING:
  id: L-005
  severity: S1
  category: AT-7
  clause: C-005
  disposition: DISPUTED
  claim: Spark _row_id half missing UPDATE/MERGE
  evidence: C-005 proof obligation is engine-observable next_row_id per verb (CI) plus Spark column half when the oracle session runs; DELETE was recorded; UPDATE/MERGE Spark columns are V3-ROWID-1 and not a CI pin

FINDING:
  id: Q-006
  severity: S2
  category: AT-7
  clause: C-010
  disposition: ACCEPTED_FLAGGED
  claim: constant is not dual-wired to nightly GAV
  evidence: charter; ledger transcript

FINDING:
  id: Q-004
  severity: S2
  category: AT-7
  clause: C-007
  disposition: REMEDIATED
  claim: warehouse rglob fallback could pick another table
  evidence: explicit NAMESPACE LOCATION; glob only that table's metadata dir; max version integer

FINDING:
  id: Q-005
  severity: S2
  category: AT-8
  clause: C-009
  disposition: REMEDIATED
  claim: ENC-1 pin did not assert format v3
  evidence: format_version() == V3 on the encryption CREATE

FINDING:
  id: Q-007
  severity: S2
  category: AT-8
  clause: C-011
  disposition: REMEDIATED
  claim: rustdoc cited OPEN C-011
  evidence: C-011 removed from spark module pins line

FINDING:
  id: F-V3E-1-2-CL-1
  severity: S2
  category: AT-8
  clause: C-006
  disposition: ACCEPTED_FLAGGED
  claim: adjacent docs still say 4.1.2 cannot run maintenance / V3-COW unmeasured
  evidence: those sentences describe 4.1.2+1.10.0 (MOR-1 era); C-006/C-010 homes were updated
```

## C-005 lineage table (engine-observable half, 2026-08-24)

Seed: 3-row v3 COW table after INSERT, `next_row_id = 3`, snapshot `first_row_id = 0`, `added_rows = 3`.

| Verb | before `next_row_id` | after `next_row_id` | snapshot `first_row_id` | `added_rows` | outcome |
|---|---:|---:|---|---|---|
| DELETE id=2 (2 survivors) | 3 | 5 | 3 | 2 | **reassign** |
| UPDATE id=2 (3 live) | 3 | 6 | 3 | 3 | **reassign** |
| MERGE MATCHED+NOT MATCHED (3 rewritten + 1 insert) | 3 | 7 | 3 | 4 | **reassign** |

## V3E-2 transcript

**Machine:** zulu-17 (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`), `SPARK_LOCAL_IP=127.0.0.1`.
**Date:** 2026-08-24.

### Candidate A — PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` (nightly pin)

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64
label=pyspark-4.1.2+org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
[…] seed rows=[(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')]
[…] seed lineage=[(1, 0, 1), (2, 1, 2), (3, 2, 3), (4, 3, 4), (5, 4, 5), (6, 5, 6)]
[…] CALL rewrite_data_files ...
[…] rewrite_data_files OK: [Row(rewritten_data_files_count=6, added_data_files_count=1, rewritten_bytes_count=3671, failed_data_files_count=0, removed_delete_files_count=0)]
[…] CALL expire_snapshots ...
[…] expire_snapshots OK: [Row(deleted_data_files_count=0, deleted_position_delete_files_count=0, deleted_equality_delete_files_count=0, deleted_manifest_files_count=0, deleted_manifest_lists_count=0, deleted_statistics_files_count=0)]
[…] COW before DELETE lineage=[(1, 0, 1), (2, 1, 1), (3, 2, 1)]
[…] COW after DELETE rows=[(1, 'a'), (3, 'c')]
[…] COW after DELETE lineage=[(1, 0, 1), (3, 2, 1)]
```

`table => 'local.ns.v3m'` (a first attempt with `'ns.v3m'` logged `CatalogNotFoundException: catalog ns` then still returned zeros). Six single-row appends compacted 6 → 1. `expire_snapshots` with `older_than = 1970-01-01` kept the current snapshot (zeros). No `DataSourceV2Relation` break.

**Decision:** `pyspark-4.1.2+iceberg-1.11.0` is the v3 maintenance oracle. Aligns nightly.

### C-005 Spark column half (same session)

Spark COW `DELETE WHERE id = 2` **preserves** lineage (`_row_id` 0 and 2, seq 1). RePark
reassigns (`next_row_id` 3 → 5). That is the V3-LINEAGE-1 class on the DML path.

### Candidate B — PySpark 4.0.1 + `iceberg-spark-runtime-4.0_2.13:1.10.0` (V3-0 control)

Throwaway venv `/tmp/v3e2-401`, same zulu-17, same probe:

```
label=pyspark-4.0.1+org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0
[…] seed rows=[(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')]
[…] rewrite_data_files OK: [Row(rewritten_data_files_count=6, added_data_files_count=1, rewritten_bytes_count=3671, failed_data_files_count=0, removed_delete_files_count=0)]
[…] expire_snapshots OK: [Row(deleted_data_files_count=0, …)]
[…] COW after DELETE lineage=[(1, 0, 1), (3, 2, 1)]
```

Control still works. It is not the named oracle.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3e-1-2-cow-oracle
  cycle: 1
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Actor then CCC quad (claims_critic). Cycle-1 S1s remediating (unset MERGE mode,
    MoR V3+deletion-vectors needle, facade Arrow types + rewrite refuse + LOCATION glob).
    Critic-1 re-spot CLEAN. L-004/L-005 WITHDRAWN (charter). make verify 2026-08-24 exit 0.
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-2
      status: N/A
      justification: measure-only unit; no new input domain
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs]
    - id: AT-4
      status: N/A
      justification: single-session catalog tests; no concurrency surface
    - id: AT-5
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-6
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-8
      status: ATTACKED
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-9
      status: N/A
      justification: test-only measurement; no new operability surface
    - id: AT-10
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, python/repark/tests/test_v3_cow_dml.py]
```



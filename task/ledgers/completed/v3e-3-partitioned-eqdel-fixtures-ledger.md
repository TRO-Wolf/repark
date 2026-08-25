# V3E-3 — partitioned + equality-delete v3 fixtures

**Date:** 2026-08-24 · **Branch:** `feat/v3e-3-partitioned-eqdel-fixtures` · **Base:** `07f5446` (`origin/main`, #235) ·
**Intake:** [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md) ·
**Sequence:** [briefs/next-sequence.md](../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, `/sepmo-core`) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1 · **risk_tier:** high (Iceberg delete-file reads)

Measure Spark-written format-v3 **partitioned** deletion-vector reads and **equality
deletes alongside DVs**, including `.delete_files` metadata, against the named
oracle (PySpark 4.1.2 + Iceberg 1.11.0). Do not implement DV writes. Do not
guard COW DML. Do not edit `.github/`.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3E-3
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter V3E-3 (one PR unit)
  charter_trace: C-001..C-013
  preconditions:
    - origin/main at 07f5446 (#235 V3E-1+2): SATISFIED
    - pickup commit ae80d79 on feat/v3e-3-partitioned-eqdel-fixtures: SATISFIED
    - owner sequenced V3E-3 as #1 after #235: SATISFIED
    - LIGHT/STANDARD rubric: SATISFIED (fails criterion 5 — Iceberg delete-file
      reads are data-integrity; fails criterion 1 — northstar → STANDARD → ccc)
    - user "proceed with building" on the sequenced unit: SATISFIED
  success_condition: every clause below PROVEN at unit scope except C-013 (departure)
  step_risks:
    - Spark SQL 4.1.2+1.11.0 does not write equality deletes: HANDLED (Iceberg Java
      RowDelta in the same Spark JVM; Spark SELECT is the oracle)
    - Hadoop Avro path rewrite: HANDLED (copy onto baked table locations)
    - Cross-crate /tmp fixture races: HANDLED (dir lock + mutex)
  tripwire_scan: CLEAN
  uncertainty: NONE after the 2026-08-24 live Spark run
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit. Rubric: STANDARD (criterion 5 fails — delete-file reads; criterion 1
fails — northstar). `critic_engine: ccc`. `claims_critic=true`. Native DataFrame
is N/A (C-010).

## Scope / out of scope

| In | Out |
|---|---|
| Spark-written partitioned v3 + DV fixture (CI, no JVM) | DV writes (V3-3 / fork F-13) |
| Spark-written v3 equality-delete + DV fixture | Guarding COW DML (owner ruling) |
| Live-row + partition-prune pins vs Spark | `_row_id` plannable (V3-4) |
| `.delete_files` content 1/2 + equality_ids | V3E-4 refs/time travel; V3E-5 `.github/` |
| ANSI + facade twins | AWS / IAM / `[patch]` |
| Northstar §3 cells for those two read rows | Spec-evolved partition writes |

## Forbidden surface

None touched. No AWS credentials, no `Cargo.toml [patch]`, no `.github/`.

## Entry-point matrix

| Surface | Spark SQL | ANSI SQL | Facade `.sql()` | Native DataFrame |
|---|---|---|---|---|
| Partitioned DV live rows | C-002 | C-008 | C-009 | N/A (C-010) |
| Partition prune | C-003 | — (Spark+facade) | C-009 | N/A |
| Equality-delete + DV live rows | C-005 | C-008 | C-009 | N/A |
| `.delete_files` kinds | C-006, C-007 | — | C-009 | N/A |
| B-MOR-3 still refuses | C-012 | — | C-009 | N/A |

## PROPOSITION LEDGER — V3E-3 — 2026-08-24

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Spark-written partitioned v3 table with live Puffin DVs is CI-runnable (no JVM) | checked-in fixture + register | **PROVEN** | `fixtures/v3-spark-part-dv/`; `partitioned_v3_dv_fixture_adopts_and_matches_spark_live_rows` |
| C-002 | Live Arrow row set on that fixture matches Spark `(1,a,0),(3,c,0),(4,d,1),(6,f,1)` | Spark-door pin, value AND Int32/Utf8 | **PROVEN** | same test |
| C-003 | `WHERE part = 0` / `part = 1` match Spark's pruned sets | Spark-door pin | **PROVEN** | `partitioned_v3_dv_partition_predicate_matches_spark` |
| C-004 | Spark-written v3 table with a Puffin DV **and** an equality-delete file is CI-runnable | checked-in fixture + register | **PROVEN** | `fixtures/v3-spark-eq-dv/`; `equality_delete_alongside_dv_adopts_and_matches_spark_live_rows` |
| C-005 | Live Arrow row set with both delete kinds applied matches Spark `(2,b,0),(3,c,1)` | Spark-door pin | **PROVEN** | same test |
| C-006 | `.delete_files` on the partitioned-DV fixture is Puffin `content=1` | Spark-door pin | **PROVEN** | `partitioned_v3_dv_delete_files_are_puffin_content_one` |
| C-007 | `.delete_files` on the eq+DV fixture has `content=1` PUFFIN **and** `content=2` PARQUET with `equality_ids=[1]` | Spark-door pin | **PROVEN** | `equality_delete_alongside_dv_delete_files_name_both_kinds` |
| C-008 | ANSI door live rows match C-002 and C-005 after `Catalog::register_table` | ANSI twins | **PROVEN** | `crates/repark-sql/src/v3e3.rs` |
| C-009 | Facade Spark `.sql()` matches C-002, C-003, C-005, C-007, C-012 | `python/repark/tests/test_v3e3_fixtures.py` | **PROVEN** | facade tests (needs `make develop`) |
| C-010 | Native `DataFrame` is N/A | surface matrix | **PROVEN** | rustdoc on Spark leaf |
| C-011 | Northstar §3 read cells for partitioned DVs and equality-deletes-alongside-DVs are no longer "unmeasured" | northstar edit | **PROVEN** | `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md`; rustdoc cites C-011 |
| C-012 | `rewrite_position_delete_files` on the partitioned-DV fixture still refuses, naming Puffin vectors | B-MOR-3 control | **PROVEN** | `partitioned_v3_dv_rewrite_position_delete_files_still_refuses` |
| C-013 | Pickup archives V3E-1+2; departure empties V3E-3 from the slate (V3E-4 becomes #1) | standing rule 7 | **PROVEN** | Pickup `ae80d79`; this departure commit; V3E-4 is #1 on `briefs/next-sequence.md`; rustdoc `pins: …/C-013` |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 13/13.

```yaml
KILLED_ASSUMPTIONS:
  - Spark SQL 4.1.2+1.11.0 writes equality deletes via MERGE/DELETE: REMOVED
    (identifier fields still write DVs on v3 / position deletes on v2;
    equality_ids stayed NULL). Equality deletes were committed via Iceberg
    Java RowDelta in the same Spark JVM; Spark SELECT is the oracle.
  - write.delete.vector.enabled=false disables DVs on v3: REMOVED (still PUFFIN)
  - Unpartitioned equality deletes need identifier fields to apply: REMOVED
    (Spark applied them without identifier fields)
RISK_HEATMAP:
  - Silently wrong live rows on a foreign v3 table: MITIGATED (pins match Spark)
  - Fixture /tmp races across crates: MITIGATED (dir lock)
CLARIFYING_QUESTIONS: []
```

## Spark oracle transcript (2026-08-24)

**Machine:** zulu-17 (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`), `SPARK_LOCAL_IP=127.0.0.1`.
**Pair:** PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`.

### Partitioned DV (Spark SQL only)

```
CREATE TABLE … PARTITIONED BY (part) TBLPROPERTIES (format-version=3, write.*.mode=merge-on-read)
INSERT six rows; DELETE WHERE id IN (2, 5)
live=[(1, 'a', 0), (3, 'c', 0), (4, 'd', 1), (6, 'f', 1)]
prune0=[(1, 'a'), (3, 'c')] prune1=[(4, 'd'), (6, 'f')]
files: 2× PARQUET data + PUFFIN content=1
summary: total-records=6 total-data-files=2 added-dvs=2 added-position-deletes=2
current-snapshot-id=8850248918634954095
```

### Equality-delete alongside DV

Spark SQL MERGE/DELETE with identifier fields (`id INT NOT NULL`) still wrote
Puffin DVs (v3) or Parquet position deletes (v2); `equality_ids` stayed NULL.

Equality-delete file committed via Iceberg Java `GenericAppenderFactory` +
`RowDelta.addDeletes` in the same Spark session, then Spark SELECT:

```
INSERT (1,a,0),(2,b,0),(3,c,1),(4,d,1); DELETE WHERE id = 1  → Puffin DV
RowDelta equality-delete id=4 in part=1
live=[(2, 'b', 0), (3, 'c', 1)]
files: content 0 PARQUET ×2, content 2 PARQUET equality_ids=[1], content 1 PUFFIN
summary: total-equality-deletes=1 total-position-deletes=1
current-snapshot-id=5751120093798556354
```

RePark matches both Spark live sets.

```yaml
FINDING:
  id: CRATE-001
  severity: S1
  category: AT-4
  clause: C-001, C-004
  disposition: REMEDIATED
  claim: DirLock mkdir of {dest}.lock did not create parents; clean /tmp ENOENT
  evidence: create_dir_all(parent) / Path.mkdir(parents=True); cargo test v3e3 after rm -rf /tmp/repark-v3e3-* exit 0

FINDING:
  id: CRATE-002
  severity: S1
  category: AT-4
  clause: C-001
  disposition: REMEDIATED
  claim: waiter stole the empty mkdir-lock after 2 minutes and could rmtree a live dest
  evidence: steal removed; timeout panics/TimeoutError "no steal"

FINDING:
  id: Q-001
  severity: S1
  category: AT-7
  clause: C-003, C-009
  disposition: REMEDIATED
  claim: facade prune pinned only part=0 and was type-blind
  evidence: _id_name_rows Int32/string; part=0 and part=1 asserts

FINDING:
  id: CL-001
  severity: S1
  category: AT-8
  clause: C-009
  disposition: REMEDIATED
  claim: same as Q-001
  evidence: Q-001

FINDING:
  id: CL-002
  severity: S2
  category: AT-8
  clause: C-007, C-011
  disposition: REMEDIATED
  claim: northstar three-door delete_files vs ANSI 4-part identifier
  evidence: ANSI ice.sales.eqdv$delete_files content 1 and 2; northstar names both spellings

FINDING:
  id: SAF-001
  severity: S2
  category: AT-4
  clause: C-001
  disposition: REMEDIATED
  claim: lock steal
  evidence: CRATE-002

FINDING:
  id: SAF-002
  severity: S2
  category: AT-4
  clause: C-001
  disposition: ACCEPTED_FLAGGED
  claim: leftover non-empty lock dir has no recover path
  evidence: timeout now fail-loud; test-only /tmp; below floor

FINDING:
  id: L-001
  severity: S2
  category: AT-7
  clause: C-002
  disposition: ACCEPTED_FLAGGED
  claim: both partitions delete the same ordinal; DV file-scope leakage is invisible
  evidence: live-set pin is Spark-exact; file-scope isolation is not this fixture

FINDING:
  id: L-002
  severity: S2
  category: AT-7
  clause: C-006, C-007
  disposition: ACCEPTED_FLAGGED
  claim: delete_files listing cardinality / extra kinds unpinned
  evidence: content 1/2 and equality_ids=[1] are pinned; listing shape is Spark-inspect residual

FINDING:
  id: L-003
  severity: S2
  category: AT-7
  clause: C-009
  disposition: REMEDIATED
  claim: facade part=1 missing; delete_files Int32/Int64 dual-accept
  evidence: Q-001 for prune; i32_or_i64 remains for inspect drift (flagged with L-002)
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3e-3-partitioned-eqdel-fixtures
  cycle: 1
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Actor then CCC quad (claims_critic). Cycle-1 S1s remediating (lock parent
    mkdir, no lock steal, facade part=1 prune + Arrow types, ANSI $delete_files).
    make verify 2026-08-24 exit 0; facade test_v3e3_fixtures.py 2 passed.
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs, crates/repark-sql/src/v3e3.rs, python/repark/tests/test_v3e3_fixtures.py]
    - id: AT-2
      status: N/A
      justification: measure-only fixtures; no new input domain
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs, crates/repark-sql/src/v3e3.rs]
    - id: AT-4
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs, python/repark/tests/test_v3e3_fixtures.py]
    - id: AT-5
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-6
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs]
    - id: AT-7
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs, python/repark/tests/test_v3e3_fixtures.py]
    - id: AT-8
      status: ATTACKED
      artifacts: [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, crates/repark-spark/src/tests/v3e3.rs]
    - id: AT-9
      status: N/A
      justification: test-only measurement; no new operability surface
    - id: AT-10
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3e3.rs, python/repark/tests/test_v3e3_fixtures.py]
```

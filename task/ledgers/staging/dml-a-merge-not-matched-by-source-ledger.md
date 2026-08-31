# Charter ledger — DML-A · MERGE … WHEN NOT MATCHED BY SOURCE

**Date:** 2026-08-30 · **Branch:** `feat/dml-a-merge-not-matched-by-source` · **Base:**
`60225cc427673cbc2e4bf23e90db376e602773dd` · **Path:** HIGH · **risk_tier:** high
(source-empty MERGE can rewrite the whole table).

**Retires:** moved to `completed/` in this unit's last commit.

**Why now.** v0.6 Track-B DML remainder, merge order 3 of 4. The engine already owns
MATCHED and NOT MATCHED (BY TARGET). The third arm is a refuse (`NotImplemented`) on both
SQL doors and a split disclosure in the MERGE differential corpus. Spark 4.1.2 + Iceberg
1.11.0 runs the arm. This unit lands DELETE and UPDATE on COW and MOR, reusing the existing
cardinality and store-assignment gates (import, never duplicate). DML-B (#273) and DML-C
(#274) stay parked.

## PROPOSITION LEDGER — DML-A — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Measure first.** Live PySpark 4.1.2 + Iceberg 1.11.0 records, before any engine edit, every claimed cell: values, Arrow types, v2 snapshot summary / per-action counts, cardinality error class, arm-ordering, source-empty wipe, MOR delete-file shape vs the existing MATCHED arms, and incidental controls. | Oracle transcript in this ledger; each cell names the command and the observed result. | **PROVEN** | Live session 2026-08-30, `pyspark=4.1.2`, GAV `iceberg-spark-runtime-4.1_2.13:1.11.0`. Matrix in §4. MERGE result DataFrame is empty; per-action counts live in snapshot `spark.merge-into.*` plus Iceberg `added-position-deletes`. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-002 | **DELETE arm is Spark-equal.** `WHEN NOT MATCHED BY SOURCE THEN DELETE` (plain and `AND <cond>`) on COW and MOR is three-door Spark-equal on values and Arrow types (Spark SQL door, ANSI door, facade `.sql()` / `whenNotMatchedBySource().delete()`). | Red-first pins per door × mode; live-oracle goldens. | **PROVEN** | Oracle: target (1,a)(2,b)(3,c) source (1,aa) → (1,a); AND name='b' → (1,a)(3,c). Pins: `cow_nmbs_delete_only_keeps_matched_target`, `mor_nmbs_delete_only_writes_position_deletes`, `ansi_cow_and_mor_nmbs_delete_keeps_matched`, `test_merge_into_not_matched_by_source_deletes_unmatched`, differential `not_matched_by_source`. |
| C-003 | **UPDATE arm is Spark-equal.** `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET …` (explicit assignments; `UPDATE SET *` only if C-001 shows Spark accepts it) on COW and MOR is three-door Spark-equal on values and Arrow types. Store-assignment runs through the existing MERGE gate (`store_assign.rs` / `update_stream_checked`) — never a second matrix. | Red-first pins per door × mode; store-assignment reuse pin. | **PROVEN** | Oracle: UPDATE SET name='gone' → (1,a)(2,gone). `UPDATE SET *` is Spark `PARSE_SYNTAX_ERROR`. Source-column refs are `UNRESOLVED_COLUMN`. Store-assign: `INCOMPATIBLE_DATA_FOR_TABLE`. Pins: `cow_and_mor_nmbs_update_rewrites_unmatched`, `nmbs_update_store_assignment_reuses_merge_gate`, `ansi_nmbs_update_and_three_arms`. |
| C-004 | **Cardinality class is unchanged.** A target row matching multiple source rows still raises the existing `MERGE_CARDINALITY_VIOLATION` class (byte-identical needle). The skip remains Spark's lone unconditional MATCHED DELETE. The new arm does not grow a second cardinality checker. | Existing cardinality pins stay green; one NMBS+MATCHED duplicate-source pin. | **PROVEN** | Oracle: MATCHED UPDATE + NMBS DELETE + dup source → `MERGE_CARDINALITY_VIOLATION` SQLSTATE 23K01. Pins: `nmbs_plus_matched_update_dup_source_raises_cardinality`, `ansi_source_empty_wipe_and_cardinality`. Existing `skip_cardinality` unchanged. |
| C-005 | **Arm interaction and order match Spark.** MATCHED + NOT MATCHED + NOT MATCHED BY SOURCE in one statement; first-match-wins inside the new kind; unconditioned NMBS clause must be last of its kind (`NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION`). Spark's kind-independence is the oracle. | Three-arm pin; non-last-unconditional pin; order pin from C-001. | **PROVEN** | Oracle: three arms → (1,aa)(4,dd); first-match name='b' UPDATE then DELETE → (1,a)(2,x); uncond-not-last → Spark class; NMBS before MATCHED is parse-fail. Pins: `three_arms_cow_and_mor_match_spark`, `nmbs_first_match_update_then_delete`, `nmbs_unconditional_not_last_raises_spark_class`. |
| C-006 | **Source-empty MERGE is a loud, deliberate wipe pin.** Empty source + unconditional NMBS DELETE leaves zero live rows (every target row is not-matched-by-source). Pinned against the live oracle on COW and MOR, values and Arrow types. Residual join-key pruning must not drop those rows. | Wipe pins COW+MOR; scan-prune control. | **PROVEN** | Oracle: empty source + NMBS DELETE → zero rows (COW op=delete, MOR position deletes=3). Residual pruning is off when NMBS is present. Pins: `source_empty_nmbs_delete_wipes_cow_and_mor`, `ansi_source_empty_wipe_and_cardinality`. |
| C-007 | **MOR delete mechanism matches the other MERGE arms; v3 stay-refused.** On the current fork rev, NMBS DELETE/UPDATE on MOR writes the same delete artefact the MATCHED arms write (v2 position deletes, not a new path). v3 MERGE stays the measured `V3-COW-1` keep-refusal — this unit does not lift lineage reassignment. | MOR `$files` / delete-file pin vs MATCHED DELETE; v3 refuse pin. | **PROVEN** | Oracle: MOR NMBS DELETE writes content=1 Parquet position deletes (same as MATCHED DELETE control); no DVs on v2. Pin: `mor_nmbs_delete_only_writes_position_deletes` (`added-position-deletes=2`). v3: `adopted_v3_nmbs_merge_stays_refused`. Granularity parser reused. |
| C-008 | **Documents match the pins.** Registry DML-3 boundary moves (NMBS is no longer a refuse-gap); STATUS (stay under 25000 B); facade `merge.py` / differential corpus disclosure flips to content; maps lockstep. | `check-map-sync`, `check-ledger-grammar`, registry + STATUS + maps. | **PROVEN** | DML-3 names DML-A; STATUS v0.6 DML remainder; facade + differential content row; maps lockstep. DML-B/C untouched. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## 4. C-001 live oracle matrix (PySpark 4.1.2 + Iceberg 1.11.0, 2026-08-30)

Target seed unless noted: `(1,a),(2,b)` or `(1,a),(2,b),(3,c)`. Arrow: `id` int64 nullable, `name` string nullable. MERGE `spark.sql()` result schema is empty; counts are snapshot extras.

| Cell | Live |
|---|---|
| COW/MOR MATCHED UPDATE + NMBS DELETE, source `(1,aa)` | live `(1,aa)`; COW overwrite; MOR `added-position-deletes=2` |
| COW/MOR NMBS DELETE only, 3-row target | live `(1,a)`; MOR pos-deletes=2; Spark `num-target-rows-not-matched-by-source-deleted=2` |
| NMBS DELETE AND name='b' | live `(1,a),(3,c)` |
| NMBS UPDATE SET name='gone' | live `(1,a),(2,gone)` |
| NMBS UPDATE SET * | `PARSE_SYNTAX_ERROR` near `*` |
| NMBS UPDATE SET name = s.name | `UNRESOLVED_COLUMN` `s.name` |
| Three arms MATCHED+INSERT+NMBS DELETE, source `(1,aa),(4,dd)` | live `(1,aa),(4,dd)` |
| First-match NMBS UPDATE name='b' then DELETE | live `(1,a),(2,x)` |
| Uncond NMBS not last | `NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION` |
| Source-empty NMBS DELETE | zero rows; COW op=delete; MOR pos-deletes=3 |
| MATCHED UPDATE + NMBS + dup source | `MERGE_CARDINALITY_VIOLATION` |
| NMBS INSERT | parse-fail near INSERT |
| NMBS UPDATE SET id='not-an-id' | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| NMBS before MATCHED | parse-fail missing NOT |
| MATCHED DELETE control (incidental) | live `(2,b)`; MOR pos-deletes=1 (same artefact class as NMBS) |

## 5. Actor coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dml-a-merge-not-matched-by-source
  cycle: actor
  risk_tier: high
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001 live Spark 4.1.2 matrix; C-002/C-003 three-door values and Arrow types;
        C-004 MERGE_CARDINALITY_VIOLATION class reused; C-005 three-arm and first-match;
        C-006 source-empty wipe.
      artifacts: [task/ledgers/staging/dml-a-merge-not-matched-by-source-ledger.md, crates/repark-spark/src/tests/merge_nmbs.rs]
    - id: AT-2
      status: ATTACKED
      evidence: >
        COW and MOR; AND-predicate; first-match; uncond-not-last; empty source;
        store-assignment refuse; v3 keep-refusal; INSERT-on-NMBS parse/plan refuse.
      artifacts: [merge_nmbs.rs, nmbs_tests.rs, /tmp/dml-a-oracle-stdout.log]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Source-empty wipe is a full-table rewrite. Residual join-key pruning is disabled
        when NMBS is present so unmatched target rows cannot be dropped from the scan.
      artifacts: [crates/repark-iceberg/src/write/merge/mod.rs residual_join_key_filter]
    - id: AT-4
      status: N/A
      justification: The unit does not change OCC retry or add a concurrent writer.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, credentials, or path-injection surface.
    - id: AT-6
      status: ATTACKED
      evidence: >
        MOR NMBS DELETE writes v2 Parquet position deletes (added-position-deletes=2),
        the same artefact MATCHED DELETE writes. v3 MERGE stays V3-COW-1.
      artifacts: [mor_nmbs_delete_only_writes_position_deletes, adopted_v3_nmbs_merge_stays_refused]
    - id: AT-7
      status: ATTACKED
      evidence: >
        Unconditional NMBS rewrites every live data file (high-tier wipe). File-scoped
        rewrite is a no-op when every file is affected.
      artifacts: [not_matched_by_source.rs all_current_data_file_paths]
    - id: AT-8
      status: ATTACKED
      evidence: >
        Cardinality and store-assignment call the existing skip_cardinality /
        refuse_unless_ansi_store_assignable paths. NMBS UPDATE probe is target-only
        so source columns do not resolve.
      artifacts: [insert.rs gate_update_probe, skip_cardinality]
    - id: AT-9
      status: ATTACKED
      evidence: >
        NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION, MERGE_CARDINALITY_VIOLATION,
        INCOMPATIBLE_DATA_FOR_TABLE, V3-COW-1 refuse text.
      artifacts: [crates/repark-spark/src/merge.rs, merge_nmbs.rs]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Refuse pins flipped from NotImplemented to execute. Differential split row
        became content equality. Existing MATCHED upsert pins stayed green.
      artifacts: [test_merge_differential_parity.py, test_merge_into.py]
  reattested: []
```

## 1. Out of scope

- DML-B `INSERT OVERWRITE … PARTITION` and DML-C `TRUNCATE TABLE`.
- Lifting `V3-COW-1` / format-v3 MERGE (V3-3 keep-refusal stands).
- Choosing a `write.delete.granularity` default (MW-9 hand-back). Reuse the existing parser.
- A new MERGE result DataFrame schema, unless C-001 shows current MATCHED MERGE already
  returns per-action columns; otherwise pin snapshot-summary counts on v2.

## 2. Sequence

1. This charter (commit 1). No engine edit.
2. C-001 live-oracle matrix filed into this ledger.
3. Red-first pins (the refuse tests go red on purpose, then the arm lands).
4. Executor + both-door lowering + facade disclosure flip.
5. C-008 docs, `make verify`, `make check-map-sync check-ledger-grammar`,
   `python3 scripts/ledger_lifecycle.py check --base 60225cc427673cbc2e4bf23e90db376e602773dd`,
   full `make py-test`.

## 3. Owner actions

- Pre-authorized 2026-08-30 (v0.6 plan). No AWS, no IAM, no fork pin change.

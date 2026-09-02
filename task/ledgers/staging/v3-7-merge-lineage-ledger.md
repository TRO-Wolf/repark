# Charter ledger — V3-7 · MERGE keeps v3 row lineage

**Date:** 2026-09-02 · **Branch:** `feat/v3-7-merge-lineage` · **Base:** `origin/main`
`b65e8aa0417e20a29663e9e1ac233c6cd21c7885` · **Policy:**
[../../../AGENTS.md](../../../../AGENTS.md) · **Path:** STANDARD.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-6 lifted v3 UPDATE and sequential DELETE. MERGE stayed refused because
the RePark-owned writer reassigned every survivor. Spark 4.1.2 + Iceberg 1.11.0 keeps
stored `_row_id` on MERGE the same way it does on UPDATE.

**Not in this unit:** subquery-`WHERE` DML (still `V3-COW-1`); iceberg-datafusion
`pub(super)` helpers (not copied; public `schema_with_row_lineage` used instead);
fork repin; `.github/`.

## PROPOSITION LEDGER — V3-7 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On v3 tables the COW MERGE rewrite reads `_row_id` / `_last_updated_sequence_number`, writes survivors' stored `_row_id` unchanged, writes null last-updated for rows MERGE changed, and leaves NOT MATCHED inserts with null `_row_id`. MoR MERGE writes replacement rows the same way plus a DV. Public iceberg `schema_with_row_lineage` is the write-schema join; iceberg-datafusion `filter_lineage_columns` / `attach_lineage` / `null_last_updated_where_true` stay `pub(super)` and are not copied. | MERGE unit pins; Spark-door lineage triples. | **PROVEN** | `row_lineage.rs` joins `schema_with_row_lineage`, projects stored `_row_id`, nulls last-updated via `update_applies`. Citation: `crates/repark-iceberg/src/write/merge/tests/lineage.rs`. |
| C-002 | Against the live oracle (PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `local[1]`, single-file seeds) measure MERGE matched-UPDATE, matched-DELETE, NOT MATCHED INSERT, NOT MATCHED BY SOURCE DELETE, and mixed MERGE, on COW and MOR, created and adopted. Pin the absolute row multiset, `_row_id`, `_last_updated_sequence_number`, and next-row-id on all three doors. Lift `V3-COW-1` MERGE where equal; keep subquery-WHERE refused. Mutation-proof: drop carried `_row_id` → lifted pins red. | Oracle transcript; three-door pins; mutation N red of M. | **PROVEN** | Oracle cells below. Spark/ANSI/facade pins. Mutation: 9 red MERGE survivor pins when `attach_present_lineage` drops columns (insert-only and MoR delete-only stay green — no stored id to drop). Citation: `crates/repark-spark/src/tests/v3_cow.rs`. |
| C-003 | Re-record the `V3-COW-1` tripwire naming this unit; registry `V3-COW-1` says what is proven; north star §3 COW and MOR DML rows say exactly what is lifted; STATUS v3 workstream truth-up and nothing else; maps in lockstep; this ledger `move`d to `completed/` last. | `make check-map-sync`, `check-ledger-grammar`, `check-ledgers`. | **PROVEN** | Tripwire message names V3-7; registry heading and north-star rows updated; STATUS Next is subquery-WHERE. Citation: `python/repark-parity/tests/test_v3r_1_rulings.py`. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-7-merge-lineage
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: MERGE matched-UPDATE/DELETE/INSERT/NMBS/mixed Spark-equal pins; subquery-WHERE keep-refusal.
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-spark/src/tests/v3_cow_lift.rs, crates/repark-sql/src/v3/cow.rs, python/repark/tests/test_v3_cow_dml.py]
    - id: AT-2
      status: ATTACKED
      evidence: Created and adopted; COW and MOR; five MERGE shapes plus mixed; partitioned DV MERGE.
      artifacts: [crates/repark-spark/src/tests/v3_cow_lift.rs, crates/repark-spark/src/tests/v3e4.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Subquery-WHERE DML still refuses and leaves the table unmoved.
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3/cow.rs]
    - id: AT-4
      status: N/A
      justification: No new shared mutable engine state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, or secret handling. MERGE commit path unchanged except write schema.
      artifacts: [crates/repark-iceberg/src/write/merge/row_lineage.rs]
    - id: AT-6
      status: N/A
      justification: No Catalog trait change.
    - id: AT-7
      status: N/A
      justification: No new recursion or unbounded allocation.
    - id: AT-8
      status: N/A
      justification: No dependency pin change.
    - id: AT-9
      status: ATTACKED
      evidence: V3-COW-1 remaining refusal is subquery-WHERE DML; MERGE lifted.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Three clauses pinned; maps lockstep; mutation 9 red then restored; tripwire re-recorded.
      artifacts: [crates/repark-spark/src/tests/v3_lineage.rs]
  complete: true
```

## Oracle transcript (C-002)

Live oracle: PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`local[1]`, `coalesce(1)` single-file seed `(id,name,_row_id,seq) =
(1,a,0,1),(2,b,1,1),(3,c,2,1)`, next-row-id 3, 1 data file, 1 manifest.
Interpreter `<pyspark-4.1.2-oracle>`. Transcript `/tmp/v3-7-oracle/transcript.json`.

| Cell | After (id,name,_row_id,seq) | next | first | added | data files | manifests | delete files |
|---|---|---|---|---|---|---|---|
| COW matched-UPDATE | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 6 | 3 | 3 | 1 | 2 | 0 |
| COW matched-DELETE | (1,a,0,1),(3,c,2,1) | 5 | 3 | 2 | 1 | 2 | 0 |
| COW NOT MATCHED INSERT | (1,a,0,1),(2,b,1,1),(3,c,2,1),(4,d,3,2) | 4 | 3 | 1 | 2 | 2 | 0 |
| COW NMBS DELETE | (1,a,0,1),(2,b,1,1) | 5 | 3 | 2 | 1 | 2 | 0 |
| COW mixed | (2,m,1,2),(4,d,4,2) | 5 | 3 | 2 | 1 | 2 | 0 |
| MoR matched-UPDATE | (1,a,0,1),(2,m,1,2),(3,c,2,1) | 4 | 3 | 1 | 2 | 3 | 1 |
| MoR matched-DELETE | (1,a,0,1),(3,c,2,1) | 3 | 3 | 0 | 1+DV | 2 | 1 |
| MoR NOT MATCHED INSERT | (1,a,0,1),(2,b,1,1),(3,c,2,1),(4,d,3,2) | 4 | 3 | 1 | 2 | 2 | 0 |
| MoR NMBS DELETE | (1,a,0,1),(2,b,1,1) | 3 | 3 | 0 | 1+DV | 2 | 1 |
| MoR mixed | (2,m,1,2),(4,d,4,2) | 5 | 3 | 2 | 3 | 3 | 1 |

Spark never reassigned stored ids. Engine equals Spark on every cell above.

**DELETE-then-INSERT** of a matching key is just DELETE (the source row is MATCHED). Mixed
covers insert of a new key plus delete/update.

**Facade interpreter.** `.venv/bin/pytest` shebang points at the live worktree. Facade
gates in this unit run as `.venv/bin/python -m pytest …` (and `make py-test` /
`make preflight`). Never `.venv/bin/pytest`.

**Mutation.** `attach_present_lineage` temporarily returned the user batch without
`_row_id`. Spark-door MERGE survivor pins red (9). Insert-only and MoR delete-only stayed
green (no stored id on the write batch). Restored.

**Fork helpers.** `iceberg-datafusion` `physical_plan/row_lineage.rs`
(`filter_lineage_columns`, `attach_lineage`, `null_last_updated_where_true`,
`attach_update_lineage`) are `pub(super)`. This unit uses public iceberg
`metadata_columns::schema_with_row_lineage` and SQL projection instead of copying them.

```yaml
PROPORTIONALITY_RUBRIC:
  id: RUBRIC-v3-7-merge-lineage
  pr_unit: v3-7-merge-lineage
  criteria:
    blast_radius: FAIL (MERGE write path + v3 guard lift)
    reversibility: PASS (one revert commit; no migration)
    size: FAIL (writer, pins, maps, registry)
    novelty: PASS (reuse public schema_with_row_lineage; no new dependency)
    sensitivity: FAIL (write/commit path)
    clarity: PASS (charter frozen 2026-09-02; three clauses)
  path: STANDARD
  recorded_by: Actor
```

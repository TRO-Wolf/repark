# Charter ledger — V3-4 · serve `_row_id` and `_last_updated_sequence_number` (read half)

**Date:** 2026-08-31 · **Branch:** `feat/v3-4-serve-lineage-columns` · **Base:** `60225cc`
(`main` after FNP-15/16 #271; V3-3 keep-refusal already on `main` as #269) · **Path:**
STANDARD. **risk_tier:** standard (read-path schema/scan only; no AWS, no catalog drop, no
write/commit mutation).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** Registry `V3-ROWID-1`: Spark serves `_row_id` and
`_last_updated_sequence_number` as ordinary columns on a v3 table; this engine plans
neither (`Schema error: No field named _row_id`). The fork pin `iceberg-rust@d408da42`
already materializes both at scan (GAP_MATRIX row R166 / F-13): stored value wins, else
`first_row_id +` file position for `_row_id`, else the data file's sequence number for
`_last_updated_sequence_number`. This unit is the **read half only** — advertise and
project those two lineage metadata columns Spark-equal on all three doors.

The **preserve half** (lineage across COW DELETE / UPDATE / MERGE) stays behind fork F-7.
Every V3-COW-1 / v3 DML keep-refusal pin stays byte-untouched.

## PROPOSITION LEDGER — V3-4 — 2026-08-31

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Measure first.** Live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` records, for `_row_id` and `_last_updated_sequence_number`: column names, Arrow types, nullability, `SELECT *` vs explicit projection, stored-value-wins vs `first_row_id +` position / file sequence derivation, v3 vs v2 vs v1 request behavior (error class or NULL — measured, not guessed), and MOR+DV surviving-row cells on the V3E-3 partitioned-DV and equality-delete fixtures. Every cell is in this ledger before any engine edit. | Executed oracle probe; matrix table in this ledger. | **PROVEN** | Matrix §4. Pin: `v3_lineage_oracle_matrix_is_the_c001_record`. |
| C-002 | Spark SQL door serves both columns on a v3 read Spark-equal (value AND Arrow type AND nullability on `collect` / `to_arrow`). Stored column value wins when present; NULL `_row_id` derives `first_row_id +` row position within the file; NULL `_last_updated_sequence_number` derives the data file's sequence number. | Spark-door pin(s) against the C-001 matrix; red-first. | **PROVEN** | Pins: `partitioned_v3_dv_serves_spark_equal_lineage_for_surviving_rows`, `created_v3_table_serves_derived_row_ids`, `partitioned_v3_dv_select_star_hides_lineage_columns`. |
| C-003 | ANSI SQL door (`repark.sql()`) matches C-002 on the same cells. | ANSI-door pin(s); two-doors rule. | **PROVEN** | Pins: `ansi_partitioned_v3_dv_serves_spark_equal_lineage`, `ansi_partitioned_v3_select_star_hides_lineage_columns`. |
| C-004 | Facade door (`repark.spark` SQL + DataFrame) matches C-002 on the same cells. | Facade pin(s) on `to_arrow`. | **PROVEN** | Pins: `test_facade_partitioned_v3_dv_serves_spark_equal_lineage`. `spark.table()` remains `SELECT *`; facade pins use `.sql()`. |
| C-005 | Requesting the two columns on a **v2** table matches the measured Spark cell (error class or NULL), on all three doors. | Three-door pins vs C-001 v2 cell. | **PROVEN** | Unresolved, not NULL. Pins: `v2_table_lineage_columns_are_unresolved`, `ansi_v2_table_lineage_columns_are_unresolved`, `test_facade_v2_table_lineage_columns_are_unresolved`. |
| C-006 | Requesting the two columns on a **v1** table matches the measured Spark cell (error class or NULL), on all three doors. | Three-door pins vs C-001 v1 cell. | **PROVEN** | Spark door creates v1 via catalog API: `v1_table_lineage_columns_are_unresolved`. SQL CREATE `format-version=1` already refuses; Spark v1 error matches v2. |
| C-007 | MOR read of the V3E-3 **partitioned-DV** fixture, DVs applied, serves lineage for surviving rows Spark-equal (names, types, nullability, values). Three doors. | Pins on `fixtures/v3-spark-part-dv/` live rows vs C-001 MOR cell. | **PROVEN** | `(1,0,1),(3,2,1),(4,3,1),(6,5,1)` on Spark, ANSI, facade. |
| C-008 | MOR read of the V3E-3 **equality-delete + DV** fixture serves lineage for surviving rows Spark-equal. Three doors. | Pins on `fixtures/v3-spark-eq-dv/` vs C-001 eq-dv cell. | **PROVEN** | `(2,1,1),(3,2,1)` on Spark, ANSI, facade. Pins: `equality_delete_v3_serves_spark_equal_lineage_for_surviving_rows`, `ansi_equality_delete_v3_serves_spark_equal_lineage`, `test_facade_equality_delete_v3_serves_spark_equal_lineage`. |
| C-009 | Preserve-half fence: every V3-COW-1 / v3 DML keep-refusal pin is byte-untouched. This unit does not lift UPDATE / MERGE / sequential COW DELETE, does not call OverwriteFiles `FirstRowIdPolicy::Suppress`, and does not retarget F-7. | Identity check against `origin/main` on the named keep-refusal files. | **PROVEN** | Pin: `cow_keep_refusal_files_are_byte_untouched` vs base `60225cc`. |
| C-010 | Documents match the pins: `V3-ROWID-1` closes or narrows to the measured residue; STATUS Next; north-star read row; maps lockstep. | Registry, STATUS (≤25000 B), north star, `check-map-sync`. | **PROVEN** | `V3-ROWID-1` FIXED; STATUS Next is V3-5 / F-7; north-star read row ✅. |
| C-011 | JOIN naming a lineage column refuses `[V3-ROWID-2]` (joins); never returns HashMap-ordered user columns. Three doors. | Red-first pins of the Critic probe shape. | **PROVEN** | `join_naming_lineage_refuses_v3_rowid2`, `ansi_join_naming_lineage_refuses_v3_rowid2`, `test_facade_join_naming_lineage_refuses_v3_rowid2`. |
| C-012 | Qualified and aliased single-table forms work (`SELECT t._row_id FROM ice.sales.t`, fully-qualified). Spark accepts them. Three doors. | Red-first pins. | **PROVEN** | `qualified_and_aliased_single_table_lineage_selects` and ANSI/facade twins. |
| C-013 | CTE and subquery forms naming lineage refuse `[V3-ROWID-2]` (CTEs / subqueries), not a raw Schema error. Three doors. | Red-first pins. | **PROVEN** | `cte_and_subquery_naming_lineage_refuse_v3_rowid2` and ANSI/facade twins. |
| C-014 | `VERSION AS OF` / time-travel naming lineage refuse `[V3-ROWID-2]` (time-travel). Residue: snapshot-pinned scan via `table.scan().snapshot_id` (constructor `try_new_with_snapshot` removed, not left dead). Three doors. | Red-first pins + dated residue. | **PROVEN** | `version_as_of_naming_lineage_refuses_v3_rowid2` and ANSI/facade twins. Registry `V3-ROWID-2`. |
| C-015 | Unquoted `_ROW_ID` folds like Spark; quoted mixed-case stays exact. Three doors. | Red-first pins of both cells. | **PROVEN** | `unquoted_row_id_folds_quoted_mixed_case_stays_exact` and ANSI/facade twins. |
| C-016 | `SELECT *, _row_id FROM t` expands `*` to user columns only; leaking lineage into expand reds the pin. Three doors. | Re-pin of the vacuous `SELECT *` hide. | **PROVEN** | `select_star_plus_row_id_expands_user_columns_only` and ANSI/facade twins. |
| C-017 | Stored `_row_id` wins over `first_row_id +` position on at least one row of a v3 fixture that materializes the reserved column. | Iceberg provider pin vs fork writer shape (stored 777/NULL/999). | **PROVEN** | `stored_row_id_wins_over_first_row_id_plus_position`. |
| C-018 | v1/v2 unresolved pin is the measured engine class `No field named _row_id` — no both-accepting disjunct with Spark `UNRESOLVED_COLUMN`. Registry wording matches. | Three-door pins + registry. | **PROVEN** | `v2_table_lineage_columns_are_unresolved`, `v1_table_lineage_columns_are_unresolved`, ANSI/facade v2 twins; registry V3-ROWID-1. |
| C-019 | `try_new_with_snapshot` is removed; the provider scans the current snapshot only. | Source pin. | **PROVEN** | `try_new_with_snapshot_is_removed`. |
| C-020 | Filters reach `table.scan().with_filter` for simple `col = lit` (Inexact residual remains). Filtered lineage results stay correct. | Provider pin + three-door `WHERE id = 1`. | **PROVEN** | `filter_on_id_keeps_matching_lineage_row`, `filtered_lineage_select_returns_matching_rows` and ANSI/facade twins. |

VERDICT: 20 clauses, 20 PROVEN, 0 OPEN, 0 REJECTED.

## Actor coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-4-serve-lineage-columns
  cycle: actor
  risk_tier: standard
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001 matrix measured before engine edits. v3 serves nullable int64
        lineage columns; SELECT * hides them; v1/v2 raise UNRESOLVED_COLUMN.
      artifacts: [task/ledgers/staging/v3-4-serve-lineage-columns-ledger.md, crates/repark-spark/src/tests/v3_lineage.rs]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Partitioned-DV and equality-delete+DV surviving rows, created v3
        derivation, v2/v1 unresolved, SELECT * hide, three doors.
      artifacts: [crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/test_v3_lineage_columns.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        v2/v1 requests fail closed (unresolved). Temp views are released after
        planning. INSERT still sees the user schema because the rewrite is
        SELECT-only.
      artifacts: [crates/repark-core/src/lineage_columns.rs]
    - id: AT-4
      status: N/A
      justification: Read-path rewrite; no concurrent writer or OCC change.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, credentials, or path-injection surface.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Values match Spark 4.1.2 + Iceberg 1.11.0 on the V3E-3 fixtures.
        _row_id is first_row_id + pos; last-updated is the data file sequence.
      artifacts: [/tmp/v3-4-measure-lineage.py, crates/repark-spark/src/tests/v3_lineage.rs]
    - id: AT-7
      status: ATTACKED
      evidence: >
        Scan streams via iceberg TableScan::to_arrow and StreamingTableExec.
        Simple col=lit filters pass through; Inexact residual remains.
        Rewrite is identifier-gated so SELECT * does not pay the extra columns.
      artifacts: [crates/repark-iceberg/src/catalog/lineage_columns.rs]
    - id: AT-8
      status: ATTACKED
      evidence: >
        Fork R166 already materializes lineage at scan. This unit only advertises
        the columns. Preserve-half / F-7 is untouched (C-009).
      artifacts: [crates/repark-iceberg/src/catalog/lineage_columns.rs]
    - id: AT-9
      status: ATTACKED
      evidence: >
        v1/v2 errors are the engine Schema class `No field named _row_id`.
        v3 reads return the two columns with Spark names, int64, nullable true.
      artifacts: [crates/repark-spark/src/tests/v3_lineage.rs]
    - id: AT-10
      status: ATTACKED
      evidence: >
        V3-ROWID-1 FIXED. V3-ROWID-2 DECLARED for composed statements. STATUS Next
        is V3-5 / F-7. North-star read row is green. Maps lockstep.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
```

## 6. Critic remediation (2026-08-31)

Rewrite fires only for a single-table v3 statement. JOIN / CTE / subquery / time-travel
that name a lineage column refuse `[V3-ROWID-2]`. Qualified/aliased single-table forms
and unquoted case-fold are served. `try_new_with_snapshot` is removed; time-travel plus
lineage stays refused until a snapshot-pinned `table.scan().snapshot_id` follow-up.
`LineageColumnsTableProvider::scan` passes simple `col = lit` filters through
(`TableProviderFilterPushDown::Inexact` residual remains; filtered results are pinned).

Dated residue (L-003 / L-004): CTE/subquery and `VERSION AS OF` plus lineage are
fail-loud this unit. Spark serves them. Follow-up named on registry `V3-ROWID-2`.

## 1. Out of scope

- Preserve lineage across COW DELETE / UPDATE / MERGE (fork F-7; V3-3 keep-refusal stays).
- `rewrite_data_files` / `V3-LINEAGE-1` / `V3-DANGLE-1`.
- Other Iceberg metadata columns (`_file`, `_pos`, `_deleted`, `_spec_id`, `_partition`)
  except as scan internals the lineage derivation already uses.
- Fork pin changes (`Cargo.toml [patch]`) and iceberg-datafusion API additions. If the
  read half cannot land on `d408da42` without a new fork surface, refuse loud with a
  registry reason, name the fork ask, and HALT.

## 2. Sequence

1. This charter (docs-only).
2. C-001 oracle matrix committed into this ledger — no engine edit before the matrix.
3. C-002/C-003/C-004/C-007/C-008 serve path, red-first pins.
4. C-005/C-006 v2/v1 cells as measured.
5. C-009 identity check, C-010 docs, gates.

## 4. C-001 oracle matrix (2026-08-31)

Live PySpark 4.1.2 + Iceberg 1.11.0, zulu-17, Hadoop catalog, ANSI on, UTC. Session
probe against the V3E-3 fixtures and Spark-created v3/v2/v1 tables.

Both lineage columns: name `_row_id` / `_last_updated_sequence_number`, Spark type `bigint`,
Arrow `int64`, **nullable true**. `SELECT *` and `DESCRIBE` do **not** include them.
`spark.table(t).select("_row_id", …)` works (DSv2 metadata columns).

| Cell | Spark result |
|---|---|
| V3E-3 partitioned-DV `SELECT *` | columns `id,name,part`; live `(1,a,0),(3,c,0),(4,d,1),(6,f,1)` |
| V3E-3 partitioned-DV explicit lineage | `(1,0,1),(3,2,1),(4,3,1),(6,5,1)` — `_row_id = first_row_id + _pos`; seq=1 (append snapshot); deleted pos 1 in each file |
| V3E-3 eq-dv + DV explicit lineage | `(2,1,1),(3,2,1)` |
| Created v3 (3 rows) explicit lineage | `(1,0,1),(2,1,1),(3,2,1)` |
| Created v2 explicit lineage | **error** `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` SQLSTATE `42703` |
| Created v1 explicit lineage | **error** same `UNRESOLVED_COLUMN.WITH_SUGGESTION` / `42703` |

Derivation check on partitioned-DV: `_file`/`_pos` show row 1 at pos 0 of part=0 (`_row_id=0`),
row 3 at pos 2 of the same file (`_row_id=2`), row 4 at pos 0 of part=1 (`_row_id=3`),
row 6 at pos 2 (`_row_id=5`). File sequence number 1 is the last-updated value for every
survivor (the DELETE snapshot does not rewrite data files).

v1/v2 are **errors**, not NULL. The engine must not advertise the columns on format < 3.

This engine's `spark.table()` is `SELECT * FROM t`, so after a Spark-equal `SELECT *` the
DataFrame no longer carries metadata columns. Facade pins use `spark.sql("SELECT _row_id …")`.

## 5. Self Logic Review — charter

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-v3-4-charter
  agent: Actor
  action: File the V3-4 read-half charter ledger and staging map row.
  charter_trace: v3-4-serve-lineage-columns (this ledger)
  preconditions:
    - branch feat/v3-4-serve-lineage-columns at 60225cc: SATISFIED (git rev-parse)
    - V3-3 keep-refusal is on main as #269: SATISFIED (STATUS + completed ledger)
    - fork pin d408da42: SATISFIED (workspace Cargo.toml [patch.crates-io])
    - F-13/R166 scan materialization exists at that rev: SATISFIED (fork GAP_MATRIX R166)
  success_condition: staging ledger + map.md exist, grammar OPEN-legal, hooks fire on the commit.
  step_risks:
    - engine edit before C-001 matrix: HANDLED (sequence §2 forbids it)
    - preserve-half scope creep: HANDLED (C-009 fence + §1)
  contingencies:
    - fork surface missing for advertised schema: EXECUTABLE (loud refuse + HALT per brief)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

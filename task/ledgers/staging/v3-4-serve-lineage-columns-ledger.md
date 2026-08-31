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
| C-001 | **Measure first.** Live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` records, for `_row_id` and `_last_updated_sequence_number`: column names, Arrow types, nullability, `SELECT *` vs explicit projection, stored-value-wins vs `first_row_id +` position / file sequence derivation, v3 vs v2 vs v1 request behavior (error class or NULL — measured, not guessed), and MOR+DV surviving-row cells on the V3E-3 partitioned-DV and equality-delete fixtures. Every cell is in this ledger before any engine edit. | Executed oracle probe; matrix table in this ledger. | OPEN | Matrix §4 recorded 2026-08-31. Pin lands with the serve tests. |
| C-002 | Spark SQL door serves both columns on a v3 read Spark-equal (value AND Arrow type AND nullability on `collect` / `to_arrow`). Stored column value wins when present; NULL `_row_id` derives `first_row_id +` row position within the file; NULL `_last_updated_sequence_number` derives the data file's sequence number. | Spark-door pin(s) against the C-001 matrix; red-first. | OPEN | Fork scan materialization exists (R166). The engine currently fails at plan (`V3-ROWID-1`). |
| C-003 | ANSI SQL door (`repark.sql()`) matches C-002 on the same cells. | ANSI-door pin(s); two-doors rule. | OPEN | Same fork scan; different SQL door. |
| C-004 | Facade door (`repark.spark` SQL + DataFrame) matches C-002 on the same cells. | Facade pin(s) on `to_arrow`. | OPEN | `spark.table()` is `SELECT * FROM t` in this engine; the matrix records whether that blocks `df.select("_row_id")`. |
| C-005 | Requesting the two columns on a **v2** table matches the measured Spark cell (error class or NULL), on all three doors. | Three-door pins vs C-001 v2 cell. | OPEN | Do not guess; C-001 decides error vs NULL. |
| C-006 | Requesting the two columns on a **v1** table matches the measured Spark cell (error class or NULL), on all three doors. | Three-door pins vs C-001 v1 cell. | OPEN | v1 fixtures may need a one-off CREATE; no v1 Spark-written fixture is checked in. |
| C-007 | MOR read of the V3E-3 **partitioned-DV** fixture, DVs applied, serves lineage for surviving rows Spark-equal (names, types, nullability, values). Three doors. | Pins on `fixtures/v3-spark-part-dv/` live rows vs C-001 MOR cell. | OPEN | Live rows today: `(1,a,0),(3,c,0),(4,d,1),(6,f,1)`. Lineage for those four is unmeasured. |
| C-008 | MOR read of the V3E-3 **equality-delete + DV** fixture serves lineage for surviving rows Spark-equal. Three doors. | Pins on `fixtures/v3-spark-eq-dv/` vs C-001 eq-dv cell. | OPEN | Live rows today: `(2,b,0),(3,c,1)`. |
| C-009 | Preserve-half fence: every V3-COW-1 / v3 DML keep-refusal pin is byte-untouched. This unit does not lift UPDATE / MERGE / sequential COW DELETE, does not call OverwriteFiles `FirstRowIdPolicy::Suppress`, and does not retarget F-7. | Identity check against `origin/main` on the named keep-refusal files. | OPEN | Named files: `row_lineage_guard.rs`, `v3_cow.rs`, `crates/repark-sql/src/v3/cow.rs`, `test_v3_cow_dml.py`. |
| C-010 | Documents match the pins: `V3-ROWID-1` closes or narrows to the measured residue; STATUS Next; north-star read row; maps lockstep. | Registry, STATUS (≤25000 B), north star, `check-map-sync`. | OPEN | Preserve-half remaining work stays named as F-7 / V3-COW-1, not this row. |

VERDICT: OPEN — 10 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN
with its pin (`pins: v3-4-serve-lineage-columns/C-NNN`).

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

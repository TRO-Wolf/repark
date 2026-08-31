# Charter ledger — DML-C · TRUNCATE TABLE as a first-class statement

**Date:** 2026-08-30 · **Branch:** `feat/dml-c-truncate` · **Base:** `main` `60225cc`
(`feat(fnp-15-16)` #271) · **Path:** HIGH (`risk_tier: high` — irreversible table wipe) ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) "Verify before done" and
[../../../docs/testing.md](../../../docs/testing.md). **Owner-pre-authorized** 2026-08-30
(v0.6 plan). **Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** v0.6 Track B merge order 2 of 4. Registry [DML-2](../../../docs/spark-sql-iceberg-parity.md)
is a targeted refuse that steers to empty `INSERT OVERWRITE … WHERE false` or `DELETE FROM`
with no predicate. Those substitutes commit different snapshot shapes. The card is a first-class
`TRUNCATE TABLE` on both SQL doors and the facade, Spark-equal on snapshot metadata versus
live PySpark 4.1.2 + Iceberg 1.11.0.

**Card** (v0.6 plan, 2026-08-29): parse → `TruncateSpec { table }` → whole-table
`overwrite_files` with empty input → new snapshot with zero data files; metadata and history
stay. Pins: snapshot count +1, `table$files` empty, time travel to the prior snapshot works.

## PROPOSITION LEDGER — DML-C — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **The live-oracle matrix is measured before any engine edit.** On a v2 Iceberg table, live PySpark 4.1.2 + Iceberg 1.11.0 records, cell by cell: (a) `TRUNCATE TABLE t`, (b) `INSERT OVERWRITE t SELECT * FROM t WHERE false`, (c) `DELETE FROM t` with no predicate. Cells: live row count, live data-file count, snapshot count, current snapshot `summary.operation`, every other summary property, whether `VERSION AS OF` the pre-statement snapshot still reads the old rows. The same cells for TRUNCATE on a view and on a missing table, including Spark's error class. | This ledger's oracle table, each cell from an executed probe; no engine edit before the table is filed. | OPEN | Charter question: does Spark stamp TRUNCATE as `overwrite`, `delete`, or something else, and which summary keys differ from empty overwrite? |
| C-002 | **Spark SQL door executes whole-table `TRUNCATE TABLE` Spark-equal.** After TRUNCATE: zero live rows, zero live data files, snapshot count +1, operation stamp and summary properties match C-001's TRUNCATE cells (value and type on the Arrow path). The empty-overwrite SQL rewrite is not the product spelling. | Spark-door pin; red-first against the current C4-L-001 refuse. | OPEN | Today `router.rs` returns `NotImplemented` naming the empty-overwrite substitute. |
| C-003 | **ANSI SQL door executes the same statement Spark-equal.** Same snapshot and row cells as C-002. The Q9/C4-L-001 permanent-absence row in `repark-sql` `matrix.rs` flips to tested. The old "two meanings" refuse is gone; the door commits Iceberg truncate (wipe rows, keep table and history), not drop-and-recreate. | ANSI-door pin; matrix row; red-first against `refusals::truncate`. | OPEN | `m2_closes_the_ansi_door` currently lists `TRUNCATE` among four deliberate absences. |
| C-004 | **Facade `.sql()` is the third door.** `spark.sql("TRUNCATE TABLE …")` matches C-002 on rows, files, snapshot count, and operation stamp. Native `repark.sql()` matches C-003. | Facade pin on `collect`/`to_arrow`, value and type. | OPEN | Facade today surfaces the Spark-door C4-L-001 refuse (`repark-python` session pin). |
| C-005 | **No second spelling silently diverges.** `INSERT OVERWRITE … SELECT … WHERE false` stays a real overwrite statement and is not deleted. TRUNCATE is not a silent rewrite onto that path when C-001 shows a snapshot-metadata split. If C-001 shows the two statements stamp equal metadata, TRUNCATE may share the overwrite commit helper and must still be a first-class statement (parse → `TruncateSpec`). Documented TRUNCATE substitutes that name empty overwrite as the product spelling are retired. | Pin that TRUNCATE and empty overwrite each keep their own statement; a snapshot-metadata pin when C-001 records a split; docs no longer steer TRUNCATE users at empty overwrite. | OPEN | The split is C-001's job; this clause binds the product consequence. |
| C-006 | **Time travel to a pre-truncate snapshot still reads the old rows.** `VERSION AS OF` / snapshot id on both SQL doors and the facade returns the pre-truncate Arrow table (values and types). Current-snapshot reads are empty. | Three-door time-travel pin. | OPEN | Card pin: snapshot count +1 and prior snapshot readable. |
| C-007 | **Error surface matches Spark's class.** TRUNCATE of a view, a nonexistent table, and a metadata-table path (`t.files` / `t$files`) refuse loud. Error class (or the mapped RePark class) matches C-001's Spark cells. A refused TRUNCATE leaves the target untouched. Whole-table form only: `TRUNCATE TABLE … PARTITION (…)` refuses loud and does not full-table wipe. | Error pins per target class; partition-form refuse pin; untouched-table pin. | OPEN | Metadata-path refuse already exists (`test_truncate_metadata_loud`); it must stay after TRUNCATE is live. |
| C-008 | **Documents match the pins.** Registry DML-2 moves from DECLARED-refuse to FIXED (or a dated split if C-001 records one). ANSI matrix absence count drops TRUNCATE. Spark matrix pin retargets from `truncate_table_refuses_loud_naming_gap`. Guide pages that name the refuse are updated. STATUS stays under the 25000 B ceiling. Maps in lockstep. | Registry, both `matrix.rs`, guide pages, STATUS, `check-map-sync`, `check-ledger-grammar`. | OPEN | STATUS has ~538 B of headroom; condense what is added. |

VERDICT: OPEN — 8 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN
with its pin (`pins: dml-c-truncate/C-NNN`).

## 1. Out of scope

- `TRUNCATE TABLE t PARTITION (…)` — partition-scoped truncate. Refuse loud (C-007). DML-B
  owns partition overwrite; this card is whole-table only (`TruncateSpec { table }`).
- Format-v3 lineage (`V3-COW-1`, `_row_id`). The oracle table is a v2 Iceberg table, per
  the unit ask. A v3 TRUNCATE incidental control is recorded if cheap; a lineage split is
  a finding, not a silent lift of the v3 guard.
- `DROP TABLE` / `CREATE OR REPLACE TABLE` as truncate substitutes.
- AWS Glue / S3 Tables live legs.
- DML-B (`INSERT OVERWRITE … PARTITION`) and DML-A (`WHEN NOT MATCHED BY SOURCE`).

## 2. Sequence

1. This charter (docs-only). No engine edit.
2. C-001 oracle table in this ledger, from live PySpark 4.1.2 + Iceberg 1.11.0.
3. Red-first pins for C-002..C-007 against the current refuse.
4. Shared Iceberg truncate commit (empty `overwrite_files` or the helper C-001 requires).
5. Wire both doors and the facade; flip the refuse tests; C-008 docs.
6. `make verify`, `make check-map-sync check-ledger-grammar`,
   `python3 scripts/ledger_lifecycle.py check --base <branch-base-sha>`, full `make py-test`.

## 3. Owner actions

- Pre-authorized 2026-08-30 (v0.6 plan). No mid-unit ask unless C-001 shows a snapshot
  stamp that the owned fork cannot emit without a fork change — that is a HALT, not a
  workaround.
- No AWS, no IAM, no push, no PR from this Actor session.

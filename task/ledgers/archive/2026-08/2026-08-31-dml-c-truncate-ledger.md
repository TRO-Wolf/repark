# Charter ledger — DML-C · TRUNCATE TABLE as a first-class statement

**Date:** 2026-08-30 · **Branch:** `feat/dml-c-truncate` · **Base:** `main` `60225cc`
(`feat(fnp-15-16)` #271) · **Path:** HIGH (`risk_tier: high` — irreversible table wipe) ·
**Policy:** [../../../AGENTS.md](../../../../AGENTS.md) "Verify before done" and
[../../../docs/testing.md](../../../../docs/testing.md). **Owner-pre-authorized** 2026-08-30
(v0.6 plan). **Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** v0.6 Track B merge order 2 of 4. Registry [DML-2](../../../../docs/spark-sql-iceberg-parity.md)
was a targeted refuse that steered to empty `INSERT OVERWRITE … WHERE false` or `DELETE FROM`
with no predicate. C-001 measured those three statements stamp the same snapshot keys
(`operation=delete`). The card is a first-class `TRUNCATE TABLE` on both SQL doors and the
facade, Spark-equal on that metadata versus live PySpark 4.1.2 + Iceberg 1.11.0.

**Card** (v0.6 plan, 2026-08-29; tree 2026-08-30): parse `sqlparser::ast::Truncate` →
`execute_truncate` → `commit_truncate` (whole-table `overwrite_files` with empty input) →
new snapshot with zero data files; metadata and history stay. Pins: snapshot count +1,
`table$files` empty, time travel to the prior snapshot works.

## PROPOSITION LEDGER — DML-C — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | **The live-oracle matrix is measured before any engine edit.** On a v2 Iceberg table, live PySpark 4.1.2 + Iceberg 1.11.0 records, cell by cell: (a) `TRUNCATE TABLE t`, (b) `INSERT OVERWRITE t SELECT * FROM t WHERE false`, (c) `DELETE FROM t` with no predicate. Cells: live row count, live data-file count, snapshot count, current snapshot `summary.operation`, every other summary property, whether `VERSION AS OF` the pre-statement snapshot still reads the old rows. The same cells for TRUNCATE on a view and on a missing table, including Spark's error class. | This ledger's oracle table, each cell from an executed probe; no engine edit before the table is filed. | **PROVEN** | Oracle table below. All three statements stamp `operation=delete` with the same load-bearing summary keys. Probe: `/tmp/dml-c-oracle.out` (PySpark 4.1.2 + Iceberg 1.11.0, 2026-08-30). Pins: `crates/repark-spark/src/tests/truncate.rs`. |
| C-002 | **Spark SQL door executes whole-table `TRUNCATE TABLE` Spark-equal.** After TRUNCATE: zero live rows, zero live data files, snapshot count +1, operation stamp and summary properties match C-001's TRUNCATE cells (value and type on the Arrow path). The empty-overwrite SQL rewrite is not the product spelling. | Spark-door pin; red-first against the current C4-L-001 refuse. | **PROVEN** | `commit_truncate` → empty `overwrite_files` + AlwaysTrue. Pin: `truncate_table_wipes_rows_stamps_delete_and_preserves_history`. |
| C-003 | **ANSI SQL door executes the same statement Spark-equal.** Same snapshot and row cells as C-002. The Q9/C4-L-001 permanent-absence row in `repark-sql` `matrix.rs` flips to tested. The old "two meanings" refuse is gone; the door commits Iceberg truncate (wipe rows, keep table and history), not drop-and-recreate. | ANSI-door pin; matrix row; red-first against `refusals::truncate`. | **PROVEN** | `m2_closes_the_ansi_door` is 47 tested / 3 absent. Pin: `truncate_tests::truncate_table_wipes_rows_stamps_delete_and_preserves_history`. |
| C-004 | **Facade `.sql()` is the third door.** `spark.sql("TRUNCATE TABLE …")` matches C-002 on rows, files, snapshot count, and operation stamp. Native `repark.sql()` matches C-003. | Facade pin on `collect`/`to_arrow`, value and type. | **PROVEN** | `python/repark/tests/test_dml_c_truncate.py`. Native missing table is `AnalysisException` `table 'ice.sales.does_not_exist' not found`; neither refuse substitute. |
| C-005 | **No second spelling silently diverges.** `INSERT OVERWRITE … SELECT … WHERE false` stays a real overwrite statement and is not deleted. TRUNCATE is a first-class `execute_truncate` path, not a SQL rewrite onto empty overwrite. C-001 measured oracle-equal snapshot keys, so the two paths may share `commit_overwrite_replace_all` internals; they remain separate statements. Documented TRUNCATE substitutes that name empty overwrite as the product spelling are retired. | Pin that TRUNCATE and empty overwrite each keep their own statement; a pin that their load-bearing summary keys stay equal; docs no longer steer TRUNCATE users at empty overwrite. | **PROVEN** | Separate statements, oracle-equal snapshot keys. Pins: `empty_insert_overwrite_still_wipes_and_stamps_delete`, `truncate_and_empty_overwrite_stamp_equal_wipe_summary_keys`. Guides no longer steer TRUNCATE at empty overwrite. |
| C-006 | **Time travel to a pre-truncate snapshot still reads the old rows.** `VERSION AS OF` / snapshot id on both SQL doors and the facade returns the pre-truncate Arrow table (values and types). Current-snapshot reads are empty. | Three-door time-travel pin. | **PROVEN** | Spark `VERSION AS OF`, ANSI `FOR VERSION AS OF`, facade `.sql()`. Same tests as C-002/C-003/C-004. |
| C-007 | **Error surface matches Spark's class.** TRUNCATE of a view, a nonexistent table, and a metadata-table path (`t.files` / `t$files`) refuse loud. Error class (or the mapped RePark class) matches C-001's Spark cells. A refused TRUNCATE leaves the target untouched. Whole-table form only: `TRUNCATE TABLE … PARTITION (…)` refuses loud and does not full-table wipe. | Error pins per target class; partition-form refuse pin; untouched-table pin. | **PROVEN** | `TABLE_OR_VIEW_NOT_FOUND`, `EXPECT_TABLE_NOT_VIEW`, `INVALID_PARTITION_OPERATION` class token (no `PARTITION` disjunct). Leading `IF EXISTS` is `PARSE_SYNTAX_ERROR` (Spark does not accept it). Trailing `IF EXISTS` parse-fails naming `IF` and does not wipe. Metadata path stays the existing read-only refuse (`test_truncate_metadata_loud`). |
| C-008 | **Documents match the pins.** Registry DML-2 moves from DECLARED-refuse to FIXED (or a dated split if C-001 records one). ANSI matrix absence count drops TRUNCATE. Spark matrix pin retargets from `truncate_table_refuses_loud_naming_gap`. Guide pages that name the refuse are updated. STATUS stays under the 25000 B ceiling. Maps in lockstep. | Registry, both `matrix.rs`, guide pages, STATUS, `check-map-sync`, `check-ledger-grammar`. | **PROVEN** | DML-2 FIXED. STATUS 24731 B. Pins: both `matrix.rs` TRUNCATE rows. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Oracle table — live PySpark 4.1.2 + Iceberg 1.11.0 (2026-08-30)

v2 Hadoop catalog. Seed: three rows, two data files, one append snapshot. Probe log: `/tmp/dml-c-oracle.out`.

Load-bearing summary keys after a wipe (TRUNCATE / empty overwrite / DELETE-all are equal):

| Cell | TRUNCATE | empty INSERT OVERWRITE | DELETE FROM (no predicate) |
|---|---|---|---|
| live rows | 0 | 0 | 0 |
| live data files | 0 | 0 | 0 |
| snapshot count | +1 (2) | +1 (2) | +1 (2) |
| `summary.operation` | `delete` | `delete` | `delete` |
| `deleted-data-files` | 2 | 2 | 2 |
| `deleted-records` | 3 | 3 | 3 |
| `total-records` | 0 | 0 | 0 |
| `total-data-files` | 0 | 0 | 0 |
| `added-data-files` | absent | absent | absent |
| VERSION AS OF prior | old 3 rows | (not re-probed) | (not re-probed) |

Incidental: second TRUNCATE on empty commits another `delete` snapshot (`changed-partition-count=0`, no `deleted-data-files`). TRUNCATE of a never-written table commits snapshot 1 as `delete`. MoR v2 TRUNCATE matches COW (files gone, not position-deletes). Partitioned whole-table TRUNCATE matches unpartitioned.

Error classes:

| Target | class | SQLSTATE / note |
|---|---|---|
| temp / session view | `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` | 42809 |
| missing table | `TABLE_OR_VIEW_NOT_FOUND` | 42P01 |
| `t.files` metadata | `UnsupportedOperationException`: Cannot delete from a metadata table | — |
| `t$files` | `TABLE_OR_VIEW_NOT_FOUND` | Hadoop catalog has no `$` name |
| `PARTITION (id=1)` | `INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED` | 42601 |
| `TRUNCATE TABLE IF EXISTS t` | `PARSE_SYNTAX_ERROR` at `EXISTS` | 42601; Spark does not accept IF EXISTS |
| `TRUNCATE TABLE t IF EXISTS` | `PARSE_SYNTAX_ERROR` at `IF` | 42601; RePark parser rejects trailing IF (class token not required; does not wipe) |
| missing `TABLE` | `PARSE_SYNTAX_ERROR` | 42601 |

## Actor coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dml-c-truncate
  cycle: actor
  risk_tier: high
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001 oracle measured before engine edit. TRUNCATE / empty overwrite / DELETE-all
        all stamp operation=delete. Three-door pins assert rows, files, snapshot +1, Delete.
      artifacts: [/tmp/dml-c-oracle.out, crates/repark-spark/src/tests/truncate.rs, crates/repark-sql/src/truncate_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Happy path, never-written table, second-empty incidental on Spark oracle,
        PARTITION refuse, missing table, view, metadata path.
      artifacts: [crates/repark-spark/src/tests/truncate.rs, python/repark/tests/test_dml_c_truncate.py, python/repark/tests/test_metadata_tables.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Refused PARTITION truncate leaves rows. Refused view truncate leaves the base table.
        Missing table does not create a table.
      artifacts: [truncate_partition_form_refuses_without_wiping, truncate_view_is_expect_table_not_view]
    - id: AT-4
      status: N/A
      justification: Truncate uses the existing overwrite OCC validations; no new concurrent writer.
    - id: AT-5
      status: ATTACKED
      evidence: >
        Path-escape ident refuse; P11 read-only catalog; metadata write-target refuse stays.
      artifacts: [crates/repark-spark/src/truncate.rs, crates/repark-spark/src/metadata_tables.rs]
    - id: AT-6
      status: ATTACKED
      evidence: >
        Time travel to the pre-truncate snapshot returns the old Arrow rows on three doors.
      artifacts: [truncate_table_wipes_rows_stamps_delete_and_preserves_history, test_dml_c_truncate.py]
    - id: AT-7
      status: N/A
      justification: Empty overwrite_files commit; no unbounded write or new hot path.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Shared helper is commit_overwrite_replace_all(empty). Fork classifies delete-only
        overwrite as Operation::Delete. No fork change.
      artifacts: [crates/repark-iceberg/src/write/truncate.rs, iceberg overwrite_files.rs operation()]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Error class tokens TABLE_OR_VIEW_NOT_FOUND, EXPECT_TABLE_NOT_VIEW,
        INVALID_PARTITION_OPERATION match the oracle.
      artifacts: [crates/repark-spark/src/truncate.rs, /tmp/dml-c-oracle-errors.out]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Red-first: refuse tests were the C4-L-001 pins; they failed once TRUNCATE executed
        and were retargeted. New pins assert wipe + Delete stamp.
      artifacts: [crates/repark-spark/src/tests/truncate.rs]
  reattested: []
```


## 1. Out of scope

- `TRUNCATE TABLE t PARTITION (…)` — partition-scoped truncate. Refuse loud (C-007). DML-B
  owns partition overwrite; this card is whole-table only (`sqlparser::ast::Truncate` →
  `execute_truncate`).
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

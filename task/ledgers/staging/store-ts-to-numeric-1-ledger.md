# Unit ledger — WO STORE-TS-TO-NUMERIC-1 · TIMESTAMP, DATE, STRING and `-NULL` sources follow Spark's store assignment on every write door

**Date:** 2026-09-28 · **Branch:** `fix/store-ts-to-numeric-1` · **Base:** `adc26586` (`origin/main`)
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Verifier findings VG-5 (S1) and VG-7 (S2) on #882 and release differential
PE-9: `INSERT INTO sc.ns.b VALUES (0, TIMESTAMP'1970-01-01 00:00:01')` into a BIGINT
column stored `1` (epoch seconds), DOUBLE stored `1.0` and DECIMAL(10,2) `1.00`; a STRING
into a DATE column through VALUES stored; `-NULL` into TIMESTAMP stored NULL. Spark 4.1.2
refuses all of them with `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` under the default
`spark.sql.storeAssignmentPolicy=ANSI`. LTZ-STORE-INT-1 closed numeric into TIMESTAMP on
the VALUES door; this unit closes the other direction and the doors the Step 0 matrix
found open.

**Step 0 (measured before any edit, 2026-09-28, zone UTC).** One matrix, 3,645 cells:
15 sources (TIMESTAMP, TIMESTAMP_NTZ and DATE literals, `CAST(1 AS TIMESTAMP)`, a
TIMESTAMP table column, `'1'`, `'2024-01-01'`, `'abc'`, `-NULL`, `NULL`, `1`, `1.5`,
`1.5D`, `3000000000L`, `true`) × 10 targets (TINYINT, SMALLINT, INT, BIGINT, FLOAT,
DOUBLE, DECIMAL(10,2), DATE, BOOLEAN, TIMESTAMP) × 10 doors (VALUES, OVERWRITE VALUES,
UPDATE, INSERT … SELECT, INSERT OVERWRITE … SELECT, BY NAME, MERGE UPDATE, MERGE INSERT,
`writeTo().append()`, `write.insertInto()`) with the SELECT-shaped doors bare, through a
derived table and through a temp view, plus CTAS as a control. Spark 4.1.2 + Iceberg
1.11.0 through `jvm-lock.sh`; RePark on `adc26586`. Classes on base:

| Class | Cells |
|---|---|
| RePark stores, Spark refuses (must change) | 123 |
| Both refuse, same class and first line | 84 |
| Both refuse, same class, message differs | 6 |
| Both refuse, class differs | 2,146 |
| Both store, same value | 1,230 |
| RePark refuses, Spark stores (pre-existing, not touched) | 56 |

The 123 must-change cells: VALUES (single-row `INSERT INTO`) TIMESTAMP and
`CAST(1 AS TIMESTAMP)` into every numeric target (14), STRING into numeric and BOOLEAN
(8), STRING into DATE (1), BOOLEAN into numeric (6), numeric into BOOLEAN (3); STRING
into FLOAT/DOUBLE through INSERT … SELECT and both DataFrame doors, every wrap (18);
STRING into DECIMAL through UPDATE (1); `-NULL` into DATE, BOOLEAN and TIMESTAMP on every
door and wrap (72). Spark types `-NULL` as DOUBLE (`typeof(-NULL)` is `double`); RePark
types it `void`.

**Cause.** Four gaps around one shared matrix (`ansi_store_assignable`), which already
refuses every must-change pair: (1) the VALUES gate judged only TIMESTAMP (LTZ) targets;
(2) the Spark door's `spark_float_stringify` rule rewrites the synthesized STRING →
FLOAT/DOUBLE conform cast into `__repark_parse_java_double__` (or folds an aliased
literal to a float literal) before `InsertStoreAssignment` runs, so the analyzer gate
never sees the cast; (3) `spark_update_type_name` has no DECIMAL name, so the UPDATE gate
answered "no message" for a DECIMAL target; (4) a `-NULL` column plans as Arrow `Null`,
which the matrix stores anywhere.

**Fix.** Refusals only; nothing new is admitted.
- `void_type/ltz_values_store.rs`: the VALUES gate judges numeric, DATE and BOOLEAN
  targets as well as LTZ ones, through the shared matrix; the LTZ-only numeric fallback
  stays LTZ-only.
- `repark-iceberg` `update_cast.rs`: `incompatible_store_message` is the shared message
  with DECIMAL names (`DECIMAL(10,2)`); `incompatible_update_message` keeps its answers,
  so the native door and the VOID, NTZ and nested-MERGE gates are unchanged.
- `repark-iceberg` `negated_null_store.rs`: `refuse_negated_null_writes` walks a planned
  source's column lineage (projections, aliases, derived tables, joins, VALUES, UNION,
  DataFusion views, and Spark-door temp views through a session resolver) and judges a
  `Null` column that is a sign applied to NULL as DOUBLE. Callers: both MERGE gates, the
  Spark-door UPDATE gate, and the Spark-door INSERT gate below.
- `void_type/insert_source_types.rs`: one Spark-door INSERT gate, called from
  `router/insert_positional.rs` `prepare_positional_insert` (positional append, owned
  append, OVERWRITE, VALUES) and from `execute_insert_by_name`, plans the source once and
  refuses a negated NULL into a column DOUBLE cannot store and a STRING into FLOAT/DOUBLE,
  with Spark's text.
- `view_ddl/temp_view.rs`: `ReplanningTempView` keeps its creation-time plan;
  `definition_plan` answers it to the lineage walk (registered in `extension.rs`).

**After the fix (same matrix, same Spark answers).** All 123 must-change cells refuse with
Spark's class, first line and SQLSTATE (`KD000`), and write nothing. 132 cells that both
engines already refused now also carry Spark's exact text. No cell that stores on base
changed value or started refusing; the 56 RePark-only refusals and the 6 same-class
message differences are unchanged.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | On the VALUES door, TIMESTAMP (literal or `CAST`) into a numeric column, STRING into a numeric, BOOLEAN or DATE column, BOOLEAN into a numeric column and a numeric into a BOOLEAN column refuse with Spark's recorded class, SQLSTATE and message body, and write nothing. | `test_store_ts_to_numeric_1.py` replays the recorded `values/*` refusal cells and their read-backs; the classifier unit test names the judged targets. | PROVEN | 8 VALUES refusal cells equal `store_ts_to_numeric_1_spark_oracle.json`; all 8 miss on `adc26586`. |
| C-002 | `-NULL` and `-(NULL)` into DATE, BOOLEAN, TIMESTAMP and TIMESTAMP_NTZ columns refuse with Spark's text naming `"DOUBLE"` on VALUES, OVERWRITE VALUES, UPDATE, INSERT … SELECT, INSERT OVERWRITE, BY NAME, MERGE UPDATE, MERGE INSERT and both DataFrame doors, bare, through a derived table and through a temp view; they still store NULL into numeric and STRING columns. | The replay test's `-NULL` refusal cells (every door, all three wraps, NTZ on VALUES, UPDATE and MERGE INSERT) and store cells (INT, STRING, DECIMAL through a view, DOUBLE through MERGE INSERT, BIGINT through UPDATE); the lineage unit tests. | PROVEN | 17 refusal cells and 5 store cells equal the oracle; the 17 refusals miss on `adc26586`; `cargo test -p repark-iceberg --lib negated_null`: 4 passed. |
| C-003 | STRING into FLOAT or DOUBLE refuses with Spark's text on INSERT … SELECT and both DataFrame doors, bare, through a derived table and through a temp view. | The replay test's `select`, `df_append` and `df_insertinto` STRING → FLOAT/DOUBLE cells. | PROVEN | 4 refusal cells equal the oracle; all 4 stored on `adc26586`. |
| C-004 | STRING into DECIMAL(10,2) refuses on the UPDATE door with Spark's text naming `"DECIMAL(10,2)"`. | The replay test's `update/str_num/dec` cell. | PROVEN | Equals the oracle; stored `1.00` on `adc26586`. |
| C-005 | Every must-not-change cell still stores with Spark's value: DATE → TIMESTAMP and TIMESTAMP → DATE (VALUES, SELECT, UPDATE, MERGE, BY NAME, OVERWRITE, `insertInto`), INT → BIGINT, INT → DOUBLE, DECIMAL → DOUBLE, in-range BIGINT → INT, an explicit `CAST(TIMESTAMP AS BIGINT)`, NULL into DATE and BOOLEAN, a TIMESTAMP column through a derived table into DATE, and the CTAS controls; LTZ-STORE-INT-1's INT → TIMESTAMP refusal stays. | The replay test's 22 non-`-NULL` store cells and the INT → TIMESTAMP refusal cell; `test_ltz_store_int_1.py`; the Step 0 matrix re-run (no stored value changed). | PROVEN | Store cells equal the oracle read-backs on head and on `adc26586`; LTZ pins green. |

## Mutation record (2026-09-28)

Each mutation rebuilt the native module (`make develop`) on `55ba71bd`, ran the replay test,
`test_ltz_store_int_1.py` and `test_insert_store_assign.py`, and was reverted; `git status`
was clean before the next.

| # | Mutation | Replay cells red | Other files |
|---|---|---|---|
| M1 | The VALUES gate judges LTZ targets only (`is_judged_target` returns `is_microsecond_ltz`) | 8: every C-001 VALUES refusal | `test_a_literal_values_row_refuses_like_spark` red; LTZ pins green |
| M2 | The lineage walk never reports a negated NULL (`Negative` answers `Other`) | 17: every `-NULL` refusal, all doors and wraps | LTZ pins green |
| M3 | The INSERT gate skips STRING → FLOAT/DOUBLE | 4: every C-003 cell | LTZ pins green |
| M4 | `incompatible_store_message` stops naming DECIMAL | 2: `values/cast_ts/dec`, `update/str_num/dec` | LTZ pins green |
| MALL | M1 + M2 + M3 + M4 (the new direction off) | all 30 must-change cells; every store cell stays green | `test_a_literal_values_row_refuses_like_spark` red; LTZ pins green |

**Neighbours (2026-09-28).** 40 statements drawn from the diff-probe corpus
(`ins_probe.py`, `upd_probe.py`, `ovf_probe.py`, `intdiv_probe.py`: fractional and
overflowing quotients into BIGINT/INT through INSERT … SELECT, VALUES, OVERWRITE, UPDATE,
MERGE and a CTE) answer the same on `adc26586` and on this unit; the one textual
difference is the row order of an unordered `SELECT * FROM sc.ns.c` (same multiset).

**Existing pins changed (2).** `test_insert_store_assign.py`'s VALUES-residual pin
(`VALUES (true)` into INT stored `1`) becomes `test_a_literal_values_row_refuses_like_spark`;
`tests/update_cast.rs`'s `insert_string_into_bigint_keeps_the_insert_path` (a cast-kernel
error, explicitly not the store text) becomes `insert_string_into_bigint_refuses_like_spark`.
Both old pins recorded answers Spark does not give.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: store-ts-to-numeric-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against recorded Spark answers — 30 must-change refusal cells across every door, source class and wrap, 27 store cells and the LTZ-STORE-INT-1 refusal control, and the 3,645-cell Step 0 matrix re-run.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py, python/repark/tests/store_ts_to_numeric_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Bare, derived-table, temp-view, column-list, BY NAME, multi-row VALUES, `-(NULL)`, UNION of `-NULL` and NULL, joins in the MERGE probes, and numeric, STRING and DECIMAL targets for `-NULL` are exercised; partitioned inserts, unmapped columns and unplannable sources fall through to base behaviour.
      artifacts: [crates/repark-spark/src/void_type/insert_source_types.rs, crates/repark-iceberg/src/write/negated_null_store.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is a planning error raised before any file is staged; the replay test reads back the target after each refusal and matches Spark's unchanged rows, including the seeded row UPDATE and MERGE UPDATE would have changed and the OVERWRITE target.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state; the view resolver is a plain function stored in the session config and the cached view plan is immutable.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the gates read table metadata and plan the statement's own source.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; the silent epoch-seconds, STRING and NULL writes are closed and the replay test asserts refused statements leave prior rows intact.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: The INSERT gate loads the target schema and plans the source once per INSERT, and only when the table has a DATE, BOOLEAN, TIMESTAMP, BINARY or floating-point column; the VALUES gate probes non-literal cells for numeric, DATE and BOOLEAN targets. Measured cost is recorded in residue R-STN-6.
      artifacts: [crates/repark-spark/src/void_type/insert_source_types.rs, crates/repark-spark/src/void_type/ltz_values_store.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge or dependency; `incompatible_update_message` keeps its answers so the native door, VOID, NTZ and nested-MERGE gates are unchanged; the temp-view plan is exposed through a session-config resolver, not through `get_logical_plan`, so DataFusion never inlines a replanning view.
      artifacts: [crates/repark-iceberg/src/write/update_cast.rs, crates/repark-spark/src/view_ddl/temp_view.rs, crates/repark-spark/src/extension.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every new refusal carries Spark's class, condition and SQLSTATE through the facade's stamped-message parse; the replay test compares all three with the recorded oracle.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Each mechanism has a mutation that reds the replay test while the LTZ-STORE-INT-1 pins stay green; the lineage walk has positive and negative unit tests; the judged-target classifier has a unit test.
      artifacts: [crates/repark-iceberg/src/write/negated_null_store.rs, crates/repark-spark/src/void_type/ltz_values_store.rs, python/repark/tests/test_store_ts_to_numeric_1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-STN-1 | Dated 2026-09-28 (pre-existing, out of scope): 2,014 matrix cells refuse on both engines with a different class — the SELECT-shaped doors (INSERT … SELECT, OVERWRITE, BY NAME, MERGE, DataFrame) still speak the WI-1 text `… cannot store-assign column …` for pairs their existing gates already refused. Moving those gates to Spark's text changes many existing pins and is its own unit. |
| R-STN-2 | Dated 2026-09-28 (pre-existing): 56 cells where RePark refuses and Spark stores — TIMESTAMP_NTZ literals into DATE and TIMESTAMP through the SELECT-shaped doors (`'__repark_timestamp_ntz__' expects an Int64 wall`), and CTAS of `-NULL` (`Invalid schema for v2`). |
| R-STN-3 | Dated 2026-09-28: `- -NULL` still stores into DATE through the SQL doors. The INSERT gate plans the rendered source, and `- -NULL` renders as `--NULL`, a comment (the VG-1/VG-3 stacked-sign rendering, owned by LTZ-STACKED-SIGN-1 #882). |
| R-STN-4 | Dated 2026-09-28: `+NULL` fails in RePark's planner (`Unary operator '+' only supports numeric…`) where Spark types it DOUBLE and refuses `CANNOT_SAFELY_CAST`; `abs(NULL)` names `"INT"` where Spark names `"DOUBLE"`. Both engines refuse; the NULL typing of functions is not store assignment. |
| R-STN-5 | Dated 2026-09-28 (same class as R-LTZ-2): a multi-row VALUES whose rows have incompatible types (`(15, '1'), (16, 2)` into INT) refuses `CANNOT_SAFELY_CAST`/KD000 per row where Spark answers `INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE`/42000. On base these rows stored. A scalar subquery in VALUES now refuses `CANNOT_SAFELY_CAST` where Spark answers `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES`; base refused with a physical-plan error. |
| R-STN-6 | Dated 2026-09-28 (extends R-LTZ-4): every Iceberg INSERT into a table with a DATE, BOOLEAN, TIMESTAMP, BINARY or floating-point column plans its source once more, and every VALUES insert into a table with a numeric, DATE or BOOLEAN column loads the table schema and probes each non-literal cell. Measured on a debug build, median of 5: 2,000 `DATE'…'` rows into DATE 3.78 s → 4.66 s (+23%); 2,000 integer rows into BIGINT 1.12 s → 1.13 s; 2,000 strings into STRING 2.33 s → 2.32 s; `INSERT … SELECT` of 100,000 rows into DATE 0.116 s → 0.103 s; one DATE row 0.061 s both. |
| R-STN-7 | Dated 2026-09-28: the three UPDATE cells `1` into DATE, BOOLEAN and TIMESTAMP refuse naming `"BIGINT"` where Spark names `"INT"` (the UPDATE probe types an integer literal BIGINT). Unchanged from base. |

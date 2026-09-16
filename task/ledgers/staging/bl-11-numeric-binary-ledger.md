# Unit ledger — BL-11 numeric→BINARY under runtime ANSI off

**Unit:** `bl-11-numeric-binary` · **Date:** 2026-09-16 · **Branch:** `feat/bl-11-numeric-binary` · **Base:** `origin/main` `6f74897f`
**Model:** muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.
**Oracle:** `/tmp/oc-worker/rc-oracle/fixtures-batch17-bl11-binary.json` (live PySpark 4.1.2, 30 cells).

## 1. Scope and fence

Run 17a owns `functions*.py` and the Python function registry; run 17b owns `dataframe/**`,
`column.py`, `catalog.py`. This unit edits none of those: both doors (`spark.sql` and the Python
`Column`/`DataFrame` API) compose SQL that executes through `repark-spark` `execute_passthrough`,
so the Rust change covers both with no facade half and no P2 hand-off is owed.

One adjudicated out-of-fence touch: C-005 orders `test_sqp_1_string_literals.py` updated in place,
and that file is sha256-frozen by `python/repark-parity/tests/test_pr_245_revalidation_record.py`,
so the same commit re-baselines that one hash. Mechanical lockstep, no behaviour in the record file.

## 2. Oracle rule

ANSI off encodes only the integrals as big-endian bytes of natural width (TINYINT 1, SMALLINT 2,
INT 4, BIGINT 8, two's complement; NULL stays NULL). FLOAT, DOUBLE, DECIMAL, BOOLEAN, DATE,
TIMESTAMP, INTERVAL refuse in both modes with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`.
ANSI on refuses the integrals with `DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION` plus the sentence
naming `spark.sql.ansi.enabled` as `'false'`. `CAST('ab' AS BINARY)` works in both modes.
`CAST(CAST(NULL AS INT) AS BINARY)` follows the INT rule per mode.

## 3. Design

- Encoder kernel: new `crates/repark-functions/src/int_to_binary.rs`, one `ScalarUDF`
  `__repark_int_to_binary__` over Int8/16/32/64 (Scalar and Array, NULL propagates), output
  `Binary`, nullability taken from the input field. Any other input type is a loud plan error.
- Rewrite home: `SparkExprSemantics` (`crates/repark-functions/src/analyzer.rs`) already threads
  `ansi_enabled` from #639's carrier and runs inside `analyze_eagerly` at frame-build time, so the
  `Cast(int → Binary)` → UDF rewrite gated on ANSI off binds at analysis on both doors (C-004).
  `TryCast` is never rewritten. Non-`Binary` targets are untouched.
- Refusal home: `refuse_illegal_binary_cast` (`crates/repark-spark/src/spark_ast.rs`) reads ANSI from
  `state.config_options()` at build. ANSI off skips integral sources; ANSI on refuses them with
  `CAST_WITH_CONF_SUGGESTION`; never-castable sources refuse in both modes with
  `CAST_WITHOUT_SUGGESTION`. Messages gain Spark's `Cannot resolve "CAST(<inner> AS BINARY)"`
  prefix, the conf remedy sentence, and keep `SQLSTATE: 42K09`.
- `TRY_CAST(int AS BINARY)` under ANSI off is unmeasured by the oracle; it keeps today's refusal
  (WITHOUT_SUGGESTION) rather than guessing an encode.
- The resolve slot echoes the plan expression (repo precedent in `cast_legality.rs`), not the source
  SQL text; plan altitude has no line/pos, so none is printed (precedent: every other Spark-shaped
  error in the tree). Pins assert class, cast sentence, remedy, and SQLSTATE with `in`, never exact
  equality.
- Interval DAY quotes as `INTERVAL DAY` per the oracle cell; the other Arrow interval units take
  Spark's closest spelling, flagged unpinned (only DAY is measured).

## 4. Risks

- Optimizer constant-folding of the UDF on literals must equal the Scalar path; pinned by the
  literal cells on both doors.
- The encoder must never see non-integrals (the refusal admits only Int8/16/32/64); the kernel
  still fails loud on anything else.
- Native `repark.sql()` never runs `SparkExprSemantics` (Spark-door-only rule set), so the rewrite
  cannot leak; the sweep `-k "binary or ansi or cast"` guards the shared refusal helper.
- `F.lit(1)` type and `F.lit(None).cast("int")` shape on the Python door are verified empirically
  in the red run, not assumed.

## 5. Clauses

| Clause | Statement | Pins | Verdict |
|---|---|---|---|
| C-001 | ANSI-off integral encode, value + Arrow type + nullability, both doors | `test_bl_11_numeric_binary.py` encode cases + Rust `int_to_binary`/`cast_binary` tests | OPEN |
| C-002 | ANSI-off never-castable refusals, WITHOUT_SUGGESTION + message | refusal params, both doors (representative) | OPEN |
| C-003 | ANSI-on integrals refuse WITH_CONF_SUGGESTION + conf message; rest keep WITHOUT | refusal params, both doors (representative) | OPEN |
| C-004 | Mode binds at analysis: stale encode frame survives SET on; refusal binds at build; fresh frame encodes | `test_c004_*` | OPEN |
| C-005 | `CAST('ab' AS BINARY)` both modes both doors; `test_numeric_to_binary_refuses` flips in place to ANSI-on | `test_c005_*` + flipped pin | OPEN |
| C-006 | Registry BL-11 BACKLOG → FIXED with pins; set-ansi-runtime-1 C-006 noted discharged | registry row, ledger note | OPEN |

## 6. Red first

`test_bl_11_numeric_binary.py` (new, 30 oracle cells on the SQL door + Python-door representatives)
against base `6f74897f`, release native, 2026-09-16: **27 failed, 14 passed.**

```
FAILED test_c001_sql_door_integrals_encode_big_endian[B11-tinyint]
FAILED test_c001_sql_door_integrals_encode_big_endian[B11-smallint]
FAILED test_c001_sql_door_integrals_encode_big_endian[B11-int]
FAILED test_c001_sql_door_integrals_encode_big_endian[B11-int-neg]
FAILED test_c001_sql_door_integrals_encode_big_endian[B11-int-big]
FAILED test_c001_sql_door_integrals_encode_big_endian[B11-bigint]
FAILED test_c001_sql_door_null_int_stays_null
FAILED test_c001_python_door_int_encodes
FAILED test_c001_python_door_small_types_encode
FAILED test_c001_python_door_bigint_and_negative
FAILED test_c001_python_door_column_values_and_null
FAILED test_c002_sql_door_never_castable_refuses_ansi_off[B11-ts]
FAILED test_c002_sql_door_never_castable_refuses_ansi_off[B11-interval]
FAILED test_c002_python_door_double_refuses_ansi_off
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-tinyint]
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-smallint]
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-int]
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-int-neg]
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-int-big]
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-bigint]
FAILED test_c003_sql_door_integrals_refuse_ansi_on[B11-null-int]
FAILED test_c003_python_door_int_refuses_ansi_on
FAILED test_c003_sql_door_never_castable_refuses_ansi_on[B11-ts]
FAILED test_c003_sql_door_never_castable_refuses_ansi_on[B11-interval]
FAILED test_c004_stale_encode_frame_survives_set_to_on
FAILED test_c004_python_door_stale_frame_survives_set_to_on
FAILED test_c004_refusal_binds_at_build
27 failed, 14 passed
```

Head failure (every C-001 cell): `AnalysisException: Error during planning:
[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION] due to data type mismatch: cannot cast
"TINYINT" to "BINARY" with ANSI mode on. SQLSTATE: 42K09` — the ANSI-off encode path does
not exist, and the message lacks the resolve prefix and the conf remedy.

## 7. Log

- 2026-09-16: ledger opened; pins written; red run pasted above.
- 2026-09-16, probe findings that shaped the design: (a) the red ts/interval cells show the
  door plans `TIMESTAMP'...'` tz-naive and `INTERVAL '1' DAY` as `Interval(MonthDayNano)` while
  the oracle quotes `TIMESTAMP` / `INTERVAL DAY` — the refusal names both per the oracle.
  (b) The Python `select` path builds native `Expr::Cast` (`_plan().select(natives)`) and never
  reaches the door pre-refusal: under ANSI off `F.lit(1).cast("binary")` already answers
  little-endian `01 00 00 00` (DataFusion's native cast leaks), and `F.lit(1.5).cast("binary")`
  dies in the optimizer with `Unsupported CAST from Float64 to Binary`. The verdict therefore
  lives in the session analyzer rule, which both doors reach. (c) `analyzer.rs` is frozen at
  exactly 1150 lines by `scripts/check_rust_file_size.py` (growth needs owner approval), so the
  rewrite is a standalone `IntToBinaryCast` rule slotted after `SparkExprSemantics`, and the
  door pre-refusal is deleted in favour of the single analyzer verdict (SQL door still fails at
  build through `analyze_eagerly`). (d) Plain DataFusion cannot plan `BINARY`
  (`Unsupported SQL type BINARY` — the reason for the door's `BYTEA` rewrite), so the
  `int_to_binary.rs` module tests spell `BYTEA`. (e) The `audit-repark-parity` skill was not
  loaded: it measures against live PySpark and this lane forbids starting a JVM; the recorded
  batch-17 fixture is the oracle and the pins assert its cells in the same change.
- 2026-09-16: Rust lands — new `crates/repark-functions/src/int_to_binary.rs`
  (`IntToBinaryCast` rule + `__repark_int_to_binary__` UDF, 11 module tests), one-line rule
  registration after `SparkExprSemantics`, door pre-refusal deleted from `spark_ast.rs`
  (single analyzer verdict; SQL door still fails at build via `analyze_eagerly`), new door
  battery `cast_binary_ansi.rs` (5 tests; frozen `cast_binary.rs` untouched). Rust:
  `repark-functions --lib int_to_binary` 11 passed, `repark-spark --lib cast_binary`
  10 passed (5 frozen + 5 new).

# Unit ledger — DOOR-CONVERGE-1 · facade and SQL door answer Spark with one kernel — step 1

**Date:** 2026-09-15 · **Branch:** `feat/door-kernel-converge-1` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card DOOR-CONVERGE-1 (run 15c, 2026-09-14): `EXPECTED_DIVERGENCES` in
`door_parity_tests.rs` lists 22 names whose facade and SQL door resolve different kernels.
This unit closes the eight clause names: every function here answers PySpark 4.1.2 on BOTH
doors — `spark.sql(...)` and the Python API — for argument shapes, nulls, edge cases, values
AND Arrow type/nullability, or carries a dated refusal with Spark's error class.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`,
`python/repark/src/repark/spark/functions*.py`, `dataframe/**`, `column.py`, `session/**`,
`crates/repark-spark/src/{extension,normalize,spark_literals}.rs` (another lane owns them).

**Oracle:** `fixtures-batch1.json` / `fixtures-batch2.json` (PySpark 4.1.2 answers) vs
`repark-batch1.json` / `repark-batch2.json` (today's RePark answers). Cells whose SQL uses a
`2.5D` suffix are re-pinned with `CAST(2.5 AS DOUBLE)` per the card note — the `D` suffix is a
lexer divergence owned by another unit.

## PROPOSITION LEDGER — DOOR-CONVERGE-1 step 1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `base64` pads RFC 4648 AND chunks every 76 output chars with `\r\n`, non-null for non-null input, on both doors (cells BL17-0..8, DIV-api-base64-long); `unbase64` returns BINARY, accepts unpadded input, skips `\r\n`, answers empty binary for `'!!'` (cells DIV-unbase64-0..3). | `test_door_converge_1.py::test_c001_*` green on both doors. | OPEN | Red (base tree): `base64(repeat('x',100))` answers unchunked and nullable (`At index 1 diff: True != False`); `unbase64('!!')` raises `Execution error: Failed to decode value using base64pad: Invalid symbol 33, offset 0.`; facade `F.base64` returns unpadded `U3Bhcms`/`QQ`. |
| C-002 | `hypot` is registered on the SQL door, overflow-safe rescaled (`f64::hypot`): `hypot(1e200,1e200)` = 1.414213562373095e+200 non-null when both inputs non-null, NULL propagates, `hypot(inf, NaN)` = inf (cells BL16-0..8, BL16-api). | `test_c002_hypot_registers_rescaled_on_both_doors` green. | OPEN | Red (base tree): `AnalysisException: Error during planning: Invalid function 'hypot'. Did you mean 'pow'?`; facade `F.hypot(1e200,1e200)` answers `inf`. |
| C-003 | `abs` of a signed integer minimum raises `[ARITHMETIC_OVERFLOW]` on the SQL door under ANSI (cells DIV-abs-0..2); result type keeps the input width (`abs(CAST(-5 AS TINYINT))` = 5 tinyint); facade keeps raising. | `test_c003_abs_integer_min_raises_on_the_sql_door` green. | OPEN | Red (base tree): `Failed: DID NOT RAISE PySparkException` — door answers -128/-32768/-2147483648/-9223372036854775808 wrapped (SparkAbs reads `execution.enable_ansi_mode`, never set). |
| C-004 | `size(NULL array)` is NULL with nullable `int` on both doors (Spark 4 `sizeOfNull=false` default); `cardinality` answers `int` (cells DIV-size-0..3, DIV-api-size-null). | `test_c004_size_null_array_is_null_int_on_both_doors` green. | OPEN | Red (base tree): `size(CAST(NULL AS ARRAY<INT>))` answers `-1` `int32 not null`; `cardinality` answers `uint64`. |
| C-005 | `array_contains` is three-valued NULL on both doors (`array_contains(array(1,NULL), 2)` → NULL); a NULL-typed needle on the SQL door refuses `DATATYPE_MISMATCH.NULL_TYPE` (cells DIV-array_contains-0..3, DIV-api-contains). | `test_c005_array_contains_three_valued_on_both_doors` green. | OPEN | Red (base tree): `Failed: DID NOT RAISE AnalysisException` — door answers NULL for a NULL-typed needle; facade `F.array_contains` answers `False`. |
| C-006 | `approx_count_distinct` and `regr_count` derive NON-null `bigint` on both doors, also over empty input (cells BL18-sql, BL18-api, BL18-sql-empty). | `test_c006_count_aggregates_non_null_bigint_on_both_doors` green + `test_types_1.py` nullability pair trued up. | OPEN | Red (base tree): `assert (DataType(int64), True) == (DataType(int64), False)` — both UDAFs derive nullable today. |
| C-007 | `ascii` answers the codepoint (`ascii('é')`=233, `'€x'`=8364, `''`=0) and `length`/`character_length` of BINARY counts bytes (`X'C3A9'` → 2) on both doors (cells DIV-ascii-0..3, DIV-length-0..3, DIV-api-ascii, DIV-api-length-bin). | `test_c007_ascii_codepoint_and_binary_length_on_both_doors` green. | OPEN | Red (base tree): facade `F.length` on `X'C3A9'` answers `1` (DF-core char path), Spark `2` bytes. |
| C-008 | `bin(true)` / `rint(true)` refuse with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` in the message on the SQL door (cells BL6-sql-0..1). Facade half (`F.bin`/`F.rint` cast first, in `functions*.py`) is outside the fence — listed in `out_of_scope_observed`. | `test_c008_bin_rint_refuse_boolean_on_the_sql_door` green. | OPEN | Red (base tree): `bin(true)` raises `Internal error: Function 'bin' failed to match any signature ... requires Int64, but received Boolean`; `rint(true)` raises `expects Numeric but received Boolean` — internal signature errors, not Spark's class. |
| C-009 | No regression: the full facade suite, the Rust test set for `repark-functions`/`repark-python`, lint, fmt, and the parity-harness suite stay green; `EXPECTED_DIVERGENCES` ratchets 22 → 14 and every remaining row is still real. | `cargo test -p repark-functions -p repark-python`, `cargo clippy ... -D warnings`, `cargo fmt --all --check`, `pytest python/repark/tests -q`, `pytest python/repark-parity/tests -q` all green. | OPEN | — |

## Evidence

### Red-first run — `pytest python/repark/tests/test_door_converge_1.py -q` on the base tree

```
FAILED test_c001_base64_pads_and_chunks_on_both_doors
  At index 1 diff: True != False   (base64(repeat('x',100)) unchunked + nullable today)
FAILED test_c001_unbase64_lenient_decode_on_both_doors
  PySparkException: Execution error: Failed to decode value using base64pad:
  Invalid symbol 33, offset 0.
FAILED test_c002_hypot_registers_rescaled_on_both_doors
  AnalysisException: Error during planning: Invalid function 'hypot'. Did you mean 'pow'?
FAILED test_c003_abs_integer_min_raises_on_the_sql_door
  Failed: DID NOT RAISE PySparkException  (door wraps on every signed minimum)
FAILED test_c004_size_null_array_is_null_int_on_both_doors
  assert (DataType(int32), False, [-1]) == (DataType(int32), True, [None])
FAILED test_c005_array_contains_three_valued_on_both_doors
  Failed: DID NOT RAISE AnalysisException  (NULL-typed needle answers NULL today)
FAILED test_c006_count_aggregates_non_null_bigint_on_both_doors
  assert (DataType(int64), True) == (DataType(int64), False)
FAILED test_c007_ascii_codepoint_and_binary_length_on_both_doors
  assert [1] == [2]   (facade F.length on X'C3A9' answers 1, Spark answers 2 bytes)
FAILED test_c008_bin_rint_refuse_boolean_on_the_sql_door
  Regex 'DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE' did not match:
  "Internal error: Function 'bin' failed to match any signature ..."
9 failed in 0.32s
```

### Measured divergences behind the reds (fixtures vs repark batches)

- C-001: door `base64` pads but never chunks; facade `base64` lowers to `encode(x,'base64')`
  (unpadded); door `unbase64` fails on `'!!'` and `\r\n` (strict `base64pad` decoder).
- C-002: `hypot` unregistered on the door; facade composes `sqrt(x*x+y*y)` → `inf`.
- C-003: door `SparkAbs` reads `execution.enable_ansi_mode` (never set) → wraps on every
  signed minimum; facade raises through Arrow's checked abs.
- C-004: door `size` is legacy `-1` non-null (Spark 4 `sizeOfNull=false` wants NULL);
  `cardinality` resolves DF-core `uint64`, Spark wants `int`.
- C-005: door kernel already three-valued (datafusion-spark SparkArrayContains) but accepts
  a NULL-typed needle; facade `array_has` answers `False` instead of NULL.
- C-006: `SignedAggregate` narrows `UInt64`→`Int64` but keeps the default `is_nullable`
  (true); Spark marks count aggregates non-null.
- C-007: door already converged (datafusion-spark SparkAscii/SparkLengthFunc); facade uses
  DF-core `ascii`/`length` (binary `X'C3A9'` → 1 char, Spark 2 bytes).
- C-008: door errors are internal signature texts; Spark's class is
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` (BIGINT required for `bin`, DOUBLE for `rint`).

<!-- COVERAGE_ATTESTATION goes here once every clause is PROVEN. -->

# Unit ledger — DOOR-CONVERGE-1 · facade and SQL door answer Spark with one kernel

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

## PROPOSITION LEDGER — DOOR-CONVERGE-1 steps 1–4 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `base64` pads RFC 4648 AND chunks every 76 output chars with `\r\n`, non-null for non-null input, on both doors (cells BL17-0..8, DIV-api-base64-long); `unbase64` returns BINARY, accepts unpadded input, skips `\r\n`, answers empty binary for `'!!'` (cells DIV-unbase64-0..3). | `test_door_converge_1.py::test_c001_*` green on both doors. | PROVEN | Red (base tree): `base64(repeat('x',100))` answers unchunked and nullable (`At index 1 diff: True != False`); `unbase64('!!')` raises `Execution error: Failed to decode value using base64pad: Invalid symbol 33, offset 0.`; facade `F.base64` returns unpadded `U3Bhcms`/`QQ`. |
| C-002 | `hypot` is registered on the SQL door, overflow-safe rescaled (`f64::hypot`): `hypot(1e200,1e200)` = 1.414213562373095e+200 non-null when both inputs non-null, NULL propagates, `hypot(inf, NaN)` = inf (cells BL16-0..8, BL16-api). | `test_c002_hypot_registers_rescaled_on_both_doors` green. | PROVEN | Red (base tree): `AnalysisException: Error during planning: Invalid function 'hypot'. Did you mean 'pow'?`; facade `F.hypot(1e200,1e200)` answers `inf`. |
| C-003 | `abs` of a signed integer minimum raises `[ARITHMETIC_OVERFLOW]` on the SQL door under ANSI (cells DIV-abs-0..2); result type keeps the input width (`abs(CAST(-5 AS TINYINT))` = 5 tinyint); facade keeps raising. | `test_c003_abs_integer_min_raises_on_the_sql_door` green. | PROVEN | Red (base tree): `Failed: DID NOT RAISE PySparkException` — door answers -128/-32768/-2147483648/-9223372036854775808 wrapped (SparkAbs reads `execution.enable_ansi_mode`, never set). |
| C-004 | `size(NULL array)` is NULL with nullable `int` on both doors (Spark 4 `sizeOfNull=false` default); `cardinality` answers `int` (cells DIV-size-0..3, DIV-api-size-null). | `test_c004_size_null_array_is_null_int_on_both_doors` green. | PROVEN | Red (base tree): `size(CAST(NULL AS ARRAY<INT>))` answers `-1` `int32 not null`; `cardinality` answers `uint64`. |
| C-005 | `array_contains` is three-valued NULL on both doors (`array_contains(array(1,NULL), 2)` → NULL); a NULL-typed needle on the SQL door refuses `DATATYPE_MISMATCH.NULL_TYPE` (cells DIV-array_contains-0..3, DIV-api-contains). | `test_c005_array_contains_three_valued_on_both_doors` green. | PROVEN | Red (base tree): `Failed: DID NOT RAISE AnalysisException` — door answers NULL for a NULL-typed needle; facade `F.array_contains` answers `False`. |
| C-006 | `approx_count_distinct` and `regr_count` derive NON-null `bigint` on both doors, also over empty input (cells BL18-sql, BL18-api, BL18-sql-empty). | `test_c006_count_aggregates_non_null_bigint_on_both_doors` green + `test_types_1.py` nullability pair trued up. | PROVEN | Red (base tree): `assert (DataType(int64), True) == (DataType(int64), False)` — both UDAFs derive nullable today. |
| C-007 | `ascii` answers the codepoint (`ascii('é')`=233, `'€x'`=8364, `''`=0) and `length`/`character_length` of BINARY counts bytes (`X'C3A9'` → 2) on both doors (cells DIV-ascii-0..3, DIV-length-0..3, DIV-api-ascii, DIV-api-length-bin). | `test_c007_ascii_codepoint_and_binary_length_on_both_doors` green. | PROVEN | Red (base tree): facade `F.length` on `X'C3A9'` answers `1` (DF-core char path), Spark `2` bytes. |
| C-008 | `bin(true)` / `rint(true)` refuse with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` in the message on the SQL door (cells BL6-sql-0..1). Facade half (`F.bin`/`F.rint` cast first, in `functions*.py`) is outside the fence — listed in `out_of_scope_observed`. | `test_c008_bin_rint_refuse_boolean_on_the_sql_door` green. | PROVEN | Red (base tree): `bin(true)` raises `Internal error: Function 'bin' failed to match any signature ... requires Int64, but received Boolean`; `rint(true)` raises `expects Numeric but received Boolean` — internal signature errors, not Spark's class. |
| C-009 | No regression: the full facade suite, the Rust test set for `repark-functions`/`repark-python`, lint, fmt, and the parity-harness suite stay green; `EXPECTED_DIVERGENCES` ratchets 22 → 14 and every remaining row is still real. | `cargo test -p repark-functions -p repark-python`, `cargo clippy ... -D warnings`, `cargo fmt --all --check`, `pytest python/repark/tests -q`, `pytest python/repark-parity/tests -q` all green. | PROVEN | — |

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

## Green run — `pytest python/repark/tests/test_door_converge_1.py -q` on the fixed tree

```
9 passed in 0.16s
```

Kernel layout: `repark_functions::spark_math` (SparkAbs ANSI carrier / SparkHypot /
SparkBin / SparkRint, `Signature::user_defined` + `coerce_types` for Spark's
`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`), `repark_functions::spark_base64` (hand-rolled
`java.util.Base64` MIME codec — no `base64` crate dep), `collection/size.rs`
(`sizeOfNull=false`: NULL array → nullable `int`), `collection/array_contains.rs` (wraps
datafusion-spark's three-valued kernel + `coerce_types` refuses a `Null`-typed needle with
`DATATYPE_MISMATCH.NULL_TYPE`; registered under `array_has` alias so the facade's
`F.array_contains` spelling converges), `spark_result_types::SignedAggregate`
(`is_nullable=false`, `default=0`). Facade arms for the clause names moved to
`crates/repark-python/src/column/function_dispatch/dispatch_spark.rs` and delegate to the
same `repark_functions::expr_fn` builders the SQL door registers.

Recorded-answer pins flipped to the measured Spark answers: `test_abs_expr_1` (SQL door
raises), `test_bl15_bl16_math_divergences` (rescaled), `test_bl17_base64_padding` (padded),
`test_fn_batch2` (`aGk=`), `test_types_1` (`(int64, False)` pair),
`fnp8_repark_dispositions.json` `binding-nested_array-*-column` (list item `int32 not
null`, now identical to the sql/expr doors). `EXPECTED_DIVERGENCES` ratcheted 22 → 14;
`abs`/`hypot`/`bin`/`rint`/`base64`/`unbase64`/`size`/`cardinality`/`array_contains`/
`ascii`/`length`/`character_length` joined `SCALAR_NAMES`.

Out-of-scope observed: (1) `F.bin`/`F.rint` still cast BOOLEAN first in
`python/repark/src/repark/spark/functions*.py` — the BL-6 facade half lives outside the
fence (registry note added). (2) `repeat('x',100)` derives `nullable` on the door where
Spark's `Repeat` does not for non-null args — inherited by `base64`'s arg-nullability
propagation; the chunked-shape pin holds type+value, nullability stays pinned on the
literal-input shapes. (3) `1e200` (no `D` suffix) parses as decimal scale -200, over the
supported minimum — pinned via `CAST('1e200' AS DOUBLE)` like the card's `2.5D` note.

## PROPOSITION LEDGER — DOOR-CONVERGE-1 round 2 (Grok logic critic + S2-21 perf review) — 2026-09-16

Oracle for this round: `fixtures-batch6.json` (live PySpark 4.1.2, `C6-*` cells, binding).
Every logic fix landed red-first: the pin went up on the base tree, failed, then the kernel
moved.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-010 | `unbase64` refuses malformed endings with `java.util.Base64` MIME-decoder text on both doors: alphabet after padding (`'QQ==QQ'` → `incorrect ending byte at 5`, `'U3Bhcms=QQ'` → `… at 9`), wrong 4-byte ending unit (`'QQ='`, `'=QQ'`), lone quantum (`'Q'` → `Last unit does not have enough valid bits`); still answers `'QR'` → `b'A'`, `'QQQ'` → `b'A\x04'`, skips interior space (cells C6-unb64-*, C6-api-unb64). | `test_l001_unbase64_strict_endings_on_both_doors` green. | PROVEN | Red (base tree): the lenient decoder stopped at the first pad and answered `b'A'` for `'QQ==QQ'` — silent truncation, no error. |
| C-011 | `array_contains` coerces element and needle to their tightest common type: `array_contains(array(1,2), CAST(3 AS DOUBLE)/CAST(2 AS DOUBLE))` → `False` NULLABLE, `CAST(2 AS DOUBLE)` → `True` non-null, out-of-range BIGINT needle → `False` non-null, decimal and nested arrays still `True` (cells C6-ac-double-needle/hit, C6-ac-bigint-out/in, C6-api-ac-double/bigint, C6-ac-decimal, C6-ac-nested). | `test_l002_array_contains_coerces_to_tightest_common_type` green. | PROVEN | Red (base tree): `coerce_types` narrowed the needle to the element type — `3.0/2.0` cast DOUBLE→INT (1) answered `True` where Spark's DOUBLE comparison answers `False`. |
| C-012 | STRING needle vs `array<int>` and INT needle vs `array<string>` refuse `[DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES]` on both doors, ANSI on and off (cells C6-ac-string-needle, C6-ac-int-in-strings, C6-api-ac-string, C6-ac-string-off). | `test_l003_array_contains_diff_types_refuses_on_both_doors` + `_when_ansi_off` green. | PROVEN | Red (base tree): the needle was cast into the element type — `'x'`→NULL int — and the call answered NULL instead of refusing. |
| C-013 | `array_contains(array(), 1)` → `False` non-null on both doors; the `NULL_TYPE` refusal fires only for an untyped NULL needle (cell C6-ac-empty-untyped). | `test_l004_array_contains_empty_untyped_array_answers_false` green. | PROVEN | Red (base tree): `array()`'s `Null` element coerced the needle to `Null`, which tripped the `NULL_TYPE` refusal — the call raised instead of answering `False`. |
| C-014 | ANSI-off `abs` wraps signed minima with width kept: every signed minimum returns itself, `abs(CAST(-5 AS TINYINT))` → `5` tinyint; the facade column case stays nullable because its input column is (cells C6-abs-off-*, C6-api-abs-off-tiny). | `test_l007_abs_wraps_signed_minima_when_ansi_off` green (builder-configured `spark.sql.ansi.enabled=false` session). | PROVEN | Green on the fixed tree; the pin locks the existing wrap arm so a future ANSI-off regression goes red. |

### Round-2 perf lines (S2-21 method, best of 3, same session, release build)

`SELECT octet_length(base64(concat(repeat('x', 900), CAST(value AS STRING)))) FROM
range(1000000)` vs `encode(…, 'base64pad')`; `unbase64`/`decode` over the same payload:

```
                    before (1a7302d8)   after (this tree)   DataFusion reference
base64   900B:      1.039s              0.813s              encode 0.672–0.743s
unbase64 900B:      1.139s              0.833s              decode 0.763–0.830s
base64    90B:      0.545s              0.517s              encode 0.447–0.451s
unbase64  90B:      0.486s              0.450s              decode 0.459–0.461s
```

`base64` writes into one `StringBuilder` (ASCII bytes, 57-byte input chunks per 76-char
line); `unbase64` uses a `static` decode table and decodes straight into one batch buffer
with offsets, reusing the input null bitmap. P2-1..P2-4: `abs` keeps the once-per-invoke
ANSI flag and uses Arrow `try_unary` (ANSI on, with a non-null-MIN fallback scan) /
`wrapping_abs` (ANSI off) and `Arc::clone`s unsigned inputs; `hypot`/`rint` use
`Float64Array::unary` / a binary zip with OR-ed null buffers; `size` uses Arrow's `length`
kernel on the dense path with input nulls copied; `bin` renders bits into a `StringBuilder`
instead of `format!` per row.

### Round-2 notes

- L-008 (P3): runtime `spark.conf.set("spark.sql.ansi.enabled", …)` not moving kernels is
  the SET-ANSI-RUNTIME-1 residue owned by unit sql-set-door-1 — recorded here only.
- The array-constructor work (`array`/`make_array` Spark `containsNull`, one kernel per
  spelling, `element` vs `item` child-field names) and the registry-wide `promise_retag`
  alignment of planned vs physical Arrow fields ride in the same commit; the flipped
  recorded answers (`test_types_1`, `test_nullability_2`, `fnp8` dispositions) match the
  oracle's embedded `list<element: int32 not null>` schema.

## COVERAGE_ATTESTATION — DOOR-CONVERGE-1 — 2026-09-16

- C-001..C-008, C-010..C-014 PROVEN: every clause's both-door pin is green on the rebuilt
  release native module (`16 passed` for `test_door_converge_1.py`); the measured
  divergences in the red-first runs are each closed by a registered Spark kernel the
  facade and the SQL door share (`door_parity_tests` keeps the divergence table at 14).
- C-009 PROVEN: facade suite `pytest python/repark/tests -q` green, `cargo test -p
  repark-functions -p repark-python` green, clippy/fmt/size gates green.

```
COVERAGE_ATTESTATION:
  pr_unit: door-converge-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against its fixture cell, not a paraphrase — C6-unb64-* error texts, C6-ac-* nullability and values, C6-abs-off-* widths; the array ctor's element-vs-item child naming and containsNull match Spark's CreateArray semantics, verified against the oracle's embedded schema in the fnp8 cells.
      artifacts: [python/repark/tests/test_door_converge_1.py, fixtures-batch6.json]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs exercised — 'QQ=='/'QQ='/'=QQ'/'Q'/'QR'/'QQQ'/'!!' unbase64 endings, empty untyped array, out-of-range and in-range BIGINT needles, decimal and nested-array needles, signed minima of all four widths, ANSI on and off legs.
      artifacts: [python/repark/tests/test_door_converge_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Failure paths pin Spark's classes and texts — Java MIME-decoder messages on decode errors, DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES on incompatible pairs, NULL_TYPE only for an untyped NULL needle; ANSI-off takes the same refusal (C6-ac-string-off).
      artifacts: [python/repark/tests/test_door_converge_1.py]
    - id: AT-4
      status: N/A
      justification: Kernels are pure row functions; no shared mutable state, no locks, no ordering assumptions. The static decode table is immutable.
    - id: AT-5
      status: N/A
      justification: No auth, injection, secret, or deserialization surface — in-process Arrow kernels over typed input.
    - id: AT-6
      status: ATTACKED
      evidence: The promise_retag sweep aligns every registered scalar UDF's planned return field with its physical Arrow output (list element name/nullability, outer nullability); recorded-answer dispositions flipped only where the oracle's embedded schema proved the new shape (list<element: int32 not null>).
      artifacts: [crates/repark-functions/src/promise_retag.rs, python/repark/tests/fnp8_repark_dispositions.json]
    - id: AT-7
      status: ATTACKED
      evidence: P1-1/P1-2 measured before/after on 1M rows — base64 1.039s -> 0.813s and unbase64 1.139s -> 0.833s at 900B payloads, within ~1.2x of DataFusion's encode/decode; per-row String/Vec/table allocations removed (StringBuilder, static table, batch buffer with reused null bitmap).
      artifacts: [crates/repark-functions/src/spark_base64.rs, crates/repark-functions/src/spark_math.rs, crates/repark-functions/src/collection/size.rs]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion contracts honored — the wrapper invokes the inner ScalarUDFImpl directly so DF's own promise assertion is never bypassed against the corrected field; element_at's alias key binds only to repark's kernel (alias-clobber order dependence removed by per-key named wrapping); schema_name reproduces datafusion-sql's verbose alias text for alias-resolved calls.
      artifacts: [crates/repark-functions/src/promise_retag.rs, crates/repark-functions/src/lib.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal carries Spark's error class and Java's message text in the exception, so the failure is diagnosable from the error alone; no logging changes.
      artifacts: [python/repark/tests/test_door_converge_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Pins-first held — the four round-2 reds failed on the base tree with the predicted behavior (silent truncation, needle-to-element narrowing, cast-instead-of-refuse, NULL_TYPE refusal on the empty array); every new branch has a naming input: pad-then-alphabet, truncated pad, lone quantum, untyped-empty vs NULL needle, ANSI on vs off.
      artifacts: [python/repark/tests/test_door_converge_1.py, crates/repark-python/src/column/door_parity_tests.rs]
  complete: true
```

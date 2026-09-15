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
| C-003 | `abs` of a signed integer minimum raises `[ARITHMETIC_OVERFLOW]` on the SQL door under ANSI (cells DIV-abs-0..2); result type keeps the input width (`abs(CAST(-5 AS TINYINT))` = 5 tinyint); facade keeps raising. Round 5 adds: `abs` takes Spark's implicit STRING→DOUBLE cast on both doors — `abs('-1')` = `1.0` double nullable, `'x'` literal and column inputs raise `[CAST_INVALID_INPUT]` (cells A11-sql-abs-1/-x, A11-api-abs-x, A11-callfn-abs-x). | `test_c003_abs_integer_min_raises_on_the_sql_door` + `test_c003_abs_casts_string_to_double_on_both_doors` green. | PROVEN | Red (base tree): `Failed: DID NOT RAISE PySparkException` — door answers -128/-32768/-2147483648/-9223372036854775808 wrapped (SparkAbs reads `execution.enable_ansi_mode`, never set). Round 5 red-first: STRING input refused `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` at `coerce_types` where Spark implicitly casts to DOUBLE. |
| C-004 | `size(NULL array)` is NULL with nullable `int` on both doors (Spark 4 `sizeOfNull=false` default); `cardinality` answers `int` (cells DIV-size-0..3, DIV-api-size-null). | `test_c004_size_null_array_is_null_int_on_both_doors` green. | PROVEN | Red (base tree): `size(CAST(NULL AS ARRAY<INT>))` answers `-1` `int32 not null`; `cardinality` answers `uint64`. |
| C-005 | `array_contains` is three-valued NULL on both doors (`array_contains(array(1,NULL), 2)` → NULL); a NULL-typed needle on the SQL door refuses `DATATYPE_MISMATCH.NULL_TYPE` (cells DIV-array_contains-0..3, DIV-api-contains). | `test_c005_array_contains_three_valued_on_both_doors` green. | PROVEN | Red (base tree): `Failed: DID NOT RAISE AnalysisException` — door answers NULL for a NULL-typed needle; facade `F.array_contains` answers `False`. |
| C-006 | `approx_count_distinct` and `regr_count` derive NON-null `bigint` on both doors, also over empty input (cells BL18-sql, BL18-api, BL18-sql-empty). | `test_c006_count_aggregates_non_null_bigint_on_both_doors` green + `test_types_1.py` nullability pair trued up. | PROVEN | Red (base tree): `assert (DataType(int64), True) == (DataType(int64), False)` — both UDAFs derive nullable today. |
| C-007 | `ascii` answers the codepoint (`ascii('é')`=233, `'€x'`=8364, `''`=0) and `length`/`character_length` of BINARY counts bytes (`X'C3A9'` → 2) on both doors (cells DIV-ascii-0..3, DIV-length-0..3, DIV-api-ascii, DIV-api-length-bin). | `test_c007_ascii_codepoint_and_binary_length_on_both_doors` green. | PROVEN | Red (base tree): facade `F.length` on `X'C3A9'` answers `1` (DF-core char path), Spark `2` bytes. |
| C-008 | `bin(true)` / `rint(true)` refuse with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` in the message on the SQL door (cells BL6-sql-0..1). Facade half (`F.bin`/`F.rint` cast first, in `functions*.py`) is outside the fence — listed in `out_of_scope_observed`. | `test_c008_bin_rint_refuse_boolean_on_the_sql_door` green. | PROVEN | Red (base tree): `bin(true)` raises `Internal error: Function 'bin' failed to match any signature ... requires Int64, but received Boolean`; `rint(true)` raises `expects Numeric but received Boolean` — internal signature errors, not Spark's class. |
| C-009 | No regression: the full facade suite, the Rust test set for `repark-functions`/`repark-python`, lint, fmt, and the parity-harness suite stay green; `EXPECTED_DIVERGENCES` ratchets 22 → 14 and every remaining row is still real. | `cargo test -p repark-functions -p repark-python`, `cargo clippy ... -D warnings`, `cargo fmt --all --check`, `pytest python/repark/tests -q`, `pytest python/repark-parity/tests -q` all green. | PROVEN | Every listed gate green on the post-R-13 tree — facade suite 6135 passed, 0 failed (the C-011/C-013 nullability legs now pin the recorded divergence per R-13). |

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
| C-011 | `array_contains` coerces element and needle to their tightest common type: `array_contains(array(1,2), CAST(3 AS DOUBLE)/CAST(2 AS DOUBLE))` → `False` NULLABLE, `CAST(2 AS DOUBLE)` → `True` non-null, out-of-range BIGINT needle → `False` non-null, decimal and nested arrays still `True` (cells C6-ac-double-needle/hit, C6-ac-bigint-out/in, C6-api-ac-double/bigint, C6-ac-decimal, C6-ac-nested). | `test_l002_array_contains_coerces_to_tightest_common_type` green. | PROVEN (R-13: nullability legs handed off) | Widening and every value leg PROVEN on both doors. The literal-haystack nullability sub-claim (Spark non-null) is handed to DOOR-CONVERGE-2 under R-13: it rides on the array constructor's declared `containsNull`, which DataFusion marks unconditionally nullable — recorded divergence ARRAY-LITERAL-CONTAINSNULL-1; the pin asserts today's `nullable=True` and flips back when the ctor fix lands. |
| C-012 | STRING needle vs `array<int>` and INT needle vs `array<string>` refuse `[DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES]` on both doors, ANSI on and off (cells C6-ac-string-needle, C6-ac-int-in-strings, C6-api-ac-string, C6-ac-string-off). | `test_l003_array_contains_diff_types_refuses_on_both_doors` + `_when_ansi_off` green. | PROVEN | Red (base tree): the needle was cast into the element type — `'x'`→NULL int — and the call answered NULL instead of refusing. |
| C-013 | `array_contains(array(), 1)` → `False` non-null on both doors; the `NULL_TYPE` refusal fires only for an untyped NULL needle (cell C6-ac-empty-untyped). | `test_l004_array_contains_empty_untyped_array_answers_false` green. | PROVEN (R-13: nullability leg handed off) | Value `False` and the `NULL_TYPE` scope PROVEN on both doors. The non-null sub-claim is handed to DOOR-CONVERGE-2 under R-13 — `array()`'s element field is DF's unconditionally-nullable `item`, so `containsNull` reads true where Spark's empty constructor declares `containsNull=false`; recorded divergence ARRAY-LITERAL-CONTAINSNULL-1, pin asserts today's `nullable=True`. |
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
  alignment of planned vs physical Arrow fields rode in the round-2 commit and were
  reverted in round 3 under ruling R-10 — see "Handed to DOOR-CONVERGE-2" below; the
  re-recorded answers (`test_types_1`, `test_nullability_2`, `fnp8` dispositions)
  returned to their pre-round-2 forms.

## RULING R-10 — DOOR-CONVERGE-1 round 3 (orchestrator audit of 99e61a94, run 15c) — 2026-09-15

Ruling R-10 (G-2, AGENTS.md "fixes stay narrow" — a semantic-adjacent rewrite is a
separate change, never a passenger on a fix): the registry-wide
`crates/repark-functions/src/promise_retag.rs` wrapper and the
`crates/repark-functions/src/collection/make_array.rs` `containsNull` change were
removed from this branch, together with every edit that existed only because of
them — the `lib.rs` registration hook, the `function_dispatch.rs` facade wrap, the
eight re-recorded `fnp8_repark_dispositions.json` entries, the `test_nullability_2`
/`test_types_1` element-nullability expectations, and the registry note marking
COMPLEX-ELEM-NULL-1's array arm FIXED. All of it moves to DOOR-CONVERGE-2, measured
against `fixtures-batch7.json` with its own review.

### Handed to DOOR-CONVERGE-2 — batch-7 probe evidence

`contains_null_probe.py` over batch-7 cells through `spark.sql`,
`probe-branch.txt` (round-2 tree) vs `probe-mainlike.txt` (main-like):

- `SELECT array(1, 2)` still reports `containsNull=True` on this branch where Spark
  answers False — the round-2 constructor fix never reached the SQL door.
- `array_append(array(1,2), 3)` reported `nullable=True` on the branch where the
  main-like probe and Spark are non-null (N7-11). Post-revert measurement shows
  the retag did not cause it: the branch still answers `nullable=True`, and the
  answer is unchanged from this branch's true base `acbb6a8e`, where
  `array_append` is DataFusion's builtin (default field always nullable). The
  non-null main-like answer comes from `fix(array-null-1)` (`44ca3aea`,
  repark-owned `array_append`/`array_prepend` with arg-nullability propagation),
  which landed on main after this branch's base — a base-version delta, not a
  regression this branch introduced. Verified against a sibling build of the
  newer main (`/tmp/pc-bitmap` at `f6312c7b`): `nullable=False`,
  `IntegerType` elements.
- `array_contains(array(1,2), 1)` (N7-31) returned to main's answer: `nullable=True`
  where Spark is non-null — the same ctor divergence surfacing through
  `array_contains` (the residual red pins below).
- Twenty other batch-7 divergences are identical on main and this branch —
  pre-existing DOOR-CONVERGE-2 work.
- Under R-13 (round 4): C-011's and C-013's literal-haystack nullability
  sub-claims move here — `array_contains` over a literal `array(…)`/`array()`
  haystack must answer Spark's non-null once the constructor declares
  `containsNull` from its children (cells C6-ac-double-hit, C6-ac-empty-untyped,
  N7-31; registry ARRAY-LITERAL-CONTAINSNULL-1). Value legs stay PROVEN in this
  unit.

Post-revert probe, release build, last line: `equal 10 different 23`. The single
line delta against `probe-mainlike.txt` is N7-11, accounted for above — the
main-like baseline carries `array-null-1` (`44ca3aea`), which this branch's base
predates; the branch's own answer is unchanged by the R-10 revert.

### Residual reds under R-10 — disposition under RULING R-13 (round 4)

Two this-unit pin legs asserted the oracle's non-null answer that only the removed
constructor work can reach:

- `test_l002` literal-haystack legs — `CAST(2 AS DOUBLE)` hit, BIGINT out/in-range,
  decimal, nested — answer `nullable=True` where Spark pins non-null.
- `test_l004` (`array_contains(array(), 1)`) — answers `False` NULLABLE where Spark
  pins `False` non-null.

Root cause: `array_contains` computes `left.nullable || right.nullable ||
containsNull(element)`; the declared element `containsNull` comes from the array
constructor, which DataFusion marks unconditionally nullable. On the analyzed plan
— the schema repark exports (`analyze_eagerly`) — `array(1,2)` is still a
`ScalarFunction`, indistinguishable from a nullable-element column at field level;
the kernel-local alternative (inspecting `scalar_arguments` for a folded literal
haystack) was implemented and reverted because it never fires on the analyzed
schema.

Ruling R-13 (round 4) disposed the legs: value legs and column-haystack legs keep
asserting Spark's answer (green); the literal-haystack nullability legs now pin
today's `nullable=True` as the recorded divergence ARRAY-LITERAL-CONTAINSNULL-1
(§7 registry, owner DOOR-CONVERGE-2), and flip back to non-null when the
constructor fix lands. C-011's and C-013's nullability sub-claims moved to
"Handed to DOOR-CONVERGE-2"; their value sub-claims stay PROVEN.

The round-3 audit's N7-11 claim is withdrawn: `probe-mainlike.txt` was measured
on a native built from a newer main that includes `fix(array-null-1)`
(`44ca3aea`, repark-owned `array_append`/`array_prepend`); this branch's base
`acbb6a8e` predates it, so the `array_append` nullability delta is a base-version
difference, not a retag regression.

The `element_at` → `map_extract` alias clobber needs no fix on this branch (it was
an artifact of the removed re-registration sweep); the hazard and today's answer
are pinned as ELEMENT-AT-ALIAS-1 (BACKLOG, §7 registry).

## ROUND 5 — rebase onto main `23ba2e53` + A11 `abs`/`char_length` fixes — 2026-09-16

The orchestrator rebased the branch onto main `23ba2e53` (7 commits replayed;
`collection.rs` keeps main's ARRAY-NULL-1 `array_append`/`array_prepend` shims
beside this unit's `array_contains`/`size` modules, `make_array` stays removed).
Two post-rebase facade reds in `test_fnp_misc_1.py` landed here per run-15a
clearance, measured against `fixtures-batch11.json` (live PySpark 4.1.2):

- **`abs` over STRING** — Spark implicitly casts STRING→DOUBLE:
  `abs('-1')` = `1.0` double nullable (A11-sql-abs-1); a malformed input raises
  `[CAST_INVALID_INPUT]` on all three entry spellings (A11-sql-abs-x,
  A11-api-abs-x, A11-callfn-abs-x). The kernel now accepts the utf8 family in
  `coerce_types` (passed through unchanged, so no engine CAST is inserted) and
  safe-casts inside `abs_typed` — under ANSI the first non-null input that
  casts to NULL raises the Spark-classed `CAST_INVALID_INPUT` sentence; under
  ANSI-off the malformed value answers NULL, matching Spark's try-cast
  semantics. Red-first: the pin failed on the base tree with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` before the kernel moved.
  `test_fnp_misc_1_call_function_abs_string_mismatch_is_not_unresolved` now
  asserts `CAST_INVALID_INPUT` (not UNRESOLVED_ROUTINE — its original intent)
  plus the `call_function('abs', lit('-1'))` = 1.0 value leg.
- **`char_length`** leaves `FACADE_ONLY_ROUTINE_NAMES` in `functions_byname.py` —
  the Rust dispatch already serves it, so the derived facade-only set no longer
  contains it (`call_function('char_length', lit('abc'))` = 3,
  A11-callfn-char-length).

## COVERAGE_ATTESTATION — DOOR-CONVERGE-1 — 2026-09-16 (round 5, A11)

- C-001..C-009, C-010, C-012, C-014 PROVEN: every clause's both-door pin is green on the
  rebuilt release native module; the measured divergences in the red-first runs are each
  closed by a registered Spark kernel the facade and the SQL door share
  (`door_parity_tests` keeps the divergence table at 14). Round 5 extends C-003 with
  the A11 string-coercion cells — `abs` over STRING casts to DOUBLE on both doors,
  CAST_INVALID_INPUT on malformed input.
- C-011, C-013 PROVEN under ruling R-13: value and column-haystack legs assert Spark's
  answer green; the literal-haystack nullability sub-claims moved to
  "Handed to DOOR-CONVERGE-2" — the pins assert today's `nullable=True` as recorded
  divergence ARRAY-LITERAL-CONTAINSNULL-1 and red when the constructor fix lands.

```
COVERAGE_ATTESTATION:
  pr_unit: door-converge-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against its fixture cell, not a paraphrase — C6-unb64-* error texts, C6-ac-* nullability and values, C6-abs-off-* widths; the round-3 batch-7 probe (probe-branch.txt vs probe-mainlike.txt) measured which divergences the round-2 scope widening caused and which predate it.
      artifacts: [python/repark/tests/test_door_converge_1.py, docs/spark-sql-iceberg-parity.md]
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
      evidence: The R-10 revert was verified subtraction-only where it touched shared surface — git-checkout restores of collection.rs/lib.rs/function_dispatch.rs/fnp8 dispositions/test_nullability_2/test_types_1 to the 1a7302d8 forms, both new modules deleted, no retained fix disturbed; the batch-7 probe re-measured post-revert (equal 10 different 23) and its single delta vs probe-mainlike.txt is N7-11, traced to main advancing past the branch base (array-null-1, 44ca3aea), not to this branch's edits.
      artifacts: [python/repark/tests/fnp8_repark_dispositions.json, python/repark/tests/test_nullability_2.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: P1-1/P1-2 measured before/after on 1M rows — base64 1.039s -> 0.813s and unbase64 1.139s -> 0.833s at 900B payloads, within ~1.2x of DataFusion's encode/decode; per-row String/Vec/table allocations removed (StringBuilder, static table, batch buffer with reused null bitmap).
      artifacts: [crates/repark-functions/src/spark_base64.rs, crates/repark-functions/src/spark_math.rs, crates/repark-functions/src/collection/size.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The element_at -> map_extract alias-clobber hazard was isolated to the removed re-registration sweep — post-revert element_at resolves to repark's dedicated binding on both doors (pin codifies it); the hazard is filed as ELEMENT-AT-ALIAS-1 BACKLOG for DOOR-CONVERGE-2's registry work.
      artifacts: [python/repark/tests/test_door_converge_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal carries Spark's error class and Java's message text in the exception, so the failure is diagnosable from the error alone; no logging changes.
      artifacts: [python/repark/tests/test_door_converge_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Pins-first held — the four round-2 reds failed on the base tree with the predicted behavior (silent truncation, needle-to-element narrowing, cast-instead-of-refuse, NULL_TYPE refusal on the empty array); the round-3 residual reds were reproduced post-revert before being flagged, never silently greened.
      artifacts: [python/repark/tests/test_door_converge_1.py, crates/repark-python/src/column/door_parity_tests.rs]
  complete: true
```

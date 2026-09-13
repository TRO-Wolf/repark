# Unit ledger — ABS-EXPR-1 · `F.abs`/`F.cbrt` lower to one native call — step 1

**Date:** 2026-09-13 · **Branch:** `fix/abs-expr-1` · **Base:** `4f121ab9` (`main`,
FACADE-3 step 2 merge)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card ABS-EXPR-1 (run 9 OBS-R9-6 / INC-R9-1, P1): the facade builds `F.abs`
as `when(c < 0, 0 - c).otherwise(c)`, embedding its child three times per level, so a
nested `F.abs` chain is exponential in native memory — ~76 MB at depth 10, 705 MB at
12, 1 963 MB at 13, 5 742 MB at 14, then an aborting allocation failure; uncapped the
same chain reached 84 GB and OOM-killed the box. `F.cbrt` has the same shape. The fix
lowers both to the single DataFusion scalar calls (`expr_fn::abs`, `expr_fn::cbrt`) and
audits every other facade-side `when(...)` rewrite that embeds its argument more than
once.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, `python/repark/src/repark/spark/dataframe/core.py`
(another lane owns it — its `replace` rewrite is audited, not edited).

## PROPOSITION LEDGER — ABS-EXPR-1 step 1 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The D-1 oracle cells are measured on live PySpark 4.1.2 (ANSI on, one session, stopped before the gates): `abs` int32 min → `ARITHMETIC_OVERFLOW` integer; int64 min → long; NULL → NULL same type; −1.5 → 1.5 (double and float32); −0.0 → +0.0; decimal(10,3) −12.345 → 12.345 same type; tinyint −5 → 5 tinyint; tinyint/smallint min → `ARITHMETIC_OVERFLOW` byte/short; decimal(38,0) min → `NUMERIC_VALUE_OUT_OF_RANGE`; bool → `DATATYPE_MISMATCH`; string → `CAST_INVALID_INPUT` (implicit cast to double). `cbrt` −8/27/−1 → −2.0/3.0/−1.0 double; NULL → NULL double; int → double; −0.0 → −0.0; ±inf → ±inf; decimal → double; bool → `DATATYPE_MISMATCH`; string → cast-to-double first. | The measured table in Evidence + the pin cells that encode it. | PROVEN | Measured 2026-09-13, PySpark 4.1.2, `spark.sql.ansi.enabled=true` (default). Full cell table under Evidence below. |
| C-002 | `F.abs` / `F.cbrt` / `F.nullif` lower to one native scalar call each (`call_scalar` → `expr_fn::abs` / `expr_fn::cbrt` / `expr_fn::nullif`); repark's answers on the C-001 cells match the oracle (error cells: still raises; value+type cells: exact). | `test_abs_expr_1.py` answer pins green; `make develop` rebuild. | PROVEN | Pin asserts Arrow-path value AND type per cell. |
| C-003 | A depth-40 `F.abs` chain plans and collects under a bounded RSS delta — bound = the flat depth-40 `F.sqrt` chain's delta ×2 (floor 64 MB) — in a subprocess under `RLIMIT_AS` 8 GB. Red on the base tree (abort ~depth 14), green after the lowering. | `test_abs_chain_depth40_memory_linear` red output pasted in Evidence, then green. | PROVEN | Subprocess worker shape follows `test_h3_spill_matrix.py`. |
| C-004 | Every facade-side `when(...)` rewrite that embeds its child more than once is listed in the audit table with its measured depth-12 RSS; each super-linear one is rewritten in this unit (one native call) or flagged for its own card when no one-call answer exists. | The audit table under Evidence; per-row dispositions. | PROVEN | Sites: `abs` (3×), `cbrt` (3×), `nullif` (2×), `_glue_element` for `array_append`/`array_prepend` (2×), `DataFrame.replace` dict loop in `dataframe/core.py` (2× per mapping entry — audited only, file fenced). Linear sites recorded for completeness: `nvl2`, `count_if`, `polars.null_count`, `reader_support` failed-count, `reader.py` nullValue (leaf children). |
| C-005 | Every existing `abs`/`cbrt`/`nullif` pin, the `docs/examples/functions/abs.py` example, and the parity-doc claims stay green or are trued up: `test_facade_polish.py` `sum(abs(x))` name pins, `test_g4b_semi_join.py` origin-thread pins, `test_h2_group_h2.py` display pin, `test_select_naming.py`/`test_select_global_agg.py`, `test_functions_a.py::test_cbrt_real_root_including_negatives`, `test_nullif_and_nullifzero`, `test_fnp15_16_declared_refuse.py`, `docs/design/spark-function-parity.md` §4.4 (the `abs` "not an engine abs" callout becomes false → dated note). | Named files green unchanged; dated doc note where a claim changed. | PROVEN | — |

## Evidence

### C-001 oracle measurement (live PySpark 4.1.2, `local[1]`, ANSI on — measured 2026-09-13)

| Cell | PySpark 4.1.2 answer |
|---|---|
| `abs(int32 min)` | raises `SparkArithmeticException [ARITHMETIC_OVERFLOW] integer overflow` |
| `abs(int64 min)` | raises `[ARITHMETIC_OVERFLOW] long overflow` |
| `abs(NULL int)` | `None`, `int` |
| `abs(-1.5 double)`, `abs(0.5)` | `1.5`, `0.5` double |
| `abs(-0.0)` | `0.0` double |
| `abs(-1.5 float)` | `1.5` float |
| `abs(-12.345 decimal(10,3))` | `12.345` `decimal(10,3)` |
| `abs(-5 tinyint)` / `abs(-5 smallint)` | `5` tinyint / `5` smallint (type kept) |
| `abs(int8 min)` / `abs(int16 min)` | raise `[ARITHMETIC_OVERFLOW] byte/short overflow` |
| `abs(-(10^38−1) decimal(38,0))` | raises `[NUMERIC_VALUE_OUT_OF_RANGE]` |
| `abs(bool)` | raises `AnalysisException [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` |
| `abs('a' string)` | raises `[CAST_INVALID_INPUT]` (Spark implicitly casts string → double) |
| `cbrt(-8.0/27.0/-1.0)` | `-2.0`, `3.0`, `-1.0` double |
| `cbrt(NULL)` | `None` double |
| `cbrt(int)` | double (`-8`→`-2.0`, `64`→`4.0` exactly) |
| `cbrt(float 27.5)` | `3.018405368398843` **double** — Spark's `cbrt` is a UnaryMathExpression: DoubleType for EVERY accepted input (float32, tinyint, smallint, int, bigint, decimal all measured `double`) |
| `cbrt(-0.0)` | `-0.0` (sign kept) |
| `cbrt(±inf)` | `±inf` |
| `cbrt(decimal(10,0) -8)` | `-2.0` double |
| `cbrt(bool)` | raises `[DATATYPE_MISMATCH]` |
| `cbrt('a' string)` | raises `[CAST_INVALID_INPUT]` (implicit cast → double) |

### C-002 base-tree repark answers (pre-fix, for contrast)

`abs(int32/int64 min)` raises `[ARITHMETIC_OVERFLOW]` (repark's checked `0 - c` path);
`abs(tinyint)`/smallint **widen to int32** and min values return +128/+32768 without an
error (Spark: same type, error at min); `abs(decimal(10,3))` returns `decimal128(14,3)`
(Spark keeps (10,3)); `cbrt(int 64)` returns `3.9999999999999996` (Spark `4.0`);
`cbrt(-0.0)` loses the sign (Spark `-0.0`); `cbrt(decimal)` returns decimal (Spark double).
The native lowering fixes every one of these cells except the int-min error *class*
(DataFusion `checked_abs` surfaces `Int64Array overflow on abs(...)`, still an execution
error — pinned as-is) and the decimal38-min cell (repark's createDataFrame decimal
envelope already refuses |value| ≥ 10^20 — out of reach).

Door-parity finding (measured, recorded not fixed): once `F.abs` resolves a real kernel,
`door_parity_tests::every_registered_name_the_facade_reaches_resolves_the_same_kernel`
sees that the SQL door's `abs` is datafusion-spark `SparkAbs`, which reads
`execution.enable_ansi_mode` — repark never sets it, so the door *wraps* on every signed
minimum (Spark ANSI-on raises `ARITHMETIC_OVERFLOW`). Measured on typed columns and
CASTs through `spark.sql`: `abs(x)` over tinyint -128 → -128, smallint -32768 → -32768,
int -2147483648 → -2147483648, bigint -9223372036854775808 → -9223372036854775808;
`abs(CAST(-2147483648 AS INT))` → -2147483648; the untyped literal
`abs(-2147483648)` → -2147483648 too (repark's spark dialect types the literal Int32,
not Int64). The facade's core `checked_abs` raises (same as the old `0 - c` path). The
divergence is pre-existing — the facade previously composed a CASE, so the ratchet could
not see it — and `("abs", Kernel(1), …)` is added to `EXPECTED_DIVERGENCES` with the
reason (table 21 → 22); `test_abs_door_parity_integer_min` value-pins both doors.
Wiring `spark.sql.ansi.enabled` to `execution.enable_ansi_mode` is a door-side fix for
its own card. `cbrt` and `nullif` resolve identically on both doors (datafusion-spark
ships neither kernel, so the door falls back to the same core UDFs).

P2 remediation (post-review, 2026-09-13): (a) `F.cbrt` — DataFusion's `cbrt` returns
Float32 for a Float32 input, but Spark's `cbrt` is a UnaryMathExpression that always
returns DoubleType; the arm now wraps the argument as `arg * lit(1.0f64)`, which coerces
every numeric input to f64 before the call (f32 value `3.018405368398843` matches the
oracle bit-for-bit) while bool/string still refuse at analysis (Boolean × Float64 does
not coerce). Schema pinned double for float32/tinyint/int32/int64/decimal in
`test_cbrt_returns_double_for_every_numeric_input` — red first on the branch
(`x float → float`). Casting the argument instead was rejected: `CAST(bool AS double)`
is legal in DataFusion and would drop the refusal. (b) The abs door divergence is now
value-pinned, not narrated — `test_abs_door_parity_integer_min` asserts the facade
raises while `SELECT abs(x) FROM v` returns the wrapped minimum on all four signed
widths, and the EXPECTED_DIVERGENCES reason restates the measured values.

### C-003 red-first

Base tree `4f121ab9` (debug native), `pytest test_abs_expr_1.py::test_abs_chain_depth40_memory_linear`:

```
E   AssertionError: F.abs depth-40 crossed the bound 67108864 B (2x flat F.sqrt delta
E   2830336 B, floor 67108864 B): 10 at delta 71380992 B;
E   assert 2 == 0
FAILED python/repark/tests/test_abs_expr_1.py::test_abs_chain_depth40_memory_linear
```

The worker exits `rc=2` at the bound-crossing (level 10, 71 MB) — no abort needed to red it;
uncapped the same chain aborts between depth 11–12 on this debug build (release measured
705 MB at 12, 5 742 MB at 14 then `memory allocation of 112 bytes failed` / SIGABRT).
After the lowering, the same run is green: depth-40 RSS deltas sqrt 2.6 MB, abs 1.4 MB,
cbrt 2.6 MB, nullif 2.4 MB — all flat, all under the 64 MB floor bound.

### C-004 `when(...)` audit — facade rewrites that embed their child more than once

Base-tree depth-12 RSS delta (debug native, `RLIMIT_AS` 8 GB unless noted; the audit
worker sets the cap after session warmup because the lazy numpy/pyarrow import needs the
address space a session already reserved).

| Rewrite | Site | Embeds per level | Base depth-12 | Post-fix | Disposition |
|---|---|---|---|---|---|
| `F.abs` | `functions.py` `abs` | 3× | abort (SIGABRT) at ~11–12; release 705 MB @12 → 5 742 MB @14 | 1.4 MB @40, linear | lowered to `expr_fn::abs` — this unit |
| `F.cbrt` | `functions_expr.py` `cbrt` | 3× | `PanicException: PyObject pointer is null` inside `case_when` at ~5–8 under the cap (release: 444 MB @10) | 2.6 MB @40, linear | lowered to `expr_fn::cbrt` — this unit |
| `F.nullif` | `functions_expr.py` `nullif` | 2× | same panic at ~9–10 under the cap | 2.4 MB @40, linear | lowered to `expr_fn::nullif` — this unit (`nullifzero` inherits) |
| `F.array_append` / `F.array_prepend` (`_glue_element`) | `functions_collections.py:120` | 2× | 19 MB @12, ~×2.2/level (66 MB @14) | unchanged | NOT rewritten — DataFusion `array_append`/`array_prepend` drop the input's null buffer (NULL array → `[x]`, not NULL); Spark's answer is NULL. No one-call native answer → needs its own card |
| `DataFrame.replace` dict loop | `dataframe/core.py:2513` | 2× per mapping entry | 28 MB @12-entry dict, ~×3.3/entry (297 MB @16) | unchanged | audited only — the file is fenced to another lane; a dict of N entries nests `expression` 2^N → needs its own card |
| `F.nvl2` | `functions_expr.py` `nvl2` | 1× per arg | 90 KB @40 — linear | — | no action |
| `F.count_if` | `functions_agg.py:32` | 1× | 94 KB @40 — linear | — | no action |
| `polars.null_count` | `polars.py:288` | 1× | — | — | linear; not user-chainable |
| `reader_support` failed-count | `session/reader_support.py:407` | 1× | — | — | linear; internal |
| `reader.py` `nullValue` | `session/reader.py:747` | 2× but the child is a leaf `F.col(name)` per call | — | — | leaf child each call — cannot compound |

Two adjacent findings filed for their own cards, not fixed here: (a) under address-space
pressure `case_when` dies as `PanicException: PyObject pointer is null` (a pyo3 panic on a
NULL Python object) rather than a catchable `MemoryError` — the `when()` sites panic before
they abort; (b) a linear-but-deep chain (sqrt/nvl2/count_if at depth ≥30) SIGSEGVs at
`select` under an 8 GB AS cap on the debug native — the cap stops the planner's stack from
growing; both legs collect fine at depth 40 uncapped and under a 12 GB cap.

### C-005 existing-pin sweep

- `pytest python/repark/tests -q -k "abs or cbrt or functions_a or functions_b"` →
  **121 passed** (covers `sum(abs(x))` naming, `log(abs(col))`, `array_prepend`,
  `nullif`/`nullifzero`/`nvl2`, the `cbrt` cells in `test_string_numeric_functions.py`,
  and both `test_examples_functions_*.py` example sweeps).
- Whole facade suite `pytest python/repark/tests -q` → **6028 passed, 359 skipped**
  (incl. `test_facade_polish.py`, `test_g4b_semi_join.py`, `test_h2_group_h2.py`,
  `test_select_naming.py`, `test_select_global_agg.py`, `test_fnp15_16_declared_refuse.py`,
  `test_integer_overflow_parity.py` — all green unchanged).
- `scripts/check_example_coverage.py --require-execute` → exit 0 (923 public names,
  809 covered, 216 examples executed) — no example relies on the old composed tree; the
  `functions_*.py` examples now exercise the native arm through `call_scalar`
  (`abs(c)`, `cbrt(c)`, `nullif(c, 1)` display unchanged).
- No §7 registry row covers the cells that changed (the old abs int8→int32 widening and
  `nullif`'s `when` coercion were never pinned there; `F.abs` was never in
  EXPECTED_DIVERGENCES). `docs/design/spark-function-parity.md` §4.4 trued up: `abs`,
  `cbrt`, `nullif` removed from `PY_COMPOSED` (count 45→42) and the stale "not an
  engine abs" callout replaced by a dated ABS-EXPR-1 note.
- File-size baselines ratcheted DOWN only: `functions.py` 1985→1962 and
  `functions_expr.py` 2255→2247 (the composed bodies were deleted); the mirror row in
  `test_cap_1_source_file_line_cap.py` moved in lockstep. `function_dispatch.rs` lands at
  exactly 1000 — the `concat_ws` arm's two temporary `let` bindings inlined to keep the
  table under the default ceiling without touching the exception table.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: abs-expr-1
  complete: true
  reattested: [AT-1, AT-2, AT-6, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-005 walked one by one against behavior — the oracle cells were measured on live PySpark 4.1.2, the lowering verified against them, the memory pin run red-first on the base tree, the when() audit measured per site, and the existing-pin sweep re-run; every clause is PROVEN and cited from the maps.
      artifacts: [task/ledgers/staging/abs-expr-1-ledger.md, python/repark/tests/test_abs_expr_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary cells actually exercised — NULL, int32/int64/int8/int16 minimums, -0.0, decimal(10,3) and decimal(10,0), float32, boolean, uncastable string, and chain depths 1..40; the answer pins assert Arrow value AND type per cell.
      artifacts: [python/repark/tests/test_abs_expr_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Integer-min raises PySparkException with 'overflow' on all four signed widths; bool and string refuse with AnalysisException; the memory worker exits cleanly (rc 2) at the bound-crossing instead of aborting — the base-tree red was the bound exit at level 10, not an OOM.
      artifacts: [python/repark/tests/test_abs_expr_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The _scalar path carries _thread_origin, aggregate, foldable and ungroupable flags unchanged; the join-origin and naming pins stayed green in the facade suite, and each pin session is created and stopped inside its own fixture while the memory legs run in isolated subprocesses.
      artifacts: [python/repark/tests/test_g4b_semi_join.py, python/repark/tests/test_abs_expr_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no injection or deserialization surface — the dispatch arms are literal name matches over a closed table.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per oracle cell; display names, sql_expr and join_sql fragments render identically through call_scalar; the door-kernel divergence the lowering exposed is recorded in EXPECTED_DIVERGENCES, not papered over.
      artifacts: [python/repark/tests/test_abs_expr_1.py, crates/repark-python/src/column/door_parity_tests.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The unit is the AT-7 fix — the exponential facade rewrite is replaced by one native call each; the depth-40 pin measures RSS deltas in a capped subprocess (abs 1.4 MB, cbrt 2.6 MB, nullif 2.4 MB vs the 64 MB bound), and the audit measured depth-12 memory per remaining when() rewrite (array_append ~x2.2/level, replace ~x3.3/entry — both filed for their own cards).
      artifacts: [python/repark/tests/test_abs_expr_1.py, task/ledgers/staging/abs-expr-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1.0 expr_fn signatures verified against the crate source; the door-parity ratchet held (abs recorded in EXPECTED_DIVERGENCES 21 -> 22, cbrt/nullif resolve the same kernel on both doors); file-size baselines ratcheted DOWN only (functions.py 1985 -> 1962, functions_expr.py 2255 -> 2247, CAP-1 mirror in lockstep, function_dispatch.rs at exactly 1000 with no exception raised).
      artifacts: [crates/repark-python/src/column/function_dispatch.rs, crates/repark-python/src/column/door_parity_tests.rs, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change; every failure path raises the same typed exception family as before (PySparkException / AnalysisException), and the cap-pressure panic/segv observations are recorded in C-004 for their own cards.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — the depth-40 memory pin and the answer pins failed on the base tree (bound crossed at level 10; int8 widening, tinyint-min silence, cbrt inexactness all red) and pass after the lowering; the bound-exit branch has a named input (the base tree at depth 10) so no dead branch ships.
      artifacts: [python/repark/tests/test_abs_expr_1.py, task/ledgers/staging/abs-expr-1-ledger.md]
```

## Review findings

```yaml
FINDING:
  id: F-ABS-EXPR-1-001
  severity: S2
  category: AT-1
  clause: C-002
  claim: F.cbrt returned Float32 for a Float32 input; Spark's cbrt is a UnaryMathExpression and returns DoubleType for every accepted input.
  evidence: measured facade schema 'x float -> float' vs live PySpark 4.1.2 'x float -> struct<c:double>'; value divergence too (3.0184054374694824 vs 3.018405368398843)
  disposition: REMEDIATED (the arm wraps the argument as arg * lit(1.0f64) so every numeric input coerces to f64 before the call; bool/string still refuse since Boolean x Float64 does not coerce — pinned by test_cbrt_returns_double_for_every_numeric_input and test_cbrt_non_numeric_refuses in python/repark/tests/test_abs_expr_1.py)
```

```yaml
FINDING:
  id: F-ABS-EXPR-1-002
  severity: S2
  category: AT-10
  clause: C-002
  claim: The abs door divergence was narrated from an untyped-literal probe, not value-pinned, and the probe could not have exercised the Int32 wrap path the way it was described.
  evidence: EXPECTED_DIVERGENCES 'abs' reason row; measured on typed columns — spark.sql 'SELECT abs(x) FROM v' returns the input minimum unchanged on tinyint/smallint/int/bigint and abs(CAST(-2147483648 AS INT)) -> -2147483648
  disposition: REMEDIATED (the reason restates the typed-column measurements, and test_abs_door_parity_integer_min in python/repark/tests/test_abs_expr_1.py value-pins both doors — facade raises PySparkException overflow, door returns the wrapped minimum)
```

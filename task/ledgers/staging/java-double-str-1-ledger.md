# Unit ledger — JAVA-DOUBLE-STR-1 · DOUBLE/FLOAT stringify as Java does

**Date:** 2026-09-15 · **Branch:** `feat/java-double-str-1` · **Base:** `462c1eaf`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Registry BL-7: the engine's Arrow `float64 → utf8` cast answers Rust/ryu
text (`inf`, `1e21`, `10000000.0`) where Spark answers `java.lang.Double.toString` /
`Float.toString` (`Infinity`, `1.0E21`, `1.0E7`). A Java-shaped formatter already
exists (`crates/repark-functions/src/json/reader.rs::java_double_text` /
`java_float_text`, used by `to_json`). Card decisions (G-2): D-1 move both formatters
to a shared module in `crates/repark-functions` (JSON imports from there); D-2 rewrite
`CAST(<Float64|Float32> AS Utf8|Utf8View|LargeUtf8)` to a Spark stringify UDF on the
Spark analyzer-rule path so SQL, `selectExpr` and `col.cast` all pass through it,
native ANSI `repark.sql()` keeps Arrow text (ADR-0002); D-3 the owned `bit_length` /
`octet_length` kernels stringify with the same formatter; D-4 BL-7 → FIXED 2026-09-15
and the gt1 pin flips to equality.

**Oracle (no JVM this round):** `fixtures-batch1.json` ids `BL7-*` (17 double cells +
`BL7-float`), `fixtures-batch4.json` ids `JD-*` (`JD-cast-0..14`, `JD-float-0..5`,
`JD-api-cast`, `JD-api-selectExpr`, `JD-api-float` + literal-type probes).
`repark-batch1.json` is main's pre-change answer for BL7. Live PySpark 4.1.2 values are
quoted from those fixtures, not re-measured.

**Not in this round:** `STATUS.md`, `briefs/next-sequence.md`, any `Cargo.toml` /
`Cargo.lock` / `pyproject.toml` / `uv.lock` / `.github/` change, anything under
`python/repark/src/repark/functions*.py`, `dataframe/**`, `column.py`, `session/**`.
Pin inputs use `CAST(<text> AS DOUBLE|FLOAT)` or DataFrame columns, never a bare
exponent literal (FNP-4B, not yet on main, makes exponent literals DOUBLE on the Spark
door; today they are DECIMAL). `lib.rs` registration hunks stay one added line each
(door-converge-1 and fnp-6d edit `crates/repark-functions` tonight).

## PROPOSITION LEDGER — JAVA-DOUBLE-STR-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Measurement census: every Spark-door and facade path that reaches Arrow's float→utf8 cast is named with the RePark answer per JD/BL7 cell on base `462c1eaf`. | Census section below, measured with the step-1 native build. | **PROVEN** | Census below (2026-09-15): SQL CAST, concat coercion, col.cast, selectExpr, length kernels all answer Arrow text; facade identical; native identical (kept by D-2). |
| C-002 | Red pins both doors land in `python/repark/tests/test_java_double_str_1.py` and FAIL on the base tree; the gt1 pin flips to equality and fails with it. | Red pytest output pasted in evidence; green after step 2. | **PROVEN** | 5 failed / 2 passed 2026-09-15: `assert ['inf'] == ['Infinity']`, `assert ['10000000000.0'] == ['1.0E10']`, `assert ['10000000.0'] == ['1.0E7']`, `assert [3] == [8]`; gt1 flip: `assert ['inf'] == ['Infinity']`. The 2 passes are guards that hold on both trees (neg-zero paths already agree; native keeps Arrow by D-2). |
| C-003 | D-1: `java_double_text` / `java_float_text` live in one shared module in `crates/repark-functions`; the JSON code imports from there; no second formatter exists. | `grep` shows exactly one definition site; `cargo test -p repark-functions` green. | **PROVEN** | `crates/repark-functions/src/java_double.rs` holds both; `json/reader.rs`, `decode.rs`, `to_json.rs` import from there; `grep java_double_text\|java_float_text` finds no other definition. `cargo test -p repark-functions --lib`: 463 passed. |
| C-004 | D-2: on the Spark door and the facade every `CAST(<Float64\|Float32> AS Utf8\|Utf8View\|LargeUtf8)` answers Java text (SQL, `selectExpr`, `col.cast`), value AND type; native `repark.sql()` keeps Arrow text. | Step-1 pins green on Spark door + facade; native guard still green. | **PROVEN** | `SparkFloatToStringCast` in `analyzer_rules()` after `SparkExprSemantics`; `test_java_double_str_1.py` CAST/neg-zero/float/facade pins + flipped gt1 pin green (8 passed 2026-09-15); native guard still `inf`/`10000000.0`. `lib.rs` stays at its 175 ceiling (two one-line registration hunks paid by condensing the `approx_percentile_cont` binding 5→3). |
| C-005 | Shape rule, concat arm: SQL `concat` implicit coercion and `F.concat` answer Java text for DOUBLE/FLOAT inputs, value AND type. | Concat pins green both doors. | **PROVEN** | `SparkConcat` coercion keeps `Float32`/`Float64`; the kernel formats via the shared module (arrays and scalars). Concat pins green both doors, typed `string`. |
| C-006 | D-3: the owned `bit_length` / `octet_length` kernels stringify DOUBLE/FLOAT with the shared formatter. | Length pins green, incl. float `1.0E10` → 6. | **PROVEN** | Coercion keeps `Float32`/`Float64`; `byte_lengths` counts shared-formatter bytes. Length pins green incl. float `1.0E10` → 6, typed `int32`. |
| C-007 | D-4: registry BL-7 reads FIXED 2026-09-15 (JAVA-DOUBLE-STR-1); the gt1 pin asserts equality. | Registry section + green gt1 pin. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` BL-7 rewritten as FIXED 2026-09-15 with the new pins; flipped gt1 pin green. |
| C-008 | Gates: `cargo test -p repark-functions`, clippy/fmt, full facade suite, parity harness green; COVERAGE_ATTESTATION filed. | Commands and counts in evidence. | **PROVEN** | `cargo test -p repark-functions`: 463 passed, 0 failed, 1 ignored. `make rust-fmt-check` + `make rust-clippy` (`--locked --workspace --all-targets -D warnings`): clean. `make ci`: green end to end. `make py-test-facade`: 6681 passed, 373 skipped, 1 failed — the failure is the FNP10 pin this unit necessarily converges (shared formatter, D-1); after the lockstep flip the json + unit files run 134 passed. `make py-test`: 757 passed, 2 skipped, 12 xfailed. Attestation below. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: java-double-str-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked card decisions D-1 through D-4 plus the shape rule clause by
        clause against the ledger: shared module, CAST rewrite with native
        kept, concat arm, length kernels, registry row. Every clause carries a
        value-AND-type pin on the Arrow path.
      artifacts: [python/repark/tests/test_java_double_str_1.py, python/repark/tests/test_functions_gt1.py::test_sql_door_double_infinity_stringify_is_named_divergence]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Exercised inf, -inf, NaN, signed zero by both spellings, the E-notation
        thresholds either side (1.0E-4 vs 0.001, 1.0E-3, 1.0E7 vs 1.0E6),
        1.0E20/1.0E21, 3.4E38, max-double 1.7976931348623157E308, min-subnormal
        4.9E-324, the 0.1+0.2 long tail, float edges 1.0E10/0.1/123456.7/1.0E-5,
        and NULL through CAST, concat and octet_length. Step-4 hardening added
        the NULL pin and the float -inf/NaN/neg-zero rows (green on arrival,
        mechanism-identical to red-first cells).
      artifacts: [python/repark/tests/test_java_double_str_1.py::test_spark_door_null_float_stringify_is_null, crates/repark-functions/src/java_double.rs::tests]
    - id: AT-3
      status: ATTACKED
      evidence: >
        The rule's passthrough arms (non-float source, non-string target,
        unresolvable type) ran unchanged under the whole green suite, so no
        adjacent CAST changes shape; the UDF's wrong-type arm returns a plan
        error naming the UDF instead of misformatting.
      artifacts: [make py-test-facade 6681 passed, crates/repark-functions/src/java_double.rs return_type]
    - id: AT-4
      status: N/A
      justification: >
        The formatter, the UDF and the analyzer rule are stateless pure
        functions of their input; no shared mutable state, no ordering
        assumption, no cross-row communication exists to race.
    - id: AT-5
      status: N/A
      justification: >
        No privileged action, no credential, no secret, no deserialized
        untrusted structure: the formatter reads f64/f32 values, the rule
        matches plan-node types. SQL text parsing stays with the engine.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Native-door Arrow text pinned unchanged by the guard pin; to_json
        MIN_VALUE convergence recorded with the FNP10 row note and pin flip;
        no catalog, storage or wire format touched.
      artifacts: [python/repark/tests/test_java_double_str_1.py::test_native_door_keeps_arrow_spelling, docs/spark-sql-iceberg-parity.md FNP10-JAVA-DOUBLE-TEXT-1]
    - id: AT-7
      status: N/A
      justification: >
        Per-row O(digits) formatting into sibling-sized StringBuilders; the
        concat and length kernels already allocated equivalent Utf8 buffers on
        this path, so no new growth, loop or leak class is introduced.
    - id: AT-8
      status: ATTACKED
      evidence: >
        No dependency change (Cargo.toml, Cargo.lock, uv.lock untouched);
        output Arrow contracts hold (Utf8 strings, int32 lengths) per the type
        asserts; no public Python name added or altered.
      artifacts: [git diff --stat steps 1-4, type asserts in test_java_double_str_1.py]
    - id: AT-9
      status: N/A
      justification: >
        Value formatting has no failure alarm surface; the only new error (UDF
        wrong-type plan refusal) travels the existing engine error path with
        the UDF name in the message.
    - id: AT-10
      status: ATTACKED
      evidence: >
        Red-first: step-1 pins failed on base with the Arrow spellings pasted
        in C-002, then passed unmodified after step 2. Bite history: the
        MIN_VALUE branch is load-bearing (without it the 4.9E-324 pin answers
        5.0E-324); every rule, coerce and kernel branch names a pinned input
        (float32 vs float64 rows, null rows, scalar F.lit concat arms,
        non-float passthrough via the full suite).
      artifacts: [ledger C-002 red output, 8 passed run 2026-09-15, 134 passed re-run]
  reattested: [AT-1, AT-6, AT-10]
  complete: true
```

## Census — paths reaching Arrow float→utf8 on base (C-001, measured 2026-09-15)

Native module built from base (`uvx maturin@1.14.1 develop --release`,
`repark.__file__` under `/tmp/pc-jdouble`). `CAST(x AS STRING)` answers Arrow
`string` (`DataType(string)`); length kernels answer `int32`.

| Path | Probe input | RePark base answer | Spark oracle |
|---|---|---|---|
| SQL CAST double | `CAST(CAST('Infinity' AS DOUBLE) AS STRING)` | `'inf'` | `'Infinity'` (BL7-0) |
| SQL CAST double | `CAST(CAST('1.0E7' AS DOUBLE) AS STRING)` | `'10000000.0'` | `'1.0E7'` (BL7-5) |
| SQL CAST double | `CAST(CAST('1.0E6' AS DOUBLE) AS STRING)` | `'1000000.0'` | `'1000000.0'` (BL7-6, agrees) |
| SQL CAST double | `CAST(CAST('1.0E21' AS DOUBLE) AS STRING)` | `'1e21'` | `'1.0E21'` (BL7-3) |
| SQL CAST double | `CAST(CAST('1.0E-4' AS DOUBLE) AS STRING)` | `'0.0001'` | `'1.0E-4'` (BL7-9) |
| SQL CAST double | `CAST(CAST('-0.0' AS DOUBLE) AS STRING)` | `'-0.0'` | `'-0.0'` (JD-cast-7, agrees) |
| SQL CAST double | `CAST(CAST(-0.0 AS DOUBLE) AS STRING)` | `'0.0'` | `'0.0'` (BL7-10, agrees: decimal `-0.0` is positive zero) |
| SQL CAST float | `CAST(CAST('1.0E10' AS FLOAT) AS STRING)` | `'10000000000.0'` | `'1.0E10'` (BL7-float) |
| SQL concat coerce | `concat(CAST('1.0E7' AS DOUBLE), '')` | `'10000000.0'` | `'1.0E7'` (JD-cast-0) |
| SQL concat coerce | `concat(CAST('NaN' AS DOUBLE), '')` | `'NaN'` | `'NaN'` (agrees) |
| Facade col.cast | `F.lit(inf).cast('string')` | `'inf'` | `'Infinity'` (JD-api-cast) |
| Facade concat | `F.concat(F.lit(1.0e7), F.lit(''))` | `'10000000.0'` | `'1.0E7'` |
| Length kernels | `octet_length(CAST('Infinity' AS DOUBLE))` | `3` | `8` (BL7-0) |
| Length kernels | `bit_length(CAST('Infinity' AS DOUBLE))` | `24` | `64` (BL7-0) |
| Length kernels | `octet_length(CAST('1.0E7' AS DOUBLE))` | `10` | `5` (BL7-5) |
| Native door | `repark.sql CAST Infinity AS STRING` | `'inf'` | kept by D-2 (ADR-0002) |

## Step-2 finding — `Double.MIN_VALUE` spells `4.9E-324`

`CAST('4.9E-324' AS DOUBLE)` parses to the min subnormal; Rust shortest prints
`5e-324` but the oracle (BL7-16, JD-cast-13) answers Java `4.9E-324` — Java's
`FloatingDecimal` spells `Double.MIN_VALUE` longhand (both spellings round-trip).
The shared formatter matches that exact bit pattern (1 / `0x8000_0000_0000_0001`);
every other BL7/JD oracle cell agrees with shortest-plus-thresholds. The FNP10 oracle
(`FNP10-JAVA-DOUBLE-TEXT-1`) records three further cells where JDK longest wins over
shortest (`8.41E21` → `8.409999999999999E21`, `1.0E23` → `9.999999999999999E22`, FLOAT
`1.4E-45` → `1.4E-45`): those stay BACKLOG per that row's ruling (full closure means
porting `FloatingDecimal`), and the Spark-door CAST path shares that residual — D-1
(one formatter) and the card's oracle leave no other consistent choice. No oracle cell
covers `Float.MIN_VALUE` beyond FNP10's divergent `1.0E-45`, so the float path is
untouched there.

Step-4 lockstep fallout of the shared formatter: `to_json` of `Double.MIN_VALUE` now
renders `4.9E-324` (= Spark), so the FNP10 pin's first case flipped to equality and that
row carries a dated convergence note; the other three cases still pin the divergence.
The pin file sits outside the card fence — the flip is forced by D-1 + the full-suite
gate, recorded here and in the handback for orchestrator review.

Out of scope observed: `createDataFrame` refuses non-finite floats
(`PySparkTypeError: createDataFrame does not support infinite float values`), so the
facade pins build inf-bearing columns through SQL `VALUES` instead; that refusal is
pre-existing behavior, not this unit's claim.

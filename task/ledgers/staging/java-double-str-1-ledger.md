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
| C-007 | D-4: registry BL-7 reads FIXED 2026-09-15 (JAVA-DOUBLE-STR-1); the gt1 pin asserts equality. | Registry section + green gt1 pin. | OPEN | Pin flipped red in step 1; registry edit lands step 3. |
| C-008 | Gates: `cargo test -p repark-functions`, clippy/fmt, full facade suite, parity harness green; COVERAGE_ATTESTATION filed. | Commands and counts in evidence. | OPEN | Awaits step 4. |

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
every other oracle cell agrees with shortest-plus-thresholds. No oracle cell covers
`Float.MIN_VALUE` (`1.4E-45`), so the float path is untouched there.

Out of scope observed: `createDataFrame` refuses non-finite floats
(`PySparkTypeError: createDataFrame does not support infinite float values`), so the
facade pins build inf-bearing columns through SQL `VALUES` instead; that refusal is
pre-existing behavior, not this unit's claim.

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
## Round-2 proposition ledger — 2026-09-15 (run 15c follow-up, oracle `fixtures-batch10.json`)

Base answers below were measured on the rebased tree before the round-2 edits
(`.venv` release native, 2026-09-15): TRY_CAST `10000000.0/inf/0.001`,
array_join `10000000,0.1,0.00001` / float `10000000000,0.1`, format `%s`
`10000000/0.1/0.00001`, float-min `1.0E-45`, LIKE planning error, CASE answering
`x`/`1.0E7`, coalesce Arrow cast error, fd cells `8.41E21`/`1.0E23` divergent and
`0.002`/`4.9E-324`/`2.2250738585072014E-308`/`9.007199254740992E15`/`1.1` equal.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-009 | L-001: `TRY_CAST(<double> AS STRING)` and `Column.try_cast("string")` answer Java text (`1.0E7`, `Infinity`, `0.001`), NULL stays NULL. | J10 pins green, value AND type, SQL + facade. | **PROVEN** | Rule extended to `TryCast`; `try_cast` routes through the Rust `Column` binding (no Python facade change — no HALT). Pins green 2026-09-15. |
| C-010 | L-002: `array_join` over `array<double>` / `array<float>` joins Java text (SQL + `F.array_join`). | J10 pins green, value AND type. | **PROVEN** | Owned `__repark_array_join__` shim, rule-built only for float-element lists (facade dispatches past the registry, so a registry overwrite could not cover it); string/other leaves mirror upstream. Pins green. |
| C-011 | L-003: `format_string`/`printf` `%s` renders Java text; `%f`/`%.2f` pin today's answers into BACKLOG. | J10 pins green; `%f` verdict recorded. | **PROVEN** | `%s`-only formats pre-cast float args to the Java UDF (unconditional pre-cast would regress `%f`-of-string, which errors); `%f`=`10000000.000000`, `%.2f`(0.125)=`0.12` pinned as today's answer in JAVA-DOUBLE-FD-1 (Java HALF_UP `0.13` is a one-line change to nothing — full `Formatter` parity is the BACKLOG). |
| C-012 | L-005: float `1.4E-45` and `3.4028235E38` stringify exactly. | J10 pin green. | **PROVEN** | Exact `f32::MIN_VALUE` bit-pattern match in the shared formatter (J10-float-min measured, no guessing); MAX already shortest-exact. Pin green. |
| C-013 | L-006: every listed coercion shape matches its cell. | J10 pins green per shape. | **PROVEN** | Already-equal shapes pinned as-is (cmp, IN, concat_ws via the post-seat dividend, VARCHAR/CHAR, decimal, to_json, neg-zero). LIKE wraps float sides pre-coercion (one shared-insertion slot — post-coercion never sees it). CASE steers string branches to DOUBLE pre-coercion; bad string literals fold to `CAST_INVALID_INPUT` (ANSI on) or NULL; column-driven malformed casts keep the Arrow error (pre-existing class, test_cast_failure_parity posture unchanged). The pre-coercion seat is load-bearing (reverted 2026-09-15: the LIKE and CASE/coalesce pins go red), so it lives in the shared HOF-preparation helper and the FNP-8 contract test now pins both seats (`analyzer_configuration_seats_hof_preparation_and_float_stringify_before_type_coercion`). Eager folding of un-taken literal branches is the known residual. |
| C-014 | L-004: R-11 path recorded — BACKLOG row JAVA-DOUBLE-FD-1 plus pins for exactly the fd cells. | Registry row + pins green. | **PROVEN** | BACKLOG path taken (a faithful `FloatingDecimal` port does not fit this round with every pin green); equal cells pin equality, divergent cells pin today's answers. |
| C-015 | P2-1: stack-buffer formatter, no per-value `String`; before/after reported on the 5M-row query. | Best-of-3 numbers in evidence. | **PROVEN** | `with_java_double/float_text` compose into caller stack buffers; `NaN`/`Infinity`/min-subnormals are static strings; `SELECT octet_length(CAST(CAST(value AS DOUBLE) / 7 AS STRING)) FROM range(5000000)`, release, best of 3: reviewer-before 2.31x → after 1.54x (spark 0.069s vs native 0.045s; per-round spark [0.072, 0.069, 0.078], native [0.045, 0.046, 0.055]). |
| C-016 | P2-2: length kernels compute from the stack formatter without allocating. | Length pins green; no `StringArray`. | **PROVEN** | `byte_lengths` float arms sum `java_*_text_len` per row; `text_len_matches_text_bytes` unit pin ties len to text. P3-1/P3-2 not taken (optional). |
| C-008 | Gates: `cargo test -p repark-functions`, clippy/fmt, full facade suite, parity harness green; COVERAGE_ATTESTATION filed. | Commands and counts in evidence. | **PROVEN** | Round 2 (2026-09-15, private `CARGO_TARGET_DIR=/tmp/pc-jdouble/target` after shared-dir cross-lane ghost errors): `make verify` RC=0; `cargo test -p repark-functions` 468 passed, 1 ignored; `make rust-clippy` clean; `cargo fmt --all --check` clean; size/map/ledger/grammar gates clean; `make py-test-facade` RC=0: 6799 passed, 373 skipped; `make py-test` RC=0: 757 passed, 2 skipped, 12 xfailed; refreshed attestation below. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: java-double-str-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked card decisions D-1 through D-4 plus the shape rule clause by
        clause against the ledger, then every round-2 item L-001/L-002/L-003/
        L-005/L-006/L-004 and P2-1/P2-2 the same way: TRY_CAST, array_join,
        format_string %s, float min/max, each coercion shape, the FD-1 ruling
        path, the stack formatter. Every clause carries a value-AND-type pin
        on the Arrow path against its J10 cell.
      artifacts: [python/repark/tests/test_java_double_str_1.py, python/repark/tests/test_functions_gt1.py::test_sql_door_double_infinity_stringify_is_named_divergence]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Round-1 domain (inf, -inf, NaN, signed zeros, thresholds, 1.0E20/1.0E21,
        3.4E38, max-double, min-subnormal, long tails, float edges, NULL)
        plus round-2: TRY_CAST nullability, array_join null sketches via the
        shim's null arms, format %s/%f verb split, float MIN_VALUE and MAX,
        LIKE/VARCHAR/CHAR shapes, CASE/coalesce mixes (ANSI-on raise and the
        literal fold's empty/whitespace repairs), decimal/to_json/neg-zero
        guards, fd equality cells, and the FD-1 backlog cells. The `%f`
        HALF_UP gap and the two JDK-longhand cells are pinned as today's
        answers, not absorbed.
      artifacts: [python/repark/tests/test_java_double_str_1.py, crates/repark-functions/src/java_double.rs::tests]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Every new arm fails open to today's behavior: non-float sources,
        non-string targets, unresolvable types, non-literal formats, formats
        with non-%s verbs, non-list and non-float-element joins, non-mixed
        CASE/coalesce, and Arrow-accepted literals all pass through untouched,
        verified by the whole green suite; the UDF wrong-type arms and the
        bad-literal fold return plan errors naming the site instead of
        misformatting.
      artifacts: [make py-test-facade 6799 passed, crates/repark-functions/src/java_double.rs return_type]
    - id: AT-4
      status: N/A
      justification: >
        The formatter, the UDFs, the shim and the analyzer rule are stateless
        pure functions of their input; no shared mutable state, no ordering
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
        Native-door Arrow text pinned unchanged by the guard pin; both FNP10
        min-subnormal convergences recorded with row notes and lockstep pin
        flips; the FD-1 row carries the remaining divergence; no catalog,
        storage or wire format touched.
      artifacts: [python/repark/tests/test_java_double_str_1.py::test_native_door_keeps_arrow_spelling, docs/spark-sql-iceberg-parity.md FNP10-JAVA-DOUBLE-TEXT-1, docs/spark-sql-iceberg-parity.md JAVA-DOUBLE-FD-1]
    - id: AT-7
      status: ATTACKED
      evidence: >
        Best-of-3 on the brief's 5M-row query, release build: reviewer-before
        2.31x to after 1.54x (spark 0.069s vs native 0.045s). No per-value
        heap String remains on the Spark-door path; buffers are fixed-size
        stack arrays with documented bounds.
      artifacts: [ledger C-015, /tmp/perf_jd2.py run 2026-09-15]
    - id: AT-8
      status: ATTACKED
      evidence: >
        No dependency change (Cargo.toml, Cargo.lock, uv.lock untouched);
        output Arrow contracts hold (Utf8 strings, int32 lengths, booleans)
        per the type asserts; no public Python name added or altered;
        try_cast needed no facade change (Rust binding passes Expr through).
      artifacts: [git diff --stat round 2, type asserts in test_java_double_str_1.py]
    - id: AT-9
      status: N/A
      justification: >
        Value formatting has no failure alarm surface; the new errors (UDF
        wrong-type plan refusal, bad-literal CAST_INVALID_INPUT) travel the
        existing engine error path with the site named in the message.
    - id: AT-10
      status: ATTACKED
      evidence: >
        Red-first: step-1 pins failed on base with the Arrow spellings pasted
        in C-002; round-2 base answers were measured on the rebased tree before
        editing and pasted in the round-2 ledger head, then pinned green.
        Bite history: the MIN_VALUE branches (without the f64 one the 4.9E-324
        pin answers 5.0E-324), the safe-cast probe (it masked every rejection
        until the strict probe replaced it), the coalesce fixpoint seat (the
        standalone fold passed while the cell failed until the coalesce arm
        landed); every rule, coerce and kernel branch names a pinned input.
      artifacts: [ledger C-002 red output, round-2 base census, 21 passed run 2026-09-15]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-7, AT-10]
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
(`FNP10-JAVA-DOUBLE-TEXT-1`) records two further cells where JDK longest wins over
shortest (`8.41E21` → `8.409999999999999E21`, `1.0E23` → `9.999999999999999E22`): those
stay BACKLOG per that row's ruling (full closure means porting `FloatingDecimal`), and
the Spark-door CAST path shares that residual — D-1 (one formatter) and the card's
oracle leave no other consistent choice. Round 2 the `1.4E-45` cell converged through
the same bit-pattern match (J10-float-min), which flipped the FNP10 pin's last case
in lockstep.

Step-4 lockstep fallout of the shared formatter: `to_json` of `Double.MIN_VALUE` now
renders `4.9E-324` (= Spark), so the FNP10 pin's first case flipped to equality and that
row carries a dated convergence note; the other two cases still pin the divergence
(the float case converged in round 2 the same way). The pin file sits outside the card
fence — the flips are forced by D-1 + the full-suite gates, recorded here and in the
handback for orchestrator review.

Out of scope observed: `createDataFrame` refuses non-finite floats
(`PySparkTypeError: createDataFrame does not support infinite float values`), so the
facade pins build inf-bearing columns through SQL `VALUES` instead; that refusal is
pre-existing behavior, not this unit's claim.

# DOUBLE-DECIMAL-STORE-ROUND-1 — the built seats, the pins, and the halt

Owner, 2026-11-13, about 20:50 UTC, option A ("defaults"): up to five product files for the label-scope
SQL and DML doors; F.expr's literal fold and `fillna` go to DOUBLE-DECIMAL-DATAFRAME-1, nested leaves to a
card; NaN, ±Infinity and ANSI-off overflow keep today's refusals; JDK 17's text.

## The seats (four product files; the fifth was not needed)

| # | seat | file | what it does |
|---|---|---|---|
| K | the kernel | `crates/repark-common/src/java_double/mod.rs` (`java_double_unscaled`) | the JDK 17 `Double.toString` text of the f64 (a FLOAT widened first by the caller), rounded half up at the target scale, as an `i128`; a binary fast path when the product `10^s·v` is below `2^52` and more than four ulps from a half (the text and the binary value then round alike); `None` for a non-finite value or one past `i128`. |
| R | the plan seat | `crates/repark-functions/src/bool_decimal.rs` | the end-of-list `bool_decimal_cast` rule also rewrites `CAST`, `TRY_CAST` and `arrow_cast(<FLOAT/DOUBLE>, 'Decimal128(p, s)')` into `__repark_float_to_decimal__` over K. `CAST` and `arrow_cast` keep Arrow's refusal text (`Overflowing on NaN`, `… is too large to store in a Decimal128 of precision P`); `TRY_CAST` answers NULL. |
| S | the stage seat | `crates/repark-iceberg/src/write/conform_stage.rs` (`leaf_store`) | at every stage site a FLOAT/DOUBLE leaf whose target is `Decimal128` is cast in the plan, so R rounds it on the execute pass, before the copy-on-write union and before the batch writer. This is what reaches MERGE INSERT, the merge-on-read UPDATE and the copy-on-write UPDATE's FLOAT and 17-digit cells. |
| B | the batch seat | `crates/repark-iceberg/src/write/conform.rs` (`entry_store_cast`, `float_store_decimal`) | a FLOAT/DOUBLE array into `Decimal128` stores K's value with Arrow's refusal texts. |

The `uuid_presentation.rs:134` guard (file 5) was not needed: with S, MERGE INSERT and the merge-on-read
UPDATE pass their pins.

## Pins

- **Rust, at the seats:** `bool_decimal::tests::the_kernel_rounds_the_jdk_17_text_half_up`,
  `the_fast_path_agrees_with_the_text_on_a_dense_sweep` (more than a million values and scales against a
  string half-up of the JDK 17 text), `a_float_cast_try_cast_and_arrow_cast_to_decimal_round_the_text`,
  `a_float_cast_to_decimal_keeps_todays_refusals` (ANSI on, off and unset);
  `conform::tests::a_float_batch_stores_into_decimal_by_the_doubles_text`;
  `conform_stage::tests::every_stage_site_casts_a_float_leaf_to_its_decimal_target_in_the_plan`.
- **Facade:** `python/repark/tests/sql/test_double_decimal_store_round_1.py`, both ANSI modes: 21 doors × 12
  cells (DOUBLE `1.255`, `1.005`, `9.995`, `-1.255`, `1.2549999999999997`, `2.675` at (10,2); two values at
  (38,18); `1e23` at (38,0); FLOAT `1.255`, `2.675` at (10,2) and `1.255` at (38,18)), the card rows, the
  CAST-DOUBLE-TO-DECIMAL-ROUNDING-1 cast cells, and the kept refusals.
  - **Spark 4.1.2 answers every pinned cell** on all 21 doors in both modes (252 of 252 per mode;
    [raw/spark-pins-ansi-true.jsonl](raw/spark-pins-ansi-true.jsonl), runner
    [spark_pins.py](spark_pins.py)).
  - **Red on main** (`69aba5df`, `ddr-pkg-base`): 46 failed, 8 passed (the 8 are the kept-refusal pins).
  - **Green on the head** (`bd639c4e`, `ddr-pkg-head`): 54 passed.
- **The step 0 table on the head:** every one of the 321 WRONG-VALUE cells per mode turns EQUAL; no other
  cell changes (EQUAL 2095, EQUAL-ERR and ERR-FOR-NULL unchanged)
  ([raw/table-head-ansi-true.jsonl](raw/table-head-ansi-true.jsonl)).

## The halt: a value that matches Spark today changes

The critic's coercion fuzz ([coercion_fuzz.py](coercion_fuzz.py)): 1,000 doubles (16- and 17-digit
neighbours of halfway values, short decimals, uniform values) through eight shapes where a DECIMAL meets a
DOUBLE (`=` at two scales, `=` a literal, `coalesce`, `CASE`, `IN`, `BETWEEN`, `UNION`), base, head and
Spark, both modes ([raw/](raw/map.md)).

| ANSI | cells | base ≠ Spark, head = Spark | base = Spark, head ≠ Spark |
|---|---|---|---|
| on | 8,000 | 171 | **1** |
| off | 8,000 | 171 | **1** |

The one cell: `CAST(ROUND(v, 3) AS DECIMAL(10,3)) = v` with `v = 2.6750000000000003`. Spark compares as
DOUBLE and answers `false`. Both RePark builds coerce both sides to `decimal(30,15)` (DataFusion
`binary.rs:1227-1239`, DECIMAL-DOUBLE-UNION-COERCION-1's fault): main's Arrow product
`2.6750000000000003 · 10^15` rounds to `…0000.5` in f64 and then to `2.675000000000001`, so it answers
`false` by accident; the head rounds the text `2.6750000000000003` half up at 15 digits to
`2.675000000000000` and answers `true`. The same coercion's other cells move the other way (171 per mode
become Spark's, e.g. `coalesce(CAST(NULL AS DECIMAL(10,2)), 123.45500000000003D)` now `123.455000000000030`
where main gave `…032`; Spark answers the double), but the brief's halt fires on the one.

## After the owner's 23:59 ruling ("Go with defaults", option A)

- **The accepted cell** is a strict xfail in the facade pin file
  (`test_a_decimal_compared_with_a_double_answers_as_spark`): it fails on the head as recorded (XFAIL) and passes
  on main (XPASS(strict)); DECIMAL-DOUBLE-UNION-COERCION-1 retires it.
- **Pre-existing, not this branch's:** `CAST(v AS STRING)` over a `UNION` of a DOUBLE and a `DECIMAL(10,2)` refuses
  identically on `nwc/pkg-base`, main `69aba5df` and the head, in both ANSI modes (`'__repark_float_to_string__'
  expects a FLOAT or DOUBLE argument, got Decimal128(30, 15)`), whichever branch of the `UNION` comes first when the
  DOUBLE is a column; with the DECIMAL first over literals both answer `0.000000000000000`, `1.255000000000000`
  ([union_string_probe.py](union_string_probe.py), [raw/union-string.txt](raw/union-string.txt)).

## The cost row

10M rows; five alternating base/head pairs; each sample under `flock -x` measure, then opus-cargo and
opus2-cargo; median seconds ([cost.py](cost.py)).

| query | base `69aba5df` | head `bd639c4e` | ratio | head `3e093a45` | ratio |
|---|---|---|---|---|---|
| `SELECT max(CAST(v AS DECIMAL(10,2)))` | 0.0482 | 0.0936 | 1.94 | 0.0679 (base 0.0585) | **1.16** |
| `SELECT max(CAST(v AS DECIMAL(38,18)))` | 0.0613 | 0.2149 | 3.51 | 0.1576 (base 0.0616) | **2.56** |
| `count(*) WHERE d = v` (DECIMAL(16,4) = DOUBLE) | 0.0938 | 0.2391 | 2.55 | 0.1831 (base 0.0913) | **2.01** |
| `INSERT … SELECT` into DECIMAL(10,2) | 0.1531 | 0.1921 | 1.25 | 0.1580 (base 0.1554) | **1.02** |
| `INSERT … SELECT` into DECIMAL(38,18) | 0.3064 | 0.4534 | 1.48 | 0.4048 (base 0.2990) | **1.35** |

`3e093a45` reads the JDK 17 digits straight from the converter (no text layout and parse) and runs both seats as
Arrow unary kernels. **Four of five rows are above the 1.05x bar, so the brief's cost halt fires.** The time is the
JDK 17 digit generation: at scale 18 (and at the `decimal(30,15)` a `decimal = double` comparison coerces to) the
product `10^s·v` is past `2^52` for any `|v| ≥ 0.005`, so every row takes the exact digit path; at scale 2 the binary
fast path covers rows away from a half and the remaining 16% is the per-row check. Raw:
[raw/cost-bd639c4e.jsonl](raw/cost-bd639c4e.jsonl), [raw/cost-3e093a45.jsonl](raw/cost-3e093a45.jsonl).

The finishing run on the merged head `3eed6799` against main `6f9f2aaa` re-measures these rows and adds the
rows without that cast and the C-012 like-for-like set: [cost-finish.md](cost-finish.md). The cast rows' exception is
owner word owed (asked 2026-11-14); see the card's cost section.

Up: [map.md](map.md).

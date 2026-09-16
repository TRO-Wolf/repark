# map — repark-functions/src/analyzer

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

File-backed submodules of `../analyzer.rs` (`SparkExprSemantics`). The rule file itself keeps the
rewrites; anything with its own matrix and its own error idiom lives here so `analyzer.rs` stays
under its `check_rust_file_size` ceiling and each matrix has one home.

## Contents

- `like_escape.rs` — **FN-FIX-2 (2026-09-04):** a LIKE/ILIKE pattern that ends in the
  unconsumed escape char is `DataFusionError::Plan` `[INVALID_FORMAT.ESC_AT_THE_END]`
  SQLSTATE 42601. Foldable literals only. pins: fn-fix-2-string-rows/C-002
  **DOOR-CONVERGE-2b (2026-09-15):** `LIKE … ESCAPE e` / `ILIKE … ESCAPE e` with a
  non-backslash foldable escape rewrite onto the `spark_like` kernels (3-arg
  call); the backslash case keeps DataFusion's `Like`/`ILike` nodes.
  pins: door-converge-2b/C-006
- `date_part.rs` — **DOOR-CONVERGE-2b (2026-09-15):** rewrites the
  planner-embedded `date_part` builtin onto `__repark_date_part__`. EXTRACT
  plans through DataFusion's `DatetimeFunctionPlanner`, which bakes the builtin
  UDF into the node, so only the analyzer sees the name; a named `date_part`
  call resolves through the repark UDF's alias anyway, and the arm is
  idempotent against it. pins: door-converge-2b/C-003
- `subscript.rs` — **DOOR-CONVERGE-2b (2026-09-15):** `rewrite_array_subscript`
  moved here from `analyzer.rs`. Skips the registered refusing `array_element`
  UDF (downcast check — only the planner-embedded DataFusion builtin is
  rewritten) and strips DataFusion's first-pass `CAST(… AS List(item …))`
  coercion on the array operand: the cast target was computed pre-narrowing or
  against DF's `item` field name, and stripping it lets Spark literal widths
  reach `__repark_array_get__`. pins: door-converge-2b/C-004, C-005
- `overlay.rs` — `overlay(..., -1)` drops the Spark default length to the 3-arg form.
  **SQL-LITERAL-TYPING-1 (2026-09-16):** the `-1` match sees through one compiler
  `CAST` (early Int32 narrowing makes the first coercion wrap the literal before
  this rewrite runs; Spark treats every `-1` length as omit).
  pins: sql-literal-typing-1/C-002
- `time_window/` — **FNP-WIN-1 (2026-09-15):** `SparkTimeWindow`, the
  `TimeWindowing` equivalent. Bare `window(...)` group/projection keys take the
  `window` name (explicit aliases win); sliding calls expand one row per
  overlapping bucket through the internal starts UDF plus `Unnest`, so the
  existing GroupedData path carries struct grouping keys untouched; dangling
  parent references to the pre-rename display remap to `window`. Non-literal
  durations and a second window specification per block refuse loud.
  pins: fnp-win-1/C-002, C-005, C-006. Step 3 adds `SparkWindowTimeGrouping`,
  which refuses a `window_time(...)` call placed directly in an aggregate with
  the oracle `[MISSING_AGGREGATION]` text. pins: fnp-win-1/C-005.
  `mod.rs` keeps the window rules; `session_window.rs` (step 4) owns the
  `SparkSessionWindow` sessionize rewrite so each file stays under the
  `check_rust_file_size` ceiling. Month/year gaps refuse loud beside the
  rule. pins: fnp-win-1/C-004, C-006, C-008, C-011. Round 2 (2026-09-15):
  `session_window.rs` chains dynamic gaps on the running maximum of ends.
  pins: fnp-win-1/C-004. The `window_time` provenance walk recurses through
  Filter / Limit / Sort / Distinct / Repartition / Subquery / Join / Union
  into the defining input, failing closed otherwise. pins: fnp-win-1/C-003.
- `cast_legality.rs` — Spark's CAST / TRY_CAST type-legality deny matrix covers exactly
  `{Date32, Date64} ↔ {Int8, Int16, Int32, Int64}`. Refusals are `DataFusionError::Plan` with
  `[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]`, both Spark type names, and the applicable
  `UNIX_DATE` / `DATE_FROM_UNIX_DATE` remedy; `CastKeyword` preserves `CAST` or `TRY_CAST`.
  Legality is independent of ANSI evaluation mode.

Deliberately NOT here: the ANSI **store-assignment** matrix
(`crates/repark-iceberg/src/write/store_assign.rs`). It answers a different question and is laxer
where this one is strict (`Date32 → Timestamp` is store-assignable) and stricter where this one is
lax (`Timestamp → Int64` is not). Wiring either to the other would break TZ-5.

## Pointers

- Up: [../map.md](../map.md) — the rule file and every rewrite it dispatches
- The gate runs during analysis because `unix_date` lowers to `Expr::Cast(arg, Int32)` during
  optimizer simplification; placing it later would reject its own remedy.
- The recorded cross-engine rows: `python/repark/tests/test_cast_failure_parity.py`

## Debug

| Symptom | First check |
|---|---|
| `unix_date(d)` refuses | Keep the gate in the analyzer; optimizer simplification lowers `unix_date(a)` to `CAST(a AS Int32)`, which the analyzer must distinguish from user SQL. |
| `CAST(ts AS BIGINT)` refuses | Keep the deny matrix limited to `Date32`/`Date64`; timestamp rewrites in `../analyzer.rs` must remain outside it. |
| `F.col("d") / 2` reports "you wrote a CAST" | Keep `Float64` outside the deny targets; `Column.__truediv__` casts both operands to `Float64`. |
| An `INSERT INTO int_col SELECT date_col` cites this class instead of `INCOMPATIBLE_DATA_FOR_TABLE` | The WI-2 DML store-assignment rule must run BEFORE `SparkExprSemantics` — see `crates/repark-spark/src/extension.rs`. |

First checks: `cargo test -p repark-functions cast_legality`. Escalate to:
[../map.md#debug](../map.md).

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
- `overlay.rs` — `overlay(..., -1)` drops the Spark default length to the 3-arg form.
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

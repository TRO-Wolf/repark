# map — repark-functions/src/analyzer

## Purpose

File-backed submodules of `../analyzer.rs` (`SparkExprSemantics`). The rule file itself keeps the
rewrites; anything with its own matrix and its own error idiom lives here so `analyzer.rs` stays
under its `check_rust_file_size` ceiling and each matrix has one home.

## Contents

- `cast_legality.rs` — **G6-3 / G6-5 (2026-08-15):** Spark's CAST / TRY_CAST **type-legality**
  deny matrix and the refusal it raises. Exactly `{Date32, Date64} ↔ {Int8, Int16, Int32, Int64}`
  — a DENY list, so every pair not named keeps today's behaviour byte-for-byte. `Date → Int32|Int64`
  were the two silently-wrong pairs (arrow-rs 58.4 reinterprets the Date32 backing value, so
  `CAST(DATE '2020-01-01' AS INT)` answered `18262`); `Date → Int8|Int16` already refused with a
  DataFusion needle and now carry Spark's class; `Int* → Date` is the same class in reverse
  (G6-5). The refusal is `DataFusionError::Plan` carrying
  `[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]`, the two Spark type names and the remedy Spark
  names (`UNIX_DATE` / `DATE_FROM_UNIX_DATE`) — the `window_range.rs` precedent, folding to
  `AnalysisException` at the PyO3 boundary. `CastKeyword` spells `CAST` or `TRY_CAST` into the
  `Cannot resolve "…"` clause. **Not** ANSI-gated: `Cast.checkInputDataTypes` is a check on the
  type PAIR, and `TryCast` is `Cast(evalMode = TRY)` — eval mode governs values, not legality.
  Design: `planning/hardening/G63-DATE-INT-DESIGN.md`.

Deliberately NOT here: the ANSI **store-assignment** matrix
(`crates/repark-iceberg/src/write/store_assign.rs`). It answers a different question and is laxer
where this one is strict (`Date32 → Timestamp` is store-assignable) and stricter where this one is
lax (`Timestamp → Int64` is not). Wiring either to the other would break TZ-5.

## Pointers

- Up: [../map.md](../map.md) — the rule file and every rewrite it dispatches
- Why the gate must be at ANALYSIS, not the optimizer: `datafusion-spark`'s `unix_date` — the
  remedy this module's own message names — lowers to `Expr::Cast(arg, Int32)` in
  `ScalarUDFImpl::simplify`, which runs in the OPTIMIZER. A gate one stage later would refuse the
  remedy it recommends (design §3.4)
- The recorded cross-engine rows: `python/repark/tests/test_cast_failure_parity.py`

## Debug

| Symptom | First check |
|---|---|
| `unix_date(d)` started refusing | The gate moved out of the analyzer. `simplify_expressions` rewrites `unix_date(a)` to a textually identical `CAST(a AS Int32)`; only the analyzer stage still tells the user's cast from the engine's. |
| `CAST(ts AS BIGINT)` (TZ-5) started refusing | The deny matrix grew a `Timestamp` source. It is keyed on `Date32`/`Date64` only, precisely so the three timestamp rewrites in `../analyzer.rs` are unreachable from it. |
| `F.col("d") / 2` reports "you wrote a CAST" | `Float64` entered the target set. `Column.__truediv__` wraps BOTH operands in `Cast(… AS Float64)`; float targets are excluded on purpose (design §3.3). |
| An `INSERT INTO int_col SELECT date_col` cites this class instead of `INCOMPATIBLE_DATA_FOR_TABLE` | The WI-2 DML store-assignment rule must run BEFORE `SparkExprSemantics` — see `crates/repark-spark/src/extension.rs`. |

First checks: `cargo test -p repark-functions cast_legality`. Escalate to:
[../map.md#debug](../map.md).

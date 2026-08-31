# map — repark-functions/src/try_invert

## Purpose

FNP-7a/7b Spark `try_*` scalar kernels. Each name is the NULL-yielding inversion of a
raising path that already exists. Aggregates `try_sum` (datafusion-spark reuse) and
`try_avg` (own UDAF: decimal overflow NULL; `avg` still raises) register in
[`../aggregate.rs`](../aggregate.rs) and [`../lib.rs`](../lib.rs). `try_element_at` is an
alias of `element_at` in [`../collection.rs`](../collection.rs).

pins: fnp-7-try-inversions/C-001, C-002, C-004, C-005, C-006, C-007, C-009, C-010, C-011, C-014, C-015, C-017, C-018, C-019

## Contents

- `mod.rs` — register the nine scalar kernels.
- `arith.rs` — `try_divide`, `try_mod`, `try_add`, `try_subtract`, `try_multiply`.
  Integer overflow including SMALLINT/Int16 yields NULL. Divide/mod by zero yields NULL.
  Decimal reuses `decimal_spark::try_decimal_op` (ANSI-off overflow path). Coerce/result
  types for temporal overloads live here; invoke is `temporal.rs`.
- `temporal.rs` — DATE/TIMESTAMP ± INTERVAL and INTERVAL / numeric (including `/0` → NULL).
  DATE + HOUR/MINUTE/SECOND (nanos ≠ 0) promotes to timestamp; DATE + DAY/MONTH stays date.
  Interval-interval add/sub NULLs when the day-time duration exceeds i64 microseconds
  (Spark Duration max, 106751991 days). Interval `try_avg` is not here (FNP-11 refuse in
  [`../aggregate.rs`](../aggregate.rs)).
  pins: fnp-7-try-inversions/C-015, C-019
- `convert.rs` — `try_to_date`, `try_to_number`, `try_to_binary`, `try_to_time`.
  Live Spark 4.1.2 `try_to_time` raises `UNSUPPORTED_TIME_TYPE`; this kernel matches that.
  `try_to_date` Java patterns default missing month/day to 01 and parse `MMM`/`MMMM`.

## I want to...

| ...do this | go to |
|---|---|
| change a numeric try_* | `arith.rs` |
| change DATE/TIMESTAMP ± INTERVAL or INTERVAL / numeric | `temporal.rs` |
| change a parse try_* | `convert.rs` |
| change `try_element_at` | [`../collection.rs`](../collection.rs) |
| change `try_sum` / `try_avg` | [`../aggregate.rs`](../aggregate.rs) |

# AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 — insertion inventory

The owner's rule of 2026-11-09: every path where a decimal `avg`'s sum and count are added, merged, shipped, spilled,
converted to state or evaluated, with file:line on main `6903fb48`. **Trusts** marks a path whose answer depends on
the input holding no value past its declared precision, or on which rows meet in one partial. The last column mirrors
#1360's path audit for `sum` (`N/sum-delta3-out/delta-ready.md`, "The path audit", at `845766fd`).

## The averager's own paths (`crates/repark-functions/src/avg_decimal.rs`)

| # | path | file:line | what it does | trusts | `sum` after #1360 |
|---|---|---|---|---|---|
| A1 | sum precision | `:189-200` | `min(p + 10, 38)` for `Decimal128` (`76` for `Decimal256`); `sum_limit = 10^that` | no | `SumCheck::new` (`sum_decimal.rs:50`), result precision |
| A2 | per-value carry | `:101-110` (`i128`), `:45-54` (`i32`), `:73-82` (`i64`), `:129-134` (`i256`, wrapping) | add or subtract one value, counting each wrap past the native width | no | the same trait (`sum_decimal.rs:11` imports it) |
| A3 | `safe_rows` | `:303-317` | `N::MAX / (10^p - 1)`: the row count under which the batch sum "cannot wrap", from the **declared** precision | **yes (declared precision)** | removed in delta 3 (`bounded_rows` gone) |
| A4 | `add_batch` | `:319-337` | at or under `safe_rows`, Arrow's **wrapping** `sum` of the batch, one carry for the total; above it, every row with the carry | **yes**: an over-precision batch wraps with no carry | `fold_batch` + `exact_total` (`sum_decimal.rs:112`, `:130`): checked lanes, per-row carry on overflow |
| A5 | `sub_batch` | `:339-357` | the retract twin of A4 | **yes** | `fold_batch` with `sub_carry` |
| A6 | ungrouped `update_batch` | `:406-411` | `count += rows`; A4 | **yes** (A4) | `DecimalSum::update_batch` `:192`, fixed |
| A7 | ungrouped `retract_batch` (sliding windows) | `:455-460` | `count -= rows`; A5 | **yes** (A5) | `retract_batch` `:200`, fixed |
| A8 | ungrouped `state` | `:424-435` | ships `[count UInt64, sum decimal(p, s)]`; the sum is **NULL** (the marker) unless `!overflowed && wraps == 0 && |low| < 10^(p+10)` | **yes (row layout)**: a partial whose own total leaves the sum precision marks the query | `state` `:229`: the raw total; `≤ 19` refuses on a counted wrap, `≥ 20` ships the wrap count |
| A9 | ungrouped `merge_batch` | `:437-453` | adds counts; adds each partial with the carry; a NULL partial with `count > 0` sets `overflowed` | **yes**: the marker of A8 | `merge_batch` `:212`: every partial with the carry, wraps from 20 |
| A10 | ungrouped `evaluate` | `:413-418` → `average_parts` `:254-280` | `overflowed` → `ARITHMETIC_OVERFLOW` / NULL; else the exact `low + wraps·2^128` against `sum_limit` (A11) | only through A8/A9 | `evaluate` (both) `:241`: `wraps == 0 && |low| < 10^p` |
| A11 | `average` / `narrow_quotient` | `:219-252` | one check: `|sum| ≥ sum_limit` → `ARITHMETIC_OVERFLOW` with the `try_avg` hint (ANSI) or NULL; the quotient rounds half up at `s + 4`; past `10^(p+4)` → `NUMERIC_VALUE_OUT_OF_RANGE` or NULL | no | — |
| A12 | grouped `update_batch` | `:526-549` | every row with the carry into the group's `low` / `wraps`; `count += 1` | no (exact per row) | `fold_rows` `:361`, the same |
| A13 | grouped `state` (partial output, early emit, **spill**) | `:576-607` | ships `[count, sum decimal(p, s)]`; the sum is **NULL** unless `!overflowed && carried == 0 && |low| < 10^(p+10)` | **yes (row layout)**: the card's cause | `shipped` `:428`: the raw low word, plus the wrap count from 20 |
| A14 | grouped `merge_batch` | `:609-657` | counts; partial sums with the carry; a NULL partial sum with `count > 0` sets the group's `overflowed` | **yes**: A13's marker | `fold_partials` `:382` |
| A15 | grouped `evaluate` | `:551-574` | A11 per group; `overflowed` → `ARITHMETIC_OVERFLOW` (ANSI raises for the batch) or NULL | through A13/A14 | `evaluate` `:461` |
| A16 | grouped `convert_to_state` (skipped partial) | `:659-674` | one row per state: the raw value, `count` 1 | no (one declared value is under `10^(p+10)`; an over-precision value still ships raw, past its declared type) | `convert_to_state` `:489`, zero wraps |
| A17 | input coercion | `:502-511` | casts the batch to the input type when it differs | no | `decimals` `:146` (`Decimal128` only) |

## Wiring (`crates/repark-functions/src/aggregate.rs`, `avg_groups.rs`)

| # | path | file:line | what it does | trusts |
|---|---|---|---|---|
| W1 | `avg`, `mean`, `try_avg` UDFs | `aggregate.rs:55-76` | `AvgMode::Ansi` / `Try`; `decimal_null_on_overflow` `:127` | no |
| W2 | ANSI-off swap | `aggregate.rs:297-351` (`SparkAvgAnsiMode`, `legacy_avg_expr`) | swaps `avg` for the legacy (NULL on overflow) UDF when ANSI is off, plans and windows | no |
| W3 | return type | `aggregate.rs:149-175` | `decimal(min(p+4, 38), min(s+4, 38))` | no |
| W4 | ungrouped / window accumulator | `aggregate.rs:177-230` | `Decimal32/64/128/256` → `DecimalAvgAccumulator<T>`; `DISTINCT` refuses here (`DistinctAvgAccumulator`) | no |
| W5 | **state schema** | `aggregate.rs:248-285` | decimal: `[count UInt64, sum <input type>]` for every precision | the seat for a wrap column (DataFusion passes `input_fields`, `datafusion-physical-expr-54.1.0/src/aggregate.rs:732-741`) |
| W6 | grouped accumulator | `aggregate.rs:232-246` → `avg_groups.rs:30-116` | `DecimalAvgGroups<T>`; refuses `DISTINCT` | no |
| W7 | DataFrame door | `crates/repark-python/src/column/function_dispatch.rs:940-941` | `F.avg` / `F.try_avg` reach W1 | no |

## DataFusion's calls into the averager (`datafusion-physical-plan-54.1.0`, `-physical-expr-54.1.0`)

| # | path | file:line | reaches |
|---|---|---|---|
| D1 | grouped partial `update_batch` / final `merge_batch` | `aggregates/row_hash.rs:999`, `:1013` | A12, A14 |
| D2 | grouped emit: partial output and **spill** (`emit(EmitTo::All, true)`) | `row_hash.rs:1103-1126`, `spill` `:1241` | A13 (state), A15 (evaluate) |
| D3 | skip partial aggregation (ratio 0.8, 100,000 rows) | `row_hash.rs:639`, `transform_to_states` `:1443-1463`; `config.rs:658-662` | A16 |
| D4 | ungrouped `update_batch` / `merge_batch` | `aggregates/no_grouping.rs:469-470` | A6, A9 |
| D5 | ungrouped state and evaluate | `aggregates/mod.rs:2062-2070` | A8, A10 |
| D6 | window, whole frame | `window/window_expr.rs:324-325` | A6, A10 (single phase) |
| D7 | window, sliding frame | `window/sliding_aggregate.rs:201-233` | A6, A7, A10 |
| D8 | `avg(DISTINCT c)` | `datafusion-optimizer-54.1.0/src/single_distinct_to_groupby.rs` | rewritten to `avg` over `GROUP BY c`: A6–A15 |
| D9 | which rows meet in a partial | `enable_file_stream_work_stealing` (`config.rs:653`, default on), `target_partitions`, `RoundRobinBatch` | the race behind A8 / A13 |

## What trusts what

- **Row layout (the card):** A8 and A13 test each partial against `10^(p+10)` and ship a NULL marker past it; A9 and
  A14 turn the marker into the query's or the group's overflow. Only precision 29 and up can reach it with declared
  values (`p + 10 > 38`, so the limit is `10^38`); below that a partial needs more than `10^10` rows near the maximum.
- **Declared precision:** A3–A7, the ungrouped and window batch add. A batch under `safe_rows` is summed with a
  wrapping add and one carry, so an over-precision Parquet value wraps without a count. `safe_rows` is
  `i128::MAX / (10^p - 1)`: about `1.7 × 10^19` rows at precision 19, `17,014` at 34, `1` at 38, so every batch of
  8,192 rows or fewer takes the wrapping add at precision 34 and under. The grouped path (A12) adds every row with the
  carry and is exact.
- **Not trusted:** A11's one check, A12, A16.
- **Narrow natives:** `Decimal32` and `Decimal64` inputs keep their sum in `i32` / `i64` with a wrap count (A2). There
  the single column wraps after two or three values near the maximum, and A13 marks it at any precision. Every
  measured table here reads as `Decimal128`; a `Decimal32` / `Decimal64` column comes only from an Arrow input that
  declares one (not measured, a seat note below).

## Can `avg` reuse #1360's carrier without copying it

At `845766fd` (`crates/repark-functions/src/aggregate/sum_decimal.rs`):

| item | line | visibility | type | reuse by `avg` |
|---|---|---|---|---|
| `NARROW_PRECISION` (19), `Carrier::of` | `:16`, `:26-39` | `pub(crate)` | — | as is |
| `exact_total` | `:112-128` | private | `PrimitiveArray<Decimal128Type>` → `Option<i128>` | needs `pub(crate)`; `Decimal128` only |
| `fold_batch` | `:130-144` | private | `Decimal128`, `i128` low, `i64` wraps | needs `pub(crate)`; replaces A4/A5 for `Decimal128` |
| `partial_wraps`, `wraps_array` | `:155-167` | private | `Int64` state column | needs `pub(crate)`; its error text names `sum` |
| `unwrapped` | `:102-110` | private | the narrow-carrier refusal | needs `pub(crate)`; its text names "sum state" |
| `WideDecimal` (`add_carry`, `sub_carry`, `join`) | `avg_decimal.rs:22-144` | `pub(crate)` | every native | `sum` already imports it from `avg`'s file |

- **Yes, without copying, for `Decimal128`:** five private helpers become `pub(crate)` (or move, unchanged, to a shared
  `aggregate/decimal_carry.rs`), and two error texts take the function's name. That edits #1360's file, so this unit
  lands **after** #1360 merges.
- **Not for the other natives:** the helpers are `Decimal128`-only. `avg`'s generic `T` keeps A2's per-row carry for
  `Decimal32`, `Decimal64` and `Decimal256`; a `Decimal32` / `Decimal64` partial needs the wrap column at every
  precision (or a widening to `i128` before the add), since the narrow bound (`n × 10^p < 2^127`) is an `i128` bound.
- The state schema seat is `aggregate.rs:248-285` (W5), not `spark_sum.rs`; #1360's `Carrier` decides it the same
  way.

Up: [map.md](map.md).

# map — repark-core/src/session/df_guards/cast_shield

## Purpose

CAST-LITERAL-UNWRAP-1 (2026-11-16), seat W: the identity UDF that hides a narrowing cast from DataFusion's
unwrap, its `preimage`, and the unit tests. The rule that places it is [../cast_shield.rs](../cast_shield.rs).
Design and rulings: `task/wo/cast-literal-unwrap-1/` (`sketch.md` §3). **CAST-STATS-INTERVAL-1 (2026-11-22):** the
statistics barrier lives here too, beside the shield it shares `node_schema` and `reads_input` with
([card](../../../../../../task/roadmap/mid-term/cast-stats-interval-1-card-2026-11-22.md)).

## Contents

- [udf.rs](udf.rs) — `__repark_cast_shield__(x)` returns `x` unchanged (same type, nullability and kernel, so ANSI
  raises and NULLs are the cast's own). Its `preimage` answers `shield(CAST(x AS T)) <op> k` for a non-NULL
  literal `k` of `T` and a `DECIMAL(p,s)` `x`, with the range `[lo, hi)` on `x` in unscaled units (`u` = 1):
  integer `T`, truncation toward zero, `f = 10^s`: `k > 0` `[k·f, (k+1)·f)`, `k < 0` `[(k-1)·f+u, k·f+u)`, `k = 0`
  `[-f+u, f)`; `DECIMAL(p2,s2)` with `s2 < s`, half away from zero, `m = 10^(s-s2)`, `h = 5·10^(s-s2-1)`: `k > 0`
  `[k·m-h, k·m+h)`, `k < 0` `[k·m-h+u, k·m+h+u)`, `k = 0` `[-h+u, h)`. The lower bound is clipped to `-(10^p-1)`; an
  upper bound above `10^p-1`, an empty range, an arithmetic overflow, and every cast that can overflow for some
  `x` (integer to a narrower integer; a decimal whose integer part does not fit the integer target; a decimal
  whose integer digits, plus one when rounding drops a digit, exceed the target's) decline, so the cast runs per
  row. The narrowing predicate is sketch §3's, with "integer digits exceed the target's" read as "`10^(p-s)-1`
  exceeds the target's maximum". The lossless predicate (the chain the rule walks through) is widening only.
- [tests.rs](tests.rs) — the `preimage` table (truncation, half away from zero both signs, declines), the
  narrowing and lossless predicates, answers on a core session, the select-list names and idempotence, the `IN`
  cap, and the four ruling-1 shapes red with the optimizer placement stripped and green with it.
- [stats_barrier.rs](stats_barrier.rs) — `StatsBarrierRule` (`repark_stats_barrier`), the last optimizer rule, wraps the
  operand of every `CAST` whose value order the cast does not keep in `__repark_stats_barrier__(x)`, an identity UDF
  (same type, nullability and kernel). DataFusion 54.1.0 carries a column's statistics through a `CastExpr` by casting
  its min and max (`projection.rs` `project_column_statistics_through_expr`, `cast.rs` `evaluate_bounds`), so STRING
  ids `'10'` and `'7'` became the INT range `[10, 7]` (a filter above it failed on `Interval::try_new`) and `max`/`min`
  of the cast answered from the footer (`7`, `10` for `25`, `3`). Behind the barrier the cast's bounds are unknown.
  `keeps_order` is DataFusion's own order family (`is_order_preserving_cast_family`), tightened for
  CAST-STATS-INTERVAL-1 (2026-11-22) and ANSI-OFF-NARROWING-OVERFLOW-1 (2026-11-23): a numeric or BOOLEAN to numeric
  cast keeps order only when the cast cannot overflow — a FLOAT/DOUBLE source only to the same or a wider float type
  (the footer leaves NaN out), non-float to Float64 always, to Float32 when decimal integer digits <= 38, to Float16
  only from BOOLEAN/Int8/UInt8, integer/BOOLEAN to integer only when `[from_low, from_high] ⊆ [to_low, to_high]`,
  DECIMAL to signed integer only when `10^(p-s)-1` fits the target, DECIMAL to DECIMAL only when `to_int > from_int`
  (when `to_scale < from_scale`) or `to_int >= from_int` (when `to_scale >= from_scale`), and integer/BOOLEAN to
  DECIMAL only when `to_scale >= 0` and max magnitude fits `10^(to_p - to_s) - 1`. Every narrowing numeric cast that
  can wrap or overflow to NULL is barred, preventing silent footer folding and pruning across row groups.
  Text to text, binary to binary, and time casts (DATE to DATE, DATE to and from a zone-less TIMESTAMP, TIMESTAMP to
  TIMESTAMP with the zone kept or kept absent; dictionaries read as their values) keep order. Every other
  cast (STRING to anything else, anything to STRING, numeric to BOOLEAN, a zoned TIMESTAMP to DATE or to a zone-less
  one) is barred. A cast of a constant, a `TRY_CAST` (DataFusion carries no statistics through it) and an operand
  already behind the barrier are left alone; names are kept by `NamePreserver`, so the select list does not change.
- [stats_barrier_tests.rs](stats_barrier_tests.rs) — the order table (narrowing integer, decimal, and float casts
  barred; lossless widenings kept), the four inverted-range shapes (`max`/`min` direct and through a projection,
  `BETWEEN` over a projection, the `row_number` dedup) on a core session, an INT to BIGINT cast whose `max`/`min` still
  fold from the footer (no `AggregateExec`, no barrier), the multi-row-group Parquet narrowing barrier pin (`min`/`max`/`=`
  over `[1, 2]` and `[100, 300]`), and the names and idempotence.

Up: [../map.md](../map.md).

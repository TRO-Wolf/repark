# map — repark-core/src/session/df_guards/cast_shield

## Purpose

CAST-LITERAL-UNWRAP-1 (2026-11-16), seat W: the identity UDF that hides a narrowing cast from DataFusion's
unwrap, its `preimage`, and the unit tests. The rule that places it is [../cast_shield.rs](../cast_shield.rs).
Design and rulings: `task/wo/cast-literal-unwrap-1/` (`sketch.md` §3).

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

Up: [../map.md](../map.md).

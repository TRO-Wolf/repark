# map — repark-core/src/series_order

## Purpose

TA-SINGLE-SERIES-PARALLEL-1 slice S2a (2026-10-05): the test module of `../series_order.rs`, the
generic order resolver a bare `ta.*` column binds through (design: the series sketch §1.3 and
§3.2; owner rulings Q1 a → b → c and Q2 timestamps before dates).

`../series_order.rs` holds the code:

- `resolve_series_order(plan, schema) -> SeriesOrder { keys, source }`. **(a) Declared:** walk
  down through `Projection` (a key survives only as a plain column or an alias of one), `Filter`,
  `SubqueryAlias`, `Window` and `Limit` to a `Sort` (its keys, aliases stripped) or to a
  `TableScan` over a `MemTable` with a non-empty `sort_order` (its first ordering, mapped through
  the scan's projection). Any other node, a computed sort key, or a key that does not survive a
  projection means no declared order. **(b) FirstTemporal:** the first `Timestamp(_, _)` column,
  else the first `Date32` / `Date64` column, ascending NULLS FIRST, exactly what
  `Window.orderBy(col)` builds. **(c) CurrentRowOrder:** no keys. Keys come back as qualified
  columns of the frame's own schema.
- `SeriesOrderNotice`: the once-per-session warning flag. A `ConfigExtension` under the
  two-segment prefix `repark.series`, the same carrier shape as `repark.parallel` (`SET`
  refuses, `entries()` is empty), holding an `Arc<AtomicBool>` so every `SessionConfig` cloned
  from the session shares one flag. `claim_series_order_notice` is true exactly once per
  session (absent carrier → always true).

## Contents

- `tests.rs` — `series_order_declared_beats_temporal` (P-S2a-2: a frame sorted by a later
  timestamp column through an aliased sort key, carried through filter / aliased projection /
  limit / subquery alias / window, and a declared `MemTable` order through a projection);
  `series_order_declared_lost_falls_through` (a dropped key, a computed key and an aggregate
  fall to (b)); `series_order_timestamp_before_date` (P-S2a-3, the column choice; a zoned
  nanosecond timestamp counts); `series_order_date_fallback` (P-S2a-3b);
  `series_order_current_row_order_without_temporal`; `series_order_notice_claims_once_per_session`
  (clones share the flag, a second session has its own, `SET` refuses, a bare config always
  claims). pins: ta-series-s2a/C-002, C-003

## Pointers

- Up: [../map.md](../map.md)
- The bind-time rewrite that calls it: `crates/repark-python/src/column/series.rs`
- The partition-index read for case (c): [../parallel_window/map.md](../parallel_window/map.md)

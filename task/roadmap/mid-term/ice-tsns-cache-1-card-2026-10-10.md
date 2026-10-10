# Card ICE-TSNS-CACHE-1: a cached or persisted frame keeps nanoseconds

**Date:** 2026-10-10. **Filed by:** the ICE-TSNS-NARROW-REFUSE-1 build lane (Claude Opus 5.5), from the owner's ruling of 2026-10-10 on question Q4 of that unit's ledger.

**Status:** open. Target v1.5.5. Its own unit.

**Sibling:** card ICE-TSNS-NARROW-REFUSE-1:
[ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md),
and card ICE-TSNS-COERCION-1:
[ice-tsns-coercion-1-card-2026-10-10.md](ice-tsns-coercion-1-card-2026-10-10.md).
Parity row: ICE-TSNS-SQL-1-R-017 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Why

A frame that holds a nanosecond value narrowed by type coercion, written into a
`timestamp_ns` or `timestamptz_ns` column, refuses by name on every door since
ICE-TSNS-NARROW-REFUSE-1. The same frame written after `cache()` or `persist()` stores the
value cut to microseconds. The owner's ruling: this is not a refusal; the fix is to keep
nanoseconds through cache and persist.

## The site, measured

`cache()` and `persist()` reach `materialize_dataframe_as_cache_view`, which calls
`register_collected_memtable` (`crates/repark-core/src/session/temp_views.rs`). That function
collects the frame and registers the batches as a `MemTable`. The narrowing cast is part of
the frame's plan, so it is evaluated by that collect: the `MemTable` column is a microsecond
instant and holds no expression. The later write reads a table of microseconds; the store
guard has no mark to read.

A plain `timestamp_ns` column survives the cache with all nine digits. The cut is made by the
cast the coercion inserted, not by the collect.

## The count

Door `cached_frame_append` of the matrix of ICE-TSNS-NARROW-REFUSE-1 (both nanosecond
targets, both source types, five zones), head `60687f7e`:

- Of the 300 cells the owner asked about (fifteen spellings narrowed beside an untyped NULL),
  **290 are the cache alone**: the same frame appended without the cache refuses. The other
  10 are `date_trunc('second', coalesce(tzns, NULL))`, which stores uncached too since the
  ruling on Q1.
- Over the whole matrix the cache alone accounts for 420 cells: those 290, 120 of the six
  spellings narrowed beside a typed NULL or a microsecond value, and 10 of
  `CAST(coalesce(tzns, NULL) AS DATE)`.
- 180 more cached cells store a cut value the statement wrote (`CAST`, `date_trunc`); they
  store the same value uncached and are not this card's.

## The ask

- A frame cached or persisted with a narrowed nanosecond value stores what the uncached
  frame stores, or refuses as the uncached frame refuses; it never stores a cut value the
  uncached write would refuse.
- Two ways are open and the unit chooses with evidence: keep the nanoseconds through the
  coercion (card ICE-TSNS-COERCION-1, which removes the cut at its source), or carry the
  narrowing through the materialisation (a mark on the `MemTable` field that the store guard
  reads).
- A cached frame with no narrowed value does not move.

## Acceptance

- Five session zones, both targets, both sources, the spellings of the base matrix, `cache()`
  and `persist()` at each storage level; values read from the Parquet files.
- Never worse than the base matrix
  (`python/repark/tests/ice_tsns_narrow_refuse_1_base.json`, door `cached_frame_append`).

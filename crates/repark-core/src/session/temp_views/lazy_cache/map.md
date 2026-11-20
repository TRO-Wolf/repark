# map — repark-core/src/session/temp_views/lazy_cache

## Purpose

The pins of [../lazy_cache.rs](../lazy_cache.rs) (`#[cfg(test)] mod tests;`), through
`ReparkSession::register_lazy_cache_view`.

## Contents

- `tests.rs` — **TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 (2026-11-20):** physical planning and `EXPLAIN` fill nothing and
  the first execution fills; a fill refused by `repark.cache.max_bytes` keeps nothing, keeps the old text and
  retries at the next use; two concurrent first uses serve one fill (one buffer); the retained total counts a filled
  table; a released table passes through (a row inserted into the source after the fill shows) and keeps nothing; a
  `LIMIT 1` read fills the whole table; the session's state frees with a filled table registered (the table holds
  only a weak reader of it).

Up: [../map.md](../map.md).

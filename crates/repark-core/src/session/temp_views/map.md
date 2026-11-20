# map — repark-core/src/session/temp_views

## Purpose

Child modules of [../temp_views.rs](../temp_views.rs) (`mod lazy_cache;`), declared there because `../../session.rs`
sits at its line ceiling.

## Contents

- `lazy_cache.rs` — **TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 (2026-11-20):** the lazy cache table, Spark's `cache()` moment.
  `ReparkSession::register_lazy_cache_view` registers it under the `__repark_cache_*` name at `cache()` /
  `persist()`; the frame is bound to it at once, so every child, view, write, `explain` and join built after
  `cache()` scans it.
  - **Holds:** the stripped executable twin's logical plan, that twin's `ConfigOptions` (the frame's settings), a
    weak reader of the live session state (never the state itself, so the catalog that holds the table holds no
    cycle), the budgets, the grown-stack segment for the twin's depth, the declared order and the owner memo.
    The declared order and the single-partition flag come from the twin's physical plan at registration, planned
    under a throwaway memo (Spark builds its cached physical plan at `cache()`).
  - **States:** armed, filled, released.
    - `scan` (planning, `explain`, `EXPLAIN`) never fills. It records the acting thread's reader-parse memo in a
      `LazyCacheExec` leaf.
    - The first `execute` decides synchronously under a fresh memo. It rebuilds the state from the live one with
      the twin's options, analyses and optimizes, then fills once through an async once-cell: a concurrent first
      use waits, and a failed or dropped fill keeps nothing.
    - Filled, it serves the kept batches. In both cases the acting memo adopts the fill's decisions where it
      holds none (`ReaderParseMemo::adopt`).
    - `release` (from `unpersist` and `clearCache`) makes it a pass-through: each execution plans the lineage
      under the acting memo, or live, and keeps nothing.
    - The owner memo, kept only when it was already settled at `cache()`, always gets the pass-through (rule (a)
      part 3: a frame acted on before `cache()` keeps its own plan).
  - **Execution:** the fill and the pass-through run their physical planning and collection inside a future that
    re-enters the chosen memo on every poll and on a stack grown to the twin's depth. The fill applies
    `repark.cache.max_bytes` and `max_total_bytes` through the shared `drain_budgeted` (refusals are
    IllegalArgument, with the old text). Batches pass tighten provenance, conform and the attribute-id strip, and
    are sorted to the declared order if the fill's plan did not deliver it. `LazyCacheExec` reports unknown
    statistics, so no count is answered from an empty template.
  - `cache_budget::buffer_set_of` counts a filled table's batches with the `MemTable` caches.

Up: [../map.md](../map.md).

# map — repark-core/src/session/temp_views

## Purpose

Child modules of [../temp_views.rs](../temp_views.rs) (`mod lazy_cache;`, pins in [lazy_cache/](lazy_cache/map.md)), declared there because `../../session.rs`
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
  - **ICE-HELD-FRAME-SNAPSHOT-1 unit 2 (2026-11-21): the fill follows the session's commits.** A fill records, for
    each Iceberg table its plan read (the lineage's bound scans at registration, and every scan the fill's memo
    noted, nested views and nested caches included), the session table map's epoch and the table it read. A use
    whose epoch has moved drops the fill and refills under the live state (Spark's same-session re-cache), except
    when the table keeps its UUID, current snapshot and last snapshot-log entry (an `ALTER` of the schema or the
    properties, a branch): then the fill is kept and the dep moves to the new epoch. A `REFRESH` (a new epoch over
    the same state), a drop, a recreate and every data commit refill; a refill that the re-bind refuses keeps
    nothing and raises its condition. **Delta 1 (F1):** the dep reads the map's local epoch, which only the
    session's own commits, drops, creates, renames and refreshes move, so another writer's commit keeps the fill also
    when a fresh read on Glue or S3 Tables records it (Q2 (b) moves the epoch, not the local epoch), as Spark keeps
    its fill until a same-session write or a refresh. A refresh since the fill always refills, whatever the state.
    **Delta 2 (2026-11-22):** that rule holds only for a cache of the table itself, so the dep now splits by
    `CacheLevel`, which the caller passes to `register_lazy_cache_view` (the facade: `Table` for `spark.table(t)`, its
    aliases and `CACHE TABLE`, `Query` for everything else). `Table` keys on the map's `data` tick, so it keeps its
    fill through another writer's commit and a same-session metadata-only commit, and refills on a refresh or an own
    data commit. `Query` keys on the epoch and `refreshed`: it refills when a load has recorded new data and on
    every refresh, so it answers what the session's latest read answers.
  - **Own-plan actions keep their first binding through the cache.** `cache()` notes the lineage's tables in the
    acting frame's memo at their current state (Spark fixes the frame's own plan there). A memo that holds a table
    whose data differs from what the cache would serve reads the lineage under that memo (the pass-through) and
    keeps nothing; one that holds none reads the cache, and `adopt` gives it the fill's tables.

Up: [../map.md](../map.md).

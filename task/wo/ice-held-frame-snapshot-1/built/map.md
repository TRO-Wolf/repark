# task/wo/ice-held-frame-snapshot-1/built/ — unit 1a, the re-bind and the command scope (2026-11-20)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh, on Fable's guidance of 2026-11-20 under the owner's delegation
(`Fable reply — ICE-HELD-FRAME-SNAPSHOT-1 step 0 2026-11-20.md`, §1 to §12, outside the repo) and the owner's cost
rule of 2026-11-19 (C-012 a hard 1.05x; a short row over 1.05x a flag; nothing over 1.15x without the owner).

- [bar.md](bar.md) — the deciding cell, the new schema-change cells, the bar on the step 0 cells, the must-hold
  sets, the gap cells and the EQUAL lost; fix round 1's re-run, its ENC-1 cells, the classes of the cells round 0
  left open and the 8 stored cells.
- [cost.md](cost.md) — main's baseline and the head under the exclusive lock, and fix round 1's cause of the C-012
  miss.
- [unit-2.md](unit-2.md) — **unit 2 (2026-11-21): the cached cells.** The state table on today's main, Spark's cache
  invalidation surface (97 cells per mode), the design (a lazy cache fill bound to the session table map's epochs,
  kept across a schema-only change), the three product files, the bar against 1a's and A1's, the mutation proof, the
  cost and the gates, a card line and the C-009 question. **Delta 1 (§9):** the verifier's F1, a local epoch in the
  session table map so a cached fill outlives another writer's commit, its Rust pins and gates. **Delta 2
  (§10):** the owner's cache-shape rule: a data tick in the map, the dep split by `CacheLevel`, the facade's level mark
  and the narrowed C-009 drop, the pins, the event table and `PENDING_1394` emptied.
- [live-aws.md](live-aws.md) — the held-frame cells for the live Glue and S3 Tables run (2026-11-21, tests only):
  the cells and their live tests, Spark's measured answers and cache interval, main's local answers, the
  local-only list and how to read the cost lines. LIVE-SECOND-WRITER-1 (2026-11-22): a second Spark process as the
  other writer, the seven cached shapes on a RePark and a Spark reader leg, the grading, main's predicted answers
  written before any live run, `PENDING_1394` and what stays open.
- [cards.md](cards.md) — the residual cards of fix round 1 and delta 2, and unit 2 delta 1's F3, for the
  orchestrator to file.
- [evidence/](evidence/map.md) — the probes and the raw cells of this build.

## What 1a builds (seat A, Q9 (a), the command scope moved in)

1. **The session table state** (`crates/repark-iceberg/src/catalog/session_tables/`, its [map](../../../../crates/repark-iceberg/src/catalog/session_tables/map.md)):
   a decorator over the registered catalog handle, applied before the catalog provider is built, records every
   same-session commit, drop, rename, refresh and load, ordered by a session counter, with the race guard.
2. **The binding at resolution:** the catalog provider's schemas capture each planning load and register the
   resolved provider with it, so the re-bind knows what a held scan was bound to (its table UUID, metadata location
   and schema) without a fork accessor. Fix round 1: each Iceberg schema is wrapped once, where the provider registers
   it, so `schema()` keeps one `Arc` and a non-Iceberg schema comes back as registered (F0, F1).
3. **The re-bind** (`crates/repark-spark/src/view_ddl/frame_settings/held_iceberg.rs`, its row in the
   [map](../../../../crates/repark-spark/src/view_ddl/frame_settings/map.md)): inside the optimizer's first rule,
   once per `(catalog, identifier)` per plan, with the own-plan memo's table slot (`ReaderParseMemo::held_table` and
   `hold_table`, `crates/repark-core/src/extension.rs`), Spark's checks by name, type and nullability, and a wrapper
   over a provider built through a one-table shim.
4. **The command scope:** a `_repark_writer_` scratch view (every DataFrame write door goes through one) binds its
   scans in command mode, checked under `PROHIBIT_CHANGES`; a user DataFrame view binds in view mode (no identity
   check, Spark's view error), and keeps it under a writer view; other internal scratch views stay in query mode.
   Delta 2: a frame built over a user DataFrame view binds those scans in query mode at its build, to the table the
   session holds then, as Spark analyses it.
5. **`explain` under the memo:** the `EXPLAIN` frame shares the held frame's memo (`surface_a._owned`, one line of
   `core.py` changed, line-neutral).
6. **`REFRESH TABLE` and `catalog.refreshTable`:** both load the named table once through the decorator (the
   epoch moves); `refreshTable` now runs the SQL door.
7. **ENC-1 (fix round 1, F2, owner 2026-11-20):** the decorator and the one-table shim join the guard gate's
   allowlist on three conditions: they wrap the guarded handle only (the decorator takes the guard's own
   `GuardedCatalog` token), a refused commit records nothing, and a map-served table behaves as a fresh guarded load.
   The tests are named in the module's map; the facade pairs are in `bar.md`.

Not in 1a: the bare `writeTo` re-plan (unit 1b), the cached cells (unit 2, after A1), the path read (Q4 (b), its own
card), and **refs and metadata tables (Q5): shed** (the two `time_travel.rs` files), as Fable §5 allows; their 16
cells per build and mode stay WRONG and the 68 pinned cells stay EQUAL.

Up: [../map.md](../map.md).

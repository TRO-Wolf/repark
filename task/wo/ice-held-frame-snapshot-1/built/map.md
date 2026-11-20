# task/wo/ice-held-frame-snapshot-1/built/ — unit 1a, the re-bind and the command scope (2026-11-20)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh, on Fable's guidance of 2026-11-20 under the owner's delegation
(`Fable reply — ICE-HELD-FRAME-SNAPSHOT-1 step 0 2026-11-20.md`, §1 to §12, outside the repo) and the owner's cost
rule of 2026-11-19 (C-012 a hard 1.05x; a short row over 1.05x a flag; nothing over 1.15x without the owner).

- [bar.md](bar.md) — the deciding cell, the new schema-change cells, the bar on the step 0 cells, the must-hold
  sets, the gap cells and the EQUAL lost.
- [cost.md](cost.md) — main's baseline and the head under the exclusive lock.
- [evidence/](evidence/map.md) — the probes and the raw cells of this build.

## What 1a builds (seat A, Q9 (a), the command scope moved in)

1. **The session table state** (`crates/repark-iceberg/src/catalog/session_tables/`, its [map](../../../../crates/repark-iceberg/src/catalog/session_tables/map.md)):
   a decorator over the registered catalog handle, applied before the catalog provider is built, records every
   same-session commit, drop, rename, refresh and load, ordered by a session counter, with the race guard.
2. **The binding at resolution:** the catalog provider's schemas capture each planning load and register the
   resolved provider with it, so the re-bind knows what a held scan was bound to (its table UUID, metadata location
   and schema) without a fork accessor.
3. **The re-bind** (`crates/repark-spark/src/view_ddl/frame_settings/held_iceberg.rs`, its row in the
   [map](../../../../crates/repark-spark/src/view_ddl/frame_settings/map.md)): inside the optimizer's first rule,
   once per `(catalog, identifier)` per plan, with the own-plan memo's table slot (`ReaderParseMemo::held_table` and
   `hold_table`, `crates/repark-core/src/extension.rs`), Spark's checks by name, type and nullability, and a wrapper
   over a provider built through a one-table shim.
4. **The command scope:** a `_repark_writer_` scratch view (every DataFrame write door goes through one) binds its
   scans in command mode, checked under `PROHIBIT_CHANGES`; a user DataFrame view binds in view mode (no identity
   check, Spark's view error), and keeps it under a writer view; other internal scratch views stay in query mode.
5. **`explain` under the memo:** the `EXPLAIN` frame shares the held frame's memo (`surface_a._owned`, one line of
   `core.py` changed, line-neutral).
6. **`REFRESH TABLE` and `catalog.refreshTable`:** both load the named table once through the decorator (the
   epoch moves); `refreshTable` now runs the SQL door.

Not in 1a: the bare `writeTo` re-plan (unit 1b), the cached cells (unit 2, after A1), the path read (Q4 (b), its own
card), and **refs and metadata tables (Q5): shed** (the two `time_travel.rs` files), as Fable §5 allows; their 16
cells per build and mode stay WRONG and the 68 pinned cells stay EQUAL.

Up: [../map.md](../map.md).

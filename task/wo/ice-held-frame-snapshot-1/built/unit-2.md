# ICE-HELD-FRAME-SNAPSHOT-1 unit 2 — the cached cells

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh, 2026-11-21, after A1 (#1386) and 1a (#1390) merged; the brief's
design (sketch §5: "A1's lazy cache table drops its fill when the map's epoch for a table it reads moves"), refined
by what Spark measured (below). `S` is the lane scratchpad and `N` the session scratchpad; neither is in the
repository. Builds: main `S/ddu-work/pkg-a1m-bb8ae8fc` (its product tree equals main `0cc65e9a`), head
`S/ddu-work/pkg-iceu2-eac90dd7` (`BUILT_FROM` `eac90dd7`). Spark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0`,
the local `type=hadoop` catalog, zone UTC, both ANSI modes; every answer, on every engine, is the same in both modes.
The probes and the 616 recorded answers are in [evidence/unit-2/](evidence/unit-2/map.md).

## 1. The state table on today's main

| cell | Spark | main `0cc65e9a` | head |
|---|---|---|---|
| `C` `cache()`, `count`, `INSERT`, `count`, fresh `count` | 2, 3, 3 | 2, 2, 3 | 2, 3, 3 |
| `C` `cache()` (lazy), `INSERT`, `count`, fresh `count` | 3, 3 | **3, 3 (EQUAL)** | 3, 3 |
| `C` `CACHE TABLE t`, `INSERT`, held `count`, fresh `count` | 3, 3 | **3, 3 (EQUAL)** | 3, 3 |
| `C` `cache()`, `count`, `writeTo(t).append()`, `count`, fresh `count` | 2, 3, 3 | 2, 2, 3 | 2, 3, 3 |
| `K|cache_held` `cache()`, `count`, `INSERT`, `count` | 2, 3 | 2, 2 | 2, 3 |

**Two of the four `C` cells were already EQUAL on main** (the domain table's main column is main before A1 and 1a):
A1's lazy table fills at the first `count`, which runs after the insert, and `CACHE TABLE`'s held `count` reads the
held frame through 1a's re-bind, not the cache (1a's bar already showed both EQUAL). Unit 2 moves 3 cells per mode.

## 2. Spark's invalidation surface, measured

97 cells per mode on Spark, main and the head ([evidence/unit-2/](evidence/unit-2/map.md); the full table is
`N/ice-u2-out/state-table.md` and `state-head.md`). What they show:

1. **A same-session data commit re-caches** for the next new query: every write verb (`INSERT`, `INSERT … SELECT`,
   `DELETE`, `UPDATE`, `MERGE`, `INSERT OVERWRITE`, `writeTo().append()`, `overwrite`, `insertInto`,
   `saveAsTable` append), two inserts in a row, `rollback_to_snapshot`, `rewrite_data_files`, `expire_snapshots`
   after an insert. Through a child built before or after the write, a DataFrame view, a cached `CACHE TABLE` SQL
   view, a cached frame over a SQL view, a nested cache (parent and child both cached), and a cached join on
   either side.
2. **`REFRESH TABLE` and `refreshTable` re-cache** with the same rows: a cached `current_timestamp()` column shows a
   new value (`T|refresh_t`, `T|refresh_api_t`, `T3|refresh`).
3. **Spark keeps the fill** after a write to another table, a failed write (`UNRESOLVED_COLUMN`), a fresh read, a
   property change, a new branch, and another process's commit (`T|ext_process_t`; and `E|process_*` keeps 2, 2
   until the refresh, then 3, 3). A `newSession()` writer re-caches through the shared cache manager with the old
   count; RePark's `newSession()` cannot see the table (the named `N|*` residual), so those 5 cells are not
   comparable.
4. **A Dataset's own plan keeps the binding it had at `cache()`**, and a child's keeps its first action's
   (`O|cache_insert_collect`: the own `collect` after an insert answers the two old rows while `count` answers 3;
   `O|cache_insert_count_collect`, `O|own_count_collect`, `O|own_collect_collect`, `O|child_collect_twice`). A
   child's first own action reads the re-cached table (`O|child_count_collect`).
5. **A schema change with no new data:** a cached bare `spark.table(t)` keeps serving its fill after `ADD`, `DROP` or
   `RENAME COLUMN` (`A|bare|*`, `D|alter_drop`, `D|rename_col`: count 2, no error); a cached derived frame
   (`select`, `filter`, `withColumn`) raises `COLUMNS_MISMATCH` after a drop or rename and misses the cache after an
   add (a new timestamp at every read). With new data after the change every cached frame re-binds: the old columns
   over the new rows after an add (`D|alter_add`), `COLUMNS_MISMATCH` after a drop or rename (`D|alter_drop_insert`,
   `D|rename_col_insert`), `TABLE_ID_MISMATCH` after a recreate, `TABLE_OR_VIEW_NOT_FOUND` after a drop, and the frame
   stays cached.
6. **C-009** (catalog-surface-1) unpersists a cached frame when a fresh `spark.table(t)` or `isCached` sees a new
   snapshot id, so `isCached` answers `false` after a same-session write where Spark answers `true`
   (`U|is_cached_after_write`, `U|frame_cached_after_write`). Its pins assert `false` (rulings R-6, R-7).

**Against the domain table:** its main column for the four `C` rows is main before A1 and 1a; two are EQUAL today.
**Against the brief's design** ("dropped, or refilled on next use, when the map's epoch for a table the cached plan
reads has moved"): a pure epoch rule refills after a schema-only `ALTER`, and the refill re-binds through 1a's
rule, which raises `COLUMNS_MISMATCH` for a dropped or renamed column (read from 1a's rule and its measured
`drop_column_count` pin; no pure-epoch build was made). Spark keeps a cached bare table's fill there, so
`D|alter_drop`, `D|rename_col`, `A|bare|drop` and `A|bare|rename` (EQUAL on main) would be lost; the design below
keeps a fill whose table kept its data.

## 3. The design

A lazy cache table's fill is bound to the session table map (1a's `SessionTables`), read only:

- **What a fill read.** At registration the table walks the analysed lineage for 1a's bound Iceberg scans
  (`session_tables::binding`). During the fill, 1a's re-bind notes every bound scan it decides under the fill's memo
  (`ReaderParseMemo::note_read`), inside nested SQL views and, through `adopt`, inside nested caches. When the fill
  completes, each table it read becomes a dep: the map's epoch for it and the table the fill read. A dep whose current
  state no longer has the fill's data at that moment (a commit raced the fill) starts stale.
- **When a fill is stale.** At each use, a dep whose epoch has not moved holds. One whose epoch moved holds only if
  the table kept its UUID, its current snapshot and its last snapshot-log entry and its metadata location changed (a
  schema or property `ALTER`, a branch); the dep then moves to the new epoch and state. Anything else is stale: a data
  commit, a rollback or rewrite (a new log entry), a drop, a recreate, and a `REFRESH` (a new epoch over the same
  state). A stale fill is dropped (its cell re-armed) and the use refills under the live state, which re-binds through
  1a's rule: new rows, the old columns after an add, or the re-bind's condition, which keeps nothing.
- **Own-plan actions.** `cache()` notes the lineage's tables, at their current state, in the acting frame's memo
  (Spark fixes the frame's own plan there). An own-plan use whose memo holds a table whose data differs from what the
  cache would serve takes the pass-through under that memo (its own binding, nothing kept). A memo with no table
  reads the cache, and `adopt` now gives it the fill's tables and reads, so its later own-plan actions keep that
  binding.
- **What moves no epoch** keeps the fill: another table's write, a failed or refused write (1a's ENC-1 condition 2),
  a fresh read of an unchanged table, and another writer's commit on the local catalog. **On Glue and S3 Tables** a
  fresh read that sees another writer's commit records it (1a's Q2 (b)) and moves the epoch, so before delta 1 the
  next use refilled where Spark keeps the old fill until a refresh (the #1394 verifier's F1, by code trace). Delta 1
  (§9) has the dep read the map's local epoch, which that read does not move, so the fill stays.
- **C-009 is unchanged.** Its token check still evicts a cached frame when a fresh read sees a new snapshot; the
  lazy refill makes the eviction unnecessary for lazy caches, but its pins assert `isCached` `false` after a write
  (R-6, R-7), where Spark answers `true`. A question, below.

## 4. The files

Three product files, no fork file, none of ENC-1's list:

| file | change |
|---|---|
| `crates/repark-core/src/session/temp_views/lazy_cache.rs` | the deps, the staleness check, the re-arm, the own-plan pass-through and the registration pin (614 to 793 lines) |
| `crates/repark-core/src/extension.rs` | `ReaderParseMemo::note_read`, `reads`, `TableRead`; `adopt` copies tables and reads |
| `crates/repark-spark/src/view_ddl/frame_settings/held_iceberg.rs` | the re-bind notes each bound scan under a memo (3 lines) |

Reading the epoch and the current state (`SessionTables::epoch`, `current`, `session_tables::binding`) is all unit 2
takes from 1a's map; `session_tables/mod.rs`, `encryption_guard.rs`, every `impl Catalog for` and every commit
record are untouched. **Delta 1 (§9) adds a fourth product file,** `crates/repark-iceberg/src/catalog/session_tables/mod.rs`:
each entry records a local epoch, and the dep reads it in place of the epoch; it is on ENC-1's allowlist, so delta 1
is ENC-1: yes. Tests: `held_iceberg/tests.rs` (3 Rust pins) and the new pin file
`python/repark/tests/iceberg/held_frame/test_ice_held_frame_snapshot_1_cached.py`. **Delta 2 (§10)** adds the data
tick to `session_tables/mod.rs`, the `CacheLevel` split to `lazy_cache.rs` (re-exported through `temp_views.rs`,
`session.rs` and `lib.rs`), one keyword to `crates/repark-python/src/session.rs`, and the facade's level mark and
narrowed C-009 drop (`catalog_surface.py`, `dataframe/cache_handle.py`, one line of `dataframe/core.py`).

## 5. The bar

**The cells of this unit** (§1, both modes): `C|cache_count`, `C|cache_append` and `K|cache_held` move WRONG to
EQUAL; `C|cache_lazy` and `C|cache_table` stay EQUAL. **The probes' 97 cells per mode** ([evidence/unit-2/](evidence/unit-2/map.md)):
main 35 EQUAL and 62 WRONG per mode, the head 77 EQUAL and 20 WRONG; **42 cells per mode move WRONG to EQUAL, 0
EQUAL lost**. The 20 left per mode:

| cells per mode | what | why it stays |
|---|---|---|
| 13 | a cached derived frame (`select`, `filter`, `withColumn`) after a schema-only `ALTER`: `A|select_*|drop`, `rename`, `A|filter|drop`, `rename`, `A|ts|drop`, `rename`, `add`, `T3|alter_drop`, `rename_col`, `alter_add`, `T|alter_add_t` | Spark raises `COLUMNS_MISMATCH` (a drop or rename) or misses the cache at every read (an add); the head keeps the fill, as main does, because a bare frame must keep it (§2 point 5). Card line below |
| 4 | `E|process_*` (an external Spark process, then a refresh), `E|session_*` | the local hadoop catalog never sees another process's commit (1a's `refresh_sql_external`, sketch §5 "not a unit"); RePark's `newSession()` cannot see the table (the `N|*` residual) |
| 1 | `T|ext_session_t` | the same `newSession()` residual |
| 2 | `U|is_cached_after_write`, `U|frame_cached_after_write` | C-009 evicts where Spark keeps the frame cached; its pins assert the eviction (question below) |

**1a's bar** (`N/ice-1a-out/bar/run_v.sh`, `BAROUT=N/ice-u2-out/bar`, compared by the committed
[probes/barcmp.py](evidence/probes/barcmp.py) over step 0's raw cells): today's main equals 1a's fix-round run in
every verdict (1,730 EQUAL, 142 RAISE, 108 WRONG, 8 stored). The head: **1,734 EQUAL, 142 RAISE, 104 WRONG, 8
stored; moved 4 WRONG to EQUAL** (`C|cache_append` and `C|cache_count`, both modes), **0 EQUAL lost**. Of the 40
answers that differ from main, 36 differ only in a generated `_repark_jl_` or `_repark_jr_` view name inside an error
both engines raise, as in 1a's delta 1.

**A1's bar** (a turnstile copy of `N/cache-a1-out/probes/run_bar.sh` and its `bar_cmp.py`, on main's package and on
the head, each against Spark's recorded cells): the 188 required cells (F1 158, F5 8, F8 20, own-then-cache 2) and
the 128 forecast join cells are EQUAL on both. Of 2,812 cells, **6 move WRONG to EQUAL** (`M|ice_stale` `append`,
`ctable`, `sql`, both modes: A1's records gave them to this card) and **0 EQUAL are lost**.

**Three more cells, outside the 97** (the #1394 verifier's F2, class (c); main answers the same or worse):
`PR|set_current_same_ts` (`set_current_snapshot` to the current snapshot) and `X|expire_ts` (`expire_snapshots` with
no other change), where Spark re-caches and only a cached `current_timestamp()` column shows it, while the head keeps
its fill; and `RC|cache_twice` (a second `cache()` after a write), where Spark re-pins the frame's own `collect` to the
new rows and the head's `count` is right but its own `collect` still gives the two old rows.

**The pins** on the head package: the held-frame directory 141 passed (unit 2's 68, 1a's 60 Spark cells and its 13
ENC-1 cells); on main the 68 new cells give 50 red, 18 green. The Rust pins: `held_iceberg::tests` 10 passed (3 new).
**With main merged** (#1391 at `d5db1adf`, #1393 at `108d665c`), on the pre-delta package: the held-frame directory
gives 186 passed and 4 xfailed at `108d665c`, which is the verifier's 163 at `d5db1adf` (141 and #1391's 22) and
#1393's live twin (23 passed, and 4 xfailed on `type=hadoop`, ICE-CATALOG-SESSION-HADOOP-1); `held_iceberg::tests`
gives 12 (#1391's 2 more). Delta 1's counts are in §9.
catalog-surface-1's C-009 pins and A1's pin files pass in the facade run (§7).

**The mutation proof** (`N/ice-u2-out/mutant.diff`, built in a scratch worktree of the head, never in the lane
clone): the staleness check returns "fresh" for every fill. On that package `C|cache_count` and `C|cache_append` go
back to 2, 2, 3 and `K|cache_held` to 2, 2, in both modes; `C|cache_lazy` and `C|cache_table` stay 3, 3, since they
were EQUAL before this unit and do not reach the check. The unit's pin file: 48 of 68 red (24 shapes: every refill,
re-bind and refresh cell); the 20 green are the kept-fill cells and the own-plan cells, which the own-plan
pass-through answers without the check. 1a's 73 pins pass on the mutant.

## 6. Cost

Unit 2's cost record is this section: the tree has no `costtables.md`, and [cost.md](cost.md) is 1a's. Delta 1 took
no cost sample (§9).

Every sample ran under the measure lock's exclusive side, through its turnstile wrapper, on cores 48-57, base (main's package) against the head against base again; medians, and "head / mean" is
the head over the mean of the two base runs. 1a's runner (`N/ice-1a-out/cost/run_ts.sh`, `BASE` set to main's
package, `OUTC=N/ice-u2-out/cost`) did C-012 and 1a's held rows; a copy of A1's runner (`N/ice-u2-out/cost_run.sh`,
`MAIN` main's package, its lock the turnstile, `C012_RUNS=0` since 1a's runner measured C-012) did A1's cache rows.
The bar, the mutant and the gates ran in the same window under the shared side, so the samples waited for them; no
sample ran beside them.

| case | n | base | head | base again | head / mean |
|---|---|---|---|---|---|
| C-012 `c012p6` (s) | 5 | 39.55 | 39.64 | 39.47 | **1.003** |
| C-012 `c012rest` (s) | 5 | 43.03 | 43.02 | 43.03 | **1.000** |

**C-012 passes** (all 30 replays exit 0).

| row (ms) | n | base | head | base again | head / mean |
|---|---|---|---|---|---|
| `fresh_write` | 10 | 12.91 | 12.89 | 13.27 | 0.985 |
| `held_filter_count` | 10 | 11.99 | 11.66 | 11.82 | 0.980 |
| `held_nowrite` | 10 | 10.75 | 10.63 | 9.05 | 1.073 |
| `held_write` | 10 | 11.85 | 11.68 | 10.55 | 1.042 |
| `plan_100` | 10 | 0.52 | 0.54 | 0.51 | 1.043 |

| A1 row, mark (ms from start) | n | main | head | main again | head / mean |
|---|---|---|---|---|---|
| `collect_first` build | 10 | 9.0 | 9.1 | 9.0 | 1.004 |
| `collect_first` collect | 10 | 2023.5 | 2048.6 | 2064.0 | 1.002 |
| `collect_first` count | 10 | 2033.6 | 2060.7 | 2074.6 | 1.003 |
| `count_collect` build | 10 | 9.0 | 9.0 | 9.2 | 0.989 |
| `count_collect` collect | 10 | 2071.8 | 2084.3 | 2078.6 | 1.004 |
| `count_collect` count | 10 | 309.9 | 330.7 | 325.3 | 1.041 |
| `second_child` first | 10 | 321.4 | 347.0 | 339.7 | 1.050 |
| `second_child` second | 10 | 324.4 | 349.9 | 342.9 | 1.049 |
| `selective_child` build | 10 | 9.1 | 9.0 | 9.0 | 0.997 |
| `selective_child` child | 10 | 357.3 | 263.0 | 305.9 | 0.793 |
| `view_count` count | 10 | 360.1 | 334.9 | 356.8 | 0.934 |
| `view_count` register | 10 | 9.2 | 9.1 | 9.1 | 1.000 |

- **1a's held rows** (`held_*` on a 2M-row Iceberg table): one row is over 1.05x, `held_nowrite` at 1.073, a flag. Its
  two base runs differ by 19% (10.75 and 9.05 ms) at n=10, and the head sits between them. **Attributed by
  instruction counts** (`perf stat -e instructions:u`, the same held 2M-row `toArrow` with no write, a process at 40
  actions less the process at 0, per action, two rounds per build, under the exclusive lock): main 264.41 M and
  264.67 M, the head 264.76 M and 265.25 M, **+0.2% (1.002x)**. The head adds one memo note per bound scan to a held
  own-plan action and nothing else on this path; the cycles row is code layout (this box moves cycles up to about
  10% between builds at identical instructions). `held_write` 1.042 and `plan_100` 1.043 (0.52 against 0.54 ms) are
  under the line.
- **A1's cache rows** (JSON readers, no Iceberg table, so no dep and no epoch read): every row is at or under 1.05x;
  `second_child` reads 1.050 and 1.049 with main and main again 5.7% apart, and the selective child 0.793. Nothing
  is over 1.15x.

## 7. Gates

On the head `a9bbac9b` (product `eac90dd7`) and the package `pkg-iceu2-eac90dd7`, `N/ice-u2-out/gates.sh` with
`ONLY=static,clippy,cargo,facade` (a copy of 1a's, pointed at this clone, `UV_OFFLINE=1` on every `uv` call, the
comment gate against `0cc65e9a`):

| gate | exit | count |
|---|---|---|
| `cargo fmt --check`, rust file size, lib-py, map-size, map-sync | 0 | |
| `typos`, `ruff check`, `ruff format --check` | 0 | |
| the comment gate against `0cc65e9a` | 0 | 0 hits |
| `make rust-clippy`, `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-iceberg` | 0 | 1,081 passed |
| `cargo test --locked -p repark-core` | 0 | 1,659 passed |
| `cargo test --locked -p repark-spark` | 0 | 2,995 passed |
| `make py-test-facade FACADE_WORKERS=6` | 0 | 41,442 passed |
| the package facade at `-n 6` | 0 | 41,447 passed |
| docs-links, and `make py-test` | 0 after the records commit | 1,133 passed |

The first run of docs-links (and the one `make py-test` pin that runs it) failed only on this record's
`evidence/unit-2/`, linked before it was tracked; both pass on the records commit.

## 8. Card lines and questions

- **ICE-CACHED-DERIVED-ALTER-1 (S3, a named divergence on main and the head; not label-blocking by this record's
  reading).** A cached *derived* Iceberg frame (`select`, `filter`, `withColumn` over `spark.table(t)`) read after a
  schema-only `ALTER` of `t` answers its old fill: Spark raises `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH`
  after a dropped or renamed column, and after an added column it misses the cache and recomputes at every read (a
  cached `current_timestamp()` changes each time; the rows equal). 13 cells per build and mode (§5). A cached bare
  `spark.table(t)` keeps its fill on Spark too, which is why this unit keeps a fill whose data did not move. The rows
  returned are the table's rows (no new data was committed), so nothing old is stored.
- **Question (C-009, catalog-surface-1, rulings R-6/R-7).** With the lazy cache now refilling after a same-session
  commit, C-009's eviction is no longer what keeps a cached frame fresh for lazy caches, and it answers
  `isCached` `false` after a same-session write where Spark answers `true` (2 cells per mode). Its three pins assert
  `false` (`test_cache_table_insert_serves_fresh_rows`, `..._insert_overwrite_...`, `..._writer_append_...`). Options:
  (a) keep C-009 as is (today's state, the pins hold); (b) skip the eviction for a frame whose cache is a lazy table,
  keep it for the eager fallback and for views, and re-pin the three `isCached` asserts to Spark's `true`. Default
  (a) in this unit, since the brief holds the C-009 pins. **Delta 2 (§10)** takes (b) for frames only, as the
  owner's rule needs: a lazily held table-level frame is no longer evicted (`U|frame_cached_after_write` answers
  Spark's `true`); the `cacheTable` entry and its three pins are unchanged, so `U|is_cached_after_write` stays.
- **Glue and S3 Tables.** A fresh read that sees another writer's commit records it (1a's Q2 (b)) and moves the
  epoch; before delta 1 a cached frame then refilled where Spark keeps its fill until a refresh (F1). Delta 1 keeps the
  fill (§9, by Rust pins with a second session as the other writer). The AWS acceptance leg should still carry "cache
  a frame; an external commit; a fresh read; the cached frame's `count`" (Spark: the old count).

## 9. Delta 1 (2026-11-21): a cached fill holds across another writer's commit

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh: the review cap's one delta, on the #1394 Opus verifier's FAIL of
`d5db1adf` for F1 alone (F2 to F4 are class (c), recorded in §3, §5, §6 and [cards.md](cards.md)).

**F1 (class (a), S2, by code trace on the label's catalogs).** On Glue and S3 Tables a plain fresh read
(`spark.table(t)` or any SQL read) that sees another writer's commit moves the session table map's epoch: `load_table`
calls `observe` with `refresh=false`, and a state other than the recorded one ticks. The dep then saw a moved epoch
over new data, and the cached frame refilled. The cell: `d = spark.table(t).filter("n > 0").cache(); d.count()`, then
another writer's `INSERT`, then `spark.sql("SELECT count(*) FROM t")`, then `d.count()`:

| engine | last `d.count()` | why |
|---|---|---|
| Spark 4.1.2 | 2 | `CacheManager` keeps the fill until a same-session write or a refresh (`T|ext_process_t`, `E|process_*`) |
| main `dac2af61` | 2 | its lazy cache has no deps; C-009 evicts only the frame `session_table` returned |
| `108d665c` | 3 | the fresh read moved the epoch, so the use refilled |
| delta 1 | 2 | the fresh read moves the epoch and not the local epoch, so the fill holds |

No local catalog shows a second writer (`type=hadoop` is a memory catalog per session), so the delta's answer is
pinned in Rust with a second session over the same memory catalog as the other writer.

**The seat: a local epoch in the session table map** (`crates/repark-iceberg/src/catalog/session_tables/mod.rs`), the
brief's first shape. Each entry keeps `LocalEpoch { changed, refreshed }` beside its epoch, read through
`SessionTables::local_epoch`:

- `record` (every same-session commit, create, register, drop and rename) sets `changed` to its tick;
- a refresh load (`REFRESH TABLE`, `catalog.refreshTable`) sets both to its tick;
- a plain load that sees a new state moves the epoch and the state, as before, and carries the local epoch unchanged;
  a new entry starts with `changed` at its tick and `refreshed` at 0.

The dep (`lazy_cache.rs`) records `changed` at the fill. At a use: unchanged, the fill holds; `refreshed` past the
dep's tick (a refresh since the fill), it refills whatever the state; otherwise #1394's rule (kept, and the dep moves,
when the table keeps the fill's data; else a refill).

Why this seat, and not the other:

- The dep cannot tell why an epoch moved: a same-session commit and another writer's commit leave the same kind of
  new state. Only the map knows the cause, at the moment it records. The second shape (the dep records why each tick
  happened) needs that same cause from the map and a per-tick log as well, so it is the larger change.
- The epoch stays as it was: the race guard compares it (a load that began before a newer state never puts its older
  table back, another writer's included), the held frames read the state it orders, and its frozen signature
  (`table_epoch`, `SessionTables::epoch`) and tests are unchanged.
- `refreshed` is the one extra counter. With `changed` alone, a refresh after another writer's property-only change
  would find new metadata over the fill's data and keep the fill, where Spark re-caches at every refresh (§2
  point 2).

**What it does not change:** what a load fetches, the state it records, and what the map serves or refuses; the epoch;
held frames; an uncached fresh read, which still sees another writer's commit (the pins assert it); C-009.

**What it adds to an uncached read:** nothing on a load of an unchanged state (every repeat read takes the same early
return); on a load that sees a new state, the entry copies the 16-byte local epoch inside the lookup and insert it
already made. No new lookup, lock, allocation or catalog call. A commit's `record` makes one more lookup under the lock
it holds. No cost sample was taken.

**ENC-1: yes, by the brief's rule**, since the map now writes a new field into each entry. Its three conditions
(1a's map) still hold: the decorator wraps only the guarded handle; the local epoch moves only after a successful
inner call, and the four refusal tests now also hold the local epoch; and no table is loaded, served or refused
differently.

**The pins.** Four in `held_iceberg/tests.rs`, over a second Spark-door session registered on the same memory catalog
(another writer whose commits the first session's map never records), and one in `session_tables/tests.rs`. Each
held-frame pin caches the frame, fills it (`[1, 2]`), lets the other writer insert `9`, takes a fresh SQL read (it
answers `[1, 2, 9]` and moves the epoch: `observe` with `refresh=false` over a new state), and asserts the fill is kept
(the same rows and buffer); then its own step. A read compared by buffer keeps its batches alive, so its address
cannot be reused by a later fill. The same test file ran on all three trees:

| pin | main `5a3db151` | `108d665c` | delta 1 |
|---|---|---|---|
| `a_cached_frame_keeps_its_fill_when_a_fresh_read_sees_another_writers_commit` | pass | **fail**: `[1, 2, 9]`, a refill | pass |
| `a_cached_frame_kept_over_another_writer_refills_after_a_same_session_commit` (`[1, 2, 8, 9]`, a new buffer) | **fail**: `[1, 2]`, no refill | **fail** at the kept step | pass |
| `a_cached_frame_kept_over_another_writer_refills_after_a_refresh` (`REFRESH TABLE` refills; the other writer's property-only change then keeps the refill, and a second refresh refills again) | **fail**: `[1, 2]`, no refill | **fail** at the kept step | pass |
| `a_fresh_read_of_an_unchanged_table_after_another_writer_moves_nothing` (the epoch and the fill stay) | pass | **fail** at the kept step | pass |
| `session_tables::tests::a_load_of_another_writers_commit_moves_the_epoch_and_not_the_local_epoch` | not run | does not build (`local_epoch` is new) | pass |

Main keeps every fill, so it passes the two keep pins and fails the two refill pins; `108d665c` refills after the
fresh read, so all four fail at the kept step; delta 1 passes all five. The map test drives the map directly: a commit
through the guard's own handle, then a session load, moves the epoch and the state and not the local epoch; a reload
moves nothing; a refresh sets both local ticks; an own commit moves only `changed`. The ENC-1 refusal tests'
`assert_unmoved` now also holds the local epoch.

**The one-counter mutant** (the dep's `refreshed` check deleted, in a scratch copy of the delta, never in the lane
clone): 15 of the 16 `held_iceberg::tests` pass, and the refresh pin fails at its second refresh (the same buffer: the
other writer's property-only change kept new metadata over the fill's data, so the refresh kept the fill). So the
second counter is what makes every refresh refill.

**The local cells are unchanged.** On the delta's package (`BUILT_FROM` `7ebe4502`) the builder's three probes (97
cells per mode) and the verifier's `vcells.py` (43 per mode) answer exactly as the pre-delta head did in the verifier's
run: 0 answers differ out of 194 and 86. Against Spark: main 35, the head 77 and delta 1 77 EQUAL per mode on the 97;
main 15, the head 36 and delta 1 36 per mode on the 43; **0 EQUAL lost**. The held-frame directory 186 passed and 4
xfailed (as on `108d665c`); A1's and C-009's pin files 111 passed, and their siblings 43.

**Gates** (head `7ebe4502`, package `pkg-d1-7ebe4502`), each exit 0 unless noted:

| gate | exit | count |
|---|---|---|
| `cargo test --locked -p repark-iceberg` | 0 | 1,082 |
| `cargo test --locked -p repark-core` | 101, then 0 | 1,706; the first run's one failure, `microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope`, was its time bound under the facade run's load, and the rerun passed |
| `cargo test --locked -p repark-spark` | 0 | 3,271 |
| `make rust-clippy`, `make rust-panic-ban`, `cargo fmt --check` | 0 | |
| the facade `python/repark/tests/` at `-n 6` on the package | 1 | 41,487 passed, 5 failed: 4 start a child Python without `PYTHONPATH` by design (`test_session_package_imports_without_a_cycle` pops it, the 3 `test_cross_release_local` cells run `-I`), and `test_pg_acceptance` reads a repository-relative path (3 passed from the repository root); the same 5 fail the same way on the pre-delta package |
| docs-links, python-conventions, map-sync, map-size, `typos`, `ruff check`, `ruff format --check` | 0 | |
| the comment gate against `5a3db151` | 0 | 0 hits |

**Left as it was (named, not measurable locally):** another writer's commit, a fresh read, then a same-session
property-only `ALTER`: the dep compares the fill's data with the commit's, which carries the other writer's snapshot,
so it refills where Spark keeps the fill (closed for table-level caches by delta 2, §10; the re-check's G2). The same holds for a refill raced by another writer's commit observed during
the fill (the dep starts stale).

## 10. Delta 2 (2026-11-22): the fill follows what is cached

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh: one more targeted change after the review cap, which the owner
adopted on 2026-11-22 together with both verifiers again, on the delta-1 re-check's G1, G2, G4 and G5 (G3 is the PR
body, below) and the live baseline of main `550789fd` (run 44985182689, all 8 held-frame tests passed).

**The rule** (measured by Fable on Spark 4.1.2 with Iceberg 1.11.0, a second Spark process as the other writer;
LIVE-SECOND-WRITER-1 in [live-aws.md](live-aws.md)):

- **A table-level cache** (`spark.table(t).cache()`, `spark.read.table(t)`, `spark.read.format("iceberg").load(t)`
  with no time-travel or incremental option, the same under `.alias(...)`, `CACHE TABLE t`) keeps its rows
  until `REFRESH TABLE`, `spark.catalog.refreshTable` or a same-session data commit to the table. A same-session
  metadata-only commit (`SET TBLPROPERTIES`, `ADD COLUMN`, `CREATE BRANCH` or `TAG`, a write to another branch) and
  another writer's commit seen by a fresh read do not unpin it.
- **A query-level cache** (anything else: `spark.sql("SELECT * FROM t")`, `select`, `filter`, `limit`, `orderBy`,
  `distinct`, `groupBy().count()`, a DataFrame temp view read by name, a self-join) answers what the session's latest
  read of the table answers. RePark makes no catalog call on a held action, so before any fresh read it may still
  answer the old state.
- Not in this delta: a fresh read or a new frame while the table itself is cached (Spark answers the cache, RePark
  the committed state; [FRESH-READ-WHILE-TABLE-CACHED-1](../../../roadmap/mid-term/cards-2026-11-21/fresh-read-while-table-cached-1-card-2026-11-22.md)).
  It is unchanged.

So delta 1's premise (every fill holds until a same-session write or a refresh, G1) is right only for the table-level
row, and delta 1 refilled a table-level fill after another writer's commit and a same-session metadata commit (G2).

**The seat, in four parts.**

1. **A data tick in the session table map** (`session_tables/mod.rs`). `LocalEpoch` gains `data`. `record` moves it
   only when the committed state's data differs from the previous entry's (`same_data`: the UUID, the current
   snapshot and the last snapshot-log entry, moved here from the lazy cache so both read one test); a refresh load
   moves it; a plain load carries it, as it carries the rest of the local epoch. A metadata-only commit therefore
   moves `changed` and not `data`, and so does one made over another writer's data, since the load that the commit
   makes first has already recorded that data as the previous entry. `ticks` reads the epoch and the local epoch
   under one lock. **G4:** a refresh whose load returns on the race guard now takes a tick and sets `changed`,
   `data` and `refreshed` to it; the state and the epoch stay, so the guard still never puts an older table back.
2. **The dep splits by level** (`lazy_cache.rs`). `register_lazy_cache_view` takes a `CacheLevel` and the fill's
   deps record `Ticks`. `Table` holds while `data` is the fill's. `Query` refills when `refreshed` moved, holds while
   the epoch is the fill's, and otherwise keeps the fill only when the recorded table keeps the fill's data (the
   pre-delta-1 rule for derived frames). Ticks are compared for equality, not order, so the G4 tick (which the epoch
   does not take) never makes a fill refill twice.
3. **Provenance in the facade.** `spark.table(t)` and `spark.sql("SELECT * FROM t")` both build `SELECT * FROM <ref>`,
   so the plan cannot tell them apart; the frame's history can. `session_table` marks a frame it builds over a table
   (not a temp view), `catalog_surface.cache_table` (`cacheTable`, `CACHE [LAZY] TABLE`) marks its frame, the travel-free
   `format("iceberg").load(t)` return in `session/reader.py` marks its frame (the Opus verifier's A1, 2026-11-22), and
   `DataFrame.alias` passes the mark to its child (`cache_handle._aliased`, a one-line change in `core.py`, which
   stays on its exact baseline). Every other transformation builds an unmarked frame. `_bind_lazy` passes the mark
   to the native registration. That is Spark's test (the cached plan is the table relation, maybe under an alias)
   read off how the frame was built.
4. **C-009's snapshot-token drop** (`_drop_stale_identity_frames`) skips a frame that `_held_by_table_fill` finds:
   marked, and its cache view a lazy cache table. That drop is what unpersisted a cached `spark.table(t)` at the next
   `spark.table(t)` or `isCached` after another writer's commit (main's `bare` held-again 3). An eager cache (the
   MemTable fallback) and a view still take the drop. The `cacheTable` entry check in `session_table` and
   `isCached` is untouched, so what a fresh `spark.table(t)` answers while the table is cached does not change. This
   is §8's C-009 option (b) for frames only; the three C-009 pins are on the `cacheTable` entry and still hold.

**ENC-1, unchanged in kind.** No new table provider; what a load fetches, records as state, serves and refuses is
unchanged; the map writes one more field per entry. A refused commit, refill or failed load moves no tick (every
`record` follows the inner call's `?`, `observe` follows the load's `?`, deps are built only in the fill's `Ok` arm;
the four refusal tests' `assert_unmoved` compare the whole `LocalEpoch`). Nothing outside the map sets a tick:
`Entry`, `observe` and `record` stay private, and `Ticks` and `LocalEpoch` are `Copy` values.

**The event table, re-traced.** "Table" and "query" are what a fill at that level does at its next use; main is
`550789fd` (no #1394); `[M]` live baseline or local Spark, `[R]` read.

| event | data tick | epoch | table-level fill | query-level fill | Spark | main | head |
|---|---|---|---|---|---|---|---|
| same-session data commit (`INSERT`, `DELETE`, `MERGE`, overwrite, `writeTo`) | ticks | ticks | refills | refills | re-caches both | keeps; a bare frame is unpersisted at the next `spark.table` | = Spark |
| same-session metadata-only commit (`SET TBLPROPERTIES`, `ADD COLUMN`, `CREATE BRANCH`, a branch write) | — | ticks | keeps | keeps if the recorded data is the fill's | table keeps; query answers the fresh read | keeps | = Spark |
| another writer commits, before any load | — | — | keeps | keeps | table 2; query 2 inside Iceberg's 30 s window, else 3 `[M]` | 2 | 2 (allowed: no catalog call) |
| a load that sees another writer's state (fresh read, new frame, `DESCRIBE`) | — (carried) | ticks | keeps | refills | table 2; query what a fresh read answers `[M]` | 2; bare unpersisted at the next `spark.table` (3) | table 2, query 3 |
| a load of an unchanged state | — | — | keeps | keeps | keeps | keeps | = |
| `REFRESH TABLE`, `refreshTable` | ticks | ticks | refills | refills | re-caches (3) `[M]` | bare 3 (unpersisted); alias, query shapes 2 | 3 |
| a refresh that loses the race guard (G4) | ticks | — | refills | refills | re-caches | keeps | = (was W on delta 1) |
| another writer, a fresh read, a same-session metadata-only commit (G2) | — | ticks | keeps (2) | the fresh read's (3) | bare 2, derived 3 `[M]` | 2, 2 | = (delta 1: bare 3) |
| create or register after a drop, a recreate | ticks | ticks | refill refuses `TABLE_ID_MISMATCH` | the same | the same | keeps | = (as before) |
| drop | ticks | ticks | refill refuses `TABLE_OR_VIEW_NOT_FOUND` | the same | the same | keeps | = (as before) |
| rename | source ticks (`Dropped`); destination forgotten | ticks | refill refuses or re-binds | the same | the same | keeps | = (as before) |
| snapshot procedures (rollback, `set_current_snapshot`, cherry-pick, fast-forward) | ticks | ticks | refills | refills | re-caches | keeps | = (same-snapshot `set_current` stays (c)) |
| `expire_snapshots` alone | — | ticks | keeps | keeps | re-caches (same rows) | keeps | W, as before ((c), F2) |
| another writer drops and recreates, then a fresh read | — (carried) | ticks | keeps | refill refuses `TABLE_ID_MISMATCH` `[R]` | table keeps; query not measured | keeps | table =; query not measured |
| a commit observed during a fill (residual 2) | — | — | refills once | refills once | — | keeps | unchanged ((c), S3) |
| a later `spark.table(t)` or `isCached(t)` after the snapshot moved | — | — | stays cached | (not an identity frame) | stays cached (`is_cached` true) `[M]` | bare unpersisted | = Spark (eager caches keep the drop) |
| a fresh read or new frame while the table is cached | — | — | — | — | 2 (the cache) `[M]` | 3 | 3 (open card, unchanged) |

**The live shapes, predicted for the head** (each step: held, fresh, new, held again; `[R]`):

| shape | after the writer | after the property commit | after `REFRESH TABLE` | the rule |
|---|---|---|---|---|
| `bare`, `alias` | 2, 3, 3, 2 | 2, 3, 3, 2 | 3, 3, 3, 3 | passes (fresh and new open) |
| `cache_table` | 2, 3, 3, 3 | 3, 3, 3, 3 | 3, 3, 3, 3 | all open (as main) |
| `sql_star`, `filter`, `agg`, `df_view` | 2, 3, 3, 3 | 3, 3, 3, 3 | 3, 3, 3, 3 | passes |

**`PENDING_1394`:** 27 reads before, none after (below). The twin pins main's two predictions breaking the rule at
exactly those 27 reads and the head's prediction passing with no exemption. Main's live answers are those 27 reads'
own evidence (the baseline: `bare` and the query shapes held again 3 or 2 where the rule wants the fresh read or 2;
`alias` held 2 after `REFRESH TABLE`).

**The pins.** In Rust, the other writer is a second Spark-door session registered on the same memory catalog (its
commits never reach the first session's map); each held cell fills `[1, 2]`, lets it insert `9`, reads the cache once
before any fresh read (kept: no catalog call), then takes a fresh SQL read (`[1, 2, 9]`, the epoch moves). A read
compared by buffer keeps its batches alive. The same test files ran on the head and, with only the test helper's
level argument dropped (those trees have no `CacheLevel`), on `e0cb360d` (delta 1) and `108d665c` (before delta 1),
every source touched so the shared target rebuilt them:

| pin | `e0cb360d` | `108d665c` | head |
|---|---|---|---|
| `levels::a_table_level_fill_holds_over_another_writer_and_a_metadata_commit_until_a_refresh` (W kept, P kept, R refilled) | **fail**: `[1, 2, 9]` after `SET TBLPROPERTIES` (G2) | **fail**: `[1, 2, 9]` after the fresh read (F1) | pass |
| `levels::a_table_level_fill_holds_over_each_metadata_only_commit_after_another_writer` (`SET TBLPROPERTIES`, `ADD COLUMN`, `CREATE BRANCH`) | **fail**: `[1, 2, 9]` (G2) | **fail**: `[1, 2, 9]` | pass |
| `levels::a_query_level_fill_answers_the_latest_read_of_its_table` (W refilled, an unchanged read and P kept, R refilled, then a property commit keeps the refill: G5) | **fail**: `[1, 2]` after the fresh read (G1) | pass | pass |
| `levels::the_level_and_not_the_plan_decides_after_another_writer` (the table, an alias, a filter, an aggregate, at both levels) | **fail**: `Query` kept | **fail**: `Table` refilled | pass |
| `levels::both_levels_refill_after_a_same_session_data_commit_over_another_writer` (`[1, 2, 8, 9]`) | pass | pass | pass (a guard) |
| delta 1's four, now at `Table` | pass | **fail** at the kept step (F1) | pass |
| unit 2's three base cells, now at both levels | pass | pass | pass |
| `session_tables::tests::a_refresh_that_loses_the_race_guard_still_moves_refreshed` (G4) | **fail**: `after.refreshed > local.changed` | not built (no local epoch) | pass |
| `session_tables::tests::a_same_session_metadata_commit_moves_changed_and_not_the_data_tick`, `a_metadata_commit_over_another_writers_data_keeps_the_data_tick` (G2's sequence; G5: a commit carries `data` and `refreshed`) | not built (`data`, `ticks` are new) | not built | pass |

Facade pins, `python/repark/tests/iceberg/held_frame/test_ice_held_frame_snapshot_1_cache_levels.py`, both ANSI modes:
after a same-session insert and a later `spark.table(t)` (or `isCached(t)`), a cached `spark.table(t)` stays cached
and answers 3, as Spark 4.1.2 does (measured locally, `S/u2-delta2/raw/samesession-spark-*.jsonl`:
`[2, 3, true, 3, true]` and `[2, true, true, 3]`); a property commit and an alias are guards; each of ten shapes caches
at Spark's level and passes it to the native registration; `cacheTable`, `CACHE TABLE` and `CACHE LAZY TABLE` cache at
the table level; an eager cache keeps the snapshot-token drop. On the delta-1 package (`pkg-d1-7ebe4502`, its product
equal to `e0cb360d`) 32 of the 36 fail: the two Spark cells answer `is_cached` false (`[2, 3, false, 3, false]`,
`[2, false, false, 3]`), and the 28 level cells find no level (`_table_level` does not exist).

**The one-tick mutant** (the table-level dep reads `changed` in place of `data`, in a scratch copy only): 18 of the 21
`held_iceberg` tests pass; the three that fail are the ones that hold a table-level fill over a same-session
metadata-only commit: the base cell's `Table` pass at `SET TBLPROPERTIES` (a new buffer), and the two
`levels::a_table_level_fill_*` cells (`[1, 2, 9]`). So the data tick is what keeps that fill.

**Nothing regressed.** On the head's package (`BUILT_FROM` `0b51d3fa`), the three probes (97 cells per mode) and the
verifier's `vcells.py` (43 per mode), against the verifier's `raw` and `rawv` and delta 1's answers:

| probe | cells (both modes) | answers that differ from delta 1 | EQUAL main / pre-delta head / delta 1 / delta 2 | EQUAL lost |
|---|---|---|---|---|
| u2cells | 140 | 2 (`U|frame_cached_after_write`, both modes: WRONG to EQUAL, `is_cached` true) | 44 / 124 / 124 / 126 | 0 |
| u2ts | 14 | 0 | 4 / 8 / 8 / 8 | 0 |
| u2alter | 40 | 0 | 22 / 22 / 22 / 22 | 0 |
| vcells | 86 | 0 | 30 / 72 / 72 / 72 | 0 |

**Gates** (head `0b51d3fa` for cargo and the package; the pin-file fix `997d2ead` is tests only):

| gate | exit | count / note |
|---|---|---|
| `cargo test --locked -p repark-iceberg session_tables` | 0 | 17 passed |
| `cargo test --locked -p repark-spark held_iceberg` | 0 | 21 passed |
| `cargo test --locked -p repark-core temp_views` | 0 | 17 passed |
| the same pins on `e0cb360d` / `108d665c` (helper shim only) | 101 / 101 | 4 held and 1 map fail / 7 held fail, as tabled |
| the one-tick mutant | 101 | 3 of 21 fail, as tabled |
| `make rust-clippy`, `make rust-panic-ban`, `cargo fmt --all --check` | 0 | |
| held-frame directory (package) | 0 | 260 passed (after `997d2ead`; the first run's 36 failures were that file's module-scoped session, which the suite's autouse reset stops) |
| A1's `test_temp_view_cache_lazy_parse_1.py` and C-009's `test_catalog_surface_1.py` | 0 | 111 passed |
| A1's siblings `test_eager_budget_1.py`, `test_cache_persist.py` | 0 | 43 passed |
| the three probes and `vcells.py`, both modes | 0 (8 runs) | 0 EQUAL lost, above |
| the full facade `python/repark/tests/` at `-n 6`, from the repository root | 1 | 41,608 passed, 823 skipped, 180 xfailed, 40 failed: the 36 of the pin file before `997d2ead` (36 of 36 pass after it), and the 4 that start a child Python without `PYTHONPATH` by design (the 3 `test_cross_release_local` cells, `test_session_package_imports_without_a_cycle`), which fail the same way on the delta-1 package (delta 1's `facade-five-*.log`) |
| `make check-docs-links check-python-conventions`, `sync_map_md.py --check`, `check_map_size.py` | 0 | |
| `typos` 1.47.2, `ruff` 0.15.22 `check .` and `format --check .`, `check_docstring_presence.py`, `check_lib_py.py` | 0 | |
| the comment gate against `5a3db151` | 0 | 0 hits |

Two maps sit exactly at their size ceilings and were not touched: `python/repark/src/repark/spark/map.md` (its
C-009 paragraph still reads true for the `cacheTable` entry) and `crates/repark-python/src/map.md` (one keyword);
the lockstep check is advisory. The change to `catalog_surface.py` is recorded in the dataframe map's
`cache_handle.py` row and here.

**What stays open.**

- FRESH-READ-WHILE-TABLE-CACHED-1 (a fresh read or new frame while the table is cached), unchanged; every
  `cache_table` read on the RePark leg stays open.
- `cacheTable` then a same-session write: `isCached` answers false where Spark answers true (`U|is_cached_after_write`,
  the `cacheTable` entry path, which the brief keeps); C-009's three pins assert it.
- `REFRESH TABLE` and `uncacheTable` unpersist a cached `spark.table(t)`, so its `is_cached` is false after a refresh
  where Spark re-caches; the rows are equal (3). Not measured, predates this unit.
- `format("iceberg").load(t)` with a time-travel or incremental option builds an unmarked (query-level) frame;
  Spark's level for those shapes is not measured.
- A frame derived from a cached `spark.table(t)`, or a temp view of one, is query-level but reads the base's cache
  view, so by trace it answers the base's pinned rows as Spark does; only a live run can measure it (the Opus
  verifier's C1).
- A query-level fill before any fresh read answers the fill (no catalog call; the rule allows it), and Spark's fresh
  read inside Iceberg's 30 s catalog cache answers 2 where RePark's answers 3 (the rule allows it).
- Residual 2 (a commit observed during a fill), `expire_snapshots` alone and a same-snapshot `set_current_snapshot`,
  as before.
- **The live proof comes after the merge**: the acceptance workflow runs on main only. The revert trigger: an
  asserted live cell green on the baseline (run 44985182689) and red on the first acceptance run after the merge
  means revert that day.

## 11. The live acceptance run after the merge (2026-11-22)

#1394 merged as `7dd27ff5`. Run 45027067515 on that commit (dispatched 13:21Z) passed every leg, all 8 held-frame
tests included, so the revert trigger did not fire. Glue and S3 Tables gave identical answers on both legs. Each
group is held, fresh `SELECT count(*)`, new frame, held again; W after the other writer's insert, P after a
same-session `SET TBLPROPERTIES`, R after `REFRESH TABLE`.

| shape | Spark W / P / R | baseline RePark (`550789fd`) | RePark after #1394 |
|---|---|---|---|
| `bare` `spark.table(t).cache()` | 2222 / 2222 / 3333 | 2333 / 3333 / 3333 | 2332 / 2332 / 3333 |
| `alias` | 2222 / 2222 / 3333 | 2332 / 2332 / 2332 | 2332 / 2332 / 3333 |
| `cache_table` `CACHE TABLE t` | 2222 / 2222 / 3333 | 2333 / 3333 / 3333 | 2333 / 3333 / 3333 |
| `sql_star` | 2222 / 3333 / 3333 | 2332 / 2332 / 2332 | 2333 / 3333 / 3333 |
| `filter` | 2222 / 3333 / 3333 | 2332 / 2332 / 2332 | 2333 / 3333 / 3333 |
| `agg` | 2222 / 3333 / 3333 | 2332 / 2332 / 2332 | 2333 / 3333 / 3333 |
| `df_view` | 2222 / 3333 / 3333 | 2333 / 3333 / 3333 | 2333 / 3333 / 3333 |

The held reads of `bare` and `alias` now keep their fill through the other writer's commit and the property commit
and refill on `REFRESH TABLE`, as Spark does. The query-level shapes answer the session's latest read. The fresh read
and new frame under a table-level cache (3 where Spark answers 2) and every `CACHE TABLE` read are
FRESH-READ-WHILE-TABLE-CACHED-1, a kept divergence (owner, 2026-11-22). `df_view` is unchanged. The 27 reads
`PENDING_1394` held (27 per catalog, 54 in all) were asserted on this run and passed: 0 pending, 0 failed.

Up: [map.md](map.md).

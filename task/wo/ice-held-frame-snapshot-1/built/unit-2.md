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
  a fresh read of an unchanged table, and another writer's commit on the local catalog. **On Glue and S3 Tables**
  (read, not measured), a fresh read that sees another writer's commit records it (1a's Q2 (b)) and moves the epoch,
  so the next use refills where Spark keeps the old fill until a refresh; the same divergence 1a's held frames have
  there, for the AWS acceptance leg.
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
record are untouched. Tests: `held_iceberg/tests.rs` (3 Rust pins) and the new pin file
`python/repark/tests/iceberg/held_frame/test_ice_held_frame_snapshot_1_cached.py`.

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

**The pins** on the head package: the held-frame directory 141 passed (unit 2's 68, 1a's 60 Spark cells and its 13
ENC-1 cells); on main the 68 new cells give 50 red, 18 green. The Rust pins: `held_iceberg::tests` 10 passed (3 new).
catalog-surface-1's C-009 pins and A1's pin files pass in the facade run (§7).

**The mutation proof** (`N/ice-u2-out/mutant.diff`, built in a scratch worktree of the head, never in the lane
clone): the staleness check returns "fresh" for every fill. On that package `C|cache_count` and `C|cache_append` go
back to 2, 2, 3 and `K|cache_held` to 2, 2, in both modes; `C|cache_lazy` and `C|cache_table` stay 3, 3, since they
were EQUAL before this unit and do not reach the check. The unit's pin file: 48 of 68 red (24 shapes: every refill,
re-bind and refresh cell); the 20 green are the kept-fill cells and the own-plan cells, which the own-plan
pass-through answers without the check. 1a's 73 pins pass on the mutant.

## 6. Cost

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
  (a) in this unit, since the brief holds the C-009 pins.
- **Glue and S3 Tables (read, not measured).** A fresh read that sees another writer's commit records it (1a's Q2 (b))
  and moves the epoch, so a cached frame refills where Spark keeps its fill until a refresh. The AWS acceptance leg
  should carry "cache a frame; an external commit; a fresh read; the cached frame's `count`" (Spark: the old count).

Up: [map.md](map.md).

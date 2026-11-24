# TEMP-VIEW-CACHE-LAZY-PARSE-1 step 0 — the measured sketch

**Date:** 2026-11-20 (UTC). **Tier:** Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. **No product code, no build.**
**Card:** `task/roadmap/mid-term/cards-2026-11-19/temp-view-cache-lazy-parse-1-card-2026-11-19.md` (S1 by class,
label-blocking). **Rulings carried:** the owner's "We better go with B to be safe" (2026-11-18: the reader-parse
family blocks the label); "go with recommendations" (2026-11-19: C-012 a hard 1.05x, a row under 100 ms over 1.05x a
flag attributed by swap or instruction counts, nothing over 1.15x without the owner); "Let's go with defaults across
the board" (2026-11-20, about 00:22 UTC: the questions in §6 go to a Fable medium consult); Rust-first (2026-09-14);
the insertion-inventory rule (2026-11-09).

**Builds:** main `9f9650f5` (unit 1 of TEMP-VIEW-FRAME-READER-PARSE-1, #1373; package `S/ddu-work/pkg-rp1d-52f468ce`,
`BUILT_FROM` `52f468ce`, the tree of #1373's final head), label `main`; main before unit 1, `8bc26cba`
(`S/ddu-work/pkg-rp1base-8bc26cba`), label `base`; Spark 4.1.2 with Iceberg 1.11.0. Provenance:
[evidence/map.md](evidence/).

## 0. The findings

- **2,402 cells per build** in step 0 ([domain-table.md](domain-table.md)), 240 more in the revision (§8 there);
  every WRONG cell is **silent** (rows, no error). Step 0: main 1,656 EQUAL, 686 WRONG, 60 RAISE (the
  `mapInPandas` `STRING` refusal, Q7); base 1,100 EQUAL, 1,242 WRONG, 60 RAISE. Both steady ANSI modes give the same
  pattern; no step 0 cell flips ANSI between building a frame and its first action (the revision's do).
- **Unit 1 moved 556 cells WRONG → EQUAL and none the other way in step 0's zone cells** (§3; the revision's ANSI
  flips add 6 EQUAL → RAISE): a held, cached frame acted on after
  the `SET` (the `cache`, `persist` × 2, `recache` × 2 and `after_set` doors, every action but the join) now parses
  under the reading zone, as Spark does, because the cache materialises inside the action. The card's
  `cache_lazy` cells did not move.
- **F1, registration materialises (the card's `cache_lazy`, 158 cells):** `createOrReplaceTempView`, `explain` and
  a write materialise a pending cache at the call (`_native_for_registration`), under the creating zone; Spark
  materialises at the first use. `cache_view`, `cache_view_sql` 144 (all six actions, 12 text cells each), `explain`
  first 2, the label path's `INSERT … SELECT` over a view of a cached frame 12.
- **F2, lazy treated as eager (146 cells):** `CACHE LAZY TABLE` and `spark.catalog.cacheTable` count at the call;
  Spark's are lazy (`Catalog.cacheTable` calls `cacheQuery` only). `clazy` 72, `catcache` 72, `clazy_then` 2.
- **F3, the SQL door skips the cache (84 cells):** `SELECT … FROM v` after `CACHE TABLE v` re-plans `v` under the
  live zone; Spark reads the cache. `ctable_sql` 72; `clazy_sql`'s `count` 12 (the other `clazy_sql` actions are
  EQUAL by accident: skipping the cache lands on the parse a lazy cache would have fixed).
- **F4, the bridge snapshot (the card's `udf` door, 124 cells):** every scalar `udf` and `pandas_udf` column, and
  `mapInArrow` / `mapInPandas`, is a map bridge, and building any frame on it runs the bridge at once
  (`_prepare_for_plan` → `_materialize_map_bridge_once`), under the creating zone. `U` 96 (`first` and
  `countfirst`, all four kinds; `second` is EQUAL), the cached UDF and map frames 4, the label path's UDF frames 24;
  a UDF or map frame acted on directly (`T`) is EQUAL on main (unit 1 moved 72 of those cells).
- **Outside the card, measured here:** F5, the object-identity cache (a twin plan, a bare reader cache read by
  another frame or session: 10 `Q` cells; a child built before materialisation, 2); F6, a cache made after a first
  own-plan action (2); **F7, a held frame joined after a `SET` (140 cells, on base and main, Parquet and Iceberg
  too):** the held frame's own expressions read the live zone in the join where Spark keeps the frame's stamps; it
  is the already-filed **TEMP-VIEW-FRAME-CROSSJOIN-1** (S1, label-blocking, owner 2026-11-18), sibling
  HELD-FRAME-USING-JOIN-1 (§5, Q4); F8, `writeTo().create()` of a cached frame keeps the frame's stamps where
  Spark's writer re-analyses (20 cells, `t` and `h`).
- **The revision round (2026-11-20, after the critic's UNSOUND on seat B; [domain-table.md](domain-table.md) §8,
  240 more cells `M`):** seat B's fill rule was wrong and seat B is **NOT READY TO BUILD** (§4); Q1's "no option
  gives both" was wrong (§4, Q1); missed shapes measured: `REFRESH TABLE` on a cached view, `clearCache()` after a
  view of a lazy cache, `CACHE [LAZY] TABLE … AS SELECT` (loud), **a cached Iceberg frame stale after an `INSERT`
  into its table** (silent, the label's format; Q11), ANSI flips between building and the first action (unit 1
  moved 6 cells EQUAL → RAISE and 6 RAISE → EQUAL; Q10), a cast under ANSI off (SET-ANSI-RUNTIME-3).

## 1. Spark's rule (Spark 4.1.2, from the jar and from live runs)

From the bytecode ([evidence/jar/javap-cache.txt](evidence/jar/javap-cache.txt)) and the rule cells (`R`, `Q`,
[domain-table.md](domain-table.md) §1):

1. **`Dataset.cache()` / `persist(level)` build the cached plan at the call and run nothing.** `cache()` is
   `persist()`, which is `persist(defaultCacheStorageLevel)`; `persist(level)` calls
   `CacheManager.cacheQuery(this, None, level)`, which takes the Dataset's normalized analysed plan (its expressions
   already stamped with the zone of the Dataset's analysis) and, in `cacheQueryInternal`, executes it to a physical
   plan in `getOrCloneSessionWithConfigsOff(session)` (a clone, its conf copied at that moment, whenever auto bucketed
   scan is on, the default) and wraps it in an `InMemoryRelation`. The storage level changes nothing here:
   `MEMORY_ONLY` and `DISK_ONLY` answer as `cache()` in all 240 of their cells.
2. **The cache materialises on its first use, under the reader session's live settings at that moment.**
   `CachedRDDBuilder.cachedColumnBuffers` builds the buffers once (`buildBuffers` → `cachedPlan.execute()`), and the
   file scan inside reads `relation.sparkSession.sessionState.conf` when `FileSourceScanExec.inputRDD` is first asked
   for (the parent unit's jar reading). The clone's copied conf does not reach the scan. The first use is any query
   whose plan matches (`sameResult`, the zone stamps included): `count`, `show`, `collect`, a derived `select` or
   `filter`, a join, a write, `CACHE TABLE`'s own `count`. Every later use reads the buffers (`lazy_two`: a first
   action under UTC, a second under Tokyo, a derived frame and `count` all answer UTC's parse).
3. **`explain` and `storageLevel` do not materialise** (`explain_first`: UTC's parse after an `explain` under Los
   Angeles).
4. **The Dataset's own executed plan.** `collectToPython` and the other own-plan actions read the Dataset's final
   `queryExecution` field (unit 1's first-action rule). A Dataset cached before its first own-plan action reads the
   cache through that plan, so the cache materialises at that action under the live settings: the same moment unit 1's
   memo settles. A Dataset whose first own-plan action ran **before** `cache()` keeps its old plan: later own-plan
   actions never read the cache, and the cache materialises at the first other query (`own_then_cache`: `collect`
   under UTC, `cache()`, `collect` under Tokyo answers UTC, `count` under Tokyo materialises, a derived frame answers
   Tokyo's parse).
5. **A child built before materialisation shares the cache** (plan matching): `child_before` answers UTC's parse
   for the child's first action and for the parent's later one. A twin plan built separately shares it too
   (`twin_plan`).
6. **Another session** shares the `CacheManager`, but a frame built there under another zone does not match: its
   zone-aware expressions are stamped differently (`newsess_lazy`, `newsess_mat`: the other session's frame parses
   under its own zone). A bare reader frame cached in one session and read through another session's plan matches,
   and then the parse is the one the first use fixed (`newsess_bare_*`).
7. **`CACHE TABLE`, `CACHE LAZY TABLE`, `spark.catalog.cacheTable`.** `BaseCacheTableExec.run` calls `cacheQuery`
   and then `count()` unless the statement is `LAZY`, so `CACHE TABLE` materialises at the statement (the creating
   zone) and `CACHE LAZY TABLE` at the first use. `Catalog.cacheTable` calls `cacheQuery` only: **it is lazy.** The
   SQL door and `spark.table` both read the cache (the plan matches).
8. **Python UDF columns and map functions plan nothing when built.** `BatchEvalPythonExec`, `ArrowEvalPythonExec`
   and `MapInBatchExec` (`mapInPandas`, `mapInArrow`) run `child.execute()` in `doExecute` and read
   `sessionLocalTimeZone` there, so the input's parse follows the same first-action rule as any frame: a first
   own-plan action fixes it, `count` fixes nothing (`U`, `T`: `first` and `countfirst` answer the reading zone's
   parse, `second` the first action's). A cached UDF frame materialises at its first use (`udf_cache`, `mia_cache`).
9. **Only a Dataset's own physical plan keeps a parse** (the critic's reading, re-measured in `M`). Each Dataset's
   physical plan holds its own `inputRDD`, a lazy val, so a parse is pinned for that Dataset's later own-plan
   actions only. `count`, `show`, `take`, a write and a SQL query each build a new plan and keep nothing; two
   sibling frames built on one UDF or map frame each decide at their own first action. Measured on a frame built on
   a UDF or map frame (`M bridge_*`, 90 cells): `count`, `show`, `take` or a write under UTC, then `collect` under
   Tokyo, parse under Tokyo; sibling `a` acted on under UTC parses under UTC and sibling `b` under Tokyo. Spark
   re-runs the Python function at every one of those actions.

## 2. RePark's mechanism on main

The sites, with file:line, are in [inventory.md](inventory.md); the trace of when each door plans is
[domain-table.md](domain-table.md) §7.

- **The parse is decided where the plan is optimized.** Unit 1's `ReaderParseRule` compares the session's live
  settings (or, inside an own-plan scope, the memo's first decision) with the frame's snapshot each time a plan
  holding a `ReaderParse` node is optimized (inventory P1–P5). So the question for every door is only *when* the
  frame's plan is optimized.
- **A cache materialises in `_materialize_cache_if_needed`** (C3): at an action (C3a: inside the own-plan scope for
  `collect` and its kin, live for `count`, `show`, `take`) — Spark's moment, which is why unit 1 moved the held
  doors — but also at **view registration, writes and `explain`** (C3b, `_native_for_registration`), which is
  Spark's cache-build moment, not its materialisation moment (F1).
- **The cache table is eager on all three doors** (T1, T2): `catalog_surface.cache_table` always calls `count()`
  (F2), and the token's overlay is read by `spark.table` only (T3, T4: F3).
- **A map bridge runs when a child plan is built** (U5): `_plan()` → `_prepare_for_plan` →
  `_materialize_map_bridge_once` executes the parent stream and the function once, outside any own-plan scope, so
  every frame built on a UDF or map frame reads a snapshot taken under the settings of the moment it was built (F4).
  Acted on directly, the bridge runs inside the action (U6), and unit 1's rule holds (`T`: EQUAL).
- **The cache is object-identity** (C7): children built before materialisation and twin plans never share (F5).

## 3. What unit 1 moved

Base → main, every cell compared ([domain-table.md](domain-table.md) §6): **WRONG → EQUAL 556, EQUAL → WRONG
0.** The moves are the held doors of §0 (360 `C` cells, the six held doors × five actions × 12 text cells),
`clazy_sql`'s five non-`count` actions (60: the SQL door re-plans the view under the live zone), the no-cache
control's five non-join actions (60), the UDF-on-top doors (72: `udf`, `pudf`, `mapInArrow` acted on
directly decide inside the action) and four rule cells (`lazy_two`, `newsess_lazy`). Nothing in F1, F2, F3, F4, F7
or F8 moved: those doors plan before the action, so unit 1's decision is taken at the wrong moment, faithfully.

**The revision cells (`M`, 240):** unit 1 moved 24 WRONG → EQUAL (`unpersist_blocking`, `unpersist_reread`,
`clear_mat`, 6 text cells each, and the ANSI on → off flips 6) and **6 EQUAL → RAISE**: a JSON or CSV frame built
under ANSI off and acted on under ANSI on (`plain`, `cache`, `count`, `json` and `csv`) now raises
`CAST_INVALID_INPUT`, because unit 1 decides the parse under the live ANSI setting (Spark's timing) and RePark's
parse raises under ANSI on where Spark's `PERMISSIVE` parse answers `NULL` (the steady ANSI-on cells raise on both
builds). Step 0's "0 EQUAL → WRONG" holds for the zone; for an ANSI flip it is these 6 loud cells (Q10).

## 4. Seats (Rust-first, on unit 1's `ReaderParse` and memo)

### Seat A — a lazy cache table, materialised at its first use (F1, F2, F3, F8, F5 children; built as A1 and A2)

- **Rust:** a `LazyCacheTable` provider in a new file `crates/repark-core/src/session/lazy_cache.rs` (default cap
  1,000 lines; about 250 expected): it holds the frame's lineage as a `DataFrame`, the cache budgets, a once-cell of
  batches and the settings its fill was decided under. Its first `scan` fills it under a fresh memo (the rule below),
  applies the budgets as `register_collected_memtable` does and keeps the batches; later scans serve them.
  `temp_views.rs` (820 of 1,000) gains `register_lazy_cache_view` beside `materialize_dataframe_as_cache_view`;
  `crates/repark-python/src/session.rs` (833 of 1,000) one method and one getter for the fill's settings.
- **How the memo reaches the async `scan`: it does not need to; the decision is taken synchronously.**
  `READER_PARSE_MEMO` is a thread-local (`extension.rs:79-81`) and `TableProvider::scan` is async, so a memo entered
  around an await could be missed by an optimizer run continued on another runtime worker. The provider's first
  `scan` therefore runs, before its first `await`, `ReaderParseMemo::run` with a fresh memo around
  `SessionState::optimize` of the held logical plan. `ReaderParseRule` is an optimizer rule, so the decision is
  fixed in the optimized plan: a decided node becomes its input or a closed frame view, so a later optimization of
  that plan finds no `ReaderParse` node and decides nothing again. Physical planning and the collect then run from
  the optimized plan on any worker. No memo crosses a thread.
- **The memo rule (Q1, rewritten after the critic):**
  1. the cache fills under a fresh memo at its first use (the live settings then: Spark's rule 2);
  2. a frame's first own-plan action that reads its own cache adopts the fill's settings into the frame's own memo
     (the facade reads them back through the getter inside `_own_plan`, `surface_a.py:406-412`);
  3. a frame whose own memo was settled before `cache()` keeps its own lineage for its own-plan actions; its
     children, its views, `count` and SQL read the cache.

  Against the cells: `own_then_cache` (R, 2): part 3 keeps the own `collect` under UTC and `count` under Tokyo fills
  the cache under Tokyo, so the derived frame reads Tokyo; both sub-cells EQUAL. `unpersist_blocking` (M, 6 text,
  EQUAL on main): the first `collect` under UTC fills and adopts UTC (part 2); after `unpersist(blocking=True)` the
  own `collect` under Tokyo replans its lineage under the settled memo, UTC, as Spark keeps its own plan; the fresh
  child decides Tokyo. Under the old default (a), a fresh memo with no adoption, the own `collect` re-decided under
  Tokyo: **EQUAL → WRONG on 6 cells**; under (b), the acting memo, `unpersist_reread` and `own_then_cache`'s
  derived sub-cell turned WRONG. `unpersist_reread` (6): `count` fills under UTC and adopts nothing (not own); the
  own `collect` after `unpersist` is the first own action, Tokyo, as Spark builds its own plan then. `clear_mat`
  (6): `count` fills under Los Angeles; `clearCache` empties the fill; the own `collect` under Tokyo refills under a
  fresh memo and adopts Tokyo; the child reads Tokyo. `lazy_two` (3) and `cache_siblings` (6): the first use fills
  under UTC, everything after reads it. Every one of these cells is EQUAL under the rule.
- **Facade:** `persist()` registers the lazy view at the call and binds `_inner` at once
  (`cache_handle.bind_registered_view`), so registration, writes, `explain`, joins, the `mergeInto` source
  (`merge.py:157`) and every child built after `cache()` read the same lazy table. **`core.py` is exactly 3,461
  lines, its exact baseline (`scripts/check_lib_py.py:62-63`), so the change nets zero or fewer lines there:**
  `persist`'s three binding lines (`core.py:740-742`) become one call `cache_handle.bind_lazy(self, level)` (−2);
  the cache branch of `_materialize_cache_if_needed` (`core.py:332-338`, seven lines) becomes one line for a
  non-checkpoint frame (−6); `unpersist` (`core.py:745-767`) keeps its lines and calls a `cache_handle` helper for
  the lazy table. Net −8. The bodies go to `cache_handle.py` (174 of 1,000); the adoption goes to `surface_a.py`
  (475 of 1,000). The builder shows `check_lib_py` with `core.py` at or under 3,461.
- **The cache-table doors (A2):** `catalog_surface.cache_table` (753 of 1,000) counts only for `CACHE TABLE`;
  `CACHE LAZY TABLE` and `catalog.cacheTable` stay lazy (`sql_cache_statements.py`, 161, passes the keyword).
  `CACHE TABLE v` re-points the temp view `v` at the lazy table and `UNCACHE` restores its provider (Q2).
  `REFRESH TABLE v` on a cached view drops the fill and keeps the lazy entry, as Spark caches lazily again
  (`refresh_table`'s view return, `catalog_surface.py:731-748`). `clearCache()` empties every lazy table's fill, so a
  view over a lazy cache reads its lineage at its next first use instead of the registration snapshot. `CACHE
  [LAZY] TABLE … AS SELECT` stays a loud refusal unless Q12 folds it in. A JSON or CSV catalog table read by SQL
  after `CACHE TABLE` keeps the overlay (Q2); not measured: the builder adds a cell or records that RePark cannot
  create such a table.
- **Across sessions:** the lazy table belongs to the creating session's token; Spark's shared `CacheManager` serves
  `CACHE TABLE db.t` to another session and RePark does not (named; no zone effect on Parquet or Iceberg).
- **Side effects to forecast and bar:** binding `_inner` at `cache()` puts the frame's own expressions inside the
  provider, so the join no longer re-analyses them under the live settings: **the 120 held cache doors' join cells
  and the cached self-join (`selfjoin`, 8) move to EQUAL**, leaving F7's residue at the 20 `N` cells inside
  TEMP-VIEW-FRAME-CROSSJOIN-1. The F5 children built after `cache()` move: `child_before` 2, `cache_siblings` 6,
  and the critic's `derived_over_cached` and `udf_over_cached` parent cells. F5's twins and other sessions (10 `Q`
  cells) do not.
- **Cells it moves:** A1 F1 158, F8 20, F5 children 8, `own_then_cache` 2, the join side effect 128: **316**; A2
  F2 146, F3 84, `refresh` 6, `clear_view` 6: **242** (+16 if Q12 folds in AS SELECT). **Must hold EQUAL:**
  `unpersist_blocking` 8, `unpersist_reread` 8, `clear_mat` 8, `lazy_two` 3, `mat` and `ctable` (240), the held
  doors' non-join actions (360), every `T` cell but the `mapInPandas` refusal.
- **`EXPLAIN`:** a cached frame's plan shows the lazy table's scan before and after its fill (today a `MemTable`
  scan after it, the lineage before it). Disclosed, as unit 1's Q11.
- **Cost (design estimates from 1 or 2 samples on main, step 0's series: the revision's series under `flock -x` on cores
  48-57 got no sample, because lane A's cargo builder held the measure lock; its first wait timed out at 600 s):** a cached 2M-row JSON read: `count` 294 ms (it
  fills) then `collect` 1,778 ms; `collect` first 2,072 ms, then `count` 8 ms; a view over the cached frame 272 ms
  at registration, then `count` 4 ms. Seat A keeps the first two sequences' work and moments and moves the third's
  272 ms from the registration to the first read. **The selective child** (`df.cache(); df.filter(sel).count()`):
  today the child keeps its filter pushdown and never fills `df` (C7); under seat A its first action fills the whole
  parent: about the 294 ms of the materialising `count` above, against a filtered scan today (not sampled; the
  lock was held). That is Spark's behaviour (the child's plan reads the `InMemoryRelation`), but a regression
  against main that can exceed 1.15x on a selective child: the row goes to the owner if the build measures it over
  1.15x. Fixed cost per scan: one once-cell read.

### Seat B — the map bridge (F4): **NOT READY TO BUILD**; a later re-sketch after A1 and A2

- **Why the step 0 sketch was wrong.** It filled the bridge once per bridge object, live for `count`, and served
  that fill to every later action. Spark keeps a parse only inside one Dataset's own physical plan (§1 point 9):
  `count`, `show`, `take`, a write and a SQL query each build a new plan and keep nothing, and siblings decide on
  their own. As written it leaves WRONG: **`U countfirst` 48** (`count` would fill under the creating zone and serve
  it to the `collect`), and the revision's `bridge_count`, `bridge_show`, `bridge_take`, `bridge_write` and
  `bridge_sibling` (18 each, 90). Of the 214 bridge cells now measured (`U` first and countfirst 96, the cached UDF
  and map frames 4, `L` UDF 24, `M` 90), it would move at most 76.
- **The corrected rule (for the re-sketch):** key the fill by the acting frame's own-plan memo. Inside an own-plan
  scope (`collect`, `toArrow`, `toLocalIterator`, `tail`, `checkpoint`) the bridge runs under the memo and its
  output is kept for that frame's later own actions only. Every other action (`count`, `show`, `take`, a write, a
  SQL query over a view of the frame, `INSERT … SELECT`, a sibling's action) runs the bridge afresh and keeps
  nothing.
- **Its cost under Spark's rule:** each non-own action re-runs the bridge. Measured on main (1 sample each, step 0's
  series): a UDF column over a 2M-row CSV read acted on directly runs the bridge at every action, 666 ms and 685 ms
  for two `count`s; with the shape on top, today's snapshot costs 619 ms at construction and 2 ms per `count`. Under
  Spark's rule a child counted twice costs about 1,250 ms against today's 623 ms, and the second `count` about
  620 ms against 2 ms: far over 1.15x on that row (Q9). Spark re-runs Python UDFs per action too.
- **Options for the re-sketch:**
  - **B1, the facade fill keyed by the memo:** the fillable table in `temp_views.rs`, the pending bridges handed
    to children, a per-query fill for views and SQL; Python-heavy; every non-own action re-runs the bridge.
  - **B2, a Rust execution node, Spark's `MapInBatchExec` shape (the critic's option):** the child's physical plan
    holds a node that drives the parent stream in Rust and hands batches to Python for the function alone,
    attaching to the GIL from the executor thread while the acting thread has detached. The parse is decided per
    acting plan with no snapshot, no fillable table and no view registry, because the parent is planned inside the
    acting plan. A larger unit (a new `ExecutionPlan`, pyo3, GIL discipline across runtime workers); Rust-first.
  - **B3, a retained fill:** cheaper, but not Spark's answer on the 138 cells above.
  - The step 0 reason for rejecting a call into Python from a scan still stands for B1's shape (a provider that
    pulls the parent through Python's `__arrow_c_stream__` re-enters the runtime, `session.rs:498`); B2 avoids it,
    because the parent stream is driven in Rust and only the user function is called in Python.
- **Default for the re-sketch:** measure B2 first (Spark's shape, Rust-first), B1 as the fallback; cost under Q9.

**C-012** was not run (no seat is built). Neither seat changes a plan that holds no cache and no map bridge; each
builder measures C-012 base and head under the hard 1.05x and records whether its replay calls `cache`, `persist`
or a UDF at all.

## 5. The unit split under the one-round cap

- **A1 — the lazy provider and the DataFrame doors** (F1: registration, `explain`, writes, the `mergeInto` source;
  F8; the F5 children built after `cache()`; the memo rule; the join side effect). Files and caps: new
  `crates/repark-core/src/session/lazy_cache.rs` (default 1,000; about 250); `crates/repark-core/src/session/temp_views.rs`
  (820 of 1,000); `crates/repark-python/src/session.rs` (833 of 1,000); `python/repark/src/repark/spark/dataframe/core.py`
  (3,461, exact baseline, net ≤ 0); `dataframe/cache_handle.py` (174 of 1,000); `dataframe/surface_a.py` (475 of 1,000);
  the `writeTo` lineage read in `dataframe/writer_layout.py` (default 1,000). Bar: 316 cells EQUAL; the must-hold
  set EQUAL; 0 EQUAL lost over the 2,642 cells of both rounds; `EXPLAIN` disclosed; cost rows for the cached JSON
  read, the selective child and C-012.
- **A2 — the cache-table doors, after A1** (F2, F3 with the SQL re-point and the `UNCACHE` restore, `REFRESH`,
  `clearCache` over views, AS SELECT if Q12 folds it in). Files and caps: `python/repark/src/repark/spark/catalog_surface.py`
  (753 of 1,000), `session/sql_cache_statements.py` (161 of 1,000), `catalog.py` (default 1,000), `cache_handle.py`.
  Bar: 242 cells EQUAL (258 with AS SELECT); 0 EQUAL lost.
- **B — the map bridge, after A1 and A2, from a new step 0 re-sketch** under Spark's per-plan rule (§4, Q9). It is
  not built from this sketch.
- **Not in A1, A2 or B:** F7 (TEMP-VIEW-FRAME-CROSSJOIN-1's residue, 20 `N` cells); F5's twins and other sessions
  (10, Q5); the stale held Iceberg frame (12, Q11); the ANSI parse (Q10, READER-TYPED-SCAN-1); the held cast under
  ANSI off (SET-ANSI-RUNTIME-3); the `mapInPandas` `STRING` refusal (Q7).

## 6. Questions (for the Fable consult; each ruled from the tables)

- **Q1. Seat A's memo at the fill (rewritten).** (a) the critic's single rule (§4: fill under a fresh memo; a
  frame's first own action that reads its own cache adopts the fill; a frame whose memo was settled before `cache()`
  keeps its lineage); (b) a fresh memo with no adoption (step 0's old default); (c) the acting frame's memo. Cells:
  under (a) `own_then_cache` 2, `unpersist_blocking` 6, `unpersist_reread` 6, `clear_mat` 6, `lazy_two` 3 and
  `cache_siblings` 6 are all EQUAL; (b) turns `unpersist_blocking` 6 EQUAL → WRONG and leaves `own_then_cache`
  WRONG; (c) turns `unpersist_reread` 6 EQUAL → WRONG and `own_then_cache`'s derived sub-cell. **Default (a).**
- **Q2. The SQL door after `CACHE TABLE` (F3, 84 cells).** (a) re-point the temp view at the cache, restore on
  `UNCACHE`; (b) make the SQL resolver read the token's overlay; (c) leave it and name it. **Default (a):** one seat
  for temp views; catalog tables keep the overlay (no zone-dependent parse there).
- **Q3. Withdrawn:** seat B's door coverage moves to the seat B re-sketch with Q9 (the step 0 default inherited the
  keying error).
- **Q4. F7, a held frame joined after a `SET` (rewritten).** It is the already-filed TEMP-VIEW-FRAME-CROSSJOIN-1
  (S1, label-blocking, owner 2026-11-18, "Defaults on all three"), sibling HELD-FRAME-USING-JOIN-1. (a) no new
  card: add this step 0's 20 `N` join cells (Parquet and Iceberg included) and the 8 `selfjoin` cells to that card's
  evidence, and let A1 move the 120 cached-join cells and `selfjoin` as its side effect, measured in A1's bar;
  (b) fold F7 into A1. **Default (a).**
- **Q5. F5, the object-identity cache (10 `Q` cells: a twin plan, a bare reader cache read through a new frame or
  another session).** (a) a named residual under the documented contract ("Child-plan cache sharing is OUT"), S3;
  (b) a card for plan-matched caches. **Default (a).** A1 moves the children built after `cache()` (8).
- **Q6. F8, `writeTo().create()` of a cached frame (20 cells, `t` and `h`; the uncached `writeTo` is EQUAL).** (a)
  fold into A1 (the writer reads the uncached lineage when its settings differ); (b) a separate card. **Default (a).**
- **Q7. The `mapInPandas` `STRING` refusal (60 `T` cells, base and main):** a `STRING` column back through
  `mapInPandas` under pandas 3 refuses (`expected type string, got large_string`). (a) a new card; (b) note it only.
  **Default (a)**, not label-blocking (it raises; nothing is silent). The card says the Spark side ran pandas 3.0.5
  linked onto the oracle, a version PySpark 4.1.2 is not tested against, so these cells are evidence about pandas 3,
  not a Spark reference.
- **Q8. The rating.** (a) A1, A2 and seat B block the label (the card's S1 by class and the owner's B); (b) A1 and
  A2 only. **Default (a).**
- **Q9. Seat B's cost under Spark's per-plan rule.** Each non-own action re-runs the bridge (about 620 ms over a
  2M-row CSV read, against 2 ms today for a second `count` of a snapshotted child: far over 1.15x on that row).
  (a) Spark's rule, the re-runs accepted by the owner over 1.15x (138 cells move: `U countfirst` 48, `M bridge_*`
  90); (b) a retained fill, cheaper and WRONG on those 138; (c) B2, the Rust node, whose per-action cost is the
  same work as Spark's. **Default (a)** with B2 measured first in the re-sketch; the over-1.15x row goes to the owner.
- **Q10. The ANSI-parse divergence and unit 1's ANSI-flip moves.** RePark's JSON and CSV parse raises
  `CAST_INVALID_INPUT` under ANSI on where Spark's `PERMISSIVE` parse answers `NULL` (steady ANSI on: 6 RAISE on
  both builds); unit 1 moved the flips (off → on 6 EQUAL → RAISE; on → off 6 RAISE → EQUAL). Loud. (a) the
  already-filed READER-TYPED-SCAN-1 (S3, not label-blocking; its D2 is this divergence) takes the 18 flip and
  steady cells as evidence, with the note that its fix also restores the 6 off → on cells; (b) a new card; (c)
  re-rate READER-TYPED-SCAN-1. **Default (a).** The held `CAST(s AS INT)` under ANSI off raises on both builds
  with or without a flip, on Parquet too (6 RAISE): it is SET-ANSI-RUNTIME-3 ("most failing casts raise even with
  ANSI off", the cast path never reads the flag), not a timing finding.
- **Q11. A held Iceberg frame stale after a write to its table (12 cells, base and main).** `spark.table(t)` held,
  cached or not, lazy or counted, or via `CACHE TABLE t`, then `INSERT INTO t` (SQL) or `writeTo(t).append()`, then
  the held frame's `count`: RePark 1, Spark 2; a fresh `spark.table(t)` and SQL answer 2. Silent, on the label's
  format and door; **not the cache** (the uncached `held` and `held_append` cells are stale too), so not this
  card's mechanism. (a) a new card, ICE-HELD-FRAME-SNAPSHOT-1, **S1, label-blocking**: a silent wrong count on an
  Iceberg v2 table through the Spark door after the same session's write; (b) the same card at S2, not
  label-blocking, if Fable holds a held frame reused across a write outside the label's scope; (c) fold into A2.
  **Default (a)**; (c) is wrong because the cache adds nothing.
- **Q12. `CACHE [LAZY] TABLE … AS SELECT` (16 cells RAISE on both builds; Spark answers, eager under the creating
  zone, lazy at the first use).** (a) fold into A2 (register the query as a temp view, then the lazy cache, counted
  unless `LAZY`); (b) name it, a loud divergence, S3, not label-blocking. **Default (b)**, A2 taking it only if its
  bar has room.

## 7. What the brief got wrong

- "`CACHE TABLE` in `repark-spark`": there is no planning site there. `CACHE [LAZY] TABLE` is answered by the
  facade (`sql_cache_statements.py` → `catalog_surface.cache_table`); `repark-spark` only lists `Statement::Cache`
  among the statements its write door does not route (`write_door.rs:215`).
- "the `udf` door … RePark plans the frame when the UDF column is built": it plans when a frame is built **on top
  of** the UDF frame (`selectExpr`, `select`, `filter`, `join`, a column reference, …); a UDF frame acted on
  directly answers as Spark on main (`T`, unit 1 moved it).
- The Spark oracle has no pandas: the `pandas_udf` and `mapInPandas` cells ran Spark with the RePark interpreter's
  pandas 3.0.5 and NumPy 2.5.1 linked onto `PYTHONPATH` (the oracle's own environment untouched); PySpark's version
  checks pass; the caveat is Q7's.

## 8. Revision record

- **2026-11-20, the critic's round (verdict UNSOUND on seat B; outside the repository, `cache-critic/verdict.md`):**
  141 cells re-measured with 0 disagreements; Spark's rule, the table, the inventory and seat A's mechanism stood.
  This revision: §1 point 9; seat B marked NOT READY with its corrected rule, cost and the Rust-node option; Q1's
  single rule against the cells; F7 re-pointed to TEMP-VIEW-FRAME-CROSSJOIN-1 with the side-effect forecast; the
  missed shapes measured (`M`, 240 cells per build, [domain-table.md](domain-table.md) §8); the inventory's
  additions and its count of 77 `_plan()` sites; seat A's async-memo, `core.py` and selective-child gaps; the split
  into A1, A2 and a later B; Q9 to Q12.

Up: [map.md](map.md).

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
[evidence/map.md](evidence/map.md).

## 0. The findings

- **2,402 cells per build** ([domain-table.md](domain-table.md)); every cell that is not EQUAL is WRONG, and every
  WRONG cell is **silent** (rows, no error); no cell raises on either build where Spark answers; both ANSI modes
  give the same pattern. Main: 1,656 EQUAL, 686 WRONG, 60 RAISE (the `mapInPandas` `STRING` refusal, Q7). Base: 1,100 EQUAL, 1,242
  WRONG, 60 RAISE.
- **Unit 1 moved 556 cells WRONG → EQUAL and none the other way** (§3): a held, cached frame acted on after
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
  too):** the held frame's own expressions read the live zone in the join where Spark keeps the frame's stamps; the
  no-cache control `N` shows it with no cache at all, so it is not this card's mechanism; F8, `writeTo().create()` of
  a cached frame keeps the frame's stamps where Spark's writer re-analyses (20 cells, `t` and `h`).

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

## 4. Seats (two, Rust-first, on unit 1's `ReaderParse` and memo)

### Seat A — a lazy cache table, materialised at its first use (F1, F2, F3; F8 folded)

- **Rust:** a `LazyCacheTable` provider in a new file `crates/repark-core/src/session/lazy_cache.rs` (default cap
  1,000 lines; about 200 expected): it holds the frame's lineage as a `DataFrame`, the cache budgets and a
  once-cell of batches. Its first `scan` executes the held frame with a fresh `ReaderParseMemo` entered, so unit 1's
  rule decides under the live settings of that moment (Spark's rule 2: the cached plan's scan reads the live conf,
  not the acting Dataset's own plan), applies the budgets as `register_collected_memtable` does, and keeps the
  batches; later scans serve them. `temp_views.rs` (820 of 1,000) gains `register_lazy_cache_view` beside
  `materialize_dataframe_as_cache_view`; `crates/repark-python/src/session.rs` (833 of 1,000) one method.
- **Facade:** `persist()` registers the lazy view at the call and binds `_inner` at once
  (`cache_handle.bind_registered_view`), so registration, writes, `explain`, joins and every child built after
  `cache()` read the same lazy table; the cache branch of `_materialize_cache_if_needed` goes. `core.py` sits on its
  exact baseline (3,461), so the bodies move into `cache_handle.py` (174 lines) behind the same names, line-neutral
  or shorter. `catalog_surface.cache_table` (753) counts only for `CACHE TABLE`; `CACHE LAZY TABLE` and
  `catalog.cacheTable` stay lazy (`sql_cache_statements.py` 161 passes the keyword). For the SQL door (F3), `CACHE
  TABLE v` re-points the temp view `v` at the cache and `UNCACHE` restores its provider (Q2). F8: the `writeTo`
  scratch view reads the frame's uncached lineage when the writer's settings differ from the frame's, as Spark's
  re-analysed plan misses its cache (Q6).
- **Cells it moves:** F1 158, F2 146, F3 84, F8 20, and `child_before` 2 (a child built after `cache()` now shares):
  410. **EQUAL cells at risk:** none in the table by construction (the held doors already materialise at the
  action, and Seat A keeps that moment); the build re-measures all 2,402. `own_then_cache` stays WRONG in one
  sub-cell either way (Q1).
- **`EXPLAIN`:** a cached frame's plan shows the lazy table's scan before and after materialisation (today a
  `MemTable` scan after it, the lineage before it). Disclosed, as unit 1's Q11.
- **Cost:** the same single collect, moved to the first use; registering a view over a cached frame stops
  collecting. Cached 2M-row JSON on main (medians of 2 samples, [evidence/raw/cost-samples.jsonl](evidence/raw/cost-samples.jsonl)): `count` 294 ms (it
  materialises), then `collect` 1,778 ms; `collect` first 2,072 ms, then `count` 8 ms; a view over the cached frame
  272 ms at registration (the collect Seat A moves to the first use), then `count` 4 ms. Seat A keeps the first two
  sequences' work and moments, and moves the third's 272 ms from registration to the first read: an estimate of no
  change in total, the short-row rule applying to the build's rows. Fixed cost per scan: one once-cell read.

### Seat B — a map bridge decided at the first action of the frame that reads it (F4)

- **The hazard that rules out a scan-time callback:** a provider that calls back into Python from `scan` would run
  the parent's native plan from inside the runtime that is executing the child (`py.detach` → `block_on` nested in
  a runtime thread), and `session.rs`'s stream registration already records that re-entrant attach and detach
  aborts the process. So the bridge must still run in Python, before the native action starts.
- **Rust:** a fillable table in `temp_views.rs` (a provider whose batches are set once from the facade and read by
  every plan that already holds it; about 60 lines), so plans built before the bridge runs see its output.
- **Facade:** `_prepare_for_plan` stops executing; it registers the fillable view and hands the pending bridge to
  the child (carried like `_handles`). `_action_inner` and `_native_for_registration` fill the frame's pending
  bridges inside the acting scope (the own-plan memo for `collect` and its kin, live for `count`), once per bridge
  object, so a second action keeps the first decision (Spark's `second`). Views and the SQL door: a view registered
  over a frame with pending bridges registers the plan unfilled and records the bridges in a session registry keyed
  by the fillable view, and an action whose plan scans such a view fills them first. The bridge methods move out of
  `core.py` (exact baseline) into a new `dataframe/map_bridge.py` (default cap 1,000).
- **Cells it moves:** `U` 96, `udf_cache` and `mia_cache` 4, the label path's UDF cells 24: 124. **EQUAL cells at
  risk:** `U`'s `second` (48) and every `T` cell, which depend on the fill staying once per bridge object.
- **`EXPLAIN`:** unchanged in shape (a memory scan of the bridge's output), but `explain` of a child no longer runs
  the bridge; before the first action it shows an unfilled scan.
- **Cost:** the bridge's work is the same, moved from the child's construction to its first action; a child acted
  on twice reads the filled view once, as today. UDF column over a 2M-row CSV read on main (1 sample each): with the shape on top, 619 ms at the shape's
  construction (the snapshot) and 2 ms per `count`; with the UDF on top, 12 ms to build and 666 ms and 685 ms for
  two `count`s (the bridge re-runs per action). Seat B moves the 619 ms into the first action and keeps the second
  action at the filled view's cost: an estimate of no change in total. The cost runs were cut at two rounds (lock
  contention with lane A); the build measures base and head under the short-row rule.

**C-012** was not run in step 0 (no seat is built). Neither seat changes a plan that holds no cache and no map
bridge; the builder measures C-012 base and head under the hard 1.05x and records whether its replay calls `cache`,
`persist` or a UDF at all.

## 5. The unit split under the one-round cap

- **Unit 1 — Seat A** (F1, F2, F3, F8 and `child_before`; 410 cells; the label path's cached-frame `INSERT`). One
  build, one verifier round. Bar: the 410 cells EQUAL, 0 EQUAL lost over the 2,402, `EXPLAIN` disclosed, cost rows
  for the cached JSON read and C-012.
- **Unit 2 — Seat B** (F4; 124 cells; the label path's UDF cells). After unit 1 merges, because both change when a
  held frame materialises and Seat B reuses Seat A's registration path. Bar: the 124 cells EQUAL, `U` `second` and
  `T` held, cost rows for the UDF read and C-012.
- **Not in either unit:** F5, F6, F7 and the `mapInPandas` STRING refusal (§0, §6).

## 6. Questions (for the Fable consult; each ruled from the table)

- **Q1. Seat A's memo at materialisation.** (a) a fresh memo, the live settings at first use (Spark's rule 2);
  (b) the acting frame's memo. Cells: `R own_then_cache` (2): (a) makes the frame's own second `collect` answer the
  later zone where Spark keeps its first plan; (b) makes the derived frame keep the first zone where Spark
  materialises later. No option gives both. **Default (a)**, `own_then_cache` named, S3.
- **Q2. The SQL door after `CACHE TABLE` (F3, 84 cells).** (a) re-point the temp view at the cache, restore on
  `UNCACHE`; (b) make the SQL resolver read the token's overlay; (c) leave it and name it. **Default (a):** one seat
  for temp views; catalog tables keep the overlay (no zone-dependent parse there).
- **Q3. Seat B's door coverage.** (a) the fill before the action plus the view registry (the label path's
  `INSERT … SELECT` over a view of a UDF frame, 12 cells, included); (b) DataFrame doors only, views over UDF frames
  named. **Default (a)**, because the owner's B puts the label path in scope.
- **Q4. F7, a held frame joined after a `SET` (140 cells: the `N` control's join 20 and the held cache doors' join
  120; Parquet and Iceberg too; base and main alike).** Not a cache or a parse finding: the held frame's own
  expressions read the live zone in the join. (a) a new card, rated by Fable; (b) fold into unit 1. **Default (a)**
  (the one-round cap; a different mechanism, inside the join, not the cache).
- **Q5. F5, the object-identity cache (10 `Q` cells: a twin plan, a bare reader cache read through a new frame or
  another session).** (a) a named residual under the documented contract ("Child-plan cache sharing is OUT"), S3;
  (b) a card for plan-matched caches. **Default (a).** Seat A moves the child built after `cache()` (2).
- **Q6. F8, `writeTo().create()` of a cached frame (20 cells, `t` and `h`).** (a) fold into Seat A (the writer reads
  the uncached lineage when its settings differ); (b) a separate card. **Default (a)**, the cache path at the writer
  door.
- **Q7. The `mapInPandas` STRING refusal (60 `T` cells, base and main):** a frame with a `STRING` column through
  `mapInPandas` under pandas 3 refuses (`expected type string, got large_string`). (a) a new card; (b) note it
  only. **Default (a)**, not label-blocking (it raises; nothing is silent).
- **Q8. The rating.** (a) both units block the label (the card's S1 by class and the owner's B); (b) unit 1 only.
  **Default (a).**

## 7. What the brief got wrong

- "`CACHE TABLE` in `repark-spark`": there is no planning site there. `CACHE [LAZY] TABLE` is answered by the
  facade (`sql_cache_statements.py` → `catalog_surface.cache_table`); `repark-spark` only lists `Statement::Cache`
  among the statements its write door does not route (`write_door.rs:215`).
- "the `udf` door … RePark plans the frame when the UDF column is built": it plans when a frame is built **on top
  of** the UDF frame (`selectExpr`, `select`, `filter`, `join`, …); a UDF frame acted on directly answers as Spark on
  main (`T`, unit 1 moved it).
- The Spark oracle has no pandas: the `pandas_udf` and `mapInPandas` cells ran Spark with the RePark interpreter's
  pandas 3.0.5 and NumPy 2.5.1 linked onto `PYTHONPATH` (the oracle's own environment untouched); PySpark's version
  checks pass.

Up: [map.md](map.md).

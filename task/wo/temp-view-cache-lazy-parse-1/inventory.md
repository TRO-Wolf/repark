# TEMP-VIEW-CACHE-LAZY-PARSE-1 — the insertion inventory (owner's rule of 2026-11-09)

Every place on main `9f9650f5` where RePark builds or materialises a cache, and every place a UDF column or a map
function plans its input, with file:line and the exact moment it plans. "Plans" means the frame's plan is optimized
and executed, so unit 1's `ReaderParse` node is decided there. The timings are measured, not read: the trace probe
([evidence/probes/trace.py](evidence/probes/trace.py)) wraps each site and records the live session zone at each
call (`evidence/raw/trace-main.jsonl`; summary in [domain-table.md](domain-table.md) §7). Line numbers are main's.

## 1. Where the parse is decided (unit 1, the base every seat builds on)

| # | site | file:line | when it decides |
|---|---|---|---|
| P1 | `_reader_parse` wraps each JSON and CSV reader frame | `python/repark/src/repark/spark/session/reader.py:35`, callers `:519`, `:553`; native `crates/repark-python/src/dataframe/mod.rs:378` → `crates/repark-core/src/extension.rs:317` (`reader_parse`) | at `spark.read…json()` / `csv()`: builds the `ReaderParse` node (`extension.rs:204-240`) holding the parse plan, an unanalysed copy, the session's one `LiveOptions` handle and the frame's analyzer; decides nothing |
| P2 | `LiveOptions::of_session`, `read` | `extension.rs:36-75` | reads the shared session state's live options at the moment of the decision (bounded wait, then the last read) |
| P3 | `ReaderParseRule` (`repark_reader_parse`), first optimizer rule | `crates/repark-spark/src/view_ddl/frame_settings.rs:423-443`, `parse_decided` `:458-477`, `parse_closed` `:479-495`; installed at `:318-327` | every time a plan holding the node is optimized: live options equal to the options the plan is optimized under (the frame's snapshot) → the node is its input; different → a closed frame view over the copy analysed under the live options, everything above keeps the frame's settings |
| P4 | `ReaderParseMemo` (`settled`, `enter`, `restore`, `run`) and the thread-local slot | `extension.rs:79-127` | when a memo is entered, the first decision per `LiveOptions` source is recorded and every later decision on that thread reads it |
| P5 | the facade's per-object memo and own-plan scope | `python/repark/src/repark/spark/dataframe/surface_a.py:51` (`_PARSE_MEMOS`), `_parse_memo` `:399-403`, `_own_plan` `:406-412`; native `crates/repark-python/src/dataframe/mod.rs:72-90`, `:372` | entered around the own-plan actions: `to_arrow` (`core.py:3252`), `to_arrow_batches` (`core.py:3271`, which `collect` `:3094` uses), `_local_rows` (`surface_a.py:415`, `toLocalIterator`, `tail`), `localCheckpoint(eager=True)` (`surface_a.py:387`), `checkpoint` (`surface_a.py:391-396`, which also hands the memo to its child). `count`, `show`, `take`, writes and view registration run with no memo, so they decide live |

## 2. Where a cache is built and materialised

| # | site | file:line | when it plans |
|---|---|---|---|
| C1 | `DataFrame.cache` → `persist(MEMORY_AND_DISK_DESER)` | `python/repark/src/repark/spark/dataframe/core.py:701-719` | never: records the request |
| C2 | `DataFrame.persist(level)` | `core.py:721-743` | never: sets `_persist_requested`, `_storage_level`, registers the frame for `clearCache`; the level is cosmetic (`cache_handle.py:139-165`) |
| C3 | `_materialize_cache_if_needed` | `core.py:314-349` | the materialisation: `session.materialize_as_cache_view(view, self._inner, budgets)` (`:335`), then `cache_handle.bind_registered_view` (`cache_handle.py:102-117`) points `self._inner` at `SELECT * FROM <view>` and keeps the lineage in `_lineage_inner`. It runs under whatever memo is entered at the call. Callers below |
| C3a | `_action_inner` | `core.py:374-389` | every action's native frame, so a pending cache materialises at the action: inside the own-plan scope for `collect`, `toArrow`, `toLocalIterator`, `tail` (memo-settled); with no memo for `count` (`eager.py:128-135`), `show` (`display.py:84`, `:349`), `take` |
| C3b | `_native_for_registration` → `_action_inner` | `core.py:370-372` | **view registration, writes and `explain` materialise a pending cache, under the live settings at that call, with no memo:** `createOrReplaceTempView` (`core.py:852-857` → `surface_b.register_view_without_fill` `surface_b.py:229` → `catalog_surface._register_temp_view` `catalog_surface.py:320-330`, at `:326`); the writers (`writer_layout.py:73`, `:500`, `writer_text.py:78`, `:176`); `explain` (`core.py:2460-2482` registers a scratch view at `:2477`, then reads it with `toLocalIterator`). Measured: `cache_view` and `cache_explain` materialise under the creating zone, before the `SET` |
| C3c | `eager()` | `eager.py:86-112` | materialises at the call |
| C4 | Rust: `materialize_as_cache_view` → `materialize_dataframe_as_cache_view` → `register_collected_memtable` | `crates/repark-python/src/session.rs:419-438`; `crates/repark-core/src/session/temp_views.rs:88-96` | collects the frame once (the optimizer runs `ReaderParseRule` here, P3) and registers a `MemTable` temp view; later scans never re-plan |
| C5 | `unpersist` | `core.py:745-767` | releases the view hold, restores `_inner` from `_lineage_inner`; a later `cache()` starts again at C1 |
| C6 | `localCheckpoint` / `checkpoint` | `surface_a.py:375-396`; `core.py:784-791`; `cache_handle.bind_checkpoint_scan` `cache_handle.py:120-124` | eager: inside the own-plan scope at the call; lazy (`eager=False`): at the first action, as C3a |
| C7 | the object-identity contract | `core.py:701-719` docstring: "Child-plan cache sharing is OUT (architectural)" | a child spawned before C3 keeps the uncached lineage; a twin plan never shares |

## 3. The session's cache table (`CACHE TABLE`, `spark.catalog.cacheTable`)

| # | site | file:line | when it plans |
|---|---|---|---|
| T1 | `CACHE [LAZY] TABLE name` on the `spark.sql` door | `python/repark/src/repark/spark/session/sql_cache_statements.py:33` (prefix), `try_sql_cache_statement` `:42-86`, `_try_cache_table` `:88-100`; routed from `sql_run.py:8`, `:38` | at the statement: hands to T2. **`LAZY` is accepted and cached at once** (module docstring, `:11`). `CACHE TABLE … AS SELECT` is not intercepted and stays a refusal |
| T2 | `catalog_surface.cache_table` | `python/repark/src/repark/spark/catalog_surface.py:530-558`; `Catalog.cache_table` / `cacheTable` `catalog.py:655-668` | **eager for all three doors:** builds `SELECT * FROM <name>` (`:544-548`), `.cache()` or `.persist(level)`, then `.count()` (`:553`), so the cache materialises at the statement under the creating settings (C3a, no memo); records the frame in the token's cache table (`:554-558`) |
| T3 | `session_table` (`spark.table`) | `catalog_surface.py:340-367` | reads the cache overlay: when the token holds a live entry, scans the cached view (`:360`) |
| T4 | the SQL door's reads | `ReparkSession.sql` → the engine | **does not read the overlay:** `SELECT … FROM v` after `CACHE TABLE v` plans `v`'s own body, so unit 1's node decides it live (measured `ctable_sql`, `clazy_sql`) |
| T5 | `uncache_table`, `is_cached`, `clearCache` | `catalog_surface.py:561-600`, `:741`; `cache_handle.py:168-174` | release and query only |
| T6 | Rust `Statement::Cache` / `UNCache` | `crates/repark-spark/src/write_door.rs:215-216` | listed among the statements the write door does not route; the brief's "`CACHE TABLE` in `repark-spark`" has no planning site there |

## 4. The UDF builders and the map bridge

| # | site | file:line | when it plans |
|---|---|---|---|
| U1 | `udf`, `pandas_udf` | `python/repark/src/repark/spark/functions_udf.py:1206`, `:646` | never: they build a column |
| U2 | `select` with a scalar UDF column → `_select_with_python_udfs` / `_select_with_pandas_udfs` | `core.py:1223`, `:1228`; `dataframe/udf_projection.py:204-349`, `:27-201` | builds an intermediate `frame._plan().select(…)` (`:306`, `:145`), then `intermediate.mapInArrow(…)` (`:338`, `:189`): every scalar UDF column becomes a map bridge (U4). `frame._plan()` plans nothing unless `frame` is itself a bridge (U5) |
| U3 | `mapInPandas` | `core.py:677-697` | a `mapInArrow` over a pandas adapter (U4) |
| U4 | `mapInArrow` | `core.py:634-673` | never at the call: registers an empty placeholder view and records `_map_bridge` (parent, function, schema) |
| U5 | **the bridge snapshot:** `_plan` → `_prepare_for_plan` → `_materialize_map_bridge_once` → `_execute_map_in_arrow_bridge(replace_ephemeral_views=False)` → `_iter_map_in_arrow_output` | `core.py:364-368`, `:351-362`, `:391-397`, `:534-564`, `:422-532` (the parent stream at `:454`) | **whenever any frame is built on top of a bridge frame** (`_plan()` has 74 call sites: `select`, `selectExpr` via `filter_quote._select_expr_frame` `filter_quote.py:582-594`, `filter`, `join`, `withColumn`, …): the parent is pulled and the function run once, outside any own-plan scope, under the live settings at that call. Measured: `udf`, `pudf`, `mia`, `mip` each run at the `selectExpr` built under the creating zone |
| U6 | the bridge at an action | `_action_inner` `core.py:374-389` → `_execute_map_in_arrow_bridge` | when the bridge frame itself is acted on and no snapshot exists: inside the action (and its own-plan scope for `collect`), so the parent's parse is decided then |
| U7 | a cached bridge | `_materialize_cache_if_needed` `core.py:326-329` | runs the bridge, then the cache (C3) |

## 5. What each finding rests on

- `cache_view`, `cache_view_sql` (the card's `cache_lazy`): C3b, registration materialises.
- `explain` before the first action: C3b.
- `clazy`, `catcache`: T1 and T2 eager where Spark is lazy.
- `ctable_sql`, `clazy_sql` (count): T4, the SQL door skips the cache.
- The `udf` door: U5 snapshot at child-plan construction; U6 (a bridge acted on directly) decides at the action.
- `child_before`, `twin_plan`: C7.
- `own_then_cache`: C3a inside the own-plan scope reuses a memo settled before `cache()`.

Up: [map.md](map.md).

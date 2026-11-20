# TEMP-VIEW-CACHE-LAZY-PARSE-1 — the domain table

**2,402 cells per build**, each answered by Spark 4.1.2, main `9f9650f5` (`main`) and main before unit 1 `8bc26cba`
(`base`). Verdicts: EQUAL (the same rows), WRONG (other rows, no error: **every WRONG cell is silent**), RAISE
(RePark raises where Spark answers). Spark raised in no cell. Columns: `u` = `unix_timestamp(ts)` (the parse), `t` =
`CAST(ts AS STRING)` and `h` = `hour(ts)` (the zone the frame's own expressions read). Readers: `json_s` (JSON with a
schema), `csv_s` (CSV with a schema), `csv_i` (CSV, `inferSchema`), controls `pq` (Parquet) and `ice` (Iceberg v2).
Directions `la_utc` (create under Los Angeles, read under UTC, third zone Tokyo) and `utc_tk` (UTC, Tokyo, Los
Angeles); ANSI on and off. A door × action row holds 12 text cells (3 readers × 2 directions × 2 modes) and 8
control cells. Raw: [evidence/raw/](evidence/raw/map.md); verdicts `verdicts.jsonl.gz`, counts `summary.txt`.

## 0. Totals

| group | cells | main EQUAL / WRONG / RAISE | base EQUAL / WRONG / RAISE |
|---|---|---|---|
| `R` Spark's rule | 27 | 15 / 12 / 0 | 11 / 16 / 0 |
| `Q` bare caches, twins, other sessions | 15 | 5 / 10 / 0 | 5 / 10 / 0 |
| `C` 14 cache doors × 6 actions | 1,680 | 1,188 / 492 / 0 | 768 / 912 / 0 |
| `N` no cache, the control | 120 | 100 / 20 / 0 | 40 / 80 / 0 |
| `U` UDF frame with the shape on top | 240 | 144 / 96 / 0 | 144 / 96 / 0 |
| `T` UDF frame acted on directly | 240 | 180 / 0 / 60 | 108 / 72 / 60 |
| `L` the label path | 80 | 24 / 56 / 0 | 24 / 56 / 0 |
| **all** | **2,402** | **1,656 / 686 / 60** | **1,100 / 1,242 / 60** |

ANSI on and off give the same counts in every group. Main's WRONG cells by reader: `json_s`, `csv_s`, `csv_i` 200
each, `pq` 32 and `ice` 32 (the join and `writeTo` stamp cells, F7 and F8).

## 1. Spark's rule cells (`R`, `Q`; `json_s`, `csv_i`, `pq`; ANSI on; create LA, then UTC, then Tokyo)

| cell | Spark | main | base |
|---|---|---|---|
| `lazy_two`: `cache()`; `SET`; `collect`; `SET`; `collect`, derived, `count` | UTC parse throughout | EQUAL | WRONG (text) |
| `own_then_cache`: `collect` under UTC, `cache()`, `collect` under Tokyo, `count`, derived, `collect` | own plan UTC; the cache Tokyo (materialised by `count`) | WRONG (text: derived UTC) | WRONG |
| `child_before`: `cache()`, a child; `SET`; the child's `collect`, then the parent's | UTC for both | WRONG (text: the parent Tokyo) | WRONG |
| `explain_first`: `cache()`, `explain`; `SET`; `collect` | UTC | WRONG (text: LA, `explain` materialised) | WRONG |
| `newsess_lazy`, `newsess_mat`: a shaped frame in another session under Tokyo | its own zone (stamps differ, no match) | EQUAL | `newsess_lazy` WRONG |
| `udf_cache`, `mia_cache`: a cached UDF or map frame; `SET`; `collect` | UTC | WRONG (text: LA) | WRONG |
| `clazy_then`: `CACHE LAZY TABLE`; `SET`; SQL read, then `spark.table`, then the frame | UTC for all three | WRONG (text: UTC, LA, Tokyo) | WRONG |
| `Q bare_lazy`, `bare_mat`: a bare reader cache read through a new frame | the cache's parse | WRONG (text) | WRONG |
| `Q newsess_bare_lazy`, `newsess_bare_mat`: the same through another session | the cache's parse, under the caching session's live zone | WRONG (text) | WRONG |
| `Q twin_plan`: a twin of a lazily cached frame acted on first | UTC for both | WRONG (text: the original Tokyo) | WRONG |

`pq` is EQUAL in every `R` and `Q` cell on both builds. The `csv_i` `mia_cache` cell needs `CAST(n AS INT)` (RePark
infers `BIGINT` for the CSV integer column where Spark infers `INT`); the cells cast for both engines.

## 2. The cache doors × actions (`C`): text cells not EQUAL on main / base, of 12

Every control (`pq`, `ice`) row is EQUAL on both builds except the join column of the held doors (8 of 8 WRONG on
both, F7).

| door | `count`* | `collect` | `show` | derived | join | write | family |
|---|---|---|---|---|---|---|---|
| `mat` (`cache()`, `count()`, then `SET`) | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | control |
| `cache` | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 12 / 12 | 0 / 12 | F7 (join) |
| `persist_mo` (`MEMORY_ONLY`) | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 12 / 12 | 0 / 12 | F7 |
| `persist_disk` (`DISK_ONLY`) | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 12 / 12 | 0 / 12 | F7 |
| `cache_view` (`cache()`, a view; `spark.table`) | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | **F1** |
| `cache_view_sql` (the same; `spark.sql`) | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | **F1** |
| `ctable` (`CACHE TABLE`; `spark.table`) | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | control |
| `ctable_sql` (`CACHE TABLE`; `spark.sql`) | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | **F3** |
| `clazy` (`CACHE LAZY TABLE`; `spark.table`) | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | **F2** |
| `clazy_sql` (`CACHE LAZY TABLE`; `spark.sql`) | 12 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | **F3** (count) |
| `catcache` (`spark.catalog.cacheTable`) | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | 12 / 12 | **F2** |
| `recache` (`unpersist`, `cache()` before `SET`) | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 12 / 12 | 0 / 12 | F7 |
| `recache_after` (re-cache after `SET`) | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 12 / 12 | 0 / 12 | F7 |
| `after_set` (`cache()` after `SET`) | 0 / 12 | 0 / 12 | 0 / 12 | 0 / 12 | 12 / 12 | 0 / 12 | F7 |

\* `count` is answered as the count, then a `collect` after a `SET` to the third zone, which shows where `count`
materialised. The WRONG cells differ in `u`, `t` and `h` (F1, F2, F3: the parse and the stamps move together), the
join cells in `t` and `h` only. Spark's `persist(MEMORY_ONLY)` and `persist(DISK_ONLY)` answer as its `cache()` in
all 240 of their cells.

## 3. The no-cache control (`N`): a held frame built under one zone, acted on after the `SET`

| action | text, main / base | controls, main / base |
|---|---|---|
| `count`, `collect`, `show`, derived, write | 0 / 12 each | 0 / 0 |
| join with a Parquet frame built after the `SET` | 12 / 12 | 8 / 8 |

**F7:** the join answers the held frame's `CAST(ts AS STRING)` and `hour(ts)` under the live zone (`t`, `h`; `u`
EQUAL), where Spark keeps the frame's stamps, for Parquet and Iceberg as well, on both builds. No cache is involved;
with the six held cache doors' joins it is 140 cells.

## 4. The UDF doors (`U`, `T`): text cells not EQUAL on main / base, of 12

| kind | `U` first | `U` second | `U` countfirst | `T` first | `T` second | `T` countfirst |
|---|---|---|---|---|---|---|
| `udf` | 12 / 12 | 0 / 0 | 12 / 12 | 0 / 12 | 0 / 0 | 0 / 12 |
| `pandas_udf` | 12 / 12 | 0 / 0 | 12 / 12 | 0 / 12 | 0 / 0 | 0 / 12 |
| `mapInArrow` | 12 / 12 | 0 / 0 | 12 / 12 | 0 / 12 | 0 / 0 | 0 / 12 |
| `mapInPandas` | 12 / 12 | 0 / 0 | 12 / 12 | RAISE 12 / 12 | RAISE 12 / 12 | RAISE 12 / 12 |

`U` builds the timestamp shape on top of the UDF frame (the card's door); `T` builds it underneath, so the UDF
frame is the one acted on. `first`: built, `SET`, `collect`; `second`: `collect`, `SET`, `collect`; `countfirst`:
`count`, `SET`, `collect`. Controls: EQUAL everywhere except `T mapInPandas`, which raises on every reader (8 of 8)
on both builds: `mapInArrow schema mismatch on field 't': expected type string (string), got large_string` (a
`STRING` column back through pandas 3.0.5). `U`'s WRONG cells differ in `u`, `t`, `h` (**F4**).

## 5. The label path (`L`): into an Iceberg v2 table after the `SET`, read back

| source → write | text, main / base | controls, main / base | columns |
|---|---|---|---|
| lazily cached frame → `writeTo().create()` | 12 / 12 | 8 / 8 | `t`, `h` (**F8**: Spark's writer re-analyses under its zone and misses the cache) |
| lazily cached frame, view made before `SET` → `INSERT INTO … SELECT` | 12 / 12 | 0 / 0 | `u`, `t`, `h`, stored instant (**F1**) |
| UDF frame → `writeTo().create()` | 12 / 12 | 0 / 0 | `u`, `t`, `h`, stored instant (**F4**) |
| UDF frame, view made before `SET` → `INSERT INTO … SELECT` | 12 / 12 | 0 / 0 | `u`, `t`, `h`, stored instant (**F4**) |

## 6. The WRONG set on main by family, and what unit 1 moved

| family | cells | where |
|---|---|---|
| F1 registration materialises | 158 | `cache_view`, `cache_view_sql` 144; `explain_first` 2; `L` cached `INSERT` 12 |
| F2 lazy treated as eager | 146 | `clazy` 72, `catcache` 72, `clazy_then` 2 |
| F3 the SQL door skips the cache | 84 | `ctable_sql` 72, `clazy_sql` `count` 12 |
| F4 the bridge snapshot | 124 | `U` first and countfirst 96; `udf_cache`, `mia_cache` 4; `L` UDF 24 |
| F5 object-identity cache | 12 | `Q` 10, `child_before` 2 |
| F6 cache after a first own-plan action | 2 | `own_then_cache` |
| F7 held frame joined after a `SET` | 140 | `N` join 20, held doors' join 120 |
| F8 `writeTo` of a cached frame | 20 | `L` cached `writeTo` |
| **all WRONG** | **686** | every one silent: text readers 600 and controls 64 (F7 56, F8 8) in `C`, `N`, `U`, `L`; `R` and `Q` 22 |

**Unit 1, base → main: 556 cells WRONG → EQUAL, 0 EQUAL → WRONG, 0 RAISE moved.** `C` 420 (the six held doors ×
five actions × 12 = 360, `clazy_sql`'s five non-`count` actions 60), `N` 60 (five actions × 12), `T` 72 (`udf`,
`pandas_udf`, `mapInArrow`, `first` and `countfirst`), `R` 4 (`lazy_two`, `newsess_lazy`). The card's `cache_lazy`
(here `cache_view`) and `udf` (here `U`) cells did not move.

## 7. When each door plans (the trace, main, `json_s`, create LA, read UTC)

From `evidence/raw/trace-main.jsonl` (the live zone at each wrapped call):

| scenario | the plan runs at | zone | answer |
|---|---|---|---|
| `cache()`, a view, `SET`, `spark.table` | `createOrReplaceTempView` → `_native_for_registration` → `_materialize_cache_if_needed` | LA | LA parse |
| `cache()`, `SET`, `collect` | `collect` → `_own_plan` → `__arrow_c_stream__` → `_action_inner` → materialise | UTC | UTC |
| `cache()`, `SET`, `count`, `collect` | `count` → `_count_rows` → materialise (no memo) | UTC | UTC |
| `cache()`, `SET`, derived `collect` | the child's own action; the parent never materialises | UTC | UTC |
| `cache()`, `explain`, `SET`, `collect` | `explain` → `_explain_text` → registration → materialise, then a `toLocalIterator` | LA | LA |
| a view, `CACHE TABLE` / `CACHE LAZY TABLE` / `catalog.cacheTable`, `SET` | `catalog_surface.cache_table` → `count` → materialise | LA | `spark.table` LA; `spark.sql` UTC (re-planned) |
| `udf`, `pandas_udf`, `mapInArrow`, `mapInPandas`, then `selectExpr`, `SET`, `collect` | `selectExpr` → `_plan` → `_prepare_for_plan` → `_materialize_map_bridge_once` → `_execute_map_in_arrow_bridge` | LA | LA |

Up: [map.md](map.md).

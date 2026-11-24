# TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 — the bar

**Builds.** Main: `S/ddu-work/pkg-rp1d-52f468ce` (`BUILT_FROM` `52f468ce`, main's product tree), label `main`. Head:
`S/ddu-work/pkg-a1-<sha>` (`BUILT_FROM` the head's product commit), label `head`; the bar ran on the first head
package `pkg-a1-a55e6a5f` and the pins again on the final one (see [map.md](./)). Spark 4.1.2 with Iceberg
1.11.0: step 0's recordings, Fable's recordings (`fable-cl`, outside the repository) and two new recordings
([evidence/](evidence/)). Every RePark run printed `repark._native.__file__` from its package.

**Verdicts** as step 0's (EQUAL, WRONG, RAISE; a cell where both engines raise is EQUAL by class). The revision
round's `M` cells keep step 0's verdict where the head's output is byte-identical to main's and are judged afresh
where it is not.

## 1. Step 0's 2,402 cells and the revision round's 240

| group | cells | main EQUAL / WRONG / RAISE | head EQUAL / WRONG / RAISE |
|---|---|---|---|
| `R` Spark's rule | 27 | 15 / 12 / 0 | 21 / 6 / 0 |
| `Q` bare caches, twins, other sessions | 15 | 5 / 10 / 0 | 5 / 10 / 0 |
| `C` 14 cache doors × 6 actions | 1,680 | 1,188 / 492 / 0 | 1,452 / 228 / 0 |
| `N` no cache | 120 | 100 / 20 / 0 | 100 / 20 / 0 |
| `U` UDF frame, shape on top | 240 | 144 / 96 / 0 | 144 / 96 / 0 |
| `T` UDF frame acted on directly | 240 | 180 / 0 / 60 | 180 / 0 / 60 |
| `L` the label path | 80 | 24 / 56 / 0 | 56 / 24 / 0 |
| **step 0** | **2,402** | **1,656 / 686 / 60** | **1,958 / 384 / 60** |
| `M` the revision round | 240 | 78 / 128 / 34 | 98 / 108 / 34 |
| **both rounds** | **2,642** | **1,734 / 814 / 94** | **2,056 / 492 / 94** |

**322 cells move WRONG → EQUAL; 0 EQUAL cells are lost; no cell moves to RAISE.**

## 2. The required 188 (Fable's Q4 split) — all EQUAL

| family | cells | where | main | head |
|---|---|---|---|---|
| F1, registration and `explain` no longer fill | 158 | `C` `cache_view` 72, `cache_view_sql` 72; `R` `explain_first` 2; `L` cached `INSERT … SELECT` 12 | WRONG | EQUAL |
| F8, `writeTo().create()` of a cached frame | 20 | `L` cached `create` (12 text, 8 Parquet and Iceberg) | WRONG | EQUAL |
| F5 children built after `cache()` | 8 | `R` `child_before` 2; `M` `cache_siblings` 6 | WRONG | EQUAL |
| a first own-plan action before `cache()` | 2 | `R` `own_then_cache` | WRONG | EQUAL |

## 3. The 128 forecast cells (TEMP-VIEW-FRAME-CROSSJOIN-1's) — all moved

The six held cache doors' join cells (`cache`, `persist_mo`, `persist_disk`, `recache`, `recache_after`,
`after_set`; 12 text and 8 controls each, 120) and the cached self-join (`selfjoin`, 8) move WRONG → EQUAL: the
frame's own expressions now sit inside the lazy table, so the join no longer re-analyses them under the live
settings. No join-specific code was written. F7's residue stays with TEMP-VIEW-FRAME-CROSSJOIN-1: the 20 `N` join
cells (no cache) are WRONG on both builds.

**Also moved, outside A1's bar:** `M clear_view` 6 (an A2 cell): `clearCache()` now releases the lazy table, so a
view over the cached frame reads its lineage under the live zone, as Spark's.

## 4. Fable's cells (`fable-cl`, 10) — all EQUAL

| cell | Spark | main | head |
|---|---|---|---|
| child `collect` UTC; `unpersist`; Tokyo child, parent | U, U, T | EQUAL | EQUAL |
| the same with child `count` first | T, T | EQUAL | EQUAL |
| the first with `clearCache()` | U, U, T | EQUAL | EQUAL |
| UTC `count`; child; Tokyo child; `unpersist`; LA child, new child | U, U, L | EQUAL | EQUAL |
| UTC own `collect`; `unpersist`; `cache()`; Tokyo child `count`, child, own | U, T, U | EQUAL | EQUAL |
| a child's `explain` under UTC, then `collect` under Tokyo | T, T | EQUAL | EQUAL |
| `EXPLAIN SELECT * FROM v` under UTC over a view of a cached frame, then SQL under Tokyo | T, T | **WRONG** (L, L) | **EQUAL** |
| `schema`, `columns`, `storageLevel` under UTC, then Tokyo | T, T | EQUAL | EQUAL |
| `limit(0).count()` under UTC, then Tokyo | T | EQUAL | EQUAL |
| `take(1)` under UTC, then Tokyo child and own | U, U | EQUAL | EQUAL |

The five must-hold shapes are the first five rows. `take(1)` fills the whole table (the fill ignores the scan's
limit and projection); `limit(0)` executes nothing, so the table stays armed.

## 5. Q6, the `writeTo` verbs (new, 160 cells per build)

[evidence/probes/f8.py](evidence/probes/f8.py): `create`, `createOrReplace`, `append`, `overwritePartitions` into an
Iceberg v2 table, five readers, two directions, ANSI on, over a cached frame (`nofill`: the write is the first use;
`fill_made`: `count` under the creating zone first; `fill_read`: `count` under the reading zone, then back to the
creating zone for the write) and over the uncached frame. **Spark answers every verb the same way:** the writer
re-analyses the plan under the session's settings, so it reads the cache when they equal the frame's (`fill_read`:
the fill's parse, the frame's stamps) and misses it when they differ (the live parse and stamps). Main: 80 of 160
EQUAL (every cached `nofill` and `fill_made` cell WRONG). Head: **160 of 160 EQUAL**. The rule therefore sits at
the V2 writer's registration (`writer_layout.run_through_temp_view`, the `_repark_writer_v2_` prefix, which every
`writeTo` verb takes): the lineage when the frame's settings differ from the live ones, else the cache.

## 6. The pins and the other sweeps

- **The pin file** `python/repark/tests/session/test_temp_view_cache_lazy_parse_1.py`: 56 cells recorded from Spark
  (`evidence/raw/pin-spark.json`). Main: 24 not EQUAL (`child_before`, `explain_first`, `explain_view_sql`,
  `own_then_cache`, `siblings`, `view_insert`, `view_read` ×2, the four verbs' `differ` cells; two readers each).
  Head: 56 of 56 EQUAL. Plus RePark-only pins: a child over `repark.cache.max_bytes` raises and the table stays armed
  (retried, then the parent raises too); `repr` of an unfilled cache runs nothing; a 2,000-filter lineage cached, read
  through a view, released and read again in a subprocess on an 8 MB thread.
- **Rust pins** `crates/repark-core/src/session/temp_views/lazy_cache/tests.rs`: planning and `EXPLAIN` fill nothing; a refused
  fill keeps nothing and retries; two concurrent first uses serve one fill; the retained total counts a filled
  table; a released table passes through and keeps nothing; `LIMIT 1` fills the whole table; the session's state
  frees with a filled table registered.
- **Unit 1's pins** (`test_temp_view_frame_reader_parse_1.py`) and **the session-settings pins**
  (`test_temp_view_session_settings_1.py`, `_2.py`): green in the full facade run (see [map.md](./), gates).
- **The reader sweep** (`view2-delta3-work/p1probes/readers.py`, outside the repository; 1,176 cells over every reader and view door): main and the head differ in 2 cells, both a snapshot time inside an Iceberg error message (the clock); 950 cells EQUAL to Spark's recording on both builds; **0 lost**.

## 7. What stays WRONG or RAISE on the head, and where it belongs

| cells | where | home |
|---|---|---|
| 144 + 2 + 84 | `C` `clazy` 72, `catcache` 72; `R` `clazy_then` 2; `C` `ctable_sql` 72, `clazy_sql` `count` 12 | A2 (F2, F3; CACHE-HONESTY-1) |
| 6 | `M refresh` | A2 |
| 96 + 4 + 24 + 90 | `U` first and countfirst; `R` `udf_cache`, `mia_cache`; `L` UDF; `M bridge_*` | seat B (F4): map-bridge frames keep today's materialisation at the action |
| 10 | `Q` twins, bare caches, other sessions | F5's named residual (Q5) |
| 20 | `N` join | TEMP-VIEW-FRAME-CROSSJOIN-1 |
| 12 | `M ice_stale` | ICE-HELD-FRAME-SNAPSHOT-1 (Q11) |
| 60 RAISE | `T mapInPandas` | Q7's card |
| 16 RAISE | `M ctas`, `ctas_lazy` | TEMP-VIEW-ALTER-AND-CACHE-AS-1 (Q12) |
| 18 RAISE | `M` ANSI parse 12, cast 6 | READER-TYPED-SCAN-1 (Q10), SET-ANSI-RUNTIME-3 |

## 8. `EXPLAIN`

A cached frame's plan shows a `LazyCacheExec: pending` leaf before its first use and `LazyCacheExec: filled` after
it, where main showed the lineage before materialisation and a `MemTable` scan after it. Disclosed, as unit 1's Q11.

## 9. Pins whose meaning changed, and an uncovered path

- **C-007 of EAGER-BUDGET-1** (`test_eager_budget_1.py:218` and `:237`): the "before" snapshot now comes after `cache()`, so the armed lazy table counts as present. A refused or failed fill still adds no table, temp name or handle beyond it, and keeps no batches. `:309`'s table count moves from 1 to 2, and `test_cache_persist.py`'s `_cache_view is None` checks become `lazy_cache_filled` checks. The ledger carries a dated note.
- **C-006 of EAGER-BUDGET-1:** the bound is now 60% of the 48 MB cached result (`54ec22b5`): 0.464 on the head and 0.442 on main, over five runs each. A refusal after the collection (a 47 MB budget) reads 1.29x of the cached size and fails the bound (the verifier's mutation run, 2026-11-20).
- **`_bind_lazy`'s fallback** (`cache_handle.py`, the `except` that logs "lazy cache registration fell back to the action") is not covered by any cell. It is reached when the lazy registration raises: the parse memo, plan depth over the maximum, the strip, re-analysis, the stored schema, or `replace_view`. The frame then takes main's eager cache at its first own action. None of the verifier's 26 cached frame shapes took it.

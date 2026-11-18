# Card CACHE-HONESTY-1: `CACHE TABLE` caches a frame that SQL reads of the view do not use

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the TEMP-VIEW-SESSION-SETTINGS-1 step 0 (PR #1356, head `a2e34b9e`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: not rated.** The owner's ruling (question 8) sets this as a separate card; it gives no severity.

## Finding

`CACHE TABLE` on a view builds the cached frame as `SELECT * FROM v` under the live session (`catalog_surface.py:530-558`). SQL reads never consult the cached rows (`_cached_tables` is read only by `catalog.py:258`), so a SQL read re-plans the view and does not read the cache. The cached rows and the re-planned read differ when the data, or a non-deterministic body, changes after the cache.

## Cells

Step 0 domain table §7.2: six cache cells, all wrong on main, each with `isCached` true. A `CACHE TABLE` read after a `SET`, 18 against Spark's 2; a DataFrame view's `CACHE TABLE`; `CACHE LAZY TABLE`; `catalog.cacheTable`; a `SET` before `CACHE TABLE`; and an ANSI off-to-on view that raises where Spark answers. The held frame cached before the `SET` is equal.

On the reading in the step 0 sketch, the settings seats close the six cells without substitution. The build pins that reading.

## Relation

TEMP-VIEW-SESSION-SETTINGS-1 (the settings seats). This card is the substitution question, which the owner ruled out of that unit (question 8).

## Source

TEMP-VIEW-SESSION-SETTINGS-1 step 0 sketch §7, question 8; step 0 domain table §7.2; step 0 revision hand-back, item 3; the round 2 re-critic's verdict (GAPS, `a2e34b9e`), items 2 and 3.

Up: [map.md](map.md).

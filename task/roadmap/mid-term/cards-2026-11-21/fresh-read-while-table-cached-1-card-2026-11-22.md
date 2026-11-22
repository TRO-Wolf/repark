# FRESH-READ-WHILE-TABLE-CACHED-1 — with a table cached, Spark answers every session read from the cache; RePark's SQL reads see the committed state, so a job that mixes both is not repeatable

**Rating: S3, a kept divergence, ruled by the owner 2026-11-22 (proposed by adviser guidance the same day).**

**Date:** 2026-11-22. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** the Spark side is a measurement by a Fable agent on 2026-11-21: Spark 4.1.2 with Iceberg 1.11.0, `type=hadoop`, a second Spark process as the other writer. The RePark side is read from the base tree at `8357bcfb`, not measured. Tags: `[M]` measured, `[R]` read or reasoned, not run.

**Status:** filed 2026-11-22; not dispatched.

**Severity: S3 (owner, 2026-11-22), kept divergence.** RePark is fresher than Spark `[R]`: both answer committed states, RePark a newer one. The cost is that a job mixing a cached table with fresh reads is not repeatable.

## Finding

- **[M] Spark 4.1.2 (Fable measurement, 2026-11-21).** While the table itself is cached, every read of the table in the session answers from the cache (2) until `REFRESH TABLE` or a same-session data write. Fresh `SELECT count(*)` and new frames are included. The cache forms named are `spark.table(t).cache()`, the same with `.alias()`, and `CACHE TABLE t`.
- **[R] RePark, table-level cache.** `catalog_surface.cache_table` (`python/repark/src/repark/spark/catalog_surface.py:530-558`) caches a private `SELECT * FROM <table>` frame and records it in the session's table registry. `CACHE TABLE t` (`python/repark/src/repark/spark/session/sql_cache_statements.py:99`) and `spark.catalog.cacheTable` (`python/repark/src/repark/spark/catalog.py:666`) both reach it. Only `cache_table` writes the registry.
- **[R] RePark, frame-level cache.** `spark.table(t).cache()` does not write the table registry. It records a per-frame identity token (`python/repark/src/repark/spark/dataframe/cache_handle.py:119`, `_note_frame_cached` in `catalog_surface.py:278`).
- **[R] RePark, readers of the registry.** `spark.table` reads it through `catalog_surface.session_table` (`catalog_surface.py:340-367`), whose only caller is `spark.table` (`python/repark/src/repark/spark/session/session_core.py:986`). When the registry entry's snapshot id still matches the one `DESCRIBE TABLE EXTENDED` reports (`_describe_extended_rows`, :136-147), `session_table` builds the new frame from the cached view. Otherwise it drops the entry and unpersists the frame. `is_cached` (:577), `uncache_table` (:561) and `refresh_table` (:731) also read the registry, and `clear_cache` clears it (`python/repark/src/repark/spark/catalog.py:229-258`).
- **[R] RePark, SQL reads.** A `SELECT ... FROM t` does not go through `session_table` or the registry. So a fresh SQL read answers the other writer's committed state (3), not the cache (2).
- **[open] RePark, `spark.table` after another writer's commit.** Whether it answers 2 or 3 depends on whether `DESCRIBE TABLE EXTENDED` in the session reports the new snapshot id. Not measured.
- **[R] Consequence.** RePark is fresher than Spark: both answer committed states, RePark a newer one. A job that mixes a cached table with fresh reads is not repeatable, because the cached read and the fresh read can disagree when another writer commits between them.

## Matching Spark

Matching Spark needs either a load that serves other than it fetched (outside ENC-1), or a session-wide cached-plan substitution seat (its own unit).

## Ruling status

Kept divergence, ruled by the owner 2026-11-22 (proposed by adviser guidance for ICE-HELD-FRAME-SNAPSHOT-1 unit 2 the same day). Matching Spark here would need a table load to serve a state other than the one it fetched, which ENC-1 forbids. This card is the record of the ruling; the label packet lists it with the other kept divergences.

## Scope

One divergence. Spark is measured; RePark is read from the code and not measured. Not dispatched.

**Label path:** owner to decide.

Up: [map.md](map.md).

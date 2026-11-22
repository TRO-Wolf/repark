# CACHED-JOIN-DUPLICATE-COLUMN-1 — caching a join whose output has two columns of the same name fails on RePark, and the frame stays unusable for count and collect; Spark caches and reads it

**Rating: S3, rated by the owner 2026-11-22 (the visibility diagnosis proposed S2).** The refusal is loud, not a wrong answer, and the owner's workload caches no Iceberg frames.

**Date:** 2026-11-22. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** observation (b) of the visibility diagnosis of 2026-11-22. Measured locally with no Iceberg, not live. The raw outputs (`selfjoin-repark.out`, `selfjoin-spark.out`, `dupjoin-repark.out`) and their probes (`probe_selfjoin.py`, `probe_dupjoin.py`) are kept outside the repository. Tags: `[M]` measured, `[R]` read or reasoned. Random cache ids are written `__repark_cache_<id>`.

**Status:** filed 2026-11-22; not dispatched.

**Severity: S3 (owner, 2026-11-22; proposed S2).** Caching an ordinary join with an overlapping non-key column is a common shape. The refusal is loud, but count and collect on the cached frame stay refused.

## Finding

- **[M] The shape.** `df = createDataFrame([(1,'a'),(2,'b')], ['n','c'])`, `j = df.alias('a').join(df.alias('b'), 'n')`. Uncached, `j.columns` is `['n', 'c', 'c']`, and `j.count()` is 2 on both engines.
- **[M] RePark, cache.** `j.cache()` raises at the call: `AnalysisException: Schema error: Schema contains duplicate qualified field name datafusion.public.__repark_cache_<id>.c`.
- **[M] RePark, after the failed cache.** `j.count()` and `j.collect()` raise the same error on every try. `j.columns` still returns `['n', 'c', 'c']`, `j.select("n").count()` returns 2, and `j.select("a.c").collect()` returns `[Row(c='a'), Row(c='b')]`.
- **[M] RePark, temp view.** On the same frame after the failed cache, `j.createOrReplaceTempView("jv")` raises `AnalysisException [COLUMN_ALREADY_EXISTS] The column `c` already exists. Choose another name or rename the existing column. SQLSTATE: 42711`.
- **[M] Spark 4.1.2, same probe.** `j.cache()` works. `count` is 2, `columns` is `['n', 'c', 'c']`, and `collect` is `[(1, 'a', 'a'), (2, 'b', 'b')]`. The temp view is created, and `SELECT count(*) FROM jv` returns 2.
- **[M] RePark, two tables.** `l = createDataFrame([(1,'a')], ['id','name'])`, `r = createDataFrame([(1,'x')], ['id','name'])`, `j = l.join(r, 'id')`. `j.cache()` raises `Schema error: Schema contains duplicate qualified field name datafusion.public.__repark_cache_<id>.name`, and `j.count()` raises the same. So this is not specific to self-joins or to Iceberg. A separate frame, `l.join(r, l.id == r.id).persist()`, returned ok; the probe did not count or collect it.
- **[M] RePark, shapes that work.** A fresh join of the same shape counts 2. A self-join of a cached base frame (`c = df.cache()`, then the same join on `c`) counts 2, and its `select("n")` counts 2.

## Root (the diagnosis's reading) `[R]`

- `python/repark/src/repark/spark/dataframe/cache_handle.py:107` (`bind_registered_view`) points the frame at `SELECT * FROM __repark_cache_<id>`. `_bind_lazy` (:131) reaches this after the lazy registration.
- A named view qualifies every output column with the view name. The two `c` columns, distinct under `a` and `b`, become one qualified name, and DataFusion's DFSchema refuses it.
- The failed bind drops the view and re-raises, but the frame's later actions take the same path again.

## Scope

One observation. The root is the diagnosis's reading; the verdict proposes no fix shape for it. Not dispatched.

**Label path:** owner to decide.

Up: [map.md](map.md).

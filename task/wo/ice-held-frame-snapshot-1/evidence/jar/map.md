# task/wo/ice-held-frame-snapshot-1/evidence/jar/ — `javap -c -p` excerpts

- `javap-refresh.txt` — Spark 4.1.2's execution-time table refresh (`V2TableRefreshUtil$` `refresh`, the identity
  and column checks, the per-plan `currentTables` load; `QueryExecution.lazyTableVersionsRefreshed`), the
  own-plan actions of `classic.Dataset` (`collect` on `queryExecution`, `count` on a new `groupBy().count()`
  Dataset), and Iceberg 1.11's `SparkTable` (`version`, `id`, `determineLatestSnapshot`, `newScanBuilder` reading
  the captured `schema` and `snapshot`), `SparkCatalog`'s cache keys and `CachingCatalog.wrap`, and
  `CachingCatalog`'s `expireAfterAccess`. Extracted from `spark-sql_2.13-4.1.2.jar`,
  `spark-catalyst_2.13-4.1.2.jar` and `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` into a scratch directory, read
  only.

Up: [../map.md](../map.md).

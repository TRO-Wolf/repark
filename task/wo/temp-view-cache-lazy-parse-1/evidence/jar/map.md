# task/wo/temp-view-cache-lazy-parse-1/evidence/jar/ — Spark 4.1.2's cache and Python-exec paths, read from the jar

- [javap-cache.txt](javap-cache.txt) — `javap -c -p` excerpts from `spark-sql_2.13-4.1.2.jar`, classes extracted into a
  scratch directory (the jar untouched): `Dataset.persist`, `Dataset.collectToPython` (the final `queryExecution`
  field), `CacheManager.cacheQuery` / `cacheQueryInternal` / `getOrCloneSessionWithConfigsOff`,
  `CachedRDDBuilder.cachedColumnBuffers` / `buildBuffers`, `BaseCacheTableExec.run`, `Catalog.cacheTable`, and the
  `doExecute` of `BatchEvalPythonExec`, `ArrowEvalPythonExec` and `MapInBatchExec`. The file scan's settings
  (`FileSourceScanExec.inputRDD`, `getSqlConf`) were read by the parent unit:
  [../../../temp-view-frame-reader-parse-1/evidence/jar/](../../../temp-view-frame-reader-parse-1/evidence/jar/map.md).

Up: [../map.md](../map.md).

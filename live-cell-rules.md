# Live Spark cell rules (every new live test)
| rule | why |
|---|---|
| record `SparkSession.getActiveSession()` BEFORE `getOrCreate()`; stop only a session you created | the suite shares one JVM; killing it strands sibling tests |
| single-file seed (one write per fixture) | layout-dependent answers otherwise |
| module-private catalog name (never `local`) | collisions across modules |
| never pop `PYSPARK_SUBMIT_ARGS`; no `spark.jars.ivy` | the harness owns the classpath |
| prove co-collection: `test_parity_live.py::test_live_disclosure_still_diverges` must still collect beside the new leg | a leg that only runs alone is not a pin |
| always-run tests are repark-only; Spark only behind `REPARK_PARITY_LIVE=1` | CI has no JVM |
| `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`, engine via `python/repark/tests/_live_parity.build_spark_iceberg_engine` | the pinned oracle |
| ONE Spark JVM at a time on this box | power budget |

# task/wo/temp-view-frame-reader-parse-1/evidence/jar/ — Spark 4.1.2's file-source settings, read from the jars

- [javap-file-source.txt](javap-file-source.txt) — `javap -c -p` excerpts from `spark-sql_2.13-4.1.2.jar` and
  `spark-catalyst_2.13-4.1.2.jar`, classes extracted into a scratch directory (the jars untouched):
  `JsonFileFormat.getJsonOptions` and its callers, `CSVFileFormat.getCsvOptions` and its callers,
  `SessionStateHelper.getSqlConf`, `FileSourceScanExec.inputRDD$lzycompute`, `ViewHelper$.createTemporaryViewRelation`
  and `ResolveTimeZone$$anonfun$1`. The view-capture classes (`CapturesConfig`, `View$.effectiveSQLConf`,
  `Analyzer$.retainResolutionConfigsForAnalysis`) were read by part 1's step 0:
  [../../../temp-view-session-settings-1/evidence/jar/](../../../temp-view-session-settings-1/evidence/jar/map.md).

Up: [../map.md](../map.md).

# task/wo/temp-view-session-settings-1/evidence/jar/ — Spark 4.1.2 bytecode readings

- `javap-excerpts.txt` — `javap -c -p` excerpts (constant-pool indexes stripped): `CapturesConfig.sqlConfigsToProps`,
  `getModifiedConf`, `shouldCaptureConfig` and the deny and allow lists in `$init$`; `View$.effectiveSQLConf`;
  `Analyzer$.retainResolutionConfigsForAnalysis`, its filter and `trySetAnsiValue`;
  `ViewHelper$.createTemporaryViewRelation`, and which of `prepareTemporaryView`,
  `prepareTemporaryViewStoringAnalyzedPlan` and `generateViewProperties` reach `sqlConfigsToProps`. Read in
  [../../sketch.md](../../sketch.md) §2.

Up: [../map.md](../map.md).

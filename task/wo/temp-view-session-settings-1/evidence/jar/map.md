# task/wo/temp-view-session-settings-1/evidence/jar/ — Spark 4.1.2 bytecode readings

- `javap-excerpts.txt` — `javap -c -p` excerpts (constant-pool indexes stripped): `CapturesConfig.sqlConfigsToProps`,
  `getModifiedConf`, `shouldCaptureConfig` and the deny and allow lists in `$init$`; `View$.effectiveSQLConf`;
  `Analyzer$.retainResolutionConfigsForAnalysis`, its filter and `trySetAnsiValue`;
  `ViewHelper$.createTemporaryViewRelation`, and which of `prepareTemporaryView`,
  `prepareTemporaryViewStoringAnalyzedPlan` and `generateViewProperties` reach `sqlConfigsToProps`. Read in
  [../../sketch.md](../../sketch.md) §2.
- `javap-current-time.txt` — the revision's excerpts: `ComputeCurrentTime`'s match arms (`CurrentDate` and
  `LocalTimestamp` folded with the expression's `zoneId`, `CurrentTimeZone` replaced by the `timezone$1` literal built
  from the optimizer's `conf.sessionLocalTimeZone`) and `ReplaceCurrentLike`'s (`CurrentDatabase`, `CurrentCatalog`,
  `CurrentUser` only). Read in sketch §2.

Up: [../map.md](../map.md).

# Card NATIVE-DATAFRAME-WRITE-SILENT-1: a native-door DataFrame write reports success and stores nothing

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) verifier 2, 2026-11-13, finding F4.

**Status:** filed 2026-11-13, not dispatched. Severity **S1**, a silent lost write, as rated at filing. The verifier
recorded it as S2 and outside its unit's scope.

## Measured

On the native door, on the memory catalog `ice`, a frame from `repark.sql(…)` written through any of these returns
without an error, and the target's rows are unchanged afterwards:

- `.write.mode("append").saveAsTable("ice.ns.src")`;
- `.write.insertInto(…)`;
- `.writeTo(…).append()`.

On the same door, `writeTo(…).create()`, `writeTo(…).createOrReplace()` and `saveAsTable` into a new table or with
overwrite fail to parse on `USING`.

**Builds:** #1317's head `ed9f678c` and its base, main `a3f8c7d9`, identical.

## Not known yet

- The cause. The verifier did not root-cause it.
- Whether the hadoop catalog, Glue and S3 Tables behave the same. Only `ice` is named in the source.

## Fix direction

None given by the source.

## Close

Each of the three calls either stores Spark's rows or refuses loudly before any effect, pinned per catalog kind.

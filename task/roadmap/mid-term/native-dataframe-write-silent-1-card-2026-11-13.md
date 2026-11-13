# Card NATIVE-DATAFRAME-WRITE-SILENT-1: a native-door DataFrame write reports success and stores nothing

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) verifier 2, 2026-11-13, finding F4.

**Status:** filed 2026-11-13, not dispatched. Severity **S1**, a silent lost write, as rated at filing. The verifier
recorded it as S2 and outside its unit's scope.
**Ruled 2026-11-13** (below): measure first, then a refusal guard; it rides v1.5.7.

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

## Owner ruling, 2026-11-13

The owner, verbatim "Defaults", to the board's decision as Fable refined it (Fable guidance, 2026-11-13); the record
is [rulings-2026-11-13.md](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-13.md), item 5.

1. **A measured table first:** every writer verb on every target kind (beside the measured memory catalog, at
   least the hadoop catalog, Glue and S3 Tables listed above), the overwrite forms included. An overwrite that stores nothing may also remove rows; #1317's first
   premise was wrong for the same reason.
2. **Then a refusal guard like #1317's** (NATIVE-CTAS-ICEBERG-REFUSE-1): each verb that cannot store the frame's
   rows refuses loudly before any effect.
3. **It rides v1.5.7,** queued after the label blockers and before the v1.5.7 tag, as #1317 was.
4. **The known-issues text names it now:** `STATUS.md` (Known correctness issues) and the parity registry row
   NATIVE-DATAFRAME-WRITE-SILENT-1 in `docs/spark-sql-iceberg-parity.md`: on the native door, `repark.sql(…)`
   DataFrame appends report success and store nothing in 1.5.6.

## Fix direction

The refusal guard above, after the measured table. No source gives the cause.

## Close

Each of the three calls either stores Spark's rows or refuses loudly before any effect, pinned per catalog kind.

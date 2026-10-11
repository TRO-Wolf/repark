# TEMP-CHECKPOINT-1 — `foreachBatch` with no `checkpointLocation` runs on a temporary checkpoint (Spark-matching answer)

**Filed:** 2026-10-09 by the MB-4 lane as the W-Q2 follow-through. W-Q2 is ruled REFUSE
EVERYWHERE for this slice (owner's delegate, 2026-10-08 evening): the product keeps refusing
with the W8 text on every door including `foreachBatch` (MBE-4). This card holds the
Spark-matching answer for a future slice, measured, not inferred.

## What Spark does (MB-0c cells D2 and D3)

`writeStream.foreachBatch(body).trigger(availableNow=True).start()` with no `checkpointLocation`
runs: one batch per available append (D2: a 3-row batch written, no error). At analysis Spark
logs this warning on the driver (D3, exact text, the random temp path normalized):

`WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn't fail: $TEMP_CHECKPOINT. If it's required to delete it under any circumstances, please set spark.sql.streaming.forceDeleteTempCheckpointLocation to true. Important to know deleting temp checkpoint folder is best effort.`

The temporary directory is `/tmp/temporary-<uuid>`; it is deleted when the query does not fail.
A second start gets a fresh temporary directory and replays from the start (D3: both starts read
batch 0 with the same 3 rows; the sink holds each row twice).

## What RePark does today

Every start door without the `checkpointLocation` option or the
`spark.sql.streaming.checkpointLocation` conf refuses with `AnalysisException`
`_LEGACY_ERROR_TEMP_1298` and the W8 text verbatim (MBE-4, MB0-W8), pinned as the W8 rule with
its citation, never as a parity answer. Divergence DM-1 (MB-4 ledger) stays dated and open.

## Design questions for the matching slice (unruled)

Where the temporary directory lives and when it is deleted (copy Spark's create-on-start and
delete-on-clean-termination, or keep it for post-mortem); whether
`spark.sql.streaming.forceDeleteTempCheckpointLocation` rides along; where the warning is logged
(Spark logs it on the driver — the facade has no driver log today); how a no-checkpoint query
restarts inside the offset machinery (a fresh temp means replay, so resume planning must treat
"no checkpoint" as "from the start", never as an error). Step 0 is recorded (cells D2, D3). This
card closes when a slice lands temporary-checkpoint support or the owner declines it.

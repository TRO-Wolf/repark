# Card S3T-STAGE-CREATE-ORACLE-1: on S3 Tables the flat matrix's `createOrReplace` seat has no Spark answer to grade against

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** the 2026-11-12 morning board
([page](../../../docs/artifacts/morning-board-2026-11-12.html), "For the records clerk", item "Matrix"), live
flat-matrix runs `43203902018` and `43368025158`, and the parity entry S3T-CTAS-KILL-1 in
[spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Status:** filed 2026-11-13, not dispatched, unrated: a harness grading question, not a RePark wrong answer. It
waits on one owner question (below).

## Measured

- On S3 Tables, Spark's own `createOrReplace` and CTAS reach the REST endpoint's Stage-create refusal (HTTP 400,
  `Stage-create is currently not supported`; AWS documents it).
- So the flat-matrix seat `column-api|create-or-replace` labels every value group ACCEPTED-WHERE-SPARK-REFUSED, and
  RePark answers (runs `43203902018` and `43368025158`).
- The board's line: "createOrReplace on S3 Tables, which RePark answers and Spark refuses (Stage-create)".

**Builds:** main at each run; the run records name the commit.

## Open question (owner)

May the harness count this seat as a declared Spark-side limitation (Spark cannot stage-create through the S3
Tables REST endpoint), rather than as ACCEPTED-WHERE-SPARK-REFUSED?

## Fix direction

None until the ruling. Either answer leaves RePark's behaviour as it is; the ruling sets the harness label.

## Close

The owner's ruling, then the harness label and a pinned classifier cell to match.

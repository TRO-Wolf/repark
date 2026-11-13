# Card S3T-STAGE-CREATE-ORACLE-1: on S3 Tables the flat matrix's `createOrReplace` seat has no Spark answer to grade against

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** the 2026-11-12 morning board
([page](../../../docs/artifacts/morning-board-2026-11-12.html), "For the records clerk", item "Matrix"), live
flat-matrix runs `43203902018` and `43368025158`, and the parity entry S3T-CTAS-KILL-1 in
[spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Status:** filed 2026-11-13, not dispatched, unrated: a harness grading question, not a RePark wrong answer.
**Ruled 2026-11-13** (below): a declared Spark-side limitation on S3 Tables only, under two conditions. The harness
change is queued, not built.

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

## Owner ruling, 2026-11-13

The owner, verbatim "Defaults", to the board's decision as Fable refined it (Fable guidance, 2026-11-13); the record
is [rulings-2026-11-13.md](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-13.md), item 4. **Yes, declared, on S3 Tables only**, under two conditions:

1. **Spark's exact refusal.** The cell is declared only when Spark's outcome is HTTP 400 with
   `Stage-create is currently not supported`. Any other Spark error, or a Spark success, turns the cell red. The same
   outcome on Glue stays red.
2. **A substitute check for RePark's answer.** Declaring the seat removes its oracle, so Spark reads back the table
   RePark wrote, and its rows must equal the Glue cell's rows for the same value group.

Fable also asks the label text to say that on S3 Tables create-or-replace and CTAS have no Spark answer through the
REST endpoint, next to the C′ caveat.

## Fix direction

RePark's behaviour stays as it is. The harness change is **queued, not built**: the seat's label under the two
conditions, and the Spark read-back against the Glue rows. Fable did not check whether the harness already reads
back.

## Close

The harness change, with a pinned classifier cell for each condition (the exact refusal declared; any other Spark
outcome red; a read-back that differs from the Glue rows red).

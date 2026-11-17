# Card FEXPR-COUNT-STAR-AGG-1: `F.expr('count(*)')` inside `.agg()` refuses where Spark answers

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SIGNED-ZERO-NORMALIZE-1 part B
part 1 verifier 1, 2026-11-13 (finding F8, cell V-G3 on the DataFrame route); FEXPR-WINDOW-NULL-ORDER-1 (PR #1316)
verifier 2, 2026-11-13 (finding C2).

**Status:** filed 2026-11-13, not dispatched. Severity **S3**: an over-refusal, no wrong answer.

## Measured

- `F.expr('count(*)')` inside `.agg(...)` refuses with `No field named "count(Int64(1))"`. Spark 4.1.2 answers.
  Builds: part B's head `b84adbc7` and its base, main `276dc503`, identical.
- Related refusals on the same door, where Spark answers (#1316 verifier 2, C2; #1316's head `35c5de77` and its base,
  main `a3f8c7d9`, identical): `select(F.expr(<aggregate>))`, and a window over an aggregate in `groupBy().agg`.

## Fix direction

None given by the sources.

## Close

`F.expr('count(*)')` in `agg` and `groupBy().agg` answers Spark's count, with the two related forms measured and
either answered or carded, each pinned.

## Append (2026-11-17, afternoon): decimal bare aggregates in an F.expr select

The AVG-DECIMAL-ROUND-1 step 0 (#1352, `9c96a963`) found that decimal bare aggregates in a DataFrame `select` hit this refusal too. `select(F.expr("avg(x)"))` and `select(F.expr("mean(x)"))`, with no `groupBy`, raise `This feature is not implemented: Physical plan does not support logical expression AggregateFunction(…)` where Spark answers: 26 `R` per mode (domain table, "After the inventory critic", M1; inventory, "`avg` — the doors", the `F.expr` row). `selectExpr("avg(x)")` answers, truncated. This is the class this card already names, so no new card is filed.

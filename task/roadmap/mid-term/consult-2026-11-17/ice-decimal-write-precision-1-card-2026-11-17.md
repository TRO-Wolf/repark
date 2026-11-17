# Card ICE-DECIMAL-WRITE-PRECISION-1: the Iceberg writer checks decimal precision only from column bounds, so a 39-digit value commits under `none`, `counts` and past column 100

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the SUM-DEC-I128WRAP-1 step 0 revision (PR #1357, head `2f63e2c7`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S1, label-blocking until the writer checks each value** (owner, 2026-11-17, "defaults on both", question 5(b) of the step 0). The critic's alternative, S2 and not label-blocking once SUM-DEC-I128WRAP-1 lands, was not adopted.

## Finding

The fork's Iceberg writer checks decimal precision only from the column's min and max statistics. Under `write.metadata.metrics.default` set to `none` or `counts` the bounds are not kept, so the check never runs. On a table past 100 columns with default properties, the column past the 100th gets no bounds either. RePark then commits values that do not fit a `decimal(38,s)` column. Spark 4.1.2 cannot read such a table: every scan that touches a bad row fails with `NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION`, with its ANSI on and off alike. A filter to a good row and `count(*)` still answer.

## Cells

Measured by the sum step 0 revision on main `6f80793b` against Spark 4.1.2, in both ANSI modes. Committed 39-digit values, 14 Iceberg statements (both modes):

- `ice/{insert,ctas,merge_cow,merge_mor}/metrics_{none,counts}/c38_0` (8);
- `ice/{insert,ctas}/metrics_{none,counts}/c38_38` (4);
- `ice/{ctas,insert}/wide101/c38_0` (2): a 101-column table with default properties. The 100-column and `full` controls refuse.

The critic measured the same class with `INSERT … SELECT g, sum(x)` and `CTAS` under `none` and `counts`, committing `100000000000000000000000000000000000000` and `-140282366920938463463374607431768211458` into a `decimal(38,0)` column. Spark's reads of the same tables fail on every scan of a bad row (28 of the 44 tables RePark wrote in the revision's cross-read).

Code sites, from the revision: the fork's `parquet_writer.rs:463-466`, `:480-482` and `update` `:322-351` (the check runs from bounds only); `metrics_config.rs:336-347` gives every column past the 100th `MetricsMode::None` when no default is set (read by the critic, measured by the revision for the 101-column case). Iceberg's Java writer checks every value (`DecimalUtil.toReusedFixLengthBytes`, "Cannot write value as decimal(%s,%s), too large").

## Relation

SUM-DEC-I128WRAP-1 (the `sum` route to these commits; that unit removes the `sum` producer). This card is the third, writer-side class: any other out-of-precision decimal batch would commit unchecked under `none` or `counts` and past column 100.

## Fix (proposed in the sketch, not ruled)

A per-value `Decimal128` precision check in the fork or RePark's write path: one vectorised check per decimal column per batch, with its own cost case (sum sketch §6, question 5(b)).

## Source

SUM-DEC-I128WRAP-1 step 0 sketch, §6 question 5(b); step 0 hand-back, inventory I-3; step 0 revision hand-back, items 1 and "Wrong writes, restated"; the sum critic's verdict (GAPS, step 0 `3fecb0ff`), which corrects I-3 and restates question 5. PR #1357 (head `2f63e2c7`).

Up: [map.md](map.md).

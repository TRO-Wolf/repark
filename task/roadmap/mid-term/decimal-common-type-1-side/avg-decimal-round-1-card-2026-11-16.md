# Card AVG-DECIMAL-ROUND-1: `avg` of a DECIMAL truncates where Spark rounds half up

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the "Other causes" table and the Aggregates section; [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** step 0 merged as #1352 (`9c96a963`) and the build is running (2026-11-17). **Label-blocking** (ruled B7).

**Severity: S1** (ruled B7, label-blocking). The sketch proposed S2 in §9 and §10 question 7; the ruling sets S1, and this card carries the ruled value. A wrong answer on a scalar decimal column.

## Finding

`avg` over a DECIMAL column truncates the quotient where Spark rounds it half up. On the measured pair, `2.51 / 3` is `0.836667` on Spark and `0.836666` on main.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `agg/sum_type` and the side cell `avg_k28`: `avg` of a `DECIMAL(28,2)` sum | `0.836667` | `0.836666` (truncates) |
| `HAVING avg(k28) = max(a)` | group 1 (`0.836667` rounds to 1) | no rows |

The table states no ANSI mode for these cells.

## Seat (not diagnosed)

Not diagnosed. The step 0 of a build finds where the decimal `avg` sets its result scale and its rounding.

Up: [map.md](map.md).

## Append (2026-11-17): placement, step 0 scope, and Fable's readings

**Placement (ruling 1):** AVG-DECIMAL-ROUND-1 runs first, before NAN-MINMAX-1 part 2 (item 1 of the order in the [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). The owner's "Defaults." of 2026-11-17 ruled the place. Its step 0 measures:

- precisions 10, 28, 34 and 38, with quotients that do and do not terminate, and negative values;
- grouped and ungrouped aggregation, a merge of partial states, window frames, `avg(DISTINCT …)` and `mean`;
- the DataFrame door: `F.avg`, `F.mean`, `GroupedData.avg`, `agg({"x": "avg"})`, `describe` and `summary`;
- the cells of [DECIMAL-DIVIDE-LITERAL-1](../consult-2026-11-17/decimal-divide-literal-1-card-2026-11-17.md). If they share a seat, they fold in; if not, that card stays its own unit.

**Readings from Fable** (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 2, Q1). These are readings, not checks of RePark's code:

- RePark has its own average accumulator, `crates/repark-functions/src/avg_groups.rs:158` (`SparkAvgGroupsAccumulator`), and imports its own `DecimalAverager` at `avg_groups.rs:16` from `aggregate.rs`. DataFusion's `DecimalAverager::avg` divides with `div_wrapping`, which truncates (`datafusion-functions-aggregate-common/src/utils.rs:157-176`). RePark's copy was not read.
- Registry DEC-4 in [the parity doc](../../../../docs/spark-sql-iceberg-parity.md) records the facade's `avg` result type as `DECIMAL(min(38,p+4), min(38,s+4))`. The rounding is not in that row.
- From memory of Spark, unmeasured: "Spark divides the sum by the count as a decimal division and then casts to `(p + 4, s + 4)`. For wide decimals the division's own scale is cut below `s + 4` first, so the last digits are zeros, not a longer quotient rounded. 'Round half up at `s + 4`' is right only for narrow decimals."

## Append (2026-11-17, afternoon): step 0 merged, scope, Spark's rule, build dispatched

- **Step 0 merged** as #1352 (`9c96a963`). Its sketch, domain table and inventory are in [the step 0 folder](../../../wo/avg-decimal-round-1/map.md). The owner's afternoon ruling of 2026-11-17 (about 14:05 UTC) is in [the rulings record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md).
- **Scope after rulings 1, 4, 7 and 10:** seat A1, with the `mean` alias at the same registration site (ruling 1). The SQL name `mean` is DataFusion's `Avg` and truncates; the alias routes it to A1. So `mean(DISTINCT x), count(x)` (S1 here, ruling 4) refuses like `avg` until AVG-DISTINCT-MIXED-1 lands. The ANSI-off overflow of a decimal `avg` and `mean` (Spark answers NULL, RePark raises at g20 and g24 at p = 38) is fixed here, with the sum unbounded and checked once at `evaluate`, so `s04` keeps answering `1.0000` (ruling 7); ANSI off is outside the label's scope. `avg(d / 3)` is not in this card's scope: its pin belongs to DECIMAL-DIVIDE-LITERAL-1, and after this unit alone it stays wrong (ruling 10).
- **Spark's rule, read from the 4.1.2 jar** (sketch §2, `javap` of `Average` and `DecimalDivideWithOverflowCheck`): `avg` does not use `Divide`. The quotient is `Decimal.$div`, truncated at scale 39, then rounded half up to `s + 4` at every precision. The digit that decides the rounding is kept, so this is one half-up rounding of the exact quotient. Measured: `(38,18)` and `(34,30)` round as `(10,2)` does. This replaces the from-memory warning in the append above (the scale cut below `s + 4` for wide decimals): that memory did not hold for 4.1.2.
- **Build dispatched** on 2026-11-17 under these rulings.

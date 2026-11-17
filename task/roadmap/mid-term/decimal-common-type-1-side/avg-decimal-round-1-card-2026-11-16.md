# Card AVG-DECIMAL-ROUND-1: `avg` of a DECIMAL truncates where Spark rounds half up

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the "Other causes" table and the Aggregates section; [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Label-blocking** (ruled B7).

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

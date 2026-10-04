# Card COALESCE-NAN-DOUBLE-1 — a DOUBLE NaN cast to Decimal128(30, 15) overflows

**Date:** 2026-10-03 · **Filed by:** Grok 4.7, guided execution · **Source:** owner delegate
ruling Q5 of the same day: card the coalesce NaN-to-Decimal overflow with the other two side
findings, in one docs PR, scheduled post-1.5.2. The bug predates the ATTR-ID-1 stack. No
product code in this filing.

## Why

Spark keeps a DOUBLE NaN when `coalesce` mixes it with the decimal literal `-1.0`. RePark
casts that NaN to `Decimal128(30, 15)` and the cast overflows. `typeof(-1.0)` is
`decimal(2,1)` on both engines. Measured 2026-10-03 on Spark 4.1.2 and the installed RePark
wheel (`repark-1.5.1`).

The specified SQL is `SELECT coalesce(x, -1.0) FROM VALUES (CAST('NaN' AS DOUBLE)), (1.5) AS t(x)`.
The same VALUES list with the `coalesce` removed fails on RePark with the same overflow, so
the DataFrame form `F.coalesce(F.col("x"), F.lit(-1.0))` built on that SQL never reaches
`coalesce`. Two tighter shapes, measured the same day, separate the sites: a one-row DOUBLE
NaN coalesced with `-1.0` still overflows, and a Python `double` column through
`F.coalesce(F.col("x"), F.lit(-1.0))` already answers Spark.

| Shape | Spark 4.1.2 | RePark |
|---|---|---|
| `SELECT x FROM VALUES (CAST('NaN' AS DOUBLE)), (1.5) AS t(x)` | `double` column `x`, rows `NaN`, `1.5` | `PySparkException`. Head: `datafusion engine error: Optimizer rule 'simplify_expressions' failed`, caused by `Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN`. |
| `SELECT coalesce(x, -1.0) FROM VALUES (CAST('NaN' AS DOUBLE)), (1.5) AS t(x)` | `double` column `coalesce(x, -1.0)`, rows `NaN`, `1.5` | same `PySparkException` and the same Decimal128(30, 15) head |
| `F.coalesce(F.col("x"), F.lit(-1.0))` on that VALUES frame | `double` column `coalesce(x, -1.0)`, rows `NaN`, `1.5` | same `PySparkException` (the VALUES scan fails first) |
| `SELECT coalesce(CAST('NaN' AS DOUBLE), -1.0)` | `double`, row `NaN` | same overflow head (`simplify_expressions`, then Decimal128(30, 15) on NaN) |
| `coalesce(x, -1.0)` from `VALUES (CAST('NaN' AS DOUBLE))` only | `double` column `coalesce(x, -1.0)`, row `NaN` | `PySparkException`. Head: `Arrow error: Cast error: Cannot cast to Decimal128(30, 15). Overflowing on NaN`. |
| `VALUES (CAST('NaN' AS DOUBLE)), (CAST(1.5 AS DOUBLE))` | `double`, rows `NaN`, `1.5` | `double`, rows `NaN`, `1.5` |
| `F.coalesce(F.col("x"), F.lit(-1.0))` on `createDataFrame([(float("nan"),), (1.5,)], ["x"])` | `double` column `coalesce(x, -1.0)`, rows `NaN`, `1.5` | `double` column `coalesce(x, -1.0)`, rows `NaN`, `1.5` |

`getMessageParameters()` returned null on the RePark exceptions, and `getCondition()` returned
null. The error class is `PySparkException`.

## Decisions (proposed)

Fix direction (*inferred*): the common type of DOUBLE and a decimal literal such as `-1.0` or
`1.5` stays DOUBLE, and a NaN is not cast through `Decimal128(30, 15)`.

| id | decision |
|---|---|
| D-1 | The specified SQL returns Spark's `double` rows `NaN`, `1.5`. |
| D-2 | `coalesce(CAST('NaN' AS DOUBLE), -1.0)` returns `double` `NaN`. |
| D-3 | `VALUES (CAST('NaN' AS DOUBLE)), (1.5)` types `x` as `double` and returns `NaN`, `1.5`. |

The Python-float DataFrame shape already matches Spark, so D-1's DataFrame door is that
VALUES frame once the scan succeeds.

## Steps

| step | worker | what |
|---|---|---|
| 0 | clerk | Done in this card: the specified SQL, the DataFrame on that SQL, and the four isolating shapes. No product code. |
| 1 | executor | D-1, D-2, D-3. Re-measure against live Spark. |

**Home:** the planner path that casts this NaN to `Decimal128(30, 15)` inside
`simplify_expressions` for `coalesce(double, decimal literal)`, and the same cast when a
VALUES list unifies `CAST('NaN' AS DOUBLE)` with the bare literal `1.5`. **Gates:** `make
verify` and a pin of the specified SQL plus `coalesce(CAST('NaN' AS DOUBLE), -1.0)`. This
filing's gate is the docs checks.

## Pointers

- Up: [map.md](map.md)
- Siblings: [variance-alias-1-card-2026-10-03.md](variance-alias-1-card-2026-10-03.md),
  [qualifier-leak-h-1-card-2026-10-03.md](qualifier-leak-h-1-card-2026-10-03.md)

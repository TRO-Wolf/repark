# Card VARIANCE-ALIAS-1 — SQL `variance` is unresolved where Spark returns the sample variance

**Date:** 2026-10-03 · **Filed by:** Grok 4.7, guided execution · **Source:** owner delegate
ruling Q5 of the same day: card the `variance()` alias with the other two side findings,
in one docs PR, scheduled post-1.5.2. The bug predates the ATTR-ID-1 stack. No product code
in this filing.

## Why

Spark treats `variance` as an alias of `var_samp` (sample variance). On the installed RePark
wheel (`repark-1.5.1`) the SQL routine `variance` is unresolved. The DataFrame door and the
`var_samp` control already answer Spark's double. Measured 2026-10-03 on Spark 4.1.2 and that
wheel. The VALUES list `(1.0), (2.0), (4.0)` types `x` as `decimal(2,1)` on both engines. The
DataFrame was `createDataFrame([(1.0,), (2.0,), (4.0,)], ["x"])`, `x` as `double` on both.

| Shape | Spark 4.1.2 | RePark |
|---|---|---|
| `SELECT variance(x) FROM VALUES (1.0),(2.0),(4.0) AS t(x)` | `double` column `variance(x)`, value `2.333333333333333` | `AnalysisException` `UNRESOLVED_ROUTINE`, SQLSTATE `42883`. Head: `[UNRESOLVED_ROUTINE] Cannot resolve routine `variance` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; line 1 pos 7`. `getMessageParameters()` returned null. |
| `SELECT var_samp(x) FROM VALUES (1.0),(2.0),(4.0) AS t(x)` | `double` column `var_samp(x)`, value `2.333333333333333` | `double` column `var_samp(t.x)`, value `2.333333333333333` |
| `F.variance("x")` | `double` column `variance(x)`, value `2.333333333333333` | `double` column `variance(x)`, value `2.333333333333333` |
| `F.var_samp("x")` | `double` column `var_samp(x)`, value `2.333333333333333` | `double` column `variance(x)`, value `2.333333333333333` |

## Decisions (proposed)

Fix direction (*inferred*): register SQL `variance` as the sample-variance routine `var_samp`
already is, so the SQL shape returns that same double.

| id | decision |
|---|---|
| D-1 | SQL `variance(x)` on this VALUES list returns `double` `2.333333333333333`, the Spark column name `variance(x)`, and the same value `var_samp` already returns. |
| D-2 | The DataFrame door stays on the measured answer: `F.variance("x")` already matches Spark's double. |

The DataFrame control's column label is recorded above: RePark's `F.var_samp("x")` names the
column `variance(x)`, and Spark names it `var_samp(x)`. D-1 is the SQL alias.

## Steps

| step | worker | what |
|---|---|---|
| 0 | clerk | Done in this card: the four shapes, both engines. No product code. |
| 1 | executor | D-1 on the SQL routine catalog. Re-measure the four rows against live Spark. |

**Home:** the SQL builtin registry that already answers `var_samp` and raises
`UNRESOLVED_ROUTINE` for `variance` (`crates/repark-core/src/unknown_routine.rs` is the
refusal). The DataFrame pairing that already accepts both names is
`crates/repark-python/src/column/function_dispatch.rs` (`"variance" | "var_samp"`).
**Gates:** `make verify` and a SQL pin of this VALUES list next to `var_samp`. This filing's
gate is the docs checks.

## Pointers

- Up: [map.md](map.md)
- Siblings: [coalesce-nan-double-1-card-2026-10-03.md](coalesce-nan-double-1-card-2026-10-03.md),
  [qualifier-leak-h-1-card-2026-10-03.md](qualifier-leak-h-1-card-2026-10-03.md)

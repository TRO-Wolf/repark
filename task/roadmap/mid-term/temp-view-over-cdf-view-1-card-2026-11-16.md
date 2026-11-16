# Card TEMP-VIEW-OVER-CDF-VIEW-1: a SQL temp view over a `createDataFrame` view raises `TABLE_OR_VIEW_NOT_FOUND` on the internal `__repark_cdf_...` name

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from item 5 of section 6 of the step 0 sketch of CAST-LITERAL-UNWRAP-1, [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md). The owner ruled on 2026-11-16, about 16:25, verbatim: "adopt Fable's, run the string-cast sizing", which adopted the four side cards.

**Status:** filed 2026-11-16, not dispatched. **Severity: S2**, a refusal of an ordinary view chain, not label-blocking (the step 0 default, adopted with the side cards).

## Measured

On the Spark door, in both ANSI modes:

- `CREATE OR REPLACE TEMP VIEW w AS SELECT id, CAST(c AS INT) AS k FROM <createDataFrame view>`, then `SELECT ... FROM w`, raises `TABLE_OR_VIEW_NOT_FOUND`, naming the internal `__repark_cdf_...` view.
- Spark answers the same chain.
- Over a `range`, a Parquet view or a plain SQL view, the same chain answers on main.

The critic's s16 was re-measured at step 0.

## Coverage

A search of `task/roadmap` for `__repark_cdf_` finds four files. Two are not about this failure: [roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md) (the duplicated qualified-field schema name) and [overnight-report-2026-09-12-run9.md](overnight-report-2026-09-12-run9.md) (the goldens store the trailing field). The fourth is the map. The one card that names the internal view is [TEMP-VIEW-SURFACE-LEADS-1](temp-view-surface-leads-1-card-2026-10-29.md): lead 1, where `SHOW VIEWS` lists those internal names. That card does not cover a failed resolution, so this card is needed.

Up: [map.md](map.md).

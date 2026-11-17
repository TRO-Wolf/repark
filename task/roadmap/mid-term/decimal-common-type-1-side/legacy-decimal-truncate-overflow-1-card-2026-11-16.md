# Card LEGACY-DECIMAL-TRUNCATE-OVERFLOW-1: with the legacy truncate flag set, an overflowing DECIMAL union refuses where Spark answers NULL with ANSI off

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), rulings B3 and B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), "The legacy flag"; [sketch §6 and §10 question 3](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7). The flag only skips the common-type rule (ruling B3): the NULL-on-overflow is carded here, not fixed in DECIMAL-COMMON-TYPE-1.

**Severity: S3** (ruled B3 and B7). A refusal where Spark answers NULL, with ANSI off, on a value that does not fit.

## Finding

With `spark.sql.legacy.decimal.retainFractionDigitsOnTruncate=true`, Spark keeps the scale of the legacy type, so a value can overflow. With ANSI off Spark answers NULL for the value that does not fit; with ANSI on it raises `NUMERIC_VALUE_OUT_OF_RANGE`. Main raises an Arrow error in both modes. In range, the flag's rule is DataFusion's rule and agrees with Spark.

## Measured

Ninety-four cells per mode, the flag set by `SET` on both engines, on the three core positions, all 14 pairs, both tables, seven extra cells and an Iceberg insert.

| ANSI | agree | over-raise | both raise | wrong answer |
|---|---|---|---|---|
| off | 64 | 28 | 0 | 2 |
| on | 64 | 0 | 28 | 2 |

The 28 overflow cells are the over-raises with ANSI off and both-raise with ANSI on. The two wrong answers are the cells after `RESET`, where Spark returns to its default rule and main stays on its own; the domain table calls those this unit's ordinary cells, not this card's.

Cells: `legacy/pair/*/tx/*`, `legacy/extra/v_bc`, `legacy/extra/u_const_ab`.

## Seat (not diagnosed)

Whether the analyzer can read the legacy flag is open (sketch §12). Not diagnosed beyond that.

Up: [map.md](map.md).

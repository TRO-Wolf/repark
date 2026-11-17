# Card NAN-QUANTILE-SIGNSET-1: `median` and `percentile_approx` over a sign-set NaN, split out of NAN-MINMAX-1 part 2

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 3 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Parent:** [NAN-MINMAX-1](../nan-minmax-1-card-2026-11-13.md), part 2. **Source:** the parent's "Part 2" section (its quantiles bullet) and the inventory row I4 in [inventory.md](../../../wo/nan-compare-spark-1/inventory.md); the domain table's `median_sign` and `pct_sign` cells in [domain-table.md](../../../wo/nan-compare-spark-1/domain-table.md); Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 2, Q2.

**Status:** filed 2026-11-17, not dispatched. Its place in the order is not ruled.

**Severity: S1, label-blocking** (kept from the parent, ruling 3).

## Scope

`median` and `percentile_approx` over a sign-set NaN.

Inventory row I4 gives the verdict "no for a sign-set NaN". The two seats are DataFusion's `median.rs:597` (`x.compare(*y)`) and RePark's `quantile_summaries.rs:117` (`sort_by(f64::total_cmp)`). The cells are `median_sign` and `pct_sign` in the domain table.

## Reading (Fable, a recommendation)

Fable, Q2 (consult reply of 2026-11-17): the parent's part 2 "holds three seats: the aggregate, the scalar `array_min`/`array_max`, and `median`/`percentile_approx`. Under the one-round cap I would make the aggregate the unit and card the other two, unless step 0 shows one seat serves all three." This card is one of those other two. Step 0 measures whether the quantile seat is shared with `array_min`/`array_max` (NAN-ARRAY-MINMAX-1) before any seat is chosen.

Up: [map.md](map.md).

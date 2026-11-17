# Card NAN-ARRAY-MINMAX-1: `array_min` and `array_max` over a sign-set NaN, split out of NAN-MINMAX-1 part 2

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 3 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Parent:** [NAN-MINMAX-1](../nan-minmax-1-card-2026-11-13.md), part 2. **Source:** the parent's "Part 2" section (its `array_min`/`array_max` bullet) and the inventory row B6 in [inventory.md](../../../wo/nan-compare-spark-1/inventory.md); the domain table's `arr_min` and `arr_max` cells in [domain-table.md](../../../wo/nan-compare-spark-1/domain-table.md); Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 2, Q2.

**Status:** filed 2026-11-17, not dispatched. Its place in the order is not ruled.

**Severity: S1, label-blocking** (kept from the parent, ruling 3).

## Scope

The scalar `array_min` and `array_max` over a sign-set NaN. The parent's part 2 names these in its scope, and ruling 3 splits them out as their own card.

Inventory row B6 (inventory.md) gives the verdict "canonical yes, sign-set no". The row reads: `datafusion-functions-nested/src/min_max.rs:34`, `:115`, `:193`, `:212`, with `min_batch`/`max_batch` and cells `arr_min`, `arr_max`. RePark binds DataFusion's functions at `crates/repark-python/src/column/function_dispatch.rs:444-451`. Inventory row H5 names the same dispatch lines as the natural home of the seat.

## Reading (Fable, a recommendation)

Fable, Q2 (consult reply of 2026-11-17): "Part 2 as carded holds three seats: the aggregate, the scalar `array_min`/`array_max`, and `median`/`percentile_approx`. Under the one-round cap I would make the aggregate the unit and card the other two, unless step 0 shows one seat serves all three." This card is one of those other two.

## Cells

The domain table's `arr_min` and `arr_max` cells, over a sign-set NaN, are the scope. Step 0 measures them on current main before any seat is chosen.

Up: [map.md](map.md).

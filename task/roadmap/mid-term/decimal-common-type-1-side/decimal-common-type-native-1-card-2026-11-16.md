# Card DECIMAL-COMMON-TYPE-NATIVE-1: the native door (`repark.sql()`) answers the decimal common type wrongly

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the Headline, Doors and "After the critic" sections; [sketch §9](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7). The Spark door is the unit; this card is the native door only.

**Severity: S2** (ruled B7). A wrong answer on the native door, on the common-type family.

## Finding

The same decimal common-type family, measured on the native door (`repark.sql()`) over a view of the same rows, is wrong in the same classes as the Spark door, plus refusals of Spark-only names (`if`, `array`, `map_concat`). One agreeing cell is wrong on this door only: the same-type `(38,38)` union answers `0.4999…` (binary noise, `dec:0.49999999999999998874404911728017014784`) for 0.5, where the Spark door answers `0.5…0` exactly.

## Measured

| count, native door | ANSI off | ANSI on |
|---|---|---|
| first count, 655 cells per mode: agree, wrong answers, wrong types, over-raises, both raise | 43, 258, 7, 347, 0 | 43, 258, 7, 341, 6 |
| after the critic (revision, 675 cells): wrong | 632 of 675 | 632 of 675 |

The first count's ANSI-off and ANSI-on rows are from the domain table's Headline. The revision reports one wrong total per door, not per ANSI mode.

## Seat (not diagnosed)

Not diagnosed on this door. The Spark door's seat (sketch §4) is the unit's seat; this card owns whatever the native door's own path adds.

Up: [map.md](map.md).

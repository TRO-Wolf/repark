# Card COLLECTION-FLOAT-EQ-1: array and map functions compare float elements by total order, not IEEE equality

**Date:** 2026-11-14. **Filed by:** the SIGNED-ZERO-CONST-COMPARE-1 clerk (`claude-haiku-5-5`), from the unit's step 0
table and inventory. **Status:** OPEN, filed, not dispatched. **Severity:** S3, the default: neither the sketch nor the
step 0 table states a severity.

**Scope.** The array and map functions and `collect_set` that the signed-zero sketch carded under ruling G
(`task/wo/signed-zero-normalize-1/sketch.md:593`): `array_contains`, `array_position`, `array_remove`,
`array_distinct`, `array_union`/`array_intersect`/`array_except`, `array_has*`, the map subscript and `collect_set`
(the inventory row is `task/wo/signed-zero-normalize-1/evidence/inventory.md:161`). Only `array_contains` was measured
by the step 0 table. The others stay carded on the sketch's D17 measurement.

## Measured: `array_contains(array(<zero>), <other zero>)`

`array_contains(array(-0.0D), 0.0D)` is true on Spark 4.1.2 and false on RePark. The `select` part of the step 0 table
(`task/wo/signed-zero-const-compare-1/evidence/step0/table.md`), both ANSI modes:

| door | pairs | WRONG (opposite sign) | EQUAL (same sign) | REFUSED | NOT-EXPRESSIBLE |
|---|---|---|---|---|---|
| `dataframe` | 142 per ANSI mode | 120 per mode | 22 per mode | 0 | 0 |
| `fexpr` | 142 per ANSI mode | 120 per mode | 22 per mode | 0 | 0 |
| `spark-sql` | 142 per ANSI mode | 120 per mode | 22 per mode | 0 | 0 |
| `native` | 142 (no ANSI mode) | 0 | 0 | 98 | 44 |

The total is 720 WRONG cells, every one answering `false` on RePark where Spark answers `true`. The native door refuses
the `array(...)` routine itself (`REFUSED`, an unresolved routine), so its 98 cells are a door gap, not this fault.
The cells are in `task/wo/signed-zero-const-compare-1/evidence/step0/raw/cells.jsonl.gz` (`op` `array_contains`, `part`
`select`).

The `nan` part's `array_contains` cells (120 WRONG, NaN elements) are NaN constants, not zeros, and are not claimed
here; the signed-zero card gives NaN constants to NAN-COMPARE-LITERAL-1.

## Not changed by SIGNED-ZERO-CONST-COMPARE-1

The unit's literal seat covers comparison operators only, so `array_contains` is outside it. The 720 cells stay WRONG on
the head build, as the after table shows.

## Provenance

`task/wo/signed-zero-const-compare-1/inventory.md` M8 (`array_contains` belongs to this card); `task/wo/signed-zero-normalize-1/order.md:16`
(ruling G, "G yes and defaults", 2026-11-08); `task/roadmap/mid-term/signed-zero-const-compare-1-card-2026-11-14.md`, the
table of cells not changed; `task/wo/signed-zero-normalize-1/evidence/part-b/inventory.md:68` (seat 2 must not wrap
function arguments, so AR01 and AR02 stay EQUAL).

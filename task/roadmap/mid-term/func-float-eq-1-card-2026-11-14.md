# Card FUNC-FLOAT-EQ-1: function-internal float comparisons answer by total order, not IEEE equality

**Date:** 2026-11-14. **Filed by:** the SIGNED-ZERO-CONST-COMPARE-1 clerk (`claude-haiku-5-5`), from the unit's step 0
table and inventory. **Status:** OPEN, filed, not dispatched. **Severity:** S3, the default: neither the sketch nor the
step 0 table states a severity.

**Scope.** The float comparisons inside scalar functions that the signed-zero sketch carded under ruling G
(`task/wo/signed-zero-normalize-1/sketch.md:594`): `nullif`, `greatest`/`least`, `approx_count_distinct` and `mode`.
The `greatest`/`least` ties have their own card, [GREATEST-LEAST-ZERO-TIE-1](greatest-least-zero-tie-1-card-2026-11-14.md),
and are not counted here. `approx_count_distinct` and `mode` were not measured by the step 0 table; they stay carded
on the sketch's D17 measurement.

## Measured: `nullif` with two constant zeros

`nullif(<zero>, <other zero>) IS NULL` is true on Spark 4.1.2 and false on RePark, on every door that can express it.
Example: `nullif(-0.0D, 0.0D) IS NULL` (Spark `true`, RePark `false`). Both ANSI modes, `select` part of the step 0
table (`task/wo/signed-zero-const-compare-1/evidence/step0/table.md`):

| door | pairs | WRONG (opposite sign) | EQUAL (same sign) | NOT-EXPRESSIBLE |
|---|---|---|---|---|
| `dataframe` | 142 per ANSI mode | 120 per mode | 22 per mode | 0 |
| `fexpr` | 142 per ANSI mode | 120 per mode | 22 per mode | 0 |
| `spark-sql` | 142 per ANSI mode | 120 per mode | 22 per mode | 0 |
| `native` | 142 (no ANSI mode) | 80 | 18 | 44 |

The total is 800 WRONG cells. Every WRONG cell answers `false` on RePark; the 150 same-sign cells agree with Spark.
The cells are in `task/wo/signed-zero-const-compare-1/evidence/step0/raw/cells.jsonl.gz` (`op` `nullif`, `part`
`select`), and `task/wo/signed-zero-const-compare-1/evidence/step0/compare.py` joins them.

The `nan` part's `nullif` cells (90 WRONG, NaN against `sqrt(-1)` or `ln(-1)`) are NaN constants, not zeros, and are
not claimed here; the signed-zero card gives NaN constants to NAN-COMPARE-LITERAL-1.

## Not changed by SIGNED-ZERO-CONST-COMPARE-1

The unit's literal seat covers comparison operators only (the signed-zero card lists them), so `nullif` is outside it.
The 800 cells stay WRONG on the head build, as the after table shows.

## Provenance

`task/wo/signed-zero-const-compare-1/inventory.md` M8 (the `nullif` cells belong here); `task/wo/signed-zero-normalize-1/order.md:16`
(ruling G, "G yes and defaults", 2026-11-08) and `order.md:29` (default 12: `nullif` and other function-internal
comparisons carded as FUNC-FLOAT-EQ-1); `task/roadmap/mid-term/signed-zero-const-compare-1-card-2026-11-14.md`, the
table of cells not changed.

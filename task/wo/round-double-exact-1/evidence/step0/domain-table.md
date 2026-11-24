# ROUND-DOUBLE-EXACT-1 step 0 — the domain table

Rendered by [probes/render.py](probes/render.py) from the probe output; do not edit by hand.

## Provenance and size

- **RePark:** main `954423e0`, built `--profile release` in the lane clone; every RePark provenance row
  names `<scratchpad>/lane-round-exact-1/python/repark/src/repark/_native.abi3.so`. **Spark:** 4.1.2 on
  JDK 17.0.15 (`zulu-17`), Iceberg 1.11.0 runtime jar, `local[1]`, hadoop catalog; both ANSI modes.
- **Values:** 519 DOUBLE and 421 FLOAT ([raw/values.json](raw/values.json)):
  halves at every scale from -3 to 18 with the nearest value and ±1, ±2, ±3 ulps (three half points per
  scale: `1 + 5·10^-(s+1)`, a random 12-digit or 5-digit `k + 0.5`, and `-2.5·10^-s`), subnormals, `±0.0`,
  NaN, ±Infinity, large magnitudes, ordinary values and twelve values whose JDK 17 text is not the
  shortest. Scales: -3 to 18 for every context, and 12 extreme scales from -400 to 400 in the misc cells.
- **Doors:** Spark-door SQL (`sql`), `F.round`/`F.bround` (`fcol`), `F.expr` (`fexpr`), the native door
  `repark.sql` (`native`, compared with Spark's SQL answer). **Contexts:** `sel` (SELECT list over an
  Iceberg table), `lit` (a folded literal, 20 values by 22 scales per query), `where` (`round(v, s)` `>`, `=`,
  `<` `v`, NaN rows left out), `group` (`GROUP BY round(v, s)`, one cell per group), `store` (`INSERT …
  SELECT round(v, s)` into DOUBLE or FLOAT, `DECIMAL(10,2)` and `DECIMAL(38,18)`; the DECIMAL stores take
  the rows a DECIMAL can hold, `abs(v) < 10^7` and `< 10^19`, not NaN).
- **Cells:** 585,737 per ANSI mode, 1,171,474 in all, plus
  the misc cells below. **The two ANSI modes agree cell for cell** in every context.
  The SELECT diff table is identical in both modes.
- **Source check:** both RePark doors read back the exact bits Spark stored for every value.

## Headline: non-EQUAL cells per door (each ANSI mode)

| door | `round` non-EQUAL | of | `bround` non-EQUAL | of | SIGN-ZERO | WRONG-VALUE | ERR-FOR-VALUE |
|---|---|---|---|---|---|---|---|
| sql | **11,021** | 74,648 | **20,504** | 71,792 | 14,278 | 17,247 | 0 |
| fcol | **11,158** | 74,651 | **20,504** | 71,792 | 14,278 | 17,384 | 0 |
| fexpr | **11,021** | 74,648 | **20,504** | 71,792 | 14,278 | 17,247 | 0 |
| native | **11,158** | 74,651 | **71,763** | 71,763 | 7,288 | 3,870 | 71,763 |

Per context, all functions:

| door | sel | lit | where | group | store |
|---|---|---|---|---|---|
| sql | 11,585 of 41,360 | 11,585 of 41,360 | 3,264 of 41,272 | 4,146 of 16,068 | 945 of 6,380 |
| fcol | 11,633 of 41,360 | 11,633 of 41,360 | 3,277 of 41,272 | 4,170 of 16,071 | 949 of 6,380 |
| fexpr | 11,585 of 41,360 | 11,585 of 41,360 | 3,264 of 41,272 | 4,146 of 16,068 | 945 of 6,380 |
| native | 24,891 of 41,360 | 24,891 of 41,360 | 21,369 of 41,272 | 9,271 of 16,042 | 2,499 of 6,380 |

- `SIGN-ZERO`: RePark answers `-0.0` where Spark answers `0.0` (Spark's `BigDecimal` has no negative zero).
- `ERR-FOR-VALUE` on `native` is `bround`, which the native door does not have (`UNRESOLVED_ROUTINE`).
- `fexpr` equals `sql` in every context, and `native` `round` equals `fcol` `round`: two kernels (inventory A, B).
- **Types:** FLOAT `bround` answers DOUBLE on `sql`, `fcol` and `fexpr` (Spark: FLOAT), every scale; every other
  SELECT type matches Spark.

## Verdicts by context, door, source and function (ANSI on; ANSI off is identical)

| ctx | door | src | fn | target | EQUAL | SIGN-ZERO | WRONG-VALUE | ERR-FOR-VALUE | VALUE-FOR-ERR | EQUAL-ERR | NULL-MISMATCH |
|---|---|---|---|---|---|---|---|---|---|---|---|
| group | fcol | DOUBLE | bround |  | 3,460 | · | 321 | · | · | · | · |
| group | fcol | DOUBLE | round |  | 3,299 | · | 488 | · | · | · | · |
| group | fcol | FLOAT | bround |  | 1,670 | · | 2,555 | · | · | · | · |
| group | fcol | FLOAT | round |  | 3,472 | · | 806 | · | · | · | · |
| group | fexpr | DOUBLE | bround |  | 3,460 | · | 321 | · | · | · | · |
| group | fexpr | DOUBLE | round |  | 3,311 | · | 475 | · | · | · | · |
| group | fexpr | FLOAT | bround |  | 1,670 | · | 2,555 | · | · | · | · |
| group | fexpr | FLOAT | round |  | 3,481 | · | 795 | · | · | · | · |
| group | native | DOUBLE | bround |  | · | · | · | 3,752 | · | · | · |
| group | native | DOUBLE | round |  | 3,299 | · | 488 | · | · | · | · |
| group | native | FLOAT | bround |  | · | · | · | 4,225 | · | · | · |
| group | native | FLOAT | round |  | 3,472 | · | 806 | · | · | · | · |
| group | sql | DOUBLE | bround |  | 3,460 | · | 321 | · | · | · | · |
| group | sql | DOUBLE | round |  | 3,311 | · | 475 | · | · | · | · |
| group | sql | FLOAT | bround |  | 1,670 | · | 2,555 | · | · | · | · |
| group | sql | FLOAT | round |  | 3,481 | · | 795 | · | · | · | · |
| lit | fcol | DOUBLE | bround |  | 9,522 | 1,710 | 186 | · | · | · | · |
| lit | fcol | DOUBLE | round |  | 9,408 | 1,709 | 301 | · | · | · | · |
| lit | fcol | FLOAT | bround |  | 3,736 | 1,670 | 3,856 | · | · | · | · |
| lit | fcol | FLOAT | round |  | 7,061 | 1,670 | 531 | · | · | · | · |
| lit | fexpr | DOUBLE | bround |  | 9,522 | 1,710 | 186 | · | · | · | · |
| lit | fexpr | DOUBLE | round |  | 9,417 | 1,709 | 292 | · | · | · | · |
| lit | fexpr | FLOAT | bround |  | 3,736 | 1,670 | 3,856 | · | · | · | · |
| lit | fexpr | FLOAT | round |  | 7,100 | 1,670 | 492 | · | · | · | · |
| lit | native | DOUBLE | bround |  | · | · | · | 11,418 | · | · | · |
| lit | native | DOUBLE | round |  | 9,408 | 1,709 | 301 | · | · | · | · |
| lit | native | FLOAT | bround |  | · | · | · | 9,262 | · | · | · |
| lit | native | FLOAT | round |  | 7,061 | 1,670 | 531 | · | · | · | · |
| lit | sql | DOUBLE | bround |  | 9,522 | 1,710 | 186 | · | · | · | · |
| lit | sql | DOUBLE | round |  | 9,417 | 1,709 | 292 | · | · | · | · |
| lit | sql | FLOAT | bround |  | 3,736 | 1,670 | 3,856 | · | · | · | · |
| lit | sql | FLOAT | round |  | 7,100 | 1,670 | 492 | · | · | · | · |
| sel | fcol | DOUBLE | bround |  | 9,522 | 1,710 | 186 | · | · | · | · |
| sel | fcol | DOUBLE | round |  | 9,408 | 1,709 | 301 | · | · | · | · |
| sel | fcol | FLOAT | bround |  | 3,736 | 1,670 | 3,856 | · | · | · | · |
| sel | fcol | FLOAT | round |  | 7,061 | 1,670 | 531 | · | · | · | · |
| sel | fexpr | DOUBLE | bround |  | 9,522 | 1,710 | 186 | · | · | · | · |
| sel | fexpr | DOUBLE | round |  | 9,417 | 1,709 | 292 | · | · | · | · |
| sel | fexpr | FLOAT | bround |  | 3,736 | 1,670 | 3,856 | · | · | · | · |
| sel | fexpr | FLOAT | round |  | 7,100 | 1,670 | 492 | · | · | · | · |
| sel | native | DOUBLE | bround |  | · | · | · | 11,418 | · | · | · |
| sel | native | DOUBLE | round |  | 9,408 | 1,709 | 301 | · | · | · | · |
| sel | native | FLOAT | bround |  | · | · | · | 9,262 | · | · | · |
| sel | native | FLOAT | round |  | 7,061 | 1,670 | 531 | · | · | · | · |
| sel | sql | DOUBLE | bround |  | 9,522 | 1,710 | 186 | · | · | · | · |
| sel | sql | DOUBLE | round |  | 9,417 | 1,709 | 292 | · | · | · | · |
| sel | sql | FLOAT | bround |  | 3,736 | 1,670 | 3,856 | · | · | · | · |
| sel | sql | FLOAT | round |  | 7,100 | 1,670 | 492 | · | · | · | · |
| store | fcol | DOUBLE | bround | DECIMAL(10,2) | 437 | · | 3 | · | · | · | · |
| store | fcol | DOUBLE | bround | DOUBLE | 397 | 116 | 6 | · | · | · | · |
| store | fcol | DOUBLE | round | DECIMAL(10,2) | 432 | · | 8 | · | · | · | · |
| store | fcol | DOUBLE | round | DECIMAL(38,18) | 489 | · | 18 | · | · | · | · |
| store | fcol | DOUBLE | round | DOUBLE | 1,242 | 269 | 46 | · | · | · | · |
| store | fcol | FLOAT | bround | DECIMAL(10,2) | 409 | · | 1 | · | · | · | · |
| store | fcol | FLOAT | bround | FLOAT | 307 | 114 | · | · | · | · | · |
| store | fcol | FLOAT | round | DECIMAL(10,2) | 399 | · | 11 | · | · | · | · |
| store | fcol | FLOAT | round | DECIMAL(38,18) | 378 | · | 35 | · | · | · | · |
| store | fcol | FLOAT | round | FLOAT | 941 | 261 | 61 | · | · | · | · |
| store | fexpr | DOUBLE | bround | DECIMAL(10,2) | 437 | · | 3 | · | · | · | · |
| store | fexpr | DOUBLE | bround | DOUBLE | 397 | 116 | 6 | · | · | · | · |
| store | fexpr | DOUBLE | round | DECIMAL(10,2) | 432 | · | 8 | · | · | · | · |
| store | fexpr | DOUBLE | round | DECIMAL(38,18) | 489 | · | 18 | · | · | · | · |
| store | fexpr | DOUBLE | round | DOUBLE | 1,246 | 269 | 42 | · | · | · | · |
| store | fexpr | FLOAT | bround | DECIMAL(10,2) | 409 | · | 1 | · | · | · | · |
| store | fexpr | FLOAT | bround | FLOAT | 307 | 114 | · | · | · | · | · |
| store | fexpr | FLOAT | round | DECIMAL(10,2) | 399 | · | 11 | · | · | · | · |
| store | fexpr | FLOAT | round | DECIMAL(38,18) | 378 | · | 35 | · | · | · | · |
| store | fexpr | FLOAT | round | FLOAT | 941 | 261 | 61 | · | · | · | · |
| store | native | DOUBLE | bround | DECIMAL(10,2) | · | · | · | 440 | · | · | · |
| store | native | DOUBLE | bround | DOUBLE | · | · | · | 519 | · | · | · |
| store | native | DOUBLE | round | DECIMAL(10,2) | 432 | · | 8 | · | · | · | · |
| store | native | DOUBLE | round | DECIMAL(38,18) | 489 | · | 18 | · | · | · | · |
| store | native | DOUBLE | round | DOUBLE | 1,242 | 269 | 46 | · | · | · | · |
| store | native | FLOAT | bround | DECIMAL(10,2) | · | · | · | 410 | · | · | · |
| store | native | FLOAT | bround | FLOAT | · | · | · | 421 | · | · | · |
| store | native | FLOAT | round | DECIMAL(10,2) | 399 | · | 11 | · | · | · | · |
| store | native | FLOAT | round | DECIMAL(38,18) | 378 | · | 35 | · | · | · | · |
| store | native | FLOAT | round | FLOAT | 941 | 261 | 61 | · | · | · | · |
| store | sql | DOUBLE | bround | DECIMAL(10,2) | 437 | · | 3 | · | · | · | · |
| store | sql | DOUBLE | bround | DOUBLE | 397 | 116 | 6 | · | · | · | · |
| store | sql | DOUBLE | round | DECIMAL(10,2) | 432 | · | 8 | · | · | · | · |
| store | sql | DOUBLE | round | DECIMAL(38,18) | 489 | · | 18 | · | · | · | · |
| store | sql | DOUBLE | round | DOUBLE | 1,246 | 269 | 42 | · | · | · | · |
| store | sql | FLOAT | bround | DECIMAL(10,2) | 409 | · | 1 | · | · | · | · |
| store | sql | FLOAT | bround | FLOAT | 307 | 114 | · | · | · | · | · |
| store | sql | FLOAT | round | DECIMAL(10,2) | 399 | · | 11 | · | · | · | · |
| store | sql | FLOAT | round | DECIMAL(38,18) | 378 | · | 35 | · | · | · | · |
| store | sql | FLOAT | round | FLOAT | 941 | 261 | 61 | · | · | · | · |
| where | fcol | DOUBLE | bround |  | 11,210 | · | 186 | · | · | · | · |
| where | fcol | DOUBLE | round |  | 11,095 | · | 301 | · | · | · | · |
| where | fcol | FLOAT | bround |  | 6,882 | · | 2,358 | · | · | · | · |
| where | fcol | FLOAT | round |  | 8,808 | · | 432 | · | · | · | · |
| where | fexpr | DOUBLE | bround |  | 11,210 | · | 186 | · | · | · | · |
| where | fexpr | DOUBLE | round |  | 11,104 | · | 292 | · | · | · | · |
| where | fexpr | FLOAT | bround |  | 6,882 | · | 2,358 | · | · | · | · |
| where | fexpr | FLOAT | round |  | 8,812 | · | 428 | · | · | · | · |
| where | native | DOUBLE | bround |  | · | · | · | 11,396 | · | · | · |
| where | native | DOUBLE | round |  | 11,095 | · | 301 | · | · | · | · |
| where | native | FLOAT | bround |  | · | · | · | 9,240 | · | · | · |
| where | native | FLOAT | round |  | 8,808 | · | 432 | · | · | · | · |
| where | sql | DOUBLE | bround |  | 11,210 | · | 186 | · | · | · | · |
| where | sql | DOUBLE | round |  | 11,104 | · | 292 | · | · | · | · |
| where | sql | FLOAT | bround |  | 6,882 | · | 2,358 | · | · | · | · |
| where | sql | FLOAT | round |  | 8,812 | · | 428 | · | · | · | · |

## The targeted halves: wrong at their own scale

Each value scale `s` has three half points; the cell is `round(v, s)` at that same `s`. Counts are of 3.
`sql` (datafusion-spark) / `fcol` (DataFusion core); `bround` is RePark's on both.

**DOUBLE `round`**

| s | -3 ulp | -2 | -1 | nearest | +1 | +2 | +3 |
|---|---|---|---|---|---|---|---|
| -3 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -2 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -1 | 0/0 | 0/0 | 0/2 | 0/0 | 0/1 | 0/0 | 0/0 |
| 0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 2 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 0/0 | 0/0 |
| 3 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 4 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 5 | 0/0 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 |
| 6 | 0/0 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 |
| 7 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 8 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 9 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 10 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 11 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 12 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 13 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 14 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 15 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 16 | 1/1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 17 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 18 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 0/0 | 0/0 |

**DOUBLE `bround`**

| s | -3 ulp | -2 | -1 | nearest | +1 | +2 | +3 |
|---|---|---|---|---|---|---|---|
| -3 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -2 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 2 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 3 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 4 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 5 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 6 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 7 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 8 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 | 0/0 |
| 9 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 10 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 11 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 12 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 13 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 14 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 15 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 16 | 1/1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 17 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 18 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |

**FLOAT `round`**

| s | -3 ulp | -2 | -1 | nearest | +1 | +2 | +3 |
|---|---|---|---|---|---|---|---|
| -3 | 0/3 | 0/3 | 0/3 | 0/3 | 0/3 | 0/3 | 0/3 |
| -2 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 1 | 0/0 | 0/0 | 0/0 | 2/2 | 0/0 | 0/0 | 0/0 |
| 2 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 3 | 0/0 | 0/0 | 0/0 | 2/2 | 0/0 | 0/0 | 0/0 |
| 4 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 5 | 0/0 | 0/0 | 0/0 | 2/2 | 0/0 | 0/0 | 0/0 |
| 6 | 0/0 | 0/0 | 0/0 | 3/3 | 0/0 | 0/0 | 0/0 |
| 7 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 8 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 9 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 10 | 0/0 | 0/0 | 1/1 | 1/1 | 0/0 | 0/0 | 0/0 |
| 11 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 12 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 13 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 |
| 14 | 0/0 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |
| 15 | 1/1 | 1/1 | 1/1 | 1/1 | 0/0 | 0/0 | 0/0 |
| 16 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 |
| 17 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 | 0/0 |
| 18 | 1/1 | 1/1 | 1/1 | 2/2 | 0/0 | 0/0 | 0/0 |

**FLOAT `bround`**

| s | -3 ulp | -2 | -1 | nearest | +1 | +2 | +3 |
|---|---|---|---|---|---|---|---|
| -3 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -2 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| -1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 |
| 1 | 1/1 | 1/1 | 1/1 | 1/1 | 3/3 | 3/3 | 3/3 |
| 2 | 2/2 | 2/2 | 2/2 | 2/2 | 3/3 | 3/3 | 3/3 |
| 3 | 2/2 | 2/2 | 2/2 | 2/2 | 3/3 | 3/3 | 3/3 |
| 4 | 2/2 | 2/2 | 2/2 | 2/2 | 3/3 | 3/3 | 3/3 |
| 5 | 2/2 | 2/2 | 2/2 | 3/3 | 3/3 | 3/3 | 3/3 |
| 6 | 2/2 | 2/2 | 2/2 | 2/2 | 3/3 | 3/3 | 3/3 |
| 7 | 3/3 | 3/3 | 3/3 | 2/2 | 2/2 | 2/2 | 2/2 |
| 8 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 9 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 10 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 11 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 12 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 13 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 14 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 15 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 16 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 17 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |
| 18 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 | 2/2 |

## Other value classes: non-EQUAL cells by scale (`sql` / `fcol`, `round`)

**DOUBLE**

| class | n | -3 | -2 | -1 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| special | 13 | 5/5 | 3/5 | 3/3 | 3/3 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 | 5/5 |
| large | 15 | 0/0 | 0/2 | 0/1 | 0/0 | 1/1 | 5/5 | 5/5 | 4/4 | 4/4 | 2/2 | 4/4 | 3/3 | 5/5 | 9/9 | 7/7 | 5/5 | 7/7 | 8/8 | 6/6 | 7/7 | 5/5 | 6/6 |
| ordinary | 41 | 9/9 | 8/8 | 7/7 | 2/2 | 1/1 | 8/8 | 1/1 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 0/0 | 1/1 | 0/0 | 2/2 | 2/2 | 1/1 | 2/2 |
| jdk-not-shortest | 12 | 3/4 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 0/0 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 0/0 | 0/0 | 1/1 | 0/0 | 0/0 | 0/0 |

**FLOAT**

| class | n | -3 | -2 | -1 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| special | 11 | 2/4 | 4/4 | 2/2 | 2/2 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 |
| large | 8 | 0/1 | 1/1 | 0/0 | 0/0 | 3/3 | 0/0 | 0/0 | 2/2 | 1/1 | 0/0 | 2/2 | 1/1 | 2/2 | 4/4 | 2/2 | 2/2 | 4/4 | 2/2 | 3/3 | 4/4 | 3/3 | 2/2 |
| ordinary | 24 | 4/5 | 4/4 | 3/3 | 1/1 | 1/1 | 6/6 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 3/3 | 2/2 | 2/2 | 0/0 | 4/4 | 1/1 | 2/2 | 1/1 |

## The mechanism: unchanged cells against rounding cells (SELECT, ANSI on)

A cell is *unchanged* when JDK 17's text of the value has no more than `s` fractional digits, so Spark
answers the input (or `+0.0` for `-0.0`); otherwise it is a *rounding* cell.

| door | src | fn | unchanged: SIGN-ZERO | unchanged: WRONG | rounding: SIGN-ZERO | rounding: WRONG | cells |
|---|---|---|---|---|---|---|---|
| sql | DOUBLE | round | 19 | 246 | 1,690 | 46 | 11,418 |
| sql | DOUBLE | bround | 19 | 146 | 1,691 | 40 | 11,418 |
| sql | FLOAT | round | 19 | 197 | 1,651 | 295 | 9,262 |
| sql | FLOAT | bround | 19 | 69 | 1,651 | 3,787 | 9,262 |
| fcol | DOUBLE | round | 19 | 250 | 1,690 | 51 | 11,418 |
| fcol | DOUBLE | bround | 19 | 146 | 1,691 | 40 | 11,418 |
| fcol | FLOAT | round | 19 | 199 | 1,651 | 332 | 9,262 |
| fcol | FLOAT | bround | 19 | 69 | 1,651 | 3,787 | 9,262 |

## Named cells (SELECT, `round`, ANSI on): Spark, `sql`, `fcol`

| src | v | s | Spark | sql | fcol |
|---|---|---|---|---|---|
| DOUBLE | `1.005` | 2 | `1.01` | `1.0` | `1.0` |
| DOUBLE | `0.285` | 2 | `0.29` | `0.28` | `0.28` |
| DOUBLE | `1.255` | 2 | `1.26` | `1.25` | `1.25` |
| DOUBLE | `2.675` | 2 | `2.68` | `2.68` | `2.68` |
| DOUBLE | `-22.129547339983247` | 18 | `-22.129547339983247` | `-22.12954733998325` | `-22.12954733998325` |
| DOUBLE | `1.0000000000000004` | 18 | `1.0000000000000004` | `1.0000000000000004` | `1.0000000000000004` |
| DOUBLE | `-0.0` | 2 | `0.0` | `-0.0` | `-0.0` |
| DOUBLE | `-0.4` | 0 | `0.0` | `-0.0` | `-0.0` |
| DOUBLE | `1e+307` | 2 | `1e+307` | `inf` | `inf` |
| DOUBLE | `1.7976931348623157e+308` | 1 | `1.7976931348623157e+308` | `inf` | `inf` |
| DOUBLE | `5e-324` | 2 | `0.0` | `0.0` | `0.0` |
| DOUBLE | `1.949133209895265e+16` | -2 | `1.94913320989526e+16` | `1.94913320989526e+16` | `1.94913320989526e+16` |
| DOUBLE | `1250.0` | -2 | `1300.0` | `1300.0` | `1300.0` |
| DOUBLE | `123456789012345.67` | 1 | `123456789012345.7` | `123456789012345.7` | `123456789012345.7` |
| FLOAT | `1.2549999952316284` | 2 | `1.25` | `1.2599999904632568` | `1.2599999904632568` |

## Misc cells (both modes; a cell differing between modes is marked)

| family | door | verdicts |
|---|---|---|
| bigintscale | native | ERR-FOR-VALUE 2, WRONG-VALUE 2 |
| bigintscale | sql | WRONG-VALUE 3, WRONG-VALUE+TYPE 1 |
| card | native | ERR-FOR-VALUE 4, WRONG-VALUE 2 |
| card | sql | EQUAL 1, ERR-FOR-VALUE 1, WRONG-VALUE 3, WRONG-VALUE+TYPE 1 |
| colscale | fcol | VALUE-FOR-ERR 4 |
| colscale | fexpr | EQUAL-ERR 2, VALUE-FOR-ERR 2 |
| colscale | native | EQUAL-ERR 2, VALUE-FOR-ERR 2 |
| colscale | sql | EQUAL-ERR 2, VALUE-FOR-ERR 2 |
| ctl_dec | native | ERR-FOR-VALUE 10, WRONG-VALUE+TYPE 10 |
| ctl_dec | sql | EQUAL 2, TYPE-ONLY 8, WRONG-VALUE+TYPE 10 |
| ctl_declit | native | ERR-FOR-VALUE 8 |
| ctl_declit | sql | EQUAL 3, WRONG-VALUE 1, WRONG-VALUE+TYPE 4 |
| ctl_int | fexpr | EQUAL 30, EQUAL-ERR (ANSI off: EQUAL) 8, VALUE-FOR-ERR (ANSI off: EQUAL) 8 |
| ctl_int | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) 8, ERR-FOR-VALUE 15, SIGN-ZERO+TYPE 2, VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) 8, WRONG-VALUE+TYPE 13 |
| ctl_int | sql | EQUAL 30, EQUAL-ERR (ANSI off: EQUAL) 8, VALUE-FOR-ERR (ANSI off: EQUAL) 8 |
| extreme | fcol | SIGN-ZERO 4, SIGN-ZERO+TYPE 2, WRONG-VALUE 32, WRONG-VALUE+TYPE 10 |
| extreme | native | ERR-FOR-VALUE 24, SIGN-ZERO 2, WRONG-VALUE 22 |
| extreme | sql | SIGN-ZERO 7, SIGN-ZERO+TYPE 2, WRONG-VALUE 29, WRONG-VALUE+TYPE 10 |
| noscale | fcol | SIGN-ZERO 3, SIGN-ZERO+TYPE 1 |
| noscale | fexpr | SIGN-ZERO 3, SIGN-ZERO+TYPE 1 |
| noscale | native | ERR-FOR-VALUE 2, SIGN-ZERO 2 |
| noscale | sql | SIGN-ZERO 3, SIGN-ZERO+TYPE 1 |
| nullscale | native | EQUAL 2, ERR-FOR-VALUE 2 |
| nullscale | sql | EQUAL 3, TYPE-ONLY 1 |
| nullscale_typed | fcol | EQUAL 3, TYPE-ONLY 1 |
| nullscale_typed | fexpr | EQUAL 3, TYPE-ONLY 1 |
| nullscale_typed | native | EQUAL 2, ERR-FOR-VALUE 2 |
| nullscale_typed | sql | EQUAL 3, TYPE-ONLY 1 |

| cell | door | verdict | Spark (first row) | RePark (first row) |
|---|---|---|---|---|
| `nullscale/DOUBLE/round` | sql | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale/DOUBLE/round` | native | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale_typed/DOUBLE/round` | sql | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale_typed/DOUBLE/round` | native | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale_typed/DOUBLE/round` | fexpr | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale_typed/DOUBLE/round` | fcol | EQUAL | double/int: 0, None | double/int: 0, None |
| `colscale/DOUBLE/round` | sql | EQUAL-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | ERR PySparkException: Execution error: Unsupported type for round scal |
| `colscale/DOUBLE/round` | native | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `colscale/DOUBLE/round` | fexpr | EQUAL-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | ERR PySparkException: Execution error: Unsupported type for round scal |
| `colscale/DOUBLE/round` | fcol | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `noscale/DOUBLE/round` | sql | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/DOUBLE/round` | native | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/DOUBLE/round` | fcol | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/DOUBLE/round` | fexpr | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `bigintscale/DOUBLE/round` | sql | WRONG-VALUE | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `bigintscale/DOUBLE/round` | native | WRONG-VALUE | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `nullscale/DOUBLE/bround` | sql | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale/DOUBLE/bround` | native | ERR-FOR-VALUE | double/int: 0, None | ERR UNRESOLVED_ROUTINE |
| `nullscale_typed/DOUBLE/bround` | sql | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale_typed/DOUBLE/bround` | native | ERR-FOR-VALUE | double/int: 0, None | ERR UNRESOLVED_ROUTINE |
| `nullscale_typed/DOUBLE/bround` | fexpr | EQUAL | double/int: 0, None | double/int: 0, None |
| `nullscale_typed/DOUBLE/bround` | fcol | EQUAL | double/int: 0, None | double/int: 0, None |
| `colscale/DOUBLE/bround` | sql | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `colscale/DOUBLE/bround` | native | EQUAL-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | ERR UNRESOLVED_ROUTINE |
| `colscale/DOUBLE/bround` | fexpr | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `colscale/DOUBLE/bround` | fcol | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `noscale/DOUBLE/bround` | sql | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/DOUBLE/bround` | native | ERR-FOR-VALUE | double/int: 0, 0.0 | ERR UNRESOLVED_ROUTINE |
| `noscale/DOUBLE/bround` | fcol | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/DOUBLE/bround` | fexpr | SIGN-ZERO | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `bigintscale/DOUBLE/bround` | sql | WRONG-VALUE | double/int: 0, 0.0 | double/int: 0, 0.0 |
| `bigintscale/DOUBLE/bround` | native | ERR-FOR-VALUE | double/int: 0, 0.0 | ERR UNRESOLVED_ROUTINE |
| `nullscale/FLOAT/round` | sql | EQUAL | float/int: 0, None | float/int: 0, None |
| `nullscale/FLOAT/round` | native | EQUAL | float/int: 0, None | float/int: 0, None |
| `nullscale_typed/FLOAT/round` | sql | EQUAL | float/int: 0, None | float/int: 0, None |
| `nullscale_typed/FLOAT/round` | native | EQUAL | float/int: 0, None | float/int: 0, None |
| `nullscale_typed/FLOAT/round` | fexpr | EQUAL | float/int: 0, None | float/int: 0, None |
| `nullscale_typed/FLOAT/round` | fcol | EQUAL | float/int: 0, None | float/int: 0, None |
| `colscale/FLOAT/round` | sql | EQUAL-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | ERR PySparkException: Execution error: Unsupported type for round scal |
| `colscale/FLOAT/round` | native | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | float/int: 0, 0.0 |
| `colscale/FLOAT/round` | fexpr | EQUAL-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | ERR PySparkException: Execution error: Unsupported type for round scal |
| `colscale/FLOAT/round` | fcol | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | float/int: 0, 0.0 |
| `noscale/FLOAT/round` | sql | SIGN-ZERO | float/int: 0, 0.0 | float/int: 0, 0.0 |
| `noscale/FLOAT/round` | native | SIGN-ZERO | float/int: 0, 0.0 | float/int: 0, 0.0 |
| `noscale/FLOAT/round` | fcol | SIGN-ZERO | float/int: 0, 0.0 | float/int: 0, 0.0 |
| `noscale/FLOAT/round` | fexpr | SIGN-ZERO | float/int: 0, 0.0 | float/int: 0, 0.0 |
| `bigintscale/FLOAT/round` | sql | WRONG-VALUE | float/int: 0, 0.0 | float/int: 0, 0.0 |
| `bigintscale/FLOAT/round` | native | WRONG-VALUE | float/int: 0, 0.0 | float/int: 0, 0.0 |
| `nullscale/FLOAT/bround` | sql | TYPE-ONLY | float/int: 0, None | double/int: 0, None |
| `nullscale/FLOAT/bround` | native | ERR-FOR-VALUE | float/int: 0, None | ERR UNRESOLVED_ROUTINE |
| `nullscale_typed/FLOAT/bround` | sql | TYPE-ONLY | float/int: 0, None | double/int: 0, None |
| `nullscale_typed/FLOAT/bround` | native | ERR-FOR-VALUE | float/int: 0, None | ERR UNRESOLVED_ROUTINE |
| `nullscale_typed/FLOAT/bround` | fexpr | TYPE-ONLY | float/int: 0, None | double/int: 0, None |
| `nullscale_typed/FLOAT/bround` | fcol | TYPE-ONLY | float/int: 0, None | double/int: 0, None |
| `colscale/FLOAT/bround` | sql | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `colscale/FLOAT/bround` | native | EQUAL-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | ERR UNRESOLVED_ROUTINE |
| `colscale/FLOAT/bround` | fexpr | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `colscale/FLOAT/bround` | fcol | VALUE-FOR-ERR | ERR DATATYPE_MISMATCH.NON_FOLDABLE_INPUT | double/int: 0, 0.0 |
| `noscale/FLOAT/bround` | sql | SIGN-ZERO+TYPE | float/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/FLOAT/bround` | native | ERR-FOR-VALUE | float/int: 0, 0.0 | ERR UNRESOLVED_ROUTINE |
| `noscale/FLOAT/bround` | fcol | SIGN-ZERO+TYPE | float/int: 0, 0.0 | double/int: 0, 0.0 |
| `noscale/FLOAT/bround` | fexpr | SIGN-ZERO+TYPE | float/int: 0, 0.0 | double/int: 0, 0.0 |
| `bigintscale/FLOAT/bround` | sql | WRONG-VALUE+TYPE | float/int: 0, 0.0 | double/int: 0, 0.0 |
| `bigintscale/FLOAT/bround` | native | ERR-FOR-VALUE | float/int: 0, 0.0 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/TINYINT/0/round` | native | SIGN-ZERO+TYPE | tinyint: 0, 0, 0, 0 | double: 0.0, 0.0, 0.0, 0.0 |
| `ctl_int/TINYINT/0/bround` | native | ERR-FOR-VALUE | tinyint: 0, 0, 0, 0 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/TINYINT/25/round` | native | WRONG-VALUE+TYPE | tinyint: 30, 25, 25, 25 | double: 30.0, 25.0, 25.0, 25.0 |
| `ctl_int/TINYINT/25/bround` | native | ERR-FOR-VALUE | tinyint: 20, 25, 25, 25 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/TINYINT/-25/round` | native | WRONG-VALUE+TYPE | tinyint: -30, -25, -25, -25 | double: -30.0, -25.0, -25.0, -25.0 |
| `ctl_int/TINYINT/-25/bround` | native | ERR-FOR-VALUE | tinyint: -20, -25, -25, -25 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/TINYINT/127/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | tinyint: -126, 127, 127, 127 |
| `ctl_int/TINYINT/127/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: 130.0, 127.0, 127.0, 127.0 |
| `ctl_int/TINYINT/127/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | tinyint: -126, 127, 127, 127 |
| `ctl_int/TINYINT/127/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/TINYINT/127/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/TINYINT/127/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/TINYINT/-128/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | tinyint: 126, -128, -128, -128 |
| `ctl_int/TINYINT/-128/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: -130.0, -128.0, -128.0, -128.0 |
| `ctl_int/TINYINT/-128/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | tinyint: 126, -128, -128, -128 |
| `ctl_int/TINYINT/-128/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/TINYINT/-128/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/TINYINT/-128/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/SMALLINT/125/round` | native | WRONG-VALUE+TYPE | smallint: 130, 125, 125, 125 | double: 130.0, 125.0, 125.0, 125.0 |
| `ctl_int/SMALLINT/125/bround` | native | ERR-FOR-VALUE | smallint: 120, 125, 125, 125 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/SMALLINT/-125/round` | native | WRONG-VALUE+TYPE | smallint: -130, -125, -125, -125 | double: -130.0, -125.0, -125.0, -125.0 |
| `ctl_int/SMALLINT/-125/bround` | native | ERR-FOR-VALUE | smallint: -120, -125, -125, -125 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/SMALLINT/32767/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | smallint: -32766, 32767, 32767, 32767 |
| `ctl_int/SMALLINT/32767/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: 32770.0, 32767.0, 32767.0, 32767.0 |
| `ctl_int/SMALLINT/32767/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | smallint: -32766, 32767, 32767, 32767 |
| `ctl_int/SMALLINT/32767/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/SMALLINT/32767/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/SMALLINT/32767/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/SMALLINT/-32768/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | smallint: 32766, -32768, -32768, -32768 |
| `ctl_int/SMALLINT/-32768/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: -32770.0, -32768.0, -32768.0, -32768.0 |
| `ctl_int/SMALLINT/-32768/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | smallint: 32766, -32768, -32768, -32768 |
| `ctl_int/SMALLINT/-32768/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/SMALLINT/-32768/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/SMALLINT/-32768/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/INT/0/round` | native | SIGN-ZERO+TYPE | int: 0, 0, 0, 0 | double: 0.0, 0.0, 0.0, 0.0 |
| `ctl_int/INT/0/bround` | native | ERR-FOR-VALUE | int: 0, 0, 0, 0 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/5/round` | native | WRONG-VALUE+TYPE | int: 10, 5, 5, 5 | double: 10.0, 5.0, 5.0, 5.0 |
| `ctl_int/INT/5/bround` | native | ERR-FOR-VALUE | int: 0, 5, 5, 5 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/15/round` | native | WRONG-VALUE+TYPE | int: 20, 15, 15, 15 | double: 20.0, 15.0, 15.0, 15.0 |
| `ctl_int/INT/15/bround` | native | ERR-FOR-VALUE | int: 20, 15, 15, 15 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/25/round` | native | WRONG-VALUE+TYPE | int: 30, 25, 25, 25 | double: 30.0, 25.0, 25.0, 25.0 |
| `ctl_int/INT/25/bround` | native | ERR-FOR-VALUE | int: 20, 25, 25, 25 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/-25/round` | native | WRONG-VALUE+TYPE | int: -30, -25, -25, -25 | double: -30.0, -25.0, -25.0, -25.0 |
| `ctl_int/INT/-25/bround` | native | ERR-FOR-VALUE | int: -20, -25, -25, -25 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/125/round` | native | WRONG-VALUE+TYPE | int: 130, 125, 125, 125 | double: 130.0, 125.0, 125.0, 125.0 |
| `ctl_int/INT/125/bround` | native | ERR-FOR-VALUE | int: 120, 125, 125, 125 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/-125/round` | native | WRONG-VALUE+TYPE | int: -130, -125, -125, -125 | double: -130.0, -125.0, -125.0, -125.0 |
| `ctl_int/INT/-125/bround` | native | ERR-FOR-VALUE | int: -120, -125, -125, -125 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/2147483647/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | int: -2147483646, 2147483647, 2147483647, 2147483647 |
| `ctl_int/INT/2147483647/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: 2147483650.0, 2147483647.0, 2147483647.0, 2147483647.0 |
| `ctl_int/INT/2147483647/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | int: -2147483646, 2147483647, 2147483647, 2147483647 |
| `ctl_int/INT/2147483647/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/INT/2147483647/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/2147483647/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/INT/-2147483648/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | int: 2147483646, -2147483648, -2147483648, -2147483648 |
| `ctl_int/INT/-2147483648/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: -2147483650.0, -2147483648.0, -2147483648.0, -2147483648.0 |
| `ctl_int/INT/-2147483648/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | int: 2147483646, -2147483648, -2147483648, -2147483648 |
| `ctl_int/INT/-2147483648/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/INT/-2147483648/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/INT/-2147483648/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/BIGINT/15/round` | native | WRONG-VALUE+TYPE | bigint: 20, 15, 15, 15 | double: 20.0, 15.0, 15.0, 15.0 |
| `ctl_int/BIGINT/15/bround` | native | ERR-FOR-VALUE | bigint: 20, 15, 15, 15 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/BIGINT/-15/round` | native | WRONG-VALUE+TYPE | bigint: -20, -15, -15, -15 | double: -20.0, -15.0, -15.0, -15.0 |
| `ctl_int/BIGINT/-15/bround` | native | ERR-FOR-VALUE | bigint: -20, -15, -15, -15 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/BIGINT/1234567890123/round` | native | WRONG-VALUE+TYPE | bigint: 1234567890120, 1234567890123, 1234567890123, 1234567890123 | double: 1234567890120.0, 1234567890123.0, 1234567890123.0, 1234567890123.0 |
| `ctl_int/BIGINT/1234567890123/bround` | native | ERR-FOR-VALUE | bigint: 1234567890120, 1234567890123, 1234567890123, 1234567890123 | ERR UNRESOLVED_ROUTINE |
| `ctl_int/BIGINT/9223372036854775807/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | bigint: -9223372036854775806, 9223372036854775807, 9223372036854775807, 922337203685477580 |
| `ctl_int/BIGINT/9223372036854775807/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: 9.223372036854776e+18, 9.223372036854776e+18, 9.223372036854776e+18, 9.22337203685 |
| `ctl_int/BIGINT/9223372036854775807/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | bigint: -9223372036854775806, 9223372036854775807, 9223372036854775807, 922337203685477580 |
| `ctl_int/BIGINT/9223372036854775807/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/BIGINT/9223372036854775807/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/BIGINT/9223372036854775807/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/BIGINT/-9223372036854775808/round` | sql | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | bigint: 9223372036854775806, -9223372036854775808, -9223372036854775808, -9223372036854775 |
| `ctl_int/BIGINT/-9223372036854775808/round` | native | VALUE-FOR-ERR (ANSI off: WRONG-VALUE+TYPE) | ERR ARITHMETIC_OVERFLOW | double: -9.223372036854776e+18, -9.223372036854776e+18, -9.223372036854776e+18, -9.2233720 |
| `ctl_int/BIGINT/-9223372036854775808/round` | fexpr | VALUE-FOR-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | bigint: 9223372036854775806, -9223372036854775808, -9223372036854775808, -9223372036854775 |
| `ctl_int/BIGINT/-9223372036854775808/bround` | sql | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_int/BIGINT/-9223372036854775808/bround` | native | EQUAL-ERR (ANSI off: ERR-FOR-VALUE) | ERR ARITHMETIC_OVERFLOW | ERR UNRESOLVED_ROUTINE |
| `ctl_int/BIGINT/-9223372036854775808/bround` | fexpr | EQUAL-ERR (ANSI off: EQUAL) | ERR ARITHMETIC_OVERFLOW | ERR ARITHMETIC_OVERFLOW |
| `ctl_dec/1.005/10,3/round` | sql | WRONG-VALUE+TYPE | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:1.0, dec:1.01, dec:1.005, dec:1 | decimal(10,3): dec:1.000, dec:1.010, dec:1.005, dec:1.005 |
| `ctl_dec/1.005/10,3/round` | native | WRONG-VALUE+TYPE | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:1.0, dec:1.01, dec:1.005, dec:1 | decimal(11,3): dec:1.000, dec:1.010, dec:1.005, dec:1.005 |
| `ctl_dec/1.005/10,3/bround` | sql | TYPE-ONLY | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:1.0, dec:1.00, dec:1.005, dec:1 | decimal(10,2)/decimal(10,3)/decimal(8,0)/decimal(9,1): dec:1.0, dec:1.00, dec:1.005, dec:1 |
| `ctl_dec/1.005/10,3/bround` | native | ERR-FOR-VALUE | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:1.0, dec:1.00, dec:1.005, dec:1 | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/-1.005/10,3/round` | sql | WRONG-VALUE+TYPE | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:-1.0, dec:-1.01, dec:-1.005, de | decimal(10,3): dec:-1.000, dec:-1.010, dec:-1.005, dec:-1.005 |
| `ctl_dec/-1.005/10,3/round` | native | WRONG-VALUE+TYPE | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:-1.0, dec:-1.01, dec:-1.005, de | decimal(11,3): dec:-1.000, dec:-1.010, dec:-1.005, dec:-1.005 |
| `ctl_dec/-1.005/10,3/bround` | sql | TYPE-ONLY | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:-1.0, dec:-1.00, dec:-1.005, de | decimal(10,2)/decimal(10,3)/decimal(8,0)/decimal(9,1): dec:-1.0, dec:-1.00, dec:-1.005, de |
| `ctl_dec/-1.005/10,3/bround` | native | ERR-FOR-VALUE | decimal(10,2)/decimal(11,3)/decimal(8,0)/decimal(9,1): dec:-1.0, dec:-1.00, dec:-1.005, de | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/123.456/6,3/round` | sql | WRONG-VALUE+TYPE | decimal(4,0)/decimal(5,1)/decimal(6,2)/decimal(7,3): dec:123.5, dec:123.46, dec:123.456, d | decimal(6,3): dec:123.500, dec:123.460, dec:123.456, dec:123.456 |
| `ctl_dec/123.456/6,3/round` | native | WRONG-VALUE+TYPE | decimal(4,0)/decimal(5,1)/decimal(6,2)/decimal(7,3): dec:123.5, dec:123.46, dec:123.456, d | decimal(7,3): dec:123.500, dec:123.460, dec:123.456, dec:123.456 |
| `ctl_dec/123.456/6,3/bround` | sql | TYPE-ONLY | decimal(4,0)/decimal(5,1)/decimal(6,2)/decimal(7,3): dec:123.5, dec:123.46, dec:123.456, d | decimal(4,0)/decimal(5,1)/decimal(6,2)/decimal(6,3): dec:123.5, dec:123.46, dec:123.456, d |
| `ctl_dec/123.456/6,3/bround` | native | ERR-FOR-VALUE | decimal(4,0)/decimal(5,1)/decimal(6,2)/decimal(7,3): dec:123.5, dec:123.46, dec:123.456, d | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/99.95/4,2/round` | sql | WRONG-VALUE+TYPE | decimal(3,0)/decimal(4,0)/decimal(4,1)/decimal(5,2): dec:100.0, dec:99.95, dec:99.95, dec: | decimal(4,2): dec:100.00, dec:99.95, dec:99.95, dec:99.95 |
| `ctl_dec/99.95/4,2/round` | native | WRONG-VALUE+TYPE | decimal(3,0)/decimal(4,0)/decimal(4,1)/decimal(5,2): dec:100.0, dec:99.95, dec:99.95, dec: | decimal(5,2): dec:100.00, dec:99.95, dec:99.95, dec:99.95 |
| `ctl_dec/99.95/4,2/bround` | sql | TYPE-ONLY | decimal(3,0)/decimal(4,0)/decimal(4,1)/decimal(5,2): dec:100.0, dec:99.95, dec:99.95, dec: | decimal(3,0)/decimal(4,1)/decimal(4,2): dec:100.0, dec:99.95, dec:99.95, dec:99.95 |
| `ctl_dec/99.95/4,2/bround` | native | ERR-FOR-VALUE | decimal(3,0)/decimal(4,0)/decimal(4,1)/decimal(5,2): dec:100.0, dec:99.95, dec:99.95, dec: | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/-22.129547339983247/38,18/round` | sql | WRONG-VALUE+TYPE | decimal(21,0)/decimal(22,1)/decimal(23,2)/decimal(24,3)/decimal(38,18): dec:-22.1, dec:-22 | decimal(38,18): dec:-22.100000000000000000, dec:-22.130000000000000000, dec:-22.1300000000 |
| `ctl_dec/-22.129547339983247/38,18/round` | native | WRONG-VALUE+TYPE | decimal(21,0)/decimal(22,1)/decimal(23,2)/decimal(24,3)/decimal(38,18): dec:-22.1, dec:-22 | decimal(38,18): dec:-22.100000000000000000, dec:-22.130000000000000000, dec:-22.1300000000 |
| `ctl_dec/-22.129547339983247/38,18/bround` | native | ERR-FOR-VALUE | decimal(21,0)/decimal(22,1)/decimal(23,2)/decimal(24,3)/decimal(38,18): dec:-22.1, dec:-22 | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/0.5/3,1/round` | sql | WRONG-VALUE+TYPE | decimal(3,0)/decimal(4,0)/decimal(4,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 | decimal(3,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 |
| `ctl_dec/0.5/3,1/round` | native | WRONG-VALUE+TYPE | decimal(3,0)/decimal(4,0)/decimal(4,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 | decimal(4,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 |
| `ctl_dec/0.5/3,1/bround` | sql | TYPE-ONLY | decimal(3,0)/decimal(4,0)/decimal(4,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 | decimal(3,0)/decimal(3,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 |
| `ctl_dec/0.5/3,1/bround` | native | ERR-FOR-VALUE | decimal(3,0)/decimal(4,0)/decimal(4,1): dec:0.5, dec:0.5, dec:0.5, dec:0.5 | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/2.5/2,1/round` | sql | WRONG-VALUE+TYPE | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:2.5, dec:2.5, dec:2.5, dec:2.5 | decimal(2,1): dec:2.5, dec:2.5, dec:2.5, dec:2.5 |
| `ctl_dec/2.5/2,1/round` | native | WRONG-VALUE+TYPE | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:2.5, dec:2.5, dec:2.5, dec:2.5 | decimal(3,1): dec:2.5, dec:2.5, dec:2.5, dec:2.5 |
| `ctl_dec/2.5/2,1/bround` | sql | TYPE-ONLY | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:2.5, dec:2.5, dec:2.5, dec:2.5 | decimal(2,0)/decimal(2,1): dec:2.5, dec:2.5, dec:2.5, dec:2.5 |
| `ctl_dec/2.5/2,1/bround` | native | ERR-FOR-VALUE | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:2.5, dec:2.5, dec:2.5, dec:2.5 | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/-2.5/2,1/round` | sql | WRONG-VALUE+TYPE | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 | decimal(2,1): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 |
| `ctl_dec/-2.5/2,1/round` | native | WRONG-VALUE+TYPE | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 | decimal(3,1): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 |
| `ctl_dec/-2.5/2,1/bround` | sql | TYPE-ONLY | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 | decimal(2,0)/decimal(2,1): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 |
| `ctl_dec/-2.5/2,1/bround` | native | ERR-FOR-VALUE | decimal(2,0)/decimal(3,1)/decimal(4,0): dec:-2.5, dec:-2.5, dec:-2.5, dec:-2.5 | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/99999999.995/11,3/round` | sql | WRONG-VALUE+TYPE | decimal(10,1)/decimal(11,2)/decimal(12,3)/decimal(9,0): dec:100000000.0, dec:100000000.00, | decimal(11,3): dec:100000000.000, dec:100000000.000, dec:99999999.995, dec:99999999.995 |
| `ctl_dec/99999999.995/11,3/round` | native | WRONG-VALUE+TYPE | decimal(10,1)/decimal(11,2)/decimal(12,3)/decimal(9,0): dec:100000000.0, dec:100000000.00, | decimal(12,3): dec:100000000.000, dec:100000000.000, dec:99999999.995, dec:99999999.995 |
| `ctl_dec/99999999.995/11,3/bround` | sql | TYPE-ONLY | decimal(10,1)/decimal(11,2)/decimal(12,3)/decimal(9,0): dec:100000000.0, dec:100000000.00, | decimal(10,1)/decimal(11,2)/decimal(11,3)/decimal(9,0): dec:100000000.0, dec:100000000.00, |
| `ctl_dec/99999999.995/11,3/bround` | native | ERR-FOR-VALUE | decimal(10,1)/decimal(11,2)/decimal(12,3)/decimal(9,0): dec:100000000.0, dec:100000000.00, | ERR UNRESOLVED_ROUTINE |
| `ctl_dec/1.255/38,18/round` | sql | WRONG-VALUE+TYPE | decimal(21,0)/decimal(22,1)/decimal(23,2)/decimal(24,3)/decimal(38,18): dec:1.3, dec:1.26, | decimal(38,18): dec:1.300000000000000000, dec:1.260000000000000000, dec:1.2550000000000000 |
| `ctl_dec/1.255/38,18/round` | native | WRONG-VALUE+TYPE | decimal(21,0)/decimal(22,1)/decimal(23,2)/decimal(24,3)/decimal(38,18): dec:1.3, dec:1.26, | decimal(38,18): dec:1.300000000000000000, dec:1.260000000000000000, dec:1.2550000000000000 |
| `ctl_dec/1.255/38,18/bround` | native | ERR-FOR-VALUE | decimal(21,0)/decimal(22,1)/decimal(23,2)/decimal(24,3)/decimal(38,18): dec:1.3, dec:1.26, | ERR UNRESOLVED_ROUTINE |
| `ctl_declit/1.005/round` | sql | WRONG-VALUE+TYPE | decimal(4,2)/double: dec:1.01, 1.01, 1.01 | decimal(4,3)/double: dec:1.010, 1.0, 1.0 |
| `ctl_declit/1.005/round` | native | ERR-FOR-VALUE | decimal(4,2)/double: dec:1.01, 1.01, 1.01 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/1.005/bround` | sql | EQUAL | decimal(4,2)/double: dec:1.00, 1.0, 1.0 | decimal(4,2)/double: dec:1.00, 1.0, 1.0 |
| `ctl_declit/1.005/bround` | native | ERR-FOR-VALUE | decimal(4,2)/double: dec:1.00, 1.0, 1.0 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/2.675/round` | sql | WRONG-VALUE+TYPE | decimal(4,2)/double: dec:2.68, 2.68, 2.68 | decimal(4,3)/double: dec:2.680, 2.68, 2.68 |
| `ctl_declit/2.675/round` | native | ERR-FOR-VALUE | decimal(4,2)/double: dec:2.68, 2.68, 2.68 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/2.675/bround` | sql | EQUAL | decimal(4,2)/double: dec:2.68, 2.68, 2.68 | decimal(4,2)/double: dec:2.68, 2.68, 2.68 |
| `ctl_declit/2.675/bround` | native | ERR-FOR-VALUE | decimal(4,2)/double: dec:2.68, 2.68, 2.68 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/1.255/round` | sql | WRONG-VALUE+TYPE | decimal(4,2)/double: dec:1.26, 1.26, 1.26 | decimal(4,3)/double: dec:1.260, 1.25, 1.25 |
| `ctl_declit/1.255/round` | native | ERR-FOR-VALUE | decimal(4,2)/double: dec:1.26, 1.26, 1.26 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/1.255/bround` | sql | WRONG-VALUE | decimal(4,2)/double: dec:1.26, 1.26, 1.26 | decimal(4,2)/double: dec:1.26, 1.25, 1.25 |
| `ctl_declit/1.255/bround` | native | ERR-FOR-VALUE | decimal(4,2)/double: dec:1.26, 1.26, 1.26 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/0.285/round` | sql | WRONG-VALUE+TYPE | decimal(3,2)/double: dec:0.29, 0.29, 0.29 | decimal(3,3)/double: dec:0.290, 0.28, 0.28 |
| `ctl_declit/0.285/round` | native | ERR-FOR-VALUE | decimal(3,2)/double: dec:0.29, 0.29, 0.29 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `ctl_declit/0.285/bround` | sql | EQUAL | decimal(3,2)/double: dec:0.28, 0.28, 0.28 | decimal(3,2)/double: dec:0.28, 0.28, 0.28 |
| `ctl_declit/0.285/bround` | native | ERR-FOR-VALUE | decimal(3,2)/double: dec:0.28, 0.28, 0.28 | ERR ParseException: SQL error: ParserError("Expected: ), found: D at |
| `card/card_float_direct` | sql | WRONG-VALUE+TYPE | float: 1.25, 1.25 | double/float: 1.2599999904632568, 1.25 |
| `card/card_float_direct` | native | ERR-FOR-VALUE | float: 1.25, 1.25 | ERR UNRESOLVED_ROUTINE |
| `card/card_string_arg` | sql | ERR-FOR-VALUE | double: 1.01 | ERR AnalysisException: Error during planning: Internal error: Function  |
| `card/card_string_arg` | native | ERR-FOR-VALUE | double: 1.01 | ERR AnalysisException: Error during planning: Internal error: Function  |
| `card/card_where_eq` | sql | WRONG-VALUE | int: 28 | int: 186 |
| `card/card_where_eq` | native | WRONG-VALUE | int: 28 | int: 186 |
| `card/card_where_eq_bround` | sql | EQUAL | int: 31 | int: 31 |
| `card/card_where_eq_bround` | native | ERR-FOR-VALUE | int: 31 | ERR UNRESOLVED_ROUTINE |
| `card/card_typeof` | sql | WRONG-VALUE | string: 'float', 'float', 'double', 'int' | string: 'float', 'double', 'double', 'int' |
| `card/card_typeof` | native | ERR-FOR-VALUE | string: 'float', 'float', 'double', 'int' | ERR UNRESOLVED_ROUTINE |
| `card/card_nested` | sql | WRONG-VALUE | double: 1.01, 1.01, -1.01 | double: 1.0, 1.0, -1.0 |
| `card/card_nested` | native | WRONG-VALUE | double: 1.01, 1.01, -1.01 | double: 1.0, 1.0, -1.0 |

## The rule and the kernel, measured

- **Model against Spark:** 0 differences over every SELECT cell of both modes
  (the model is [probes/model.py](probes/model.py); see [spark-rule.md](spark-rule.md)).
- **K1 at scale s, converted back once, against Spark's `ROUND(v, s)`:** 0 differences
  in 17,540 cells (s from 0 to 18, DOUBLE and FLOAT; [probes/kcheck.py](probes/kcheck.py)).
- **Every non-EQUAL cell of the SELECT context** is in [raw/sel-diff.tsv](raw/sel-diff.tsv): the source, the
  function, the scale, the value id, Spark's answer as the double's bits, then each door's bits (`=` where
  it equals Spark). 24,891 rows. The `lit` context's non-EQUAL cells are exactly these per door;
  a DOUBLE store is wrong exactly where its SELECT cell is; a FLOAT or DECIMAL store is never wrong where
  its SELECT cell is right.

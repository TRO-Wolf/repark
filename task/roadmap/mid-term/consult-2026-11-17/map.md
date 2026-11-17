# map — task/roadmap/mid-term/consult-2026-11-17/

Four cards from Fable's consult of 2026-11-17 (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, sections 1 to 4) and the owner's ruling "Defaults." of the same date, recorded in [rulings-2026-11-17.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md). Each card's severity and label status are the ruling's, and a card with no ruled severity reads "not rated; the owner's to set". None is dispatched. The two NaN split cards keep their parent's S1 and label-blocking. The DECIMAL-COMMON-TYPE-1 build's four cards are in [decimal-common-type-1-side/](../decimal-common-type-1-side/map.md). Five more cards, from the AVG-DECIMAL-ROUND-1 step 0 (#1352, `9c96a963`) and the owner's afternoon ruling of 2026-11-17 (about 14:05 UTC), are filed here as well; that ruling is the afternoon section of the same [rulings record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md).

## Cards

- [DECIMAL-DIVIDE-LITERAL-1](decimal-divide-literal-1-card-2026-11-17.md) (S1, label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 2):** a DECIMAL divided by an integer literal takes the wrong type, and the wrong digits when the quotient does not terminate.
- [TRY-ARITH-DECIMAL-INT-1](try-arith-decimal-int-1-card-2026-11-17.md) (S1, label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 6):** `try_add`, `try_subtract`, `try_multiply`, `try_divide` and `try_mod` cast a decimal to `INT` beside an integer.
- [AVG-DISTINCT-MIXED-1](avg-distinct-mixed-1-card-2026-11-17.md) (S3, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 4):** `avg(DISTINCT x)` beside another aggregate raises `DistinctAvgAccumulator`.
- [DECIMAL-PRECISION-LOSS-CONF-1](decimal-precision-loss-conf-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 5):** `allowPrecisionLoss=false` is accepted and ignored.
- [DECIMAL-MULTIPLY-OVERFLOW-1](decimal-multiply-overflow-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 8):** `d38_10 * 3` raises an Arrow overflow where Spark answers `decimal(38,8)`.
- [NATIVE-DECIMAL-AVG-DIVIDE-1](native-decimal-avg-divide-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 9):** the native door's `avg`, `mean` and decimal `/` truncate.
- [NAN-ARRAY-MINMAX-1](nan-array-minmax-1-card-2026-11-17.md) (S1, label-blocking, kept from NAN-MINMAX-1 part 2; not placed) — **card (2026-11-17, ruling 3):** `array_min` and `array_max` over a sign-set NaN.
- [NAN-QUANTILE-SIGNSET-1](nan-quantile-signset-1-card-2026-11-17.md) (S1, label-blocking, kept from NAN-MINMAX-1 part 2; not placed) — **card (2026-11-17, ruling 3):** `median` and `percentile_approx` over a sign-set NaN.
- [STRUCT-COLLECT-ROW-1](struct-collect-row-1-card-2026-11-17.md) (S2, not label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 5; rated by afternoon ruling 11):** `collect()` returns a struct as a `dict`, where PySpark returns a `Row`.
- [SUM-DEC-I128WRAP-1](sum-dec-i128wrap-1-card-2026-11-17.md) (not rated; the owner's to set) — **card (2026-11-17, evening, from Fable's consult reply §7, seen in passing; not in the counts above):** a decimal `sum` past 38 digits answers silently where Spark raises. The registry carries the id as BACKLOG (2026-09-10).

Up: [../map.md](../map.md).

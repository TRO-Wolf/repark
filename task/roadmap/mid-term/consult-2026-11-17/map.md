# map — task/roadmap/mid-term/consult-2026-11-17/

Four cards from Fable's consult of 2026-11-17 (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, sections 1 to 4) and the owner's ruling "Defaults." of the same date, recorded in [rulings-2026-11-17.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md). Each card's severity and label status are the ruling's, and a card with no ruled severity reads "not rated; the owner's to set". None is dispatched. The two NaN split cards keep their parent's S1 and label-blocking. The DECIMAL-COMMON-TYPE-1 build's four cards are in [decimal-common-type-1-side/](../decimal-common-type-1-side/map.md).

## Cards

- [DECIMAL-DIVIDE-LITERAL-1](decimal-divide-literal-1-card-2026-11-17.md) (S1, label-blocking, ruled 2026-11-17) — **card (2026-11-17, ruling 2):** a DECIMAL divided by an integer literal takes the wrong type, and the wrong digits when the quotient does not terminate.
- [NAN-ARRAY-MINMAX-1](nan-array-minmax-1-card-2026-11-17.md) (S1, label-blocking, kept from NAN-MINMAX-1 part 2; not placed) — **card (2026-11-17, ruling 3):** `array_min` and `array_max` over a sign-set NaN.
- [NAN-QUANTILE-SIGNSET-1](nan-quantile-signset-1-card-2026-11-17.md) (S1, label-blocking, kept from NAN-MINMAX-1 part 2; not placed) — **card (2026-11-17, ruling 3):** `median` and `percentile_approx` over a sign-set NaN.
- [STRUCT-COLLECT-ROW-1](struct-collect-row-1-card-2026-11-17.md) (not rated; the owner's to set) — **card (2026-11-17, ruling 5):** `collect()` returns a struct as a `dict`, where PySpark returns a `Row`.

Up: [../map.md](../map.md).

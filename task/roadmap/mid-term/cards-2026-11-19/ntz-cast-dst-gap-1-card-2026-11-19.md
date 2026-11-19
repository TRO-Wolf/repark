# Card NTZ-CAST-DST-GAP-1: a text cast to `TIMESTAMP_NTZ` moves a wall clock in a DST gap forward an hour (`02:30:00` answers `03:30:00`)

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 2, item 6, about 14:00 UTC). **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — reader-parse units, cost noise and the next queue 2026-11-19.md`), §6.1, its table, its reach paragraph and its first step; §5 item 4 (the card for F2's root).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S1 by class, label-blocking** (ruling 2, item 6, about 14:00 UTC: "the NTZ DST-gap cast card, S1 and label-blocking").

## Finding

**[M]** Confirmed on main `2900efde`, and it reaches Iceberg (Fable §6.1). The text is `2024-03-10 02:30:00`; Spark answers `02:30:00` in every cell.

| cell | Los Angeles | New York | UTC |
|---|---|---|---|
| `CAST('…' AS TIMESTAMP_NTZ)`, a literal | **03:30** | **03:30** | EQUAL |
| `CAST(c AS TIMESTAMP_NTZ)`, `c` a `STRING` column of an Iceberg v2 table | **03:30** | **03:30** | EQUAL |
| the same over Parquet, in SQL and through `selectExpr` | **03:30** | **03:30** | EQUAL |
| `to_timestamp_ntz(c)`; `hour(CAST(c AS TIMESTAMP_NTZ))` | **03:30**; **3** | the same | EQUAL |
| CTAS into an Iceberg table, then read back, under the zone and under UTC | **03:30** stored | **03:30** stored | — |
| the JSON reader with a `TIMESTAMP_NTZ` field | **03:30** | **03:30** | EQUAL |
| the CSV reader with a `TIMESTAMP_NTZ` field and no `TIMESTAMP` field | EQUAL | EQUAL | EQUAL |
| the typed literal `TIMESTAMP_NTZ '…'` | EQUAL | EQUAL | EQUAL |
| the overlap wall clock `2024-11-03 01:30:00`, every door above | EQUAL | EQUAL | EQUAL |

- **Silent.** The cast sits on a scalar column of a v2 table, and a write stores the moved wall clock.
- **Reach** (Fable's reading, for the owner's weighing). Only under a session zone with daylight saving, and only for a wall clock inside that zone's spring gap: one hour a year per zone. Data kept in UTC and cast under a Los Angeles session hits it for about 1 row in 8,760 if the rows are spread evenly.
- **Reading** (Fable, [R]). The fix should be small: this cast must not consult the session zone. Fable did not read the kernel.

## First step

Step 0, clerk grade (Fable §6.1): the gap hour of three zones across the doors above, plus `try_cast`, `F.col.cast`, and an `INSERT` of text into an NTZ column.

Up: [map.md](map.md).

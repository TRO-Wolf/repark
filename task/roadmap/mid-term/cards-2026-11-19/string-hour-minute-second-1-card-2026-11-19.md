# Card STRING-HOUR-MINUTE-SECOND-1: `hour`, `minute` and `second` over a `STRING` timestamp column answer 0 under matching settings, on an Iceberg v2 table

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** Fable's reply of 2026-11-19 (reader-parse step 0), §2 row Q6 and §5 item 2, kept outside the repository; the step 0 [domain table](../../../wo/temp-view-frame-reader-parse-1/domain-table.md), §7 row D4 and §8.1.

**Status:** built, PR #1370 (2026-11-19). Filed 2026-11-19, not dispatched at filing. Its step 0 first runs the ten expressions of `<scratchpad>/fable-rp/p4.py` on Spark. For the owner's morning read, as Fable's guidance.

**Severity: S1 by class, label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's Q6 ruling, changed from its default: its own card, S1 by class, label-blocking, not unit 1).

## Finding

Fable's Q6 row, [M]: "It is not a reader gap. On main, `hour(c)`, `minute(c)` and `second(c)` over a `STRING` column of an **Iceberg v2 table** answer `0` for `'2024-01-01 03:04:05'` and `'2024-06-01 23:30:00'`, under matching settings. `year`, `month`, `dayofmonth`, `date_format`, `to_date` and `unix_timestamp` over the same column are right, and `hour(CAST(c AS TIMESTAMP))` is right. So it is three functions, silent, on a scalar column the label covers. I did not run Spark for these ten expressions; Spark's answers `3`, `4`, `5` are the domain table's D4 reading."

- Those Spark answers are not re-run: they are the domain table's D4 reading.
- Domain table D4: "`hour()` over a timestamp **string** column answers `0` (`json_i`, `csv_ns`, `csv_0`, matching settings too)"; 20 matching and 20 cross-setting cells per reader; silent. The domain table files it as a reader gap.
- Fable §5 item 2, [M]: "D4 reaches Iceberg `STRING` columns (Q6). It was filed as a reader gap. It is a function gap on label-scope tables."

**Main:** `hour`, `minute` and `second` answer `0` over an Iceberg v2 `STRING` column under matching settings. The six other functions in Fable's measurement (`year`, `month`, `dayofmonth`, `date_format`, `to_date`, `unix_timestamp`), and `hour(CAST(c AS TIMESTAMP))`, are right.

**Dated 2026-11-19: built, and the neighbours.** The card is built as PR #1370, on the step 0 merged as #1369 (`81ae984e`). The seat A verifier's verdict, held outside the repository, was FAIL on one class (a) finding, F1: the N8 family, which loses EQUAL cells beyond the three the owner accepted at about 15:47 UTC. The owner accepted the whole family at about 17:40 UTC ([rulings-2026-11-19.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-19.md), sections 4 and 5). The family's card is [FEXPR-STRING-LITERAL-ZONE-1](fexpr-string-literal-zone-1-card-2026-11-19.md).

The eleven neighbour cards, one per row N1 to N11 of the domain table, are filed in this folder: [STRING-DATE-FIELD-OFFSET-1](string-date-field-offset-1-card-2026-11-19.md) (N1), [STRING-DATE-FIELD-RAISE-1](string-date-field-raise-1-card-2026-11-19.md) (N2), [STRING-DATE-TRUNC-FORMAT-1](string-date-trunc-format-1-card-2026-11-19.md) (N3), [EXTRACT-SECOND-DECIMAL-1](extract-second-decimal-1-card-2026-11-19.md) (N4), [SQL-DATE-PART-STRING-1](sql-date-part-string-1-card-2026-11-19.md) (N5), [EXTRACT-FIELD-ALIASES-1](extract-field-aliases-1-card-2026-11-19.md) (N6), [EXTRACT-HOUR-DST-GAP-1](extract-hour-dst-gap-1-card-2026-11-19.md) (N7), [FEXPR-STRING-LITERAL-ZONE-1](fexpr-string-literal-zone-1-card-2026-11-19.md) (N8), [UNIX-TIMESTAMP-STRING-TRIM-1](unix-timestamp-string-trim-1-card-2026-11-19.md) (N9), [DATE-FORMAT-MICROS-1](date-format-micros-1-card-2026-11-19.md) (N10) and [NTZ-CAST-OFFSET-TEXT-1](ntz-cast-offset-text-1-card-2026-11-19.md) (N11).

Up: [map.md](map.md).

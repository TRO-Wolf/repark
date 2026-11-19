# Card STRING-HOUR-MINUTE-SECOND-1: `hour`, `minute` and `second` over a `STRING` timestamp column answer 0 under matching settings, on an Iceberg v2 table

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** Fable's reply of 2026-11-19 (reader-parse step 0), §2 row Q6 and §5 item 2, kept outside the repository; the step 0 [domain table](../../../wo/temp-view-frame-reader-parse-1/domain-table.md), §7 row D4 and §8.1.

**Status:** filed 2026-11-19, not dispatched. Its step 0 first runs the ten expressions of `<scratchpad>/fable-rp/p4.py` on Spark. For the owner's morning read, as Fable's guidance.

**Severity: S1 by class, label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's Q6 ruling, changed from its default: its own card, S1 by class, label-blocking, not unit 1).

## Finding

Fable's Q6 row, [M]: "It is not a reader gap. On main, `hour(c)`, `minute(c)` and `second(c)` over a `STRING` column of an **Iceberg v2 table** answer `0` for `'2024-01-01 03:04:05'` and `'2024-06-01 23:30:00'`, under matching settings. `year`, `month`, `dayofmonth`, `date_format`, `to_date` and `unix_timestamp` over the same column are right, and `hour(CAST(c AS TIMESTAMP))` is right. So it is three functions, silent, on a scalar column the label covers. I did not run Spark for these ten expressions; Spark's answers `3`, `4`, `5` are the domain table's D4 reading."

- Those Spark answers are not re-run: they are the domain table's D4 reading.
- Domain table D4: "`hour()` over a timestamp **string** column answers `0` (`json_i`, `csv_ns`, `csv_0`, matching settings too)"; 20 matching and 20 cross-setting cells per reader; silent. The domain table files it as a reader gap.
- Fable §5 item 2, [M]: "D4 reaches Iceberg `STRING` columns (Q6). It was filed as a reader gap. It is a function gap on label-scope tables."

**Main:** `hour`, `minute` and `second` answer `0` over an Iceberg v2 `STRING` column under matching settings. The six other functions in Fable's measurement (`year`, `month`, `dayofmonth`, `date_format`, `to_date`, `unix_timestamp`), and `hour(CAST(c AS TIMESTAMP))`, are right.

- **2026-11-19 — an escape, closed by TEMP-VIEW-FRAME-READER-PARSE-1.** Unit 1's rebuild measured 7 ANSI cells (`A/true_false/{csv_i,json_i}/{eb,ob,osb,sb}_hour`: a view made under ANSI on and read under ANSI off) EQUAL before #1370 and WRONG after it on main: `hour` of the bad text answers under the reading ANSI off, where Spark raises `CAST_INVALID_INPUT`. Unit 1 restores them, because the frame is decided under the creating ANSI. It belongs in the escapes backfill of `task/metrics.md` (owner ruling 2026-11-19).

Up: [map.md](map.md).

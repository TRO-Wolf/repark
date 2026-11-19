# Card CSV-SCHEMA-TIMESTAMP-ZONE-1: a CSV read with a schema parses a `TIMESTAMP` field as UTC, whatever the session zone

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** Fable's reply of 2026-11-19 (reader-parse step 0), §2 row Q2 and §4 step 1, kept outside the repository; the step 0 [domain table](../../../wo/temp-view-frame-reader-parse-1/domain-table.md), §7 row D1; and the step 0 [sketch](../../../wo/temp-view-frame-reader-parse-1/sketch.md), §6 (unit 0).

**Status:** building, on branch `fix/csv-schema-timestamp-zone-1` (another agent's lane); filed 2026-11-19. This is unit 0 of TEMP-VIEW-FRAME-READER-PARSE-1, its own PR, ahead of unit 1.

**Severity: S1, label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's Q2 ruling (i): an own card, S1, label-blocking, unit 0).

## Finding

`schema(...).csv` and `format("csv").schema(...)`: a `TIMESTAMP` field is parsed as UTC whatever the session zone. DataFusion infers a naive timestamp and the cast reads it as UTC, so every read under a non-UTC zone is wrong, with or without a view, and is written wrong. Silent.

- Fable's Q2 row, [M]: "Silent, 64 of 128 matching cells, written wrong. It moves matching cells, so it cannot share unit 1's bar."
- Domain table D1: 106 + 106 zone cells, 27 type cells, 8 writes; silent.
- Domain table §8.1, base data: matching cells EQUAL, `csv_s`, `csv_f` 64 (D1).
- Unit 0's bar (sketch §6): `csv_s` and `csv_f` matching 128 of 128 each, as `csv_fix`; cross as `csv_fix`'s 54 / 74; the 8 `csv_s` `d` writes EQUAL; no other cell moves; `EXPLAIN` changes only for CSV reads with a `TIMESTAMP` field (named).

**Main:** wrong under any non-UTC zone, with or without a view, and written wrong (the domain table's D1: 106 + 106 zone cells, 27 type cells, 8 writes). Silent.

Up: [map.md](map.md).

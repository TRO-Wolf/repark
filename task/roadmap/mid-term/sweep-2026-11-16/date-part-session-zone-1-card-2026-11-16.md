# Card DATE-PART-SESSION-ZONE-1: date_part and extract read a timestamp's calendar fields in UTC, not the session zone

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (difference 9). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S1 (owner to rule). A silent wrong calendar field on an ordinary `extract` in a session that is not UTC.

## Finding

In a session whose zone is `America/Los_Angeles` (the audit's probe set it on purpose), `date_part('year', ts)`, `extract(YEAR FROM ts)` and `date_part('hour', ts)` read the timestamp's fields in UTC on main. `year(ts)` and `hour(ts)` read the session zone and are right. The defect is in the two spellings of the same functions, in both ANSI modes. `date_trunc` agrees with Spark as text.

## Measured

Both ANSI modes give the same main output. The seed rows are 2024-01-01 02:00 UTC and 2025-01-01 07:00 UTC.

| row | Spark 4.1.2: `year(ts)`, `date_part` year, `extract` year, `hour(ts)`, `date_part` hour | main, same order |
|---|---|---|
| 0 (2023-12-31 18:00 in the session zone) | 2023, 2023, 2023, 18, 18 | 2023, 2024, 2024, 18, 2 |
| 1 (2024-12-31 23:00 in the session zone) | 2024, 2024, 2024, 23, 23 | 2024, 2025, 2025, 23, 7 |

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark false`, and the same with `spark`; the case `zone_parts`.

## Not yet known

The seat. Related and not the same: TRANSITION-DAY-ARITHMETIC-1 names this defect in its residue R-2 (`extract` and `date_part` read the UTC label) and does not card it. FN-DATE and FN-PREIMAGE-YEAR are the audit inventory's rules for `date_part`; the year preimage is a separate seat. FEXPR-TIMESTAMP-ZONE-1 is a different defect (F.expr timestamp literals).

Up: [map.md](map.md).

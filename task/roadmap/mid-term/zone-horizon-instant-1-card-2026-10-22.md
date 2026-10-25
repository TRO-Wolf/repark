# Card ZONE-HORIZON-INSTANT-1: `to_utc_timestamp` and `from_utc_timestamp` of an instant miss Spark at DST edges and after 2099

**Date:** 2026-10-22. **Filed by:** Claude (claude-opus-5-5), the ZONE-SEAM-1 lane, from the
measurement it made while parked on its second halt; owner ruling ZS-7 of the same day.

**Status:** open. Scheduled as the next Opus unit after GATE-RETIRE-1.

**2026-10-25:** GATE-RETIRE-1 concluded (PR #1187) with the R-011 arm retained and the micros hint corrected, not retired (GR-9).

**Retires:** when `to_utc_timestamp(<instant>, <zone>)` and `from_utc_timestamp(<instant>,
<zone>)` answer Spark 4.1.2 at every gap and overlap of the measured zones and years, and the
ZONE-HORIZON-RENDER-1 residue cells of these two functions (below) leave the residue list.

## What is wrong

The kernel is DataFusion-Spark's `to_utc_timestamp` / `from_utc_timestamp` of an instant, which
RePark registers as is and, since ZONE-SEAM-1, also calls from `SparkUtcShift` after the
implicit cast of a wall, a date or a NULL argument. On main and on ZONE-SEAM-1's head the kernel
answers the same in every measured cell. Two causes fit its misses; neither was isolated by a
direct probe:

- every `from_utc_timestamp` miss is in 2100 or 2104, which fits a zone read past chrono-tz's
  2099 table end (RePark reads `repark_common::zone_horizon` there everywhere else);
- `to_utc_timestamp` also misses in 2099 at gaps and overlaps in all three zones, which fits a
  local-wall placement rule other than Java's `atZone` (earlier offset in an overlap, shift
  forward in a gap).

This card names the kernel only; it picks no implementation.

## The edge measurement (2026-10-22)

Spark 4.1.2 `local[1]` (ANSI on), ZONE-SEAM-1's head `dc0da5a2` and main `b7a20167`. Every edge
wall the ZONE-HORIZON-RENDER-1 oracle holds for New York (NY), Sydney (SYD) and Lord Howe (LHI)
in 2099, 2100 and 2104 (a gap and an overlap each), with the session zone set to that zone
(`zone`) and to `UTC`; values are `CAST(<expr> AS STRING)` with `typeof`; `=` is equal to Spark
in value and type. (a) `CAST(<wall> AS TIMESTAMP)`, (b) `to_utc_timestamp(<wall>, <zone>)`,
(c) `to_utc_timestamp(CAST(<wall> AS TIMESTAMP), <zone>)`, (d) `from_utc_timestamp(<wall>,
<zone>)`, (e) `from_utc_timestamp(CAST(<wall> AS TIMESTAMP), <zone>)`.

| expr (36 cells each) | head = Spark | main = Spark |
|---|---:|---:|
| (a) | 36 | 36 |
| (b) | 17 | 0 (`timestamp_ntz`) |
| (c) | 17 | 17 |
| (d) | 26 | 0 (`timestamp_ntz`) |
| (e) | 26 | 26 |

Spark's (b) equals its (c), and (d) its (e), in all 36 cells: Spark's wall argument is the leaf
cast composed with the instant function. Head's (b) equals head's (c) and (d) head's (e) in all 36;
head's (c) and (e) equal main's cell for cell. So every miss is the instant kernel's.

| zone · year · edge (wall) | session | expr | Spark | head | main |
|---|---|---|---|---|---|
| NY 2099 gap `02:30` | zone | a | 2099-03-08 03:30:00 | = | = |
| NY 2099 gap `02:30` | zone | b | 2099-03-08 07:30:00 | = | 2099-03-08 07:30:00 (timestamp_ntz) |
| NY 2099 gap `02:30` | zone | c | 2099-03-08 07:30:00 | = | = |
| NY 2099 gap `02:30` | zone | d | 2099-03-07 22:30:00 | = | 2099-03-07 21:30:00 (timestamp_ntz) |
| NY 2099 gap `02:30` | zone | e | 2099-03-07 22:30:00 | = | = |
| NY 2099 gap `02:30` | UTC | a | 2099-03-08 02:30:00 | = | = |
| NY 2099 gap `02:30` | UTC | b | 2099-03-08 07:30:00 | = | 2099-03-08 07:30:00 (timestamp_ntz) |
| NY 2099 gap `02:30` | UTC | c | 2099-03-08 07:30:00 | = | = |
| NY 2099 gap `02:30` | UTC | d | 2099-03-07 21:30:00 | = | 2099-03-07 21:30:00 (timestamp_ntz) |
| NY 2099 gap `02:30` | UTC | e | 2099-03-07 21:30:00 | = | = |
| NY 2099 overlap `01:30` | zone | a | 2099-11-01 01:30:00 | = | = |
| NY 2099 overlap `01:30` | zone | b | 2099-11-01 05:30:00 | 2099-11-01 04:30:00 | 2099-11-01 05:30:00 (timestamp_ntz) |
| NY 2099 overlap `01:30` | zone | c | 2099-11-01 05:30:00 | 2099-11-01 04:30:00 | 2099-11-01 04:30:00 |
| NY 2099 overlap `01:30` | zone | d | 2099-10-31 21:30:00 | = | 2099-10-31 21:30:00 (timestamp_ntz) |
| NY 2099 overlap `01:30` | zone | e | 2099-10-31 21:30:00 | = | = |
| NY 2099 overlap `01:30` | UTC | a | 2099-11-01 01:30:00 | = | = |
| NY 2099 overlap `01:30` | UTC | b | 2099-11-01 05:30:00 | = | 2099-11-01 05:30:00 (timestamp_ntz) |
| NY 2099 overlap `01:30` | UTC | c | 2099-11-01 05:30:00 | = | = |
| NY 2099 overlap `01:30` | UTC | d | 2099-10-31 21:30:00 | = | 2099-10-31 21:30:00 (timestamp_ntz) |
| NY 2099 overlap `01:30` | UTC | e | 2099-10-31 21:30:00 | = | = |
| NY 2100 gap `02:30` | zone | a | 2100-03-14 03:30:00 | = | = |
| NY 2100 gap `02:30` | zone | b | 2100-03-14 07:30:00 | 2100-03-14 08:30:00 | 2100-03-14 07:30:00 (timestamp_ntz) |
| NY 2100 gap `02:30` | zone | c | 2100-03-14 07:30:00 | 2100-03-14 08:30:00 | 2100-03-14 08:30:00 |
| NY 2100 gap `02:30` | zone | d | 2100-03-13 22:30:00 | 2100-03-13 21:30:00 | 2100-03-13 21:30:00 (timestamp_ntz) |
| NY 2100 gap `02:30` | zone | e | 2100-03-13 22:30:00 | 2100-03-13 21:30:00 | 2100-03-13 21:30:00 |
| NY 2100 gap `02:30` | UTC | a | 2100-03-14 02:30:00 | = | = |
| NY 2100 gap `02:30` | UTC | b | 2100-03-14 07:30:00 | = | 2100-03-14 07:30:00 (timestamp_ntz) |
| NY 2100 gap `02:30` | UTC | c | 2100-03-14 07:30:00 | = | = |
| NY 2100 gap `02:30` | UTC | d | 2100-03-13 21:30:00 | = | 2100-03-13 21:30:00 (timestamp_ntz) |
| NY 2100 gap `02:30` | UTC | e | 2100-03-13 21:30:00 | = | = |
| NY 2100 overlap `01:30` | zone | a | 2100-11-07 01:30:00 | = | = |
| NY 2100 overlap `01:30` | zone | b | 2100-11-07 05:30:00 | = | 2100-11-07 06:30:00 (timestamp_ntz) |
| NY 2100 overlap `01:30` | zone | c | 2100-11-07 05:30:00 | = | = |
| NY 2100 overlap `01:30` | zone | d | 2100-11-06 21:30:00 | 2100-11-06 20:30:00 | 2100-11-06 20:30:00 (timestamp_ntz) |
| NY 2100 overlap `01:30` | zone | e | 2100-11-06 21:30:00 | 2100-11-06 20:30:00 | 2100-11-06 20:30:00 |
| NY 2100 overlap `01:30` | UTC | a | 2100-11-07 01:30:00 | = | = |
| NY 2100 overlap `01:30` | UTC | b | 2100-11-07 05:30:00 | 2100-11-07 06:30:00 | 2100-11-07 06:30:00 (timestamp_ntz) |
| NY 2100 overlap `01:30` | UTC | c | 2100-11-07 05:30:00 | 2100-11-07 06:30:00 | 2100-11-07 06:30:00 |
| NY 2100 overlap `01:30` | UTC | d | 2100-11-06 21:30:00 | 2100-11-06 20:30:00 | 2100-11-06 20:30:00 (timestamp_ntz) |
| NY 2100 overlap `01:30` | UTC | e | 2100-11-06 21:30:00 | 2100-11-06 20:30:00 | 2100-11-06 20:30:00 |
| NY 2104 gap `02:30` | zone | a | 2104-03-09 03:30:00 | = | = |
| NY 2104 gap `02:30` | zone | b | 2104-03-09 07:30:00 | 2104-03-09 08:30:00 | 2104-03-09 07:30:00 (timestamp_ntz) |
| NY 2104 gap `02:30` | zone | c | 2104-03-09 07:30:00 | 2104-03-09 08:30:00 | 2104-03-09 08:30:00 |
| NY 2104 gap `02:30` | zone | d | 2104-03-08 22:30:00 | 2104-03-08 21:30:00 | 2104-03-08 21:30:00 (timestamp_ntz) |
| NY 2104 gap `02:30` | zone | e | 2104-03-08 22:30:00 | 2104-03-08 21:30:00 | 2104-03-08 21:30:00 |
| NY 2104 gap `02:30` | UTC | a | 2104-03-09 02:30:00 | = | = |
| NY 2104 gap `02:30` | UTC | b | 2104-03-09 07:30:00 | = | 2104-03-09 07:30:00 (timestamp_ntz) |
| NY 2104 gap `02:30` | UTC | c | 2104-03-09 07:30:00 | = | = |
| NY 2104 gap `02:30` | UTC | d | 2104-03-08 21:30:00 | = | 2104-03-08 21:30:00 (timestamp_ntz) |
| NY 2104 gap `02:30` | UTC | e | 2104-03-08 21:30:00 | = | = |
| NY 2104 overlap `01:30` | zone | a | 2104-11-02 01:30:00 | = | = |
| NY 2104 overlap `01:30` | zone | b | 2104-11-02 05:30:00 | = | 2104-11-02 06:30:00 (timestamp_ntz) |
| NY 2104 overlap `01:30` | zone | c | 2104-11-02 05:30:00 | = | = |
| NY 2104 overlap `01:30` | zone | d | 2104-11-01 21:30:00 | 2104-11-01 20:30:00 | 2104-11-01 20:30:00 (timestamp_ntz) |
| NY 2104 overlap `01:30` | zone | e | 2104-11-01 21:30:00 | 2104-11-01 20:30:00 | 2104-11-01 20:30:00 |
| NY 2104 overlap `01:30` | UTC | a | 2104-11-02 01:30:00 | = | = |
| NY 2104 overlap `01:30` | UTC | b | 2104-11-02 05:30:00 | 2104-11-02 06:30:00 | 2104-11-02 06:30:00 (timestamp_ntz) |
| NY 2104 overlap `01:30` | UTC | c | 2104-11-02 05:30:00 | 2104-11-02 06:30:00 | 2104-11-02 06:30:00 |
| NY 2104 overlap `01:30` | UTC | d | 2104-11-01 21:30:00 | 2104-11-01 20:30:00 | 2104-11-01 20:30:00 (timestamp_ntz) |
| NY 2104 overlap `01:30` | UTC | e | 2104-11-01 21:30:00 | 2104-11-01 20:30:00 | 2104-11-01 20:30:00 |
| LHI 2099 gap `02:15` | zone | a | 2099-10-04 02:45:00 | = | = |
| LHI 2099 gap `02:15` | zone | b | 2099-10-03 15:45:00 | 2099-10-03 15:15:00 | 2099-10-03 15:15:00 (timestamp_ntz) |
| LHI 2099 gap `02:15` | zone | c | 2099-10-03 15:45:00 | 2099-10-03 15:15:00 | 2099-10-03 15:15:00 |
| LHI 2099 gap `02:15` | zone | d | 2099-10-04 13:45:00 | = | 2099-10-04 13:15:00 (timestamp_ntz) |
| LHI 2099 gap `02:15` | zone | e | 2099-10-04 13:45:00 | = | = |
| LHI 2099 gap `02:15` | UTC | a | 2099-10-04 02:15:00 | = | = |
| LHI 2099 gap `02:15` | UTC | b | 2099-10-03 15:45:00 | 2099-10-03 15:15:00 | 2099-10-03 15:15:00 (timestamp_ntz) |
| LHI 2099 gap `02:15` | UTC | c | 2099-10-03 15:45:00 | 2099-10-03 15:15:00 | 2099-10-03 15:15:00 |
| LHI 2099 gap `02:15` | UTC | d | 2099-10-04 13:15:00 | = | 2099-10-04 13:15:00 (timestamp_ntz) |
| LHI 2099 gap `02:15` | UTC | e | 2099-10-04 13:15:00 | = | = |
| LHI 2099 overlap `01:45` | zone | a | 2099-04-05 01:45:00 | = | = |
| LHI 2099 overlap `01:45` | zone | b | 2099-04-04 14:45:00 | = | 2099-04-04 15:15:00 (timestamp_ntz) |
| LHI 2099 overlap `01:45` | zone | c | 2099-04-04 14:45:00 | = | = |
| LHI 2099 overlap `01:45` | zone | d | 2099-04-05 12:15:00 | = | 2099-04-05 12:15:00 (timestamp_ntz) |
| LHI 2099 overlap `01:45` | zone | e | 2099-04-05 12:15:00 | = | = |
| LHI 2099 overlap `01:45` | UTC | a | 2099-04-05 01:45:00 | = | = |
| LHI 2099 overlap `01:45` | UTC | b | 2099-04-04 14:45:00 | 2099-04-04 15:15:00 | 2099-04-04 15:15:00 (timestamp_ntz) |
| LHI 2099 overlap `01:45` | UTC | c | 2099-04-04 14:45:00 | 2099-04-04 15:15:00 | 2099-04-04 15:15:00 |
| LHI 2099 overlap `01:45` | UTC | d | 2099-04-05 12:15:00 | = | 2099-04-05 12:15:00 (timestamp_ntz) |
| LHI 2099 overlap `01:45` | UTC | e | 2099-04-05 12:15:00 | = | = |
| LHI 2100 gap `02:15` | zone | a | 2100-10-03 02:45:00 | = | = |
| LHI 2100 gap `02:15` | zone | b | 2100-10-02 15:45:00 | 2100-10-02 15:15:00 | 2100-10-02 15:15:00 (timestamp_ntz) |
| LHI 2100 gap `02:15` | zone | c | 2100-10-02 15:45:00 | 2100-10-02 15:15:00 | 2100-10-02 15:15:00 |
| LHI 2100 gap `02:15` | zone | d | 2100-10-03 13:45:00 | = | 2100-10-03 13:15:00 (timestamp_ntz) |
| LHI 2100 gap `02:15` | zone | e | 2100-10-03 13:45:00 | = | = |
| LHI 2100 gap `02:15` | UTC | a | 2100-10-03 02:15:00 | = | = |
| LHI 2100 gap `02:15` | UTC | b | 2100-10-02 15:45:00 | 2100-10-02 15:15:00 | 2100-10-02 15:15:00 (timestamp_ntz) |
| LHI 2100 gap `02:15` | UTC | c | 2100-10-02 15:45:00 | 2100-10-02 15:15:00 | 2100-10-02 15:15:00 |
| LHI 2100 gap `02:15` | UTC | d | 2100-10-03 13:15:00 | = | 2100-10-03 13:15:00 (timestamp_ntz) |
| LHI 2100 gap `02:15` | UTC | e | 2100-10-03 13:15:00 | = | = |
| LHI 2100 overlap `01:45` | zone | a | 2100-04-04 01:45:00 | = | = |
| LHI 2100 overlap `01:45` | zone | b | 2100-04-03 14:45:00 | = | 2100-04-03 14:45:00 (timestamp_ntz) |
| LHI 2100 overlap `01:45` | zone | c | 2100-04-03 14:45:00 | = | = |
| LHI 2100 overlap `01:45` | zone | d | 2100-04-04 12:15:00 | = | 2100-04-04 12:45:00 (timestamp_ntz) |
| LHI 2100 overlap `01:45` | zone | e | 2100-04-04 12:15:00 | = | = |
| LHI 2100 overlap `01:45` | UTC | a | 2100-04-04 01:45:00 | = | = |
| LHI 2100 overlap `01:45` | UTC | b | 2100-04-03 14:45:00 | = | 2100-04-03 14:45:00 (timestamp_ntz) |
| LHI 2100 overlap `01:45` | UTC | c | 2100-04-03 14:45:00 | = | = |
| LHI 2100 overlap `01:45` | UTC | d | 2100-04-04 12:15:00 | 2100-04-04 12:45:00 | 2100-04-04 12:45:00 (timestamp_ntz) |
| LHI 2100 overlap `01:45` | UTC | e | 2100-04-04 12:15:00 | 2100-04-04 12:45:00 | 2100-04-04 12:45:00 |
| LHI 2104 gap `02:15` | zone | a | 2104-10-05 02:45:00 | = | = |
| LHI 2104 gap `02:15` | zone | b | 2104-10-04 15:45:00 | 2104-10-04 15:15:00 | 2104-10-04 15:15:00 (timestamp_ntz) |
| LHI 2104 gap `02:15` | zone | c | 2104-10-04 15:45:00 | 2104-10-04 15:15:00 | 2104-10-04 15:15:00 |
| LHI 2104 gap `02:15` | zone | d | 2104-10-05 13:45:00 | = | 2104-10-05 13:15:00 (timestamp_ntz) |
| LHI 2104 gap `02:15` | zone | e | 2104-10-05 13:45:00 | = | = |
| LHI 2104 gap `02:15` | UTC | a | 2104-10-05 02:15:00 | = | = |
| LHI 2104 gap `02:15` | UTC | b | 2104-10-04 15:45:00 | 2104-10-04 15:15:00 | 2104-10-04 15:15:00 (timestamp_ntz) |
| LHI 2104 gap `02:15` | UTC | c | 2104-10-04 15:45:00 | 2104-10-04 15:15:00 | 2104-10-04 15:15:00 |
| LHI 2104 gap `02:15` | UTC | d | 2104-10-05 13:15:00 | = | 2104-10-05 13:15:00 (timestamp_ntz) |
| LHI 2104 gap `02:15` | UTC | e | 2104-10-05 13:15:00 | = | = |
| LHI 2104 overlap `01:45` | zone | a | 2104-04-06 01:45:00 | = | = |
| LHI 2104 overlap `01:45` | zone | b | 2104-04-05 14:45:00 | = | 2104-04-05 14:45:00 (timestamp_ntz) |
| LHI 2104 overlap `01:45` | zone | c | 2104-04-05 14:45:00 | = | = |
| LHI 2104 overlap `01:45` | zone | d | 2104-04-06 12:15:00 | = | 2104-04-06 12:45:00 (timestamp_ntz) |
| LHI 2104 overlap `01:45` | zone | e | 2104-04-06 12:15:00 | = | = |
| LHI 2104 overlap `01:45` | UTC | a | 2104-04-06 01:45:00 | = | = |
| LHI 2104 overlap `01:45` | UTC | b | 2104-04-05 14:45:00 | = | 2104-04-05 14:45:00 (timestamp_ntz) |
| LHI 2104 overlap `01:45` | UTC | c | 2104-04-05 14:45:00 | = | = |
| LHI 2104 overlap `01:45` | UTC | d | 2104-04-06 12:15:00 | 2104-04-06 12:45:00 | 2104-04-06 12:45:00 (timestamp_ntz) |
| LHI 2104 overlap `01:45` | UTC | e | 2104-04-06 12:15:00 | 2104-04-06 12:45:00 | 2104-04-06 12:45:00 |
| SYD 2099 gap `02:30` | zone | a | 2099-10-04 03:30:00 | = | = |
| SYD 2099 gap `02:30` | zone | b | 2099-10-03 16:30:00 | 2099-10-03 15:30:00 | 2099-10-03 15:30:00 (timestamp_ntz) |
| SYD 2099 gap `02:30` | zone | c | 2099-10-03 16:30:00 | 2099-10-03 15:30:00 | 2099-10-03 15:30:00 |
| SYD 2099 gap `02:30` | zone | d | 2099-10-04 14:30:00 | = | 2099-10-04 13:30:00 (timestamp_ntz) |
| SYD 2099 gap `02:30` | zone | e | 2099-10-04 14:30:00 | = | = |
| SYD 2099 gap `02:30` | UTC | a | 2099-10-04 02:30:00 | = | = |
| SYD 2099 gap `02:30` | UTC | b | 2099-10-03 16:30:00 | 2099-10-03 15:30:00 | 2099-10-03 15:30:00 (timestamp_ntz) |
| SYD 2099 gap `02:30` | UTC | c | 2099-10-03 16:30:00 | 2099-10-03 15:30:00 | 2099-10-03 15:30:00 |
| SYD 2099 gap `02:30` | UTC | d | 2099-10-04 13:30:00 | = | 2099-10-04 13:30:00 (timestamp_ntz) |
| SYD 2099 gap `02:30` | UTC | e | 2099-10-04 13:30:00 | = | = |
| SYD 2099 overlap `02:30` | zone | a | 2099-04-05 02:30:00 | = | = |
| SYD 2099 overlap `02:30` | zone | b | 2099-04-04 15:30:00 | = | 2099-04-04 16:30:00 (timestamp_ntz) |
| SYD 2099 overlap `02:30` | zone | c | 2099-04-04 15:30:00 | = | = |
| SYD 2099 overlap `02:30` | zone | d | 2099-04-05 12:30:00 | = | 2099-04-05 12:30:00 (timestamp_ntz) |
| SYD 2099 overlap `02:30` | zone | e | 2099-04-05 12:30:00 | = | = |
| SYD 2099 overlap `02:30` | UTC | a | 2099-04-05 02:30:00 | = | = |
| SYD 2099 overlap `02:30` | UTC | b | 2099-04-04 15:30:00 | 2099-04-04 16:30:00 | 2099-04-04 16:30:00 (timestamp_ntz) |
| SYD 2099 overlap `02:30` | UTC | c | 2099-04-04 15:30:00 | 2099-04-04 16:30:00 | 2099-04-04 16:30:00 |
| SYD 2099 overlap `02:30` | UTC | d | 2099-04-05 12:30:00 | = | 2099-04-05 12:30:00 (timestamp_ntz) |
| SYD 2099 overlap `02:30` | UTC | e | 2099-04-05 12:30:00 | = | = |
| SYD 2100 gap `02:30` | zone | a | 2100-10-03 03:30:00 | = | = |
| SYD 2100 gap `02:30` | zone | b | 2100-10-02 16:30:00 | 2100-10-02 15:30:00 | 2100-10-02 15:30:00 (timestamp_ntz) |
| SYD 2100 gap `02:30` | zone | c | 2100-10-02 16:30:00 | 2100-10-02 15:30:00 | 2100-10-02 15:30:00 |
| SYD 2100 gap `02:30` | zone | d | 2100-10-03 14:30:00 | = | 2100-10-03 13:30:00 (timestamp_ntz) |
| SYD 2100 gap `02:30` | zone | e | 2100-10-03 14:30:00 | = | = |
| SYD 2100 gap `02:30` | UTC | a | 2100-10-03 02:30:00 | = | = |
| SYD 2100 gap `02:30` | UTC | b | 2100-10-02 16:30:00 | 2100-10-02 15:30:00 | 2100-10-02 15:30:00 (timestamp_ntz) |
| SYD 2100 gap `02:30` | UTC | c | 2100-10-02 16:30:00 | 2100-10-02 15:30:00 | 2100-10-02 15:30:00 |
| SYD 2100 gap `02:30` | UTC | d | 2100-10-03 13:30:00 | = | 2100-10-03 13:30:00 (timestamp_ntz) |
| SYD 2100 gap `02:30` | UTC | e | 2100-10-03 13:30:00 | = | = |
| SYD 2100 overlap `02:30` | zone | a | 2100-04-04 02:30:00 | = | = |
| SYD 2100 overlap `02:30` | zone | b | 2100-04-03 15:30:00 | = | 2100-04-03 15:30:00 (timestamp_ntz) |
| SYD 2100 overlap `02:30` | zone | c | 2100-04-03 15:30:00 | = | = |
| SYD 2100 overlap `02:30` | zone | d | 2100-04-04 12:30:00 | = | 2100-04-04 13:30:00 (timestamp_ntz) |
| SYD 2100 overlap `02:30` | zone | e | 2100-04-04 12:30:00 | = | = |
| SYD 2100 overlap `02:30` | UTC | a | 2100-04-04 02:30:00 | = | = |
| SYD 2100 overlap `02:30` | UTC | b | 2100-04-03 15:30:00 | = | 2100-04-03 15:30:00 (timestamp_ntz) |
| SYD 2100 overlap `02:30` | UTC | c | 2100-04-03 15:30:00 | = | = |
| SYD 2100 overlap `02:30` | UTC | d | 2100-04-04 12:30:00 | 2100-04-04 13:30:00 | 2100-04-04 13:30:00 (timestamp_ntz) |
| SYD 2100 overlap `02:30` | UTC | e | 2100-04-04 12:30:00 | 2100-04-04 13:30:00 | 2100-04-04 13:30:00 |
| SYD 2104 gap `02:30` | zone | a | 2104-10-05 03:30:00 | = | = |
| SYD 2104 gap `02:30` | zone | b | 2104-10-04 16:30:00 | 2104-10-04 15:30:00 | 2104-10-04 15:30:00 (timestamp_ntz) |
| SYD 2104 gap `02:30` | zone | c | 2104-10-04 16:30:00 | 2104-10-04 15:30:00 | 2104-10-04 15:30:00 |
| SYD 2104 gap `02:30` | zone | d | 2104-10-05 14:30:00 | = | 2104-10-05 13:30:00 (timestamp_ntz) |
| SYD 2104 gap `02:30` | zone | e | 2104-10-05 14:30:00 | = | = |
| SYD 2104 gap `02:30` | UTC | a | 2104-10-05 02:30:00 | = | = |
| SYD 2104 gap `02:30` | UTC | b | 2104-10-04 16:30:00 | 2104-10-04 15:30:00 | 2104-10-04 15:30:00 (timestamp_ntz) |
| SYD 2104 gap `02:30` | UTC | c | 2104-10-04 16:30:00 | 2104-10-04 15:30:00 | 2104-10-04 15:30:00 |
| SYD 2104 gap `02:30` | UTC | d | 2104-10-05 13:30:00 | = | 2104-10-05 13:30:00 (timestamp_ntz) |
| SYD 2104 gap `02:30` | UTC | e | 2104-10-05 13:30:00 | = | = |
| SYD 2104 overlap `02:30` | zone | a | 2104-04-06 02:30:00 | = | = |
| SYD 2104 overlap `02:30` | zone | b | 2104-04-05 15:30:00 | = | 2104-04-05 15:30:00 (timestamp_ntz) |
| SYD 2104 overlap `02:30` | zone | c | 2104-04-05 15:30:00 | = | = |
| SYD 2104 overlap `02:30` | zone | d | 2104-04-06 12:30:00 | = | 2104-04-06 13:30:00 (timestamp_ntz) |
| SYD 2104 overlap `02:30` | zone | e | 2104-04-06 12:30:00 | = | = |
| SYD 2104 overlap `02:30` | UTC | a | 2104-04-06 02:30:00 | = | = |
| SYD 2104 overlap `02:30` | UTC | b | 2104-04-05 15:30:00 | = | 2104-04-05 15:30:00 (timestamp_ntz) |
| SYD 2104 overlap `02:30` | UTC | c | 2104-04-05 15:30:00 | = | = |
| SYD 2104 overlap `02:30` | UTC | d | 2104-04-06 12:30:00 | 2104-04-06 13:30:00 | 2104-04-06 13:30:00 (timestamp_ntz) |
| SYD 2104 overlap `02:30` | UTC | e | 2104-04-06 12:30:00 | 2104-04-06 13:30:00 | 2104-04-06 13:30:00 |

## Cells it would close

- **ZONE-HORIZON-RENDER-1 residue on an instant, on main:** 102 cells, `to_utc_timestamp` 54 and
  `from_utc_timestamp` 48 (`python/repark/tests/timestamps/zone_horizon_render_1_residue.json`),
  including non-edge `jul` cells after 2099. How many the fix closes is to be measured.
- **The three ZS-7 cells:** `America/New_York|2099|wall_overlap|to_utc_timestamp_wall`,
  `America/New_York|2100|wall_gap|to_utc_timestamp_wall`,
  `America/New_York|2104|wall_gap|to_utc_timestamp_wall`. EQUAL on main by value only (wrong
  type, `timestamp_ntz`), honest DIFFERENT after ZONE-SEAM-1, in the residue as expected
  divergence.
- **In this table:** the 19 (c) and 10 (e) cells, and through the composition the matching (b)
  and (d) cells.

## The fallback ZS-7 did not take (for the record)

Restricting `SparkUtcShift` to `DATE` and `NULL` and leaving a wall argument as on main would have
kept the three cells EQUAL by value and cost: the 15 ZONE-ORACLE-1 wall cells
(`bare.wall.to_utc_timestamp`, `bare.wall.from_utc_timestamp`, `bare.col_wall.to_utc_timestamp`,
5 zones each) back to DIFFERENT; the 15 ZONE-HORIZON-RENDER-1 wall edge cells ZONE-SEAM-1 closed
back to the residue; and the R-NTZ-S2-9 move (`w_ref_from` in `test_ntz_9_verify.py`) back to the
declared divergence.

## Out of scope

The wall argument's implicit cast (ZONE-SEAM-1's `SparkUtcShift`, equal to Spark's composition at
every measured edge); the leaf cast (equal to Spark at every measured edge).

# ICE-TSNS-FLOOR-DOORS-1 step 0: main cells

Measured 2026-10-14 on the release module of main at
`018d0efdbe8bc25ccaeebf39950704f3ef8ada52`, with the recorder
`python/repark/tests/_record_ice_tsns_floor_doors_1_main.py`.
Source table is format version 3. Each combination writes eleven probe ticks
through one door and reads the stored microsecond ticks back as integers.

Zones: UTC, America/New_York, Asia/Kolkata (the 30-minute-offset zone).
Values: -1, -1001, -999, 0, 1, 999, 1001 ns; one wall inside New York's
repeated hour (`2026-11-01 01:30:00.000000001`) and one inside its skipped hour
(`2026-03-08 02:30:00.000000001`), both read as UTC nanoseconds; the int64
minimum and maximum nanosecond instants. Sources: `timestamp_ns` (naive wall),
`timestamptz_ns` (instant). Targets: `TIMESTAMP` and `TIMESTAMP_NTZ`, format
versions 2 and 3. Doors: INSERT, INSERT OVERWRITE, MERGE insert arm, MERGE
update arm, UPDATE from a sibling column, `overwritePartitions`,
`insertInto(overwrite=True)`, `writeTo().append()`, `insertInto` append.
216 combinations, 2376 ticks.

References are derived by the recorder from the rule, never typed in: the
floor `tick // 1000` on Python integers, and the cross-domain conversion
through `zoneinfo` (naive wall to instant for `timestamp_ns` into `TIMESTAMP`;
instant to wall for `timestamptz_ns` into `TIMESTAMP_NTZ`). Labels: EQUAL-RULE
when the stored tick equals the rule; otherwise DIFFERS with truncated (toward
zero where the floor differs), wall-as-UTC (the UTC reading where the session
zone differs), both, other, or error.

## Counts by door and target (ticks)

Each row holds 66 ticks (3 zones x 2 sources x 11), except where noted.

TIMESTAMP targets, both versions identically:

| door | EQUAL | truncated | wall-as-UTC | both | error |
|---|---|---|---|---|---|
| INSERT | 66 | 0 | 0 | 0 | 0 |
| MERGE update arm | 66 | 0 | 0 | 0 | 0 |
| UPDATE (v3) | 66 | 0 | 0 | 0 | 0 |
| UPDATE (v2) | 0 | 0 | 0 | 0 | 66 |
| writeTo().append() | 66 | 0 | 0 | 0 | 0 |
| insertInto append | 66 | 0 | 0 | 0 | 0 |
| INSERT OVERWRITE | 28 | 16 | 14 | 8 | 0 |
| MERGE insert arm | 28 | 16 | 14 | 8 | 0 |
| overwritePartitions | 28 | 16 | 14 | 8 | 0 |
| insertInto(overwrite=True) | 28 | 16 | 14 | 8 | 0 |

TIMESTAMP_NTZ targets, both versions identically, every door including INSERT:

| door | EQUAL | truncated | other | error |
|---|---|---|---|---|
| every door except UPDATE on v2 | 42 | 21 | 3 | 0 |
| UPDATE on v2 | 0 | 0 | 0 | 66 |

The v2 UPDATE errors are the format-version refusal, not a narrowing result:
`PySparkException` with `Invalid type for c: timestamp_ns is not supported
until v3` (or the `timestamptz_ns` spelling for that source), on all 12 v2
UPDATE combinations (132 ticks).

The NTZ `other` ticks are the Asia/Kolkata `timestamptz_ns` pre-epoch trio
(-1, -1001, -999 ns): the stored wall sits 1 micro above the floor because the
instant truncates toward zero before the zone shift. It is the truncation
family seen through a half-hour offset; New York and UTC label the same trio
truncated.

## The 72 registry cells reproduce: yes

The registry row counts 36 cells per format version over six doors (INSERT
OVERWRITE, three MERGE insert spellings, `overwritePartitions`,
`insertInto(overwrite=True)`), two sources, three zones, UTC included. This
matrix collapses the three MERGE spellings to one arm, so the overlapping
subset is 48 combinations (4 doors x 2 sources x 3 zones x 2 versions). On all
48 the split holds: INSERT stores the rule on every tick, and each overwrite
door differs wherever the rule differs from truncation or from the UTC
reading. No other TIMESTAMP door differs from INSERT.

## Files

- `main-cells.json`: the 216 cells with stored ticks, floor and rule
  references, and labels.
- The recorder derives every reference from the rule at record time.

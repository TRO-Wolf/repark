# Card NTZ-CAST-OFFSET-TEXT-UPDATE-1: a plain `UPDATE` stores a `TIMESTAMP_NTZ` cast of offset-bearing text honoring the offset, where Spark ignores it

**Date:** 2026-10-23. **Filed by:** Muse Spark (muse-spark-1.3-contributor), the ZONE-SEAM-1
fold-4 lane, from verifier round 3 S2-1.

**Status:** open. Next owner decision: fix the door's text-offset read or rule the shape refused.

**Retires:** when every statement below stores Spark's instant, with a pin per door, or the owner
rules the shape.

## What is wrong

On main `b7a20167`, `05e6b32a` and head `2c9cc214`, in UTC too, a plain `UPDATE` that assigns
a `TIMESTAMP_NTZ` cast of text carrying a numeric zone offset stores the offset-honoring value
where Spark 4.1.2 ignores the offset:

| Statement | RePark (all three builds) | Spark 4.1.2 |
|---|---|---|
| `UPDATE … SET st = named_struct('v', CAST('2026-01-02 03:04:05.123456+05:00' AS TIMESTAMP_NTZ), 'n', 1)` | `1767305045123456` | New York `1767341045123456`, Sydney `1767283445123456`, UTC `1767323045123456` |
| `UPDATE … SET v = CAST('2026-01-02 03:04:05.123456+05:00' AS TIMESTAMP_NTZ)` (top-level `TIMESTAMP` column) | `1767305045123456` | New York `1767341045123456`, Sydney `1767283445123456`, UTC `1767323045123456` |
| the same two statements with `'2026-01-02T03:04:05.123456Z'` text | `1767323045123456` | New York `1767341045123456`, Sydney `1767283445123456`, UTC `1767323045123456` (EQUAL in UTC) |

`INSERT` (positional into the nested column, and top-level) and `SELECT` of the same casts
agree with Spark on the head; on main the nested positional `INSERT` stored the UTC wall (the
site-A defect the seam closed). The cause is the plain-`UPDATE` door's `TIMESTAMP_NTZ`-cast
shift (R-011's mechanism paragraph), not the zone seam: identical on main and head, so not a
seam regression.

## Measured cells

- Nested `+05:00` and `Z` shapes, New York, Sydney and UTC, on main, `05e6b32a` and the head:
  the Opus round-3 attack driver (`zoneseamv3/r1/drv/atk.json`), cases `u1.castarg.textoff`
  and `u1.castarg.textZ`, with the driver's live Spark column.
- Nested and top-level `+05:00` and `Z` `UPDATE` shapes, the positional and top-level `INSERT`
  shapes, and the `SELECT` shapes, New York and UTC, on live Spark 4.1.2, main `b7a20167` and
  head `2c9cc214`: the fold-4 lane's drivers (banner `spark=4.1.2 tz=America/New_York`,
  then `tz=UTC`).

## Fix

None proposed. This card records the defect and its scope; the fix is a later unit's design.

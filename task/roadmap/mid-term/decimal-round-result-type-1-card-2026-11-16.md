# Card DECIMAL-ROUND-RESULT-TYPE-1: a DECIMAL `round` keeps its input type, where Spark narrows it

**Date:** 2026-11-16. **Filed by:** the records clerk, from the 2026-11-15 records backlog (item 5; #1337's
verifiers). Measured on main `a06c1088`'s package against Spark 4.1.2, both ANSI modes, the Spark door and the
native door.

**Status:** PROPOSED, not dispatched. Pre-existing: main types it the same way.

**Severity: S3, a type-only difference.** The values match Spark's in every measured cell; only the result type
differs, and only for `round` on the two RePark doors (and `bround` on the native door, where the name does not
resolve at all).

## Finding

`round` of a DECIMAL answers the input's own type on the Spark door and a widened type on the native door, where
Spark answers a narrowed type. `bround` on the Spark door already answers Spark's type; on the native door the
function name is unknown.

## Measured

`SELECT round(CAST(1.2345 AS DECIMAL(10,4)), 2)` and the `bround` twin, identical with ANSI on and off:

| cell | RePark Spark door | RePark native door | Spark 4.1.2 |
|---|---|---|---|
| `round(…, 2)` | `decimal(10,4)`, 1.2300 | `decimal(11,4)`, 1.2300 | `decimal(9,2)`, 1.23 |
| `bround(…, 2)` | `decimal(9,2)`, 1.23 — agrees | refused: `[UNRESOLVED_ROUTINE] Cannot resolve routine \`bround\`` | `decimal(9,2)`, 1.23 |

Spark's rule (`decimal(10,4)` at scale 2 → `decimal(9,2)`) is the same narrowing the
[ROUND-DOUBLE-EXACT-1](round-double-exact-1-card-2026-11-14.md) card's seat would apply to a decimal arm; that card
covers FLOAT and DOUBLE values only and its close clause does not include this shape, so this card carries it.

Up: [map.md](map.md).

## Append (2026-11-17, afternoon): round() cells measured in the AVG step 0

The AVG-DECIMAL-ROUND-1 step 0 (#1352, `9c96a963`) measured `round` on the Spark door and the native door. RePark's `round(decimal, s)` keeps the input type; Spark's takes the scale `s`:

- `round(d10_2 / 3, 2)`: Spark `decimal(11,2)`, RePark `decimal(21,13)`, values equal (`T`);
- `round(d10_2, 1)`: Spark `decimal(10,1)`, RePark `decimal(10,2)`, values equal (`T`);
- the DataFrame `round(col(d) / 3, 2)` is `double` on RePark, because of the DataFrame `/` seat (DECIMAL-DIVIDE-LITERAL-1), against `decimal(11,2)` on Spark.

Over `round(d, 1)` and `round(d / 3, 2)`: 20 cells per mode. With ANSI on, `=` 1, `T` 17, `B` 2; with ANSI off, `=` 1, `T` 19 (domain table, "After the inventory critic", M4). They were measured in the AVG step 0 and are not folded into either unit (ruling 3): this card keeps `round()`.

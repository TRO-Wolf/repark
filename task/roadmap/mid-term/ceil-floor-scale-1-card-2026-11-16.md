# Card CEIL-FLOOR-SCALE-1: two-argument `ceil` and `floor` are refused, where Spark answers a decimal

**Date:** 2026-11-16. **Filed by:** the records clerk, from the 2026-11-15 records backlog (item 4; #1337's
verifiers). Measured on main `a06c1088`'s package against Spark 4.1.2, both ANSI modes, the Spark door and the
native door.

**Status:** PROPOSED, not dispatched. Pre-existing: main refuses it too.

**Severity: S2, a refusal.** RePark refuses every two-argument form where Spark answers, and `ceil(x, n)` with a
literal scale is an ordinary Spark shape. No answer is wrong: the one-argument forms are untouched.

## Finding

`ceil` and `floor` with a second scale argument refuse on both doors. The error text depends on the argument types:
a DOUBLE or FLOAT first argument fails inside the engine (`This feature is not implemented: CEIL with scale is not
supported`), while a DECIMAL first argument fails at planning
(`Function 'ceil' expects 1 arguments but received 2` on the Spark door; `Failed to coerce arguments … Float64,
Int64` on the native door, whose literals type differently).

## Measured

All RePark rows are identical with ANSI on and off; Spark answers identically in both modes. `t4(x double)` holds
one row, 3.14159.

| cell | RePark, both doors | Spark 4.1.2 |
|---|---|---|
| `ceil(3.14159, 2)` | refused: CEIL with scale is not supported | `decimal(4,2)`, 3.15 |
| `floor(-3.14159, 1)` | refused: FLOOR with scale is not supported | `decimal(3,1)`, -3.2 |
| `ceil(125.25, -1)` | refused: `expects 1 arguments but received 2` (Spark door) | `decimal(4,0)`, 130 |
| `floor(125.25, -1)` | refused, same planning error | `decimal(4,0)`, 120 |
| `ceil(CAST(3.14159 AS DOUBLE), 2)` | refused: CEIL with scale is not supported | `decimal(18,2)`, 3.15 |
| `ceil(x, 1) FROM t4` | refused: CEIL with scale is not supported | `decimal(17,1)`, 3.2 |

Spark's result type follows its rule for the two-argument form (a decimal whose shape depends on the input type and
the scale: negatives scales give scale 0). RePark has no two-argument arm on either door.

## Not covered elsewhere

A roadmap search for `ceil`/`floor` cards finds only overnight-report mentions, no card. The scale refusal text
comes from upstream (no such string lives under `crates/`), so step 0 must find where the Spark door binds `ceil`
and `floor` and add the decimal-scale arm there.

Up: [map.md](map.md).

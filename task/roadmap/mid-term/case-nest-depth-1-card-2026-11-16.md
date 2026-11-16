# Card CASE-NEST-DEPTH-1: a `CASE` nested 48 deep fails to parse, where Spark parses 100

**Date:** 2026-11-16. **Filed by:** the records clerk, from the 2026-11-15 records backlog (item 7; the #1339 cost
run). Measured on main `a06c1088`'s package against Spark 4.1.2, both ANSI modes, the Spark door and the native
door.

**Status:** PROPOSED, not dispatched. Pre-existing: main parses the same depths.

**Severity: S3, a refusal on a rare shape.** A 48-deep `CASE` is machine-generated SQL, not hand-written; 47 deep
parses and answers on both doors.

## Finding

`SELECT CASE WHEN TRUE THEN 1 ELSE <…> END` with the `ELSE` nesting the next `CASE`, over the literal `0`, parses
and answers `1` up to depth 47 and fails to parse from depth 48, on both doors, in both ANSI modes:
`SQL error: ParserError("Expected: end of statement, found: TRUE at Line: 1, Column: 18")`. Spark 4.1.2 parses and
answers `1` (`int`) at depths 50 and 100 alike.

## Measured

| depth | RePark Spark door | RePark native door | Spark 4.1.2 |
|---|---|---|---|
| 30, 35, 40, 45, 46, 47 | parses, 1 (`int`) | parses, 1 (`bigint`) | `int`, 1 |
| 48, 49, 50, 60, 100 | refused, `ParserError … found: TRUE` | refused, same text | `int`, 1 (50 and 100 measured; 48/49/60 not run, 30–47 not run) |

The threshold sits between 47 and 48: the deepest parse is 47, the shallowest refusal is 48. The error position
(`Line: 1, Column: 18`, the second `TRUE`) reads like a nesting cap in the parser rather than a length cap. The
int/bigint door difference on the agreeing rows is the usual native-door literal typing, not this finding.

## Not covered elsewhere

[CONST-EVAL-CEILING-1](const-eval-ceiling-1-card-2026-10-01.md) was checked first: it is the constant evaluator
proving false equalities out of `coalesce`/`nullif`, a different stage from the parser. It does not cover a parse
depth cap.

Up: [map.md](map.md).

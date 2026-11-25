# Card ARRAY-REMOVE-NULLABILITY-1: `array_remove` with a nullable element declares a non-null result and crashes

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from the separate shapes on the
TRY-CAST-NONNULL-FIELD-1 card. **Severity:** S2 proposed; the owner rates it. It crashes loudly and never answers
wrongly. **Status:** filed, not dispatched.

## Measured

Seven cells are on the [TRY-CAST-NONNULL-FIELD-1 card](try-cast-nonnull-field-1-card-2026-11-24.md), under "Still
unlike Spark", identical in both ANSI modes. Each is `array_remove(arr, x)` over a NOT NULL array column, where `x`
can be NULL: a `CASE` with no `ELSE`, a NULL literal element, a left-join column, `nullif`, an empty scalar subquery,
`TRY_CAST` and its negation.
- Spark 4.1.2 answers a NULL array for the NULL rows, with a nullable result.
- Main crashes with `Column 'r' is declared as non-nullable` and reports the field as non-null.

## Cause

`array_remove`'s `return_field_from_args` ignores the nullability of its second argument (`arg_fields[1]`). Spark's
`ArrayRemove` is null when either argument is null.

## First step

Find every array function whose result nullability ignores an argument (`array_remove`, `array_position`,
`array_contains`, `arrays_overlap` and the rest), measure each one against Spark, and fix the whole family at one seat.

Up: [map.md](map.md).

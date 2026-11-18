# Card NATIVE-DECIMAL-SUM-1: the native door's decimal `sum` answers silently where Spark raises

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the SUM-DEC-I128WRAP-1 step 0 (PR #1357, head `2f63e2c7`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S2, not label-blocking** (owner, 2026-11-17, "defaults on both", question 3). The label is the Spark door's.

## Finding

On the native door (not the Spark door), a decimal `sum` past 38 digits answers a value silently where Spark's answer differs; the sample's silent cells are counted below. The native door uses DataFusion's `Sum` and has no `try_sum` (`session.rs:151-167`, `session_runtime.rs:452-461`).

## Cells

Sum step 0, native-door sample of 589 cells per mode: decimal `sum` silent 142 cells with ANSI on and 136 with ANSI off. `try_sum` unresolved (84 and 21 cells). `BIGINT` silent 15 with ANSI on, see SUM-BIGINT-ANSI-OVERFLOW-1.

## Relation

NATIVE-DECIMAL-AVG-DIVIDE-1 (the same door's `avg`, `mean` and decimal `/` truncation). Registering the Spark door's fix on the native door later is one line in `register_native_door_functions` (sum sketch §6, question 3).

## Source

SUM-DEC-I128WRAP-1 step 0 hand-back, "Native door" line; step 0 sketch §6 question 3. The sum critic did not re-measure the native door.

Up: [map.md](map.md).

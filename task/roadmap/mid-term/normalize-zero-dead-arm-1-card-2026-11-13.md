# Card NORMALIZE-ZERO-DEAD-ARM-1: an unused `__repark_normalize_zero__` arm in the facade's function dispatch

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SIGNED-ZERO-NORMALIZE-1 part B
(#1326), its branch at filing (`29049b6c`), read by the records clerk.

**Status:** filed 2026-11-13, not dispatched; a cleanup after #1326 merges. Severity **S4**: dead code, no behaviour.

## What

On #1326's branch, `crates/repark-python/src/column/function_dispatch/dispatch_json.rs` maps
`__repark_normalize_zero__` to `repark_core::extension::normalize_zero_call`. Delta 1 added it for the facade's
`dropDuplicates(subset)` float keys; since delta 2 that projection is built in `crates/repark-python/src/logical_names.rs`,
and the facade no longer calls the arm (the branch's `function_dispatch/map.md` says so).

## Close

The arm and its map line are removed after #1326 merges, with the facade suite green.

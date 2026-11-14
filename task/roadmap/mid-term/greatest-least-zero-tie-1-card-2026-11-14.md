# Card GREATEST-LEAST-ZERO-TIE-1: `greatest`/`least` over `0.0` and `-0.0` return the other sign

**Date:** 2026-11-14. **Filed by:** the SIGNED-ZERO-CONST-COMPARE-1 lane (`claude-opus-5-5`), on the inventory critic's
M8 of 2026-11-14 and the orchestrator's records ruling the same day. **Status:** filed, unrated, not dispatched.

## Measured (step 0 of SIGNED-ZERO-CONST-COMPARE-1, main `d843d37e`, Spark 4.1.2)

| expression | Spark | RePark |
|---|---|---|
| `greatest(-0.0, 0.0)` | `-0.0` | `0.0` |
| `greatest(0.0, -0.0)` | `0.0` | `0.0` |
| `least(-0.0, 0.0)` | `-0.0` | `-0.0` |
| `least(0.0, -0.0)` | `0.0` | `-0.0` |

- Spark keeps the **first** argument on a tie, deterministically. RePark compares by total order:
  `datafusion-functions-54.1.0/src/core/greatest.rs:77-83`, the mirror in `least.rs`, and the `cmp::gt_eq` /
  `cmp::lt_eq` array paths.
- 400 + 400 step-0 cells (`evidence/step0/table.md`, `greatest` and `least` rows) on every door, both ANSI modes.
- Not a SIGN-ONLY declaration beside SIGNED-ZERO-KEYS-1-MINMAX: Spark's answer is deterministic, unlike aggregate
  `min`/`max` sign or `ORDER BY` ties.
- NaN in `greatest`/`least` stays with NAN-GREATEST-ARRAY-1. FUNC-FLOAT-EQ-1 names these ties; this card is their
  home.

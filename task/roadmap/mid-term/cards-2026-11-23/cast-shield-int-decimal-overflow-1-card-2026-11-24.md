# Card CAST-SHIELD-INT-DECIMAL-OVERFLOW-1: an integer-to-decimal overflow in a filter is silently false under ANSI

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review, measured here.
**Severity:** S2 (owner, 2026-11-24). **Status:** filed, not dispatched.

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2. `t` is `(SELECT CAST(123456 AS BIGINT) AS b)`.

| Statement | ANSI | Spark 4.1.2 | RePark |
|---|---|---|---|
| `SELECT b FROM t WHERE CAST(b AS DECIMAL(5,0)) = 1` | on | `[NUMERIC_VALUE_OUT_OF_RANGE]` | **no rows** |
| `SELECT b FROM t WHERE CAST(b AS DECIMAL(5,0)) = 1` | off | no rows | no rows |
| `SELECT CAST(b AS DECIMAL(5,0)) FROM t` | on | `[NUMERIC_VALUE_OUT_OF_RANGE]` | Arrow error (loud) |
| `SELECT CAST(b AS DECIMAL(5,0)) FROM t` | off | NULL | **Arrow error** (loud) |

- **ANSI on, in a filter:** RePark gives no rows and no error. A caller who relies on ANSI to stop bad data gets no
  signal.
- **ANSI off, in a projection:** RePark crashes where Spark answers NULL.

## Cause (named by the outside review, not yet read here)

The filter-side cast shield and the projection cast disagree about which integer-to-decimal casts can overflow. A
single table of cast semantics, shared by the shield, the stats barrier and the overwrite path, was proposed.

Up: [map.md](map.md).

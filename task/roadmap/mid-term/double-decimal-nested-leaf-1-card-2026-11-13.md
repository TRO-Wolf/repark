# Card DOUBLE-DECIMAL-NESTED-LEAF-1: a DOUBLE leaf of a struct, array or map into DECIMAL rounds Arrow's product

**Date:** 2026-11-13. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 builder (`claude-opus-5-5`) under the owner's
ruling of 2026-11-13, about 20:50 UTC, verbatim "defaults" (nested leaves are carded, outside the scoped scalar-column
label). **Sources:** that unit's step 0 select cells and its inventory critic (M8);
[evidence](../../wo/double-decimal-store-round-1/map.md).

**Status:** filed, not dispatched. Severity **S3**. Outside the scoped flat label.

## Measured (RePark main against Spark 4.1.2, both ANSI modes unless noted)

| cell | RePark | Spark |
|---|---|---|
| `CAST(array(CAST('1.255' AS DOUBLE), CAST('1.005' AS DOUBLE)) AS ARRAY<DECIMAL(10,2)>)` | 1.25, 1.00 | 1.26, 1.01 |
| `CAST(named_struct('a', CAST('1.255' AS DOUBLE)) AS STRUCT<a: DECIMAL(10,2)>)` | 1.25 | 1.26 |
| `CAST(map('k', CAST('1.255' AS DOUBLE)) AS MAP<STRING, DECIMAL(10,2)>)` | 1.25 | 1.26 |
| `CAST(map(CAST('1.255' AS DOUBLE), 1) AS MAP<DECIMAL(10,2), INT>)` (ANSI on; both refuse with ANSI off) | 1.25 | 1.26 |
| INSERT of `named_struct`, `array`, `map` with a DOUBLE leaf into `STRUCT`/`ARRAY`/`MAP` of DECIMAL; `writeTo().append()` of a struct (critic) | 1.25 | 1.26 |

The leaf is cast inside Arrow's recursive cast or the map-cast UDF (`crates/repark-functions/src/cast_map/leaf.rs:454`,
`cast_map.rs:306`), never as a scalar `Expr::Cast`, so the unit's seats do not reach it. The struct-key subscript cells of
CAST-DOUBLE-TO-DECIMAL-ROUNDING-1 (ledger C-044) belong here too; over a literal map Spark refuses those lookups with
`UNEXPECTED_INPUT_TYPE`, so they are re-measured over a stored map.

## Close

A FLOAT/DOUBLE leaf of a struct, array or map cast or stored into a DECIMAL leaf rounds JDK 17's text half up, pinned on
the cells above in both ANSI modes.

Up: [map.md](map.md).

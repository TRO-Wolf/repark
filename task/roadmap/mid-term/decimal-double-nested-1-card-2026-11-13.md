# Card DECIMAL-DOUBLE-NESTED-1: nested DECIMAL and DOUBLE leaves in `UNION` and `CASE` type `decimal(30,15)`; Spark types `double`

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q3′; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md),
[domain table](../../wo/decimal-double-union-coercion-1/domain-table.md), the revision's nested cells); owner,
2026-11-13, about 14:52 UTC, verbatim "Defaults, and merge #1323 when green" (Q3′: the constructors fold into the
decimal unit, the nested leaves get a card).

**Status:** filed 2026-11-13, not dispatched. Severity **S3**. The decimal unit pins these cells unchanged.

## Measured (main `04d84ab9`, Spark 4.1.2, the same in both ANSI modes)

| cell | Spark | RePark |
|---|---|---|
| `struct_union` | `struct<a:double>`, 0.1 and 1e-20 | `struct<a:decimal(30,15)>`, 0.1 and 0 |
| `array_union` | `array<double>` | `array<decimal(30,15)>`, 1e-20 read as 0 |
| `map_union` | `map<string,double>` | `map<string,decimal(30,15)>`, 1e-20 read as 0 |
| `struct_case`, `array_case`, `map_case` | the `double` leaf, 1e-20 | the `decimal(30,15)` leaf, 0 |

The sketch also carries B idx 9's struct operands under this card.

## Close

Nested struct, array and map leaves in `UNION` and `CASE` type `double` as Spark does and keep 1e-20, pinned on the six
cells per mode.

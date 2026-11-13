# Card DOUBLE-DECIMAL-STORE-ROUND-1: storing the DOUBLE `1.255` into DECIMAL(10,2) writes `1.25`; Spark writes `1.26`

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q5′; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md),
[domain table](../../wo/decimal-double-union-coercion-1/domain-table.md)); owner, 2026-11-13, about 14:52 UTC, verbatim
"Defaults, and merge #1323 when green".

**Status:** filed 2026-11-13, not dispatched. Severity **S1**, a wrong write. **Label-blocking** as the decimal unit's
prerequisite: the owner's ruling of Q5′ puts it before the DECIMAL-DOUBLE-UNION-COERCION-1 build.

## Measured (main `04d84ab9`, Spark 4.1.2, both ANSI modes)

| cell | Spark | RePark | verdict |
|---|---|---|---|
| `ins_values_dec_round` | `decimal(10,2)`: 1=0.10, 2=1.26 | 1=0.10, 2=**1.25** | WRONG-WRITE |
| `ins_union_dec_round` | 1=0.10, 2=1.26 | 1=0.10, 2=1.26 | EQUAL today |

`ins_union_dec_round` is EQUAL only because today's `UNION` types the column `decimal(30,15)` first; once the decimal
unit widens it to `double`, it takes the same store rounding and turns WRONG. That is why this card lands first.

## Relation

[CAST-DOUBLE-TO-DECIMAL-ROUNDING-1](cast-double-to-decimal-rounding-1-card-2026-11-02.md) (P3): `CAST` from DOUBLE to
DECIMAL rounds the binary value where Spark rounds the double's shortest text (`1.005` to `1.00` against `1.01`).
Likely the same mechanism on the store door; not measured as one.

## Close

A DOUBLE stored into a DECIMAL column rounds as Spark does (`1.255` to `1.26`), pinned on `INSERT … VALUES` and the
`UNION` source in both ANSI modes.

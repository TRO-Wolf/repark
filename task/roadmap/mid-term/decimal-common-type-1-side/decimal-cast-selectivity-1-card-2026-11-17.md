# Card DECIMAL-CAST-SELECTIVITY-1: DataFusion's filter-selectivity assertion raises on a cast of a BIGINT column with extreme statistics

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 4 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the DECIMAL-COMMON-TYPE-1 build's [bar](../../../wo/decimal-common-type-1/evidence/built/bar.md) and [bar table](../../../wo/decimal-common-type-1/evidence/built/raw/bar-table.md); Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q6.

**Status:** filed 2026-11-17, not dispatched.

**Severity: S2, not label-blocking** (ruled 2026-11-17). It is its own card and its own upstream draft. Filing upstream stays the owner's decision.

The bar names this finding DECIMAL-CAST-SELECTIVITY, without the `-1` suffix. This card is that finding.

## Finding

`WHERE CAST(i64 AS DECIMAL(38,18)) = CAST('1.0…' AS DECIMAL(38,18))` hits DataFusion's assertion `Selectivity is out of limit: inf`. It happens when the BIGINT column's statistics reach ±2⁶³. The same expression in the select list agrees with Spark. Main raises the same way with a user-written cast of the same type.

## Cell

`const/i64_eq_hlit/tx/where`: an over-raise in both ANSI modes (bar table).

## Reading and step 0

Fable's lead (a reading): "Its step 0 should find the exact trigger. If a wide range short of ±2⁶³ is enough, hash-valued `BIGINT` columns reach it." Fable also says it is DataFusion's filter-statistics code, not the simplifier. It therefore stays out of [DATAFUSION-SIMPLIFIER-UPSTREAM-1](../sweep-2026-11-16/datafusion-simplifier-upstream-1-card-2026-11-16.md).

Up: [map.md](map.md).

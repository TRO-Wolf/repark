# Card NATIVE-DECIMAL-AVG-DIVIDE-1: the native door's `avg`, `mean` and decimal `/` truncate

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's afternoon ruling of 2026-11-17 (about 14:05 UTC), ruling 9 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the AVG-DECIMAL-ROUND-1 step 0, merged as #1352 (`9c96a963`): [sketch §4 (the native door) and §6, Q9](../../../wo/avg-decimal-round-1/sketch.md) and the [domain table](../../../wo/avg-decimal-round-1/domain-table.md) ("The native door `repark.sql()`" and "After the inventory critic"), measured on main `a792ce47`.

**Status:** filed 2026-11-17, not dispatched. **Not label-blocking** (ruling 9).

**Severity: S2** (ruled 2026-11-17, ruling 9).

## Finding

On the native door (`repark.sql()`), `avg` and `mean` are DataFusion's `Avg`, which truncates. The decimal `/` is DataFusion's own operator, and it truncates too: `d10_2 / 3` has Spark's type `decimal(14,6)` and the value `0.836666` (Spark `0.836667`). AVG-DECIMAL-ROUND-1 does not reach this door (sketch §4).

## Cells

- The native door's grid, all families: 268 cells per mode; with ANSI on, `t` 98 and `V` 51 (domain table, headline).
- The revision's `native: mixed/*` and `mean/grouped/*` (sketch §6, Q9). On this door the `DISTINCT` cell answers, truncated (the `n05` shape, `t` on `c10_2` and `c28_2`).

## Seat

- `avg` and `mean` on the native door: stock DataFusion planning and DataFusion's `Avg` (inventory, "the doors"; `prepare_session_sql` at `crates/repark-python/src/session_runtime.rs:414`).
- The division: stock DataFusion `BinaryExpr` division and its truncating Arrow kernel (EXPLAIN `d10_2 / Decimal128(Some(3),20,0)`). Not diagnosed further (inventory).

## Relation to DECIMAL-COMMON-TYPE-NATIVE-1

The two are separate families on the same door. [DECIMAL-COMMON-TYPE-NATIVE-1](../decimal-common-type-1-side/decimal-common-type-native-1-card-2026-11-16.md) is the native door's common-type family. The sketch found that no existing card holds this one (sketch §4: "DECIMAL-COMMON-TYPE-NATIVE-1 is the common-type family only").

Up: [map.md](map.md).

## Append (2026-11-17, evening): the native door's sum-wrap answers (delta 1)

Delta 1 measured the sum-wrap shapes on the native door (`repark.sql()`) against main and the head. Test `test_avg_decimal_sumwrap_native_door_records_divergence` (commit `46fac9af`) pins them. The head equals main on all nine native cells.

- The wrapped answers stand: `0.0000` (grouped and global) and `100000.0000` (non-zero grouped). Spark answers NULL or raises on the same fixtures.
- The window `avg` raises the old `Arithmetic Overflow in AvgAccumulator` text.
- `try_avg` is unresolved on the native door (`UNRESOLVED_ROUTINE`).

The Spark door's sum-wrap cells are closed by AVG-DECIMAL-ROUND-1 (PR #1355, open). The native door's remainder stays on this card.

# map — repark-spark/src/void_type

## Purpose

WO LTZ-STORE-INT-1 support. The parent `void_type.rs` holds the Spark-door `VOID`
handling (`CAST(NULL AS VOID)` rewrite, the non-NULL VALUES refusal into
`unknown` columns); this directory holds the VALUES store-assignment gate for
`TIMESTAMP` (LTZ) targets it calls first.

## Contents

- `ltz_values_store.rs` — `refuse_unassignable_ltz_values` judges each VALUES
  cell mapped to a microsecond `Timestamp` column through the shared
  `incompatible_update_message` gate — bare literals by their Spark kind, anything
  else by a `SELECT <cell>` probe — so `VALUES (0, 1)` refuses with Spark's
  recorded `CANNOT_SAFELY_CAST` text while NULL, DATE, TIMESTAMP, TIMESTAMP_NTZ
  and explicit `CAST`s still store. Skips TIMESTAMP_NTZ targets, partitioned
  inserts, and anything it cannot map. Reached from
  `refuse_insert_void_values`. **Fold 2026-09-28 (critic V-001):** null-valued
  cells pass; numerics refuse through a DDL-name fallback even when the shared
  gate cannot name them; `1L` reads `BIGINT`; DECIMAL casts and typed literals
  read by declared type (four classifier tests).
  pins: ltz-store-int-1/C-001

## Pointers

- Up: [../map.md](../map.md)

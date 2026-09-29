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
  read by declared type. **Fold 2026-09-28 (verifier VL-1..VL-6):** the V-001 null
  rule narrows to bare NULL — `CAST(NULL AS T)` is judged by `T`; fractionals
  read `DECIMAL(p,s)` and out-of-`INT`-range integers read `BIGINT`.
  **Fold 2026-09-28 (re-verify RL-1..RL-3):** the VL-1 function deferral narrows
  to two-argument `nvl`/`ifnull`, probed as `coalesce(a, b)`; every other
  function is judged by its probed type; leading-zero fractions count precision
  from significant digits (six classifier tests).
  pins: ltz-store-int-1/C-001
  **WO STORE-TS-TO-NUMERIC-1 (2026-09-28):** the gate also judges numeric, DATE
  and BOOLEAN targets (`is_judged_target`, one classifier test), through
  `incompatible_store_message` so a DECIMAL target has Spark's name: TIMESTAMP
  into BIGINT, STRING into INT, BOOLEAN or DATE, BOOLEAN into INT and INT into
  BOOLEAN refuse with Spark's text. The DDL-name numeric fallback stays for LTZ
  targets only, so a numeric into a numeric column still stores.
  pins: store-ts-to-numeric-1/C-001
  **Fold 2026-09-29 (verifier VT-1):** a STRING-planned cell stores again when
  Spark widens its branches to a storable type — `spark_branch_type` reads the
  CASE/nvl/nullif/coalesce/if/nvl2/greatest/least branches (literals, DATE typed
  strings and plain CASTs without a probe, `__repark_float_to_string__` is not
  present at this altitude) and `widened_stores` skips the refusal only when the
  widened type stores (a widened numeric into an LTZ target still refuses, and
  the refusal keeps naming the planned STRING so still-refusing cells keep their
  text). DATE typed strings and plain CASTs now read by declared type, so DATE
  and `CAST(i AS DOUBLE)` VALUES rows skip the per-cell probe (verifier VT-4).
  **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):** the VT-1 widening
  escape is gone. For a numeric, DATE or BOOLEAN target the gate stays silent
  when the planned source is STRING (base behaviour: STRING into those columns
  stores again) or when `source_leaves::source_type_is_reliable` rejects the
  cell; LTZ targets keep LTZ-STORE-INT-1's judgment unchanged, so a STRING cell
  into TIMESTAMP refuses as on base. TIMESTAMP, TIMESTAMP_NTZ and DATE cells into
  numeric and BOOLEAN columns, BOOLEAN into numeric and numeric into BOOLEAN still
  refuse. pins: store-ts-to-numeric-1/C-001, C-006, C-007
- `insert_source_types.rs` — **WO STORE-TS-TO-NUMERIC-1 (2026-09-28):**
  `refuse_insert_source_types` is the Spark door's INSERT gate for two source
  types the analyzer gate cannot see: a negated NULL (Spark's DOUBLE, judged by
  `repark_iceberg`'s `refuse_negated_null_writes`) and a STRING into FLOAT or
  DOUBLE (the `spark_float_stringify` rule rewrites that conform cast before
  `InsertStoreAssignment` runs). It loads the target schema, skips a table with
  no DATE, BOOLEAN, timestamp, BINARY or floating-point column, plans the source
  once, maps it positionally, by column list or by name, and refuses with
  Spark's text naming the quoted table. `void_type.rs` re-exports it behind a
  `Box::pin` wrapper. Callers: `router/insert_positional.rs`
  `prepare_positional_insert` (append, owned append, OVERWRITE, VALUES) and
  `insert_by_name.rs`. Unmapped columns and unplannable sources pass.
  pins: store-ts-to-numeric-1/C-002, C-003
  **Fold 2026-09-29 (verifier VT-1):** the STRING → FLOAT/DOUBLE refusal skips a
  source Spark widens to a storable type — `spark_source_type` walks the planned
  source like the negated-NULL lineage (views resolve through the session
  resolver, DataFrame temp views arrive inlined) and `widened_stores` judges the
  widened branch type; `__repark_float_to_string__` is transparent (it only
  wraps FLOAT/DOUBLE, so the walk reads its argument). Called now also from
  `router/insert_positional/replace_where.rs` (verifier VT-2).
  **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):** the STRING → FLOAT/DOUBLE
  refusal, its plan walk (`spark_source_type`) and the `__repark_float_to_string__`
  transparency are removed; the gate now only runs `refuse_negated_null_writes`,
  and only for a table with a DATE, BOOLEAN, timestamp or BINARY column and a
  source with a `Null`-typed column. STRING sources store on every INSERT door as
  on base. pins: store-ts-to-numeric-1/C-002, C-006
- `source_leaves.rs` — **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):**
  `source_type_is_reliable` decides whether a new refusal may trust RePark's
  planned source type. A plain cell (literal, typed string, CAST, a non-widening
  function, a scalar subquery projecting one column) is trusted. A branching cell
  — CASE, `coalesce`/`greatest`/`least`/`nvl`/`ifnull`/`nullif`/`if`/`nvl2`, any
  other scalar subquery, EXISTS, IN (subquery) — is trusted only when every leaf
  it can return has a statically known type that is not STRING and that itself
  refuses into the target (TIMESTAMP, TIMESTAMP_NTZ including
  `__repark_timestamp_ntz__(…)`, and DATE leaves into numeric and BOOLEAN
  columns; bare NULL leaves are neutral); anything else stays silent and the
  store behaves as on base. `is_string_type` names the STRING family. Five
  classifier tests. Replaces the VT-1 widening lattice `spark_widen.rs`, deleted
  with its call sites. pins: store-ts-to-numeric-1/C-007

## Pointers

- Up: [../map.md](../map.md)

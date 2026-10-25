# Card WRITE-NTZ-TO-LTZ-NESTED-1: the Iceberg write path stores a nested TIMESTAMP_NTZ value into a nested TIMESTAMP field as if the wall were UTC

**Date:** 2026-10-22. **Filed by:** Claude (claude-opus-5-5), the ZONE-SEAM-1 lane, under
rulings ZS-8 and ZS-9 of the same day.

**Status:** open. Next write-path Opus unit after GATE-RETIRE-1. Until it lands, ZONE-SEAM-1's
refusal (ZS-9) stands in front of the three sites below outside UTC.
WRITE-SPINE-1 (task/roadmap/mid-term/write-spine-1-card-2026-10-22.md, on main after PR #1149) is
the architecture unit that makes this a one-site fix; the owner picks the order.
**2026-10-22:** Folded into WRITE-SPINE-1 step 2 (owner, 2026-10-22, on the crate-boundary
evaluation's recommendation).

**2026-10-25:** GATE-RETIRE-1 landed (PR #1187).

**Retires:** when the three sites read a nested `TIMESTAMP_NTZ` wall in the session zone, the
U-8 door matrix's `ltz_nested` cells match Spark 4.1.2 on every store door in New York and
Kolkata, and ZONE-SEAM-1's refusal (`refuse_naive_into_instant` in
`crates/repark-spark/src/router/nested_ns.rs`) is removed.

## What is wrong

Spark 4.1.2 stores a `TIMESTAMP_NTZ` value written into a `TIMESTAMP` field as the wall read in
the session zone, nested or not. RePark's write path converts a nested `TIMESTAMP_NTZ` leaf into a
nested `TIMESTAMP` leaf with Arrow's cast, which reads the wall as UTC. On main a container cast
of an instant to a naive struct also read the wall as UTC, and the two cancelled on the doors
below; ZONE-SEAM-1 fixed the cast and exposed the store (74 cells, measured against live Spark on
2026-10-22).

## The three sites (measured with an instrumented build, 2026-10-22)

| site | file / function | runs | doors (cow and mor where both exist) |
|---|---|---|---|
| A | `crates/repark-iceberg/src/write/conform.rs` `entry_store_cast` (via `strict_store_cast` from `conform_batch_scoped`; also `overwrite.rs` lines 164 and 223) | at the sink, on the batch, `cast_with_options` | `insert_by_name`, `insert_overwrite`, `insert_overwrite_values`, `df_insert_into_overwrite`, `df_overwrite_partitions` |
| B | `crates/repark-iceberg/src/write/update_cast.rs` `nested_leaf_cast_sql` (`arrow_cast((expr), '<type>')` text, via `paired_assignment_cast_sql` / `store_assignment_cast_sql`) | in the planned assignment SQL; `arrow_cast` becomes a cast in the optimizer, after the analyzer | `merge_update_whole`, `merge_update_whole_literal`, `merge_update_star`, `update_whole_where`, `update_whole_nowhere`, `update_col_where`, `update_col_nowhere` |
| C | MERGE INSERT: `merge/insert.rs` `insert_stream_checked` → `store_overflow::wrap_store_outputs` (the `__repark_store_pair__` kernel and its cast, added to the analyzed plan) | neither A nor B fires; the conversion is in the wrapped store plan | `merge_insert`, `merge_insert_literal`, `merge_insert_star` |

Two facts shape the fix:
- **Site A has no session zone in scope.** `conform_batch_scoped` / `strict_store_cast` run at
  the sink on a `RecordBatch` with no `ConfigOptions`.
- **`repark-iceberg` depends only on `repark-common`**, so it cannot call `repark-functions`'
  session-aware leaf conversion; sites B and C plan SQL and can name a registered UDF, site A
  cannot.

## The fix shape

One zone-aware store conversion of a nested naive leaf into a nested zoned leaf, threaded through
the write path (the session zone passed to the sink conversion of site A, the same rule at sites
B and C), with the leaf cast's DST-edge behaviour; then ZONE-SEAM-1's refusal retires.

## The cells (U-8 door matrix, 2026-10-22, `ltz_nested`, New York and Kolkata each)

- Refused by ZONE-SEAM-1 until this unit: 134 per zone, of which 37 stored Spark's instant on main
  by the cancellation (19 `ts_struct_cast` and 18 `ts_struct_cast_column` doors) and the
  other 97 were not Spark-equal on main: 79 DIFFERENT and 18 the engine's own error (every NTZ
  source on the store doors: `ntz_literal` 25, `ntz_cast` 21, `ntz_text_cast` 21,
  `ntz_cast_column` 21, struct casts 9). Counts corrected 2026-10-23 (fix round 2): 37 + 79 + 18.
- Raw measurements: the ZONE-SEAM-1 ledger (`task/ledgers/staging/zone-seam-1-ledger.md`, the
  third halt).

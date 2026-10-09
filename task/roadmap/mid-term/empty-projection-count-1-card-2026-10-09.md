# EMPTY-PROJECTION-COUNT-1 — `COUNT(*)` fails with the engine-internal row-count error over three readers

**Status: CLOSED 2026-10-09** by unit EMPTY-PROJECTION-COUNT-1 (branch
`fix/empty-projection-count-1`, ledger
[empty-projection-count-1-ledger.md](../../ledgers/completed/empty-projection-count-1-ledger.md)):
the three repros below answer on the head with Spark-equal counts, and the shared fix also
covers the fourth `conform_batch` caller (the micro-batch provider). The MB-4 branch carries
an open copy of this card; the later merge must reconcile the two (ledger D-1).

**Filed:** 2026-10-09 by the MB-4 lane (round 4 step 7), owed from the round-3c measurement
(MB-4 ledger D-44) and re-measured first-hand on this tree the same day. One card for three
doors with one shared site.

## The three doors (repros verbatim, 2026-10-09)

All three fail with the same engine-internal error; only the wrap differs.

1. The changelog reader. `SELECT COUNT(*) FROM ice.sales.t.changes` over a two-append table:
   `PySparkException: Internal error: iceberg scan could not rebuild batch: Invalid argument
   error: must either specify a row count or at least one column.` The batch read
   `SELECT id, data FROM ice.sales.t.changes` answers its rows on this tree, so the door
   reads and only the empty projection fails.
2. The incremental snapshot-windowed reader. `spark.read.format("iceberg")`
   `.option("start-snapshot-id", ...)`, `.option("end-snapshot-id", ...)` over the same table,
   then `.count()`: `PySparkException: datafusion engine error: Internal error: iceberg scan
   could not rebuild batch: Invalid argument error: must either specify a row count or at
   least one column.`
3. The lineage reader. `SELECT COUNT(*) AS _row_id FROM ice.sales.partdv` over the v3
   position-delete fixture: `PySparkException: Internal error: iceberg scan could not rebuild
   batch: Invalid argument error: must either specify a row count or at least one column.`
   The alias trips the token-based lineage rewrite while the scan projection stays empty;
   `SELECT COUNT(*) FROM ice.sales.partdv WHERE _row_id IS NOT NULL` answers `4` because the
   filter forces a non-empty scan projection.

## The shared site

`crates/repark-iceberg/src/catalog/scan_batches.rs::conform_batch` rebuilds each scanned batch
with `RecordBatch::try_new(schema, columns)`. Under `COUNT(*)` the planner pushes an empty
projection, `columns` is empty, and Arrow refuses a zero-column batch without an explicit row
count; the site wraps that as `iceberg scan could not rebuild batch`. Every caller that can
reach an empty scan projection fails the same way.

## The pattern

The micro-batch provider already fixed its own instance locally:
`crates/repark-iceberg/src/microbatch/provider.rs::zero_column_batch` serves a zero-field
projected schema with `RecordBatch::try_new_with_options` and an explicit row count taken from
the incoming batch. The shared fix is the same shape in `conform_batch`: when the projected
schema has zero fields, rebuild with the incoming batch's row count instead of failing. The
fixing slice audits every `conform_batch` caller for empty-projection reachability and pins
`COUNT(*)` over all three doors above. This card closes when the three repros answer or the
owner declines them.

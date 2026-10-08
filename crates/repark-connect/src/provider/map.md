# map — repark-connect/src/provider

## Purpose

The DataFusion provider of a Postgres source, behind the `postgres` feature: the catalog, schema
and table providers and the scan's execution plan, which `../provider.rs` declares. C-2c
(2026-10-07; sketch [c-2-design.md](../../../../task/wo/c-2-design.md) §2.8–§2.11). See
[../map.md](../map.md).

## Contents

- **C-2d (2026-10-07):** `catalog.rs` adds `PostgresSource::ping()` (checkout, `SELECT 1`
  under `read_timeout_ms`, `release_clean`). `schema.rs` names the source on every resolution
  error (`DataFusionError::Context("database source `<name>`")`) and refuses
  `register_table` / `deregister_table` with `read_only_ddl`. `scan.rs` fuses the exec stream:
  the first error (a placement refusal, a refused resize) drops the inner scan, so its lease
  aborts (cancel, then the connection task) instead of idling inside the read-only transaction
  until the frame is collected; found by the C-2d live timestamp cell, whose teardown waited 60
  s on the held lock. pins: c-2/C-099, C-101, C-102, C-103
- `catalog.rs` — **`PostgresSource`**, one per mounted source: its name, settings door, prop map
  and `WallClockLocaliser`. `PostgresSource::mount(identity, props, localiser)` (the call C-2d's
  `SourceMount` makes) builds it on the `repark.toml` door and returns a **`PostgresCatalog`**
  as an `Arc<dyn CatalogProvider>`; it does no I/O and no validation (FL-1, CFG-2 D-4).
  `PostgresSource::new` takes the door, for `read_postgres`. The first resolution parses the
  props (`PostgresSettings::from_props`), builds the connector and the source's one `QueryPool`,
  and memoises the outcome, a refusal included, so the same refusal repeats. `resolve(ScanSource)`
  runs `discover` afresh every time (FL-7) and builds a `PostgresTable`; `table(ResolvedSource)`
  builds one from a resolution the caller injects, which is how the EXPLAIN and pushdown pins run
  with no network. `PostgresCatalog::schema_names` is empty and `schema(name)` returns a lazy
  schema provider for any name (`CONNECT-DECL-pg-listing`, `LISTING_ROW`). `Debug` shows the
  name and the door, never a prop. pins: c-2/C-069
- `schema.rs` — **`PostgresSchemaProvider`**: `table(name)` quotes the schema and the name
  through `PgIdent` and resolves the relation; `RelationNotFound` answers `Ok(None)`, so the
  door's own table-not-found fires, and every other `ConnectError` travels as
  `DataFusionError::External` (`external`). `table_names` is empty and `table_exist` is `false`
  (`CONNECT-DECL-pg-listing`). pins: c-2/C-069
- `table.rs` — **`PostgresTable`**, the `TableProvider`. Its schema is the resolved columns'
  fields, a `timestamp` column placed in the localiser's zone (`zone_label()`, read once per
  resolution) unless `prefer_timestamp_ntz` is set (`CONNECT-DIV-pg-timestamp-zone`).
  `supports_filters_pushdown` answers each conjunct through `Pushdown::support`: `Exact` or
  `Inexact`, never `Unsupported`, so `scan` sees every conjunct (D-M6). `scan` splits the
  filters again with the same rule, renders the pushed ones into the `ScanRequest`
  (`Pushdown::push`; past 1024 bound values it refuses the plan, naming the bound,
  `pushdown_predicate` and `CONNECT-DECL-pg-bound-values`), pushes `limit` only when `pushdown_limit` is set (the default; Spark's
  `pushDownLimit`, separate from `pushdown_predicate`), no residual conjunct remains (P-11, the
  scan's own re-check) and the limit, DataFusion's `skip + fetch`, fits `i64` (Postgres's `LIMIT` is a
  `bigint`; past it no limit pushes), takes `batch_rows` or else the session batch size under the 64 MiB cap, and
  returns a `PostgresScanExec`. pins: c-2/C-070, C-072, C-074, C-077, C-081, C-082, C-087
- `scan.rs` — **C-2d fold 1 (2026-10-07), N4:** `place_until_refusal` gives the localiser's per-value refusals
  the decoder's rule. When the localiser refuses row `k > 0` of a batch, the exec emits rows
  `0..k`, placed, and then the refusal. The stream still ends at its first error, so the lease
  aborts as before (C-103). pins: c-2/C-114
- `scan.rs` — **C-2d residual (2026-10-07):** gains its first unit-test child, [scan/](scan/map.md),
  for the private `place_until_refusal`. pins: c-2/C-119
- `scan.rs` — **`WallClockLocaliser`** (`localise(&TimestampMicrosecondArray)`, `zone_label()`;
  the trait sits here with no zone dependency, NS-10) and **`PostgresScanExec`**, one partition.
  `DisplayAs` renders, per scan, `PostgresScanExec: source=<name>, relation=<schema.table>` (or
  `query=<sql>`), `projection=[…]`, `pushed_filters=[…]`, `residual_filters=[…]` (the `Expr`
  displays of what this statement pushed and left) and `pushed_limit=`; `Verbose` adds
  `remote_sql=` (the COPY text with its `current_setting('repark.pN')` placeholders) and
  `bound_values=N`, never a value; the tree format gives the same keys a line each. No host,
  port, database, user, URL or prop is ever rendered (CC-1). `execute` builds the
  `scan_metered` stream on first poll (so EXPLAIN opens no connection), places each localised
  column, resizes a `MemoryReservation` (`PostgresScan`) to each batch it yields, so it holds
  the current batch alone and frees it on drop (a refusal is DataFusion's resources-exhausted
  error), and records, through DataFusion's `RecordOutput`, `output_rows`, `output_bytes`
  (`get_record_batch_memory_size` per batch, C-2c fold 1), `output_batches`,
  `elapsed_compute` (the decode), `bytes_received` and `time_to_first_byte` in its
  `MetricsSet`, which `EXPLAIN ANALYZE` shows. Accessors (`pushed_filters`, `residual_filters`,
  `pushed_limit`, `request`) serve the pins. pins: c-2/C-075, C-076, C-077, C-083, C-084

## Pointers

- Up: [../map.md](../map.md)
- The classifier the table calls: [../pushdown.rs](../pushdown.rs)

## Debug

| Symptom | First check |
|---|---|
| A filter you expected pushed shows in `residual_filters` | `Pushdown::class` for the column (§2.9's eligibility), then `Pushdown::render` for the shape; a shape the optimizer would still rewrite stays residual on purpose |
| `FilterExec` above the scan disagrees with `residual_filters` | `explain_residual_matches_filter_exec_above`: a residual must be classed `Inexact`, never `Unsupported`. A `__common_expr_N` in the `FilterExec` is DataFusion's common-subexpression elimination: the two stay semantically equal |
| A `timestamp` column has a zone you did not expect | `prefer_timestamp_ntz`, then the localiser's `zone_label()` |

First checks: `cargo test -p repark-connect --test it explain pushdown`. Escalate to:
[../map.md#debug](../map.md).

# map — repark-spark/src/normalize

## Purpose

Token rewrites and statement guards that `../normalize.rs` calls but does not have the file-size
headroom to hold. `../normalize.rs` sits close to the 1000-line ceiling, so a new helper lands
here and is re-exported in one line.

## Contents

- `replace_table.rs` — **IPI-25 (2026-09-20):** `REPLACE TABLE [AS SELECT]` is Spark's elided
  spelling of `CREATE OR REPLACE TABLE`, and the behaviour behind it already shipped (registry
  `RTAS-OPS-1`). `rewrite_replace_table` inserts `CREATE OR` before the leading `REPLACE`, and it
  runs as the **first** rewrite of `parse_single_normalized` so `strip_create_table_using`,
  `extract_partitioned_by` and `rewrite_create_column_types` — all gated on `is_create_table`,
  which needs `CREATE` as the first keyword — still see the statement. `refuse_missing_replace_target`
  carries the one semantic difference between the two spellings: `REPLACE TABLE` requires the table
  to exist, so the router asks it before dispatching a `CreateTable` statement and a missing target
  answers `[TABLE_OR_VIEW_NOT_FOUND]` / SQLSTATE `42P01`. Without that check the rewrite would
  silently create the table Spark refuses to invent. `replace_table_head` is the shared
  recognizer: it answers the index of a leading unquoted `REPLACE` keyword followed by `TABLE`,
  and both the rewrite and `is_replace_table_sql` read it, so the rewrite and the existence check
  can never disagree about which statements are the elided spelling.
  pins: ipi-21-25-42-small-parser/C-005, C-006, C-007
- `clustered_by.rs` — **IPI-26/27 round 1 (2026-09-20):** `rewrite_clustered_by`
  splices a single-column `CLUSTERED BY (col) INTO n BUCKETS` run on a `CREATE TABLE`
  into `PARTITIONED BY (bucket(n, col))`, before `extract_partitioned_by` consumes it.
  The field name falls out of the existing bucket rule as `{col}_bucket`, which is
  what Spark records (cell `D-X-CLUSTERED-BY`: `[["id_bucket","bucket[4]","id"]]`).
  Multi-column and `SORTED BY` shapes pass through untouched and fail loudly
  downstream; runs at or past the CTAS `AS` boundary are never rewritten. Unit pins
  are inline in the module; the parse-level pin is
  [../tests/ice_ddl_clauses_1.rs](../tests/ice_ddl_clauses_1.rs).
- `create_clauses.rs` — **IPI-26/27 round 2 (2026-09-20):** `extract_create_clauses`
  strips the table `COMMENT` / `LOCATION` clauses from a `CREATE TABLE` before the
  stock parser runs, because sqlparser accepts them only without `TBLPROPERTIES`
  while dbt emits `tblproperties` first. Only unquoted words at paren depth zero
  before the CTAS `AS` match, each followed by a string literal (an optional `=`
  is allowed after `COMMENT` only); column `COMMENT` options, bare table names like
  `location`, and the query pass through. Duplicates strip with last-wins. It also
  holds `strip_create_table_using`, moved here from `../normalize.rs` (comments shed
  in the move) to keep that file at its ceiling. Unit pins are inline in the module;
  the parse-level pins sit beside round 1's in `../tests/ice_ddl_clauses_1.rs`.
- `statement_guard.rs` — **WO-C10 (2026-09-23):** shared multi-statement refusal plus the
  front-door unclosed bracketed-comment guard. The guard tokenizes with `DatabricksDialect`,
  then scans outer unclosed comments while ignoring quoted text and line comments. It keeps an
  unclosed `/*+` hint on its existing path and returns Spark's parser condition otherwise.
  pins: wo-c10/C-001, C-002, C-003

## Pointers

- Up: [../map.md](../map.md). Caller: [../normalize.rs](../normalize.rs) `parse_single_normalized`.
- The behaviour this rewrite reaches: [../create_table.rs](../create_table.rs) `execute_schema_create`
  (column-def `OR REPLACE` → `StagedTableTransaction::begin_replace`, no new snapshot) and
  [../ctas.rs](../ctas.rs) `execute_ctas` (`with_replace_write(true)` → an `overwrite` stamp).
- Pins: [../tests/ctas.rs](../tests/ctas.rs), [../tests/create_table.rs](../tests/create_table.rs),
  [../../../../python/repark/tests/test_ice_small_parser_1.py](../../../../python/repark/tests/test_ice_small_parser_1.py).

## Debug

| Symptom | First check |
|---|---|
| `REPLACE TABLE` reports `Unsupported statement REPLACE` | the rewrite did not fire — `replace_table_head` needs the first two significant tokens to be the unquoted keywords `REPLACE` then `TABLE` |
| `REPLACE TABLE` created a table that did not exist | `refuse_missing_replace_target` was skipped in `../router.rs`; `CREATE OR REPLACE` is allowed to create and the rewrite erases the spelling |
| `USING iceberg` reached the stock parser | the rewrite ran too late; it must precede `is_create_table` in `parse_single_normalized` |
| `CLUSTERED BY` reached the stock parser | `rewrite_clustered_by` only fires on one unquoted column with `INTO n BUCKETS` before the CTAS `AS`; anything else is deliberately left for the loud parse error |

First checks: `cargo test -p repark-spark ctas`. Escalate to: [../map.md#debug](../map.md).

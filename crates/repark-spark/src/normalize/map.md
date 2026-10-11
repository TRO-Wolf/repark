# map — repark-spark/src/normalize

## Purpose

Token rewrites and statement guards that `../normalize.rs` calls but does not have the file-size
headroom to hold. `../normalize.rs` sits close to the 1000-line ceiling, so a new helper lands
here and is re-exported in one line.

## Contents

- `sort_key_projection.rs` — **WO TZ-ASOF-1 (2026-09-26):** `rewrite_statement`
  runs in `../spark_ast.rs` after the ORDER BY null-placement defaults and before the
  lowering rewrites. When a single-`SELECT` query (DISTINCT or not) carries an `ORDER BY`
  key that is not projected — bare and compound identifiers resolved against select
  aliases first, then select-list column references, then (unqualified keys only) the
  select items' Spark display names, with ordinals always projected —
  the statement selects from a derived table (`__repark_sort`) that projects every
  select item under `__repark_sort_sel_<i>` plus every un-projected key under
  `__repark_sort_key_<k>`, while the outer query carries the remapped `ORDER BY`,
  the original `LIMIT`/`OFFSET`/`FETCH`, and one quoted outer alias per select item
  holding Spark's display name (explicit alias verbatim; bare column as written;
  `CAST(col)` → the column; `CAST(<non-column> AS T)` → the full `CAST` text;
  parenthesized binary expressions; lower-cased calls with unqualified arguments).
  The rewrite also fires with no hidden key when a bare key binds a display name
  (verifier V-002: `ORDER BY id` on `CAST(id AS STRING)` sorts the output
  column's strings, matching Spark's output binding).
  DISTINCT stays inside the derived table, but on a `DISTINCT` select the
  rewrite fires only when every key binds a select item — a hidden key inside
  the dedup would duplicate rows (verifier V-003), so `SELECT DISTINCT s …
  ORDER BY ts` keeps main's `must appear in select list` refusal while
  `DISTINCT CAST(ts AS STRING) … ORDER BY ts` answers through the binding. Set operations, stars, `ExprWithAliases`, `SELECT INTO`,
  `TOP`, `EXCLUDE`, `PREWHERE`, `CONNECT BY`, Hive `CLUSTER`/`DISTRIBUTE`/`SORT BY`,
  non-expression orderings, `WITH FILL`, locks, `FOR`/`SETTINGS`/`FORMAT`/pipe
  clauses, unmapped cast targets or operators, wildcard function arguments,
  windowed or filtered calls, subquery/case/predicate select items, ambiguous
  alias matches, out-of-range ordinals, a `__repark_sort` collision with user text,
  any key expression mentioning a select alias, and a compound key whose last
  segment names a select column without a full qualifier-chain match (verifier
  V-001: `st.s` never binds a same-named `s`) all bail with the statement
  untouched, as does the case-sensitive door at the call site. When the rewrite fires and
  the rewritten statement then fails anywhere from lowering through the plan guards,
  `../spark_ast.rs` retries the whole passthrough once with the rewrite disabled and
  returns that outcome, so every failure is byte-identical to today and the rewrite can
  only turn failures into successes (the retry is safe because the rewrite fires only on
  `SELECT` queries, never on `DML`). Inline unit pins:
  key projection, options/limit placement (C-001), the aliased base key (C-002),
  expression outer names (C-003), the aggregate cast and double-cast names (C-006),
  DISTINCT placement (C-005), ordinals, projected keys, the bail set, alias keys
  and marker collisions (C-009), the compound-key clash bail (C-010), the
  display-name output binding plus qualified-key column binding (C-011), and
  the DISTINCT-without-bound-keys bail (C-012).
  pins: tz-asof-1/C-001, C-002, C-003, C-005, C-006, C-009, C-010, C-011, C-012
- `map_ordering.rs` — **WO U9-TYPES-1 r3 (2026-09-25):** `refuse_map_ordering` walks the
  planned statement in `../spark_ast.rs`'s passthrough (SELECT, and the UPDATE / DELETE that land
  there) and refuses a map operand of `=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `IN` and
  `ORDER BY` with Spark's `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE` text, and `SELECT
  DISTINCT` over a map column with `UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE` (verifier
  V-003). A MERGE `ON` and `GROUP BY` do not pass through it (residue R-19).
  **STAMP-2-R5P6-2 (2026-10-07):** `analyze_built_plan(state, plan)` is what the passthrough
  does to a planned SELECT after planning, `refuse_map_ordering` then
  `repark_functions::analyze_eagerly`, exported for the binding's native join door so a join
  built without the SQL planner is refused and analyzed exactly as the statement would be.
  pins: stamp-2-r5p6-2/C-002, C-005
  pins: u9-types-1/C-012
  **CROSS-JOIN-CONDITION-1 fold 2 (2026-10-08):** `render` and `input_schema` are
  `pub(crate)` for the join-condition rule below; `render_literal` renders decimal
  scalars scaled (`0.5`, never DataFusion's `Some(5),1,1` tuple), which no pin had
  depended on. pins: cross-join-condition-1/C-008
- `join_condition.rs` — **CROSS-JOIN-CONDITION-1 fold 2 (2026-10-08):**
  `JoinConditionRefusals`, the Spark door's analyzer rule over `LogicalPlan::Join`
  filters, seated directly before `type_coercion` (after the integral-literal rule,
  so `ON 1` reports `INT`). The type check runs first and the nondeterminism walk
  second, the order live Spark 4.1.2 shows for a condition that is both; the walk
  reads core's `NONDETERMINISTIC_FUNCTION_NAMES` and descends into subquery plans.
  Errors carry Spark's head lines (`INVALID_NON_DETERMINISTIC_EXPRESSIONS`,
  `JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE`, SQLSTATE `42K0E`); only the condition
  rendering is RePark's. The rule fires for every SQL-planned join, so the
  DataFrame door (whose SQL route plans here) and free SQL share the one site;
  the exact-native path carries single-equality keys only and needs no check.
  pins: cross-join-condition-1/C-008
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
  Runs at or past the CTAS `AS` boundary are never rewritten.
  **U7 PR1 (2026-09-24):** the rewrite is fallible and is the `bucketBy` kernel the facade's
  CTAS/RTAS reaches. A `PARTITIONED BY (…)` already in the statement gains the bucket as its
  LAST element (Spark's `partitionBy ++ bucketSpec`); a multi-column run or any `SORTED BY`
  refuses with `IllegalArgumentException` `Cannot convert transform with more than one column
  reference: bucket(n, a, b)` / `sorted_bucket(a, n, s)`, measured on Spark 4.1.2 for CTAS and
  column-def CREATE alike; a `SORTED BY` element carrying `DESC` or a non-number count
  still passes through to the loud parse error. Unit pins in-module.
  **Round 2 (2026-09-24):** every unquoted `CLUSTERED` before the `AS` boundary is tried and
  the first bucket run wins, so a column named `clustered` no longer hides the clause; a
  `SORTED BY` element may carry `ASC` (Spark refuses it as `sorted_bucket(id, 4, x)`); `DESC`
  stays the parse error, a residue against Spark's `_LEGACY_ERROR_TEMP_0035` text.
  pins: u7-write-df/C-013, C-017
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
  WO-C11 (2026-09-23): the inline test module pins `outermost_unclosed_bracketed_comment`,
  `skip_quoted_text` and `skip_line_comment` to exact indexes, so removing any quote kind,
  escape form, line break, nesting push or pop fails a test. The doubled-quote escape and the
  end-of-input escape guard only change `skip_quoted_text`'s resume index.
  pins: wo-c10/C-002

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
| `CLUSTERED BY` reached the stock parser | `rewrite_clustered_by` fires on an identifier list, an optional plain `SORTED BY` list and `INTO n BUCKETS` before the CTAS `AS`; multi-column and sorted runs refuse with Spark's text, other shapes are left for the loud parse error |

First checks: `cargo test -p repark-spark ctas`. Escalate to: [../map.md#debug](../map.md).

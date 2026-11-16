# CAST-LITERAL-UNWRAP-1 — the insertion inventory

Every seat that can move a cast across a comparison, or use a cast to prune, answered by the
insertion-inventory rule: **seen at the seat: yes / no, because**, with file:line and how it was searched.
"Seen" means: the seat sees a `CAST(<column> AS <narrower type>) <op> <constant>` comparison and acts on it.

**Sources read.** DataFusion 54.1.0 (the pin on main `83df6be1`) and 55.2.0, from the local cargo registry
copies only; the iceberg-rust fork at the rev main pins (`b6e578dc`, `Cargo.lock` line 3392), from the local cargo
git checkout; RePark main `83df6be1`; PR #1341's head `d78d669f`, read with `git show` in the builder's lane clone.

**How searched.** `grep -rn` over `crates/` (tests excluded) for `ExprSimplifier`, `SimplifyExpressions`,
`unwrap_cast`, `fn preimage`, `PhysicalExprSimplifier`, `PruningPredicate`, `with_optimizer_rules`,
`add_optimizer_rule`, `Expr::Cast(`, `convert_filters_to_predicate`, `with_filter`; over the DataFusion crates for
the callers of `unwrap_cast_in_comparison*`, `PhysicalExprSimplifier::new` and `rewrite_with_preimage`; and a
read of every hit. The measured column says which cells in [domain-table.md](domain-table.md) exercise the seat.

## A. DataFusion's logical simplifier (the cause)

| # | seat | seen? | why / evidence |
|---|---|---|---|
| A1 | `datafusion-optimizer-54.1.0/src/simplify_expressions/expr_simplifier.rs:1928-1956`, the `unwrap_cast_in_comparison` arm, for `cast <op> literal` and `literal <op> cast` | **yes — the cause** | Fires when `op.supports_propagation()` (`datafusion-expr-common-54.1.0/src/operator.rs:219-235`: `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`) and `is_cast_expr_and_support_unwrap_cast_in_comparison_for_binary` holds. Measured: every operator, both sides. |
| A2 | `unwrap_cast.rs:107-141`, the support test | **yes** | Checks only that the literal converts **exactly** to the column's type (`try_cast_literal_to_type`) and that both types are "supported". It never looks at the cast's own target, so a narrowing column cast passes. |
| A3 | `unwrap_cast.rs:64-105`, the rewrite | **yes** | Replaces `CAST(c AS T) op lit` with `c op lit'` where `lit'` is `lit` in `c`'s type. The repository's own test expects it: `:423-427` turns `CAST(c3 AS DECIMAL(10,0)) < 123` on `DECIMAL(18,2)` into `c3 < 123.00`. |
| A4 | `unwrap_cast.rs:143-185` and `expr_simplifier.rs:1957-1990`, the `IN` list arm | **yes** | Same test per item; `NOT IN` included. Measured: `IN`, `NOT IN`. |
| A5 | `unwrap_cast.rs:197-229`, `cast_literal_to_type_with_op` | yes, narrowly | A string literal against an integer column under `=` / `<>` (`CAST(i AS STRING) = '1'` → `i = 1`), guarded by a round trip. Not a narrowing column cast; not in this unit's wrong set. |
| A6 | `datafusion-expr-common-54.1.0/src/casts.rs:54-101`, `is_supported_type` | **yes — decides the reach** | Supported: integers, `Date32`/`Date64`, `Decimal32/64/128`, `Timestamp`, strings, dictionaries, binary. **Not** `Float32`/`Float64`. So a FLOAT or DOUBLE column, and a cast whose literal is a float, is never unwrapped; a STRING column cast to a number is never unwrapped either (a numeric literal does not convert to a string type, `casts.rs:334-347`). |
| A7 | `casts.rs:77-80`, `is_lossy_temporal_cast`, used at `:133` | **guards** | A `Date` literal against a `Timestamp` column (and back) is refused, so `CAST(ts AS DATE) = DATE'…'` is not unwrapped in 54.1.0. Measured: the temporal family agrees. |
| A8 | `casts.rs:122-330`, `try_cast_numeric_literal` | **yes** | Scales the literal to the column's decimal scale and range-checks it against the column's precision; an inexact or out-of-range literal declines. That is why `CAST(c AS INT) = 1.5`-style constants with digits the column cannot hold stay correct while `= 1` does not. |
| A9 | `expr_simplifier.rs:2008-2075` and `udf_preimage.rs:29-70`, the **preimage** arm | no (no lossy-cast UDF exists yet) | Rewrites `f(x) <op> lit` to an exact range on `x` when `f`'s `preimage` answers; covers every A1 operator and `IN`. This is the hook seat W uses. |
| A10 | `simplify_expressions/simplify_predicates.rs`, `inlist_simplifier.rs`, `linear_aggregates.rs`, `regex.rs`, `simplify_literal.rs` | no | Read in full: none moves a cast across a comparison. |

**DataFusion 55.2.0 (and 55.0.0).** The same code for every numeric and decimal narrowing. The only change is two
new guards, `is_timestamp_precision_narrowing_cast` and `is_date_narrowing_cast` (`Date64` → `Date32`), applied in
the logical and the physical unwrap, plus exact `Date32`/`Date64` scaling. A DataFusion upgrade does not fix this
unit. The non-test diffs are in [evidence/raw/datafusion-54.1.0-vs-55.2.0.diff](evidence/raw/datafusion-54.1.0-vs-55.2.0.diff).

## B. DataFusion's physical simplifier and pruning

| # | seat | seen? | why / evidence |
|---|---|---|---|
| B1 | `datafusion-physical-expr-54.1.0/src/simplifier/unwrap_cast.rs:45-140`, run by `PhysicalExprSimplifier::simplify` (`simplifier/mod.rs:69-72`) | **yes, latent** | The physical twin of A1-A3, with no type gate beyond `try_cast_literal_to_type`. Today the logical rule has already unwrapped, so it acts on nothing new; **once a fix keeps a `CastExpr` in the physical predicate, this seat unwraps it again.** |
| B2 | `datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:841-856` | **yes, latent** | Each Parquet file's predicate goes through the schema adapter and B1 before row-group and page pruning. A surviving narrowing `CastExpr` would be unwrapped here and prune by the wrong bounds. |
| B3 | `datafusion-datasource-parquet-54.1.0/src/row_group_filter.rs:360-375` | **yes, latent** | The inverted predicate for fully-matched row groups is simplified by B1 too. |
| B4 | `datafusion-physical-expr-adapter-54.1.0/src/schema_rewriter.rs:580-592` | **yes, latent** | Batch adapters simplify their projections with B1. |
| B5 | `datafusion-pruning-54.1.0/src/pruning_predicate.rs:465-500` | **yes, latent** | `PruningPredicate::try_new` runs B1 before and after building the statistics predicate; `CAST(c_min AS INT) <= 1` would become `c_min <= 1.00`, pruning a row group whose minimum is 1.25. |
| B6 | `pruning_predicate.rs:1104-1150`, `rewrite_expr_to_prunable` for `CastExpr`/`TryCastExpr`, and `:1210-1239`, `verify_support_type_for_prune` | yes, sound | Pushes the cast onto the statistics (`CAST(min)`, `CAST(max)`); correct for a monotone cast, refused for string↔non-string. Same in 55.2.0. Sound on its own; B5 is what breaks it. |
| B7 | Parquet `pushdown_filters` (row-level filter) | no | Default `false` (`datafusion-common-54.1.0/src/config.rs:807`) and no RePark site sets it (searched `pushdown_filters`). |
| B8 | `crates/repark-core/src/spark_nullable.rs:199-205`, the Parquet format wrapper | no | Makes float column bounds inexact; does not touch casts. |

## C. The Iceberg fork

| # | seat | seen? | why / evidence |
|---|---|---|---|
| C1 | `crates/integrations/datafusion/src/physical_plan/expr_to_predicate.rs:245-268` (`Expr::Cast` arm) at `b6e578dc` | **no — sound** | A cast of a literal is folded; a cast of a column is stripped only when `cast_strips_lossless(source, target)` holds, else the conjunct is `NotTransformed` (kept as a residual, no pruning). Same lines Fable read; the pin on main is the same rev. |
| C2 | `expr_to_predicate.rs:382-419`, `cast_strips_lossless` | **no — sound** | Integer widening, small integers and `Float32` to `Float64`, string↔string, same-unit timestamps, widening times. No decimal, no narrowing pair. |
| C3 | `expr_to_predicate.rs:321-323` and `:470`, scalar functions | no | Only `isnan` is translated; any other UDF (seat W's wrapper) is `NotTransformed`, a residual. |
| C4 | `crates/integrations/datafusion/src/table/mod.rs:241`, `supports_filters_pushdown` | — | Every filter is `Inexact`, so DataFusion keeps the `Filter` above the scan. A pruning miss costs speed, never rows. |
| C5 | What the fork receives today | **yes, downstream** | It receives the **already unwrapped** filter (`c = 1.00`), turns it into an exact Iceberg predicate and prunes files and manifests by it; measured: the partitioned table gives the same wrong rows as the temp view. |

## D. RePark's own seats

| # | seat | seen? | why / evidence |
|---|---|---|---|
| D1 | `crates/repark-core/src/session/df_guards.rs:217-247`, `unnest_safe_optimizer_rules` | **yes — the runner** | Takes DataFusion's recommended list (`Optimizer::new()`, `datafusion-optimizer-54.1.0/src/optimizer.rs:285` for `SimplifyExpressions`) and keeps `simplify_expressions` in place, between two signed-zero literal seats (`:233-237`). Up to three passes (`optimizer.max_passes`, default 3). Shared by every door: the session build at `crates/repark-core/src/session.rs:319` uses it for all of them. |
| D2 | `crates/repark-core/src/session/df_guards/signed_zero/literals.rs:44-62`, `SignedZeroLiterals` | **yes, again** | Builds its own `ExprSimplifier` (`:49-55`) to fold operands and, when it changed the plan, runs `SimplifyExpressions::default().rewrite` over the whole plan (`:60`): a second unwrap pass a fix must survive. |
| D3 | `crates/repark-connect/src/pushdown.rs:165-184` | yes, as a check only | Runs `ExprSimplifier::simplify` to test that a filter is already simplified before rendering it for Postgres; an unwrappable cast makes the test fail and the filter stays local. No rewrite reaches the plan. |
| D4 | `crates/repark-functions/src/bool_decimal.rs:492-527`, `DecimalToDouble::preimage` (#1339) | no (other cast) | The one `preimage` user on main: `__repark_decimal_to_double__(d) <op> <double>` → an exact decimal range. Seat W reuses the mechanism. |
| D5 | `crates/repark-iceberg/src/write/meta_delete.rs:171-336` (metadata `DELETE`) | **no** | Reads the SQL text; `column_name` (`:314-336`) accepts a bare or `alias.` identifier only, so a `CAST(...)` conjunct makes the statement fall back to the row path. |
| D6 | `crates/repark-iceberg/src/write/conflict_filter.rs:82-262` (commit conflict filters) | **no** | Same shape: `column` (`:250-262`) accepts identifiers only; a cast conjunct widens the filter to "always", which is safe. |
| D7 | `crates/repark-iceberg/src/write/scan_prune.rs:30-160` (MERGE target pruning) | **no** | `parse_bare_equality` (`:129-160`) needs `alias.col = alias.col`; a cast-against-constant conjunct contributes no bound. |
| D8 | The Iceberg DML row paths (`write/predicate_dml.rs`, `write/merge/`) | **yes, downstream** | They plan the `WHERE` / `ON` / `WHEN MATCHED AND` predicate through the session optimizer (D1), so A1 rewrites it before rows are chosen. Measured: wrong `DELETE`, `UPDATE` and `MERGE` writes, copy-on-write and merge-on-read alike. |
| D9 | The DataFrame door (`col.cast(...)`, `isin`, `between`), `F.expr`, `filter(str)` | **yes, downstream** | Each builds an `Expr::Cast` that reaches D1 unchanged. Measured. |
| D10 | The native door `repark.sql()` | **yes, downstream** | The same session build (D1) with the `NativeDoorTypes` extension instead of the Spark one (`crates/repark-python/src/session.rs:151-166`). Measured on a view; it cannot read a local Parquet path or catalog by design (`repark.sql()` refuses local-file DDL). |
| D11 | Spark-door analyzer rules that rewrite casts (`crates/repark-spark/src/spark_typed.rs:22-40`, `FoldSparkNumericCasts`; `spark_literal_typing.rs`) | no | They type literals and fold literal casts; none moves a column cast. |

## E. PR #1341's `DecimalInRanges` (head `d78d669f`)

| # | seat | seen? | why / evidence |
|---|---|---|---|
| E1 | `crates/repark-spark/src/extension/decimal_float_widen.rs:748-790` at `d78d669f`, installed by `ctx.add_optimizer_rule` (`extension.rs:181`), so it runs at the end of each optimizer pass, after `push_down_filter` | **no** | It matches only `__repark_decimal_to_double__(d) [NOT] IN (<Float64 literals>)` (`is_decimal_in`, `:795-806`). A plain `CAST` never matches. |
| E2 | Interplay | — | (1) Both #1341 and seat W use a UDF whose `preimage` gives the range; W's wrapper must not use the `__repark_decimal_to_double__` name, and E1's matcher must not be widened to it. (2) An `IN` list over W's wrapper goes through DataFusion's own preimage `IN` arm (A9), not E1. (3) Seat V (#1341's `VALUES` marker) emits casts of literals, not of columns, so W does not wrap them. No file of #1341 is touched by either seat. |

## F. What no seat covers today

- A narrowing **column** cast compared with a constant, on every door, in every position that holds an expression
  (`WHERE`, select list, `HAVING`, `CASE WHEN`, `JOIN … ON`, DML predicates), whenever the column type is an
  integer, a decimal or a date/timestamp-free supported type (A6) and the literal converts exactly to it.
- The seat that must hold the fix sits **before the first `simplify_expressions`** and must also survive D2's
  second pass and B1-B5 at execution. Seat W does that by making the cast invisible (a function, not a `Cast`).

Up: [map.md](map.md).

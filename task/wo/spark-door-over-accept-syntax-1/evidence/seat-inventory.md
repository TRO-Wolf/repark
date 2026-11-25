# SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: seat inventory

Line numbers are on main `14d2404a6`. Every seat is a token-level rewrite in the Spark door's
canonicalize layer, `crates/repark-spark/src/spark_literals.rs:278` `canonical_rewrite`,
called by `canonicalize_verbatim` (`spark_literals.rs:108`) behind a substring fast path
(`spark_literals.rs:109-121`).

## The doors that reach canonicalize

| Door | Entry | Reaches |
|---|---|---|
| `spark.sql` (statements, temp-view bodies, `DELETE` / `UPDATE` / `MERGE`) | `crates/repark-spark/src/router.rs:118` | every seat |
| internal facade SQL (`sql_built`: sampling, ML) | `crates/repark-core/src/session/write_options.rs:66`, same router | every seat |
| `F.expr` | `crates/repark-python/src/column/mod.rs:43` | expression seats (exponent, struct, wildcard; DELETE and DROP only when a fragment starts with the word) |
| `selectExpr`, `filter(str)` | `crates/repark-python/src/column/expr_build.rs:51`, `:140` | same as `F.expr` |
| native `repark.sql()` | the native session; no canonicalize | none (control) |

## The seats

| Seat | Path:line (main) | Why it exists (card or order that added it) | Over-accept |
|---|---|---|---|
| Exponent-suffix fallback | `crates/repark-spark/src/spark_rewrites/mod.rs:45-73` (`plan_suffix_regions` exponent arm; `suffix_target(…).unwrap_or("DOUBLE")` at `:59`); `adjacent_suffix` at `:101` skips one period | FNP-4B (#611, 2026-09-15) A-2 / C-010: exponent literals are DOUBLE; C-004: typed suffixes `D`/`F`/`BD`; C-013: `1e3L` is an identifier ([ledger](../../../ledgers/archive/2026-09/2026-09-15-fnp-4b-ledger.md)) | any word that touches an exponent literal, or follows it after a period, was read as the `DOUBLE` suffix and dropped: `1e2foo`, `1e2Dx`, `1e2.foo`, `1e2.D`, `1e2.BD`, `1.5e10foo` |
| Struct-field splice | `crates/repark-spark/src/spark_rewrites/struct_field.rs:9` (`plan_struct_field_regions`), extraction at `:44-55`, `extract_named_struct_span` at `:141` | FNP-4B C-014 / C-016: `named_struct('a', 1).a` is non-null with Spark's display; DataFusion cannot plan `.field` on a call result | the extraction kept only the selected value, so siblings were never resolved, type-checked or folded: `UNRESOLVED_COLUMN`, `WRONG_NUM_ARGS`, `CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO` all answered `1` |
| Wildcard-except splice | `crates/repark-spark/src/spark_rewrites/mod.rs:497` (`plan_wildcard_except_regions`); guard `spark_literals.rs:134`; call `:295` | FNP-4B: after the Databricks dialect switch, the facade's own `SELECT * EXCLUDE (…)` (sampling, ML tuning, ML feature) stopped parsing, so `EXCLUDE` was spelled `EXCEPT` | user `* EXCLUDE (…)`, which Spark's parser refuses, answered |
| `DELETE` region planner | `crates/repark-spark/src/spark_rewrites/mod.rs:407` (`plan_delete_from_regions`); guard `spark_literals.rs:143`; call `:293` | FNP-4B C-017: Rust tests written under the Generic dialect used FROM-less `DELETE t WHERE`; G3-E8 (2026-08-11) pinned that spelling as an adjacent negative ("must keep executing", [ledger](../../../ledgers/archive/2026-08/2026-08-11-g3e8-guard-ledger.md) row 82) while it also recorded "Spark rejects the spelling" | `DELETE <table> [WHERE …]` inserted `FROM` and **deleted rows** (no `WHERE`: every row) where Spark raises `PARSE_SYNTAX_ERROR … missing 'FROM'` |
| `DROP TEMPORARY` region planner | `crates/repark-spark/src/spark_rewrites/mod.rs:464` (`plan_drop_temporary_regions`), run by `plan_keyword_regions` `:450`; guard `spark_literals.rs:138` | FNP-4B slice 2: pyspark's `temp_func` emits `DROP TEMPORARY FUNCTION [IF EXISTS]`, which the Databricks lexer rejects | it dropped every `TEMPORARY` word that follows `DROP` anywhere: `SELECT drop temporary …` lost its alias; `DROP TEMPORARY VIEW` / `TABLE`, which Spark refuses, answered |

## What changed at each seat (head)

| Seat | Head | Path (head) |
|---|---|---|
| Exponent fallback | a bare non-suffix word that touches the literal makes one identifier (as Spark's lexer reads `1e2foo`); a period after the literal stays a field access; a quoted word stays an alias | `spark_rewrites/mod.rs` `plan_suffix_regions`, `AdjacentSuffix::via_period`, `is_bare_word` |
| Struct splice | the extraction passes every sibling to `__repark_spark_as__`; its `simplify` keeps constant siblings (evaluated, so they raise) and drops siblings that read a column, a subquery or a volatile call (Spark prunes them) | `spark_rewrites/struct_field.rs`, `spark_typed.rs` `SparkAs` |
| Wildcard splice | `refuse_wildcard_exclude` raises Spark's `PARSE_SYNTAX_ERROR` at `'('`; the facade's internal SQL spells Spark's `* EXCEPT (…)` | `spark_rewrites/mod.rs`; `sampling.py`, `ml/tuning.py`, `ml/feature/_transformers.py` |
| `DELETE` planner | `refuse_fromless_delete` raises Spark's `PARSE_SYNTAX_ERROR … missing 'FROM'` before any write | `spark_rewrites/mod.rs` |
| `DROP TEMPORARY` planner | only a statement-start `DROP TEMPORARY FUNCTION` drops the word; `DROP TEMPORARY VIEW` / `TABLE` raise Spark's `PARSE_SYNTAX_ERROR`; `VARIABLE` / `VAR` stay as written | `spark_rewrites/mod.rs` `plan_drop_temporary_regions` |

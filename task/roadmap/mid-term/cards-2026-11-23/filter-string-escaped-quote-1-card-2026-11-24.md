# Card FILTER-STRING-ESCAPED-QUOTE-1: a backslash-escaped quote in a `DataFrame.filter` string drops rows

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review of the SQL text
rewrites and a measurement here. **Severity:** S1 (owner, 2026-11-24): a silent wrong answer. **Status:** fixed on
`fix/filter-string-escaped-quote-1` (2026-11-24). Evidence:
[task/wo/filter-string-escaped-quote-1/evidence/summary.md](../../../wo/filter-string-escaped-quote-1/evidence/summary.md).

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2, in both ANSI modes, with the same answers in each.
`df` is `SELECT 1 AS id, 'it''s' AS s UNION ALL SELECT 2, 'ok'`.

| Call | Spark 4.1.2 | RePark |
|---|---|---|
| `df.filter("s = 'it\\'s' AND id = 1").select("id")` | 1 | **no rows** |
| `df.filter("s = 'it\\'s'").select("id")` | 1 | **no rows** |
| `df.filter("s = 'it''s' AND id = 1").select("id")` (doubled quote) | 1 | 1 |
| `spark.sql("… WHERE s = 'it\\'s'")` (the same predicate in SQL) | 1 | 1 |

Only the `filter(str)` door is wrong. SQL with the same predicate answers correctly.

## Resolution (2026-11-24)

- Shared Spark lexical span helpers (`_iter_spark_sql_spans`, `_quoted_span_closed`, `_quoted_span_end`,
  `_is_string_prefix_at`, `_spark_line_comment_end`, nested `_spark_block_comment_end`) and context-aware
  `_boolean_operator_spans` live in `python/repark/src/repark/spark/filter_quote.py` and serve every Python SQL text
  scanner (`column_fields.py`, `qualified_names.py`, `merge_aliases.py`, `_idents.py`, `udtf.py`,
  `dataframe/join_attr_tokens.py`, and `session/`).
- Matching Rust text scanners (`crates/repark-spark/src/collation.rs`, `normalize/statement_guard.rs`,
  `uuid_cast.rs`, `describe_column.rs`, `show_table_extended.rs`; `crates/repark-core/src/session/df_guards/`
  `self_join.rs` and `case_bind.rs`; `crates/repark-python/src/column/expr_build.rs`) apply the same Spark 4.1.2
  boundaries (`\` escapes inside `'…'` and `"…"`, none inside backticks or `r`/`R`/`x`/`X` raw/hex literals,
  nested `/* … */` comments, newline termination before wrapper `)` / `FROM`).
- Measured against Spark 4.1.2 in both ANSI modes: 313/338 domain cells equal Spark per ANSI mode (up from 254/338,
  +59 fixed per mode, 0 regressions) and 27/29 extra edge-case cells equal Spark per ANSI mode (up from 11/29, +16
  fixed per mode, 0 regressions). Facade pins:
  `python/repark/tests/dataframe/test_filter_string_escaped_quote_1.py` (167 red / 30 control green on main; 197/197
  green on head).

## Still unlike Spark

Pre-existing gaps measured across doors (including `spark.sql`), outside the quoted-span scanner scope:

1. **`spark.sql.ansi.doubleQuotedIdentifiers=true`** — refused loudly with `UnsupportedOperationException` across all
   RePark doors (8 cells per ANSI mode).
2. **Runtime `spark.conf.set("spark.sql.parser.escapedStringLiterals", "true")`** — builder-time
   `ReparkSession.builder.config("spark.sql.parser.escapedStringLiterals", "true")` matches Spark on every door
   (pinned), while runtime `spark.conf.set` does not yet update the Rust config carrier across any door including
   `spark.sql` (16 cells per ANSI mode).
3. **Adjacent raw + escaped literal (`r'it''s\'x'`)** — Spark lexes `r'it'` followed by `'s\'x'` (`its'x`); RePark's
   Rust `spark_literals.rs` fails with `ParseException` on `spark.sql` and `filter` alike (2 cells per ANSI mode).
4. **Unaliased `spark.sql` predicate display column name** — `spark.sql("SELECT t.id, t.s = 'it\\'t.s' FROM tbl t")`
   names the column `t.s = Utf8("it't.s")` (DataFusion formatting) where Spark names it `(s = it't.s)`. `selectExpr`
   matches Spark's `(s = it't.s)` (1 cell per ANSI mode).

Up: [map.md](map.md).

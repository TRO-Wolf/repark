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

## Resolution (2026-11-24; Delta 1)

- Shared Spark lexical span helpers (`_iter_spark_sql_spans`, `_quoted_span_closed`, `_quoted_span_end`,
  `_is_string_prefix_at`, `_spark_line_comment_end`, nested `_spark_block_comment_end`, `_skip_spark_sql_trivia`) and
  context-aware `_boolean_operator_spans` live in `python/repark/src/repark/spark/filter_quote.py` and serve
  `column_fields.py`, `qualified_names.py`, `merge_aliases.py`, `_idents.py`, `udtf.py` (comment/leading-trivia
  scanning only; literal escape decoding remains on main), `dataframe/join_attr_tokens.py`, and `session/`
  (`session_core.py`, `sql_relations.py`, `sql_udf_parsing.py`, `sql_udf_discovery.py` including both `'…'` and `"…"`
  simple args). `_select_expr_frame` joins expressions with `"\n, "` before `"\nFROM "` so a trailing `--` comment on
  any item never comments out the comma or `FROM`. `sql_set_statements.py` and `sql_cache_statements.py` keep main's
  non-backslash grammar because Spark's `SET`/`RESET` and `REFRESH 'path'` parsers do not lex RHS values as
  backslash-escaped SQL strings.
- Matching Rust text scanners (`crates/repark-spark/src/collation.rs`, `normalize/statement_guard.rs`,
  `uuid_cast.rs`, `describe_column.rs` advancing by `char::len_utf8`, `show_table_extended.rs`;
  `crates/repark-core/src/session/df_guards/self_join.rs` `parse_attr_refs` and `case_bind.rs` `sql_text_tokens`;
  `crates/repark-python/src/column/expr_build.rs`) apply the same Spark 4.1.2 boundaries (`\` escapes inside `'…'`
  and `"…"`, none inside backticks or `r`/`R`/`x`/`X` raw/hex literals, nested `/* … */` comments, newline
  termination before wrapper `)`).
- Measured against Spark 4.1.2 in both ANSI modes: 313/338 core domain cells equal Spark per mode (up from 254/338,
  +59 fixed/mode, 0 regressions), 27/29 extra edge-case cells equal Spark per mode (up from 11/29, +16 fixed/mode, 0
  regressions), and verifier attack probes improve to 218/249 (`attack`), 19/40 (`deep`), and 13/28 (`repro2`) per
  mode with 0 regressions vs main. Facade pins:
  `python/repark/tests/dataframe/test_filter_string_escaped_quote_1.py` (257/257 green on head; 48/48 Delta 1 pins
  red on pre-delta merged head `755c6e6e`; inner + `left`/`right`/`full` USING-join pins red on main).

## Still unlike Spark

Measured gaps remaining on head (all pre-existing on main or reverted to main under Delta 1):

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
5. **Python UDTF scalar string literal escape decoding (A-3, reverted to main)** — `udtf._split_sql_literal_args`
   does not decode Spark backslash escapes or raw `r'…'` literals: `SELECT * FROM echo_udtf('a\nb')` returns
   `'a\\nb'` (Spark: `'a\nb'`), `'\u0041'` returns `'\\u0041'` (Spark: `'A'`), `'it\'s'` / `"a\"b"` / `r'a\nb'` raise
   `UnsupportedOperationException` (Spark returns rows). Nested leading block comments (`/* a /* it's */ b */ SELECT
   * FROM echo_udtf('ok')`) are fixed.
6. **`filter(str)` with a column named `not` — unary `NOT -id` / `NOT +id`, bare `not NOT IN`, and bare 3-token
   `not <AND|OR> <word>` (A-4, present on main / refused rather than wrong)** — `kw_df.filter("NOT -id = -1")` and
   `kw_df.filter("NOT +id = 1")` evaluate `` `not` - `id` = -1 `` (`[[1], [2]]`) and `` `not` + `id` = 1 `` (`[[1]]`)
   where Spark evaluates `NOT (-id = -1)` / `NOT (+id = 1)` (`[[2]]`, present on main; DataFusion also fails unary
   `+` in `F.expr("NOT +id = 1")`). Bare `kw_df.filter("not NOT IN (1, 2)")` and `NOT not IN (1, 2)` raise
   `AnalysisException: [UNRESOLVED_ROUTINE] IN` rather than returning a silent wrong answer (Spark evaluates
   `NOT NOT 1 IN (2)` -> `[]`; backticked `` `not` NOT IN (1, 2) `` matches Spark `[[1]]`). Bare 3-token
   `bkw_df.filter("not OR and")`, `not AND or`, `not OR not`, and `not AND not` raise `ParseException` matching main
   (Spark parses the trailing word as an alias, e.g. `Filter NOT OR`).
7. **Aliased `join` with `F.expr` containing `\'` or trailing `--` comments (A-5, present on main)** —
   `l.alias("l").join(r.alias("r"), F.expr(r"l.id = r.id AND l.s = 'it\'s'"))` and `(l["id"] == r["id"]) & F.expr(...)`
   fail (`AnalysisException` / `ParseException`) because `crates/repark-core/src/session/df_guards/self_join.rs`
   `parse_condition` parses with `DatabricksDialect` (Spark returns `[[1]]`).
8. **`SET` / `RESET` comment-stripping edges (A-2, reverted to main)** — `SET spark.test.p1 = 'C:\path\'` and
   `SET spark.test.p2 = "C:\path\"` match Spark on main and head, and `SET … 'it\'s; still value'` /
   `RESET … /* a /* it's */ b */` refuse as on main; `SET k = 'a\' -- c'` strips `-- c` (`'a\'` vs Spark `'a\' -- c'`)
   and `SET k = val /* a /* b */ c */` strips one comment level (`val  c */` vs Spark `val /* a /* b */ c */`),
   matching main.
9. **Spill SQL leading-comment strip (C-2, present on main)** — `crates/repark-core/src/session/spill.rs:326`
   `strip_leading_sql_comments` uses non-nested `after.find("*/")`.

Up: [map.md](map.md).

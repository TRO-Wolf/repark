# task/wo/filter-string-escaped-quote-1/evidence/summary.md — FILTER-STRING-ESCAPED-QUOTE-1 measurement evidence

## Provenance

- **RePark:** `main` at `01c15ffa2` and branch `fix/filter-string-escaped-quote-1` head (`5327f5cb0`), release wheels built from each commit.
- **Spark:** live PySpark 4.1.2 (`local[2]`, Java 17), both ANSI modes (`spark.sql.ansi.enabled=true` and `false`), plus `spark.sql.parser.escapedStringLiterals=true` and `spark.sql.ansi.doubleQuotedIdentifiers=true`.
- **Scripts:** [measure_lexer.py](measure_lexer.py) (Spark 4.1.2 lexer rules), [probe_domain.py](probe_domain.py) (338 cells per ANSI mode across all 7 doors), [probe_extra.py](probe_extra.py) (29 edge-case cells per ANSI mode).
- **Full cell-by-cell table:** [domain-table.md](domain-table.md).

## Spark 4.1.2 measured lexer rules

Measured directly on live Spark 4.1.2 via [measure_lexer.py](measure_lexer.py):

1. **Single- and double-quoted literals (`'…'`, `"…"`):** a backslash `\` skips the next character (`\'`, `\"`, `\\`, `\n`); a doubled quote (`''`, `""`) skips both characters. The scan boundary is identical in default mode, under `spark.sql.parser.escapedStringLiterals=true` (which only suppresses post-lex unescaping), and under `spark.sql.ansi.doubleQuotedIdentifiers=true` (`"a\"` is `ParseException`; `"a\"b"` matches column `` `a\"b` ``; `"a""b"` matches column `` `a"b` ``).
2. **Raw and hex prefixed literals (`r'…'`, `R'…'`, `r"…"`, `R"…"`, `x'…'`, `X'…'`):** neither backslash escape nor doubled-quote escape applies. `r'a\'` closes cleanly at `'` with value `a\`; `r'it''s'` lexes as `r'it'` followed by `'s'` (`its`); `r'it''s\'x'` lexes as `r'it'` followed by `'s\'x'` (`its'x`).
3. **Backtick identifiers (`` `…` ``):** no backslash escape (`` `a\` `` closes cleanly at `` ` `` with column name `a\`; `` `a\`b` `` is `ParseException`); doubled `` `` `` escapes `` ` ``.
4. **Comments:** `-- …` runs to `\n`, `\r`, or EOF; `/* … */` **nests** (`/* outer /* it's */ inner */` is valid; `/* outer /* inner */ 1` is `ParseException`). Quotes inside comments never open literals.
5. **Adjacent string literals:** `'a' 'b'`, `"a" "b"`, `'a' "b"`, `'it\'s' ' ok'`, across newlines or comments concatenate to one string.

## Seat inventory (Python and Rust)

All defective SQL text scanners identified and fixed in this unit:

| Seat | Doors | Defect on main | Fix |
|---|---|---|---|
| `python/repark/src/repark/spark/filter_quote.py` | `filter(str)`, `where(str)`, `selectExpr` | `_quoted_span_end` and `re.split` missed `\'`, `\"`, raw/hex prefixes, backticks, `--`/nested `/* */`; `_select_expr_frame` joined items without `\n` before `FROM`; `_bind_filter_token` stole `AND`/`OR`/`NOT` operators when columns carried those names | Shared Spark lexical helpers (`_iter_spark_sql_spans`, `_quoted_span_closed`, `_quoted_span_end`, `_is_string_prefix_at`, `_spark_line_comment_end`, nested `_spark_block_comment_end`), context-aware `_boolean_operator_spans`, and newline-safe `SELECT …\nFROM` assembly |
| `python/repark/src/repark/spark/column_fields.py` | `filter(str)`, `where(str)` | `re.split` broke on `\'`, `\"`, comments, raw/hex literals, and keyword-named columns | Wired `_quote_filter_sql_identifiers` to `_iter_spark_sql_spans` and `_boolean_operator_spans` |
| `python/repark/src/repark/spark/qualified_names.py` | `filter(str)`, `where(str)`, `selectExpr`, `select`, `join` (USING frames) | `_SINGLE_QUOTED_RE`/`_DOUBLE_QUOTED_RE` missed `\'`, `\"`, raw strings, and comments; `generated=True` skipped no single-quoted literals | Wired `_refuse_using_key_text` to `_iter_spark_sql_spans` |
| `python/repark/src/repark/spark/merge_aliases.py` | `DataFrame.mergeInto` | `_STRING_LITERAL` missed `\'`, `"…"`, raw strings, comments | Wired `_qualifiers` to `_iter_spark_sql_spans` (retaining `"target".col` qualifiers followed by `.`) |
| `python/repark/src/repark/spark/_idents.py` | `spark.sql` `DROP TABLE/VIEW` | `strip_sql_comments` missed nested `/* */`, raw strings, doubled quotes | Updated to Spark boundary rules |
| `python/repark/src/repark/spark/dataframe/join_attr_tokens.py` | `filter(Column)`, `select`, `join` on display-overlay frames | `_rewrite_attr_tokens_local` rewrote inside literals/comments; `({expr_sql})` broke on trailing `--` | Wired to `_iter_spark_sql_spans` and inserts `\n` before `)` when `--` appears |
| `python/repark/src/repark/spark/session/session_core.py` | `spark.sql` (`SELECT`/`WITH`/`INSERT`/`CTAS`/`UPDATE`/`DELETE`/`MERGE`) | `_expand_from_join_region` missed `\'`, `\"`, `''`, raw strings, nested comments (silent S1 wrong answer on `FROM` after `\'`) | Wired to shared `filter_quote` helpers (`session_core.py` ratchets 2274 -> 2268) |
| `python/repark/src/repark/spark/session/sql_relations.py`, `sql_udf_parsing.py`, `sql_udf_discovery.py`, `sql_set_statements.py`, `sql_cache_statements.py`, `udtf.py` | `spark.sql` (`SET`, `CACHE`/`REFRESH`, `INSERT REPLACE WHERE`, Python UDFs/UDTFs) | Missed `\'`, `\"`, raw strings, backticks, nested comments | Wired to shared `filter_quote` helpers (`_scan_quoted_path` and `_split_sql_literal_args` skip `\` in quotes) |
| `crates/repark-spark/src/collation.rs` | `filter(str)`, `where(str)`, `F.expr`, `spark.sql` | `blank_sql_quotes_and_comments` missed `\'`, `\"`, raw strings, nested comments (false `UnsupportedOperationException` on `'it\'s STRING COLLATE utf8_lcase'`); `SELECT ({sql})` lacked `\n` before `)` | Updated scanner to Spark boundaries and `SELECT ({sql}\n)` |
| `crates/repark-python/src/column/expr_build.rs` | `F.expr` (`filter`, `withColumn`, `join`, `select`) | `SELECT ({canonical}) AS _repark_expr` broke trailing `--` comments | Changed to `SELECT ({canonical}\n) AS _repark_expr` |
| `crates/repark-spark/src/normalize/statement_guard.rs` | `spark.sql` unclosed-comment guard | `skip_quoted_text` treated `\` as escape in backticks and raw literals | Scoped `\` escape to `'`/`"` only; skip raw/hex literals without escapes |
| `crates/repark-spark/src/uuid_cast.rs`, `describe_column.rs`, `show_table_extended.rs` | UUID cast refusal, `DESCRIBE`, `SHOW TABLE EXTENDED` | Missed `\'`, `\"`, raw strings, backticks, nested comments | Aligned with Spark boundary rules |
| `crates/repark-core/src/session/df_guards/self_join.rs`, `case_bind.rs` | `join`, `filter(str)`, `selectExpr` on joined/duplicate frames | Missed comments/raw literals (`self_join.rs`) and strings/comments (`case_bind.rs`) | Aligned with Spark boundary rules |

Non-defective seats left unchanged: `crates/repark-spark/src/spark_literals.rs` and `spark_rewrites/` (use `sqlparser` `SparkLexDialect`), and `crates/repark-sql/src/scan.rs` (native ANSI SQL door, where `\` is not an escape).

## Domain table summary

| Suite | ANSI mode | Total | Equal on main | Equal on head | Fixed | Regressed | Moved toward Spark | Remaining pre-existing diffs |
|---|---|---|---|---|---|---|---|---|
| Core (`probe_domain.py`) | `ansi=true` | 338 | 254 | 313 | +59 | 0 | +1 | 25 |
| Core (`probe_domain.py`) | `ansi=false` | 338 | 254 | 313 | +59 | 0 | +1 | 25 |
| Extra (`probe_extra.py`) | `ansi=true` | 29 | 11 | 27 | +16 | 0 | 0 | 2 |
| Extra (`probe_extra.py`) | `ansi=false` | 29 | 11 | 27 | +16 | 0 | 0 | 2 |
| **Total** | **Both** | **734** | **530** | **680** | **+150** | **0** | **+2** | **54** |

- The `+1` moved-toward-Spark cell per ANSI mode (`esc_true_filter_str::esc_true_sq_bs_quote`) moved from `[]` (silent literal corruption on main) to `[[1]]` (matching `spark.sql`, leaving only the pre-existing runtime `spark.conf.set("spark.sql.parser.escapedStringLiterals", "true")` gap). Builder-time `.config("spark.sql.parser.escapedStringLiterals", "true")` equals Spark on all 6 cells per ANSI mode in `probe_extra.py`.
- All 27 remaining diffs per ANSI mode (54 total) are pre-existing out-of-scope gaps present on `spark.sql`: `doubleQuotedIdentifiers=true` refusal (8/mode), runtime `conf.set` `escapedStringLiterals=true` (16/mode), unaliased `spark.sql` expression column display name `Utf8(...)` (1/mode; `selectExpr` matches Spark), and `spark_literals.rs` adjacent raw+escaped literal `r'it''s\'x'` (2/mode).

## Pins

- **Facade pin (`python/repark/tests/dataframe/test_filter_string_escaped_quote_1.py`):** 197 parametrized cases across all 7 doors and both ANSI modes, checking Arrow values + types and `selectExpr` `columns` + `dtypes`: **167 failed, 30 control passed on `main` (`01c15ffa2`)** -> **197 passed, 0 failed on `head`**.
- **Rust unit tests:** added boundary tests in `crates/repark-spark/src/{collation,normalize/statement_guard,uuid_cast,describe_column,show_table_extended}.rs`, `crates/repark-core/src/session/df_guards/{self_join,case_bind}.rs`, and `crates/repark-python/src/column/expr_build.rs`.

# task/wo/filter-string-escaped-quote-1/evidence/summary.md — FILTER-STRING-ESCAPED-QUOTE-1 measurement evidence

## Provenance

- **RePark:** `main` at `01c15ffa2` (`af9ff7251` equivalent after merge), pre-delta merged head (`755c6e6ee`), and Delta 1 branch head (`1784f185b`), release wheels built from each commit.
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
6. **`SET` / `RESET` grammar:** Spark's `SET` / `RESET` parser does not treat RHS strings as backslash-escaped SQL string literals (`SET k = 'C:\path\'` and `SET k = "C:\path\"` are valid; `SET k = 'it\'s; still value'` and `RESET k /* a /* it's */ b */` refuse).

## Seat inventory (Python and Rust)

SQL text scanners changed in this unit (including Delta 1):

| Seat | Doors | Defect on main | Resolution |
|---|---|---|---|
| `python/repark/src/repark/spark/filter_quote.py` | `filter(str)`, `where(str)`, `selectExpr` | `_quoted_span_end` and `re.split` missed `\'`, `\"`, raw/hex prefixes, backticks, `--`/nested `/* */`; `_select_expr_frame` joined items without `\n` before `,` or `FROM`; `_bind_filter_token` stole `AND`/`OR`/`NOT` operators when columns carried those names | Shared Spark lexical helpers (`_iter_spark_sql_spans`, `_quoted_span_closed`, `_quoted_span_end`, `_is_string_prefix_at`, `_spark_line_comment_end`, nested `_spark_block_comment_end`, `_skip_spark_sql_trivia`), context-aware `_boolean_operator_spans` (handling `and`/`or`/`not` + non-reserved `over`/`mod`/`div`/`escape`/`asc`/`desc`/`nulls` columns without rewriting ambiguous `not NOT IN` / `not OR not` into wrong answers), and `"\n, "` + `"\nFROM "` assembly |
| `python/repark/src/repark/spark/column_fields.py` | `filter(str)`, `where(str)` | `re.split` broke on `\'`, `\"`, comments, raw/hex literals, and keyword-named columns | Wired `_quote_filter_sql_identifiers` to `_iter_spark_sql_spans` and `_boolean_operator_spans` |
| `python/repark/src/repark/spark/qualified_names.py` | `filter(str)`, `where(str)`, `selectExpr`, `select`, `join` (outer USING frames) | `_SINGLE_QUOTED_RE`/`_DOUBLE_QUOTED_RE` missed `\'`, `\"`, raw strings, and comments; `generated=True` skipped no single-quoted literals | Wired `_refuse_using_key_text` to `_iter_spark_sql_spans` (pinned on `left`/`right`/`full` USING joins) |
| `python/repark/src/repark/spark/merge_aliases.py` | `DataFrame.mergeInto` | `_STRING_LITERAL` missed `\'`, `"…"`, raw strings, comments | Wired `_qualifiers` to `_iter_spark_sql_spans` (retaining `"target".col` qualifiers followed by `.`) |
| `python/repark/src/repark/spark/_idents.py` | `spark.sql` `DROP TABLE/VIEW` | `strip_sql_comments` missed nested `/* */`, raw strings, doubled quotes | Updated to Spark boundary rules |
| `python/repark/src/repark/spark/dataframe/join_attr_tokens.py` | `filter(Column)`, `select`, `join` on display-overlay frames | `_rewrite_attr_tokens_local` rewrote inside literals/comments; `({expr_sql})` broke on trailing `--` | Wired to `_iter_spark_sql_spans` and inserts `\n` before `)` when `--` appears |
| `python/repark/src/repark/spark/session/session_core.py` | `spark.sql` (`SELECT`/`WITH`/`INSERT`/`CTAS`/`UPDATE`/`DELETE`/`MERGE`) | `_expand_from_join_region` missed `\'`, `\"`, `''`, raw strings, nested comments (silent S1 wrong answer on `FROM` after `\'`) | Wired to shared `filter_quote` helpers (`session_core.py` ratchets 2274 -> 2268) |
| `python/repark/src/repark/spark/session/sql_relations.py`, `sql_udf_parsing.py`, `sql_udf_discovery.py` | `spark.sql` (`CACHE`/`UNCACHE`, `INSERT REPLACE WHERE`, Python UDFs) | Missed `\'`, `\"`, raw strings, backticks, nested comments, and `"…\"…"` simple UDF args | Wired to shared `filter_quote` helpers (`_quoted_span_closed` for both `'…'` and `"…"` simple UDF args; stale docstring clause deleted from `_sql_mask_strings_and_comments`) |
| `python/repark/src/repark/spark/session/sql_set_statements.py` | `spark.sql` `SET`/`RESET` | Non-SQL RHS grammar (not backslash-escaped strings) | Kept main's `_strip_sql_comments` and non-backslash `_has_unquoted_semicolon` so `'C:\path\'` answers and `'it\'s; still value'` / nested-comment `RESET` refuse as on main |
| `python/repark/src/repark/spark/udtf.py` | `spark.sql` registered Python UDTFs | `_strip_sql_comments` ignored string literals; leading trivia regex missed nested `/* */`; `_split_sql_literal_args` does not decode Spark backslash escapes | Wired `_strip_sql_comments` to `_iter_spark_sql_spans` and leading trivia to `_skip_spark_sql_trivia`; kept `_split_sql_literal_args` on main (escape decoding carded on card's still-unlike-Spark list) |
| `crates/repark-spark/src/collation.rs` | `filter(str)`, `where(str)`, `F.expr`, `spark.sql` | `blank_sql_quotes_and_comments` missed `\'`, `\"`, raw strings, nested comments (false `UnsupportedOperationException` on `'it\'s STRING COLLATE utf8_lcase'`); `SELECT ({sql})` lacked `\n` before `)` | Updated scanner to Spark boundaries and `SELECT ({sql}\n)` |
| `crates/repark-python/src/column/expr_build.rs` | `F.expr` (`filter`, `withColumn`, `join`, `select`) | `SELECT ({canonical}) AS _repark_expr` broke trailing `--` comments | Changed to `SELECT ({canonical}\n) AS _repark_expr` |
| `crates/repark-spark/src/normalize/statement_guard.rs` | `spark.sql` unclosed-comment guard | `skip_quoted_text` treated `\` as escape in backticks and raw literals | Scoped `\` escape to `'`/`"` only; skip raw/hex literals without escapes |
| `crates/repark-spark/src/uuid_cast.rs`, `describe_column.rs`, `show_table_extended.rs` | UUID cast refusal, `DESCRIBE`, `SHOW TABLE EXTENDED` | Missed `\'`, `\"`, raw strings, backticks, nested comments | Aligned with Spark boundary rules (`describe_column.rs` advances by `char::len_utf8`) |
| `crates/repark-core/src/session/df_guards/self_join.rs`, `case_bind.rs` | `join`, `filter(str)`, `selectExpr` on joined/duplicate frames | Missed comments/raw literals (`self_join::parse_attr_refs`) and backticks inside strings/comments (`case_bind::sql_text_tokens`) | Aligned scanners (`case_bind` keeps literal words conservative; `self_join::parse_condition` `DatabricksDialect` carded) |

Seats intentionally unchanged: `python/repark/src/repark/spark/session/sql_cache_statements.py` (reverted to main), `crates/repark-spark/src/spark_literals.rs` and `spark_rewrites/` (use `sqlparser` `SparkLexDialect`), `crates/repark-core/src/session/spill.rs` (carded), and `crates/repark-sql/src/scan.rs` (native ANSI SQL door, where `\` is not an escape).

## Domain and Delta 1 probe summary

| Suite | ANSI mode | Total | Equal on main | Equal on pre-delta (`755c6e6e`) | Equal on Delta 1 (`1784f185b`) | Regressed vs main |
|---|---|---|---|---|---|---|
| Core (`probe_domain.py`) | `ansi=true` | 338 | 254 | 313 | 313 | 0 |
| Core (`probe_domain.py`) | `ansi=false` | 338 | 254 | 313 | 313 | 0 |
| Extra (`probe_extra.py`) | `ansi=true` | 29 | 11 | 27 | 27 | 0 |
| Extra (`probe_extra.py`) | `ansi=false` | 29 | 11 | 27 | 27 | 0 |
| Verifier `attack_probe.py` | `ansi=true` | 249 | 166 | 213 | 218 | 0 |
| Verifier `attack_probe.py` | `ansi=false` | 249 | 166 | 213 | 218 | 0 |
| Verifier `deep_probe.py` | `ansi=true` | 40 | 9 | 10 | 19 | 0 |
| Verifier `deep_probe.py` | `ansi=false` | 40 | 9 | 10 | 19 | 0 |
| Verifier `repro2.py` | `ansi=true` | 28 | 6 | 4 | 13 | 0 |
| Verifier `repro2.py` | `ansi=false` | 28 | 6 | 4 | 13 | 0 |

- Every remaining non-equal cell is documented in the card's `Still unlike Spark` section (`task/roadmap/mid-term/cards-2026-11-23/filter-string-escaped-quote-1-card-2026-11-24.md`).

## Pins

- **Facade pin (`python/repark/tests/dataframe/test_filter_string_escaped_quote_1.py`):** 257 parametrized cases across all 7 doors, outer (`left`/`right`/`full`) USING joins, `SET`/`RESET`, UDTF, UDF `"…\"…"`, `DESCRIBE` UTF-8, and boolean/non-reserved keyword columns in both ANSI modes: **48/48 Delta 1 pins fail on `755c6e6e`** and **257/257 pass on Delta 1 head**.
- **Rust unit tests:** boundary and UTF-8 advance tests in `crates/repark-spark/src/{collation,normalize/statement_guard,uuid_cast,describe_column,show_table_extended}.rs`, `crates/repark-core/src/session/df_guards/{self_join,case_bind}.rs`, and `crates/repark-python/src/column/expr_build.rs`.

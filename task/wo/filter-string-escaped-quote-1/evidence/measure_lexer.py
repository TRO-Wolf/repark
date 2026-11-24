import json
from typing import Any

from pyspark.sql import SparkSession


def make_spark(ansi: bool, escaped_literals: bool = False, dq_ident: bool = False) -> Any:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("filterq-lexer-probe")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .config("spark.sql.parser.escapedStringLiterals", "true" if escaped_literals else "false")
        .config("spark.sql.ansi.doubleQuotedIdentifiers", "true" if dq_ident else "false")
    )
    return builder.getOrCreate()


def run_sql_scalar(spark: Any, sql: str) -> dict[str, Any]:
    try:
        rows = spark.sql(sql).collect()
        return {"ok": True, "rows": [list(r) for r in rows], "cols": list(spark.sql(sql).columns)}
    except Exception as exc:
        msg = str(exc).splitlines()[0] if str(exc) else repr(exc)
        return {"ok": False, "err": f"{type(exc).__name__}: {msg[:240]}"}


def run_filter(df: Any, expr: str) -> dict[str, Any]:
    try:
        out = df.filter(expr).select("id")
        rows = [r[0] for r in out.collect()]
        return {"ok": True, "ids": sorted(rows)}
    except Exception as exc:
        msg = str(exc).splitlines()[0] if str(exc) else repr(exc)
        return {"ok": False, "err": f"{type(exc).__name__}: {msg[:240]}"}


def main() -> None:
    spark = make_spark(ansi=True)
    results: dict[str, Any] = {}

    def set_conf(ansi: bool, escaped: bool, dq: bool) -> None:
        spark.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")
        spark.conf.set("spark.sql.parser.escapedStringLiterals", "true" if escaped else "false")
        spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "true" if dq else "false")

    set_conf(True, False, False)
    cases_default = [
        ("sq_escaped_quote", r"SELECT 'it\'s' AS v"),
        ("sq_escaped_dquote", r"""SELECT 'a\"b' AS v"""),
        ("sq_escaped_bs_end", r"SELECT 'a\\' AS v"),
        ("sq_escaped_bs_then_quote_and_lit", r"SELECT 'a\\' 'b' AS v"),
        ("sq_escaped_bs_quote", r"SELECT 'a\\\'' AS v"),
        ("sq_escaped_n", r"SELECT 'a\nb' AS v"),
        ("sq_bare_quote_in_dq", r"""SELECT "it's" AS v"""),
        ("dq_escaped_dquote", r"""SELECT "a\"b" AS v"""),
        ("dq_escaped_squote", r"""SELECT "a\'b" AS v"""),
        ("dq_escaped_bs_end", r"""SELECT "a\\" AS v"""),
        ("dq_escaped_n", r"""SELECT "a\nb" AS v"""),
        ("sq_doubled_quote", "SELECT 'it''s' AS v"),
        ("sq_empty_doubled", "SELECT '''' AS v"),
        ("dq_doubled_quote", 'SELECT "a""b" AS v'),
        ("dq_empty_doubled", 'SELECT """" AS v'),
        ("sq_bs_then_doubled", r"SELECT 'a\''' AS v"),
        ("adjacent_sq", "SELECT 'a' 'b' AS v"),
        ("adjacent_dq", 'SELECT "a" "b" AS v'),
        ("adjacent_mixed", """SELECT 'a' "b" AS v"""),
        ("adjacent_with_escape", r"SELECT 'it\'s' ' ok' AS v"),
        ("adjacent_newline", "SELECT 'a'\n'b' AS v"),
        ("adjacent_comment", "SELECT 'a' /* c */ 'b' AS v"),
        ("raw_sq_plain", r"SELECT r'a\nb' AS v"),
        ("raw_sq_bs_quote", r"SELECT r'it\'s' AS v"),
        ("raw_sq_bs_end", r"SELECT r'a\\' AS v"),
        ("raw_sq_single_bs_end", r"SELECT r'a\' AS v"),
        ("raw_sq_doubled", "SELECT r'it''s' AS v"),
        ("raw_upper_R", r"SELECT R'a\nb' AS v"),
        ("raw_dq_bs_dquote", r"""SELECT r"a\"b" AS v"""),
        ("raw_dq_bs_end", r"""SELECT r"a\\" AS v"""),
        ("raw_dq_doubled", 'SELECT r"a""b" AS v'),
        ("backtick_doubled", "SELECT 1 AS `a``b`"),
        ("backtick_bs_end", r"SELECT 1 AS `a\`"),
        ("backtick_bs_inside", r"SELECT 1 AS `a\`b`"),
        ("backtick_bs_double", r"SELECT 1 AS `a\\b`"),
        ("backtick_bs_before_close_and_select", r"SELECT 1 AS `a\`, 2 AS b"),
        ("comment_line_quote", "SELECT 1 -- it's\n AS v"),
        ("comment_line_eof_quote", "SELECT 1 AS v -- it's"),
        ("comment_block_quote", "SELECT /* it's */ 1 AS v"),
        ("comment_block_dquote", 'SELECT /* "unclosed */ 1 AS v'),
        ("comment_block_nested", "SELECT /* outer /* it's */ inner */ 1 AS v"),
        ("comment_block_nested_unclosed", "SELECT /* outer /* inner */ 1 AS v"),
        ("hex_lit", "SELECT X'6162' AS v"),
    ]
    results["default_ansi_true"] = {name: run_sql_scalar(spark, sql) for name, sql in cases_default}

    set_conf(True, True, False)
    cases_escaped_true = [
        ("sq_bs_quote", r"SELECT 'it\'s' AS v"),
        ("sq_bs_end", r"SELECT 'a\\' AS v"),
        ("sq_single_bs_end", r"SELECT 'a\' AS v"),
        ("sq_bs_n", r"SELECT 'a\nb' AS v"),
        ("sq_doubled_quote", "SELECT 'it''s' AS v"),
        ("sq_empty_doubled", "SELECT '''' AS v"),
        ("dq_bs_dquote", r"""SELECT "a\"b" AS v"""),
        ("dq_single_bs_end", r"""SELECT "a\" AS v"""),
        ("dq_doubled_quote", 'SELECT "a""b" AS v'),
        ("adjacent_sq", "SELECT 'a' 'b' AS v"),
        ("raw_sq_bs_quote", r"SELECT r'it\'s' AS v"),
        ("raw_sq_single_bs_end", r"SELECT r'a\' AS v"),
        ("raw_sq_doubled", "SELECT r'it''s' AS v"),
    ]
    results["escaped_string_literals_true"] = {
        name: run_sql_scalar(spark, sql) for name, sql in cases_escaped_true
    }

    for ansi in (True, False):
        set_conf(ansi, False, True)
        cases_dqi = [
            ("dq_ident_simple", 'SELECT "s" FROM (SELECT 1 AS s)'),
            ("dq_ident_doubled", 'SELECT `a"b` FROM (SELECT 1 AS `a"b`) WHERE "a""b" = 1'),
            ("dq_ident_bs_end", r'SELECT `a\` FROM (SELECT 1 AS `a\`) WHERE "a\" = 1'),
            ("dq_ident_bs_quote", r'SELECT `a\"b` FROM (SELECT 1 AS `a\"b`) WHERE "a\"b" = 1'),
            ("dq_ident_bs_double", r'SELECT `a\\b` FROM (SELECT 1 AS `a\\b`) WHERE "a\\b" = 1'),
            ("dq_as_literal", 'SELECT "hello" AS v'),
            ("sq_still_bs", r"SELECT 'it\'s' AS v"),
        ]
        results[f"dq_ident_ansi_{ansi}"] = {
            name: run_sql_scalar(spark, sql) for name, sql in cases_dqi
        }

    for ansi in (True, False):
        set_conf(ansi, False, False)
        df = spark.sql(
            "SELECT 1 AS id, 'it''s' AS s, 10 AS `and`, 20 AS `or`, 30 AS `not`, "
            "'rv' AS r, 'xv' AS x "
            "UNION ALL "
            "SELECT 2 AS id, 'ok' AS s, 11 AS `and`, 21 AS `or`, 31 AS `not`, 'rw' AS r, 'xw' AS x"
        )
        kw_cases = [
            ("col_and_keyword_AND", "s = 'it\\'s' AND id = 1"),
            ("col_and_quoted_and_AND", "`and` = 10 AND id = 1"),
            ("col_and_bare", "and = 10"),
            ("col_or_keyword_OR", "id = 1 OR id = 99"),
            ("col_or_quoted_and_OR", "`or` = 20 OR id = 99"),
            ("col_or_bare", "or = 20"),
            ("col_not_keyword_NOT", "NOT (id = 2)"),
            ("col_not_IS_NOT_NULL", "s IS NOT NULL AND id = 1"),
            ("col_not_quoted_and_NOT", "`not` = 30 AND NOT (id = 2)"),
            ("col_not_bare", "not = 30"),
            ("col_r_bare", "r = 'rv'"),
            ("col_r_with_raw_sq", r"s = r'it\'s' AND r = 'rv'"),
            ("col_r_with_raw_sq_plain", r"s = r'ok' AND r = 'rw'"),
            ("col_r_with_raw_upper_R", r"s = R'ok' AND r = 'rw'"),
            ("col_r_with_raw_dq", r"""s = r"ok" AND r = 'rw' """),
            ("col_x_with_hex", "hex(X'6f6b') = '6F6B' AND x = 'xv'"),
            ("comment_line_quote_filter", "s = 'it\\'s' -- it's\n AND id = 1"),
            ("comment_block_quote_filter", "s = 'it\\'s' /* it's */ AND id = 1"),
            ("comment_block_nested_quote_filter", "s = 'it\\'s' /* a /* it's */ b */ AND id = 1"),
            ("comment_eof_quote_filter", "s = 'it\\'s' AND id = 1 -- it's"),
        ]
        results[f"kw_filter_ansi_{ansi}"] = {name: run_filter(df, expr) for name, expr in kw_cases}

    spark.stop()
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()

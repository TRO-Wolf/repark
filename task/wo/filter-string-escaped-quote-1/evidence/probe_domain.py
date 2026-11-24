import argparse
import json
import sys


def make_session(engine: str, ansi: bool):
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("filterq-domain-probe")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.ansi.enabled", "true" if ansi else "false")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")
        spark.conf.set("spark.sql.parser.escapedStringLiterals", "false")
        spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "false")
        return spark, F
    from repark.spark import ReparkSession
    from repark.spark import functions as F

    spark = (
        ReparkSession.builder.appName("filterq-domain-probe")
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")
    spark.conf.set("spark.sql.parser.escapedStringLiterals", "false")
    spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "false")
    return spark, F


def norm_rows(df):
    rows = []
    for row in df.collect():
        vals = []
        for v in row:
            if isinstance(v, (bytes, bytearray)):
                vals.append(f"bytes:{v.hex()}")
            else:
                vals.append(v)
        rows.append(vals)
    rows.sort(key=lambda r: [str(x) for x in r])
    return rows


def run_cell(fn, include_schema: bool = False):
    try:
        df = fn()
        out = {"ok": True, "rows": norm_rows(df)}
        if include_schema:
            out["columns"] = list(df.columns)
            out["dtypes"] = [[c, t] for c, t in df.dtypes]
        return out
    except Exception as exc:
        msg = str(exc).splitlines()[0] if str(exc) else repr(exc)
        return {"ok": False, "err": f"{type(exc).__name__}: {msg[:200]}"}


def build_base_frame(spark):
    data = [
        (1, "it's", "k1", "a1"),
        (2, 'a"b', "k2", "a2"),
        (3, "a\\", "k3", "a3"),
        (4, "a\nb", "k4", "a4"),
        (5, "ab", "k5", "a5"),
        (6, "it's ok", "k6", "a6"),
        (7, "a\\'", "k7", "a7"),
        (8, "it's and k", "k8", "a8"),
        (9, 'a"s and k', "k9", "a9"),
        (10, "it't.s", "k10", "a10"),
        (11, 'a"t.s', "k11", "a11"),
        (12, "a\\nb", "k12", "a12"),
        (13, "it's FROM tbl", "k13", "a13"),
        (14, "it's COLLATE utf8", "k14", "a14"),
        (15, "it's) AND a", "k15", "a15"),
        (16, "'", "k16", "a16"),
        (17, '"', "k17", "a17"),
        (18, "it\\'s", "k18", "a18"),
        (19, "a\\\\", "k19", "a19"),
        (20, "it''s", "k20", "a20"),
    ]
    return spark.createDataFrame(data, ["id", "s", "k", "a"])


def build_kw_frame(spark):
    return spark.sql(
        "SELECT 1 AS id, 'it''s' AS s, 10 AS `and`, 20 AS `or`, 30 AS `not`, "
        "'rv1' AS r, 'xv1' AS x, 100 AS `a``b`, 200 AS `a\\` "
        "UNION ALL "
        "SELECT 2 AS id, 'ok' AS s, 11 AS `and`, 21 AS `or`, 31 AS `not`, "
        "'rv2' AS r, 'xv2' AS x, 101 AS `a``b`, 201 AS `a\\` "
        "UNION ALL "
        "SELECT 3 AS id, 'a\\\\' AS s, 12 AS `and`, 22 AS `or`, 32 AS `not`, "
        "'rv3' AS r, 'xv3' AS x, 102 AS `a``b`, 202 AS `a\\` "
        "UNION ALL "
        "SELECT 12 AS id, 'a\\\\nb' AS s, 13 AS `and`, 23 AS `or`, 33 AS `not`, "
        "'rv12' AS r, 'xv12' AS x, 103 AS `a``b`, 203 AS `a\\`"
    )


def run_mode(engine: str, ansi: bool):
    spark, F = make_session(engine, ansi)
    df = build_base_frame(spark)
    df.createOrReplaceTempView("tbl")
    kw_df = build_kw_frame(spark)
    kw_df.createOrReplaceTempView("kw_tbl")

    right_df = spark.createDataFrame(
        [
            (1, "it's"),
            (2, 'a"b'),
            (3, "a\\"),
            (4, "a\nb"),
            (5, "ab"),
            (6, "it's ok"),
            (7, "a\\'"),
            (8, "it's and k"),
            (9, 'a"s and k'),
            (10, "it't.s"),
            (11, 'a"t.s'),
            (12, "a\\nb"),
            (16, "'"),
            (17, '"'),
        ],
        ["rid", "rs"],
    )

    cells = {}

    lexer_probes = [
        ("lex_sq_bs_then_doubled_char", r"SELECT 'a\'''b' AS v"),
        ("lex_sq_bs_then_adj", r"SELECT 'a\'' 'b' AS v"),
        ("lex_sq_bs_then_doubled_adj", r"SELECT 'a\''' 'b' AS v"),
    ]
    for name, sql_text in lexer_probes:
        cells[name] = run_cell(lambda q=sql_text: spark.sql(q))

    predicates = [
        ("sq_bs_quote_col_after", r"s = 'it\'s'"),
        ("sq_bs_quote_and_id", r"s = 'it\'s' AND id = 1"),
        ("sq_bs_quote_kw_and_col_after", r"s = 'it\'s and k'"),
        ("sq_bs_dquote", r"""s = 'a\"b'"""),
        ("dq_bs_dquote_col_after", r'''s = "a\"s and k"'''),
        ("dq_bs_dquote_simple", r"""s = "a\"b" AND id = 2"""),
        ("sq_bs_bs_before_close", r"s = 'a\\' AND id = 3"),
        ("dq_bs_bs_before_close", r"""s = "a\\" AND id = 3"""),
        ("sq_bs_bs_bs_quote", r"s = 'a\\\'' AND id = 7"),
        ("sq_bs_n", r"s = 'a\nb' AND id = 4"),
        ("dq_bs_n", r"""s = "a\nb" AND id = 4"""),
        ("sq_single_escaped_quote", r"s = '\''"),
        ("dq_single_bare_quote", """s = "'" """),
        ("sq_doubled_quote", "s = 'it''s' AND id = 1"),
        ("sq_quad_quote", "s = ''''"),
        ("dq_doubled_quote", 's = "a""b" AND id = 2'),
        ("dq_quad_quote", 's = """"'),
        ("adj_sq", "s = 'a' 'b' AND id = 5"),
        ("adj_sq_with_escape", r"s = 'it\'s' ' ok' AND id = 6"),
        ("raw_sq_bs_n", r"s = r'a\nb' AND id = 12"),
        ("raw_sq_bs_before_close", r"s = r'a\' AND id = 3"),
        ("comment_line_quote_lead", "-- it's\ns = 'it\\'s' AND id = 1"),
        ("comment_line_quote_mid", "s = 'it\\'s' -- it's\n AND id = 1"),
        ("comment_line_quote_eof", "s = 'it\\'s' AND id = 1 -- it's"),
        ("comment_block_quote", "/* it's */ s = 'it\\'s' AND id = 1"),
        ("comment_block_nested_quote", "/* a /* it's */ b */ s = 'it\\'s' AND id = 1"),
        ("comment_block_dquote", '/* "unclosed */ s = "a\\"b" AND id = 2'),
    ]

    for name, pred in predicates:
        cells[f"filter_str::{name}"] = run_cell(lambda p=pred: df.filter(p).select("id"))
        cells[f"where_str::{name}"] = run_cell(lambda p=pred: df.where(p).select("id"))
        cells[f"filter_fexpr::{name}"] = run_cell(lambda p=pred: df.filter(F.expr(p)).select("id"))
        cells[f"withcol_fexpr::{name}"] = run_cell(
            lambda p=pred: (
                df.withColumn("flag", F.expr(p)).filter(F.col("flag")).select("id", "flag")
            )
        )
        cells[f"join_fexpr::{name}"] = run_cell(
            lambda p=pred: df.join(right_df, F.expr(f"({p}) AND id = rid")).select("id", "rid")
        )
        cells[f"selectexpr::{name}"] = run_cell(
            lambda p=pred: df.selectExpr("id", f"({p}) AS m"), include_schema=True
        )
        cells[f"sql_control::{name}"] = run_cell(
            lambda p=pred: spark.sql(f"SELECT id FROM tbl WHERE {p}")
        )

    aliased_df = df.alias("t")
    select_expr_dotted = [
        ("dotted_after_sq_bs_quote", r"t.s = 'it\'t.s' AS m"),
        ("dotted_after_dq_bs_quote", r"""t.s = "a\"t.s" AS m"""),
        ("dotted_lit_only_sq_bs", r"'it\'t.s' AS v"),
        ("dotted_lit_only_dq_bs", r""""a\"t.s" AS v"""),
        ("dotted_no_alias_sq_bs", r"t.s = 'it\'t.s'"),
        ("dotted_after_sq_bs_bs_close", r"t.s = 'a\\' AND t.id = 3 AS m"),
        ("dotted_after_raw_bs_close", r"t.s = r'a\' AND t.id = 3 AS m"),
        ("dotted_comment_line_quote", "-- it's\nt.s = 'it\\'t.s' AS m"),
        ("dotted_comment_block_quote", "/* it's */ t.s = 'it\\'t.s' AS m"),
        ("dotted_comment_nested_quote", "/* a /* it's */ b */ t.s = 'it\\'t.s' AS m"),
    ]
    for name, expr_sql in select_expr_dotted:
        cells[f"selectexpr_aliased::{name}"] = run_cell(
            lambda e=expr_sql: aliased_df.selectExpr("t.id", e), include_schema=True
        )
        cells[f"sql_aliased::{name}"] = run_cell(
            lambda e=expr_sql: spark.sql(f"SELECT t.id, {e} FROM tbl AS t"), include_schema=True
        )

    kw_predicates = [
        ("kw_and_operator", r"s = 'it\'s' AND id = 1"),
        ("kw_and_quoted", r"`and` = 10 AND s = 'it\'s'"),
        ("kw_and_bare", "and = 10"),
        ("kw_or_operator", "id = 1 OR id = 99"),
        ("kw_or_quoted", "`or` = 20 OR id = 99"),
        ("kw_or_bare", "or = 20"),
        ("kw_not_operator", "NOT (id = 2) AND id = 1"),
        ("kw_not_is_not_null", "s IS NOT NULL AND id = 1"),
        ("kw_not_quoted", "`not` = 30 AND NOT (id = 2)"),
        ("kw_not_bare", "not = 30"),
        ("kw_r_column_bare", r"r = 'rv1' AND s = 'it\'s'"),
        ("kw_r_raw_sq", r"s = r'a\nb' AND r = 'rv12'"),
        ("kw_r_raw_upper_R", r"s = R'a\nb' AND r = 'rv12'"),
        ("kw_r_raw_dq", r"""s = r"a\nb" AND r = 'rv12' """),
        ("kw_r_raw_bs_close", r"s = r'a\' AND r = 'rv3'"),
        ("kw_x_hex_lit", "hex(X'6162') = '6162' AND x = 'xv1'"),
        ("backtick_doubled", r"`a``b` = 100 AND s = 'it\'s'"),
        ("backtick_bs_end", r"`a\` = 200 AND s = 'it\'s'"),
        ("backtick_bs_end_with_quote_after", r"`a\` = 200 AND s = 'a\\' OR `a\` = 200"),
    ]
    for name, pred in kw_predicates:
        cells[f"kw_filter_str::{name}"] = run_cell(lambda p=pred: kw_df.filter(p).select("id"))
        cells[f"kw_where_str::{name}"] = run_cell(lambda p=pred: kw_df.where(p).select("id"))
        cells[f"kw_filter_fexpr::{name}"] = run_cell(
            lambda p=pred: kw_df.filter(F.expr(p)).select("id")
        )
        cells[f"kw_selectexpr::{name}"] = run_cell(
            lambda p=pred: kw_df.selectExpr("id", f"({p}) AS m"), include_schema=True
        )
        cells[f"kw_sql_control::{name}"] = run_cell(
            lambda p=pred: spark.sql(f"SELECT id FROM kw_tbl WHERE {p}")
        )

    cells["seat_sql_from_in_bs_sq"] = run_cell(
        lambda: spark.sql(r"SELECT id FROM tbl WHERE s = 'it\'s FROM tbl'")
    )
    cells["seat_sql_from_in_doubled_sq"] = run_cell(
        lambda: spark.sql("SELECT id FROM tbl WHERE s = 'it''s FROM tbl'")
    )
    cells["seat_sql_collate_in_bs_sq"] = run_cell(
        lambda: spark.sql(r"SELECT id FROM tbl WHERE s = 'it\'s COLLATE utf8'")
    )
    left_u = df.select("id", "s").alias("a")
    right_u = df.select("id", "k").alias("b")
    joined_u = left_u.join(right_u, "id")
    cells["seat_using_filter_sq_bs"] = run_cell(
        lambda: joined_u.filter(r"s = 'it\'a.id' OR id = 1").select("id")
    )
    cells["seat_using_selectexpr_sq_bs"] = run_cell(
        lambda: joined_u.selectExpr("id", r"s = 'it\'a.id' AS m"), include_schema=True
    )
    arr_df = spark.sql(
        "SELECT 1 AS id, array('it''s) AND a', 'ok') AS arr, 'xval' AS x "
        "UNION ALL "
        "SELECT 2 AS id, array('nope') AS arr, 'xval' AS x"
    )
    cells["seat_lambda_filter_sq_bs_paren"] = run_cell(
        lambda: arr_df.filter(r"exists(arr, x -> x = 'it\'s) AND a' AND x != 'z')").select("id")
    )
    cells["seat_lambda_sql_sq_bs_paren"] = run_cell(
        lambda: spark.sql(
            r"SELECT id FROM (SELECT 1 AS id, array('it''s) AND a', 'ok') AS arr, 'xval' AS x "
            r"UNION ALL SELECT 2 AS id, array('nope') AS arr, 'xval' AS x) "
            r"WHERE exists(arr, x -> x = 'it\'s) AND a' AND x != 'z')"
        )
    )

    spark.conf.set("spark.sql.parser.escapedStringLiterals", "true")
    esc_preds = [
        ("esc_true_sq_bs_quote", r"s = 'it\'s'"),
        ("esc_true_sq_bs_bs", r"s = 'a\\'"),
        ("esc_true_sq_bs_n", r"s = 'a\nb'"),
        ("esc_true_sq_doubled", "s = 'it''s'"),
    ]
    for name, pred in esc_preds:
        cells[f"esc_true_filter_str::{name}"] = run_cell(lambda p=pred: df.filter(p).select("id"))
        cells[f"esc_true_filter_fexpr::{name}"] = run_cell(
            lambda p=pred: df.filter(F.expr(p)).select("id")
        )
        cells[f"esc_true_selectexpr::{name}"] = run_cell(
            lambda p=pred: df.selectExpr("id", f"({p}) AS m"), include_schema=True
        )
        cells[f"esc_true_sql::{name}"] = run_cell(
            lambda p=pred: spark.sql(f"SELECT id FROM tbl WHERE {p}")
        )
    spark.conf.set("spark.sql.parser.escapedStringLiterals", "false")

    dqi_preds = [
        ("dqi_simple", r""""s" = 'it\'s' """),
        ("dqi_doubled_dq", r""""a""b" = 10 AND "s" = 'it\'s' """),
        ("dqi_bs_dq", r""""a\"b" = 20 AND "s" = 'it\'s' """),
        ("dqi_bs_bs_end", r""""a\\" = 30 AND "s" = 'it\'s' """),
    ]
    try:
        spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "true")
        dqi_err = None
    except Exception as exc:
        msg = str(exc).splitlines()[0] if str(exc) else repr(exc)
        dqi_err = {"ok": False, "err": f"{type(exc).__name__}: {msg[:200]}"}
    if dqi_err is not None:
        for name, _ in dqi_preds:
            cells[f"dqi_filter_str::{name}"] = dict(dqi_err)
            cells[f"dqi_sql::{name}"] = dict(dqi_err)
    else:
        dqi_df = spark.sql(
            "SELECT 1 AS id, 'it''s' AS s, 10 AS `a\"b`, 20 AS `a\\\"b`, 30 AS `a\\\\`"
        )
        dqi_df.createOrReplaceTempView("dqi_tbl")
        for name, pred in dqi_preds:
            cells[f"dqi_filter_str::{name}"] = run_cell(
                lambda p=pred: dqi_df.filter(p).select("id")
            )
            cells[f"dqi_sql::{name}"] = run_cell(
                lambda p=pred: spark.sql(f"SELECT id FROM dqi_tbl WHERE {p}")
            )
        spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "false")

    spark.stop()
    return cells


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark", "repark"], required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    payload = {
        "engine": args.engine,
        "ansi_true": run_mode(args.engine, True),
        "ansi_false": run_mode(args.engine, False),
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    print(
        f"WROTE {args.out}: ansi_true={len(payload['ansi_true'])} ansi_false={len(payload['ansi_false'])}"
    )


if __name__ == "__main__":
    main()

import argparse
import json


def make_session(engine: str, ansi: bool, escaped_literals: bool = False):
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"filterq-extra-{ansi}-{escaped_literals}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.ansi.enabled", "true" if ansi else "false")
            .config(
                "spark.sql.parser.escapedStringLiterals", "true" if escaped_literals else "false"
            )
            .getOrCreate()
        )
        spark.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")
        spark.conf.set(
            "spark.sql.parser.escapedStringLiterals", "true" if escaped_literals else "false"
        )
        return spark, F
    from repark.spark import ReparkSession
    from repark.spark import functions as F

    spark = (
        ReparkSession.builder.appName(f"filterq-extra-{ansi}-{escaped_literals}")
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .config("spark.sql.parser.escapedStringLiterals", "true" if escaped_literals else "false")
        .getOrCreate()
    )
    return spark, F


def norm_rows(df):
    rows = [list(r) for r in df.collect()]
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


def run_all(engine: str):
    out = {}
    for ansi in (True, False):
        spark, F = make_session(engine, ansi, escaped_literals=False)
        cells = {}

        df = spark.createDataFrame(
            [
                (1, "a", "x1"),
                (2, "it's", "x2"),
                (3, "it's STRING COLLATE utf8_lcase", "x3"),
                (4, "it'b.id", "x4"),
            ],
            ["id", "s", "a"],
        )
        df.createOrReplaceTempView("t_extra")
        cells["comment_line_quote_col_a_in_lit"] = run_cell(
            lambda: df.filter("-- it's\ns = 'a'").select("id")
        )
        cells["comment_block_quote_col_a_in_lit"] = run_cell(
            lambda: df.filter("/* it's */ s = 'a'").select("id")
        )
        cells["comment_nested_quote_col_a_in_lit"] = run_cell(
            lambda: df.filter("/* outer /* it's */ inner */ s = 'a'").select("id")
        )
        cells["selectexpr_trailing_line_comment"] = run_cell(
            lambda: df.selectExpr("id", "s = 'it\\'s' AS m -- it's"), include_schema=True
        )
        cells["selectexpr_trailing_line_comment_no_alias"] = run_cell(
            lambda: df.selectExpr("id", "s = 'it\\'s' -- it's"), include_schema=True
        )
        cells["sql_type_collate_in_bs_sq"] = run_cell(
            lambda: spark.sql(r"SELECT id FROM t_extra WHERE s = 'it\'s STRING COLLATE utf8_lcase'")
        )

        left_u = df.select("id", "s").alias("a")
        right_u = df.select("id", "a").alias("b")
        joined_u = left_u.join(right_u, "id")
        cells["using_filter_right_key_in_bs_sq"] = run_cell(
            lambda: joined_u.filter(r"s = 'it\'b.id'").select("id")
        )
        cells["using_selectexpr_right_key_in_bs_sq"] = run_cell(
            lambda: joined_u.selectExpr("id", r"s = 'it\'b.id' AS m"), include_schema=True
        )

        arr_df = spark.sql(
            "SELECT 1 AS id, array('it''s) AND a', 'ok') AS arr, 'XVAL' AS X "
            "UNION ALL "
            "SELECT 2 AS id, array('nope') AS arr, 'XVAL' AS X"
        )
        cells["lambda_filter_uppercase_X_bs_paren"] = run_cell(
            lambda: arr_df.filter(r"exists(arr, x -> x = 'it\'s) AND a' AND x != 'z')").select("id")
        )

        mc_df = spark.sql(
            "SELECT 1 AS id, 'it''s' AS s, 10 AS `And`, 20 AS `Or`, 30 AS `Not`, 'rv1' AS R "
            "UNION ALL "
            "SELECT 2 AS id, 'a\\\\nb' AS s, 11 AS `And`, 21 AS `Or`, 31 AS `Not`, 'rv2' AS R"
        )
        cells["mc_and_operator"] = run_cell(
            lambda: mc_df.filter(r"s = 'it\'s' AND id = 1").select("id")
        )
        cells["mc_and_bare"] = run_cell(lambda: mc_df.filter("And = 10").select("id"))
        cells["mc_and_bare_lower"] = run_cell(lambda: mc_df.filter("and = 10").select("id"))
        cells["mc_and_quoted"] = run_cell(
            lambda: mc_df.filter(r"`And` = 10 AND s = 'it\'s'").select("id")
        )
        cells["mc_or_operator"] = run_cell(lambda: mc_df.filter("id = 1 OR id = 99").select("id"))
        cells["mc_or_bare"] = run_cell(lambda: mc_df.filter("Or = 20").select("id"))
        cells["mc_not_operator"] = run_cell(
            lambda: mc_df.filter("NOT (id = 2) AND id = 1").select("id")
        )
        cells["mc_not_bare"] = run_cell(lambda: mc_df.filter("Not = 30").select("id"))
        cells["mc_r_raw_sq"] = run_cell(
            lambda: mc_df.filter(r"s = r'a\nb' AND R = 'rv2'").select("id")
        )
        cells["mc_r_raw_upper_R"] = run_cell(
            lambda: mc_df.filter(r"s = R'a\nb' AND r = 'rv2'").select("id")
        )
        cells["mc_and_and_or"] = run_cell(
            lambda: mc_df.filter("And = 10 AND Or = 20 AND NOT (Not = 31)").select("id")
        )
        cells["mc_is_not_null_and_not_col"] = run_cell(
            lambda: mc_df.filter("Not IS NOT NULL AND Not = 30").select("id")
        )

        cells["lex_raw_adj_regular_bs"] = run_cell(lambda: spark.sql(r"SELECT r'it''s\'x' AS v"))
        cells["lex_raw_adj_regular_bs_filter"] = run_cell(
            lambda: (
                spark.createDataFrame([(1, "its'x")], ["id", "s"])
                .filter(r"s = r'it''s\'x'")
                .select("id")
            )
        )

        spark.stop()

        spark2, F2 = make_session(engine, ansi, escaped_literals=True)
        df2 = spark2.createDataFrame(
            [(1, "it's"), (3, "a\\"), (18, "it\\'s"), (19, "a\\\\"), (20, "it''s")],
            ["id", "s"],
        )
        df2.createOrReplaceTempView("t_esc")
        cells["builder_esc_true_filter_bs_quote"] = run_cell(
            lambda: df2.filter(r"s = 'it\'s'").select("id")
        )
        cells["builder_esc_true_filter_bs_bs"] = run_cell(
            lambda: df2.filter(r"s = 'a\\'").select("id")
        )
        cells["builder_esc_true_filter_doubled"] = run_cell(
            lambda: df2.filter("s = 'it''s'").select("id")
        )
        cells["builder_esc_true_sql_bs_quote"] = run_cell(
            lambda: spark2.sql(r"SELECT id FROM t_esc WHERE s = 'it\'s'")
        )
        cells["builder_esc_true_fexpr_bs_quote"] = run_cell(
            lambda: df2.filter(F2.expr(r"s = 'it\'s'")).select("id")
        )
        cells["builder_esc_true_selectexpr_bs_quote"] = run_cell(
            lambda: df2.alias("t").selectExpr("t.id", r"t.s = 'it\'s' AS m"), include_schema=True
        )
        spark2.stop()

        out[f"ansi_{ansi}"] = cells
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark", "repark"], required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    data = run_all(args.engine)
    with open(args.out, "w", encoding="utf-8") as h:
        json.dump(data, h, indent=2, sort_keys=True)
    print(f"WROTE {args.out}")


if __name__ == "__main__":
    main()

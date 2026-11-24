from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark.errors import (
    IllegalArgumentException,
    ParseException,
    UnsupportedOperationException,
)
from repark.spark import ReparkSession
from repark.spark import functions as sf
from repark.spark._idents import strip_sql_comments
from repark.spark.merge_aliases import _qualifiers
from repark.spark.session.sql_relations import (
    _find_matching_paren,
    _sql_mask_strings_and_comments,
)
from repark.spark.session.sql_udf_discovery import _sql_udf_arg_is_simple
from repark.spark.session.sql_udf_parsing import (
    _split_sql_select_list,
    _sql_top_level_keyword_index,
)
from repark.spark.types import StringType


@pytest.fixture(params=[True, False], ids=["ansi_on", "ansi_off"])
def ansi_spark(request: pytest.FixtureRequest) -> Iterator[ReparkSession]:
    ansi = bool(request.param)
    ReparkSession.getActiveSession() and ReparkSession.getActiveSession().stop()
    spark = (
        ReparkSession.builder.appName(f"filterq-pin-{ansi}")
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()


def _base_frame(spark: ReparkSession):
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
        (14, "it's STRING COLLATE utf8_lcase", "k14", "a14"),
        (15, 'a"s FROM tbl', "k15", "a15"),
        (16, "'", "k16", "a16"),
        (17, '"', "k17", "a17"),
        (18, "it'b.id", "k18", "a18"),
        (21, "a", "k21", "a21"),
    ]
    return spark.createDataFrame(data, ["id", "s", "k", "a"])


def _kw_frame(spark: ReparkSession):
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


def _mixed_kw_frame(spark: ReparkSession):
    return spark.sql(
        "SELECT 1 AS id, 'it''s' AS s, 10 AS `And`, 20 AS `Or`, 30 AS `Not`, 'rv1' AS R "
        "UNION ALL "
        "SELECT 2 AS id, 'a\\\\nb' AS s, 11 AS `And`, 21 AS `Or`, 31 AS `Not`, 'rv2' AS R"
    )


def _assert_ids_arrow(df, expected_ids: list[int]) -> None:
    table = df.to_arrow()
    assert table.schema.field("id").type in (pa.int32(), pa.int64())
    assert sorted(table.column("id").to_pylist()) == expected_ids
    assert sorted(row["id"] for row in df.collect()) == expected_ids


FILTER_RED_ON_MAIN_CASES = [
    ("sq_bs_quote", r"s = 'it\'s'", [1]),
    ("sq_bs_quote_and_id", r"s = 'it\'s' AND id = 1", [1]),
    ("sq_bs_quote_col_and_kw_after", r"s = 'it\'s and k'", [8]),
    ("dq_bs_dquote_col_after", r'''s = "a\"s and k"''', [9]),
    ("adj_sq_with_escape", r"s = 'it\'s' ' ok' AND id = 6", [6]),
    ("comment_line_quote_lead", "-- it's\ns = 'a'", [21]),
    ("comment_line_quote_mid", "s = 'it\\'s' -- it's\n AND id = 1", [1]),
    ("comment_line_quote_eof", "s = 'it\\'s' AND id = 1 -- it's", [1]),
    ("comment_block_quote", "/* it's */ s = 'a'", [21]),
    ("comment_block_nested_quote", "/* outer /* it's */ inner */ s = 'a'", [21]),
    ("comment_block_dquote", '/* "unclosed */ s = "a\\"b" AND id = 2', [2]),
    ("collate_in_bs_sq", r"s = 'it\'s STRING COLLATE utf8_lcase'", [14]),
]


@pytest.mark.parametrize("door", ["filter", "where"])
@pytest.mark.parametrize(
    ("case_id", "predicate", "expected_ids"),
    FILTER_RED_ON_MAIN_CASES,
    ids=[case[0] for case in FILTER_RED_ON_MAIN_CASES],
)
def test_red_on_main_filter_and_where_str(
    ansi_spark: ReparkSession,
    door: str,
    case_id: str,
    predicate: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    df = _base_frame(ansi_spark)
    filtered = df.filter(predicate) if door == "filter" else df.where(predicate)
    _assert_ids_arrow(filtered.select("id"), expected_ids)


KW_RED_ON_MAIN_CASES = [
    ("kw_and_operator", r"s = 'it\'s' AND id = 1", [1]),
    ("kw_and_quoted", r"`and` = 10 AND s = 'it\'s'", [1]),
    ("kw_or_operator", "id = 1 OR id = 99", [1]),
    ("kw_or_quoted", "`or` = 20 OR id = 99", [1]),
    ("kw_not_operator", "NOT (id = 2) AND id = 1", [1]),
    ("kw_not_is_not_null", "s IS NOT NULL AND id = 1", [1]),
    ("kw_not_quoted", "`not` = 30 AND NOT (id = 2)", [1]),
    ("kw_r_column_bare", r"r = 'rv1' AND s = 'it\'s'", [1]),
    ("kw_r_raw_sq", r"s = r'a\nb' AND r = 'rv12'", [12]),
    ("kw_r_raw_upper_R", r"s = R'a\nb' AND r = 'rv12'", [12]),
    ("kw_r_raw_dq", r"""s = r"a\nb" AND r = 'rv12' """, [12]),
    ("kw_r_raw_bs_close", r"s = r'a\' AND r = 'rv3'", [3]),
    ("kw_x_hex_lit", "hex(X'6162') = '6162' AND x = 'xv1'", [1]),
    ("backtick_doubled", r"`a``b` = 100 AND s = 'it\'s'", [1]),
    ("backtick_bs_end", r"`a\` = 200 AND s = 'it\'s'", [1]),
    ("backtick_bs_end_with_quote_after", r"`a\` = 200 AND s = 'a\\' OR `a\` = 200", [1]),
]


@pytest.mark.parametrize("door", ["filter", "where"])
@pytest.mark.parametrize(
    ("case_id", "predicate", "expected_ids"),
    KW_RED_ON_MAIN_CASES,
    ids=[case[0] for case in KW_RED_ON_MAIN_CASES],
)
def test_red_on_main_keyword_columns_and_backticks(
    ansi_spark: ReparkSession,
    door: str,
    case_id: str,
    predicate: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    df = _kw_frame(ansi_spark)
    filtered = df.filter(predicate) if door == "filter" else df.where(predicate)
    _assert_ids_arrow(filtered.select("id"), expected_ids)


MIXED_KW_RED_ON_MAIN_CASES = [
    ("mc_and_operator", r"s = 'it\'s' AND id = 1", [1]),
    ("mc_and_quoted", r"`And` = 10 AND s = 'it\'s'", [1]),
    ("mc_or_operator", "id = 1 OR id = 99", [1]),
    ("mc_not_operator", "NOT (id = 2) AND id = 1", [1]),
    ("mc_r_raw_sq", r"s = r'a\nb' AND R = 'rv2'", [2]),
    ("mc_r_raw_upper_R", r"s = R'a\nb' AND r = 'rv2'", [2]),
    ("mc_and_and_or", "And = 10 AND Or = 20 AND NOT (Not = 31)", [1]),
    ("mc_is_not_null_and_not_col", "Not IS NOT NULL AND Not = 30", [1]),
]


@pytest.mark.parametrize(
    ("case_id", "predicate", "expected_ids"),
    MIXED_KW_RED_ON_MAIN_CASES,
    ids=[case[0] for case in MIXED_KW_RED_ON_MAIN_CASES],
)
def test_red_on_main_mixed_case_keyword_columns(
    ansi_spark: ReparkSession,
    case_id: str,
    predicate: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    df = _mixed_kw_frame(ansi_spark)
    _assert_ids_arrow(df.filter(predicate).select("id"), expected_ids)


SELECT_EXPR_RED_ON_MAIN_CASES = [
    ("dotted_after_sq_bs_quote", True, r"t.s = 'it\'t.s' AS m", ["id", "m"], "boolean", [10]),
    ("dotted_after_dq_bs_quote", True, r"""t.s = "a\"t.s" AS m""", ["id", "m"], "boolean", [11]),
    (
        "dotted_no_alias_sq_bs",
        True,
        r"t.s = 'it\'t.s'",
        ["id", "(s = it't.s)"],
        "boolean",
        [10],
    ),
    (
        "dotted_comment_line_quote",
        True,
        "-- it's\nt.s = 'it\\'t.s' AS m",
        ["id", "m"],
        "boolean",
        [10],
    ),
    (
        "dotted_comment_block_quote",
        True,
        "/* it's */ t.s = 'it\\'t.s' AS m",
        ["id", "m"],
        "boolean",
        [10],
    ),
    (
        "dotted_comment_nested_quote",
        True,
        "/* a /* it's */ b */ t.s = 'it\\'t.s' AS m",
        ["id", "m"],
        "boolean",
        [10],
    ),
    (
        "trailing_line_comment",
        False,
        "s = 'it\\'s' AS m -- it's",
        ["id", "m"],
        "boolean",
        [1],
    ),
]


@pytest.mark.parametrize(
    ("case_id", "aliased", "expr_sql", "expected_cols", "expected_dtype", "expected_true_ids"),
    SELECT_EXPR_RED_ON_MAIN_CASES,
    ids=[case[0] for case in SELECT_EXPR_RED_ON_MAIN_CASES],
)
def test_red_on_main_select_expr_predicates(
    ansi_spark: ReparkSession,
    case_id: str,
    aliased: bool,
    expr_sql: str,
    expected_cols: list[str],
    expected_dtype: str,
    expected_true_ids: list[int],
) -> None:
    _ = case_id
    df = _base_frame(ansi_spark)
    out = df.alias("t").selectExpr("t.id", expr_sql) if aliased else df.selectExpr("id", expr_sql)
    assert list(out.columns) == expected_cols
    assert list(out.dtypes) == [(expected_cols[0], "bigint"), (expected_cols[1], expected_dtype)]
    table = out.to_arrow()
    assert table.schema.field(expected_cols[1]).type == pa.bool_()
    true_ids = sorted(
        row[0]
        for row in zip(table.column(0).to_pylist(), table.column(1).to_pylist(), strict=True)
        if row[1]
    )
    assert true_ids == expected_true_ids


@pytest.mark.parametrize(
    ("case_id", "expr_sql", "expected_value"),
    [
        ("dotted_lit_only_sq_bs", r"'it\'t.s' AS v", "it't.s"),
        ("dotted_lit_only_dq_bs", r""""a\"t.s" AS v""", 'a"t.s'),
    ],
    ids=["sq_lit", "dq_lit"],
)
def test_red_on_main_select_expr_literal_values(
    ansi_spark: ReparkSession,
    case_id: str,
    expr_sql: str,
    expected_value: str,
) -> None:
    _ = case_id
    df = _base_frame(ansi_spark).filter(sf.col("id") == 1)
    out = df.alias("t").selectExpr("t.id", expr_sql)
    assert list(out.columns) == ["id", "v"]
    assert list(out.dtypes) == [("id", "bigint"), ("v", "string")]
    table = out.to_arrow()
    assert table.schema.field("v").type in (pa.string(), pa.large_string())
    assert table.column("v").to_pylist() == [expected_value]


FEXPR_RED_ON_MAIN_CASES = [
    ("trailing_line_comment_quote", "s = 'it\\'s' AND id = 1 -- it's", [1]),
    ("collate_in_bs_sq", r"s = 'it\'s STRING COLLATE utf8_lcase'", [14]),
]


@pytest.mark.parametrize("door", ["filter_fexpr", "withcol_fexpr", "join_fexpr"])
@pytest.mark.parametrize(
    ("case_id", "predicate", "expected_ids"),
    FEXPR_RED_ON_MAIN_CASES,
    ids=[case[0] for case in FEXPR_RED_ON_MAIN_CASES],
)
def test_red_on_main_fexpr_doors(
    ansi_spark: ReparkSession,
    door: str,
    case_id: str,
    predicate: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    df = _base_frame(ansi_spark)
    if door == "filter_fexpr":
        out = df.filter(sf.expr(predicate)).select("id")
    elif door == "withcol_fexpr":
        out = df.withColumn("flag", sf.expr(predicate)).filter(sf.col("flag")).select("id")
    else:
        right = ansi_spark.createDataFrame([(1, "x"), (14, "y")], ["rid", "rv"])
        out = df.join(right, sf.expr(f"id = rid AND {predicate}")).select("id")
    _assert_ids_arrow(out, expected_ids)


@pytest.mark.parametrize(
    ("case_id", "sql_text", "expected_ids"),
    [
        ("sql_from_in_bs_sq", r"SELECT id FROM tbl WHERE s = 'it\'s FROM tbl'", [13]),
        ("sql_from_in_bs_dq", r'''SELECT id FROM tbl WHERE s = "a\"s FROM tbl"''', [15]),
    ],
    ids=["sq_from", "dq_from"],
)
def test_red_on_main_spark_sql_from_in_escaped_literal(
    ansi_spark: ReparkSession,
    case_id: str,
    sql_text: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    _base_frame(ansi_spark).createOrReplaceTempView("tbl")
    _assert_ids_arrow(ansi_spark.sql(sql_text), expected_ids)


@pytest.mark.parametrize("how", ["inner", "left", "right", "full"])
def test_red_on_main_using_join_escaped_literal(ansi_spark: ReparkSession, how: str) -> None:
    df = _base_frame(ansi_spark)
    joined = df.select("id", "s").alias("a").join(df.select("id", "a").alias("b"), "id", how)
    _assert_ids_arrow(joined.filter(r"s = 'it\'b.id'").select("id"), [18])
    out = joined.selectExpr("id", r"s = 'it\'b.id' AS m")
    assert list(out.columns) == ["id", "m"]
    assert list(out.dtypes) == [("id", "bigint"), ("m", "boolean")]
    table = out.to_arrow()
    assert table.num_rows == 19
    true_ids = sorted(
        row[0]
        for row in zip(table.column(0).to_pylist(), table.column(1).to_pylist(), strict=True)
        if row[1]
    )
    assert true_ids == [18]


@pytest.mark.parametrize("ansi", [True, False], ids=["ansi_on", "ansi_off"])
def test_red_on_main_builder_escaped_string_literals_true(ansi: bool) -> None:
    ReparkSession.getActiveSession() and ReparkSession.getActiveSession().stop()
    spark = (
        ReparkSession.builder.appName(f"filterq-esc-true-{ansi}")
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .config("spark.sql.parser.escapedStringLiterals", "true")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame(
            [(1, "it's"), (3, "a\\"), (18, "it\\'s"), (19, "a\\\\"), (20, "it''s")],
            ["id", "s"],
        )
        _assert_ids_arrow(df.filter(r"s = 'it\'s'").select("id"), [18])
        _assert_ids_arrow(df.where(r"s = 'it\'s'").select("id"), [18])
        _assert_ids_arrow(df.filter(r"s = 'a\\'").select("id"), [19])
        _assert_ids_arrow(df.filter("s = 'it''s'").select("id"), [20])
    finally:
        spark.stop()


def test_red_on_main_auxiliary_python_scanners() -> None:
    assert _qualifiers(r"target.s = 'it\'fake.x' AND target.id = src.id") == {"target", "src"}
    assert _qualifiers(r"""target.s = "a\"fake.x" AND target.id = src.id""") == {"target", "src"}
    assert _qualifiers("/* fake.x */ target.id = src.id") == {"target", "src"}
    masked = _sql_mask_strings_and_comments(r"SELECT 'it\'s; FROM t'")
    assert ";" not in masked and "FROM" not in masked
    assert _find_matching_paren(r"(s = 'it\'s)' AND id = 1)", 0) == 24
    assert _sql_top_level_keyword_index(r"SELECT 'it\'s FROM x' FROM t", "FROM") == 22
    assert _split_sql_select_list(r"'a\',b', c") == [r"'a\',b'", "c"]
    assert _sql_udf_arg_is_simple(r"'it\'s'")
    assert _sql_udf_arg_is_simple(r'"a\"b"')
    assert "still" not in strip_sql_comments("SELECT /* a /* b */ still */ 1")


CONTROL_GUARD_CASES = [
    ("sq_bs_bs_before_close", r"s = 'a\\' AND id = 3", [3]),
    ("dq_bs_bs_before_close", r"""s = "a\\" AND id = 3""", [3]),
    ("sq_bs_bs_bs_quote", r"s = 'a\\\'' AND id = 7", [7]),
    ("sq_bs_n", r"s = 'a\nb' AND id = 4", [4]),
    ("dq_bs_n", r"""s = "a\nb" AND id = 4""", [4]),
    ("sq_single_escaped_quote", r"s = '\''", [16]),
    ("dq_single_bare_quote", """s = "'" """, [16]),
    ("sq_doubled_quote", "s = 'it''s' AND id = 1", [1]),
    ("sq_quad_quote", "s = ''''", [16]),
    ("dq_doubled_quote", 's = "a""b" AND id = 2', [2]),
    ("dq_quad_quote", 's = """"', [17]),
    ("adj_sq", "s = 'a' 'b' AND id = 5", [5]),
    ("raw_sq_bs_n", r"s = r'a\nb' AND id = 12", [12]),
    ("raw_sq_bs_before_close", r"s = r'a\' AND id = 3", [3]),
]


@pytest.mark.parametrize(
    ("case_id", "predicate", "expected_ids"),
    CONTROL_GUARD_CASES,
    ids=[case[0] for case in CONTROL_GUARD_CASES],
)
def test_control_guards_all_doors(
    ansi_spark: ReparkSession,
    case_id: str,
    predicate: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    df = _base_frame(ansi_spark)
    df.createOrReplaceTempView("tbl")
    right = ansi_spark.createDataFrame([(i, i) for i in range(1, 22)], ["rid", "rv"])
    _assert_ids_arrow(df.filter(predicate).select("id"), expected_ids)
    _assert_ids_arrow(df.where(predicate).select("id"), expected_ids)
    _assert_ids_arrow(df.filter(sf.expr(predicate)).select("id"), expected_ids)
    _assert_ids_arrow(
        df.withColumn("flag", sf.expr(predicate)).filter(sf.col("flag")).select("id"),
        expected_ids,
    )
    _assert_ids_arrow(
        df.join(right, sf.expr(f"({predicate}) AND id = rid")).select("id"),
        expected_ids,
    )
    _assert_ids_arrow(ansi_spark.sql(f"SELECT id FROM tbl WHERE {predicate}"), expected_ids)


def test_control_guards_bare_keyword_columns(ansi_spark: ReparkSession) -> None:
    kw_df = _kw_frame(ansi_spark)
    _assert_ids_arrow(kw_df.filter("and = 10").select("id"), [1])
    _assert_ids_arrow(kw_df.filter("or = 20").select("id"), [1])
    _assert_ids_arrow(kw_df.filter("not = 30").select("id"), [1])
    mc_df = _mixed_kw_frame(ansi_spark)
    _assert_ids_arrow(mc_df.filter("And = 10").select("id"), [1])
    _assert_ids_arrow(mc_df.filter("and = 10").select("id"), [1])
    _assert_ids_arrow(mc_df.filter("Or = 20").select("id"), [1])
    _assert_ids_arrow(mc_df.filter("Not = 30").select("id"), [1])


class _EchoUdtf:
    def eval(self, value: str) -> Iterator[tuple[str]]:
        yield (value,)


def _echo_py(value: str) -> str:
    return f"[{value}]"


def _a4_kw_frame(spark: ReparkSession):
    return spark.sql(
        "SELECT 1 AS id, 10 AS `and`, 20 AS `or`, 0 AS `not`, "
        "1 AS `over`, 1 AS `mod`, 1 AS `div`, 1 AS `escape`, "
        "true AS `desc`, true AS `asc`, true AS `nulls` "
        "UNION ALL "
        "SELECT 2 AS id, 11 AS `and`, 21 AS `or`, 1 AS `not`, "
        "2 AS `over`, 2 AS `mod`, 2 AS `div`, 2 AS `escape`, "
        "false AS `desc`, false AS `asc`, false AS `nulls`"
    )


def _a4_bkw_frame(spark: ReparkSession):
    return spark.sql(
        "SELECT 1 AS id, true AS `and`, false AS `or`, false AS `not`, 1 AS `desc` "
        "UNION ALL "
        "SELECT 2 AS id, false AS `and`, true AS `or`, true AS `not`, 2 AS `desc`"
    )


def test_red_on_merged_delta1_a1_select_expr_multi_comments(ansi_spark: ReparkSession) -> None:
    df = ansi_spark.createDataFrame([(1, "it's"), (2, "ok")], ["id", "s"])
    out1 = df.selectExpr("id -- c1", "s")
    assert list(out1.columns) == ["id", "s"]
    assert list(out1.dtypes) == [("id", "bigint"), ("s", "string")]
    table1 = out1.to_arrow()
    assert table1.schema.field("id").type in (pa.int32(), pa.int64())
    assert table1.schema.field("s").type in (pa.string(), pa.large_string())
    assert sorted(zip(table1.column(0).to_pylist(), table1.column(1).to_pylist(), strict=True)) == [
        (1, "it's"),
        (2, "ok"),
    ]
    out2 = df.selectExpr("id -- c1", r"s = 'it\'s' AS m -- c2")
    assert list(out2.columns) == ["id", "m"]
    assert list(out2.dtypes) == [("id", "bigint"), ("m", "boolean")]
    table2 = out2.to_arrow()
    assert table2.schema.field("m").type == pa.bool_()
    assert sorted(zip(table2.column(0).to_pylist(), table2.column(1).to_pylist(), strict=True)) == [
        (1, True),
        (2, False),
    ]


@pytest.mark.parametrize(
    ("case_id", "stmt", "expected"),
    [
        ("set_win_sq", r"SET spark.test.p1 = 'C:\path\'", [("spark.test.p1", r"'C:\path\'")]),
        ("set_win_dq", r'SET spark.test.p2 = "C:\path\"', [("spark.test.p2", r'"C:\path\"')]),
    ],
    ids=["set_win_sq", "set_win_dq"],
)
def test_red_on_merged_delta1_a2_set_answers(
    ansi_spark: ReparkSession,
    case_id: str,
    stmt: str,
    expected: list[tuple[str, str]],
) -> None:
    _ = case_id
    rows = [(row[0], row[1]) for row in ansi_spark.sql(stmt).collect()]
    assert rows == expected


@pytest.mark.parametrize(
    ("case_id", "stmt"),
    [
        ("set_bs_semi", r"SET spark.test.p4 = 'it\'s; still value'"),
        ("set_win_semi_pair", r"SET spark.test.p7 = 'C:\path\'; 'y'"),
        ("reset_nested_comment", "RESET spark.test.p1 /* a /* it's */ b */"),
    ],
    ids=["set_bs_semi", "set_win_semi_pair", "reset_nested_comment"],
)
def test_red_on_merged_delta1_a2_set_reset_refusals(
    ansi_spark: ReparkSession,
    case_id: str,
    stmt: str,
) -> None:
    _ = case_id
    with pytest.raises(IllegalArgumentException):
        ansi_spark.sql(stmt).collect()


@pytest.mark.parametrize(
    ("case_id", "query", "expected"),
    [
        ("udtf_bs_n", r"SELECT * FROM echo_udtf('a\nb')", r"a\nb"),
        ("udtf_bs_t", r"SELECT * FROM echo_udtf('a\tb')", r"a\tb"),
        ("udtf_bs_r", r"SELECT * FROM echo_udtf('a\rb')", r"a\rb"),
        ("udtf_unicode", r"SELECT * FROM echo_udtf('\u0041')", r"\u0041"),
        ("udtf_nested_comment", "/* a /* it's */ b */ SELECT * FROM echo_udtf('ok')", "ok"),
    ],
    ids=["bs_n", "bs_t", "bs_r", "unicode", "nested_comment"],
)
def test_red_on_merged_delta1_a3_udtf_literals_and_comment(
    ansi_spark: ReparkSession,
    case_id: str,
    query: str,
    expected: str,
) -> None:
    _ = case_id
    ansi_spark.udtf.register("echo_udtf", sf.udtf(_EchoUdtf, returnType="v string"))
    table = ansi_spark.sql(query).to_arrow()
    assert table.column(0).to_pylist() == [expected]


@pytest.mark.parametrize(
    ("case_id", "frame_kind", "predicate"),
    [
        ("kw_not_in_1_2", "kw", "not NOT IN (1, 2)"),
        ("kw_not_not_in_1_2", "kw", "NOT not IN (1, 2)"),
        ("bkw_not_or_not", "bkw", "not OR not"),
    ],
    ids=["kw_not_in_1_2", "kw_not_not_in_1_2", "bkw_not_or_not"],
)
def test_red_on_merged_delta1_a4_refuses_ambiguous_not_chains(
    ansi_spark: ReparkSession,
    case_id: str,
    frame_kind: str,
    predicate: str,
) -> None:
    _ = case_id
    df = _a4_kw_frame(ansi_spark) if frame_kind == "kw" else _a4_bkw_frame(ansi_spark)
    with pytest.raises(ParseException):
        df.filter(predicate).select("id").collect()


@pytest.mark.parametrize(
    ("case_id", "frame_kind", "predicate", "expected_ids"),
    [
        ("kw_over_and", "kw", "id = over AND id = 1", [1]),
        ("kw_mod_and", "kw", "id = mod AND id = 1", [1]),
        ("kw_div_and", "kw", "id = div AND id = 1", [1]),
        ("kw_escape_and", "kw", "id = escape AND id = 1", [1]),
        ("kw_not_desc", "kw", "NOT desc", [2]),
        ("kw_not_asc", "kw", "NOT asc", [2]),
        ("kw_not_nulls", "kw", "NOT nulls", [2]),
        ("bkw_not_and_desc_eq", "bkw", "not AND desc = 2", [2]),
    ],
    ids=[
        "kw_over_and",
        "kw_mod_and",
        "kw_div_and",
        "kw_escape_and",
        "kw_not_desc",
        "kw_not_asc",
        "kw_not_nulls",
        "bkw_not_and_desc_eq",
    ],
)
def test_red_on_merged_delta1_a4_nonreserved_kw_columns(
    ansi_spark: ReparkSession,
    case_id: str,
    frame_kind: str,
    predicate: str,
    expected_ids: list[int],
) -> None:
    _ = case_id
    df = _a4_kw_frame(ansi_spark) if frame_kind == "kw" else _a4_bkw_frame(ansi_spark)
    _assert_ids_arrow(df.filter(predicate).select("id"), expected_ids)


@pytest.mark.parametrize(
    ("case_id", "sql_text", "marker"),
    [
        ("describe_utf8_sq", "DESCRIBE café 'unclosed", "'''"),
        ("describe_utf8_bt", "DESCRIBE café `unclosed", "'`'"),
    ],
    ids=["utf8_sq", "utf8_bt"],
)
def test_red_on_merged_delta1_a6_describe_utf8(
    ansi_spark: ReparkSession,
    case_id: str,
    sql_text: str,
    marker: str,
) -> None:
    _ = case_id
    with pytest.raises(ParseException) as exc_info:
        ansi_spark.sql(sql_text).collect()
    assert marker in str(exc_info.value)


def test_red_on_merged_delta1_a7_udf_double_quoted_escaped(ansi_spark: ReparkSession) -> None:
    ansi_spark.udf.register("echo_py", _echo_py, StringType())
    table = ansi_spark.sql(r"""SELECT echo_py("a\"b") AS v""").to_arrow()
    assert table.column("v").to_pylist() == ['[a"b]']


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_control_guards_outer_using_join_refusals(ansi_spark: ReparkSession, how: str) -> None:
    df = _base_frame(ansi_spark)
    joined = df.select("id", "s").alias("a").join(df.select("id", "a").alias("b"), "id", how)
    with pytest.raises(UnsupportedOperationException):
        joined.filter(r"s = 'a\\' AND b.id = 2").collect()
    with pytest.raises(UnsupportedOperationException):
        joined.filter(r"s = r'a\' AND b.id = 2").collect()
    with pytest.raises(UnsupportedOperationException):
        joined.selectExpr("b.id").collect()

"""STRING-LITERAL-ESCAPE-1 facade pins — SQL string literals unescape as Spark does.

Live PySpark 4.1.2 oracle; every expected value below is the Spark answer for
the same source text, recorded verbatim by the Step-0 probe (229 literals on
five doors under both ``escapedStringLiterals`` settings). PE-10: a doubled
``""`` inside a double-quoted literal collapses to one quote, an
``r"…"`` literal answers instead of refusing, and a ``''``/``""`` inside a
raw literal ends the raw token (the tail lexes as a quoted literal).

pins: string-literal-escape-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
C-008, C-009, C-010, C-011, C-012
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.ml.feature import SQLTransformer
from repark.spark.session import _reset_active_session_for_tests


@pytest.fixture
def spark() -> ReparkSession:
    """A facade session for the escape-1 pins."""
    session = ReparkSession.builder.appName("pytest-string-literal-escape-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def verbatim() -> ReparkSession:
    """A facade session with ``escapedStringLiterals=true``."""
    session = (
        ReparkSession.builder.appName("pytest-string-literal-escape-1-verbatim")
        .config("spark.sql.parser.escapedStringLiterals", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


DEFAULT_CASES: list[tuple[str, str]] = [
    (r'"x\\""y"', 'x\\"y'),
    (r'"\\"""', '\\"'),
    (r'"""\n"', '"\n'),
    (r'"\n"""', '\n"'),
    (r'"a\"b""c"', 'a"b"c'),
    (r'"a""b\"c"', 'a"b"c'),
    (r'"a""\n""b"', 'a"\n"b'),
    (r'"it""s" "!"', 'it"s!'),
    ('"say \'hi\' y""all"', "say 'hi' y\"all"),
    ("\"a\\''b\"\"c\\''d\"", "a''b\"c''d"),
    ("\"a''b\"", "a''b"),
    ("'a\"\"b'", 'a""b'),
    ("'a\"\"b''c\"\"d'", 'a""b\'c""d'),
    ("\"a''b\"\"c''d\"", "a''b\"c''d"),
    (r"r'a''b'", "ab"),
    (r"r'a''\n'", "a\n"),
    (r"r'\n''\t'", "\\n\t"),
    (r"r'a''b''c'", "ab'c"),
    (r'r"a""\n"', "a\n"),
    (r'r"\n""\t"', "\\n\t"),
    (r'r"\d"', "\\d"),
    (r'R"\d"', "\\d"),
    (r'r"a\nb"', "a\\nb"),
    (r'r"a""b"', "ab"),
    (r'r""', ""),
    (r'r"a" "b"', "ab"),
    (r"'it''s'", "it's"),
    (r'"it""s"', 'it"s'),
    ("''", ""),
    ('""', ""),
    (r"'\u0041'", "A"),
    (r"'\101'", "A"),
    (r"'\\'", "\\"),
    ("\"a\\\\b''c\"", "a\\b''c"),
    ("\"\\\\''\"", "\\''"),
    ("\"''\\\\n\"", "''\\n"),
    ("\"\\\\n''\"", "\\n''"),
    (r"R'a''b'", "ab"),
    (r'R"a""b"', "ab"),
    ("'a\\\\b\"\"c'", 'a\\b""c'),
]

VERBATIM_CASES: list[tuple[str, str]] = [
    (r"'it''s'", "it''s"),
    (r"''''", "''"),
    (r"'x\\''y'", "x\\\\''y"),
    (r'"it""s"', 'it""s'),
    (r'"x\\""y"', 'x\\\\""y'),
    (r'"\""', '\\"'),
    (r"r'a'", "'a"),
    (r"r''", "'"),
    (r"r'a''b'", "'ab"),
    (r'r""', '"'),
    (r'r"a""b"', '"ab'),
]

WRITE_CASES: list[tuple[str, str]] = [
    (r'"x\\""y"', 'x\\"y'),
    (r'"a""\n""b"', 'a"\n"b'),
    (r"r'a''b''c'", "ab'c"),
    (r'r"a""b"', "ab"),
    (r"'it''s'", "it's"),
    ("'a\"\"b''c\"\"d'", 'a""b\'c""d'),
]


def _text(table: pa.Table, name: str) -> list[str | None]:
    """Column values with the STRING type asserted."""
    assert pa.types.is_string(table.schema.field(name).type)
    return table.column(name).to_pylist()


def _props(frame: Any) -> dict[str, str]:
    """SHOW TBLPROPERTIES answers as a plain dict with STRING types asserted."""
    table = frame.to_arrow()
    return dict(zip(_text(table, "key"), _text(table, "value"), strict=True))


def test_default_literals_match_spark(spark: ReparkSession) -> None:
    """Every default-mode literal answers its Spark value as STRING."""
    for literal, expected in DEFAULT_CASES:
        table = spark.sql(f"SELECT {literal} AS v").to_arrow()
        assert _text(table, "v") == [expected], literal


def test_default_lengths_match_spark(spark: ReparkSession) -> None:
    """Lengths of the collapsed values match Spark."""
    for literal, expected in DEFAULT_CASES:
        table = spark.sql(f"SELECT length({literal}) AS n").to_arrow()
        assert table.column("n").to_pylist() == [len(expected)], literal


def test_verbatim_literals_match_spark(verbatim: ReparkSession) -> None:
    """Verbatim mode keeps backslashes and doublings, and marks raw literals."""
    for literal, expected in VERBATIM_CASES:
        table = verbatim.sql(f"SELECT {literal} AS v").to_arrow()
        assert _text(table, "v") == [expected], literal


def test_literal_doors_agree(spark: ReparkSession) -> None:
    """``F.expr`` and ``selectExpr`` unescape exactly like ``spark.sql``."""
    for literal, expected in DEFAULT_CASES:
        by_expr = spark.range(1).select(F.expr(literal).alias("v")).to_arrow()
        assert _text(by_expr, "v") == [expected], f"F.expr {literal}"
        by_select = spark.range(1).selectExpr(f"{literal} AS v").to_arrow()
        assert _text(by_select, "v") == [expected], f"selectExpr {literal}"


def test_literal_filter_matches_own_value(spark: ReparkSession) -> None:
    """``filter('<lit> = v')`` matches the row carrying the literal value."""
    for literal, _expected in DEFAULT_CASES:
        frame = spark.range(1).select(F.expr(literal).alias("v"))
        assert frame.filter(f"{literal} = v").count() == 1, literal


def test_write_round_trip_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """``INSERT INTO t VALUES (<lit>)`` stores the Spark value."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql("CREATE TABLE sc.ns.esc (id INT, v STRING) USING iceberg")
    rows = ", ".join(f"({index}, {literal})" for index, (literal, _e) in enumerate(WRITE_CASES))
    spark.sql(f"INSERT INTO sc.ns.esc VALUES {rows}")
    table = spark.sql("SELECT id, v FROM sc.ns.esc ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == list(range(len(WRITE_CASES)))
    assert table.column("v").to_pylist() == [expected for _l, expected in WRITE_CASES]


def test_verbatim_tblproperties_collapse_like_spark(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim TBLPROPERTIES keys and values collapse doublings like Spark."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql(
        "CREATE TABLE sc.ns.props (id INT) USING iceberg TBLPROPERTIES "
        "('a''b'='c', \"d\"\"e\"='f', 'dk'='x''y', \"dk2\"=\"p\"\"q\", 'ck'=\"m''n\")"
    )
    props = _props(verbatim.sql("SHOW TBLPROPERTIES sc.ns.props"))
    assert props["a'b"] == "c"
    assert props['d"e'] == "f"
    assert props["dk"] == "x'y"
    assert props["dk2"] == 'p"q'
    assert props["ck"] == "m''n"


def test_verbatim_comments_collapse_like_spark(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim column COMMENT text collapses doublings like Spark."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql(
        "CREATE TABLE sc.ns.cmt (id INT COMMENT 'it''s', v STRING COMMENT \"a\"\"b\") USING iceberg"
    )
    table = verbatim.sql("DESCRIBE TABLE sc.ns.cmt").to_arrow()
    assert table.column("col_name").to_pylist() == ["id", "v"]
    assert _text(table, "comment") == ["it's", 'a"b']


def test_verbatim_alter_set_collapses_like_spark(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim ALTER SET keys collapse and backslash values unescape like Spark."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.alt (id INT) USING iceberg")
    verbatim.sql("ALTER TABLE sc.ns.alt SET TBLPROPERTIES ('nk''k'='v', 'e'='a\\nb')")
    props = _props(verbatim.sql("SHOW TBLPROPERTIES sc.ns.alt"))
    assert props["nk'k"] == "v"
    assert props["e"] == "a\nb"


def test_verbatim_namespace_properties_collapse_like_spark(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim namespace PROPERTIES and DBPROPERTIES unescape like Spark."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql(
        "CREATE NAMESPACE sc.ddlv WITH PROPERTIES ('a''b'='x''y', 'k1'='a\\nb', 'k2'='\\u0041')"
    )
    verbatim.sql("CREATE NAMESPACE sc.ddlv2 WITH DBPROPERTIES ('c''d'='y''z')")
    rows = verbatim.sql("DESCRIBE NAMESPACE EXTENDED sc.ddlv").to_arrow().to_pylist()
    properties = next(row["info_value"] for row in rows if row["info_name"] == "Properties")
    assert "(a'b,x'y)" in properties
    assert "(k1,a\nb)" in properties
    assert "(k2,A)" in properties
    rows = verbatim.sql("DESCRIBE NAMESPACE EXTENDED sc.ddlv2").to_arrow().to_pylist()
    properties = next(row["info_value"] for row in rows if row["info_name"] == "Properties")
    assert "(c'd,y'z)" in properties


def test_verbatim_options_values_stay_verbatim(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim OPTIONS values keep doublings and backslashes like Spark."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.opt (id INT) USING iceberg OPTIONS ('k'='x''y', 'e'='a\\nb')")
    props = _props(verbatim.sql("SHOW TBLPROPERTIES sc.ns.opt"))
    assert props["k"] == "x''y"
    assert props["e"] == "a\\nb"


def test_verbatim_unset_variants_remove_collapsed_keys(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim UNSET with and without IF EXISTS removes the collapsed key (VE-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql(
        "CREATE TABLE sc.ns.un (id INT) USING iceberg TBLPROPERTIES ('a''b'='1', 'c''d'='2')"
    )
    verbatim.sql("ALTER TABLE sc.ns.un UNSET TBLPROPERTIES IF EXISTS ('a''b')")
    props = _props(verbatim.sql("SHOW TBLPROPERTIES sc.ns.un"))
    assert "a'b" not in props
    assert props["c'd"] == "2"
    verbatim.sql("ALTER TABLE sc.ns.un UNSET TBLPROPERTIES ('c''d')")
    props = _props(verbatim.sql("SHOW TBLPROPERTIES sc.ns.un"))
    assert "c'd" not in props


def test_verbatim_show_tblproperties_key_collapses(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim SHOW TBLPROPERTIES with a key collapses the doubling (VE-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.w (id INT) USING iceberg TBLPROPERTIES ('p''k'='p''v')")
    props = _props(verbatim.sql("SHOW TBLPROPERTIES sc.ns.w ('p''k')"))
    assert props == {"p'k": "p'v"}


def test_verbatim_comment_on_unescapes(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim COMMENT ON stores the fully unescaped text (VE-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.w (id INT) USING iceberg")
    verbatim.sql("COMMENT ON TABLE sc.ns.w IS 'o''n\\tc'")
    rows = verbatim.sql("DESCRIBE TABLE EXTENDED sc.ns.w").to_arrow().to_pylist()
    comment = next(row["data_type"] for row in rows if row["col_name"] == "Comment")
    assert comment == "o'n\tc"


def test_default_unset_show_comment_on_match_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """Default UNSET, SHOW-key and COMMENT ON answers equal Spark (VE-1 control)."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql(
        "CREATE TABLE sc.ns.w (id INT) USING iceberg TBLPROPERTIES ('a''b'='1', 'p''k'='p''v')"
    )
    props = _props(spark.sql("SHOW TBLPROPERTIES sc.ns.w ('p''k')"))
    assert props == {"p'k": "p'v"}
    spark.sql("ALTER TABLE sc.ns.w UNSET TBLPROPERTIES IF EXISTS ('a''b')")
    props = _props(spark.sql("SHOW TBLPROPERTIES sc.ns.w"))
    assert "a'b" not in props
    spark.sql("COMMENT ON TABLE sc.ns.w IS 'o''n\\tc'")
    rows = spark.sql("DESCRIBE TABLE EXTENDED sc.ns.w").to_arrow().to_pylist()
    comment = next(row["data_type"] for row in rows if row["col_name"] == "Comment")
    assert comment == "o'n\tc"


def test_verbatim_filter_and_where_follow_the_flag(verbatim: ReparkSession) -> None:
    """Verbatim filter/where match selectExpr values with doublings and backslashes (VE-2)."""
    table = verbatim.range(1).selectExpr("'it''s' AS v").filter("v = 'it''s'").to_arrow()
    assert _text(table, "v") == ["it''s"]
    table = verbatim.range(1).selectExpr("'a\\nb' AS v").where("v = 'a\\nb'").to_arrow()
    assert _text(table, "v") == ["a\\nb"]


def test_default_filter_and_where_match_spark(spark: ReparkSession) -> None:
    """Default filter/where match the unescaped selectExpr values (VE-2 control)."""
    table = spark.range(1).selectExpr("'it''s' AS v").filter("v = 'it''s'").to_arrow()
    assert _text(table, "v") == ["it's"]
    table = spark.range(1).selectExpr("'a\\nb' AS v").where("v = 'a\\nb'").to_arrow()
    assert _text(table, "v") == ["a\nb"]


def test_expr_follows_the_session_flag() -> None:
    """F.expr parses with the active session's verbatim setting (VE-2)."""
    verbatim = (
        ReparkSession.builder.appName("pytest-escape-1-expr-verbatim")
        .config("spark.sql.parser.escapedStringLiterals", "true")
        .getOrCreate()
    )
    try:
        table = verbatim.range(1).select(F.expr("'it''s'").alias("v")).to_arrow()
        assert _text(table, "v") == ["it''s"]
    finally:
        verbatim.stop()
    default = ReparkSession.builder.appName("pytest-escape-1-expr-default").getOrCreate()
    try:
        table = default.range(1).select(F.expr("'it''s'").alias("v")).to_arrow()
        assert _text(table, "v") == ["it's"]
    finally:
        default.stop()


def test_verbatim_dml_predicates_keep_doubled_quotes(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim UPDATE/DELETE/MERGE match and store kept-doubling values (VE-3)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.t (id INT, s STRING) USING iceberg")
    verbatim.sql("INSERT INTO sc.ns.t VALUES (1, 'it''s'), (2, 'z')")
    verbatim.sql("UPDATE sc.ns.t SET s = 'u''p' WHERE s = 'it''s'")
    table = verbatim.sql("SELECT id, s FROM sc.ns.t ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2]
    assert _text(table, "s") == ["u''p", "z"]
    verbatim.sql("CREATE TABLE sc.ns.d (id INT, s STRING) USING iceberg")
    verbatim.sql("INSERT INTO sc.ns.d VALUES (1, 'it''s'), (2, 'z')")
    verbatim.sql("DELETE FROM sc.ns.d WHERE s = 'it''s'")
    table = verbatim.sql("SELECT id, s FROM sc.ns.d ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [2]
    assert _text(table, "s") == ["z"]
    verbatim.sql("CREATE TABLE sc.ns.m (id INT, s STRING) USING iceberg")
    verbatim.sql("INSERT INTO sc.ns.m VALUES (1, 'it''s'), (2, 'z')")
    verbatim.sql(
        "MERGE INTO sc.ns.m x USING (SELECT 1 AS id) y ON x.id = y.id "
        "WHEN MATCHED THEN UPDATE SET s = 'm''g'"
    )
    table = verbatim.sql("SELECT id, s FROM sc.ns.m ORDER BY id").to_arrow()
    assert _text(table, "s") == ["m''g", "z"]
    verbatim.sql(
        "MERGE INTO sc.ns.m x USING (SELECT 9 AS id) y ON x.id = y.id "
        "WHEN NOT MATCHED THEN INSERT (id, s) VALUES (9, 'n''w')"
    )
    table = verbatim.sql("SELECT id, s FROM sc.ns.m ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 9]
    assert _text(table, "s") == ["m''g", "z", "n''w"]


def test_verbatim_merge_on_string_predicate_matches(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim MERGE ON over string keys with doublings matches (VE-3)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.o (id INT, s STRING) USING iceberg")
    verbatim.sql("INSERT INTO sc.ns.o VALUES (1, 'k''k'), (2, 'z')")
    verbatim.sql(
        "MERGE INTO sc.ns.o x USING (SELECT 'k''k' AS s, 7 AS id) y ON x.s = y.s "
        "WHEN MATCHED THEN UPDATE SET id = 7"
    )
    table = verbatim.sql("SELECT id, s FROM sc.ns.o ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [2, 7]
    assert _text(table, "s") == ["z", "k''k"]


def test_verbatim_dml_predicates_keep_backslashes(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim UPDATE matches backslash values kept raw (VE-3)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.b (id INT, s STRING) USING iceberg")
    verbatim.sql("INSERT INTO sc.ns.b VALUES (1, 'a\\nb'), (2, 'z')")
    verbatim.sql("UPDATE sc.ns.b SET s = 'q' WHERE s = 'a\\nb'")
    table = verbatim.sql("SELECT id, s FROM sc.ns.b ORDER BY id").to_arrow()
    assert _text(table, "s") == ["q", "z"]
    verbatim.sql("CREATE TABLE sc.ns.bq (id INT, s STRING) USING iceberg")
    verbatim.sql("INSERT INTO sc.ns.bq VALUES (1, 'k\\\\''m'), (2, 'z')")
    verbatim.sql("UPDATE sc.ns.bq SET s = 'q' WHERE s = 'k\\\\''m'")
    table = verbatim.sql("SELECT id, s FROM sc.ns.bq ORDER BY id").to_arrow()
    assert _text(table, "s") == ["q", "z"]
    verbatim.sql("UPDATE sc.ns.bq SET s = 'k\\\\''m' WHERE id = 2")
    table = verbatim.sql("SELECT id, s FROM sc.ns.bq ORDER BY id").to_arrow()
    assert _text(table, "s") == ["q", "k\\\\''m"]


def test_default_dml_predicates_match_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """Default UPDATE/DELETE/MERGE with collapsed values match Spark (VE-3 control)."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql("CREATE TABLE sc.ns.q (id INT, s STRING) USING iceberg")
    spark.sql("INSERT INTO sc.ns.q VALUES (1, 'a''''b'), (2, 'z')")
    spark.sql("UPDATE sc.ns.q SET s = 'x' WHERE s = 'a''''b'")
    table = spark.sql("SELECT id, s FROM sc.ns.q ORDER BY id").to_arrow()
    assert _text(table, "s") == ["x", "z"]
    spark.sql("CREATE TABLE sc.ns.q2 (id INT, s STRING) USING iceberg")
    spark.sql("INSERT INTO sc.ns.q2 VALUES (1, 'a''''b'), (2, 'z')")
    spark.sql("DELETE FROM sc.ns.q2 WHERE s = 'a''''b'")
    table = spark.sql("SELECT id, s FROM sc.ns.q2 ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [2]
    spark.sql("CREATE TABLE sc.ns.q3 (id INT, s STRING) USING iceberg")
    spark.sql("INSERT INTO sc.ns.q3 VALUES (1, 'it''s'), (2, 'z')")
    spark.sql("UPDATE sc.ns.q3 SET s = 'u''p' WHERE s = 'it''s'")
    table = spark.sql("SELECT id, s FROM sc.ns.q3 ORDER BY id").to_arrow()
    assert _text(table, "s") == ["u'p", "z"]


BUILT_DATA: list[tuple[int, str]] = [
    (1, "it's"),
    (2, "a\\b"),
    (3, "x''y"),
    (4, "k\\'m"),
    (5, "plain"),
    (6, "it''s"),
]


def _sorted_rows(table: pa.Table) -> list[tuple[Any, ...]]:
    """Rows sorted for order-free DataFrame aggs."""
    return sorted((tuple(row.values()) for row in table.to_pylist()), key=lambda row: repr(row))


def _assert_string_field(table: pa.Table, name: str) -> None:
    """Assert a result column is a Spark STRING."""
    assert pa.types.is_string(table.schema.field(name).type), name


def _built_cube_expected() -> list[tuple[Any, ...]]:
    """Spark verbatim/default rows for the backslash ``when`` inside cube."""
    rows = [(value, 0) for _id, value in BUILT_DATA] + [(None, 1)]
    return sorted(
        [(value, 1 if value == "k\\'m" else count) for value, count in rows],
        key=lambda row: repr(row),
    )


def test_verbatim_built_aggregations_keep_python_values(verbatim: ReparkSession) -> None:
    """Verbatim cube/rollup/groupingSets answer Python values exactly (VE2-1).

    groupingSets also returns a grand-total row repark adds beyond Spark; that
    row predates this unit (att3 base and head show it), so the pin asserts it
    with a Spark-equal value rather than dropping it.
    """
    frame = verbatim.createDataFrame(BUILT_DATA, ["id", "s"])
    cube = frame.cube("s").agg(F.sum(F.when(F.col("s") == "k\\'m", 1).otherwise(0)).alias("k"))
    cube_table = cube.to_arrow()
    _assert_string_field(cube_table, "s")
    assert _sorted_rows(cube_table) == _built_cube_expected()
    rollup = frame.rollup("s").agg(F.first(F.lit("q\\'r")).alias("r"))
    rollup_table = rollup.to_arrow()
    _assert_string_field(rollup_table, "r")
    rollup_rows = _sorted_rows(rollup_table)
    assert len(rollup_rows) == len(BUILT_DATA) + 1
    for _value, lit_value in rollup_rows:
        assert lit_value == "q\\'r"
    grouped = frame.groupingSets("s").agg(F.max(F.concat(F.col("s"), F.lit("\\"))).alias("g"))
    grouped_table = grouped.to_arrow()
    _assert_string_field(grouped_table, "g")
    concats = [f"{value}\\" for _id, value in BUILT_DATA]
    assert _sorted_rows(grouped_table) == sorted(
        [(value, f"{value}\\") for _id, value in BUILT_DATA] + [(None, max(concats))],
        key=lambda row: repr(row),
    )


def test_default_built_aggregations_match_spark(spark: ReparkSession) -> None:
    """Default cube/rollup/groupingSets answers equal Spark (VE2-1 control)."""
    frame = spark.createDataFrame(BUILT_DATA, ["id", "s"])
    cube = frame.cube("s").agg(F.sum(F.when(F.col("s") == "k\\'m", 1).otherwise(0)).alias("k"))
    cube_table = cube.to_arrow()
    _assert_string_field(cube_table, "s")
    assert _sorted_rows(cube_table) == _built_cube_expected()
    rollup = frame.rollup("s").agg(F.first(F.lit("q\\'r")).alias("r"))
    rollup_table = rollup.to_arrow()
    _assert_string_field(rollup_table, "r")
    rollup_rows = _sorted_rows(rollup_table)
    assert len(rollup_rows) == len(BUILT_DATA) + 1
    for _value, lit_value in rollup_rows:
        assert lit_value == "q\\'r"


def test_verbatim_unpivot_keeps_backslash_labels(verbatim: ReparkSession) -> None:
    """Verbatim unpivot variable labels keep quotes and backslashes (VE2-1)."""
    frame = verbatim.createDataFrame([(1, 5, 6, 7)], ["id", "a'b", "c\\d", "q\\'r"])
    unpivoted = frame.unpivot("id", ["a'b", "c\\d", "q\\'r"], "var", "val")
    unpivoted_table = unpivoted.to_arrow()
    _assert_string_field(unpivoted_table, "var")
    assert _sorted_rows(unpivoted_table) == sorted(
        [(1, "a'b", 5), (1, "c\\d", 6), (1, "q\\'r", 7)], key=lambda row: repr(row)
    )


def test_default_unpivot_matches_spark(spark: ReparkSession) -> None:
    """Default unpivot labels equal Spark (VE2-1 control)."""
    frame = spark.createDataFrame([(1, 5, 6, 7)], ["id", "a'b", "c\\d", "q\\'r"])
    unpivoted = frame.unpivot("id", ["a'b", "c\\d", "q\\'r"], "var", "val")
    unpivoted_table = unpivoted.to_arrow()
    _assert_string_field(unpivoted_table, "var")
    assert _sorted_rows(unpivoted_table) == sorted(
        [(1, "a'b", 5), (1, "c\\d", 6), (1, "q\\'r", 7)], key=lambda row: repr(row)
    )


def _merge_backslash_source(session: ReparkSession) -> Any:
    """Two-row merge source with a backslash-quote key."""
    return session.createDataFrame([(4, "k\\'m"), (7, "n'w")], ["id", "s"])


def test_verbatim_merge_into_matches_backslash_rows(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim mergeInto matches and stores backslash values (VE2-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.t (id INT, s STRING) USING iceberg")
    verbatim.createDataFrame(BUILT_DATA, ["id", "s"]).writeTo("sc.ns.t").append()
    (
        _merge_backslash_source(verbatim)
        .alias("src")
        .mergeInto("sc.ns.t", F.col("t.id") == F.col("src.id"))
        .whenMatched(F.col("t.s") == "k\\'m")
        .update({"s": F.concat(F.col("src.s"), F.lit("|\\'"))})
        .whenNotMatched()
        .insert({"id": F.col("src.id"), "s": F.lit("i\\'n")})
        .merge()
    )
    table = verbatim.sql("SELECT id, s FROM sc.ns.t ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5, 6, 7]
    assert _text(table, "s") == ["it's", "a\\b", "x''y", "k\\'m|\\'", "plain", "it''s", "i\\'n"]


def test_default_merge_into_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """Default mergeInto answers equal Spark (VE2-1 control)."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql("CREATE TABLE sc.ns.t (id INT, s STRING) USING iceberg")
    spark.createDataFrame(BUILT_DATA, ["id", "s"]).writeTo("sc.ns.t").append()
    (
        _merge_backslash_source(spark)
        .alias("src")
        .mergeInto("sc.ns.t", F.col("t.id") == F.col("src.id"))
        .whenMatched(F.col("t.s") == "k\\'m")
        .update({"s": F.concat(F.col("src.s"), F.lit("|\\'"))})
        .whenNotMatched()
        .insert({"id": F.col("src.id"), "s": F.lit("i\\'n")})
        .merge()
    )
    table = spark.sql("SELECT id, s FROM sc.ns.t ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5, 6, 7]
    assert _text(table, "s") == ["it's", "a\\b", "x''y", "k\\'m|\\'", "plain", "it''s", "i\\'n"]


def test_verbatim_merge_into_expr_condition_follows_flag(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """A verbatim expr condition spliced into built MERGE matches the kept value (VE2-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.e (id INT, s STRING) USING iceberg")
    verbatim.createDataFrame(BUILT_DATA, ["id", "s"]).writeTo("sc.ns.e").append()
    (
        verbatim.createDataFrame([(1, "x"), (6, "y")], ["id", "s"])
        .alias("src")
        .mergeInto("sc.ns.e", F.col("e.id") == F.col("src.id"))
        .whenMatched(F.expr("e.s = 'it''s'"))
        .update({"s": F.lit("hit")})
        .merge()
    )
    table = verbatim.sql("SELECT id, s FROM sc.ns.e ORDER BY id").to_arrow()
    assert _text(table, "s") == ["it's", "a\\b", "x''y", "k\\'m", "plain", "hit"]


def test_default_merge_into_expr_condition_matches_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """A default expr condition spliced into built MERGE matches the collapsed value."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql("CREATE TABLE sc.ns.e (id INT, s STRING) USING iceberg")
    spark.createDataFrame(BUILT_DATA, ["id", "s"]).writeTo("sc.ns.e").append()
    (
        spark.createDataFrame([(1, "x"), (6, "y")], ["id", "s"])
        .alias("src")
        .mergeInto("sc.ns.e", F.col("e.id") == F.col("src.id"))
        .whenMatched(F.expr("e.s = 'it''s'"))
        .update({"s": F.lit("hit")})
        .merge()
    )
    table = spark.sql("SELECT id, s FROM sc.ns.e ORDER BY id").to_arrow()
    assert _text(table, "s") == ["hit", "a\\b", "x''y", "k\\'m", "plain", "it''s"]


def test_verbatim_overwrite_condition_replaces_backslash_rows(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim overwrite(cond) replaces the backslash row without duplicating (VE2-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.p (id INT, s STRING) USING iceberg PARTITIONED BY (s)")
    verbatim.createDataFrame([(1, "k\\'m"), (2, "z")], ["id", "s"]).writeTo("sc.ns.p").append()
    verbatim.createDataFrame([(9, "k\\'m")], ["id", "s"]).writeTo("sc.ns.p").overwrite(
        F.col("s") == "k\\'m"
    )
    table = verbatim.sql("SELECT id, s FROM sc.ns.p ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [2, 9]
    assert _text(table, "s") == ["z", "k\\'m"]


def test_default_overwrite_condition_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """Default overwrite(cond) answers equal Spark (VE2-1 control)."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql("CREATE TABLE sc.ns.p (id INT, s STRING) USING iceberg PARTITIONED BY (s)")
    spark.createDataFrame([(1, "k\\'m"), (2, "z")], ["id", "s"]).writeTo("sc.ns.p").append()
    spark.createDataFrame([(9, "k\\'m")], ["id", "s"]).writeTo("sc.ns.p").overwrite(
        F.col("s") == "k\\'m"
    )
    table = spark.sql("SELECT id, s FROM sc.ns.p ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [2, 9]
    assert _text(table, "s") == ["z", "k\\'m"]


def test_verbatim_overwrite_partitions_keeps_quoted_values(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """Verbatim overwritePartitions replaces only the present quoted partition (VE2-1)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.q (id INT, s STRING) USING iceberg PARTITIONED BY (s)")
    verbatim.createDataFrame([(1, "it's"), (2, "x''y"), (3, "z")], ["id", "s"]).writeTo(
        "sc.ns.q"
    ).append()
    verbatim.createDataFrame([(7, "it's")], ["id", "s"]).writeTo("sc.ns.q").overwritePartitions()
    table = verbatim.sql("SELECT id, s FROM sc.ns.q ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [2, 3, 7]
    assert _text(table, "s") == ["x''y", "z", "it's"]


def test_verbatim_create_table_schema_str_unescapes_comment(
    verbatim: ReparkSession, tmp_path: Path
) -> None:
    """A verbatim DDL schema fragment keeps the always-unescape COMMENT rule (VE2-1)."""
    from repark.spark import catalog_surface

    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    catalog_surface.create_table(
        verbatim.catalog, "sc.ns.ct", schema="a STRING COMMENT 'it''s', b INT"
    )
    table = verbatim.sql("DESCRIBE TABLE sc.ns.ct").to_arrow()
    assert table.column("col_name").to_pylist() == ["a", "b"]
    assert _text(table, "comment") == ["it's", None]


def test_default_create_table_schema_str_matches_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """A default DDL schema fragment unescapes COMMENT text like Spark."""
    from repark.spark import catalog_surface

    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    catalog_surface.create_table(
        spark.catalog, "sc.ns.ct", schema="a STRING COMMENT 'it''s', b INT"
    )
    table = spark.sql("DESCRIBE TABLE sc.ns.ct").to_arrow()
    assert table.column("col_name").to_pylist() == ["a", "b"]
    assert _text(table, "comment") == ["it's", None]


def test_verbatim_nested_struct_set_keeps_values(verbatim: ReparkSession, tmp_path: Path) -> None:
    """Verbatim nested UPDATE/MERGE SET store kept doublings and backslashes (VE2-2)."""
    verbatim.register_memory_catalog("sc", tmp_path)
    verbatim.sql("CREATE NAMESPACE sc.ns")
    verbatim.sql("CREATE TABLE sc.ns.st (id INT, st STRUCT<f: STRING, g: INT>) USING iceberg")
    verbatim.sql(
        "INSERT INTO sc.ns.st VALUES (1, named_struct('f', 'a', 'g', 1)), "
        "(2, named_struct('f', 'b', 'g', 2)), (3, named_struct('f', 'c', 'g', 3))"
    )
    verbatim.sql("UPDATE sc.ns.st SET st.f = 'n''''f' WHERE id = 1")
    verbatim.sql("UPDATE sc.ns.st SET st.f = 'k\\\\''m' WHERE id = 2")
    verbatim.sql(
        "MERGE INTO sc.ns.st t USING (SELECT 3 AS id) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.st.f = 'm''''g'"
    )
    table = verbatim.sql("SELECT id, st.f AS f FROM sc.ns.st ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert _text(table, "f") == ["n''''f", "k\\\\''m", "m''''g"]


def test_default_nested_struct_set_matches_spark(spark: ReparkSession, tmp_path: Path) -> None:
    """Default nested UPDATE/MERGE SET collapse doublings and backslashes like Spark."""
    spark.register_memory_catalog("sc", tmp_path)
    spark.sql("CREATE NAMESPACE sc.ns")
    spark.sql("CREATE TABLE sc.ns.st (id INT, st STRUCT<f: STRING, g: INT>) USING iceberg")
    spark.sql(
        "INSERT INTO sc.ns.st VALUES (1, named_struct('f', 'a', 'g', 1)), "
        "(2, named_struct('f', 'b', 'g', 2)), (3, named_struct('f', 'c', 'g', 3))"
    )
    spark.sql("UPDATE sc.ns.st SET st.f = 'n''''f' WHERE id = 1")
    spark.sql("UPDATE sc.ns.st SET st.f = 'k\\\\''m' WHERE id = 2")
    spark.sql(
        "MERGE INTO sc.ns.st t USING (SELECT 3 AS id) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.st.f = 'm''''g'"
    )
    table = spark.sql("SELECT id, st.f AS f FROM sc.ns.st ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert _text(table, "f") == ["n''f", "k\\'m", "m''g"]


def test_verbatim_sql_transformer_follows_the_flag(verbatim: ReparkSession) -> None:
    """Verbatim SQLTransformer answers equal spark.sql, not the built door (VE3-1)."""
    frame = verbatim.createDataFrame([(1, "it's"), (2, "it''s")], ["id", "s"])
    out = SQLTransformer(statement="SELECT id, 'a\\\\b' AS v FROM __THIS__").transform(frame)
    assert _text(out.to_arrow(), "v") == ["a\\\\b", "a\\\\b"]
    out = SQLTransformer(statement="SELECT id, 'k\\'m' AS v FROM __THIS__").transform(frame)
    assert _text(out.to_arrow(), "v") == ["k\\'m", "k\\'m"]
    out = SQLTransformer(statement="SELECT id, 'it''s' AS v FROM __THIS__").transform(frame)
    assert _text(out.to_arrow(), "v") == ["it''s", "it''s"]
    out = SQLTransformer(statement="SELECT id FROM __THIS__ WHERE s = 'it''s'").transform(frame)
    assert sorted(out.to_arrow().column("id").to_pylist()) == [2]


def test_default_sql_transformer_matches_spark(spark: ReparkSession) -> None:
    """Default SQLTransformer collapses literals like Spark (VE3-1 control)."""
    frame = spark.createDataFrame([(1, "it's"), (2, "it''s")], ["id", "s"])
    out = SQLTransformer(statement="SELECT id, 'a\\\\b' AS v FROM __THIS__").transform(frame)
    assert _text(out.to_arrow(), "v") == ["a\\b", "a\\b"]
    out = SQLTransformer(statement="SELECT id, 'k\\'m' AS v FROM __THIS__").transform(frame)
    assert _text(out.to_arrow(), "v") == ["k'm", "k'm"]
    out = SQLTransformer(statement="SELECT id, 'it''s' AS v FROM __THIS__").transform(frame)
    assert _text(out.to_arrow(), "v") == ["it's", "it's"]
    out = SQLTransformer(statement="SELECT id FROM __THIS__ WHERE s = 'it''s'").transform(frame)
    assert sorted(out.to_arrow().column("id").to_pylist()) == [1]


_MERGE_PIN_PATTERN: str = "yyyy-MM-dd''HH:mm:ss"
_MERGE_PIN_SPARK_BYTES: bytes = b"it's,2024-06-15'12:00:00\nit''s,2024-06-15'12:00:00\n"


def _write_merge_pin_csv(verbatim_mode: bool, dest: Path) -> bytes:
    """Write the merge-pin frame as CSV in one session mode, return part bytes."""
    _reset_active_session_for_tests()
    builder = ReparkSession.builder.appName("pytest-string-literal-escape-1-merge-pin")
    if verbatim_mode:
        builder = builder.config("spark.sql.parser.escapedStringLiterals", "true")
    session = builder.getOrCreate()
    try:
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        rows = [
            ("it's", datetime.datetime(2024, 6, 15, 12, 0, 0)),
            ("it''s", datetime.datetime(2024, 6, 15, 12, 0, 0)),
        ]
        frame = session.createDataFrame(rows, ["s", "t"])
        frame.write.mode("overwrite").option("header", "false").option(
            "timestampFormat", _MERGE_PIN_PATTERN
        ).csv(str(dest))
    finally:
        session.stop()
        _reset_active_session_for_tests()
    parts = sorted(dest.rglob("*.csv"))
    assert len(parts) == 1
    return parts[0].read_bytes()


def test_merge_text_write_verbatim_matches_spark_and_default(tmp_path: Path) -> None:
    """Verbatim CSV write with a quoted timestampFormat matches Spark and default."""
    assert _write_merge_pin_csv(False, tmp_path / "default") == _MERGE_PIN_SPARK_BYTES
    assert _write_merge_pin_csv(True, tmp_path / "verbatim") == _MERGE_PIN_SPARK_BYTES

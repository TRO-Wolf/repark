"""STRING-LITERAL-ESCAPE-1 facade pins — SQL string literals unescape as Spark does.

Live PySpark 4.1.2 oracle; every expected value below is the Spark answer for
the same source text, recorded verbatim by the Step-0 probe (229 literals on
five doors under both ``escapedStringLiterals`` settings). PE-10: a doubled
``""`` inside a double-quoted literal collapses to one quote, an
``r"…"`` literal answers instead of refusing, and a ``''``/``""`` inside a
raw literal ends the raw token (the tail lexes as a quoted literal).

pins: string-literal-escape-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


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

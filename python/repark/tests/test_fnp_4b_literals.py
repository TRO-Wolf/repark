"""FNP-4B literal pins — escapedStringLiterals, out-of-range ``\\U``, suffixes, struct.

Oracle: PySpark 4.1.2 fixtures-batch1 cells BL10-*, BL12-*, fixtures-batch1/2 cells
BL6-sql-3, BL16-*, DIV-ceil-*, DIV-round-*, DIV-sec-*, fixtures-batch4 cells
JD-exp-literal-types, JD-exp-literal-cast (see the unit ledger
``task/ledgers/staging/fnp-4b-ledger.md``). Every pin collects on the Arrow path
(value AND type/nullability). The ``1S`` / ``1Y`` / ``1.5F`` / ``1.5BD`` shapes have
no oracle cell; their pins record the implemented Spark suffix contract.

pins: fnp-4b/C-004, C-005, C-006
"""

from __future__ import annotations

import decimal

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.dataframe import DataFrame


@pytest.fixture
def spark() -> ReparkSession:
    """A facade session for the FNP-4B literal pins."""
    session = ReparkSession.builder.appName("pytest-fnp-4b-literals").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def verbatim() -> ReparkSession:
    """A facade session with ``escapedStringLiterals=true`` (BL10-on)."""
    session = (
        ReparkSession.builder.appName("pytest-fnp-4b-verbatim")
        .config("spark.sql.parser.escapedStringLiterals", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def _table(result: DataFrame) -> pa.Table:
    """Collect a frame on the Arrow path (value AND type)."""
    return result.to_arrow()


def test_escapes_processed_by_default_backslash_d(spark: ReparkSession) -> None:
    """BL10-off-0: ``SELECT '\\d'`` is ``d`` when the flag is absent."""
    table = _table(spark.sql(r"SELECT '\d' AS s"))
    assert table.column("s").to_pylist() == ["d"]
    assert table.schema.field("s").nullable is False


def test_escapes_processed_by_default_escaped_quote(spark: ReparkSession) -> None:
    """BL10-off-1: ``SELECT '\\''`` is ``'`` when the flag is absent."""
    table = _table(spark.sql(r"SELECT '\'' AS s"))
    assert table.column("s").to_pylist() == ["'"]


def test_escapes_processed_by_default_tab(spark: ReparkSession) -> None:
    """BL10-off-2: ``SELECT 'a\\tb'`` is ``a<TAB>b`` when the flag is absent."""
    table = _table(spark.sql(r"SELECT 'a\tb' AS s"))
    assert table.column("s").to_pylist() == ["a\tb"]


def test_escapes_processed_by_default_double_backslash_len_1(
    spark: ReparkSession,
) -> None:
    """BL10-off-3: ``length('\\\\')`` is 1 when the flag is absent."""
    table = _table(spark.sql(r"SELECT length('\\') AS n"))
    assert table.column("n").to_pylist() == [1]
    assert table.schema.field("n").nullable is False


def test_verbatim_flag_keeps_backslash_d(verbatim: ReparkSession) -> None:
    """BL10-on-0: ``SELECT '\\d'`` is ``\\d`` with the flag on."""
    table = _table(verbatim.sql(r"SELECT '\d' AS s"))
    assert table.column("s").to_pylist() == ["\\d"]
    assert table.schema.field("s").nullable is False


def test_verbatim_flag_keeps_escaped_quote(verbatim: ReparkSession) -> None:
    """BL10-on-1: ``SELECT '\\''`` is ``\\'`` with the flag on."""
    table = _table(verbatim.sql(r"SELECT '\'' AS s"))
    assert table.column("s").to_pylist() == ["\\'"]


def test_verbatim_flag_keeps_tab_escape(verbatim: ReparkSession) -> None:
    """BL10-on-2: ``SELECT 'a\\tb'`` is ``a\\tb`` with the flag on."""
    table = _table(verbatim.sql(r"SELECT 'a\tb' AS s"))
    assert table.column("s").to_pylist() == ["a\\tb"]


def test_verbatim_flag_double_backslash_len_2(verbatim: ReparkSession) -> None:
    """BL10-on-3: ``length('\\\\')`` is 2 with the flag on."""
    table = _table(verbatim.sql(r"SELECT length('\\') AS n"))
    assert table.column("n").to_pylist() == [2]


def test_verbatim_flag_rejects_garbage(verbatim: ReparkSession) -> None:
    """A present-but-unparsable flag value refuses loud and names the key."""
    verbatim.stop()
    with pytest.raises(Exception, match="escapedStringLiterals"):
        (
            ReparkSession.builder.appName("pytest-fnp-4b-badflag")
            .config("spark.sql.parser.escapedStringLiterals", "notabool")
            .getOrCreate()
        )


def test_out_of_range_u110000_is_two_replacements(spark: ReparkSession) -> None:
    """BL12-0: ``'\\U00110000'`` has length 2 and hex ``3F3F``."""
    table = _table(spark.sql(r"SELECT length('\U00110000') AS l, hex('\U00110000') AS h"))
    assert table.column("l").to_pylist() == [2]
    assert table.column("h").to_pylist() == ["3F3F"]
    assert table.schema.field("l").nullable is False


def test_out_of_range_uffffffff_is_d7bf_plus_replacement(
    spark: ReparkSession,
) -> None:
    """BL12-1: ``'\\UFFFFFFFF'`` has length 2 and hex ``ED9EBF3F``."""
    table = _table(spark.sql(r"SELECT length('\UFFFFFFFF') AS l, hex('\UFFFFFFFF') AS h"))
    assert table.column("l").to_pylist() == [2]
    assert table.column("h").to_pylist() == ["ED9EBF3F"]


def test_valid_astral_u1f600_is_one_char(spark: ReparkSession) -> None:
    """BL12-2: ``'\\U0001F600'`` has length 1 and hex ``F09F9880``."""
    table = _table(spark.sql(r"SELECT length('\U0001F600') AS l, hex('\U0001F600') AS h"))
    assert table.column("l").to_pylist() == [1]
    assert table.column("h").to_pylist() == ["F09F9880"]


def test_lone_surrogate_is_one_replacement(spark: ReparkSession) -> None:
    """BL12-3: ``'\\ud83d'`` has length 1 and hex ``3F``."""
    table = _table(spark.sql(r"SELECT length('\ud83d') AS l, hex('\ud83d') AS h"))
    assert table.column("l").to_pylist() == [1]
    assert table.column("h").to_pylist() == ["3F"]


def test_max_scalar_u10ffff_is_one_char(spark: ReparkSession) -> None:
    """BL12-4: ``'\\U0010FFFF'`` has length 1 and hex ``F48FBFBF``."""
    table = _table(spark.sql(r"SELECT length('\U0010FFFF') AS l, hex('\U0010FFFF') AS h"))
    assert table.column("l").to_pylist() == [1]
    assert table.column("h").to_pylist() == ["F48FBFBF"]


def test_embedded_out_of_range_counts_both_replacements(
    spark: ReparkSession,
) -> None:
    """BL12-5: ``'a\\U00110000b'`` has length 4 and hex ``613F3F62``."""
    table = _table(spark.sql(r"SELECT length('a\U00110000b') AS l, hex('a\U00110000b') AS h"))
    assert table.column("l").to_pylist() == [4]
    assert table.column("h").to_pylist() == ["613F3F62"]


def test_double_suffix_rint(spark: ReparkSession) -> None:
    """BL6-sql-3: ``rint(2.5D)`` is ``2.0`` as DOUBLE."""
    table = _table(spark.sql("SELECT rint(2.5D) AS v"))
    assert table.column("v").to_pylist() == [2.0]
    assert pa.types.is_float64(table.schema.field("v").type)


def test_double_suffix_ceil(spark: ReparkSession) -> None:
    """DIV-ceil-2: ``ceil(-1.5D)`` is ``-1`` as BIGINT."""
    table = _table(spark.sql("SELECT ceil(-1.5D) AS v"))
    assert table.column("v").to_pylist() == [-1]
    assert pa.types.is_int64(table.schema.field("v").type)


def test_double_suffix_ceiling(spark: ReparkSession) -> None:
    """DIV-ceil-5: ``ceiling(2.1D)`` is ``3`` as BIGINT."""
    table = _table(spark.sql("SELECT ceiling(2.1D) AS v"))
    assert table.column("v").to_pylist() == [3]
    assert pa.types.is_int64(table.schema.field("v").type)


def test_double_suffix_round_half_up(spark: ReparkSession) -> None:
    """DIV-round-0: ``round(2.5D)`` is ``3.0`` as DOUBLE."""
    table = _table(spark.sql("SELECT round(2.5D) AS v"))
    assert table.column("v").to_pylist() == [3.0]
    assert pa.types.is_float64(table.schema.field("v").type)


def test_double_suffix_round_negative(spark: ReparkSession) -> None:
    """DIV-round-1: ``round(-2.5D)`` is ``-3.0`` as DOUBLE."""
    table = _table(spark.sql("SELECT round(-2.5D) AS v"))
    assert table.column("v").to_pylist() == [-3.0]


def test_double_suffix_round_scale(spark: ReparkSession) -> None:
    """DIV-round-5: ``round(0.125D, 2)`` is ``0.13`` as DOUBLE."""
    table = _table(spark.sql("SELECT round(0.125D, 2) AS v"))
    assert table.column("v").to_pylist() == [0.13]


def test_double_suffix_sec(spark: ReparkSession) -> None:
    """DIV-sec-0: ``sec(0D)`` is ``1.0`` as DOUBLE."""
    table = _table(spark.sql("SELECT sec(0D) AS v"))
    assert table.column("v").to_pylist() == [1.0]
    assert pa.types.is_float64(table.schema.field("v").type)


def test_double_suffix_csc_zero_is_inf(spark: ReparkSession) -> None:
    """DIV-sec-2: ``csc(0D)`` is ``inf`` as DOUBLE."""
    table = _table(spark.sql("SELECT csc(0D) AS v"))
    assert table.column("v").to_pylist() == [float("inf")]


def test_double_suffix_csc_one(spark: ReparkSession) -> None:
    """DIV-sec-4: ``csc(1D)`` is ``1.1883951057781212`` as DOUBLE."""
    table = _table(spark.sql("SELECT csc(1D) AS v"))
    assert table.column("v").to_pylist() == [1.1883951057781212]


def test_double_suffix_huge_scientific(spark: ReparkSession) -> None:
    """BL16 shape (no ``hypot``): ``1e200D`` is ``1e200`` as DOUBLE."""
    table = _table(spark.sql("SELECT CAST(1e200D AS DOUBLE) AS v"))
    assert table.column("v").to_pylist() == [1e200]
    assert pa.types.is_float64(table.schema.field("v").type)
    assert table.schema.field("v").nullable is False


def test_double_suffix_bare_huge_scientific(spark: ReparkSession) -> None:
    """``SELECT 1e200D`` is ``1e200`` as DOUBLE."""
    table = _table(spark.sql("SELECT 1e200D AS v"))
    assert table.column("v").to_pylist() == [1e200]
    assert pa.types.is_float64(table.schema.field("v").type)


def test_long_suffix_still_bigint(spark: ReparkSession) -> None:
    """``1L`` is ``1`` as BIGINT (kept working across the dialect flip)."""
    table = _table(spark.sql("SELECT 1L AS v"))
    assert table.column("v").to_pylist() == [1]
    assert pa.types.is_int64(table.schema.field("v").type)
    assert table.schema.field("v").nullable is False


def test_long_suffix_binary_hex_refusal(spark: ReparkSession) -> None:
    """BL11 shape: ``hex(CAST(1L AS BINARY))`` still refuses naming BIGINT."""
    with pytest.raises(Exception, match=r"BIGINT"):
        spark.sql("SELECT hex(CAST(1L AS BINARY)) AS h").to_arrow()


def test_smallint_suffix(spark: ReparkSession) -> None:
    """``1S`` is ``1`` as SMALLINT."""
    table = _table(spark.sql("SELECT 1S AS v"))
    assert table.column("v").to_pylist() == [1]
    assert pa.types.is_int16(table.schema.field("v").type)


def test_tinyint_suffix(spark: ReparkSession) -> None:
    """``1Y`` is ``1`` as TINYINT."""
    table = _table(spark.sql("SELECT 1Y AS v"))
    assert table.column("v").to_pylist() == [1]
    assert pa.types.is_int8(table.schema.field("v").type)


def test_float_suffix(spark: ReparkSession) -> None:
    """``1.5F`` is ``1.5`` as FLOAT."""
    table = _table(spark.sql("SELECT 1.5F AS v"))
    assert table.column("v").to_pylist() == [1.5]
    assert pa.types.is_float32(table.schema.field("v").type)


def test_decimal_suffix(spark: ReparkSession) -> None:
    """L9-1.5BD: ``1.5BD`` is ``1.5`` as decimal(2,1) non-null."""
    table = _table(spark.sql("SELECT 1.5BD AS v"))
    assert table.column("v").to_pylist() == [decimal.Decimal("1.5")]
    assert table.schema.field("v").type == pa.decimal128(2, 1)
    assert table.schema.field("v").nullable is False


def test_decimal_suffix_precision_from_digits(spark: ReparkSession) -> None:
    """L9-10BD / L9-0.001BD / L9-1.5e2BD: precision and scale follow the digits."""
    ten = _table(spark.sql("SELECT 10BD AS v"))
    assert ten.schema.field("v").type == pa.decimal128(2, 0)
    assert ten.schema.field("v").nullable is False
    assert ten.column("v").to_pylist() == [decimal.Decimal("10")]
    milli = _table(spark.sql("SELECT 0.001BD AS v"))
    assert milli.schema.field("v").type == pa.decimal128(3, 3)
    assert milli.column("v").to_pylist() == [decimal.Decimal("0.001")]
    shifted = _table(spark.sql("SELECT 1.5e2BD AS v"))
    assert shifted.schema.field("v").type == pa.decimal128(3, 0)
    assert shifted.column("v").to_pylist() == [decimal.Decimal("150")]


def test_negative_zero_bd_answers_plain_zero(spark: ReparkSession) -> None:
    """L-004 control: ``-0.0BD`` is ``decimal(1,1)`` zero, non-null, named."""
    table = _table(spark.sql("SELECT -0.0BD AS v"))
    assert table.column("v").to_pylist() == [decimal.Decimal("0.0")]
    assert table.schema.field("v").type == pa.decimal128(1, 1)
    assert table.schema.field("v").nullable is False
    bare = _table(spark.sql("SELECT -0.0BD"))
    assert bare.schema.names == ["-0.0"]


def test_struct_field_access_on_call_result(spark: ReparkSession) -> None:
    """L9-struct-dot: ``named_struct('a', 1).a`` is ``1`` as INT non-null."""
    table = _table(spark.sql("SELECT named_struct('a', 1).a AS v"))
    assert table.column("v").to_pylist() == [1]
    assert pa.types.is_int32(table.schema.field("v").type)
    assert table.schema.field("v").nullable is False


def test_struct_field_access_on_call_result_via_expr(spark: ReparkSession) -> None:
    """L9-struct-dot through ``F.expr``: same value, type, and non-null."""
    table = _table(spark.range(1).select(F.expr("named_struct('a', 1).a").alias("v")))
    assert table.column("v").to_pylist() == [1]
    assert pa.types.is_int32(table.schema.field("v").type)
    assert table.schema.field("v").nullable is False


def test_chained_struct_field_access(spark: ReparkSession) -> None:
    """L9-struct-chain: nested ``named_struct`` field access is INT non-null."""
    table = _table(spark.sql("SELECT named_struct('s', named_struct('a', 1)).s.a AS v"))
    assert table.column("v").to_pylist() == [1]
    assert pa.types.is_int32(table.schema.field("v").type)
    assert table.schema.field("v").nullable is False


def test_struct_column_field_access(spark: ReparkSession) -> None:
    """L9-struct-col: ``s.a`` on a named struct column stays INT."""
    table = _table(spark.sql("SELECT s.a FROM (SELECT named_struct('a', 1) AS s)"))
    assert table.column(0).to_pylist() == [1]
    assert pa.types.is_int32(table.schema.field(0).type)


def test_typed_numeric_literals_are_non_null(spark: ReparkSession) -> None:
    """L9-2.5D and siblings: suffixed and exponent literals are non-null."""
    cases = [
        ("SELECT 2.5D AS v", pa.float64(), [2.5]),
        ("SELECT 1e200D AS v", pa.float64(), [1e200]),
        ("SELECT .5D AS v", pa.float64(), [0.5]),
        ("SELECT 5.D AS v", pa.float64(), [5.0]),
        ("SELECT 1E-2D AS v", pa.float64(), [0.01]),
        ("SELECT 1e3 AS v", pa.float64(), [1000.0]),
        ("SELECT 1.5F AS v", pa.float32(), [1.5]),
        ("SELECT 10L AS v", pa.int64(), [10]),
        ("SELECT -1S AS v", pa.int16(), [-1]),
        ("SELECT 1Y AS v", pa.int8(), [1]),
    ]
    for sql, data_type, values in cases:
        table = _table(spark.sql(sql))
        assert table.column("v").to_pylist() == values, sql
        assert table.schema.field("v").type == data_type, sql
        assert table.schema.field("v").nullable is False, sql


def test_out_of_range_integer_suffix_raises(spark: ReparkSession) -> None:
    """L9-128Y / L9-40000S: Spark ``[INVALID_NUMERIC_LITERAL_RANGE]``."""
    with pytest.raises(Exception, match="INVALID_NUMERIC_LITERAL_RANGE"):
        spark.sql("SELECT 128Y AS v").to_arrow()
    with pytest.raises(Exception, match="INVALID_NUMERIC_LITERAL_RANGE"):
        spark.sql("SELECT 40000S AS v").to_arrow()


def test_signed_integer_suffix_minima_answer(spark: ReparkSession) -> None:
    """L-002: ``-128Y`` / ``-32768S`` are the typed minima, non-null."""
    cases = [
        ("SELECT -128Y AS v", pa.int8(), [-128]),
        ("SELECT -32768S AS v", pa.int16(), [-32768]),
        ("SELECT -1Y AS v", pa.int8(), [-1]),
        ("SELECT 127Y AS v", pa.int8(), [127]),
        ("SELECT -1S AS v", pa.int16(), [-1]),
        ("SELECT 32767S AS v", pa.int16(), [32767]),
        ("SELECT -1L AS v", pa.int64(), [-1]),
    ]
    for sql, data_type, values in cases:
        table = _table(spark.sql(sql))
        assert table.column("v").to_pylist() == values, sql
        assert table.schema.field("v").type == data_type, sql
        assert table.schema.field("v").nullable is False, sql
    with pytest.raises(Exception, match="INVALID_NUMERIC_LITERAL_RANGE"):
        spark.sql("SELECT -129Y AS v").to_arrow()
    with pytest.raises(Exception, match="INVALID_NUMERIC_LITERAL_RANGE"):
        spark.sql("SELECT -32769S AS v").to_arrow()


def test_signed_minima_on_all_doors(spark: ReparkSession) -> None:
    """L-002: the minima answer on ``F.expr`` and ``selectExpr`` too."""
    table = _table(spark.range(1).select(F.expr("-128Y").alias("v")))
    assert table.column("v").to_pylist() == [-128]
    assert table.schema.field("v").type == pa.int8()
    assert table.schema.field("v").nullable is False
    table = _table(spark.range(1).select(F.expr("-32768S").alias("v")))
    assert table.column("v").to_pylist() == [-32768]
    assert table.schema.field("v").type == pa.int16()
    assert table.schema.field("v").nullable is False
    table = _table(spark.range(1).selectExpr("-128Y AS v"))
    assert table.column("v").to_pylist() == [-128]
    assert table.schema.field("v").type == pa.int8()
    assert table.schema.field("v").nullable is False
    table = _table(spark.range(1).selectExpr("-32768S AS v"))
    assert table.column("v").to_pylist() == [-32768]
    assert table.schema.field("v").type == pa.int16()
    assert table.schema.field("v").nullable is False


def test_bigint_overflow_raises_invalid_numeric_literal(spark: ReparkSession) -> None:
    """L-004: ``L`` overflow refuses at parse, not as an optimizer cast error."""
    for sql in [
        "SELECT 9223372036854775808L AS v",
        "SELECT -9223372036854775809L AS v",
        "SELECT -(9223372036854775808L) AS v",
    ]:
        with pytest.raises(Exception, match="INVALID_NUMERIC_LITERAL_RANGE"):
            spark.sql(sql).to_arrow()


def test_exponent_l_and_zero_x_are_unresolved_identifiers(spark: ReparkSession) -> None:
    """L9-1e3L / L9-0x1D: Spark reads these as identifiers, not typed literals."""
    with pytest.raises(Exception, match=r"1e3L|No field named"):
        spark.sql("SELECT 1e3L AS v").to_arrow()
    with pytest.raises(Exception, match=r"0x1D|0x1d|No field named"):
        spark.sql("SELECT 0x1D AS v").to_arrow()
    with pytest.raises(Exception, match=r"0x1d|No field named"):
        spark.sql("SELECT 0x1d AS v").to_arrow()
    table = _table(spark.sql("SELECT X'1D' AS v"))
    assert table.column("v").to_pylist() == [b"\x1d"]
    ident = _table(spark.sql("SELECT a1d FROM (SELECT 7 AS a1d)"))
    assert ident.column("a1d").to_pylist() == [7]
    text = _table(spark.sql("SELECT '2.5D' AS v"))
    assert text.column("v").to_pylist() == ["2.5D"]


def test_exponent_literals_are_double(spark: ReparkSession) -> None:
    """JD-exp-literal-types: exponent literals are DOUBLE, plain ``1.5`` stays DECIMAL."""
    table = _table(
        spark.sql("SELECT 1.0E6 AS a, 1E2 AS b, 1.5 AS c, 1e-3 AS d, 1.0E21 AS e, 4.9E-324 AS f")
    )
    assert table.column("a").to_pylist() == [1_000_000.0]
    assert table.column("b").to_pylist() == [100.0]
    assert table.column("c").to_pylist() == [decimal.Decimal("1.5")]
    assert table.column("d").to_pylist() == [0.001]
    assert table.column("e").to_pylist() == [1e21]
    assert table.column("f").to_pylist() == [5e-324]
    for name in ("a", "b", "d", "e", "f"):
        assert pa.types.is_float64(table.schema.field(name).type)
    assert table.schema.field("c").type == pa.decimal128(2, 1)


def test_exponent_literal_cast_is_double(spark: ReparkSession) -> None:
    """JD-exp-literal-cast: ``CAST(1.0E6 AS DOUBLE)`` is ``1000000.0`` (collect, not string)."""
    table = _table(spark.sql("SELECT CAST(1.0E6 AS DOUBLE) AS v"))
    assert table.column("v").to_pylist() == [1_000_000.0]
    assert pa.types.is_float64(table.schema.field("v").type)


def test_exponent_literals_are_double_via_expr(spark: ReparkSession) -> None:
    """JD-exp-literal-types/cast through ``F.expr``: same values and types."""
    table = _table(spark.range(1).select(F.expr("1.0E6").alias("v")))
    assert table.column("v").to_pylist() == [1_000_000.0]
    assert pa.types.is_float64(table.schema.field("v").type)
    table = _table(spark.range(1).select(F.expr("CAST(1.0E6 AS DOUBLE)").alias("w")))
    assert table.column("w").to_pylist() == [1_000_000.0]
    assert pa.types.is_float64(table.schema.field("w").type)

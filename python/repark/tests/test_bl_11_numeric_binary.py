"""BL-11 numeric to BINARY under runtime ANSI — batch-17 oracle pins.

Oracle: ``/tmp/oc-worker/rc-oracle/fixtures-batch17-bl11-binary.json`` (live PySpark
4.1.2, ``local[2]``, session zone UTC): every expression under
``spark.sql.ansi.enabled=false`` and again under ``true``. ANSI off encodes only the
integrals as big-endian bytes of natural width; FLOAT, DOUBLE, DECIMAL, BOOLEAN, DATE,
TIMESTAMP and INTERVAL refuse in both modes with
``DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION``; ANSI on refuses the integrals with
``DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION`` plus the sentence naming
``spark.sql.ansi.enabled`` as ``'false'``.

Every assertion runs on the Arrow path (value AND Arrow type AND field nullability) on
both doors: ``spark.sql`` carries one case per oracle cell id, and the Python
``Column``/``DataFrame`` API carries representatives. ``CAST('ab' AS BINARY)`` is the
C-005 control that works in both modes; the old every-mode refusal pin
``test_sqp_1_string_literals.py::test_numeric_to_binary_refuses`` flips in place to the
ANSI-on behaviour.

pins: bl-11-numeric-binary/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

ANSI_KEY = "spark.sql.ansi.enabled"

_REFUSAL = (AnalysisException, PySparkException)


def _session() -> ReparkSession:
    """One session on the B11 oracle basis: ANSI on, explicit."""
    return (
        ReparkSession.builder.appName("bl-11-numeric-binary")
        .config(ANSI_KEY, "true")
        .getOrCreate()
    )


def _ansi_off(spark: ReparkSession) -> None:
    """Flip the live session to ANSI off through runtime SET."""
    spark.sql("SET spark.sql.ansi.enabled=false")


def _table(frame: Any) -> pa.Table:
    """Materialize a frame on the Arrow path."""
    return frame.to_arrow()


def _assert_bytes(table: pa.Table, expected: bytes | None, nullable: bool) -> None:
    """Pin a BINARY answer in value AND Arrow type AND nullability."""
    assert table.schema.field("b").type == pa.binary()
    assert table.schema.field("b").nullable is nullable
    assert table.column("b").to_pylist() == [expected]


@pytest.mark.parametrize(
    ("cell", "expr", "expected"),
    [
        pytest.param("B11-tinyint", "CAST(CAST(1 AS TINYINT) AS BINARY)", b"\x01", id="B11-tinyint"),
        pytest.param(
            "B11-smallint", "CAST(CAST(1 AS SMALLINT) AS BINARY)", b"\x00\x01", id="B11-smallint"
        ),
        pytest.param(
            "B11-int", "CAST(1 AS BINARY)", b"\x00\x00\x00\x01", id="B11-int"
        ),
        pytest.param(
            "B11-int-neg", "CAST(-1 AS BINARY)", b"\xff\xff\xff\xff", id="B11-int-neg"
        ),
        pytest.param(
            "B11-int-big",
            "CAST(305419896 AS BINARY)",
            b"\x12\x34\x56\x78",
            id="B11-int-big",
        ),
        pytest.param(
            "B11-bigint",
            "CAST(CAST(1 AS BIGINT) AS BINARY)",
            b"\x00\x00\x00\x00\x00\x00\x00\x01",
            id="B11-bigint",
        ),
    ],
)
def test_c001_sql_door_integrals_encode_big_endian(
    cell: str, expr: str, expected: bytes
) -> None:
    """C-001: the ANSI-off integral cells encode big-endian at natural width."""
    del cell
    spark = _session()
    _ansi_off(spark)
    table = _table(spark.sql(f"SELECT {expr} AS b"))
    _assert_bytes(table, expected, False)
    spark.stop()


def test_c001_sql_door_null_int_stays_null() -> None:
    """C-001 B11-null-int: ``CAST(CAST(NULL AS INT) AS BINARY)`` is NULL, nullable."""
    spark = _session()
    _ansi_off(spark)
    table = _table(spark.sql("SELECT CAST(CAST(NULL AS INT) AS BINARY) AS b"))
    _assert_bytes(table, None, True)
    spark.stop()


def test_c001_python_door_int_encodes() -> None:
    """C-001 B11-int on the Python door: ``F.lit(1).cast("binary")`` is ``00000001``."""
    spark = _session()
    _ansi_off(spark)
    table = _table(spark.range(1).select(F.lit(1).cast("binary").alias("b")))
    _assert_bytes(table, b"\x00\x00\x00\x01", False)
    spark.stop()


def test_c001_python_door_small_types_encode() -> None:
    """C-001 B11-tinyint/B11-smallint on the Python door: 1 and 2 byte widths."""
    spark = _session()
    _ansi_off(spark)
    tiny = _table(spark.range(1).select(F.lit(1).cast("tinyint").cast("binary").alias("b")))
    _assert_bytes(tiny, b"\x01", False)
    small = _table(
        spark.range(1).select(F.lit(1).cast("smallint").cast("binary").alias("b"))
    )
    _assert_bytes(small, b"\x00\x01", False)
    spark.stop()


def test_c001_python_door_bigint_and_negative() -> None:
    """C-001 B11-bigint/B11-int-neg on the Python door: 8 bytes, two's complement."""
    spark = _session()
    _ansi_off(spark)
    big = _table(spark.range(1).select(F.lit(1).cast("bigint").cast("binary").alias("b")))
    _assert_bytes(big, b"\x00\x00\x00\x00\x00\x00\x00\x01", False)
    neg = _table(spark.range(1).select(F.lit(-1).cast("binary").alias("b")))
    _assert_bytes(neg, b"\xff\xff\xff\xff", False)
    spark.stop()


def test_c001_python_door_column_values_and_null() -> None:
    """C-001 on a real BIGINT column: the encoder Array path answers per row incl. NULL."""
    spark = _session()
    _ansi_off(spark)
    frame = spark.createDataFrame([(1,), (-1,), (None,)], ["x"]).select(
        F.col("x").cast("binary").alias("b")
    )
    table = _table(frame)
    assert table.schema.field("b").type == pa.binary()
    assert table.schema.field("b").nullable is True
    assert table.column("b").to_pylist() == [
        b"\x00\x00\x00\x00\x00\x00\x00\x01",
        b"\xff\xff\xff\xff\xff\xff\xff\xff",
        None,
    ]
    spark.stop()


@pytest.mark.parametrize(
    ("cell", "expr", "source"),
    [
        pytest.param(
            "B11-float", "CAST(CAST(1.5 AS FLOAT) AS BINARY)", "FLOAT", id="B11-float"
        ),
        pytest.param(
            "B11-double", "CAST(CAST(1.5 AS DOUBLE) AS BINARY)", "DOUBLE", id="B11-double"
        ),
        pytest.param(
            "B11-decimal",
            "CAST(CAST(1.5 AS DECIMAL(10,2)) AS BINARY)",
            "DECIMAL(10,2)",
            id="B11-decimal",
        ),
        pytest.param("B11-boolean", "CAST(true AS BINARY)", "BOOLEAN", id="B11-boolean"),
        pytest.param(
            "B11-date", "CAST(DATE'2024-01-01' AS BINARY)", "DATE", id="B11-date"
        ),
        pytest.param(
            "B11-ts",
            "CAST(TIMESTAMP'2024-01-01 00:00:00' AS BINARY)",
            "TIMESTAMP",
            id="B11-ts",
        ),
        pytest.param(
            "B11-interval",
            "CAST(INTERVAL '1' DAY AS BINARY)",
            "INTERVAL DAY",
            id="B11-interval",
        ),
    ],
)
def test_c002_sql_door_never_castable_refuses_ansi_off(
    cell: str, expr: str, source: str
) -> None:
    """C-002: ANSI off still refuses the never-castable sources, WITHOUT_SUGGESTION."""
    del cell
    spark = _session()
    _ansi_off(spark)
    with pytest.raises(_REFUSAL) as excinfo:
        spark.sql(f"SELECT {expr} AS b").to_arrow()
    message = str(excinfo.value)
    assert "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION" in message
    assert f'cannot cast "{source}" to "BINARY"' in message
    assert "SQLSTATE: 42K09" in message
    spark.stop()


def test_c002_python_door_double_refuses_ansi_off() -> None:
    """C-002 B11-double on the Python door: ``F.lit(1.5).cast("binary")`` refuses."""
    spark = _session()
    _ansi_off(spark)
    with pytest.raises(_REFUSAL) as excinfo:
        spark.range(1).select(F.lit(1.5).cast("binary").alias("b"))
    message = str(excinfo.value)
    assert "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION" in message
    assert 'cannot cast "DOUBLE" to "BINARY"' in message
    spark.stop()


@pytest.mark.parametrize(
    ("cell", "expr", "source"),
    [
        pytest.param(
            "B11-tinyint", "CAST(CAST(1 AS TINYINT) AS BINARY)", "TINYINT", id="B11-tinyint"
        ),
        pytest.param(
            "B11-smallint",
            "CAST(CAST(1 AS SMALLINT) AS BINARY)",
            "SMALLINT",
            id="B11-smallint",
        ),
        pytest.param("B11-int", "CAST(1 AS BINARY)", "INT", id="B11-int"),
        pytest.param("B11-int-neg", "CAST(-1 AS BINARY)", "INT", id="B11-int-neg"),
        pytest.param(
            "B11-int-big", "CAST(305419896 AS BINARY)", "INT", id="B11-int-big"
        ),
        pytest.param(
            "B11-bigint",
            "CAST(CAST(1 AS BIGINT) AS BINARY)",
            "BIGINT",
            id="B11-bigint",
        ),
        pytest.param(
            "B11-null-int",
            "CAST(CAST(NULL AS INT) AS BINARY)",
            "INT",
            id="B11-null-int",
        ),
    ],
)
def test_c003_sql_door_integrals_refuse_ansi_on(cell: str, expr: str, source: str) -> None:
    """C-003: ANSI on refuses the integrals with CAST_WITH_CONF_SUGGESTION + conf remedy."""
    del cell
    spark = _session()
    with pytest.raises(_REFUSAL) as excinfo:
        spark.sql(f"SELECT {expr} AS b").to_arrow()
    message = str(excinfo.value)
    assert "DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION" in message
    assert f'cannot cast "{source}" to "BINARY" with ANSI mode on' in message
    assert f'If you have to cast "{source}" to "BINARY"' in message
    assert '"spark.sql.ansi.enabled" as \'false\'' in message
    assert "SQLSTATE: 42K09" in message
    spark.stop()


def test_c003_python_door_int_refuses_ansi_on() -> None:
    """C-003 B11-int on the Python door: ``F.lit(1).cast("binary")`` names the conf."""
    spark = _session()
    with pytest.raises(_REFUSAL) as excinfo:
        spark.range(1).select(F.lit(1).cast("binary").alias("b"))
    message = str(excinfo.value)
    assert "DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION" in message
    assert 'cannot cast "INT" to "BINARY" with ANSI mode on' in message
    assert '"spark.sql.ansi.enabled" as \'false\'' in message
    spark.stop()


@pytest.mark.parametrize(
    ("cell", "expr", "source"),
    [
        pytest.param(
            "B11-float", "CAST(CAST(1.5 AS FLOAT) AS BINARY)", "FLOAT", id="B11-float"
        ),
        pytest.param(
            "B11-double", "CAST(CAST(1.5 AS DOUBLE) AS BINARY)", "DOUBLE", id="B11-double"
        ),
        pytest.param(
            "B11-decimal",
            "CAST(CAST(1.5 AS DECIMAL(10,2)) AS BINARY)",
            "DECIMAL(10,2)",
            id="B11-decimal",
        ),
        pytest.param("B11-boolean", "CAST(true AS BINARY)", "BOOLEAN", id="B11-boolean"),
        pytest.param(
            "B11-date", "CAST(DATE'2024-01-01' AS BINARY)", "DATE", id="B11-date"
        ),
        pytest.param(
            "B11-ts",
            "CAST(TIMESTAMP'2024-01-01 00:00:00' AS BINARY)",
            "TIMESTAMP",
            id="B11-ts",
        ),
        pytest.param(
            "B11-interval",
            "CAST(INTERVAL '1' DAY AS BINARY)",
            "INTERVAL DAY",
            id="B11-interval",
        ),
    ],
)
def test_c003_sql_door_never_castable_refuses_ansi_on(
    cell: str, expr: str, source: str
) -> None:
    """C-003: ANSI on keeps WITHOUT_SUGGESTION for the never-castable sources."""
    del cell
    spark = _session()
    with pytest.raises(_REFUSAL) as excinfo:
        spark.sql(f"SELECT {expr} AS b").to_arrow()
    message = str(excinfo.value)
    assert "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION" in message
    assert f'cannot cast "{source}" to "BINARY"' in message
    assert "CAST_WITH_CONF_SUGGESTION" not in message
    spark.stop()


def test_c004_stale_encode_frame_survives_set_to_on() -> None:
    """C-004: a frame analysed under ANSI off still encodes after the SET flips on."""
    spark = _session()
    _ansi_off(spark)
    frame = spark.sql("SELECT CAST(1 AS BINARY) AS b")
    spark.sql("SET spark.sql.ansi.enabled=true")
    _assert_bytes(_table(frame), b"\x00\x00\x00\x01", False)
    spark.stop()


def test_c004_python_door_stale_frame_survives_set_to_on() -> None:
    """C-004 on the Python door: the build-time encode survives the later SET."""
    spark = _session()
    _ansi_off(spark)
    frame = spark.range(1).select(F.lit(1).cast("binary").alias("b"))
    spark.sql("SET spark.sql.ansi.enabled=true")
    _assert_bytes(_table(frame), b"\x00\x00\x00\x01", False)
    spark.stop()


def test_c004_refusal_binds_at_build() -> None:
    """C-004: the ANSI-on refusal binds at build; a fresh frame after the SET encodes."""
    spark = _session()
    with pytest.raises(_REFUSAL, match="CAST_WITH_CONF_SUGGESTION"):
        spark.sql("SELECT CAST(1 AS BINARY) AS b")
    _ansi_off(spark)
    _assert_bytes(_table(spark.sql("SELECT CAST(1 AS BINARY) AS b")), b"\x00\x00\x00\x01", False)
    spark.stop()


@pytest.mark.parametrize("ansi", ["false", "true"])
def test_c005_string_cast_works_in_both_modes_sql_door(ansi: str) -> None:
    """C-005 B11-string: ``CAST('ab' AS BINARY)`` is ``6162`` in both modes."""
    spark = _session()
    spark.sql(f"SET spark.sql.ansi.enabled={ansi}")
    _assert_bytes(_table(spark.sql("SELECT CAST('ab' AS BINARY) AS b")), b"ab", False)
    spark.stop()


@pytest.mark.parametrize("ansi", ["false", "true"])
def test_c005_string_cast_works_in_both_modes_python_door(ansi: str) -> None:
    """C-005 B11-string on the Python door: ``F.lit("ab").cast("binary")`` in both modes."""
    spark = _session()
    spark.sql(f"SET spark.sql.ansi.enabled={ansi}")
    table = _table(spark.range(1).select(F.lit("ab").cast("binary").alias("b")))
    _assert_bytes(table, b"ab", False)
    spark.stop()

"""G15 — collation refuses loudly (owner ruling 2026-08-12).

Every compare/order-changing path in the Y-7 inventory (plus Spark JSON
``__COLLATIONS``) either refuses with an actionable message (requested
collation, repark does not implement, use binary/default) or is proven absent.
``StringType(collation=…)`` construction and ``simpleString`` stay legal (A5);
first evaluation refuses. SQL ``CAST(x AS STRING COLLATE name)`` is G15 via a
quote-aware type-position scan (sqlparser cannot attach COLLATE inside CAST).

``F.collate`` / ``F.collation`` / ``Column.collate`` are not on the facade —
absence is already loud (AttributeError); this module documents that rather
than adding refusing stubs.
"""

from __future__ import annotations

import pytest

from repark.errors import UnsupportedOperationException
from repark.session import ReparkSession
from repark.types import (
    COLLATION_REFUSAL_NEEDLE,
    StringType,
    StructField,
    StructType,
    refuse_evaluated_collation,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("g15-collation-refuse").getOrCreate()
    yield session
    session.stop()


def assert_g15(exc: BaseException, requested: str) -> None:
    text = str(exc)
    assert COLLATION_REFUSAL_NEEDLE in text, text
    assert requested in text, text
    assert "binary/default" in text, text


# --- createDataFrame (the silently-wrong-count path) -----------------------------------------


def test_create_dataframe_unicode_ci_refuses(spark: ReparkSession) -> None:
    """Spark's test_create_df_with_collation: UNICODE_CI makes Alice==alice (count 1)."""
    schema = StructType([StructField("name", StringType("UNICODE_CI"), True)])
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.createDataFrame([("Alice",), ("alice",)], schema)
    assert_g15(caught.value, "UNICODE_CI")


def test_create_dataframe_utf8_lcase_refuses(spark: ReparkSession) -> None:
    schema = StructType([StructField("name", StringType("UTF8_LCASE"), True)])
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.createDataFrame([("Alice",)], schema)
    assert_g15(caught.value, "UTF8_LCASE")


def test_create_dataframe_ddl_collate_refuses(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.createDataFrame([("Alice",)], "name STRING COLLATE UNICODE_CI")
    assert_g15(caught.value, "UNICODE_CI")


def test_from_json_collations_metadata_constructs_and_create_refuses(
    spark: ReparkSession,
) -> None:
    """Spark JSON ``__COLLATIONS`` must become StringType and then refuse (Q-003 / SEC-001)."""
    field = StructField.fromJson(
        {
            "name": "name",
            "type": "string",
            "nullable": True,
            "metadata": {"__COLLATIONS": {"name": "icu.UNICODE_CI"}},
        }
    )
    assert field.dataType == StringType("UNICODE_CI")
    assert field.metadata.get("__COLLATIONS") is None
    schema = StructType([field])
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.createDataFrame([("Alice",), ("alice",)], schema)
    assert_g15(caught.value, "UNICODE_CI")


def test_from_json_bare_collations_name_refuses(spark: ReparkSession) -> None:
    """Non-Spark ``__COLLATIONS`` payload (bare name, no provider) still refuses."""
    field = StructField.fromJson(
        {
            "name": "name",
            "type": "string",
            "nullable": True,
            "metadata": {"__COLLATIONS": {"name": "UTF8_LCASE"}},
        }
    )
    assert field.dataType == StringType("UTF8_LCASE")
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.createDataFrame([("Alice",)], StructType([field]))
    assert_g15(caught.value, "UTF8_LCASE")


def test_create_dataframe_default_string_type_untouched(spark: ReparkSession) -> None:
    """Non-COLLATE path: Alice and alice remain two distinct binary strings."""
    schema = StructType([StructField("name", StringType(), True)])
    frame = spark.createDataFrame([("Alice",), ("alice",)], schema)
    assert frame.select("name").distinct().count() == 2
    table = frame.to_arrow()
    assert table.schema.field(0).type.to_pandas_dtype() is not None
    assert table.column(0).to_pylist() == ["Alice", "alice"]


# --- Column.cast / try_cast (A4) --------------------------------------------------------------


def test_cast_collated_string_type_refuses(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice",)], ["name"])
    with pytest.raises(UnsupportedOperationException) as caught:
        frame.select(frame["name"].cast(StringType("UTF8_LCASE"))).collect()
    assert_g15(caught.value, "UTF8_LCASE")


def test_try_cast_collated_string_type_refuses(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice",)], ["name"])
    with pytest.raises(UnsupportedOperationException) as caught:
        frame.select(frame["name"].try_cast(StringType("UNICODE"))).collect()
    assert_g15(caught.value, "UNICODE")


def test_cast_string_collate_type_token_refuses(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice",)], ["name"])
    with pytest.raises(UnsupportedOperationException) as caught:
        frame.select(frame["name"].cast("string collate UTF8_LCASE")).collect()
    assert_g15(caught.value, "UTF8_LCASE")


# --- Spark SQL door (parse altitude) ----------------------------------------------------------


def test_sql_collate_expression_refuses(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("SELECT 'Alice' COLLATE UTF8_LCASE").collect()
    assert_g15(caught.value, "UTF8_LCASE")


def test_sql_order_by_collate_refuses(spark: ReparkSession) -> None:
    spark.createDataFrame([("b",), ("a",)], ["name"]).createOrReplaceTempView("t")
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("SELECT name FROM t ORDER BY name COLLATE UNICODE_CI").collect()
    assert_g15(caught.value, "UNICODE_CI")


def test_sql_cast_as_string_collate_refuses(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("SELECT CAST('Alice' AS STRING COLLATE UTF8_LCASE)").collect()
    assert_g15(caught.value, "UTF8_LCASE")


def test_sql_set_collation_key_refuses(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("SET spark.sql.collation.objectLevel.enabled = true")
    assert_g15(caught.value, "spark.sql.collation.objectLevel.enabled")


def test_sql_reset_collation_key_refuses(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("RESET spark.sql.collation.objectLevel.enabled")
    assert_g15(caught.value, "spark.sql.collation.objectLevel.enabled")


def test_sql_collate_inside_literal_untouched(spark: ReparkSession) -> None:
    rows = spark.sql("SELECT 'COLLATE UTF8_LCASE' AS note").collect()
    assert rows[0][0] == "COLLATE UTF8_LCASE"


# --- F.expr / filter SQL-string (binding; not core.py) ----------------------------------------


def test_expr_collate_refuses(spark: ReparkSession) -> None:
    from repark import functions as F  # noqa: N812 — PySpark idiom

    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr("'Alice' COLLATE UTF8_LCASE")
    assert_g15(caught.value, "UTF8_LCASE")


def test_filter_sql_collate_refuses(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Alice",), ("alice",)], ["name"])
    with pytest.raises(UnsupportedOperationException) as caught:
        frame.filter("name COLLATE UTF8_LCASE = 'alice'").collect()
    assert_g15(caught.value, "UTF8_LCASE")


# --- Session / builder conf (silent-ignore path) ----------------------------------------------


def test_conf_set_collation_key_refuses(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.conf.set("spark.sql.collation.objectLevel.enabled", "true")
    assert_g15(caught.value, "spark.sql.collation.objectLevel.enabled")


def test_builder_config_collation_key_refuses() -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        ReparkSession.builder.config("spark.sql.collation.schemaLevel.enabled", "true")
    assert_g15(caught.value, "spark.sql.collation.schemaLevel.enabled")


def test_get_or_create_reuse_refuses_planted_collation_key() -> None:
    """SEC-003: reuse fold must not store a planted collation key (bypasses conf.set)."""
    session = ReparkSession.builder.appName("g15-reuse-fold").getOrCreate()
    try:
        later = ReparkSession.builder.appName("g15-reuse-fold")
        later._config["spark.sql.collation.trim.enabled"] = "true"
        with pytest.raises(UnsupportedOperationException) as caught:
            later.getOrCreate()
        assert_g15(caught.value, "spark.sql.collation.trim.enabled")
    finally:
        session.stop()


# --- Construction + absence (A5) --------------------------------------------------------------


def test_string_type_construction_and_simple_string_untouched() -> None:
    """A5: constructor + simpleString stay; evaluation (not this pin) refuses."""
    assert StringType().simpleString() == "string"
    assert StringType("UTF8_BINARY").simpleString() == "string"
    assert StringType("UTF8_LCASE").simpleString() == "string collate UTF8_LCASE"
    assert StringType("UNICODE").simpleString() == "string collate UNICODE"
    # Evaluation helper is what fires — construction itself does not.
    refuse_evaluated_collation(StringType())
    with pytest.raises(UnsupportedOperationException):
        refuse_evaluated_collation(StringType("UTF8_LCASE"))


def test_collate_and_collation_functions_are_absent() -> None:
    """ABSENCE IS LOUD: Spark's F.collate / F.collation are not stubbed."""
    import repark.functions as F  # noqa: N812 — PySpark idiom

    with pytest.raises(AttributeError, match="collate"):
        _ = F.collate  # type: ignore[attr-defined]
    with pytest.raises(AttributeError, match="collation"):
        _ = F.collation  # type: ignore[attr-defined]
    from repark.column import Column

    assert not hasattr(Column, "collate")

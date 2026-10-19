"""FNP-15/16 — declared-absent Spark functions refuse loudly.

pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
C-010, C-011, C-012, C-013, C-016, C-017
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from repark.errors import UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.functions_declared import (
    CSV_XML_XPATH_NAMES,
    GEOSPATIAL_NAMES,
    SKETCH_NAMES,
    VARIANT_NAMES,
)
from repark.spark.session import ReparkSession

FNP15_NAMES: tuple[str, ...] = (
    "java_method",
    "reflect",
    "try_reflect",
    "unwrap_udt",
    "input_file_block_start",
    "input_file_block_length",
)

FNP15_NEEDLES: dict[str, tuple[str, ...]] = {
    "java_method": ("unreachable", "Java class", "reflection"),
    "reflect": ("unreachable", "CallMethodViaReflection"),
    "try_reflect": ("unreachable", "exception-to-NULL"),
    "unwrap_udt": ("unreachable", "UserDefinedType"),
    "input_file_block_start": ("unreachable", "InputFileBlockHolder", "input_file_name"),
    "input_file_block_length": ("unreachable", "InputFileBlockHolder", "input_file_name"),
}

STILL_ABSENT: str = "aes_encrypt"


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("fnp-15-16-refuse").getOrCreate()
    yield session
    session.stop()


def _assert_declared_refusal(exc: BaseException, name: str) -> None:
    text = str(exc)
    assert isinstance(exc, UnsupportedOperationException), type(exc)
    assert name in text, text
    for needle in FNP15_NEEDLES[name]:
        assert needle in text, f"missing {needle!r} in {text}"
    assert "unreachable" in text, text
    assert "deferred by cost" not in text, text


def _sql_call(name: str) -> str:
    return f"SELECT {name}(1)"


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_facade_attribute_refuses_with_registry_reason(name: str) -> None:
    """Facade Column API raises UnsupportedOperationException, not AttributeError."""
    fn: Callable[..., Any] = getattr(F, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_declared_refusal(caught.value, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_sql_functions_reexport_refuses(name: str) -> None:
    """sed-swap path ``repark.spark.sql.functions`` carries the same stub."""
    from repark.spark.sql import functions as sql_functions

    fn: Callable[..., Any] = getattr(sql_functions, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_declared_refusal(caught.value, name)
    assert getattr(sql_functions, name) is getattr(F, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_spark_sql_door_refuses(name: str, spark: ReparkSession) -> None:
    """Spark SQL door parse-altitude refusal names the same mechanism."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(_sql_call(name)).collect()
    _assert_declared_refusal(caught.value, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_ansi_sql_door_refuses(name: str) -> None:
    """ANSI door parse-altitude refusal names the same mechanism."""
    import repark

    with pytest.raises(UnsupportedOperationException) as caught:
        repark.sql(_sql_call(name))
    _assert_declared_refusal(caught.value, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_expr_fragment_refuses(name: str) -> None:
    """``F.expr`` bypasses the router; the fragment valve must still fire."""
    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr(f"{name}(1)")
    _assert_declared_refusal(caught.value, name)


def _assert_deferred_cost(exc: BaseException, name: str, *needles: str) -> None:
    text = str(exc)
    assert isinstance(exc, UnsupportedOperationException), type(exc)
    assert name in text, text
    assert "reachable without a JVM" in text, text
    assert "deferred by cost" in text, text
    assert "unreachable" not in text, text
    for needle in needles:
        assert needle in text, f"missing {needle!r} in {text}"


@pytest.mark.parametrize("name", SKETCH_NAMES)
def test_sketch_facade_refuses_deferred_by_cost(name: str) -> None:
    """Each sketch name is reachable and deferred by cost, not unreachable."""
    fn: Callable[..., Any] = getattr(F, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "DataSketches")


@pytest.mark.parametrize("name", SKETCH_NAMES)
def test_sketch_sql_functions_reexport_refuses(name: str) -> None:
    from repark.spark.sql import functions as sql_functions

    fn: Callable[..., Any] = getattr(sql_functions, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "DataSketches")


@pytest.mark.parametrize("name", SKETCH_NAMES)
def test_sketch_expr_fragment_refuses(name: str) -> None:
    """``F.expr`` shares ``refuse_sql_fragment``; pin every family, not only FNP-15."""
    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr(f"{name}(1)")
    _assert_deferred_cost(caught.value, name, "DataSketches")


@pytest.mark.parametrize("name", SKETCH_NAMES)
def test_sketch_spark_sql_door_refuses(name: str, spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(_sql_call(name)).collect()
    _assert_deferred_cost(caught.value, name, "DataSketches")


@pytest.mark.parametrize("name", SKETCH_NAMES)
def test_sketch_ansi_sql_door_refuses(name: str) -> None:
    import repark

    with pytest.raises(UnsupportedOperationException) as caught:
        repark.sql(_sql_call(name))
    _assert_deferred_cost(caught.value, name, "DataSketches")


@pytest.mark.parametrize("name", CSV_XML_XPATH_NAMES)
def test_csv_xml_xpath_facade_refuses_deferred_by_cost(name: str) -> None:
    fn: Callable[..., Any] = getattr(F, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "XPath")


@pytest.mark.parametrize("name", CSV_XML_XPATH_NAMES)
def test_csv_xml_xpath_sql_functions_reexport_refuses(name: str) -> None:
    """sed-swap path ``repark.spark.sql.functions`` carries the same stub."""
    from repark.spark.sql import functions as sql_functions

    fn: Callable[..., Any] = getattr(sql_functions, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "XPath")
    assert getattr(sql_functions, name) is getattr(F, name)


@pytest.mark.parametrize("name", CSV_XML_XPATH_NAMES)
def test_csv_xml_xpath_expr_fragment_refuses(name: str) -> None:
    """``F.expr`` shares ``refuse_sql_fragment``; pin every family, not only FNP-15."""
    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr(f"{name}(1)")
    _assert_deferred_cost(caught.value, name, "XPath")


@pytest.mark.parametrize("name", CSV_XML_XPATH_NAMES)
def test_csv_xml_xpath_spark_sql_door_refuses(name: str, spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(_sql_call(name)).collect()
    _assert_deferred_cost(caught.value, name, "XPath")


@pytest.mark.parametrize("name", CSV_XML_XPATH_NAMES)
def test_csv_xml_xpath_ansi_sql_door_refuses(name: str) -> None:
    import repark

    with pytest.raises(UnsupportedOperationException) as caught:
        repark.sql(_sql_call(name))
    _assert_deferred_cost(caught.value, name, "XPath")


@pytest.mark.parametrize("name", VARIANT_NAMES)
def test_variant_facade_refuses_deferred_by_cost(name: str) -> None:
    fn: Callable[..., Any] = getattr(F, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "VARIANT")


@pytest.mark.parametrize("name", VARIANT_NAMES)
def test_variant_sql_functions_reexport_refuses(name: str) -> None:
    """sed-swap path ``repark.spark.sql.functions`` carries the same stub."""
    from repark.spark.sql import functions as sql_functions

    fn: Callable[..., Any] = getattr(sql_functions, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "VARIANT")
    assert getattr(sql_functions, name) is getattr(F, name)


@pytest.mark.parametrize("name", VARIANT_NAMES)
def test_variant_expr_fragment_refuses(name: str) -> None:
    """``F.expr`` shares ``refuse_sql_fragment``; pin every family, not only FNP-15."""
    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr(f"{name}(1)")
    _assert_deferred_cost(caught.value, name, "VARIANT")


@pytest.mark.parametrize("name", VARIANT_NAMES)
def test_variant_spark_sql_door_refuses(name: str, spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(_sql_call(name)).collect()
    _assert_deferred_cost(caught.value, name, "VARIANT")


@pytest.mark.parametrize("name", VARIANT_NAMES)
def test_variant_ansi_sql_door_refuses(name: str) -> None:
    import repark

    with pytest.raises(UnsupportedOperationException) as caught:
        repark.sql(_sql_call(name))
    _assert_deferred_cost(caught.value, name, "VARIANT")


@pytest.mark.parametrize("name", GEOSPATIAL_NAMES)
def test_geospatial_facade_refuses_deferred_by_cost(name: str) -> None:
    fn: Callable[..., Any] = getattr(F, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "WKB")


@pytest.mark.parametrize("name", GEOSPATIAL_NAMES)
def test_geospatial_sql_functions_reexport_refuses(name: str) -> None:
    """sed-swap path ``repark.spark.sql.functions`` carries the same stub."""
    from repark.spark.sql import functions as sql_functions

    fn: Callable[..., Any] = getattr(sql_functions, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_deferred_cost(caught.value, name, "WKB")
    assert getattr(sql_functions, name) is getattr(F, name)


@pytest.mark.parametrize("name", GEOSPATIAL_NAMES)
def test_geospatial_expr_fragment_refuses(name: str) -> None:
    """``F.expr`` shares ``refuse_sql_fragment``; pin every family, not only FNP-15."""
    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr(f"{name}(1)")
    _assert_deferred_cost(caught.value, name, "WKB")


@pytest.mark.parametrize("name", GEOSPATIAL_NAMES)
def test_geospatial_spark_sql_door_refuses(name: str, spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(_sql_call(name)).collect()
    _assert_deferred_cost(caught.value, name, "WKB")


@pytest.mark.parametrize("name", GEOSPATIAL_NAMES)
def test_geospatial_ansi_sql_door_refuses(name: str) -> None:
    import repark

    with pytest.raises(UnsupportedOperationException) as caught:
        repark.sql(_sql_call(name))
    _assert_deferred_cost(caught.value, name, "WKB")


def test_still_missing_name_stays_attribute_error() -> None:
    """FNP-Z owns wholesale __all__ completion; names outside the 62 stay absent."""
    with pytest.raises(AttributeError):
        getattr(F, STILL_ABSENT)


def test_live_function_and_default_select_still_plan(spark: ReparkSession) -> None:
    """New refusals must not change a live function or ``SELECT 1``."""
    import pyarrow as pa

    import repark

    table = spark.createDataFrame([(1,)], "x int").select(F.abs("x")).toArrow()
    assert table.column(0).to_pylist() == [1]
    ansi = repark.sql("SELECT 1 AS one").to_arrow()
    assert pa.types.is_integer(ansi.schema.field("one").type)
    assert ansi.column("one").to_pylist() == [1]


def test_registry_wording_distinguishes_unreachable_from_deferred_cost() -> None:
    """FNP-15 sections say unreachable; they must not classify as deferred-by-cost."""
    from pathlib import Path

    registry = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"
    text = registry.read_text(encoding="utf-8")
    start = text.index("## 9. Declared-absent Spark functions")
    body = text[start:]
    next_heading = body.find("\n## ", 4)
    section = body if next_heading < 0 else body[:next_heading]
    for name in FNP15_NAMES:
        heading = f"### FNP-15-{name}"
        assert heading in section, f"missing registry section {heading}"
        chunk_start = section.index(heading)
        rest = section[chunk_start + len(heading) :]
        next_chunk = rest.find("\n### ")
        chunk = rest if next_chunk < 0 else rest[:next_chunk]
        assert "unreachable" in chunk, heading
        assert "deferred by cost" not in chunk, heading
        classification = chunk.lower().replace("unsupportedoperationexception", "")
        assert "unsupported" not in classification, heading
    fnp16_headings = (
        "### FNP-16-sketches",
        "### FNP-16-csv-xml-xpath",
        "### FNP-16-variant",
        "### FNP-16-geospatial",
    )
    for heading in fnp16_headings:
        assert heading in section, f"missing registry section {heading}"
        chunk_start = section.index(heading)
        rest = section[chunk_start + len(heading) :]
        next_chunk = rest.find("\n### ")
        chunk = rest if next_chunk < 0 else rest[:next_chunk]
        assert "deferred by cost" in chunk, heading
        assert "reachable" in chunk, heading
        assert "unreachable" not in chunk, heading
        classification = chunk.lower().replace("unsupportedoperationexception", "")
        assert "unsupported" not in classification, heading


INDEPENDENT_SKETCH_NAMES: tuple[str, ...] = (
    "hll_sketch_agg",
    "hll_sketch_estimate",
    "hll_union",
    "hll_union_agg",
    "theta_difference",
    "theta_intersection",
    "theta_intersection_agg",
    "theta_sketch_agg",
    "theta_sketch_estimate",
    "theta_union",
    "theta_union_agg",
    "kll_merge_agg_bigint",
    "kll_merge_agg_double",
    "kll_merge_agg_float",
    "kll_sketch_agg_bigint",
    "kll_sketch_agg_double",
    "kll_sketch_agg_float",
    "kll_sketch_get_n_bigint",
    "kll_sketch_get_n_double",
    "kll_sketch_get_n_float",
    "kll_sketch_get_quantile_bigint",
    "kll_sketch_get_quantile_double",
    "kll_sketch_get_quantile_float",
    "kll_sketch_get_rank_bigint",
    "kll_sketch_get_rank_double",
    "kll_sketch_get_rank_float",
    "kll_sketch_merge_bigint",
    "kll_sketch_merge_double",
    "kll_sketch_merge_float",
    "kll_sketch_to_string_bigint",
    "kll_sketch_to_string_double",
    "kll_sketch_to_string_float",
)

INDEPENDENT_CSV_XML_XPATH_NAMES: tuple[str, ...] = (
    "to_csv",
    "to_xml",
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_number",
    "xpath_short",
    "xpath_string",
)

INDEPENDENT_VARIANT_NAMES: tuple[str, ...] = (
    "parse_json",
    "try_parse_json",
    "is_variant_null",
    "variant_get",
    "try_variant_get",
    "schema_of_variant",
    "schema_of_variant_agg",
    "to_variant_object",
)

INDEPENDENT_GEOSPATIAL_NAMES: tuple[str, ...] = (
    "st_asbinary",
    "st_geogfromwkb",
    "st_geomfromwkb",
    "st_setsrid",
    "st_srid",
)


def test_roster_counts_fnp15_six() -> None:
    """FNP-15 is exactly the six unreachable names."""
    assert len(FNP15_NAMES) == 6
    assert len(set(FNP15_NAMES)) == 6


def test_roster_counts_sketches_thirty_two() -> None:
    """FNP-16 sketches family is 32 names."""
    assert len(INDEPENDENT_SKETCH_NAMES) == 32
    assert len(set(INDEPENDENT_SKETCH_NAMES)) == 32
    assert set(SKETCH_NAMES) == set(INDEPENDENT_SKETCH_NAMES)


def test_roster_counts_csv_xml_xpath_eleven() -> None:
    """FNP-16 CSV/XML/XPath family is 11 names."""
    assert len(INDEPENDENT_CSV_XML_XPATH_NAMES) == 11
    assert len(set(INDEPENDENT_CSV_XML_XPATH_NAMES)) == 11
    assert set(CSV_XML_XPATH_NAMES) == set(INDEPENDENT_CSV_XML_XPATH_NAMES)


def test_roster_counts_variant_eight() -> None:
    """FNP-16 VARIANT family is 8 names."""
    assert len(INDEPENDENT_VARIANT_NAMES) == 8
    assert len(set(INDEPENDENT_VARIANT_NAMES)) == 8
    assert set(VARIANT_NAMES) == set(INDEPENDENT_VARIANT_NAMES)


def test_roster_counts_geospatial_five() -> None:
    """FNP-16 geospatial family is 5 names."""
    assert len(INDEPENDENT_GEOSPATIAL_NAMES) == 5
    assert len(set(INDEPENDENT_GEOSPATIAL_NAMES)) == 5
    assert set(GEOSPATIAL_NAMES) == set(INDEPENDENT_GEOSPATIAL_NAMES)


def test_roster_total_is_sixty_two() -> None:
    """62 names vs census F12/F13/F14/F16 + FNP-15, not the module tuples.

    pins: fnp-15-16/C-013
    """
    from repark.spark.functions_declared import DECLARED_REFUSE_NAMES

    independent = (
        set(FNP15_NAMES)
        | set(INDEPENDENT_SKETCH_NAMES)
        | set(INDEPENDENT_CSV_XML_XPATH_NAMES)
        | set(INDEPENDENT_VARIANT_NAMES)
        | set(INDEPENDENT_GEOSPATIAL_NAMES)
    )
    assert len(independent) == 62
    assert len(DECLARED_REFUSE_NAMES) == 62
    assert len(set(DECLARED_REFUSE_NAMES)) == 62
    assert set(DECLARED_REFUSE_NAMES) == independent

"""TYPES-GEO-DDL-1: ``geometry(n)`` / ``geography(n)`` in DDL schema strings.

Every expectation is a recorded PySpark 4.1.2 ``_parse_datatype_string`` cell from
``fixtures-batch13-geo.json`` (ids ``G13-0 … G13-20``); no value below is hand-computed.

pins: types-geo-ddl-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import repark.spark.types as spark_types
from repark import ReparkSession
from repark.errors import UnsupportedOperationException

_CELLS: dict[str, dict[str, Any]] = {
    cell["id"]: cell
    for cell in json.loads(
        (Path(__file__).with_name("fixtures-batch13-geo.json")).read_text(encoding="utf-8")
    )["geo"]
}

_PARSE_IDS: tuple[str, ...] = (
    "G13-0",
    "G13-1",
    "G13-2",
    "G13-3",
    "G13-4",
    "G13-7",
    "G13-8",
    "G13-9",
    "G13-12",
    "G13-13",
    "G13-17",
    "G13-18",
    "G13-19",
    "G13-20",
)

_ERROR_IDS: tuple[str, ...] = (
    "G13-5",
    "G13-6",
    "G13-10",
    "G13-11",
    "G13-14",
    "G13-15",
    "G13-16",
)


def test_geo_ddl_parse_cells() -> None:
    """Cells ``G13-0/1/2/3/4/7/8/9/12/13/17/18/19/20`` parse on both DDL doors.

    pins: types-geo-ddl-1/C-001, C-002, C-004
    """
    for cell_id in _PARSE_IDS:
        cell = _CELLS[cell_id]
        for parsed in (
            spark_types.DataType.fromDDL(cell["ddl"]),
            spark_types._parse_datatype_string(cell["ddl"]),
        ):
            assert repr(parsed) == cell["repr"], cell_id
            assert parsed.simpleString() == cell["simple"], cell_id
            assert parsed.json() == cell["json"], cell_id


def test_geo_ddl_error_cells() -> None:
    """Cells ``G13-5/6/10/11/14/15/16`` refuse on both DDL doors.

    pins: types-geo-ddl-1/C-001
    """
    for cell_id in _ERROR_IDS:
        ddl = _CELLS[cell_id]["ddl"]
        assert _CELLS[cell_id]["condition"] == "PARSE_SYNTAX_ERROR", cell_id
        with pytest.raises(ValueError, match="cannot parse datatype"):
            spark_types.DataType.fromDDL(ddl)
        with pytest.raises(ValueError, match="cannot parse datatype"):
            spark_types._parse_datatype_string(ddl)


def test_geo_ddl_bridge_tags_both_ways() -> None:
    """The bridge carries both spatial tags each direction.

    pins: types-geo-ddl-1/C-002
    """
    from repark import _native

    assert (
        _native.simple_string_from_descriptor({"kind": "geometry", "srid": 4326})
        == "geometry(4326)"
    )
    assert (
        _native.simple_string_from_descriptor({"kind": "geography", "srid": -1}) == "geography(any)"
    )
    assert _native.ddl_token_from_descriptor({"kind": "geometry", "srid": 0}) == "GEOMETRY(0)"
    with pytest.raises(ValueError, match="unsupported spatial SRID"):
        _native.simple_string_from_descriptor({"kind": "geography", "srid": 0})
    with pytest.raises(ValueError, match="unsupported spatial SRID"):
        _native.simple_string_from_descriptor({"kind": "geometry", "srid": 9999})


def _parse_both_doors(ddl: str) -> list[spark_types.DataType]:
    """Both DDL doors over one input."""
    return [
        spark_types.DataType.fromDDL(ddl),
        spark_types._parse_datatype_string(ddl),
    ]


def test_geo_ddl_integer_value_grammar_edges() -> None:
    """SRID spellings follow Spark's ``INTEGER_VALUE`` grammar, not Python ``int``.

    No live cell covers these; the grammar (``DIGIT+``, ASCII) is the spec.
    pins: types-geo-ddl-1/C-001
    """
    for parsed in _parse_both_doors("geometry(04326)"):
        assert parsed == spark_types.GeometryType(4326)
    for ddl in ("geometry(4_326)", "geometry(+4326)", "geometry(\uff14\uff13\uff12\uff16)"):
        with pytest.raises(ValueError, match="cannot parse datatype"):
            spark_types.DataType.fromDDL(ddl)
        with pytest.raises(ValueError, match="cannot parse datatype"):
            spark_types._parse_datatype_string(ddl)


def test_geo_ddl_surrounding_whitespace_doors() -> None:
    """Tab and NBSP inside the SRID parentheses parse on both DDL doors.

    No live cell covers these; the Rust trim matches Python ``str.strip``.
    pins: types-geo-ddl-1/C-002
    """
    for ddl in ("geometry(\t4326\t)", "geometry(\xa04326\xa0)"):
        for parsed in _parse_both_doors(ddl):
            assert parsed == spark_types.GeometryType(4326), repr(ddl)
    for parsed in (
        spark_types.DataType.fromDDL("decimal(\t10\t,\t2\t)"),
        spark_types._parse_datatype_string("decimal(\t10\t,\t2\t)"),
    ):
        assert parsed == spark_types.DecimalType(10, 2)


def test_geo_ddl_create_schema_string_refuses() -> None:
    """Cell ``G13-17`` parses but CREATE keeps the V3-GEO-1 column-use refusal.

    pins: types-geo-ddl-1/C-003
    """
    parsed = spark_types.DataType.fromDDL(_CELLS["G13-17"]["ddl"])
    assert isinstance(parsed, spark_types.StructType)
    spark = ReparkSession.builder.appName("types-geo-ddl-1-create").getOrCreate()
    try:
        with pytest.raises(UnsupportedOperationException) as excinfo:
            spark.createDataFrame([], "a int, g geography(4326)")
        assert "geography(4326)" in str(excinfo.value)
    finally:
        spark.stop()


def test_geo_ddl_reader_schema_string_refuses() -> None:
    """A reader schema string with a spatial field refuses naming the type.

    pins: types-geo-ddl-1/C-003
    """
    csv_file = Path("geo_ddl_1.csv")
    csv_file.write_text("g\nPOINT(1 2)\n", encoding="utf-8")
    spark = ReparkSession.builder.appName("types-geo-ddl-1-reader").getOrCreate()
    try:
        with pytest.raises(UnsupportedOperationException) as excinfo:
            spark.read.schema("g geography(4326)").csv(str(csv_file), header=True).collect()
        assert "geography(4326)" in str(excinfo.value)
    finally:
        spark.stop()
        csv_file.unlink()

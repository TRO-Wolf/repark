"""FACADE-3 step 3 pins: the named-rows funnel and the timetuple-free temporal path.

pins: facade-3/C-019, C-020

F-FUNNEL — ``createDataFrame`` on ``Row`` and ``dict`` lists must reach
``repark._native.cdf_arrow_export_named`` and return a table without calling the
Python named-row funnel (``_rows_from_mapping_list`` / ``_bind_named_row`` /
``_apply_permutation``); anything the native screen declines falls back to that
funnel, which owns the pinned refusal class and message.

F-TIMETUPLE — the abi3 datetime/date extraction keeps byte-identical collected
values and schemas against the forced-fallback Python path for every required
case (year 1 and 9999, pre-1970, microseconds, ``fold=1``, a non-UTC
fixed-offset tz, ``datetime``/``date`` subclasses), and ``NaT`` still falls back
to the Python normalizer.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest

import repark.spark.session.create_dataframe_columns as columns_module
import repark.spark.session.create_dataframe_rows as rows_module
from repark import ReparkSession
from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark.row import Row
from repark.spark.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Session matching the FACADE-3 golden corpus (UTC, TIMESTAMP_LTZ)."""
    session = (
        ReparkSession.builder.appName("facade-3-cdf-step3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _seven_col_dicts(count: int) -> list[dict[str, Any]]:
    """The step-1 fixture shape as dicts: int, float, string, bool, date, ts, decimal."""
    base_date = datetime.date(2024, 1, 2)
    base_ts = datetime.datetime(2024, 1, 2, 3, 4, 5)
    return [
        {
            "i": index,
            "f": index + 0.5,
            "s": f"s{index}",
            "b": index % 2 == 0,
            "d": base_date,
            "t": base_ts,
            "dc": Decimal(f"{index}.25"),
        }
        for index in range(count)
    ]


def _seven_col_named_rows(count: int) -> list[Any]:
    """The step-1 fixture shape as named repark Rows."""
    base_date = datetime.date(2024, 1, 2)
    base_ts = datetime.datetime(2024, 1, 2, 3, 4, 5)
    return [
        Row(
            i=index,
            f=index + 0.5,
            s=f"s{index}",
            b=index % 2 == 0,
            d=base_date,
            t=base_ts,
            dc=Decimal(f"{index}.25"),
        )
        for index in range(count)
    ]


def _seven_col_struct() -> Any:
    """A StructType with the fixture fields in a reordered sequence."""
    return StructType(
        [
            StructField("dc", DecimalType(38, 18), True),
            StructField("t", TimestampType(), True),
            StructField("d", DateType(), True),
            StructField("b", BooleanType(), True),
            StructField("s", StringType(), True),
            StructField("f", DoubleType(), True),
            StructField("i", LongType(), True),
        ]
    )


def _funnel_spies(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count Python named-row funnel calls made from create_dataframe_rows."""
    calls = {"bind": 0, "perm": 0, "mapping_list": 0}
    delegate_bind = rows_module._bind_named_row
    delegate_perm = rows_module._apply_permutation
    delegate_list = rows_module._rows_from_mapping_list

    def bind_spy(mapping: dict[str, Any], names: list[str], **kwargs: Any) -> Any:
        calls["bind"] += 1
        return delegate_bind(mapping, names, **kwargs)

    def perm_spy(row: tuple[Any, ...], permutation: list[int]) -> Any:
        calls["perm"] += 1
        return delegate_perm(row, permutation)

    def list_spy(data: list[Any], schema: Any, **kwargs: Any) -> Any:
        calls["mapping_list"] += 1
        return delegate_list(data, schema, **kwargs)

    monkeypatch.setattr(rows_module, "_bind_named_row", bind_spy)
    monkeypatch.setattr(rows_module, "_apply_permutation", perm_spy)
    monkeypatch.setattr(rows_module, "_rows_from_mapping_list", list_spy)
    return calls


def _named_export_spy(monkeypatch: pytest.MonkeyPatch) -> list[tuple[bool, int]]:
    """Record (is_row, len(data)) per call to the native named funnel wrapper."""
    calls: list[tuple[bool, int]] = []
    delegate = rows_module._rust_cdf_named_arrow_table

    def spy(data: list[Any], schema: Any, *, is_row: bool, engine_types: Any) -> Any:
        calls.append((is_row, len(data)))
        return delegate(data, schema, is_row=is_row, engine_types=engine_types)

    monkeypatch.setattr(rows_module, "_rust_cdf_named_arrow_table", spy)
    return calls


def test_named_export_registered() -> None:
    """The native named-rows export and its Python wrapper exist (facade-3/C-019)."""
    from repark import _native

    assert callable(getattr(_native, "cdf_arrow_export_named", None))
    assert callable(getattr(columns_module, "_rust_cdf_named_arrow_table", None))


def test_dict_list_skips_python_funnel(spark: ReparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """A dict list reaches the named export and never calls the Python funnel."""
    calls = _funnel_spies(monkeypatch)
    exports = _named_export_spy(monkeypatch)
    frame = spark.createDataFrame(_seven_col_dicts(60))
    assert exports == [(False, 60)]
    assert calls == {"bind": 0, "perm": 0, "mapping_list": 0}
    collected = frame.collect()
    assert collected[5]["i"] == 5
    assert collected[5]["dc"] == Decimal("5.25")


def test_row_list_skips_python_funnel(spark: ReparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """A repark Row list reaches the named export and never calls the Python funnel."""
    calls = _funnel_spies(monkeypatch)
    exports = _named_export_spy(monkeypatch)
    frame = spark.createDataFrame(_seven_col_named_rows(60))
    assert exports == [(True, 60)]
    assert calls == {"bind": 0, "perm": 0, "mapping_list": 0}
    collected = frame.collect()
    assert collected[5]["i"] == 5
    assert collected[5]["dc"] == Decimal("5.25")


def test_dict_key_union_still_orders_natively(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spark dict key-union order holds on the native path (a,c,b,d)."""
    calls = _funnel_spies(monkeypatch)
    frame = spark.createDataFrame(
        [{"c": 1, "a": 2}, {"b": 3, "a": 4}, {"d": 5, "c": 6}]
    )
    assert calls == {"bind": 0, "perm": 0, "mapping_list": 0}
    assert frame.columns == ["a", "c", "b", "d"]
    assert frame.collect()[2]["c"] == 6


def test_dict_explicit_schema_skips_python_funnel(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dict + DDL schema (null-fill, extras dropped) dispatches natively."""
    calls = _funnel_spies(monkeypatch)
    exports = _named_export_spy(monkeypatch)
    data = _seven_col_dicts(40)
    data[10]["extra"] = "dropped"
    frame = spark.createDataFrame(
        data,
        "i BIGINT, f DOUBLE, s STRING, b BOOLEAN, d DATE, t TIMESTAMP, dc DECIMAL(38,18)",
    )
    assert exports == [(False, 40)]
    assert calls == {"bind": 0, "perm": 0, "mapping_list": 0}
    assert frame.columns == ["i", "f", "s", "b", "d", "t", "dc"]
    assert frame.count() == 40


def test_row_structtype_reorder_skips_python_funnel(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Row + reordered StructType does the strict bind and permutation natively."""
    calls = _funnel_spies(monkeypatch)
    exports = _named_export_spy(monkeypatch)
    frame = spark.createDataFrame(_seven_col_named_rows(30), _seven_col_struct())
    assert exports == [(True, 30)]
    assert calls == {"bind": 0, "perm": 0, "mapping_list": 0}
    collected = frame.collect()
    assert collected[5]["i"] == 5
    assert frame.columns[0] == "dc"


def test_row_reordered_field_names_bind_by_name(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rows whose field order differs from row 0 bind by name natively."""
    calls = _funnel_spies(monkeypatch)
    rows: list[Any] = [Row(a=index, b=index * 2) for index in range(30)]
    rows[10] = Row(b=123, a=456)
    frame = spark.createDataFrame(rows)
    assert calls == {"bind": 0, "perm": 0, "mapping_list": 0}
    collected = frame.collect()
    assert collected[10]["a"] == 456
    assert collected[10]["b"] == 123


def test_named_fallback_owns_homogeneity_refusal(spark: ReparkSession) -> None:
    """A late non-dict element falls back pre-extraction; Python raises (facade-3/C-019)."""
    data: list[Any] = [{"a": index} for index in range(50)]
    data[40] = ("not", "a-dict")
    with pytest.raises(PySparkTypeError, match=r"dict lists must be homogeneous.*index 40"):
        spark.createDataFrame(data)


def test_named_fallback_owns_row_strict_refusal(spark: ReparkSession) -> None:
    """A Row with a mismatched key set keeps the Python strict refusal (facade-3/C-019)."""
    rows: list[Any] = [Row(a=index, b=index) for index in range(30)]
    rows[20] = Row(a=20, b=20, c=20)
    with pytest.raises(PySparkValueError, match=r"unexpected field\(s\)"):
        spark.createDataFrame(rows)


def test_named_fallback_owns_uncovered_cell(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An uncovered dict cell takes the Python funnel; Python owns the outcome."""
    calls = _funnel_spies(monkeypatch)
    frame = spark.createDataFrame([{"a": index, "b": object()} for index in range(30)])
    assert calls["mapping_list"] > 0
    assert frame.schema.simpleString() == "struct<a:bigint,b:string>"
    assert str(frame.collect()[0]["b"]).startswith("<object object at 0x")


class _ProbeDate(datetime.date):
    """A plain ``datetime.date`` subclass for the value-parity pin."""


class _ProbeDatetime(datetime.datetime):
    """A plain ``datetime.datetime`` subclass for the value-parity pin."""


def _temporal_case_rows() -> dict[str, list[tuple[Any, ...]]]:
    """The C-020 value-parity corpus: one column per case family."""
    fixed_offset = datetime.timezone(datetime.timedelta(hours=-5, minutes=-30))
    utc = datetime.timezone.utc
    cases: dict[str, list[tuple[Any, ...]]] = {
        "dates": [
            (datetime.date(1, 1, 1),),
            (datetime.date(9999, 12, 31),),
            (datetime.date(1969, 12, 31),),
            (datetime.date(1900, 1, 1),),
            (datetime.date(2024, 2, 29),),
        ],
        "naive": [
            (datetime.datetime(1, 1, 1),),
            (datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),),
            (datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),),
            (datetime.datetime(1900, 1, 1, 0, 0, 0, 1),),
            (datetime.datetime(2024, 11, 3, 1, 30, 0, 500, fold=1),),
        ],
        "aware": [
            (datetime.datetime(2024, 1, 1, 12, 0, 0, 123456, tzinfo=utc),),
            (datetime.datetime(1, 1, 1, 0, 0, 0, tzinfo=utc),),
            (datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=utc),),
            (datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=utc),),
            (datetime.datetime(2024, 6, 15, 8, 30, 45, 250000, tzinfo=fixed_offset),),
            (datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=fixed_offset, fold=1),),
        ],
        "subclass_dates": [
            (_ProbeDate(2024, 3, 4),),
            (_ProbeDate(1969, 12, 31),),
        ],
        "subclass_datetimes": [
            (_ProbeDatetime(2024, 3, 4, 5, 6, 7, 890123),),
            (_ProbeDatetime(9999, 12, 31, 23, 59, 59, 999999),),
        ],
    }
    try:
        from zoneinfo import ZoneInfo

        cases["aware"].append(
            (
                datetime.datetime(
                    2024, 11, 3, 1, 30, 0, 500, tzinfo=ZoneInfo("America/New_York"), fold=1
                ),
            )
        )
        cases["aware"].append(
            (
                datetime.datetime(
                    2024, 11, 3, 1, 30, 0, 500, tzinfo=ZoneInfo("America/New_York"), fold=0
                ),
            )
        )
    except ImportError:
        pass
    return cases


def test_temporal_cells_keep_fallback_values(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Native extraction equals the forced-fallback Python path, byte for byte (C-020)."""
    import repark._native as native

    for name, data in _temporal_case_rows().items():
        rust_frame = spark.createDataFrame(data)
        rust_rows = [tuple(row) for row in rust_frame.collect()]
        rust_schema = rust_frame.schema.simpleString()
        monkeypatch.setattr(native, "cdf_arrow_export", lambda *args: None)
        monkeypatch.setattr(
            native, "cdf_arrow_export_named", lambda *args, **kwargs: None, raising=False
        )
        fallback_frame = spark.createDataFrame(data)
        fallback_rows = [tuple(row) for row in fallback_frame.collect()]
        fallback_schema = fallback_frame.schema.simpleString()
        monkeypatch.undo()
        assert (rust_schema, rust_rows) == (fallback_schema, fallback_rows), name


def test_nat_still_falls_back_to_python_normalizer(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A NaT cell is still declined by the export and seen by the Python normalizer."""
    pandas = pytest.importorskip("pandas")
    import repark.spark.session.create_dataframe_arrow as arrow_module
    import repark.spark.session.create_dataframe_schema as schema_module
    import repark.spark.session.create_dataframe_values as values_module

    seen: list[str] = []
    delegate = values_module._normalize_create_dataframe_cell

    def spy(value: Any, *, field_name: str | None = None) -> Any:
        seen.append(type(value).__name__)
        return delegate(value, field_name=field_name)

    for module in (
        values_module,
        columns_module,
        schema_module,
        rows_module,
        arrow_module,
    ):
        monkeypatch.setattr(module, "_normalize_create_dataframe_cell", spy, raising=False)

    frame = spark.createDataFrame([(pandas.NaT,), (datetime.datetime(2024, 1, 2),)])
    assert "NaTType" in seen
    assert [tuple(row) for row in frame.collect()] == [(None,), (datetime.datetime(2024, 1, 2),)]

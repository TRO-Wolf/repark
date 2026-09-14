"""FACADE-4 type-conversion goldens against the committed JSON file."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    CalendarIntervalType,
    CharType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    TimeType,
    VarcharType,
    VariantType,
    YearMonthIntervalType,
    _parse_datatype_json_value,
    repark_type_to_arrow,
    struct_type_from_arrow,
)

RECORD_ENV = "REPARK_FACADE_4_RECORD_GOLDENS"
GOLDEN_PATH = Path(__file__).with_name("facade_4_type_goldens.json")


def _running_in_ci() -> bool:
    """True when GitHub Actions or generic CI has any non-empty value."""
    return bool(os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"))


def _record_requested() -> bool:
    """True when the explicit record environment variable is `1`."""
    return os.environ.get(RECORD_ENV, "") == "1"


def _assert_record_mode_allowed() -> None:
    """Refuse record mode when CI is set so a golden cannot be rewritten in CI."""
    if _record_requested() and _running_in_ci():
        raise AssertionError(f"{RECORD_ENV} is set while CI is set; refusing to rewrite goldens")


def _canonical_json(payload: dict[str, dict[str, object]]) -> str:
    """Stable JSON bytes used both to record and to compare."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _refusal(error: BaseException) -> dict[str, str]:
    """Exception class + message snapshot for a refused conversion."""
    return {"refused": f"{type(error).__name__}: {error}"}


def _attempt(thunk: Any) -> Any:
    """Run ``thunk`` returning its answer or a refusal snapshot."""
    try:
        return thunk()
    except Exception as error:
        return _refusal(error)


def _type_shape(data_type: Any) -> dict[str, object]:
    """Class name, simpleString and nullability flags for one parsed answer."""
    shape: dict[str, object] = {
        "class": type(data_type).__name__,
        "simpleString": data_type.simpleString(),
    }
    if isinstance(data_type, ArrayType):
        shape["containsNull"] = data_type.containsNull
        shape["element"] = _type_shape(data_type.elementType)
    if isinstance(data_type, MapType):
        shape["valueContainsNull"] = data_type.valueContainsNull
        shape["key"] = _type_shape(data_type.keyType)
        shape["value"] = _type_shape(data_type.valueType)
    if isinstance(data_type, StructType):
        shape["fields"] = _struct_shape(data_type)
    return shape


def _field_shape(field: StructField) -> dict[str, object]:
    """Name, nullability and the recursive type shape for one struct field."""
    shape: dict[str, object] = {"name": field.name, "nullable": field.nullable}
    shape.update(_type_shape(field.dataType))
    return shape


def _struct_shape(struct_type: StructType) -> list[dict[str, object]]:
    """Recursive per-field shapes for a StructType answer."""
    return [_field_shape(field) for field in struct_type.fields]


def _snapshot_type(data_type: DataType) -> dict[str, object]:
    """Every conversion answer for one F1 type instance."""
    entry: dict[str, object] = {
        "class": type(data_type).__name__,
        "simpleString": _attempt(lambda: data_type.simpleString()),
        "typeName": _attempt(lambda: data_type.typeName()),
        "json": _attempt(lambda: data_type.json()),
        "repr": _attempt(lambda: repr(data_type)),
        "engine_type": _attempt(lambda: data_type._engine_type()),
        "fromddl_simplestring": _attempt(
            lambda: _type_shape(DataType.fromDDL(data_type.simpleString()))
        ),
        "json_roundtrip": _attempt(
            lambda: _type_shape(_parse_datatype_json_value(json.loads(data_type.json())))
        ),
        "arrow": _attempt(lambda: str(repark_type_to_arrow(data_type))),
    }
    if isinstance(data_type, StructType):
        entry["toddl"] = _attempt(lambda: data_type.toDDL())
        entry["toddl_roundtrip"] = _attempt(
            lambda: (
                _struct_shape(DataType.fromDDL(data_type.toDDL()))
                if isinstance(DataType.fromDDL(data_type.toDDL()), StructType)
                else _type_shape(DataType.fromDDL(data_type.toDDL()))
            )
        )
        entry["arrow_schema_back"] = _attempt(
            lambda: _struct_shape(
                struct_type_from_arrow(
                    __import__("pyarrow").schema(
                        [
                            __import__("pyarrow").field(
                                field.name,
                                repark_type_to_arrow(field.dataType),
                                nullable=field.nullable,
                            )
                            for field in data_type.fields
                        ]
                    )
                )
            )
        )
    return entry


def _flat_seven() -> StructType:
    """Flat seven-type schema: int, long, double, string, bool, date, timestamp."""
    return StructType(
        [
            StructField("i", IntegerType(), True),
            StructField("l", LongType(), True),
            StructField("f", DoubleType(), True),
            StructField("s", StringType(), True),
            StructField("b", BooleanType(), True),
            StructField("d", DateType(), True),
            StructField("t", TimestampType(), True),
        ]
    )


def _wide_fifty() -> StructType:
    """Fifty-column mixed schema cycling the flat seven types plus decimal."""
    cycle = [
        IntegerType(),
        LongType(),
        DoubleType(),
        StringType(),
        BooleanType(),
        DateType(),
        TimestampType(),
        DecimalType(10, 2),
        BinaryType(),
        FloatType(),
    ]
    return StructType(
        [StructField(f"c{index}", cycle[index % len(cycle)], index % 3 != 0) for index in range(50)]
    )


def _nested_depth3() -> StructType:
    """struct<array<map>> nesting three levels deep."""
    inner = MapType(
        StringType(),
        StructType(
            [StructField("leaf", DecimalType(10, 2), False)],
        ),
        False,
    )
    return StructType(
        [
            StructField("top", IntegerType(), False),
            StructField(
                "mid",
                ArrayType(
                    StructType(
                        [
                            StructField("m", inner, True),
                            StructField("arr", ArrayType(LongType(), False), False),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )


def _decimal_variants() -> StructType:
    """Decimal (10,2) / (38,18) / (38,0) schema."""
    return StructType(
        [
            StructField("d10", DecimalType(10, 2), True),
            StructField("d38", DecimalType(38, 18), True),
            StructField("d38s0", DecimalType(38, 0), True),
        ]
    )


def _timestamp_variants() -> StructType:
    """Timestamp tz / ntz / session-default schema."""
    from repark.spark.session.timestamp_type import default_timestamp_data_type

    return StructType(
        [
            StructField("ts_ltz", TimestampType(), True),
            StructField("ts_ntz", TimestampNTZType(), True),
            StructField("ts_session", default_timestamp_data_type(), True),
        ]
    )


def _interval_char_varchar() -> StructType:
    """Interval and char/varchar schema."""
    return StructType(
        [
            StructField("ym", YearMonthIntervalType(), True),
            StructField("dt", DayTimeIntervalType(), True),
            StructField("cal", CalendarIntervalType(), True),
            StructField("ch", CharType(8), True),
            StructField("vc", VarcharType(32), True),
        ]
    )


def _atomic_cases() -> dict[str, DataType]:
    """One representative instance per F1 atomic type class."""
    return {
        "null": NullType(),
        "string": StringType(),
        "string_collated": StringType("UNICODE_CI"),
        "char8": CharType(8),
        "varchar32": VarcharType(32),
        "binary": BinaryType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "timestamp_ntz": TimestampNTZType(),
        "time_default": TimeType(),
        "time0": TimeType(0),
        "decimal_default": DecimalType(),
        "decimal_10_2": DecimalType(10, 2),
        "decimal_38_18": DecimalType(38, 18),
        "decimal_38_0": DecimalType(38, 0),
        "decimal_76_10": DecimalType(76, 10),
        "double": DoubleType(),
        "float": FloatType(),
        "byte": ByteType(),
        "integer": IntegerType(),
        "long": LongType(),
        "short": ShortType(),
        "calendar_interval": CalendarIntervalType(),
        "day_time_interval": DayTimeIntervalType(),
        "day_time_interval_day": DayTimeIntervalType(0, 0),
        "day_time_interval_hour_second": DayTimeIntervalType(1, 3),
        "year_month_interval": YearMonthIntervalType(),
        "year_month_interval_year": YearMonthIntervalType(0, 0),
        "variant": VariantType(),
    }


def _complex_cases() -> dict[str, DataType]:
    """Array / map / struct instances incl. the non-default nullability arms."""
    return {
        "array_int": ArrayType(IntegerType()),
        "array_int_no_null": ArrayType(IntegerType(), False),
        "array_nested": ArrayType(MapType(StringType(), ArrayType(LongType()))),
        "map_str_int": MapType(StringType(), IntegerType()),
        "map_str_int_no_null": MapType(StringType(), IntegerType(), False),
        "struct_empty": StructType(),
        "struct_flat7": _flat_seven(),
        "struct_wide50": _wide_fifty(),
        "struct_nested3": _nested_depth3(),
        "struct_decimal_variants": _decimal_variants(),
        "struct_timestamp_variants": _timestamp_variants(),
        "struct_interval_char_varchar": _interval_char_varchar(),
        "struct_not_null_fields": StructType(
            [
                StructField("req", IntegerType(), False),
                StructField("opt", StringType(), True),
                StructField("meta", StringType(), True, {"k": "v"}),
            ]
        ),
    }


def _arrow_probe_schema() -> Any:
    """Arrow fields the repark table cannot produce (raw Arrow answers)."""
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("bin", pa.binary()),
            pa.field("bin_large", pa.large_binary()),
            pa.field("str_large", pa.large_string()),
            pa.field("date64", pa.date64()),
            pa.field("ts_ny", pa.timestamp("us", tz="America/New_York")),
            pa.field("ts_s", pa.timestamp("s", tz="UTC")),
            pa.field("dec256", pa.decimal256(76, 10)),
            pa.field("u8", pa.uint8()),
            pa.field("u16", pa.uint16()),
            pa.field("u32", pa.uint32()),
            pa.field("u64", pa.uint64()),
            pa.field("i8", pa.int8()),
            pa.field("i16", pa.int16()),
            pa.field("f16", pa.float16()),
            pa.field("f32", pa.float32()),
            pa.field("null_col", pa.null()),
            pa.field("time64", pa.time64("us")),
            pa.field("dur", pa.duration("us")),
            pa.field("ym", pa.month_day_nano_interval()),
            pa.field("dict", pa.dictionary(pa.int32(), pa.string())),
            pa.field("list_nonnull", pa.list_(pa.field("item", pa.int32(), False))),
            pa.field("map_col", pa.map_(pa.string(), pa.int32())),
            pa.field(
                "struct_col",
                pa.struct(
                    [
                        pa.field("req", pa.int32(), False),
                        pa.field("opt", pa.string(), True),
                    ]
                ),
                False,
            ),
        ]
    )


def _session_conf_cases() -> dict[str, object]:
    """Session-dependent timestamp answers under a fresh non-UTC session."""
    import datetime

    session = (
        ReparkSession.builder.appName("facade-4-types-ny")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .getOrCreate()
    )
    try:
        from repark.spark.session.session_time_zone import (
            active_session_time_zone,
            collect_timestamp_as_session_wall,
        )
        from repark.spark.session.timestamp_type import (
            default_timestamp_arrow_type,
            default_timestamp_data_type,
        )

        utc_instant = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
        return {
            "session_time_zone": active_session_time_zone(),
            "default_timestamp_data_type": _type_shape(default_timestamp_data_type()),
            "default_timestamp_arrow_type": str(default_timestamp_arrow_type()),
            "collect_wall_in_session_zone": collect_timestamp_as_session_wall(
                utc_instant
            ).isoformat(),
        }
    finally:
        session.stop()


def _build_payload_inner() -> dict[str, dict[str, object]]:
    """Snapshot every conversion case."""
    payload: dict[str, dict[str, object]] = {}
    for case_id, data_type in {**_atomic_cases(), **_complex_cases()}.items():
        payload[case_id] = _snapshot_type(data_type)
    payload["arrow_probe_schema_back"] = {
        "fields": _struct_shape(struct_type_from_arrow(_arrow_probe_schema()))
    }
    return payload


def _build_payload() -> dict[str, dict[str, object]]:
    """Snapshot under a pinned session (UTC, TIMESTAMP_LTZ) for determinism."""
    session = (
        ReparkSession.builder.appName("facade-4-types")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    try:
        session.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
        payload = _build_payload_inner()
    finally:
        session.stop()
    payload["session_conf_ny_ntz"] = _session_conf_cases()
    return payload


def test_type_goldens_match_committed_bytes() -> None:
    """Byte-identical conversion goldens.

    pins: facade-4/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
    """
    _assert_record_mode_allowed()
    payload = _build_payload()
    encoded = _canonical_json(payload)
    if _record_requested():
        GOLDEN_PATH.write_text(encoded, encoding="utf-8")
    if not GOLDEN_PATH.is_file():
        raise AssertionError(f"missing golden {GOLDEN_PATH.name}; set {RECORD_ENV}=1 to record")
    expected = GOLDEN_PATH.read_text(encoding="utf-8")
    if encoded != expected:
        actual_ids = set(payload)
        want = json.loads(expected)
        want_ids = set(want)
        missing = sorted(want_ids - actual_ids)
        extra = sorted(actual_ids - want_ids)
        changed = sorted(
            case_id
            for case_id in sorted(actual_ids & want_ids)
            if payload[case_id] != want[case_id]
        )
        raise AssertionError(
            f"golden byte mismatch missing={missing[:12]} extra={extra[:12]} changed={changed[:12]}"
        )


def test_conversion_answers_are_public_type_classes() -> None:
    """Every conversion answer is a public repark.spark.types class. pins: facade-4/C-003"""
    import pyarrow as pa

    for data_type in {**_atomic_cases(), **_complex_cases()}.values():
        parsed = _attempt(lambda dt=data_type: DataType.fromDDL(dt.simpleString()))
        if isinstance(parsed, DataType):
            assert type(parsed).__module__ == "repark.spark.types"
        round_tripped = _attempt(
            lambda dt=data_type: _parse_datatype_json_value(json.loads(dt.json()))
        )
        if isinstance(round_tripped, DataType):
            assert type(round_tripped).__module__ == "repark.spark.types"
        arrow = _attempt(lambda dt=data_type: repark_type_to_arrow(dt))
        if arrow is not None and not isinstance(arrow, dict):
            assert isinstance(arrow, pa.DataType)
    for schema in (_flat_seven(), _wide_fifty(), _nested_depth3(), _decimal_variants()):
        arrow_schema = pa.schema(
            [pa.field(field.name, repark_type_to_arrow(field.dataType)) for field in schema.fields]
        )
        rebuilt = struct_type_from_arrow(arrow_schema)
        assert isinstance(rebuilt, StructType)
        for field in rebuilt.fields:
            assert isinstance(field.dataType, DataType)
        for field in schema.fields:
            assert isinstance(field, StructField)
    assert isinstance(struct_type_from_arrow(_arrow_probe_schema()), StructType)


def test_record_mode_fails_when_ci_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record env is refused in CI. pins: facade-4/C-002"""
    monkeypatch.setenv(RECORD_ENV, "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
    monkeypatch.delenv("CI")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
    monkeypatch.delenv("GITHUB_ACTIONS")
    monkeypatch.setenv("CI", "1")
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
    monkeypatch.setenv("CI", "")
    _assert_record_mode_allowed()

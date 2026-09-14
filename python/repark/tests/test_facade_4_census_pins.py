"""FACADE-4 step-1 census pins: every Agree and D1-D24 row's measured answer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.types import (
    ArrayType,
    BinaryType,
    CharType,
    DataType,
    DayTimeIntervalType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    VarcharType,
    YearMonthIntervalType,
    _arrow_type_to_repark,
    repark_type_to_arrow,
    struct_type_from_arrow,
)


def _refusal(error: BaseException) -> str:
    """Exception class + message for a refused conversion."""
    return f"REFUSED {type(error).__name__}: {error}"


def _attempt(thunk: Any) -> Any:
    """Run ``thunk`` returning its answer or a refusal string."""
    try:
        return thunk()
    except Exception as error:
        return _refusal(error)


def _shape(data_type: Any) -> str:
    """Class name plus simpleString for one Spark type answer."""
    return f"{type(data_type).__name__}:{data_type.simpleString()}"


def _arrow_in(arrow_type: Any) -> Any:
    """Facade (i): ``_arrow_type_to_repark`` answer for a ``pyarrow.DataType``."""
    return _attempt(lambda: _shape(_arrow_type_to_repark(arrow_type)))


def _arrow_in_contains_null(arrow_type: Any) -> Any:
    """Facade (i): array answer plus its ``containsNull`` flag."""
    data_type = _attempt(lambda: _arrow_type_to_repark(arrow_type))
    if isinstance(data_type, str):
        return data_type
    return _shape(data_type), data_type.containsNull


def _arrow_in_value_contains_null(arrow_type: Any) -> Any:
    """Facade (i): map answer plus its ``valueContainsNull`` flag."""
    data_type = _attempt(lambda: _arrow_type_to_repark(arrow_type))
    if isinstance(data_type, str):
        return data_type
    return _shape(data_type), data_type.valueContainsNull


def _arrow_out(data_type: Any) -> Any:
    """Facade (i): ``repark_type_to_arrow`` answer or refusal."""
    return _attempt(lambda: str(repark_type_to_arrow(data_type)))


def _from_ddl(text: str) -> Any:
    """Facade (i): ``DataType.fromDDL`` answer or refusal."""
    return _attempt(lambda: _shape(DataType.fromDDL(text)))


def _to_ddl(schema: StructType) -> Any:
    """Facade (i): ``StructType.toDDL`` answer or refusal."""
    return _attempt(lambda: schema.toDDL())


def _to_ddl_reparse(schema: StructType) -> Any:
    """Facade (i): ``fromDDL`` of this schema's own ``toDDL`` output."""
    return _attempt(lambda: _shape(DataType.fromDDL(schema.toDDL())))


def _schema_back(fields: Any) -> Any:
    """Facade (i): ``struct_type_from_arrow`` per-field answers."""
    import pyarrow as pa

    struct = _attempt(lambda: struct_type_from_arrow(pa.schema(fields)))
    if isinstance(struct, str):
        return struct
    return [(field.name, _shape(field.dataType), field.nullable) for field in struct.fields]


def _rung(literal: str) -> Any:
    """CSV table (ii): rung, Spark type, engine cast, SQL cast for one literal."""
    from repark.spark import _csv_smart as smart

    resolution = smart.resolve_column_type([literal])
    return (
        resolution.rung,
        _attempt(lambda: _shape(smart.rung_to_spark_type(resolution))),
        _attempt(lambda: smart.rung_to_engine_cast(resolution)),
        _attempt(lambda: smart.rung_to_sql_cast(resolution)),
    )


def _reader(session: Any, arrow_field: Any) -> Any:
    """Reader lattice (iii): type_key, ``df.schema``, ``dtypes`` for one Arrow field."""
    import pyarrow as pa

    from repark.spark.session.create_dataframe_rows import _materialize_arrow_as_memtable_frame

    def measure() -> Any:
        value = (
            pa.array([{child.name: 1 for child in arrow_field.type}], type=arrow_field.type)
            if pa.types.is_struct(arrow_field.type)
            else pa.array([1] if not arrow_field.nullable else [None], type=arrow_field.type)
        )
        table = pa.Table.from_arrays([value], schema=pa.schema([arrow_field]))
        frame = _materialize_arrow_as_memtable_frame(session, table)
        keys = frame._inner.logical_schema_fields()
        return keys, frame.schema.simpleString(), frame.dtypes

    return _attempt(measure)


def _csv_infer(session: Any, tmp_path: Path, literal: str) -> Any:
    """Reader lattice (iii): served ``inferSchema`` dtypes for one literal."""
    path = tmp_path / "probe.csv"
    path.write_text(f"c\n{literal}\n{literal}\n", encoding="utf-8")
    return _attempt(lambda: session.read.csv(str(path), header=True, inferSchema=True).dtypes)


def _describe_col(session: Any, column: str) -> Any:
    """Reader lattice (iii): ``DESCRIBE TABLE`` spelling for a probe column."""
    return _attempt(
        lambda: {
            row[0]: row[1] for row in session.sql("DESCRIBE TABLE mem.ns.probe").collect()
        }.get(column)
    )


def _table_dtypes_col(session: Any, column: str) -> Any:
    """Reader lattice (iii): ``session.table(...).dtypes`` for a probe column."""
    return _attempt(lambda: dict(session.table("mem.ns.probe").dtypes).get(column))


def _table_key_col(session: Any, column: str) -> Any:
    """Reader lattice (iii): ``logical_schema_fields`` key for a probe column."""
    return _attempt(
        lambda: {
            name: key
            for name, key, _nullable in session.table("mem.ns.probe")._inner.logical_schema_fields()
        }.get(column)
    )


def _ntz_default(session: Any) -> Any:
    """Facade (i): session default timestamp type under ``TIMESTAMP_NTZ``."""
    from repark.spark.session.timestamp_type import (
        TIMESTAMP_TYPE_KEY,
        default_timestamp_data_type,
    )

    def measure() -> Any:
        session.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP_NTZ")
        return _shape(default_timestamp_data_type())

    try:
        return _attempt(measure)
    finally:
        session.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP_LTZ")


def _pa(arrow_type: Any) -> Any:
    """Build one ``pa.field`` for reader probes."""
    import pyarrow as pa

    return pa.field("c", arrow_type)


def _pa_nonnull(arrow_type: Any) -> Any:
    """Build one non-nullable ``pa.field`` for reader probes."""
    import pyarrow as pa

    return pa.field("c", arrow_type, False)


def _probe_table_schema() -> StructType:
    """The toDDL probe schema: one required int field, one nullable string field."""
    return StructType(
        [
            StructField("req", IntegerType(), False),
            StructField("opt", StringType(), True),
        ]
    )


def _build_rows() -> list[tuple[str, str, list[tuple[str, str, Any]]]]:
    import pyarrow as pa

    rows: list[tuple[str, str, list[tuple[str, str, Any]]]] = [
        (
            "A1",
            "date32/date64/'2024-01-02' agree",
            [
                ("arrow_in", pa.date32(), "DateType:date"),
                ("arrow_in", pa.date64(), "DateType:date"),
                ("fromddl", "date", "DateType:date"),
                ("rung", "2024-01-02", ("date", "DateType:date", "date", "date")),
                (
                    "reader",
                    _pa(pa.date32()),
                    ([("c", "date", True)], "struct<c:date>", [("c", "date")]),
                ),
                (
                    "reader",
                    _pa(pa.date64()),
                    ([("c", "date", True)], "struct<c:date>", [("c", "date")]),
                ),
                ("csv_infer", "2024-01-02", [("c", "date")]),
                ("describe", "a_date", "date"),
            ],
        ),
        (
            "A2",
            "float64/'1.5e3' agree",
            [
                ("arrow_in", pa.float64(), "DoubleType:double"),
                ("rung", "1.5e3", ("float64", "DoubleType:double", "double", "double")),
                (
                    "reader",
                    _pa(pa.float64()),
                    ([("c", "double", True)], "struct<c:double>", [("c", "double")]),
                ),
                ("csv_infer", "1.5e3", [("c", "double")]),
                ("describe", "a_double", "double"),
            ],
        ),
        (
            "A3",
            "int64/'9999999999' agree",
            [
                ("arrow_in", pa.int64(), "LongType:bigint"),
                ("arrow_out", _pa_long_hint(), "int64"),
                ("fromddl", "bigint", "LongType:bigint"),
                ("rung", "9999999999", ("int64", "LongType:bigint", "long", "bigint")),
                (
                    "reader",
                    _pa(pa.int64()),
                    ([("c", "long", True)], "struct<c:bigint>", [("c", "bigint")]),
                ),
                ("csv_infer", "9999999999", [("c", "bigint")]),
                ("describe", "a_big", "bigint"),
                ("table_dtypes", "a_big", "bigint"),
                ("table_key", "a_big", "long"),
            ],
        ),
        (
            "A4",
            "boolean/'true' agree",
            [
                ("arrow_in", pa.bool_(), "BooleanType:boolean"),
                ("rung", "true", ("bool", "BooleanType:boolean", "boolean", "boolean")),
                (
                    "reader",
                    _pa(pa.bool_()),
                    ([("c", "boolean", True)], "struct<c:boolean>", [("c", "boolean")]),
                ),
                ("csv_infer", "true", [("c", "boolean")]),
            ],
        ),
        (
            "A5",
            "timestamp[us, tz=UTC] agree",
            [
                ("arrow_in", pa.timestamp("us", tz="UTC"), "TimestampType:timestamp"),
                (
                    "rung",
                    "2024-01-02 03:04:05Z",
                    ("timestamp", "TimestampType:timestamp", "timestamp", "timestamp"),
                ),
                (
                    "reader",
                    _pa(pa.timestamp("us", tz="UTC")),
                    ([("c", "timestamp", True)], "struct<c:timestamp>", [("c", "timestamp")]),
                ),
                ("csv_infer", "2024-01-02 03:04:05Z", [("c", "timestamp")]),
                ("describe", "a_ts", "timestamp"),
            ],
        ),
        (
            "A6",
            "timestamp[us, tz=America/New_York] agree",
            [
                ("arrow_in", pa.timestamp("us", tz="America/New_York"), "TimestampType:timestamp"),
                (
                    "rung",
                    "2024-01-02 03:04:05+05:00",
                    ("string", "StringType:string", "string", "varchar"),
                ),
                (
                    "reader",
                    _pa(pa.timestamp("us", tz="America/New_York")),
                    ([("c", "timestamp", True)], "struct<c:timestamp>", [("c", "timestamp")]),
                ),
                ("csv_infer", "2024-01-02 03:04:05+05:00", [("c", "timestamp")]),
            ],
        ),
        (
            "A7",
            "decimal128(10,2) agree",
            [
                ("arrow_in", pa.decimal128(10, 2), "DecimalType:decimal(10,2)"),
                ("fromddl", "decimal(10,2)", "DecimalType:decimal(10,2)"),
                (
                    "rung",
                    "1.25",
                    ("decimal128", "DecimalType:decimal(3,2)", "decimal(3,2)", "decimal(3,2)"),
                ),
                (
                    "reader",
                    _pa(pa.decimal128(10, 2)),
                    (
                        [("c", "decimal(10,2)", True)],
                        "struct<c:decimal(10,2)>",
                        [("c", "decimal(10,2)")],
                    ),
                ),
                ("describe", "a_dec", "decimal(10,2)"),
            ],
        ),
        (
            "A8",
            "char(8)/varchar(32) DDL agree",
            [
                ("fromddl", "char(8)", "CharType:char(8)"),
                ("fromddl", "varchar(32)", "VarcharType:varchar(32)"),
                ("arrow_out", _pa_char8_hint(), "string"),
                ("arrow_out", _pa_varchar32_hint(), "string"),
                ("rung", "abcdefgh", ("string", "StringType:string", "string", "varchar")),
                ("csv_infer", "abcdefgh", [("c", "string")]),
            ],
        ),
        (
            "A9",
            "timestamp[s, tz=UTC] agree",
            [
                ("arrow_in", pa.timestamp("s", tz="UTC"), "TimestampType:timestamp"),
                (
                    "reader",
                    _pa(pa.timestamp("s", tz="UTC")),
                    ([("c", "timestamp", True)], "struct<c:timestamp>", [("c", "timestamp")]),
                ),
            ],
        ),
        (
            "A10",
            "Arrow string agree",
            [
                ("arrow_in", pa.string(), "StringType:string"),
                (
                    "reader",
                    _pa(pa.string()),
                    ([("c", "string", True)], "struct<c:string>", [("c", "string")]),
                ),
            ],
        ),
        (
            "A11",
            "csv literal 39-digit integer agree",
            [
                (
                    "rung",
                    "999999999999999999999999999999999999999",
                    ("float64", "DoubleType:double", "double", "double"),
                ),
                (
                    "csv_infer",
                    "999999999999999999999999999999999999999",
                    [("c", "double")],
                ),
            ],
        ),
        (
            "D1",
            "Arrow timestamp[us] tz-naive",
            [
                ("arrow_in", pa.timestamp("us"), "TimestampNTZType:timestamp_ntz"),
                (
                    "reader",
                    _pa(pa.timestamp("us")),
                    (
                        [("c", "timestamp_ntz", True)],
                        "struct<c:timestamp_ntz>",
                        [("c", "timestamp_ntz")],
                    ),
                ),
            ],
        ),
        (
            "D2",
            "session spark.sql.timestampType=TIMESTAMP_NTZ",
            [
                ("ntz_default", "", "TimestampNTZType:timestamp_ntz"),
                (
                    "rung_ntz",
                    "2024-01-02 03:04:05",
                    (
                        "timestamp",
                        "TimestampNTZType:timestamp_ntz",
                        "timestamp_ntz",
                        "varchar",
                    ),
                ),
                ("csv_infer_ntz", "2024-01-02 03:04:05", [("c", "timestamp")]),
            ],
        ),
        (
            "D3",
            "csv literal '1.23'",
            [
                (
                    "rung",
                    "1.23",
                    ("decimal128", "DecimalType:decimal(3,2)", "decimal(3,2)", "decimal(3,2)"),
                ),
                ("csv_infer", "1.23", [("c", "double")]),
            ],
        ),
        (
            "D4",
            "csv literal decimal(38,18)-shaped",
            [
                (
                    "rung",
                    "12345678901234567890.123456789012345678",
                    (
                        "decimal128",
                        "DecimalType:decimal(38,18)",
                        "decimal(38,18)",
                        "decimal(38,18)",
                    ),
                ),
                ("csv_infer", "12345678901234567890.123456789012345678", [("c", "double")]),
            ],
        ),
        (
            "D5",
            "csv literal '42'",
            [
                ("rung", "42", ("int32", "IntegerType:int", "int", "int")),
                ("csv_infer", "42", [("c", "bigint")]),
            ],
        ),
        (
            "D6",
            "decimal256(76,10)",
            [
                ("arrow_in", pa.decimal256(76, 10), "DecimalType:decimal(76,10)"),
                (
                    "arrow_out",
                    _pa_decimal76_hint(),
                    "REFUSED ValueError: precision should be between 1 and 38",
                ),
                (
                    "rung",
                    "123456789012345678901234567890123456789",
                    ("float64", "DoubleType:double", "double", "double"),
                ),
                (
                    "reader",
                    _pa(pa.decimal256(76, 10)),
                    (
                        [("c", "decimal(76,10)", True)],
                        "struct<c:decimal(76,10)>",
                        [("c", "decimal(76,10)")],
                    ),
                ),
            ],
        ),
        (
            "D7",
            "Arrow binary/large_binary",
            [
                ("arrow_in", pa.binary(), "BinaryType:binary"),
                ("arrow_in", pa.large_binary(), "BinaryType:binary"),
                ("arrow_out", BinaryType(), "binary"),
                ("rung", "DEADBEEF", ("string", "StringType:string", "string", "varchar")),
                (
                    "reader",
                    _pa(pa.binary()),
                    ([("c", "string", True)], "struct<c:string>", [("c", "string")]),
                ),
                ("csv_infer", "DEADBEEF", [("c", "string")]),
                ("describe", "a_bin", "binary"),
            ],
        ),
        (
            "D8",
            "Arrow float32",
            [
                ("arrow_in", pa.float32(), "FloatType:float"),
                (
                    "reader",
                    _pa(pa.float32()),
                    ([("c", "double", True)], "struct<c:double>", [("c", "double")]),
                ),
                ("describe", "a_float", "float"),
            ],
        ),
        (
            "D9",
            "Arrow int8/int16",
            [
                ("arrow_in", pa.int8(), "ByteType:tinyint"),
                ("arrow_in", pa.int16(), "ShortType:smallint"),
                ("reader", _pa(pa.int8()), ([("c", "int", True)], "struct<c:int>", [("c", "int")])),
                (
                    "reader",
                    _pa(pa.int16()),
                    ([("c", "int", True)], "struct<c:int>", [("c", "int")]),
                ),
                ("describe", "a_tiny", "int"),
                ("describe", "a_small", "int"),
            ],
        ),
        (
            "D10",
            "Arrow uint8/uint16/uint32",
            [
                ("arrow_in", pa.uint8(), "StringType:string"),
                ("arrow_in", pa.uint16(), "StringType:string"),
                ("arrow_in", pa.uint32(), "StringType:string"),
                (
                    "reader",
                    _pa(pa.uint8()),
                    ([("c", "int", True)], "struct<c:int>", [("c", "int")]),
                ),
                (
                    "reader",
                    _pa(pa.uint16()),
                    ([("c", "int", True)], "struct<c:int>", [("c", "int")]),
                ),
                (
                    "reader",
                    _pa(pa.uint32()),
                    ([("c", "int", True)], "struct<c:int>", [("c", "int")]),
                ),
            ],
        ),
        (
            "D11",
            "Arrow duration[us]",
            [
                ("arrow_in", pa.duration("us"), "StringType:string"),
                ("rung", "1 day", ("string", "StringType:string", "string", "varchar")),
                (
                    "reader",
                    _pa(pa.duration("us")),
                    ([("c", "Duration(Microsecond)", True)], "struct<c:string>", [("c", "string")]),
                ),
            ],
        ),
        (
            "D12",
            "Arrow month_day_nano_interval",
            [
                ("arrow_in", pa.month_day_nano_interval(), "StringType:string"),
                (
                    "reader",
                    _pa(pa.month_day_nano_interval()),
                    (
                        [("c", "Interval(MonthDayNano)", True)],
                        "struct<c:string>",
                        [("c", "string")],
                    ),
                ),
            ],
        ),
        (
            "D13",
            "Arrow time64[us]",
            [
                ("arrow_in", pa.time64("us"), "StringType:string"),
                (
                    "reader",
                    _pa(pa.time64("us")),
                    ([("c", "Time64(Microsecond)", True)], "struct<c:string>", [("c", "string")]),
                ),
            ],
        ),
        (
            "D14",
            "interval DDL refusal + string degradation",
            [
                ("arrow_out", _pa_daytime_hint(), "string"),
                ("arrow_out", _pa_yearmonth_hint(), "string"),
                (
                    "fromddl",
                    "interval day to second",
                    "REFUSED ValueError: cannot parse datatype: 'interval day to second'",
                ),
                (
                    "fromddl",
                    "interval year to month",
                    "REFUSED ValueError: cannot parse datatype: 'interval year to month'",
                ),
                ("fromddl", "interval", "CalendarIntervalType:interval"),
                ("rung", "interval 1 year", ("string", "StringType:string", "string", "varchar")),
                ("csv_infer", "interval 1 year", [("c", "string")]),
            ],
        ),
        (
            "D15",
            "struct field nullable=False",
            [
                (
                    "schema_back",
                    _schema_back_fields_hint(),
                    [("req", "IntegerType:int", False), ("opt", "StringType:string", True)],
                ),
                ("toddl", _probe_table_schema(), "req INT NOT NULL,opt STRING"),
                (
                    "toddl_reparse",
                    _probe_table_schema(),
                    "REFUSED ValueError: cannot parse datatype: 'req INT NOT NULL,opt STRING'",
                ),
                (
                    "reader",
                    _pa_struct_nonnull_hint(),
                    (
                        [("c", "struct<req:int>", False)],
                        "struct<c:struct<req:int>>",
                        [("c", "struct<req:int>")],
                    ),
                ),
            ],
        ),
        (
            "D16",
            "ArrayType(int, containsNull=False)",
            [
                ("arrow_out", _pa_array_nocn_hint(), "list<item: int32>"),
                ("arrow_in_cn", _pa_list_nonnull_hint(), ("ArrayType:array<int>", True)),
                (
                    "reader",
                    _pa_list_nonnull_field_hint(),
                    ([("c", "array<int>", True)], "struct<c:array<int>>", [("c", "array<int>")]),
                ),
            ],
        ),
        (
            "D17",
            "MapType(str,int, valueContainsNull=False)",
            [
                ("arrow_out", _pa_map_novcn_hint(), "map<string, int32>"),
                ("arrow_in_vcn", _pa_map_hint(), ("MapType:map<string,int>", True)),
                (
                    "arrow_in_vcn",
                    _pa_map_nonnull_value_hint(),
                    ("MapType:map<string,int>", True),
                ),
                (
                    "reader",
                    _pa_map_field_hint(),
                    (
                        [("c", "map<string,int>", True)],
                        "struct<c:map<string,int>>",
                        [("c", "map<string,int>")],
                    ),
                ),
            ],
        ),
        (
            "D18",
            "Iceberg Float32 DESCRIBE vs dtypes",
            [
                ("describe", "a_float", "float"),
                ("table_dtypes", "a_float", "double"),
                ("table_key", "a_float", "double"),
            ],
        ),
        (
            "D19",
            "Iceberg Binary DESCRIBE vs dtypes",
            [
                ("describe", "a_bin", "binary"),
                ("table_dtypes", "a_bin", "string"),
                ("table_key", "a_bin", "string"),
            ],
        ),
        (
            "D20",
            "Arrow dictionary",
            [
                ("arrow_in", pa.dictionary(pa.int32(), pa.string()), "StringType:string"),
                (
                    "reader",
                    _pa(pa.dictionary(pa.int32(), pa.string())),
                    (
                        [("c", "Dictionary(Int32, Utf8)", True)],
                        "struct<c:string>",
                        [("c", "string")],
                    ),
                ),
            ],
        ),
        (
            "D21",
            "Arrow null",
            [
                ("arrow_in", pa.null(), "NullType:void"),
                ("arrow_out", _pa_null_hint(), "null"),
                ("fromddl", "void", "NullType:void"),
                (
                    "reader",
                    _pa(pa.null()),
                    ([("c", "Null", True)], "struct<c:void>", [("c", "void")]),
                ),
            ],
        ),
        (
            "D22",
            "Arrow uint64",
            [
                ("arrow_in", pa.uint64(), "StringType:string"),
                (
                    "reader",
                    _pa(pa.uint64()),
                    ([("c", "long", True)], "struct<c:bigint>", [("c", "bigint")]),
                ),
            ],
        ),
        (
            "D23",
            "csv literal '2024-01-02 03:04:05+05:00'",
            [
                (
                    "rung",
                    "2024-01-02 03:04:05+05:00",
                    ("string", "StringType:string", "string", "varchar"),
                ),
                ("csv_infer", "2024-01-02 03:04:05+05:00", [("c", "timestamp")]),
            ],
        ),
        (
            "D24",
            "csv literal 38-digit integer",
            [
                (
                    "rung",
                    "99999999999999999999999999999999999999",
                    (
                        "decimal128",
                        "DecimalType:decimal(38,0)",
                        "decimal(38,0)",
                        "decimal(38,0)",
                    ),
                ),
                (
                    "csv_infer",
                    "99999999999999999999999999999999999999",
                    [("c", "double")],
                ),
            ],
        ),
    ]
    return rows


def _pa_long_hint() -> Any:

    return LongType()


def _pa_char8_hint() -> Any:

    return CharType(8)


def _pa_varchar32_hint() -> Any:

    return VarcharType(32)


def _pa_decimal76_hint() -> Any:
    return DecimalType(76, 10)


def _pa_daytime_hint() -> Any:
    return DayTimeIntervalType()


def _pa_yearmonth_hint() -> Any:
    return YearMonthIntervalType()


def _pa_array_nocn_hint() -> Any:
    return ArrayType(IntegerType(), False)


def _pa_map_novcn_hint() -> Any:
    return MapType(StringType(), IntegerType(), False)


def _pa_null_hint() -> Any:

    return NullType()


def _schema_back_fields_hint() -> Any:
    import pyarrow as pa

    return [pa.field("req", pa.int32(), False), pa.field("opt", pa.string(), True)]


def _pa_struct_nonnull_hint() -> Any:
    import pyarrow as pa

    return pa.field("c", pa.struct([pa.field("req", pa.int32(), False)]), False)


def _pa_list_nonnull_hint() -> Any:
    import pyarrow as pa

    return pa.list_(pa.field("item", pa.int32(), False))


def _pa_list_nonnull_field_hint() -> Any:
    import pyarrow as pa

    return pa.field("c", _pa_list_nonnull_hint())


def _pa_map_hint() -> Any:
    import pyarrow as pa

    return pa.map_(pa.string(), pa.int32())


def _pa_map_nonnull_value_hint() -> Any:
    import pyarrow as pa

    return pa.map_(pa.string(), pa.field("value", pa.int32(), nullable=False))


def _pa_map_field_hint() -> Any:
    import pyarrow as pa

    return pa.field("c", pa.map_(pa.string(), pa.int32()))


CENSUS_ROWS = _build_rows()


@pytest.fixture
def census_session(tmp_path: Path) -> Any:
    """Session plus the DESCRIBE probe table for reader-surface census rows."""
    session = (
        ReparkSession.builder.appName("facade-4-census-pins")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE IF NOT EXISTS mem.ns").collect()
    session.sql(
        "CREATE TABLE mem.ns.probe USING iceberg AS SELECT "
        "CAST(1 AS TINYINT) a_tiny, CAST(1 AS SMALLINT) a_small, CAST(1 AS INT) a_int, "
        "CAST(1 AS BIGINT) a_big, CAST(1.5 AS FLOAT) a_float, CAST(1.5 AS DOUBLE) a_double, "
        "CAST('x' AS BINARY) a_bin, CAST(1.5 AS DECIMAL(10,2)) a_dec, "
        "CAST('2024-01-02' AS DATE) a_date, "
        "CAST('2024-01-02 03:04:05' AS TIMESTAMP) a_ts"
    ).collect()
    return session


def _csv_infer_ntz(session: Any, tmp_path: Path, literal: str) -> Any:
    """Reader lattice (iii): served ``inferSchema`` under an NTZ session conf."""
    from repark.spark.session.timestamp_type import TIMESTAMP_TYPE_KEY

    session.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP_NTZ")
    try:
        return _csv_infer(session, tmp_path, literal)
    finally:
        session.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP_LTZ")


def _rung_ntz(session: Any, literal: str) -> Any:
    """CSV table (ii): rung answers for one literal under an NTZ session conf."""
    from repark.spark.session.timestamp_type import TIMESTAMP_TYPE_KEY

    session.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP_NTZ")
    try:
        return _rung(literal)
    finally:
        session.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP_LTZ")


def _run_check(check: tuple[str, str, Any], session: Any, tmp_path: Path) -> Any:
    """Measure one census surface for a row spec entry."""
    surface, argument, _expected = check
    if surface == "arrow_in":
        return _arrow_in(argument)
    if surface == "arrow_in_cn":
        return _arrow_in_contains_null(argument)
    if surface == "arrow_in_vcn":
        return _arrow_in_value_contains_null(argument)
    if surface == "arrow_out":
        return _arrow_out(argument)
    if surface == "fromddl":
        return _from_ddl(argument)
    if surface == "toddl":
        return _to_ddl(argument)
    if surface == "toddl_reparse":
        return _to_ddl_reparse(argument)
    if surface == "schema_back":
        return _schema_back(argument)
    if surface == "rung":
        return _rung(argument)
    if surface == "rung_ntz":
        return _rung_ntz(session, argument)
    if surface == "reader":
        return _reader(session, argument)
    if surface == "csv_infer":
        return _csv_infer(session, tmp_path, argument)
    if surface == "csv_infer_ntz":
        return _csv_infer_ntz(session, tmp_path, argument)
    if surface == "describe":
        return _describe_col(session, argument)
    if surface == "table_dtypes":
        return _table_dtypes_col(session, argument)
    if surface == "table_key":
        return _table_key_col(session, argument)
    if surface == "ntz_default":
        return _ntz_default(session)
    raise AssertionError(f"unknown census surface {surface!r}")


_SESSION_SURFACES = frozenset(
    {
        "reader",
        "csv_infer",
        "csv_infer_ntz",
        "describe",
        "table_dtypes",
        "table_key",
        "ntz_default",
        "rung_ntz",
    }
)


@pytest.mark.parametrize(
    "row_id,checks",
    [(row_id, checks) for row_id, _title, checks in CENSUS_ROWS],
    ids=[row_id for row_id, _title, checks in CENSUS_ROWS],
)
def test_census_row(
    row_id: str, checks: list[tuple[str, str, Any]], request: Any, tmp_path: Path
) -> None:
    """One census row's measured answers across every surface it names.

    pins: facade-4/C-009
    """
    session = None
    for check in checks:
        surface, _argument, expected = check
        if surface in _SESSION_SURFACES and session is None:
            session = request.getfixturevalue("census_session")
        measured = _run_check(check, session, tmp_path)
        assert measured == expected, f"{row_id} {surface}: {measured!r} != {expected!r}"

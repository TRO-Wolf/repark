"""FACADE-4 step 0 census probe — what each of the three conversion tables answers.

Calls (i) the facade ``types.py`` conversions, (ii) the ``_csv_smart`` rung table,
(iii) the reader lattice plus the Rust ``arrow_type_key`` / ``spark_ddl_type_name``
spellings reachable through ``logical_schema_fields`` and ``DESCRIBE TABLE`` on a
memory-catalog Iceberg table. Prints one JSON object to stdout.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any


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


def _facade_answers() -> dict[str, Any]:
    """Table (i): ``types.py`` answers for each probe input."""
    import pyarrow as pa

    from repark.spark import types as t

    arrow_probes = {
        "pa.timestamp_us_utc": pa.timestamp("us", tz="UTC"),
        "pa.timestamp_us_ny": pa.timestamp("us", tz="America/New_York"),
        "pa.timestamp_us": pa.timestamp("us"),
        "pa.timestamp_s": pa.timestamp("s", tz="UTC"),
        "pa.decimal128_10_2": pa.decimal128(10, 2),
        "pa.decimal128_38_18": pa.decimal128(38, 18),
        "pa.decimal128_38_0": pa.decimal128(38, 0),
        "pa.decimal256_76_10": pa.decimal256(76, 10),
        "pa.date32": pa.date32(),
        "pa.date64": pa.date64(),
        "pa.binary": pa.binary(),
        "pa.large_binary": pa.large_binary(),
        "pa.uint8": pa.uint8(),
        "pa.uint64": pa.uint64(),
        "pa.float16": pa.float16(),
        "pa.time64_us": pa.time64("us"),
        "pa.duration_us": pa.duration("us"),
        "pa.month_day_nano": pa.month_day_nano_interval(),
        "pa.dictionary": pa.dictionary(pa.int32(), pa.string()),
        "pa.null": pa.null(),
        "pa.list_int32": pa.list_(pa.field("item", pa.int32(), True)),
        "pa.list_int32_nonnull": pa.list_(pa.field("item", pa.int32(), False)),
        "pa.map_str_int": pa.map_(pa.string(), pa.int32()),
        "pa.struct_req": pa.struct(
            [pa.field("req", pa.int32(), False), pa.field("opt", pa.string(), True)]
        ),
    }
    spark_probes = {
        "TimestampType": t.TimestampType(),
        "TimestampNTZType": t.TimestampNTZType(),
        "DecimalType(10,2)": t.DecimalType(10, 2),
        "DecimalType(38,18)": t.DecimalType(38, 18),
        "DecimalType(38,0)": t.DecimalType(38, 0),
        "DecimalType(76,10)": t.DecimalType(76, 10),
        "DateType": t.DateType(),
        "BinaryType": t.BinaryType(),
        "CharType(8)": t.CharType(8),
        "VarcharType(32)": t.VarcharType(32),
        "YearMonthIntervalType": t.YearMonthIntervalType(),
        "DayTimeIntervalType": t.DayTimeIntervalType(),
        "CalendarIntervalType": t.CalendarIntervalType(),
        "ArrayType(int,noNull)": t.ArrayType(t.IntegerType(), False),
        "MapType(str,int,noNull)": t.MapType(t.StringType(), t.IntegerType(), False),
    }
    ddl_probes = [
        "timestamp",
        "timestamp_ntz",
        "decimal(10,2)",
        "decimal(38,18)",
        "decimal(38,0)",
        "date",
        "binary",
        "char(8)",
        "varchar(32)",
        "interval",
        "interval year to month",
        "interval day to second",
        "a INT NOT NULL",
        "a INT,b STRING",
        "struct<req:int,opt:string>",
        "array<int>",
        "map<string,int>",
    ]
    return {
        "arrow_to_repark": {
            name: _attempt(lambda key=name: _shape(t._arrow_type_to_repark(arrow_probes[key])))
            for name in arrow_probes
        },
        "repark_to_arrow": {
            name: _attempt(lambda key=name: str(t.repark_type_to_arrow(spark_probes[key])))
            for name in spark_probes
        },
        "fromddl": {
            ddl: _attempt(lambda text=ddl: _shape(t.DataType.fromDDL(text)))
            for ddl in ddl_probes
        },
        "toddl": _attempt(
            lambda: t.StructType(
                [
                    t.StructField("req", t.IntegerType(), False),
                    t.StructField("opt", t.StringType(), True),
                ]
            ).toDDL()
        ),
        "toddl_reparse": _attempt(
            lambda: _shape(
                t.DataType.fromDDL(
                    t.StructType(
                        [
                            t.StructField("req", t.IntegerType(), False),
                            t.StructField("opt", t.StringType(), True),
                        ]
                    ).toDDL()
                )
            )
        ),
        "struct_type_from_arrow_nullability": _attempt(
            lambda: [
                (field.name, _shape(field.dataType), field.nullable)
                for field in t.struct_type_from_arrow(
                    pa.schema(
                        [
                            pa.field("req", pa.int32(), False),
                            pa.field("lst", pa.list_(pa.field("item", pa.int32(), False))),
                            pa.field("mp", pa.map_(pa.string(), pa.int32())),
                            pa.field(
                                "st",
                                pa.struct([pa.field("inner", pa.int32(), False)]),
                                False,
                            ),
                        ]
                    )
                ).fields
            ]
        ),
        "repark_to_arrow_nullability": _attempt(
            lambda: str(
                pa.struct(
                    [
                        pa.field(
                            "arr",
                            t.repark_type_to_arrow(t.ArrayType(t.IntegerType(), False)),
                        ),
                        pa.field(
                            "mp",
                            t.repark_type_to_arrow(
                                t.MapType(t.StringType(), t.IntegerType(), False)
                            ),
                        ),
                        pa.field(
                            "st",
                            t.repark_type_to_arrow(
                                t.StructType([t.StructField("req", t.IntegerType(), False)])
                            ),
                        ),
                    ]
                )
            )
        ),
    }


def _csv_smart_answers() -> dict[str, Any]:
    """Table (ii): ``_csv_smart`` rung answers for each literal probe."""
    from repark.spark import _csv_smart as smart

    literals = {
        "lit_timestamp_naive": ["2024-01-02 03:04:05"],
        "lit_timestamp_tz_z": ["2024-01-02 03:04:05Z"],
        "lit_timestamp_offset": ["2024-01-02 03:04:05+05:00"],
        "lit_date": ["2024-01-02"],
        "lit_decimal_10_2": ["1.25"],
        "lit_decimal_38_18": ["0.123456789012345678"],
        "lit_decimal_38_0": ["12345678901234567890123456789012345678"],
        "lit_decimal_39_over": ["123456789012345678901234567890123456789"],
        "lit_decimal_comma": ["1,25"],
        "lit_int32": ["42"],
        "lit_int64": ["9999999999"],
        "lit_bool": ["true"],
        "lit_float_sci": ["1.5e3"],
        "lit_binary": ["DEADBEEF"],
        "lit_interval": ["interval 1 year"],
        "lit_char": ["abcdefgh"],
    }
    out: dict[str, Any] = {}
    for name, values in literals.items():
        resolution = smart.resolve_column_type(values)
        spark_type = _attempt(lambda res=resolution: _shape(smart.rung_to_spark_type(res)))
        out[name] = {
            "rung": resolution.rung,
            "decimal": (
                [resolution.decimal_precision, resolution.decimal_scale]
                if resolution.decimal_precision is not None
                else None
            ),
            "spark_type": spark_type,
            "engine_cast": _attempt(lambda res=resolution: smart.rung_to_engine_cast(res)),
            "sql_cast": _attempt(lambda res=resolution: smart.rung_to_sql_cast(res)),
        }
    return out


def _arrow_frame(session: Any, fields: list[Any]) -> Any:
    """Materialize a one-row frame from raw Arrow fields through the C-stream door."""
    import pyarrow as pa

    from repark.spark.session.create_dataframe_rows import (
        _materialize_arrow_as_memtable_frame,
    )

    arrays = [_probe_array(field) for field in fields]
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    return _materialize_arrow_as_memtable_frame(session, table)


def _probe_array(field: Any) -> Any:
    """One-row array honouring the field's nullability."""
    import pyarrow as pa

    if not field.nullable:
        if pa.types.is_struct(field.type):
            return pa.array([{child.name: 1 for child in field.type}], type=field.type)
        return pa.array([1], type=field.type)
    if pa.types.is_struct(field.type):
        return pa.array([{child.name: 1 for child in field.type}], type=field.type)
    return pa.array([None], type=field.type)


def _reader_lattice_answers(session: Any) -> dict[str, Any]:
    """Table (iii): Rust type_key + ``df.schema`` + reader inferSchema answers."""
    import pyarrow as pa

    probes = [
        ("ts_utc", pa.field("c", pa.timestamp("us", tz="UTC"))),
        ("ts_ny", pa.field("c", pa.timestamp("us", tz="America/New_York"))),
        ("ts_ntz", pa.field("c", pa.timestamp("us"))),
        ("ts_s", pa.field("c", pa.timestamp("s", tz="UTC"))),
        ("dec_10_2", pa.field("c", pa.decimal128(10, 2))),
        ("dec_38_18", pa.field("c", pa.decimal128(38, 18))),
        ("dec_38_0", pa.field("c", pa.decimal128(38, 0))),
        ("dec_256", pa.field("c", pa.decimal256(76, 10))),
        ("date32", pa.field("c", pa.date32())),
        ("date64", pa.field("c", pa.date64())),
        ("binary", pa.field("c", pa.binary())),
        ("uint8", pa.field("c", pa.uint8())),
        ("int8", pa.field("c", pa.int8())),
        ("float32", pa.field("c", pa.float32())),
        ("time64", pa.field("c", pa.time64("us"))),
        ("duration", pa.field("c", pa.duration("us"))),
        ("month_day_nano", pa.field("c", pa.month_day_nano_interval())),
        ("list_nonnull", pa.field("c", pa.list_(pa.field("item", pa.int32(), False)))),
        ("map", pa.field("c", pa.map_(pa.string(), pa.int32()))),
        (
            "struct_req",
            pa.field(
                "c",
                pa.struct([pa.field("req", pa.int32(), False)]),
                False,
            ),
        ),
    ]
    out: dict[str, Any] = {}
    for name, arrow_field in probes:
        frame = _attempt(lambda fld=arrow_field: _arrow_frame(session, [fld]))
        if isinstance(frame, str):
            out[name] = {"frame": frame}
            continue
        keys = _attempt(lambda f=frame: f._inner.logical_schema_fields())
        schema = _attempt(lambda f=frame: f.schema.simpleString())
        dtypes = _attempt(lambda f=frame: f.dtypes)
        out[name] = {"type_key": keys, "df_schema": schema, "dtypes": dtypes}
    return out


def _reader_csv_answers(session: Any, tmpdir: Path) -> dict[str, Any]:
    """``spark.read.csv(inferSchema=True)`` dtypes per literal probe."""
    literals = {
        "csv_timestamp": "2024-01-02 03:04:05",
        "csv_timestamp_z": "2024-01-02 03:04:05Z",
        "csv_date": "2024-01-02",
        "csv_decimal_10_2": "1.25",
        "csv_decimal_38_18": "0.123456789012345678",
        "csv_decimal_38_0": "12345678901234567890123456789012345678",
        "csv_decimal_39_over": "123456789012345678901234567890123456789",
        "csv_int32": "42",
        "csv_int64": "9999999999",
        "csv_bool": "true",
        "csv_float_sci": "1.5e3",
        "csv_binary": "DEADBEEF",
        "csv_interval": "interval 1 year",
    }
    out: dict[str, Any] = {}
    for name, literal in literals.items():
        path = tmpdir / f"{name}.csv"
        path.write_text(f"c\n{literal}\n{literal}\n", encoding="utf-8")
        out[name] = _attempt(
            lambda p=str(path): session.read.csv(
                p, header=True, inferSchema=True
            ).dtypes
        )
    return out


def _describe_answers(session: Any, tmpdir: Path) -> dict[str, Any]:
    """Rust ``spark_ddl_type_name`` top-level via DESCRIBE TABLE on a memory catalog."""
    out = _attempt(lambda: session.register_memory_catalog("mem", str(tmpdir / "wh")))
    if isinstance(out, str) and out.startswith("REFUSED"):
        return {"catalog": out}
    _attempt(lambda: session.sql("CREATE NAMESPACE IF NOT EXISTS mem.ns").collect())
    created = _attempt(
        lambda: session.sql(
            "CREATE TABLE mem.ns.probe USING iceberg AS SELECT "
            "CAST(1 AS TINYINT) a_tiny, CAST(1 AS SMALLINT) a_small, CAST(1 AS INT) a_int, "
            "CAST(1 AS BIGINT) a_big, CAST(1.5 AS FLOAT) a_float, CAST(1.5 AS DOUBLE) a_double, "
            "CAST('x' AS BINARY) a_bin, CAST(1.5 AS DECIMAL(10,2)) a_dec, "
            "CAST('2024-01-02' AS DATE) a_date, "
            "CAST('2024-01-02 03:04:05' AS TIMESTAMP) a_ts"
        ).collect()
    )
    if isinstance(created, str):
        return {"create": created}
    rows = _attempt(lambda: session.sql("DESCRIBE TABLE mem.ns.probe").collect())
    if isinstance(rows, str):
        return {"describe": rows}
    return {
        "describe": [str(row) for row in rows],
        "table_dtypes": _attempt(lambda: session.table("mem.ns.probe").dtypes),
        "table_type_keys": _attempt(
            lambda: session.table("mem.ns.probe")._inner.logical_schema_fields()
        ),
    }


def main() -> None:
    """Run the census and print JSON."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("facade-4-census")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    try:
        with tempfile.TemporaryDirectory(prefix="facade4-census-") as tmp:
            tmpdir = Path(tmp)
            payload = {
                "facade_types_py": _facade_answers(),
                "csv_smart_rungs": _csv_smart_answers(),
                "reader_lattice_rust": _reader_lattice_answers(session),
                "reader_csv_infer": _reader_csv_answers(session, tmpdir),
                "describe_rust_names": _describe_answers(session, tmpdir),
            }
            session.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
            from repark.spark.session.timestamp_type import default_timestamp_data_type

            payload["session_ntz_default_timestamp"] = _attempt(
                lambda: _shape(default_timestamp_data_type())
            )
            payload["reader_csv_infer_ntz"] = _reader_csv_answers(session, tmpdir)
    finally:
        session.stop()
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()

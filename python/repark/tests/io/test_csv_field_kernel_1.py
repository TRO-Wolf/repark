from __future__ import annotations

import math
import struct
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions
from repark.spark.column import Column

pytestmark = pytest.mark.xdist_group("csv_field_kernel_1")
ORACLE_SPECS = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal(10,2)",
    "boolean",
    "date",
    "timestamp",
    "timestamp_ntz",
    "string",
    "binary",
)
ADDED_SPECS = (*ORACLE_SPECS, "decimal(38,10)", "decimal(3,0)", "decimal(4,4)")
UNICODE_DIGITS = ("\u0661\u0662\u0663", "\uff11\uff12")
DECLARED = {
    (text, spec)
    for text in UNICODE_DIGITS
    for spec in (
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "decimal(10,2)",
        "decimal(38,10)",
        "decimal(3,0)",
    )
}
ORACLE = [
    ("0", "0 0 0 0 0.0 0.0 0 N N N N = ="),
    ("-0", "0 0 0 0 -0.0 -0.0 0 N N N N = ="),
    ("007", "7 7 7 7 7.0 7.0 700 N N N N = ="),
    ("+5", "5 5 5 5 5.0 5.0 500 N N N N = ="),
    ("-007", "-7 -7 -7 -7 -7.0 -7.0 -700 N N N N = ="),
    ("127", "127 127 127 127 127.0 127.0 12700 N N N N = ="),
    ("128", "N 128 128 128 128.0 128.0 12800 N N N N = ="),
    ("-129", "N -129 -129 -129 -129.0 -129.0 -12900 N N N N = ="),
    ("200", "N 200 200 200 200.0 200.0 20000 N N N N = ="),
    (
        "32767",
        "N 32767 32767 32767 32767.0 32767.0 "
        "3276700 N 11248373 971859427200000000 971859427200000000 = =",
    ),
    (
        "32768",
        "N N 32768 32768 32768.0 32768.0 "
        "3276800 N 11248738 971890963200000000 971890963200000000 = =",
    ),
    (
        "40000",
        "N N 40000 40000 40000.0 40000.0 "
        "4000000 N 13890172 1200110860800000000 1200110860800000000 = =",
    ),
    ("2147483647", "N N 2147483647 2147483647 2147483648.0 2147483647.0 N N N N N = ="),
    ("2147483648", "N N N 2147483648 2147483648.0 2147483648.0 N N N N N = ="),
    (
        "9223372036854775807",
        "N N N 9223372036854775807 9.223372036854776e+18 9.223372036854776e+18 N N N N N = =",
    ),
    ("9223372036854775808", "N N N N 9.223372036854776e+18 9.223372036854776e+18 N N N N N = ="),
    ("99999999999999999999", "N N N N 1.0000000200408773e+20 1e+20 N N N N N = ="),
    (" 7", "N N N N 7.0 7.0 N N N N N = ="),
    ("7 ", "N N N N 7.0 7.0 N N N N N = ="),
    ("\t7", "N N N N 7.0 7.0 N N N N N = ="),
    (" 7 ", "N N N N 7.0 7.0 N N N N N = ="),
    ("1.50", "N N N N 1.5 1.5 150 N N N N = ="),
    ("1.5", "N N N N 1.5 1.5 150 N N N N = ="),
    ("5.", "N N N N 5.0 5.0 500 N N N N = ="),
    (".5", "N N N N 0.5 0.5 50 N N N N = ="),
    ("1e3", "N N N N 1000.0 1000.0 100000 N N N N = ="),
    ("1E-2", "N N N N 0.009999999776482582 0.01 1 N N N N = ="),
    ("1.555", "N N N N 1.5549999475479126 1.555 156 N N N N = ="),
    ("-1.555", "N N N N -1.5549999475479126 -1.555 -156 N N N N = ="),
    ("1.554", "N N N N 1.5540000200271606 1.554 155 N N N N = ="),
    ("99999999.995", "N N N N 100000000.0 99999999.995 N N N N N = ="),
    ("123456789012", "N N N 123456789012 123456790528.0 123456789012.0 N N N N N = ="),
    ("1e400", "N N N N inf inf N N N N N = ="),
    ("1,5", "N N N N N N 1500 N N N N = ="),
    ("1,000.5", "N N N N N N 100050 N N N N = ="),
    ("1d", "N N N N 1.0 1.0 N N N N N = ="),
    ("1f", "N N N N 1.0 1.0 N N N N N = ="),
    ("0x10", "N N N N N N N N N N N = ="),
    ("0x1p3", "N N N N 8.0 8.0 N N N N N = ="),
    ("1_000", "N N N N N N N N N N N = ="),
    ("NaN", "N N N N nan nan N N N N N = ="),
    ("nan", "N N N N N N N N N N N = ="),
    ("Inf", "N N N N inf inf N N N N N = ="),
    ("+Inf", "N N N N N N N N N N N = ="),
    ("-Inf", "N N N N -inf -inf N N N N N = ="),
    ("inf", "N N N N N N N N N N N = ="),
    ("Infinity", "N N N N inf inf N N N N N = ="),
    ("-Infinity", "N N N N -inf -inf N N N N N = ="),
    ("true", "N N N N N N N true N N N = ="),
    ("TRUE", "N N N N N N N true N N N = ="),
    ("True", "N N N N N N N true N N N = ="),
    ("false", "N N N N N N N false N N N = ="),
    ("t", "N N N N N N N N N N N = ="),
    ("1", "1 1 1 1 1.0 1.0 100 N N N N = ="),
    ("y", "N N N N N N N N N N N = ="),
    ("yes", "N N N N N N N N N N N = ="),
    (" true", "N N N N N N N N N N N = ="),
    ("2024", "N 2024 2024 2024 2024.0 2024.0 202400 N 19723 1704067200000000 1704067200000000 = ="),
    ("2024-1", "N N N N N N N N 19723 1704067200000000 1704067200000000 = ="),
    ("2024-1-5", "N N N N N N N N 19727 1704412800000000 1704412800000000 = ="),
    ("2024-01-15", "N N N N N N N N 19737 1705276800000000 1705276800000000 = ="),
    ("20240115", "N N 20240115 20240115 20240116.0 20240115.0 2024011500 N N N N = ="),
    (" 2024-01-15", "N N N N N N N N 19737 1705276800000000 1705276800000000 = ="),
    ("2024-01-15T10:30:00", "N N N N N N N N 19737 1705314600000000 1705314600000000 = ="),
    ("2024-01-15 10:30:00", "N N N N N N N N 19737 1705314600000000 1705314600000000 = ="),
    ("2024-01-15 10:30", "N N N N N N N N 19737 1705314600000000 1705314600000000 = ="),
    ("2024-01-15T10:30:00Z", "N N N N N N N N 19737 1705314600000000 N = ="),
    ("2024-01-15 10:30:00+02:00", "N N N N N N N N 19737 1705307400000000 N = ="),
    ("2024-01-15 10:30:00.123456", "N N N N N N N N 19737 1705314600123456 1705314600123456 = ="),
    ("1705314600", "N N 1705314600 1705314600 1705314560.0 1705314600.0 N N N N N = ="),
    ("1-2", "N N N N N N N N N N N = ="),
    ("1 02:03:04", "N N N N N N N N N N N = ="),
    ("INTERVAL '1-2' YEAR TO MONTH", "N N N N N N N N N N N = ="),
    ("abc", "N N N N N N N N N N N = ="),
]
ADDED = [
    (" ", "N N N N N N N N N N N = = N N N"),
    (".", "N N N N N N N N N N N = = N N N"),
    ("+NaN", "N N N N nan nan N N N N N = = N N N"),
    ("-NaN", "N N N N nan nan N N N N N = = N N N"),
    ("NaN ", "N N N N nan nan N N N N N = = N N N"),
    (" Inf", "N N N N N N N N N N N = = N N N"),
    ("+Infinity", "N N N N inf inf N N N N N = = N N N"),
    ("1D", "N N N N 1.0 1.0 N N N N N = = N N N"),
    ("1e3f", "N N N N 1000.0 1000.0 N N N N N = = N N N"),
    ("\x017", "N N N N 7.0 7.0 N N N N N = = N N N"),
    ("\xa07", "N N N N N N N N N N N = = N N N"),
    ("\u20037", "N N N N N N N N N N N = = N N N"),
    ("fal\u017fe", "N N N N N N N false N N N = = N N N"),
    ("FAL\u017fE", "N N N N N N N false N N N = = N N N"),
    ("\u0661\u0662\u0663", "123 123 123 123 N N 12300 N N N N = = 1230000000000 123 N"),
    ("\uff11\uff12", "12 12 12 12 N N 1200 N N N N = = 120000000000 12 N"),
    ("1e-400", "N N N N 0.0 0.0 0 N N N N = = 0 0 0"),
    ("0x1.8p1", "N N N N 3.0 3.0 N N N N N = = N N N"),
    ("-0x1p-2", "N N N N -0.25 -0.25 N N N N N = = N N N"),
    ("0X10P0", "N N N N 16.0 16.0 N N N N N = = N N N"),
    ("0x1p3d", "N N N N 8.0 8.0 N N N N N = = N N N"),
    ("1e", "N N N N N N N N N N N = = N N N"),
    ("-", "N N N N N N N N N N N = = N N N"),
    ("+", "N N N N N N N N N N N = = N N N"),
    ("1.2.3", "N N N N N N N N N N N = = N N N"),
    ("--1", "N N N N N N N N N N N = = N N N"),
    (",", "N N N N N N N N N N N = = N N N"),
    ("1,,000", "N N N N N N 100000 N N N N = = 10000000000000 N N"),
    ("-1,5", "N N N N N N -1500 N N N N = = -150000000000 -15 N"),
    ("2024-02-30", "N N N N N N N N N N N = = N N N"),
    ("2024-02-29", "N N N N N N N N 19782 1709164800000000 1709164800000000 = = N N N"),
    ("GMT2024-01-15", "N N N N N N N N 19737 1705276800000000 N = = N N N"),
    ("2024-01-15GMT", "N N N N N N N N 19737 1705276800000000 N = = N N N"),
    ("2024-01-15 10:30:00GMT", "N N N N N N N N 19737 1705314600000000 N = = N N N"),
    ("2024-01-15 10:30:00UTC", "N N N N N N N N 19737 1705314600000000 N = = N N N"),
    (
        "2024-01-15T10:30:00.1234567",
        "N N N N N N N N 19737 1705314600123456 1705314600123456 = = N N N",
    ),
    ("+2024-01-15", "N N N N N N N N 19737 1705276800000000 1705276800000000 = = N N N"),
    ("-0001-01-01", "N N N N N N N N -719893 -62198755200000000 -62198755200000000 = = N N N"),
    ("12024-01-15", "N N N N N N N N 3672162 317274796800000000 317274796800000000 = = N N N"),
    ("1234567-01-01", "N N N N N N N N 450196810 N N = = N N N"),
    ("2024-1-5 garbage", "N N N N N N N N 19727 N N = = N N N"),
    ("2024-01-15  10:30:00", "N N N N N N N N 19737 N N = = N N N"),
    ("2024-01-15\t", "N N N N N N N N 19737 1705276800000000 1705276800000000 = = N N N"),
    ("1e10", "N N N N 10000000000.0 10000000000.0 N N N N N = = 100000000000000000000 N N"),
    ("-1e-3", "N N N N -0.0010000000474974513 -0.001 0 N N N N = = -10000000 0 -10"),
    ("0.005", "N N N N 0.004999999888241291 0.005 1 N N N N = = 50000000 0 50"),
    ("-0.005", "N N N N -0.004999999888241291 -0.005 -1 N N N N = = -50000000 0 -50"),
    ("0.0049999", "N N N N 0.00499989977106452 0.0049999 0 N N N N = = 49999000 0 50"),
    ("1E+2", "N N N N 100.0 100.0 10000 N N N N = = 1000000000000 100 N"),
    ("00.50", "N N N N 0.5 0.5 50 N N N N = = 5000000000 1 5000"),
    ("1.0e0", "N N N N 1.0 1.0 100 N N N N = = 10000000000 1 N"),
    ("1e-2147483648", "N N N N 0.0 0.0 N N N N N = = N N N"),
    ("1e2147483648", "N N N N inf inf N N N N N = = N N N"),
    ("1e9999999999", "N N N N inf inf N N N N N = = N N N"),
    ("-128", "-128 -128 -128 -128 -128.0 -128.0 -12800 N N N N = = -1280000000000 -128 N"),
    (
        "-32768",
        "N -32768 -32768 -32768 -32768.0 -32768.0 -3276800 N "
        "-12687794 -1096225401600000000 -1096225401600000000 = = -327680000000000 N N",
    ),
    (
        "-2147483648",
        "N N -2147483648 -2147483648 -2147483648.0 -2147483648.0 N N "
        "N N N = = -21474836480000000000 N N",
    ),
    (
        "-9223372036854775808",
        "N N N -9223372036854775808 -9.223372036854776e+18 -9.223372036854776e+18 N N "
        "N N N = = -92233720368547758080000000000 N N",
    ),
    ("0000000000000000000000000000007", "7 7 7 7 7.0 7.0 700 N N N N = = 70000000000 7 N"),
    (
        "99999999999999999999999999999999999999",
        "N N N N 9.999999680285692e+37 1e+38 N N N N N = = N N N",
    ),
    ("999999999999999999999999999999999999999", "N N N N inf 1e+39 N N N N N = = N N N"),
    ("0.99995", "N N N N 0.9999499917030334 0.99995 100 N N N N = = 9999500000 1 N"),
]
ZONE_CELLS = [
    ("America/Los_Angeles", "date", "2024-01-15", "19737"),
    ("America/Los_Angeles", "date", "2024-01-15 10:30:00", "19737"),
    ("America/Los_Angeles", "date", "2024-01-15T10:30:00Z", "19737"),
    ("America/Los_Angeles", "date", "2024-01-15 10:30:00+02:00", "19737"),
    ("America/Los_Angeles", "date", "2024-03-10 02:30:00", "19792"),
    ("America/Los_Angeles", "date", "2024-11-03 01:30:00", "20030"),
    ("America/Los_Angeles", "date", "007", "N"),
    ("America/Los_Angeles", "string", "2024-01-15", "="),
    ("America/Los_Angeles", "string", "2024-01-15 10:30:00", "="),
    ("America/Los_Angeles", "string", "2024-01-15T10:30:00Z", "="),
    ("America/Los_Angeles", "string", "2024-01-15 10:30:00+02:00", "="),
    ("America/Los_Angeles", "string", "2024-03-10 02:30:00", "="),
    ("America/Los_Angeles", "string", "2024-11-03 01:30:00", "="),
    ("America/Los_Angeles", "string", "007", "="),
    ("America/Los_Angeles", "timestamp", "2024-01-15", "1705305600000000"),
    ("America/Los_Angeles", "timestamp", "2024-01-15 10:30:00", "1705343400000000"),
    ("America/Los_Angeles", "timestamp", "2024-01-15T10:30:00Z", "1705314600000000"),
    ("America/Los_Angeles", "timestamp", "2024-01-15 10:30:00+02:00", "1705307400000000"),
    ("America/Los_Angeles", "timestamp", "2024-03-10 02:30:00", "1710066600000000"),
    ("America/Los_Angeles", "timestamp", "2024-11-03 01:30:00", "1730622600000000"),
    ("America/Los_Angeles", "timestamp", "007", "N"),
    ("America/Los_Angeles", "timestamp_ntz", "2024-01-15", "1705276800000000"),
    ("America/Los_Angeles", "timestamp_ntz", "2024-01-15 10:30:00", "1705314600000000"),
    ("America/Los_Angeles", "timestamp_ntz", "2024-01-15T10:30:00Z", "N"),
    ("America/Los_Angeles", "timestamp_ntz", "2024-01-15 10:30:00+02:00", "N"),
    ("America/Los_Angeles", "timestamp_ntz", "2024-03-10 02:30:00", "1710037800000000"),
    ("America/Los_Angeles", "timestamp_ntz", "2024-11-03 01:30:00", "1730597400000000"),
    ("America/Los_Angeles", "timestamp_ntz", "007", "N"),
    ("America/New_York", "date", "2024-01-15", "19737"),
    ("America/New_York", "date", "2024-01-15 10:30:00", "19737"),
    ("America/New_York", "date", "2024-01-15T10:30:00Z", "19737"),
    ("America/New_York", "date", "2024-01-15 10:30:00+02:00", "19737"),
    ("America/New_York", "date", "2024-03-10 02:30:00", "19792"),
    ("America/New_York", "date", "2024-11-03 01:30:00", "20030"),
    ("America/New_York", "date", "007", "N"),
    ("America/New_York", "string", "2024-01-15", "="),
    ("America/New_York", "string", "2024-01-15 10:30:00", "="),
    ("America/New_York", "string", "2024-01-15T10:30:00Z", "="),
    ("America/New_York", "string", "2024-01-15 10:30:00+02:00", "="),
    ("America/New_York", "string", "2024-03-10 02:30:00", "="),
    ("America/New_York", "string", "2024-11-03 01:30:00", "="),
    ("America/New_York", "string", "007", "="),
    ("America/New_York", "timestamp", "2024-01-15", "1705294800000000"),
    ("America/New_York", "timestamp", "2024-01-15 10:30:00", "1705332600000000"),
    ("America/New_York", "timestamp", "2024-01-15T10:30:00Z", "1705314600000000"),
    ("America/New_York", "timestamp", "2024-01-15 10:30:00+02:00", "1705307400000000"),
    ("America/New_York", "timestamp", "2024-03-10 02:30:00", "1710055800000000"),
    ("America/New_York", "timestamp", "2024-11-03 01:30:00", "1730611800000000"),
    ("America/New_York", "timestamp", "007", "N"),
    ("America/New_York", "timestamp_ntz", "2024-01-15", "1705276800000000"),
    ("America/New_York", "timestamp_ntz", "2024-01-15 10:30:00", "1705314600000000"),
    ("America/New_York", "timestamp_ntz", "2024-01-15T10:30:00Z", "N"),
    ("America/New_York", "timestamp_ntz", "2024-01-15 10:30:00+02:00", "N"),
    ("America/New_York", "timestamp_ntz", "2024-03-10 02:30:00", "1710037800000000"),
    ("America/New_York", "timestamp_ntz", "2024-11-03 01:30:00", "1730597400000000"),
    ("America/New_York", "timestamp_ntz", "007", "N"),
    ("Asia/Tokyo", "date", "2024-01-15", "19737"),
    ("Asia/Tokyo", "date", "2024-01-15 10:30:00", "19737"),
    ("Asia/Tokyo", "date", "2024-01-15T10:30:00Z", "19737"),
    ("Asia/Tokyo", "date", "2024-01-15 10:30:00+02:00", "19737"),
    ("Asia/Tokyo", "date", "2024-03-10 02:30:00", "19792"),
    ("Asia/Tokyo", "date", "2024-11-03 01:30:00", "20030"),
    ("Asia/Tokyo", "date", "007", "N"),
    ("Asia/Tokyo", "string", "2024-01-15", "="),
    ("Asia/Tokyo", "string", "2024-01-15 10:30:00", "="),
    ("Asia/Tokyo", "string", "2024-01-15T10:30:00Z", "="),
    ("Asia/Tokyo", "string", "2024-01-15 10:30:00+02:00", "="),
    ("Asia/Tokyo", "string", "2024-03-10 02:30:00", "="),
    ("Asia/Tokyo", "string", "2024-11-03 01:30:00", "="),
    ("Asia/Tokyo", "string", "007", "="),
    ("Asia/Tokyo", "timestamp", "2024-01-15", "1705244400000000"),
    ("Asia/Tokyo", "timestamp", "2024-01-15 10:30:00", "1705282200000000"),
    ("Asia/Tokyo", "timestamp", "2024-01-15T10:30:00Z", "1705314600000000"),
    ("Asia/Tokyo", "timestamp", "2024-01-15 10:30:00+02:00", "1705307400000000"),
    ("Asia/Tokyo", "timestamp", "2024-03-10 02:30:00", "1710005400000000"),
    ("Asia/Tokyo", "timestamp", "2024-11-03 01:30:00", "1730565000000000"),
    ("Asia/Tokyo", "timestamp", "007", "N"),
    ("Asia/Tokyo", "timestamp_ntz", "2024-01-15", "1705276800000000"),
    ("Asia/Tokyo", "timestamp_ntz", "2024-01-15 10:30:00", "1705314600000000"),
    ("Asia/Tokyo", "timestamp_ntz", "2024-01-15T10:30:00Z", "N"),
    ("Asia/Tokyo", "timestamp_ntz", "2024-01-15 10:30:00+02:00", "N"),
    ("Asia/Tokyo", "timestamp_ntz", "2024-03-10 02:30:00", "1710037800000000"),
    ("Asia/Tokyo", "timestamp_ntz", "2024-11-03 01:30:00", "1730597400000000"),
    ("Asia/Tokyo", "timestamp_ntz", "007", "N"),
]


def _field(spec: str) -> Column:
    return Column(functions.col("x")._inner.csv_field(spec)).alias("v")


def _canon(spec: str, value: Any, text: str) -> bool | str:
    if value is None:
        return "N"
    if spec in ("float", "double"):
        return value
    if spec.startswith("decimal"):
        return str(int(value.replace(".", "")))
    if spec == "boolean":
        return "true" if value else "false"
    if spec == "string":
        return "=" if value == text else value
    if spec == "binary":
        return "=" if value == text.encode() else repr(value)
    return str(value)


def _column(arrow: pa.ChunkedArray, spec: str) -> list[Any]:
    if spec == "date":
        arrow = arrow.cast(pa.int32())
    elif spec in ("timestamp", "timestamp_ntz"):
        arrow = arrow.cast(pa.int64())
    elif spec.startswith("decimal"):
        arrow = arrow.cast(pa.string())
    return arrow.to_pylist()


def _float_equal(found: float, wanted: str) -> bool:
    expected = float(wanted)
    if math.isnan(expected):
        return math.isnan(found)
    return struct.pack(">d", found) == struct.pack(">d", expected)


def _matches(spec: str, found: Any, wanted: str, text: str) -> bool:
    if spec in ("float", "double") and found != "N":
        return wanted != "N" and _float_equal(found, wanted)
    return found == wanted


def _run(
    spark: ReparkSession, rows: list[tuple[str, str]], specs: tuple[str, ...]
) -> tuple[int, int, list[str]]:
    texts = [text for text, _ in rows]
    frame = spark.createDataFrame([(text,) for text in texts], "x STRING")
    passed = 0
    declared = 0
    misses = []
    for index, spec in enumerate(specs):
        found = _column(frame.select(_field(spec)).toArrow().column(0), spec)
        for row, (text, cells) in enumerate(rows):
            wanted = cells.split(" ")[index]
            got = _canon(spec, found[row], text)
            if (text, spec) in DECLARED:
                assert wanted != "N"
                assert got == "N", (text, spec)
                declared += 1
            elif _matches(spec, got, wanted, text):
                passed += 1
            else:
                misses.append(f"{text!r} {spec}: Spark {wanted}, kernel {got}")
    return passed, declared, misses


def test_every_oracle_cell_answers_spark_through_the_builder(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    passed, declared, misses = _run(spark, ORACLE, ORACLE_SPECS)
    assert misses == []
    assert (passed, declared) == (962, 0)


def test_every_added_cell_answers_spark_or_is_declared(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    passed, declared, misses = _run(spark, ADDED, ADDED_SPECS)
    assert misses == []
    assert (passed, declared) == (978, 14)


@pytest.mark.parametrize("zone", ["America/Los_Angeles", "America/New_York", "Asia/Tokyo"])
def test_zone_cells_answer_spark_and_ntz_keeps_the_wall_clock(
    spark: ReparkSession, zone: str
) -> None:
    spark.conf.set("spark.sql.session.timeZone", zone)
    cells = [cell for cell in ZONE_CELLS if cell[0] == zone]
    texts = sorted({text for _, _, text, _ in cells})
    frame = spark.createDataFrame([(text,) for text in texts], "x STRING")
    for spec in ("string", "date", "timestamp", "timestamp_ntz"):
        found = _column(frame.select(_field(spec)).toArrow().column(0), spec)
        by_text = dict(zip(texts, found, strict=True))
        for _, kind, text, wanted in cells:
            if kind == spec:
                assert _canon(spec, by_text[text], text) == wanted, (zone, spec, text)
    assert len(cells) == 28


@pytest.mark.parametrize(
    ("spec", "name"),
    [
        ("interval year to month", "INTERVAL YEAR TO MONTH"),
        ("interval day to second", "INTERVAL DAY TO SECOND"),
        ("interval", "INTERVAL"),
        ("variant", "VARIANT"),
    ],
)
def test_intervals_and_variant_refuse_by_name_when_planned(
    spark: ReparkSession, spec: str, name: str
) -> None:
    frame = spark.createDataFrame([("1-2",)], "x STRING")
    with pytest.raises(
        AnalysisException, match=rf"\[UNSUPPORTED_DATATYPE\] a CSV field of type {name} "
    ):
        frame.select(_field(spec))

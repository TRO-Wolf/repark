from __future__ import annotations

import datetime
from decimal import Decimal

import pyarrow as pa

TYPES = [
    {
        "name": "BOOLEAN",
        "sql": "BOOLEAN",
        "family": "BOOLEAN",
        "literal": "true",
        "rows": [True, False, True, False, None],
    },
    {
        "name": "TINYINT",
        "sql": "TINYINT",
        "family": "INTEGRAL",
        "literal": "CAST(2 AS TINYINT)",
        "rows": [0, 1, 2, 127, None],
    },
    {
        "name": "SMALLINT",
        "sql": "SMALLINT",
        "family": "INTEGRAL",
        "literal": "CAST(2 AS SMALLINT)",
        "rows": [0, 1, 2, 300, None],
    },
    {
        "name": "INT",
        "sql": "INT",
        "family": "INTEGRAL",
        "literal": "2",
        "rows": [0, 1, 2, 100000, None],
    },
    {
        "name": "BIGINT",
        "sql": "BIGINT",
        "family": "INTEGRAL",
        "literal": "CAST(2 AS BIGINT)",
        "rows": [0, 1, 2, 9007199254740993, None],
    },
    {
        "name": "FLOAT",
        "sql": "FLOAT",
        "family": "FLOAT",
        "literal": "CAST(1.5 AS FLOAT)",
        "rows": [0.1, 1.0, 1.5, 10000000000.0, None],
    },
    {
        "name": "DOUBLE",
        "sql": "DOUBLE",
        "family": "FLOAT",
        "literal": "CAST(1.25 AS DOUBLE)",
        "rows": [0.1, 1.0, 1.25, 9007199254740992.0, None],
    },
    {
        "name": "DEC10_2",
        "sql": "DECIMAL(10,2)",
        "family": "DECIMAL",
        "literal": "CAST(1.25 AS DECIMAL(10,2))",
        "rows": [Decimal("0.50"), Decimal("1.00"), Decimal("1.25"), Decimal("12345678.99"), None],
    },
    {
        "name": "DEC20_0",
        "sql": "DECIMAL(20,0)",
        "family": "DECIMAL",
        "literal": "CAST(2 AS DECIMAL(20,0))",
        "rows": [Decimal("0"), Decimal("1"), Decimal("2"), Decimal("9007199254740993"), None],
    },
    {
        "name": "DEC38_0",
        "sql": "DECIMAL(38,0)",
        "family": "DECIMAL",
        "literal": "CAST(2 AS DECIMAL(38,0))",
        "rows": [
            Decimal("0"),
            Decimal("1"),
            Decimal("2"),
            Decimal("10000000000000000000000000000000000001"),
            None,
        ],
    },
    {
        "name": "DEC38_18",
        "sql": "DECIMAL(38,18)",
        "family": "DECIMAL",
        "literal": "CAST(1.25 AS DECIMAL(38,18))",
        "rows": [
            Decimal("0.500000000000000000"),
            Decimal("1.000000000000000000"),
            Decimal("1.250000000000000000"),
            Decimal("1.000000000000000001"),
            None,
        ],
    },
    {
        "name": "STR_NUM",
        "sql": "STRING",
        "family": "STRING",
        "literal": "'1.25'",
        "rows": ["1", "1.25", " 2 ", "1e2", None],
    },
    {
        "name": "STR_DATE",
        "sql": "STRING",
        "family": "STRING",
        "literal": "'2024-01-02'",
        "rows": ["2024-01-01", "2024-01-01 12:00:00", "2024-1-2", "2024-01-02T03:04:05", None],
    },
    {
        "name": "STR_OTHER",
        "sql": "STRING",
        "family": "STRING",
        "literal": "'abc'",
        "rows": ["abc", "true", "", "NaN", None],
    },
    {
        "name": "DATE",
        "sql": "DATE",
        "family": "DATETIME",
        "literal": "DATE'2024-01-02'",
        "rows": [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 2),
            datetime.date(1970, 1, 1),
            datetime.date(2262, 4, 12),
            None,
        ],
    },
    {
        "name": "TIMESTAMP",
        "sql": "TIMESTAMP",
        "family": "DATETIME",
        "literal": "TIMESTAMP'2024-01-01 12:00:00'",
        "rows": [
            datetime.datetime(2024, 1, 1, 0, 0),
            datetime.datetime(2024, 1, 1, 12, 0),
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
            None,
        ],
    },
    {
        "name": "TIMESTAMP_NTZ",
        "sql": "TIMESTAMP_NTZ",
        "family": "DATETIME",
        "literal": "TIMESTAMP_NTZ'2024-01-01 12:00:00'",
        "rows": [
            datetime.datetime(2024, 1, 1, 0, 0),
            datetime.datetime(2024, 1, 1, 12, 0),
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
            None,
        ],
    },
    {
        "name": "BINARY",
        "sql": "BINARY",
        "family": "BINARY",
        "literal": "X'616263'",
        "rows": [b"1", b"abc", b"", b"\x00\x01", None],
    },
]


def input_table() -> pa.Table:
    """Build the five-row table of the eighteen typed columns with explicit Arrow types."""
    columns = {"rid": pa.array(range(5), type=pa.int32())}
    kinds = {
        "BOOLEAN": pa.bool_(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "BIGINT": pa.int64(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "DEC10_2": pa.decimal128(10, 2),
        "DEC20_0": pa.decimal128(20, 0),
        "DEC38_0": pa.decimal128(38, 0),
        "DEC38_18": pa.decimal128(38, 18),
        "STR_NUM": pa.string(),
        "STR_DATE": pa.string(),
        "STR_OTHER": pa.string(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "TIMESTAMP_NTZ": pa.timestamp("us"),
        "BINARY": pa.binary(),
    }
    for spec in TYPES:
        columns[spec["name"].lower()] = pa.array(spec["rows"], type=kinds[spec["name"]])
    return pa.table(columns)

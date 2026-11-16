from __future__ import annotations

import struct
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

TI_ROWS = [
    (1, "0.00", "0"),
    (2, "0.50", "0.5"),
    (3, "1.25", "1.25"),
    (4, "4.50", "0.500000000000000001"),
    (5, "-0.50", "-0.5"),
    (6, None, None),
    (7, "99999999.99", "99999999999999999999.999999999999999999"),
    (8, "0.10", "0.1"),
    (9, "0.01", "0.000000000000000001"),
    (10, "2.75", "0.100000000000000006"),
]
POOL = [
    "0.5",
    "1.25",
    "-0.0",
    "4.5",
    "0.505",
    "NaN",
    "-0.5",
    "0.1",
    None,
    "1e-18",
    "Infinity",
    "2.75",
    "1e30",
    "99999999.99",
    "0.0",
    "-Infinity",
    "0.01",
    "1e20",
]
PLAIN = [item for item in POOL if item not in (None, "NaN", "Infinity", "-Infinity", "-0.0", "0.0")]
SPECIALS = {
    "mixed_dec_int": [("d", "0.5"), ("lit", "1.25"), ("lit", "2"), ("d", "4.5"), ("lit", "0.1")],
    "mixed_dec_int_small": [("d", "0.5"), ("lit", "1.25"), ("lit", "2")],
    "with_column": [("col", "v"), ("d", "0.5"), ("d", "1.25"), ("d", "4.5")],
    "null_only": [("d", None)] * 4,
    "nan_inf_only": [("d", "NaN"), ("d", "Infinity"), ("d", "-Infinity"), ("d", "NaN")],
    "zeros4": [("d", "-0.0"), ("d", "0.0"), ("d", "7.5"), ("d", "8.5")],
    "negzero4": [("d", "-0.0"), ("d", "6.5"), ("d", "7.5"), ("d", "8.5")],
    "negzero12": [("d", "-0.0")] + [("d", f"{index}.5") for index in range(11)],
    "dup4": [("d", "0.5")] * 4,
    "float4": [("f", "0.5"), ("f", "1.25"), ("f", "0.1"), ("f", "4.5")],
    "exp_lit4": [("lit", "5e-1"), ("lit", "1.25e0"), ("lit", "45e-1"), ("lit", "1e-1")],
}


def item_sql(item: tuple[str, str | None]) -> str:
    kind, text = item
    if kind == "col" or kind == "lit":
        return str(text)
    if text is None:
        return "CAST(NULL AS DOUBLE)"
    return f"CAST('{text}' AS {'FLOAT' if kind == 'f' else 'DOUBLE'})"


def item_value(item: tuple[str, str | None], row_id: int) -> float | None:
    kind, text = item
    if kind == "col":
        return row_id / 4
    if text is None:
        return None
    if kind == "f":
        return float(struct.unpack("f", struct.pack("f", float(text)))[0])
    return float(text)


def oracle(column: str, items: list[tuple[str, str | None]], negated: bool) -> list[list[str]]:
    rows = []
    for row_id, d10, d38 in TI_ROWS:
        text = d10 if column == "d10" else d38
        if text is None:
            rows.append([str(row_id), "None"])
            continue
        double = float(Decimal(text))
        values = [item_value(item, row_id) for item in items]
        member = any(value is not None and value == double for value in values)
        if member:
            answer: bool | None = not negated
        elif any(value is None for value in values):
            answer = None
        else:
            answer = negated
        rows.append([str(row_id), str(answer)])
    return rows


def cell(text: str | None, kind: str) -> str:
    return f"CAST(NULL AS {kind})" if text is None else f"CAST('{text}' AS {kind})"


def in_cases() -> dict[str, tuple[str, list[tuple[str, str | None]]]]:
    cases = {}
    for column in ("d10", "d38"):
        for count in (1, 2, 3, 4, 5, 6, 8, 12, 20):
            for start in (0, 3, 7, 11):
                items = [("d", POOL[(start + index) % len(POOL)]) for index in range(count)]
                cases[f"{column}/n{count}/s{start}"] = (column, items)
            plain = [("d", PLAIN[index % len(PLAIN)]) for index in range(count)]
            cases[f"{column}/n{count}/plain"] = (column, plain)
        for name, items in SPECIALS.items():
            cases[f"{column}/{name}"] = (column, items)
    return cases


CASES = in_cases()


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-decimal-double-in-ranges-2")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    session.sql(
        "SELECT * FROM VALUES "
        + ", ".join(
            f"({row_id}, {cell(d10, 'DECIMAL(10,2)')}, {cell(d38, 'DECIMAL(38,18)')}, "
            f"CAST({row_id} AS DOUBLE) / 4)"
            for row_id, d10, d38 in TI_ROWS
        )
        + " AS x(id, d10, d38, v)"
    ).createOrReplaceTempView("ti")
    yield session
    session.stop()


@pytest.mark.parametrize("case", sorted(CASES))
@pytest.mark.parametrize("negation", ["", "NOT "])
def test_an_in_list_of_doubles_against_a_decimal_answers_as_spark(
    spark: Any, case: str, negation: str
) -> None:
    column, items = CASES[case]
    sql = (
        f"SELECT id, {column} {negation}IN ({', '.join(item_sql(item) for item in items)}) AS p "
        "FROM ti ORDER BY id"
    )
    got = [[str(value) for value in row] for row in spark.sql(sql).collect()]
    assert got == oracle(column, items, bool(negation))


def iceberg_table(spark: Any, warehouse: Path) -> None:
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    spark.sql(
        "CREATE TABLE ice.ns.ti (id INT, d10 DECIMAL(10,2), d38 DECIMAL(38,18)) USING iceberg"
    ).collect()
    spark.sql("INSERT INTO ice.ns.ti SELECT id, d10, d38 FROM ti").collect()


def constants(count: int) -> str:
    return ", ".join(f"CAST({0.5 + index * 0.37!r} AS DOUBLE)" for index in range(count))


def explained(spark: Any, sql: str) -> str:
    rows = spark.sql(f"EXPLAIN {sql}").collect()
    return "\n".join(str(value) for row in rows for value in row)


@pytest.mark.parametrize(
    ("column", "count", "per_row"),
    [
        ("d10", 4, False),
        ("d10", 20, False),
        ("d10", 1000, False),
        ("d38", 4, False),
        ("d38", 20, False),
        ("d38", 1000, True),
    ],
)
def test_the_in_rewrite_reaches_the_filter_and_the_iceberg_scan(
    spark: Any, tmp_path: Path, column: str, count: int, per_row: bool
) -> None:
    iceberg_table(spark, tmp_path / "wh")
    for source in ("ti", "ice.ns.ti"):
        for negation in ("", "NOT "):
            text = explained(
                spark, f"SELECT id FROM {source} WHERE {column} {negation}IN ({constants(count)})"
            )
            assert ("__repark_decimal_to_double__" in text) is per_row, text
            if column == "d38" and not per_row:
                assert ">=" in text, text


def test_the_in_rewrite_keeps_a_column_item_per_row(spark: Any) -> None:
    text = explained(spark, "SELECT id FROM ti WHERE d10 IN (v, 0.5D, 1.25D, 4.5D)")
    assert "__repark_decimal_to_double__" in text, text


@pytest.mark.parametrize("count", [4, 20, 33])
def test_an_in_list_over_an_iceberg_table_answers_as_spark(
    spark: Any, tmp_path: Path, count: int
) -> None:
    iceberg_table(spark, tmp_path / "wh")
    items = [("d", repr(0.5 + index * 0.37)) for index in range(count)] + [("d", "1.25")]
    for column in ("d10", "d38"):
        sql = (
            f"SELECT id, {column} IN ({', '.join(item_sql(item) for item in items)}) AS p "
            "FROM ice.ns.ti ORDER BY id"
        )
        got = [[str(value) for value in row] for row in spark.sql(sql).collect()]
        assert got == oracle(column, items, False), sql

"""FN-B — string facade wrappers (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow path
(``to_arrow()``): value AND type. Alias names resolve and share a behavior case.
``replace`` pins the literal-vs-regexp hazard.

Deferred this batch (no stubs): ``regexp_extract_all``, ``regexp_substr`` (charter);
``to_char``, ``to_varchar`` (DESIGN-GATED). FN-GT1 later shipped ``split_part`` /
``regexp_count`` / ``regexp_instr`` / ``bit_length`` / ``octet_length``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-b").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def test_lcase_ucase_aliases(spark: ReparkSession) -> None:
    assert callable(F.lcase)
    assert callable(F.ucase)
    frame = spark.createDataFrame([("AbC",)], ["s"])
    table = _table(
        frame.select(
            F.lcase("s").alias("l"),
            F.lower("s").alias("lo"),
            F.ucase("s").alias("u"),
            F.upper("s").alias("up"),
        )
    )
    assert table.column("l").to_pylist() == table.column("lo").to_pylist() == ["abc"]
    assert table.column("u").to_pylist() == table.column("up").to_pylist() == ["ABC"]
    assert pa.types.is_string(table.schema.field("l").type) or pa.types.is_large_string(
        table.schema.field("l").type
    )


def test_char_alias_of_chr(spark: ReparkSession) -> None:
    assert callable(F.char)
    table = _table(spark.range(1).select(F.char(65).alias("c"), F.chr(65).alias("h")))
    assert table.column("c").to_pylist() == table.column("h").to_pylist() == ["A"]


def test_char_length_and_character_length(spark: ReparkSession) -> None:
    assert callable(F.char_length)
    assert callable(F.character_length)
    frame = spark.createDataFrame([("AbC",)], ["s"])
    table = _table(
        frame.select(
            F.char_length("s").alias("cl"),
            F.character_length("s").alias("ch"),
            F.length("s").alias("ln"),
        )
    )
    assert table.column("cl").to_pylist() == table.column("ch").to_pylist() == [3]
    assert table.column("ln").to_pylist() == [3]
    assert pa.types.is_integer(table.schema.field("cl").type)


def test_substring_and_substr(spark: ReparkSession) -> None:
    assert callable(F.substr)
    frame = spark.createDataFrame([("Spark",)], ["s"])
    table = _table(
        frame.select(
            F.substring("s", 2, 3).alias("a"),
            F.substr("s", 2, 3).alias("b"),
        )
    )
    assert table.column("a").to_pylist() == ["par"]
    assert table.column("b").to_pylist() == ["par"]
    assert pa.types.is_string(table.schema.field("a").type) or pa.types.is_large_string(
        table.schema.field("a").type
    )


def test_left_and_right(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Spark SQL",), ("ab",)], ["s"])
    table = _table(frame.select(F.left("s", 5).alias("l"), F.right("s", 3).alias("r")))
    assert table.column("l").to_pylist() == ["Spark", "ab"]
    assert table.column("r").to_pylist() == ["SQL", "ab"]


def test_contains_startswith_endswith(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("Spark SQL",), ("hello",)], ["s"])
    table = _table(
        frame.select(
            F.contains("s", F.lit("SQL")).alias("c"),
            F.startswith("s", F.lit("Spa")).alias("st"),
            F.endswith("s", F.lit("SQL")).alias("en"),
        )
    )
    assert table.column("c").to_pylist() == [True, False]
    assert table.column("st").to_pylist() == [True, False]
    assert table.column("en").to_pylist() == [True, False]
    assert pa.types.is_boolean(table.schema.field("c").type)


def test_like_ilike_regexp_family(spark: ReparkSession) -> None:
    assert callable(F.rlike)
    assert callable(F.regexp)
    frame = spark.createDataFrame([("Spark",), ("spark",), ("SQL",)], ["s"])
    table = _table(
        frame.select(
            F.like("s", F.lit("Spar%")).alias("lk"),
            F.ilike("s", F.lit("spar%")).alias("il"),
            F.regexp_like("s", F.lit("^S")).alias("rl"),
            F.rlike("s", F.lit("^S")).alias("rk"),
            F.regexp("s", F.lit("^S")).alias("rg"),
        )
    )
    assert table.column("lk").to_pylist() == [True, False, False]
    assert table.column("il").to_pylist() == [True, True, False]
    assert table.column("rl").to_pylist() == [True, False, True]
    assert table.column("rk").to_pylist() == table.column("rl").to_pylist()
    assert table.column("rg").to_pylist() == table.column("rl").to_pylist()
    assert pa.types.is_boolean(table.schema.field("lk").type)


def test_btrim(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("  hi  ",), ("xxhixx",)], ["s"])
    table = _table(frame.select(F.btrim("s").alias("a"), F.btrim("s", F.lit("x")).alias("b")))
    assert table.column("a").to_pylist()[0] == "hi"
    assert table.column("b").to_pylist()[1] == "hi"


def test_fn_b_str_is_column_name(spark: ReparkSession) -> None:
    """Sweep FIX: contains/like/ilike/regexp_like/btrim/starts/ends are ColumnOrName."""
    frame = spark.createDataFrame(
        [("xxhelloxx", "ell", "xx", "xx", "x", "%ell%", "%ELL%")],
        ["s", "needle", "pre", "suf", "trimc", "pat", "ipat"],
    )
    table = _table(
        frame.select(
            F.contains("s", "needle").alias("c"),
            F.contains("s", F.lit("needle")).alias("c_lit"),
            F.like("s", "pat").alias("lk"),
            F.ilike("s", "ipat").alias("il"),
            F.ilike("s", F.lit("ipat")).alias("il_lit"),
            F.startswith("s", "pre").alias("st"),
            F.endswith("s", "suf").alias("en"),
            F.btrim("s", "trimc").alias("bt"),
            F.btrim("s", F.lit("trimc")).alias("bt_lit"),
            F.regexp_like("s", "needle").alias("rl"),
        )
    )
    assert table.column("c").to_pylist() == [True]
    assert table.column("c_lit").to_pylist() == [False]
    assert table.column("lk").to_pylist() == [True]
    assert table.column("il").to_pylist() == [True]
    assert table.column("il_lit").to_pylist() == [False]
    assert table.column("st").to_pylist() == [True]
    assert table.column("en").to_pylist() == [True]
    assert table.column("bt").to_pylist() == ["hello"]
    assert table.column("bt_lit").to_pylist() == ["xxhelloxx"]
    assert table.column("rl").to_pylist() == [True]


def test_replace_is_literal_not_regexp(spark: ReparkSession) -> None:
    """Hazard: DF/Spark ``replace`` is literal; ``regexp_replace('.', …)`` matches every char."""
    frame = spark.createDataFrame([("a.b.c",)], ["s"])
    table = _table(
        frame.select(
            F.replace("s", ".", "-").alias("lit"),
            F.regexp_replace("s", ".", "-").alias("re"),
        )
    )
    assert table.column("lit").to_pylist() == ["a-b-c"]
    assert table.column("re").to_pylist() == ["-----"]
    assert pa.types.is_string(table.schema.field("lit").type) or pa.types.is_large_string(
        table.schema.field("lit").type
    )


def test_quote_sql_literal(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("O'Brien",), ("plain",)], ["s"])
    table = _table(frame.select(F.quote("s").alias("q")))
    assert table.column("q").to_pylist() == ["'O''Brien'", "'plain'"]
    assert pa.types.is_string(table.schema.field("q").type) or pa.types.is_large_string(
        table.schema.field("q").type
    )


def test_printf_aliases_format_string() -> None:
    """FNP-3 flipped ``format_string`` to shipped; ``printf`` delegates to it, so both are live."""
    assert callable(F.printf)
    assert F.printf("%s", "x") is not None
    assert F.format_string("%s", "x") is not None

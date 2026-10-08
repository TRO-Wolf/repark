"""Offline pure-logic pins for spark.read.jdbc / format('postgres') option surface (PG2).

No network, no REPARK_PG_DSN, no password guessing. These exercise the shipped facade
entry points for mutual exclusion, caps, and format alias teaching errors.

IO-JDBC-1 (R-3, 2026-09-14): PostgreSQL URLs (``jdbc:postgresql://`` and
``postgresql://``) read through the native connector via ``spark.read.jdbc`` with
main's exact teaching errors and ``read_postgres`` delegation, plus Spark's camelCase
keywords; other drivers and every ``write.jdbc`` are the declared
``NOT_IMPLEMENTED`` refusal until the 1.6 native connectors.
"""

from __future__ import annotations

import pytest

from repark import SparkSession
from repark.errors import (
    IllegalArgumentException,
    NumberFormatException,
    PySparkNotImplementedError,
)


@pytest.fixture
def spark() -> SparkSession:
    """Fresh session per test -- avoids stopped-session bleed after engine errors."""
    return SparkSession.builder.master("local[1]").appName("pg2-options").getOrCreate()


def test_format_postgres_requires_url(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="url"):
        spark.read.format("postgres").option("dbtable", "public.t").load()


def test_format_jdbc_is_postgres_alias_requires_dbtable_or_query(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="dbtable"):
        spark.read.format("jdbc").option("url", "postgresql://localhost/db").load()


def test_dbtable_query_mutually_exclusive(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="mutually exclusive"):
        (
            spark.read.format("postgres")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("query", "SELECT 1")
            .load()
        )


def test_partial_range_bag_fails_loud(spark: SparkSession) -> None:
    """An incomplete option set refuses in Spark's sentence, before any connection.

    pins: c-3/C-006
    """
    with pytest.raises(IllegalArgumentException, match="users need to specify all or none"):
        (
            spark.read.format("postgres")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("partitionColumn", "id")
            .option("numPartitions", "4")
            .load()
        )


def test_jdbc_predicates_xor_range(spark: SparkSession) -> None:
    """pins: io-declared-1/C-007"""
    with pytest.raises(IllegalArgumentException, match="cannot be combined"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            column="id",
            lower_bound=0,
            upper_bound=100,
            num_partitions=2,
            predicates=["id > 0"],
            properties={},
        )


def test_jdbc_empty_predicates_fails(spark: SparkSession) -> None:
    """pins: io-declared-1/C-007"""
    with pytest.raises(IllegalArgumentException, match="non-empty"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            predicates=[],
            properties={},
        )


def test_format_postgresql_alias_recognized(spark: SparkSession) -> None:
    # Alias accepted; fails on missing dbtable, not on unknown format.
    with pytest.raises(IllegalArgumentException, match="dbtable"):
        spark.read.format("postgresql").option("url", "postgresql://localhost/db").load()


def test_format_postgres_bad_partition_int_is_illegal_argument(spark: SparkSession) -> None:
    """Bare int() must not leak ValueError: typed IllegalArgumentException."""
    with pytest.raises(IllegalArgumentException, match="lowerBound"):
        (
            spark.read.format("postgres")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("partitionColumn", "id")
            .option("lowerBound", "not-an-int")
            .option("upperBound", "100")
            .option("numPartitions", "2")
            .load()
        )


def test_jdbc_dbtable_from_properties_is_forwarded(spark: SparkSession) -> None:
    """Shipped path: table=None + properties['dbtable'] must reach read_postgres (not None).

    pins: io-declared-1/C-007
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc(
            "postgresql://localhost/db",
            table=None,
            properties={"dbtable": "public.orders", "user": "u"},
        )
    assert captured.get("dbtable") == "public.orders"
    assert captured.get("url") == "postgresql://localhost/db"


def test_jdbc_dbtable_from_properties_is_forwarded_by_the_alternative(
    spark: SparkSession,
) -> None:
    """The format('postgres') spelling forwards the same dbtable contract.

    Kept as an additional pin beside the restored spark.read.jdbc capture (R-3).
    pins: io-declared-1/C-007
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.format("postgres").option("url", "postgresql://localhost/db").option(
            "dbtable", "public.orders"
        ).option("user", "u").load()
    assert captured.get("dbtable") == "public.orders"
    assert captured.get("url") == "postgresql://localhost/db"


def test_format_postgres_forwards_a_date_bound_as_text_for_the_door_to_judge(
    spark: SparkSession,
) -> None:
    """Spark's date spelling of a bound reaches the engine door, which names the registry row.

    pins: c-3/C-009
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        (
            reader.format("jdbc")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("partitionColumn", "day")
            .option("lowerBound", "2024-01-01")
            .option("upperBound", "2024-02-01 00:00:00")
            .option("numPartitions", "4")
            .load()
        )
    assert captured.get("partition_column") == "day"
    assert captured.get("num_partitions") == 4
    assert captured.get("lower_bound") is None
    assert captured.get("upper_bound") is None
    assert captured.get("properties") == {
        "lowerBound": "2024-01-01",
        "upperBound": "2024-02-01 00:00:00",
    }


def test_jdbc_camel_case_keywords_reach_read_postgres(spark: SparkSession) -> None:
    """Spark's camelCase keywords bind and reach read_postgres with main's argument names.

    pins: io-declared-1/C-007
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc(
            "jdbc:postgresql://localhost/db",
            "t",
            column="id",
            lowerBound=0,
            upperBound=100,
            numPartitions=2,
            properties={"user": "u"},
        )
    assert captured.get("dbtable") == "t"
    assert captured.get("url") == "jdbc:postgresql://localhost/db"
    assert captured.get("partition_column") == "id"
    assert captured.get("lower_bound") is None
    assert captured.get("upper_bound") is None
    assert captured.get("num_partitions") is None
    assert captured.get("properties") == {
        "user": "u",
        "lowerBound": "0",
        "upperBound": "100",
        "numPartitions": "2",
    }
    assert captured.get("predicates") is None


def test_jdbc_snake_case_aliases_reach_read_postgres(spark: SparkSession) -> None:
    """main's snake_case keyword spellings stay working as keyword-only aliases.

    pins: io-declared-1/C-007
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc(
            "postgresql://localhost/db",
            "t",
            column="id",
            lower_bound=0,
            upper_bound=100,
            num_partitions=2,
        )
    assert captured.get("partition_column") == "id"
    assert captured.get("properties") == {
        "lowerBound": "0",
        "upperBound": "100",
        "numPartitions": "2",
    }


def test_jdbc_both_keyword_spellings_raise_typeerror(spark: SparkSession) -> None:
    """Passing both spellings of one parameter raises TypeError.

    pins: io-declared-1/C-007
    """
    with pytest.raises(TypeError, match="lowerBound"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            column="id",
            lowerBound=0,
            lower_bound=1,
        )
    with pytest.raises(TypeError, match="upperBound"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            upperBound=0,
            upper_bound=1,
        )
    with pytest.raises(TypeError, match="numPartitions"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            numPartitions=2,
            num_partitions=3,
        )
    with pytest.raises(TypeError, match="properties"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            properties={"a": "b"},
            connection_properties={"c": "d"},
        )


def test_jdbc_non_postgres_urls_refuse_not_implemented(spark: SparkSession) -> None:
    """Non-PostgreSQL driver URLs refuse NOT_IMPLEMENTED at the call, pre-connection.

    pins: io-declared-1/C-003, C-007
    """
    for url in ("jdbc:mysql://localhost/db", "jdbc:sqlserver://localhost:1433/db"):
        with pytest.raises(PySparkNotImplementedError) as raised:
            spark.read.jdbc(url, "t")
        assert raised.value.getCondition() == "NOT_IMPLEMENTED"
        assert raised.value.getMessageParameters() == {"feature": "jdbc"}
        assert str(raised.value) == "[NOT_IMPLEMENTED] jdbc is not implemented."


def _captured_jdbc_read(spark: SparkSession, url: str) -> dict[str, object]:
    """Run one capture-probe of ``reader.jdbc(url, 't')`` against a fake session."""
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc(url, "t")
    return captured


def test_jdbc_postgres_alias_url_reaches_read_postgres(spark: SparkSession) -> None:
    """libpq's ``postgres://`` alias reaches read_postgres with the original url string.

    L-002: main forwarded every URL verbatim and libpq accepts ``postgres://`` as an
    alias of ``postgresql://``; the check is case-insensitive after stripping leading
    whitespace and forwards the caller's string untouched.

    pins: io-declared-1/C-008
    """
    for url in ("postgres://h/db", "POSTGRES://h/db", "  postgres://h/db"):
        captured = _captured_jdbc_read(spark, url)
        assert captured.get("url") == url
        assert captured.get("dbtable") == "t"


def test_jdbc_postgres_jdbc_scheme_still_refuses_not_implemented(spark: SparkSession) -> None:
    """``jdbc:postgres://`` names no documented connector scheme and keeps refusing.

    L-002: the connector's URL parser is not in this tree (the native entry defers),
    so the documented scheme set governs — ``jdbc:postgresql://`` / ``postgresql://`` /
    ``postgres://``.

    pins: io-declared-1/C-008
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.jdbc("jdbc:postgres://h/db", "t")
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "jdbc"}


def _keyword_door_capture(spark: SparkSession, **keywords: object) -> dict[str, object]:
    """Run ``reader.jdbc`` with the given keywords against a fake session; return what arrived."""
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc("postgresql://localhost/db", "t", properties={"user": "u"}, **keywords)
    return captured


def test_jdbc_keyword_door_str_and_int_bounds_reach_the_engine_door_as_the_same_text(
    spark: SparkSession,
) -> None:
    """A str or an int bound is handed on as PySpark hands it: text, never parsed here.

    pins: c-3/C-010
    """
    by_int = _keyword_door_capture(spark, column="d", lowerBound=0, upperBound=100, numPartitions=4)
    by_str = _keyword_door_capture(
        spark, column="d", lowerBound="0", upperBound="100", numPartitions="4"
    )
    assert by_int == by_str
    assert by_int["properties"] == {
        "user": "u",
        "lowerBound": "0",
        "upperBound": "100",
        "numPartitions": "4",
    }
    dates = _keyword_door_capture(
        spark,
        column="d",
        lowerBound="2024-01-01",
        upperBound="2024-02-01",
        numPartitions="4",
    )
    assert dates["properties"] == {
        "user": "u",
        "lowerBound": "2024-01-01",
        "upperBound": "2024-02-01",
        "numPartitions": "4",
    }
    assert dates.get("lower_bound") is None
    assert dates.get("upper_bound") is None
    assert dates.get("num_partitions") is None


def test_jdbc_keyword_door_keeps_a_properties_spelling_for_the_door_to_refuse(
    spark: SparkSession,
) -> None:
    """A keyword bound beside the same option in the properties never overwrites it.

    pins: c-3/C-010
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc(
            "postgresql://localhost/db",
            "t",
            column="id",
            lowerBound=0,
            upperBound=10,
            numPartitions=2,
            properties={"lowerBound": "5"},
        )
    assert captured["properties"] == {
        "lowerBound": "5",
        "lowerbound": "0",
        "upperBound": "10",
        "numPartitions": "2",
    }


def test_jdbc_keyword_door_incomplete_set_refuses_in_sparks_sentence(spark: SparkSession) -> None:
    """The engine door judges the incomplete set, as it does for the format door.

    pins: c-3/C-010
    """
    for keywords in (
        {"column": "id", "numPartitions": "4"},
        {"column": "id", "lowerBound": "0", "upperBound": "9"},
        {"lowerBound": 0, "upperBound": 9, "numPartitions": 4},
    ):
        with pytest.raises(IllegalArgumentException, match="users need to specify all or none"):
            spark.read.jdbc("postgresql://localhost/db", "t", **keywords)


def test_jdbc_keyword_door_num_partitions_text_refuses_as_the_format_door_does(
    spark: SparkSession,
) -> None:
    """A count past Spark's 32-bit Int refuses as NumberFormat, from the keyword door too.

    pins: c-3/C-010
    """
    for count in ("3000000000", 3_000_000_000):
        with pytest.raises(NumberFormatException, match="`numPartitions` must be a 32-bit"):
            spark.read.jdbc(
                "postgresql://localhost/db",
                "t",
                column="id",
                lowerBound="0",
                upperBound="9",
                numPartitions=count,
            )
        with pytest.raises(NumberFormatException, match="`numPartitions` must be a 32-bit"):
            (
                spark.read.format("jdbc")
                .option("url", "postgresql://localhost/db")
                .option("dbtable", "t")
                .option("partitionColumn", "id")
                .option("lowerBound", "0")
                .option("upperBound", "9")
                .option("numPartitions", str(count))
                .load()
            )

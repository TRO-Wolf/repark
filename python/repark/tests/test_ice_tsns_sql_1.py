"""ICE-TSNS-SQL-1: ``timestamp_ns`` / ``timestamptz_ns`` on the SQL door answer the Iceberg spec.

The oracle is the Iceberg v3 spec plus a PyIceberg 0.12.0 read-back recorded in
``ice_tsns_sql_1_oracle.json`` by ``_record_ice_tsns_sql_1_oracle.py``; Spark 4.1.2 cannot read or
write these types. Ruling Q-21c-6.

pins: ice-tsns-sql-1/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

FIXTURE: dict[str, Any] = json.loads(
    (Path(__file__).with_name("ice_tsns_sql_1_oracle.json")).read_text()
)
LITERALS: dict[str, dict[str, Any]] = FIXTURE["literals"]
V3 = "TBLPROPERTIES ('format-version' = '3')"
NS_TABLE = f"CREATE TABLE ice.ns.t (id INT, ts timestamp_ns, tz timestamptz_ns) USING iceberg {V3}"
DAYS_TABLE = (
    f"CREATE TABLE ice.ns.d (id INT, ts timestamp_ns) USING iceberg PARTITIONED BY (days(ts)) {V3}"
)
NS_IDS = ("1", "2", "3", "6", "7", "8")
HOUR_FORK_GAP = "Unsupported data type for hour transform"


def text(identifier: str) -> str:
    """Return the literal text of fixture literal ``identifier``."""
    return str(LITERALS[identifier]["text"])


def wall(identifier: str) -> int:
    """Return the int64-nanosecond wall of fixture literal ``identifier``."""
    return int(LITERALS[identifier]["wall_ns"])


def open_session(warehouse: Path, zone: str = "UTC", ansi: str = "true") -> Any:
    """Open a v3-enabled session with an ``ice`` memory catalog and namespace ``ns``."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-sql-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[Any]:
    """A UTC, ANSI session over a fresh warehouse."""
    session = open_session(tmp_path)
    try:
        yield session
    finally:
        session.stop()


def scalar(spark: Any, sql: str) -> tuple[pa.DataType, Any]:
    """Return the Arrow type and the single value of a one-row, one-column query."""
    table = spark.sql(sql).to_arrow()
    column = table.column(0)
    return column.type, column.to_pylist()[0]


def ns_scalar(spark: Any, sql: str) -> tuple[pa.DataType, int | None]:
    """Return the Arrow type and the int64-nanosecond value of a one-row timestamp query."""
    table = spark.sql(sql).to_arrow()
    column = table.column(0)
    return column.type, column.cast(pa.int64()).to_pylist()[0]


def ns_rows(spark: Any, table: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    """Read ``id`` plus ``columns`` of ``table`` through RePark as int64 nanoseconds, by id."""
    arrow = spark.sql(f"SELECT id, {', '.join(columns)} FROM {table} ORDER BY id").to_arrow()
    ints = {name: arrow.column(name).cast(pa.int64()).to_pylist() for name in columns}
    return [
        {"id": identifier, **{name: ints[name][index] for name in columns}}
        for index, identifier in enumerate(arrow.column("id").to_pylist())
    ]


def run_all(spark: Any, statements: list[str]) -> None:
    """Run fixture statements eagerly, in order."""
    for sql in statements:
        spark.sql(sql).collect()


def iceberg_metadata(warehouse: Path, table: str) -> dict[str, Any]:
    """Return the Iceberg schema type names and default spec from the latest metadata JSON."""
    paths = [
        path for path in warehouse.rglob("*.metadata.json") if path.parent.parent.name == table
    ]
    latest = max(paths, key=lambda path: int(path.name.split("-", 1)[0]))
    metadata = json.loads(latest.read_text())
    schema_id = metadata["current-schema-id"]
    schema = next(s for s in metadata["schemas"] if s["schema-id"] == schema_id)
    names = {field["id"]: field["name"] for field in schema["fields"]}
    spec_id = metadata["default-spec-id"]
    spec = next(s for s in metadata["partition-specs"] if s["spec-id"] == spec_id)
    return {
        "format_version": metadata["format-version"],
        "schema": {field["name"]: field["type"] for field in schema["fields"]},
        "spec": [
            {
                "name": field["name"],
                "source": names[field["source-id"]],
                "transform": field["transform"],
            }
            for field in spec["fields"]
        ],
    }


def partitions(spark: Any, table: str) -> list[dict[str, Any]]:
    """Return RePark's ``.partitions`` answer with partition values rendered as text."""
    rows = spark.sql(f"SELECT partition, record_count FROM {table}.partitions ORDER BY partition")
    return [
        {
            "partition": {key: str(value) for key, value in row["partition"].items()},
            "record_count": row["record_count"],
        }
        for row in rows.to_arrow().to_pylist()
    ]


def write_control(spark: Any) -> None:
    """Write the DataFrame-door control table the fixture recorded (the path that works today)."""
    import pandas as pd

    ids = sorted(int(key) for key in LITERALS)
    texts = [text(str(i)) for i in ids]
    spark.sql(
        "CREATE TABLE ice.ns.ctl (id INT, ts timestamp_ns, tz timestamptz_ns) USING iceberg "
        f"PARTITIONED BY (days(ts)) {V3}"
    ).collect()
    frame = pd.DataFrame(
        {
            "id": pd.array(ids, dtype="int32"),
            "ts": pd.to_datetime(texts, format="ISO8601"),
            "tz": pd.to_datetime(texts, format="ISO8601").tz_localize("UTC"),
        }
    )
    spark.createDataFrame(frame).writeTo("ice.ns.ctl").append()


def test_fixture_control_agrees_with_the_literals() -> None:
    """The PyIceberg control read-back equals the spec-derived nanoseconds of every literal."""
    control = FIXTURE["control"]["ctl_days"]
    assert control["schema"] == {"id": "int", "ts": "timestamp_ns", "tz": "timestamptz_ns"}
    for row in control["rows"]:
        assert row["ts"] == row["tz"] == wall(str(row["id"]))
    assert FIXTURE["control_repark_partitions"]["ctl_days"] == control["partitions"]


@pytest.mark.parametrize("identifier", NS_IDS)
def test_cast_string_as_timestamp_ns_keeps_nine_digits(spark: Any, identifier: str) -> None:
    """Clause 1: ``CAST(<string> AS timestamp_ns)`` is Arrow ``Timestamp(ns)`` with every digit."""
    arrow_type, value = ns_scalar(spark, f"SELECT CAST('{text(identifier)}' AS timestamp_ns) AS v")
    assert arrow_type == pa.timestamp("ns")
    assert value == wall(identifier)


@pytest.mark.parametrize("identifier", NS_IDS)
def test_cast_string_as_timestamptz_ns_honours_the_offset(spark: Any, identifier: str) -> None:
    """Clause 1: an explicit offset is honoured exactly into ``Timestamp(ns, UTC)``."""
    arrow_type, value = ns_scalar(
        spark, f"SELECT CAST('{text(identifier)}+01:00' AS timestamptz_ns) AS v"
    )
    assert arrow_type == pa.timestamp("ns", tz="UTC")
    assert value == wall(identifier) - 3_600_000_000_000


def test_cast_timestamptz_ns_reads_a_zoneless_string_in_the_session_zone(tmp_path: Path) -> None:
    """Clause 1: a zoneless string is a wall in the session zone, as the TIMESTAMP cast reads it."""
    zone = FIXTURE["foreign_zone"]
    spark = open_session(tmp_path, zone=zone)
    try:
        for identifier in NS_IDS:
            _, value = ns_scalar(spark, f"SELECT CAST('{text(identifier)}' AS timestamptz_ns) AS v")
            assert value == LITERALS[identifier]["foreign_zone_instant_ns"], identifier
            _, micro = ns_scalar(spark, f"SELECT CAST('{text(identifier)}' AS TIMESTAMP) AS v")
            assert micro is not None
            assert value // 1000 == micro // 1000, identifier
    finally:
        spark.stop()


def test_cast_type_names_are_case_insensitive(spark: Any) -> None:
    """Clause 1: ``TIMESTAMP_NS`` / ``TimestampTz_Ns`` spell the same types."""
    upper_type, upper = ns_scalar(spark, f"SELECT CAST('{text('1')}' AS TIMESTAMP_NS) AS v")
    mixed_type, mixed = ns_scalar(spark, f"SELECT CAST('{text('1')}' AS TimestampTz_Ns) AS v")
    assert (upper_type, upper) == (pa.timestamp("ns"), wall("1"))
    assert (mixed_type, mixed) == (pa.timestamp("ns", tz="UTC"), wall("1"))


@pytest.mark.parametrize("target", ["timestamp_ns", "timestamptz_ns"])
def test_malformed_string_fails_like_the_timestamp_cast_under_ansi(spark: Any, target: str) -> None:
    """Clause 1: a malformed string raises the class the microsecond ``TIMESTAMP`` cast raises."""
    with pytest.raises(Exception, match=r"CAST_INVALID_INPUT") as micro:
        spark.sql("SELECT CAST('not a time' AS TIMESTAMP) AS v").collect()
    with pytest.raises(Exception, match=r"CAST_INVALID_INPUT") as nano:
        spark.sql(f"SELECT CAST('not a time' AS {target}) AS v").collect()
    assert type(nano.value) is type(micro.value)


@pytest.mark.parametrize("target", ["timestamp_ns", "timestamptz_ns"])
def test_malformed_string_is_null_like_the_timestamp_cast_without_ansi(
    tmp_path: Path, target: str
) -> None:
    """Clause 1: with ANSI off a malformed string is NULL, as the microsecond cast answers."""
    spark = open_session(tmp_path, ansi="false")
    try:
        assert scalar(spark, "SELECT CAST('not a time' AS TIMESTAMP) AS v")[1] is None
        assert scalar(spark, f"SELECT CAST('not a time' AS {target}) AS v")[1] is None
    finally:
        spark.stop()


def test_insert_values_writes_every_shape_exactly(spark: Any, tmp_path: Path) -> None:
    """Clauses 1-3: ns casts, a TIMESTAMP literal and a string land as the PyIceberg control."""
    expected = FIXTURE["sql_tables"]["sql_days"]
    run_all(spark, FIXTURE["sql_statements"]["sql_days"])
    assert ns_rows(spark, "ice.ns.sql_days", ("ts", "tz")) == expected["rows"]
    metadata = iceberg_metadata(tmp_path, "sql_days")
    assert metadata["format_version"] == 3
    assert metadata["schema"] == expected["schema"]
    assert metadata["spec"] == expected["spec"]


def test_insert_values_widens_a_timestamp_literal(spark: Any) -> None:
    """Clause 2: a ``TIMESTAMP`` literal widens into ns columns (was a raw Arrow error)."""
    spark.sql(NS_TABLE)
    literal = f"TIMESTAMP '{text('4')}'"
    spark.sql(f"INSERT INTO ice.ns.t VALUES (4, {literal}, {literal})").collect()
    assert ns_rows(spark, "ice.ns.t", ("ts", "tz")) == [{"id": 4, "ts": wall("4"), "tz": wall("4")}]


def test_insert_values_widens_a_string(spark: Any) -> None:
    """Clause 2: a string literal into ns columns keeps nine digits (was a raw Arrow error)."""
    spark.sql(NS_TABLE)
    spark.sql(f"INSERT INTO ice.ns.t VALUES (5, '{text('5')}', '{text('5')}')").collect()
    assert ns_rows(spark, "ice.ns.t", ("ts", "tz")) == [{"id": 5, "ts": wall("5"), "tz": wall("5")}]


def test_insert_select_ns_cast_into_a_days_table(spark: Any) -> None:
    """Clauses 1+3: ``INSERT … SELECT CAST(… AS timestamp_ns)`` splits at the true day boundary."""
    spark.sql(DAYS_TABLE)
    spark.sql(f"INSERT INTO ice.ns.d SELECT 2, CAST('{text('2')}' AS timestamp_ns)").collect()
    spark.sql(f"INSERT INTO ice.ns.d SELECT 3, CAST('{text('3')}' AS timestamp_ns)").collect()
    assert ns_rows(spark, "ice.ns.d", ("ts",)) == [
        {"id": 2, "ts": wall("2")},
        {"id": 3, "ts": wall("3")},
    ]
    assert partitions(spark, "ice.ns.d") == [
        {"partition": {"ts_day": "2026-01-02"}, "record_count": 1},
        {"partition": {"ts_day": "2026-01-03"}, "record_count": 1},
    ]


def test_insert_select_widens_a_microsecond_column(spark: Any) -> None:
    """Clause 2: ``INSERT … SELECT`` from ``TIMESTAMP`` columns widens exactly."""
    spark.sql("CREATE TABLE ice.ns.src (id INT, a TIMESTAMP, b TIMESTAMP) USING iceberg")
    first, second = f"TIMESTAMP '{text('4')}'", f"TIMESTAMP '{text('6')}'"
    spark.sql(f"INSERT INTO ice.ns.src VALUES (4, {first}, {second})").collect()
    spark.sql(NS_TABLE)
    spark.sql("INSERT INTO ice.ns.t SELECT id, a, b FROM ice.ns.src").collect()
    assert ns_rows(spark, "ice.ns.t", ("ts", "tz")) == [{"id": 4, "ts": wall("4"), "tz": wall("6")}]


def test_insert_overwrite_widens_and_keeps_ns(spark: Any) -> None:
    """Clause 2: ``INSERT OVERWRITE … VALUES`` with TIMESTAMP literals and ns casts."""
    run_all(spark, FIXTURE["sql_statements"]["sql_overwrite"])
    expected = FIXTURE["sql_tables"]["sql_overwrite"]["rows"]
    assert ns_rows(spark, "ice.ns.sql_overwrite", ("ts", "tz")) == expected


def test_merge_insert_and_update_keep_ns(spark: Any) -> None:
    """Clause 2: MERGE insert and update with ns casts over a row from a TIMESTAMP literal."""
    run_all(spark, FIXTURE["sql_statements"]["sql_merge"])
    expected = FIXTURE["sql_tables"]["sql_merge"]["rows"]
    assert ns_rows(spark, "ice.ns.sql_merge", ("ts", "tz")) == expected


def test_ctas_carries_ns_casts(spark: Any, tmp_path: Path) -> None:
    """Clause 2: CTAS over ns casts creates ns columns holding every digit."""
    run_all(spark, FIXTURE["sql_statements"]["sql_ctas"])
    expected = FIXTURE["sql_tables"]["sql_ctas"]
    assert ns_rows(spark, "ice.ns.sql_ctas", ("ts", "tz")) == expected["rows"]
    assert iceberg_metadata(tmp_path, "sql_ctas")["schema"] == expected["schema"]


def test_days_partitions_equal_the_pyiceberg_read_back(spark: Any) -> None:
    """Clause 3: ``.partitions`` of the SQL-door ``days(ts)`` table equals PyIceberg's answer."""
    run_all(spark, FIXTURE["sql_statements"]["sql_days"])
    assert partitions(spark, "ice.ns.sql_days") == FIXTURE["sql_tables"]["sql_days"]["partitions"]


def test_hours_partitions_equal_the_spec(spark: Any) -> None:
    """Clause 3: ``hours(tz)`` on ns partitions at the true boundary (fork gap F-TSNS-HOUR-1)."""
    expected = FIXTURE["sql_tables"]["sql_hours"]
    try:
        run_all(spark, FIXTURE["sql_statements"]["sql_hours"])
    except Exception as exc:
        if HOUR_FORK_GAP in str(exc):
            pytest.xfail(f"BLOCKED-ON-FORK F-TSNS-HOUR-1: {exc}")
        raise
    assert ns_rows(spark, "ice.ns.sql_hours", ("tz",)) == expected["rows"]
    assert partitions(spark, "ice.ns.sql_hours") == expected["partitions"]


def test_cast_ns_column_as_string_is_lossless(spark: Any) -> None:
    """Clause 4: ``CAST(<ns column> AS STRING)`` keeps up to nine digits, trailing zeros trimmed."""
    anchor = FIXTURE["micro_render_anchor"]
    for identifier in ("4", "6", "7"):
        assert anchor[f"l{identifier}"] == LITERALS[identifier]["render"]
    write_control(spark)
    rendered = (
        spark.sql(
            "SELECT id, CAST(ts AS STRING) AS s, CAST(tz AS STRING) AS z "
            "FROM ice.ns.ctl ORDER BY id"
        )
        .to_arrow()
        .to_pylist()
    )
    for row in rendered:
        expected = LITERALS[str(row["id"])]["render"]
        assert (row["s"], row["z"]) == (expected, expected), row


@pytest.mark.parametrize("identifier", NS_IDS)
def test_cast_ns_cast_as_string_round_trips(spark: Any, identifier: str) -> None:
    """Clauses 1+4: a string through ``timestamp_ns`` and back renders the trimmed literal."""
    _, value = scalar(
        spark, f"SELECT CAST(CAST('{text(identifier)}' AS timestamp_ns) AS STRING) AS v"
    )
    assert value == LITERALS[identifier]["render"]


@pytest.mark.parametrize("column", ["ts", "tz"])
def test_predicate_compares_at_nanosecond_precision(spark: Any, column: str) -> None:
    """Clause 5: equality matches the row and not its neighbour one nanosecond away."""
    write_control(spark)
    target = "timestamp_ns" if column == "ts" else "timestamptz_ns"
    for identifier in ("1", "8"):
        ids = (
            spark.sql(
                f"SELECT id FROM ice.ns.ctl "
                f"WHERE {column} = CAST('{text(identifier)}' AS {target}) "
                "ORDER BY id"
            )
            .to_arrow()
            .column("id")
            .to_pylist()
        )
        assert ids == [int(identifier)], (column, identifier, ids)
    later = (
        spark.sql(
            f"SELECT id FROM ice.ns.ctl WHERE {column} > CAST('{text('8')}' AS {target}) "
            f"AND {column} < CAST('{text('6')}' AS {target}) ORDER BY id"
        )
        .to_arrow()
        .column("id")
        .to_pylist()
    )
    assert later == [1]


def test_format_v2_keeps_refusing_ns_at_create(spark: Any) -> None:
    """Clause 6 fence: a format-v2 table still refuses ``timestamp_ns`` at CREATE."""
    with pytest.raises(Exception, match=r"timestamp_ns"):
        spark.sql("CREATE TABLE ice.ns.v2 (id INT, ts timestamp_ns) USING iceberg").collect()

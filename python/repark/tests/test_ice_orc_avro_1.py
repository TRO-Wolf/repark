"""IPI-41 — ORC and Avro data files: write, read, DELETE / UPDATE / MERGE.

Red-first pins for the S6 table (plan packet `ipi-41-orc-avro.md` S6). WO1 commits
them red: the RePark-owned builder sites gain format routing in WO2 and the
`write-format` refusals invert in WO3. Clauses C-001 through C-022 below name the
S6 rows in order; the unit ledger adopts these numbers when it lands.

pins: ice-orc-avro-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
pins: ice-orc-avro-1/C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016
pins: ice-orc-avro-1/C-017, C-018, C-019, C-020, C-021, C-022
pins: ice-orc-avro-1/C-023, C-025
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException, PySparkException

CATALOG = "ice_orc_avro_1"
NS = "ns"
_WIDE_COLUMNS = (
    "a BOOLEAN, b INT, c BIGINT, d FLOAT, e DOUBLE, f DECIMAL(10,2), g DATE, "
    "h TIMESTAMP, i STRING, j BINARY, k ARRAY<INT>, l MAP<STRING,INT>, "
    "m STRUCT<x:INT,y:STRING>"
)
_WIDE_VALUES = (
    "(true, 1, 2, 3.5, 4.5, 12.34, DATE'2024-01-01', "
    "TIMESTAMP'2024-01-01 10:00:00', 's', X'0102', array(1,2), map('k',1), "
    "named_struct('x',1,'y','z'))"
)
_PRIMITIVE_COLUMNS = (
    "a BOOLEAN, b INT, c BIGINT, d FLOAT, e DOUBLE, f DECIMAL(10,2), g DATE, "
    "h TIMESTAMP, i STRING, j BINARY"
)
_PRIMITIVE_VALUES = (
    "(true, 1, 2, 3.5, 4.5, 12.34, DATE'2024-01-01', TIMESTAMP'2024-01-01 10:00:00', 's', X'0102')"
)
_WIDE_ROW: dict[str, Any] = {
    "a": True,
    "b": 1,
    "c": 2,
    "d": 3.5,
    "e": 4.5,
    "f": Decimal("12.34"),
    "g": date(2024, 1, 1),
    "h": datetime(2024, 1, 1, 10, 0, tzinfo=UTC),
    "i": "s",
    "j": b"\x01\x02",
    "k": [1, 2],
    "l": [("k", 1)],
    "m": {"x": 1, "y": "z"},
}
_MOR_MODES = (
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with an in-memory Iceberg catalog, namespace, and v3 CREATE allowed."""
    session = (
        ReparkSession.builder.appName("pytest-ice-orc-avro-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NS}")
    return session


def _ordered_rows(spark: ReparkSession, query: str) -> list[list[Any]]:
    """Return `query` as row-value lists in the returned order."""
    return [list(row.values()) for row in spark.sql(query).to_arrow().to_pylist()]


def _snapshot_count(spark: ReparkSession, table: str) -> int:
    """Return the snapshot count of `table`."""
    rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").to_arrow().to_pylist()
    return int(rows[0]["n"])


def _files_summary(spark: ReparkSession, table: str) -> list[list[Any]]:
    """Return `(content, file_format, files, records)` grouped like the TP cells."""
    return sorted(
        _ordered_rows(
            spark,
            f"SELECT content, file_format, count(*) AS files, sum(record_count) AS records "
            f"FROM {table}.files GROUP BY content, file_format",
        )
    )


def _live_data_formats(spark: ReparkSession, table: str) -> list[str]:
    """Return the sorted `file_format` values of the live `content = 0` files."""
    rows = _ordered_rows(spark, f"SELECT file_format FROM {table}.files WHERE content = 0")
    return sorted(str(row[0]) for row in rows)


def _table_property(spark: ReparkSession, table: str, key: str) -> str | None:
    """Return table property `key` of `table`, or None when the table lacks it."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").to_arrow().to_pylist()
    for row in rows:
        if row["col_name"] != "Table Properties":
            continue
        for pair in str(row["data_type"]).strip("[]").split(","):
            name, _, value = pair.partition("=")
            if name == key:
                return value
    return None


def _snapshot_appends(spark: ReparkSession, table: str) -> list[tuple[Any, Any, Any]]:
    """Return `(operation, added-records, total-records)` per snapshot in commit order."""
    rows = (
        spark.sql(f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at")
        .to_arrow()
        .to_pylist()
    )
    appends = []
    for row in rows:
        summary = dict(row["summary"])
        appends.append(
            (row["operation"], summary.get("added-records"), summary.get("total-records"))
        )
    return appends


def _data_formats_by_snapshot(spark: ReparkSession, table: str) -> list[str]:
    """Return live data-file formats ordered by the snapshot sequence that added them."""
    rows = (
        spark.sql(
            f"SELECT data_file.file_format AS file_format FROM {table}.entries "
            "WHERE data_file.content = 0 ORDER BY sequence_number"
        )
        .to_arrow()
        .to_pylist()
    )
    return [str(row["file_format"]) for row in rows]


def _seed_three(spark: ReparkSession, table: str) -> None:
    """Insert the shared three-row `(id, data, cat)` seed into `table`."""
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")


def _three_col_table(
    spark: ReparkSession, table: str, properties: str, columns: str | None = None
) -> None:
    """Create `table` with `(id, data, cat)` columns and `properties` set."""
    cols = columns or "id BIGINT, data STRING, cat STRING"
    spark.sql(f"CREATE TABLE {table} ({cols}) USING iceberg TBLPROPERTIES ({properties})")


def test_create_format_orc_insert_reads_back(spark: ReparkSession) -> None:
    """D-CREATE-FMT-ORC: rows plus one ORC file. pins: ice-orc-avro-1/C-001."""
    table = f"{CATALOG}.{NS}.create_orc"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'orc')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    assert _ordered_rows(spark, f"SELECT id, data FROM {table} ORDER BY id") == [
        [1, "a"],
        [2, "b"],
    ]
    assert _ordered_rows(
        spark, f"SELECT file_format, count(*) FROM {table}.files GROUP BY file_format"
    ) == [["ORC", 1]]


def test_create_format_avro_insert_reads_back(spark: ReparkSession) -> None:
    """D-CREATE-FMT-AVRO: rows plus one AVRO file. pins: ice-orc-avro-1/C-002."""
    table = f"{CATALOG}.{NS}.create_avro"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'avro')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    assert _ordered_rows(spark, f"SELECT id, data FROM {table} ORDER BY id") == [
        [1, "a"],
        [2, "b"],
    ]
    assert _ordered_rows(
        spark, f"SELECT file_format, count(*) FROM {table}.files GROUP BY file_format"
    ) == [["AVRO", 1]]


def test_set_format_then_insert_writes_new_format(spark: ReparkSession) -> None:
    """D-X-SET-FORMAT-ORC-THEN-INSERT: old file parquet, new file ORC.

    pins: ice-orc-avro-1/C-003.
    """
    table = f"{CATALOG}.{NS}.set_format"
    _three_col_table(spark, table, "'format-version' = '2'")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.format.default' = 'orc')")
    spark.sql(f"INSERT INTO {table} VALUES (2, 'b', 'y')")
    assert _snapshot_count(spark, table) == 2
    assert _data_formats_by_snapshot(spark, table) == ["PARQUET", "ORC"]
    assert _table_property(spark, table, "write.format.default") == "orc"
    assert _snapshot_appends(spark, table) == [("append", "1", "1"), ("append", "1", "2")]
    assert _ordered_rows(spark, f"SELECT id, data, cat FROM {table} ORDER BY id") == [
        [1, "a", "x"],
        [2, "b", "y"],
    ]


def test_tp_format_orc_files_row(spark: ReparkSession) -> None:
    """TP-FORMAT-ORC: two ORC files holding three records. pins: ice-orc-avro-1/C-004."""
    table = f"{CATALOG}.{NS}.tp_orc"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'orc'")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c', 'x')")
    assert _files_summary(spark, table) == [[0, "ORC", 2, 3]]
    assert _ordered_rows(spark, f"SELECT id, data, cat FROM {table} ORDER BY id") == [
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "x"],
    ]
    assert _table_property(spark, table, "write.format.default") == "orc"
    assert _snapshot_appends(spark, table) == [("append", "2", "2"), ("append", "1", "3")]


def test_write_format_option_orc(spark: ReparkSession) -> None:
    """W-DF-OPT-WRITE-FORMAT-ORC: the option lands ORC bytes. pins: ice-orc-avro-1/C-005."""
    table = f"{CATALOG}.{NS}.opt_orc"
    _three_col_table(spark, table, "'format-version' = '2'")
    _seed_three(spark, table)
    frame = spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )
    frame.write.format("iceberg").option("write-format", "orc").mode("append").saveAsTable(table)
    assert _ordered_rows(spark, f"SELECT id, data, cat FROM {table} ORDER BY id") == [
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "x"],
        [7, "g", "x"],
        [8, "h", "w"],
    ]
    assert _snapshot_count(spark, table) == 2
    assert _table_property(spark, table, "write.format.default") is None
    assert _snapshot_appends(spark, table) == [("append", "3", "3"), ("append", "2", "5")]
    orc_rows = _ordered_rows(
        spark, f"SELECT file_format, record_count FROM {table}.files WHERE content = 0"
    )
    assert ["ORC", 2] in orc_rows


def test_write_format_option_avro(spark: ReparkSession) -> None:
    """W-DF-OPT-WRITE-FORMAT-AVRO: the option lands AVRO bytes.

    pins: ice-orc-avro-1/C-006.
    """
    table = f"{CATALOG}.{NS}.opt_avro"
    _three_col_table(spark, table, "'format-version' = '2'")
    _seed_three(spark, table)
    frame = spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )
    frame.write.format("iceberg").option("write-format", "avro").mode("append").saveAsTable(table)
    assert _ordered_rows(spark, f"SELECT id, data, cat FROM {table} ORDER BY id") == [
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "x"],
        [7, "g", "x"],
        [8, "h", "w"],
    ]
    assert _snapshot_count(spark, table) == 2
    assert _table_property(spark, table, "write.format.default") is None
    assert _snapshot_appends(spark, table) == [("append", "3", "3"), ("append", "2", "5")]
    avro_rows = _ordered_rows(
        spark, f"SELECT file_format, record_count FROM {table}.files WHERE content = 0"
    )
    assert ["AVRO", 2] in avro_rows


def _cow_files_are(
    spark: ReparkSession, table: str, file_format: str, rows: list[list[Any]]
) -> None:
    """Assert `table` holds `rows` and every live file is `content = 0` in `file_format`."""
    assert _ordered_rows(spark, f"SELECT id, data, cat FROM {table} ORDER BY id") == rows
    live = _ordered_rows(spark, f"SELECT content, file_format FROM {table}.files")
    assert live, f"expected live files on {table}"
    assert all(content == 0 and str(kind) == file_format for content, kind in live)


def test_cow_delete_on_orc_rewrites_as_orc(spark: ReparkSession) -> None:
    """W-FMT-ORC-DELETE: copy-on-write DELETE keeps ORC bytes. pins: ice-orc-avro-1/C-007."""
    table = f"{CATALOG}.{NS}.cow_orc_del"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'orc'")
    _seed_three(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    _cow_files_are(spark, table, "ORC", [[2, "b", "y"], [3, "c", "x"]])


def test_cow_update_on_orc_rewrites_as_orc(spark: ReparkSession) -> None:
    """W-FMT-ORC-UPDATE: copy-on-write UPDATE keeps ORC bytes. pins: ice-orc-avro-1/C-008."""
    table = f"{CATALOG}.{NS}.cow_orc_upd"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'orc'")
    _seed_three(spark, table)
    spark.sql(f"UPDATE {table} SET data = 'u' WHERE id = 1")
    _cow_files_are(spark, table, "ORC", [[1, "u", "x"], [2, "b", "y"], [3, "c", "x"]])


def test_cow_merge_on_orc_rewrites_as_orc(spark: ReparkSession) -> None:
    """W-FMT-ORC-MERGE: copy-on-write MERGE keeps ORC bytes. pins: ice-orc-avro-1/C-009."""
    table = f"{CATALOG}.{NS}.cow_orc_mrg"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'orc'")
    _seed_three(spark, table)
    spark.sql(
        f"MERGE INTO {table} t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE"
    )
    _cow_files_are(spark, table, "ORC", [[2, "b", "y"], [3, "c", "x"]])


def test_cow_delete_on_avro_rewrites_as_avro(spark: ReparkSession) -> None:
    """W-FMT-AVRO-DELETE: copy-on-write DELETE keeps AVRO bytes.

    pins: ice-orc-avro-1/C-010.
    """
    table = f"{CATALOG}.{NS}.cow_avro_del"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'avro'")
    _seed_three(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    _cow_files_are(spark, table, "AVRO", [[2, "b", "y"], [3, "c", "x"]])


def test_cow_update_on_avro_rewrites_as_avro(spark: ReparkSession) -> None:
    """W-FMT-AVRO-UPDATE: copy-on-write UPDATE keeps AVRO bytes.

    pins: ice-orc-avro-1/C-011.
    """
    table = f"{CATALOG}.{NS}.cow_avro_upd"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'avro'")
    _seed_three(spark, table)
    spark.sql(f"UPDATE {table} SET data = 'u' WHERE id = 1")
    _cow_files_are(spark, table, "AVRO", [[1, "u", "x"], [2, "b", "y"], [3, "c", "x"]])


def test_cow_merge_on_avro_rewrites_as_avro(spark: ReparkSession) -> None:
    """W-FMT-AVRO-MERGE: copy-on-write MERGE keeps AVRO bytes.

    pins: ice-orc-avro-1/C-012.
    """
    table = f"{CATALOG}.{NS}.cow_avro_mrg"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'avro'")
    _seed_three(spark, table)
    spark.sql(
        f"MERGE INTO {table} t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE"
    )
    _cow_files_are(spark, table, "AVRO", [[2, "b", "y"], [3, "c", "x"]])


def test_partitioned_v3_cow_delete_rewrites_orc(spark: ReparkSession) -> None:
    """V3 partitioned copy-on-write DELETE keeps ORC bytes on the lineage path.

    pins: ice-orc-avro-1/C-023.
    """
    table = f"{CATALOG}.{NS}.v3_part_orc_del"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat) TBLPROPERTIES ('format-version' = '3', "
        "'write.format.default' = 'orc', 'write.delete.mode' = 'copy-on-write')"
    )
    _seed_three(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    _cow_files_are(spark, table, "ORC", [[2, "b", "y"], [3, "c", "x"]])


def test_mor_delete_on_orc_writes_orc_delete_file(spark: ReparkSession) -> None:
    """M-1: a v2 merge-on-read ORC table writes ORC position deletes.

    pins: ice-orc-avro-1/C-013.
    """
    table = f"{CATALOG}.{NS}.mor_orc"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg TBLPROPERTIES "
        f"('format-version' = '2', 'write.format.default' = 'orc', {_MOR_MODES})"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    assert _ordered_rows(
        spark,
        f"SELECT content, file_format, record_count FROM {table}.files "
        "ORDER BY content, file_format",
    ) == [[0, "ORC", 3], [1, "ORC", 1]]


def test_delete_format_default_overrides(spark: ReparkSession) -> None:
    """M-2: `write.delete.format.default` overrides the data format.

    pins: ice-orc-avro-1/C-014.
    """
    table = f"{CATALOG}.{NS}.del_override"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg TBLPROPERTIES "
        "('format-version' = '2', 'write.format.default' = 'orc', "
        f"'write.delete.format.default' = 'parquet', {_MOR_MODES})"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    assert _ordered_rows(
        spark, f"SELECT content, file_format FROM {table}.files ORDER BY content"
    ) == [[0, "ORC"], [1, "PARQUET"]]


def test_v3_delete_side_is_puffin_for_every_data_format(spark: ReparkSession) -> None:
    """M-3: a v3 merge-on-read DELETE writes a PUFFIN side for every data format.

    pins: ice-orc-avro-1/C-015.
    """
    for file_format in ("parquet", "orc", "avro"):
        table = f"{CATALOG}.{NS}.v3_{file_format}"
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg TBLPROPERTIES "
            f"('format-version' = '3', 'write.format.default' = '{file_format}', {_MOR_MODES})"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        spark.sql(f"DELETE FROM {table} WHERE id = 1")
        assert _ordered_rows(
            spark,
            f"SELECT content, file_format, record_count FROM {table}.files "
            "ORDER BY content, file_format",
        ) == [[0, file_format.upper(), 3], [1, "PUFFIN", 1]], file_format


def test_read_foreign_orc_table(spark: ReparkSession) -> None:
    """W-READ-FOREIGN-ORC: rows read back from an ORC table. pins: ice-orc-avro-1/C-016."""
    table = f"{CATALOG}.{NS}.read_orc"
    _three_col_table(spark, table, "'format-version' = '2', 'write.format.default' = 'orc'")
    _seed_three(spark, table)
    assert _ordered_rows(spark, f"SELECT id, data, cat FROM {table} ORDER BY id") == [
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "x"],
    ]


def _single_file_metrics(spark: ReparkSession, table: str) -> dict[str, Any]:
    """Return the `readable_metrics` struct of the only live file of `table`."""
    column = (
        spark.sql(f"SELECT readable_metrics FROM {table}.files")
        .to_arrow()
        .column("readable_metrics")
    )
    assert len(column) == 1
    metrics = column[0].as_py()
    assert isinstance(metrics, dict)
    return metrics


def test_orc_metrics_carry_full_column_metrics(spark: ReparkSession) -> None:
    """M-5: ORC files carry full column metrics; NaN counts only on float columns.

    pins: ice-orc-avro-1/C-017.
    """
    table = f"{CATALOG}.{NS}.orc_metrics"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, ratio FLOAT, data STRING) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'orc')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 1.5, 'a'), (2, 2.5, 'b')")
    metrics = _single_file_metrics(spark, table)
    assert metrics["id"]["value_count"] == 2
    assert metrics["id"]["null_value_count"] == 0
    assert metrics["id"]["nan_value_count"] is None
    assert metrics["id"]["lower_bound"] == 1
    assert metrics["id"]["upper_bound"] == 2
    assert metrics["ratio"]["value_count"] == 2
    assert metrics["ratio"]["null_value_count"] == 0
    assert metrics["ratio"]["nan_value_count"] == 0
    assert metrics["ratio"]["lower_bound"] == 1.5
    assert metrics["ratio"]["upper_bound"] == 2.5
    assert metrics["data"]["value_count"] == 2
    assert metrics["data"]["null_value_count"] == 0
    assert metrics["data"]["nan_value_count"] is None
    assert metrics["data"]["lower_bound"] == "a"
    assert metrics["data"]["upper_bound"] == "b"
    for column in ("id", "ratio", "data"):
        size = metrics[column]["column_size"]
        assert isinstance(size, int) and size > 0, column


def test_avro_metrics_are_empty(spark: ReparkSession) -> None:
    """M-6: Avro files carry no column metrics. pins: ice-orc-avro-1/C-018."""
    table = f"{CATALOG}.{NS}.avro_metrics"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, ratio FLOAT, data STRING) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'avro')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 1.5, 'a'), (2, 2.5, 'b')")
    metrics = _single_file_metrics(spark, table)
    for column in ("id", "ratio", "data"):
        assert metrics[column] == {
            "column_size": None,
            "value_count": None,
            "null_value_count": None,
            "nan_value_count": None,
            "lower_bound": None,
            "upper_bound": None,
        }, column
    counts = _ordered_rows(spark, f"SELECT record_count FROM {table}.files")
    assert counts == [[2]]


def test_all_types_round_trip_orc(spark: ReparkSession) -> None:
    """M-7: primitives round-trip through ORC; nested reads refuse naming the field.

    The fork ORC reader is primitives-only at 604edca0, so the nested columns pin
    its typed refusal instead of a round trip. A fork read-nested follow-up inverts
    those arms back to round trips. pins: ice-orc-avro-1/C-019.
    """
    table = f"{CATALOG}.{NS}.types_orc"
    spark.sql(
        f"CREATE TABLE {table} ({_PRIMITIVE_COLUMNS}) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'orc')"
    )
    spark.sql(f"INSERT INTO {table} VALUES {_PRIMITIVE_VALUES}")
    rows = spark.sql(f"SELECT * FROM {table}").to_arrow().to_pylist()
    expected = {key: _WIDE_ROW[key] for key in ("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")}
    assert rows == [expected]
    for name, column, literal in (
        ("k", "k ARRAY<INT>", "array(1,2)"),
        ("l", "l MAP<STRING,INT>", "map('k',1)"),
        ("m", "m STRUCT<x:INT,y:STRING>", "named_struct('x',1,'y','z')"),
    ):
        nested = f"{CATALOG}.{NS}.orc_nested_{name}"
        spark.sql(
            f"CREATE TABLE {nested} ({column}) USING iceberg "
            "TBLPROPERTIES ('write.format.default' = 'orc')"
        )
        spark.sql(f"INSERT INTO {nested} VALUES ({literal})")
        with pytest.raises(
            PySparkException,
            match=(
                "^External error: FeatureUnsupported => ORC data-file read of nested "
                f"type for field '{name}' is not supported yet "
                "\\(only top-level primitive/logical columns\\)$"
            ),
        ):
            spark.sql(f"SELECT * FROM {nested}").to_arrow()


def test_all_types_round_trip_avro(spark: ReparkSession) -> None:
    """M-7: every probe type round-trips through Avro. pins: ice-orc-avro-1/C-020."""
    table = f"{CATALOG}.{NS}.types_avro"
    spark.sql(
        f"CREATE TABLE {table} ({_WIDE_COLUMNS}) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'avro')"
    )
    spark.sql(f"INSERT INTO {table} VALUES {_WIDE_VALUES}")
    rows = spark.sql(f"SELECT * FROM {table}").to_arrow().to_pylist()
    assert rows == [_WIDE_ROW]


def test_unknown_write_format_refuses(spark: ReparkSession) -> None:
    """D-5.1: `write-format = csv` refuses with the Java shape.

    pins: ice-orc-avro-1/C-021.
    """
    table = f"{CATALOG}.{NS}.fmt_bogus"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, name STRING) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (0, 'name-0'), (1, 'name-1')")
    frame = spark.sql("SELECT * FROM (VALUES (2, 'name-2')) AS t(id, name)")
    with pytest.raises(IllegalArgumentException, match=r"^Invalid file format: csv$"):
        frame.writeTo(table).option("write-format", "csv").append()
    assert _snapshot_count(spark, table) == 1


def test_unknown_delete_format_property_refuses(spark: ReparkSession) -> None:
    """`write.delete.format.default = 'csv'` refuses a v2 merge-on-read DELETE.

    pins: ice-orc-avro-1/C-025.
    """
    table = f"{CATALOG}.{NS}.del_fmt_bogus"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg TBLPROPERTIES "
        f"('format-version' = '2', 'write.delete.format.default' = 'csv', {_MOR_MODES})"
    )
    spark.sql(f"INSERT INTO {table} VALUES (0, 'a'), (1, 'b')")
    with pytest.raises(IllegalArgumentException, match=r"^Invalid file format: csv$"):
        spark.sql(f"DELETE FROM {table} WHERE id = 0")
    assert _snapshot_count(spark, table) == 1


def test_compaction_keeps_table_format(spark: ReparkSession) -> None:
    """D-4 site 4: `rewrite_data_files` on an ORC table leaves ORC files.

    pins: ice-orc-avro-1/C-022.
    """
    table = f"{CATALOG}.{NS}.compact_orc"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT) USING iceberg "
        "TBLPROPERTIES ('write.format.default' = 'orc')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    spark.sql(f"INSERT INTO {table} VALUES (2)")
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files('{NS}.compact_orc', 'binpack', NULL, "
        "map('min-input-files', '2'))"
    ).to_arrow()
    assert _live_data_formats(spark, table) == ["ORC"]
    assert _ordered_rows(spark, f"SELECT id FROM {table} ORDER BY id") == [[1], [2]]

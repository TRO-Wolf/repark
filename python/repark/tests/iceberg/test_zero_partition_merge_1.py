from __future__ import annotations

import struct
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from test_v3_live_oracle import _LIVE, _LIVE_SKIP, _v37_iceberg_runtime_jar

from repark import ReparkSession

_RP = "zpm_rp"
_NS = "ns"
_NEG = {"d": "8000000000000000", "f": "80000000"}
_POS = {"d": "0000000000000000", "f": "00000000"}
_TEXT = {"neg": "-0.0", "pos": "0.0", "one": "1.5", "two": "2.0", "null": None}
_BITS = {
    "neg": ("8000000000000000", "80000000"),
    "pos": ("0000000000000000", "00000000"),
    "one": ("3ff8000000000000", "3fc00000"),
    "two": ("4000000000000000", "40000000"),
    "null": (None, None),
}
_BASE = ((1, "neg"), (2, "pos"), (3, "one"), (4, "null"), (5, "pos"), (6, "neg"))
_PAIRS = ((1, 7, "neg"), (2, 8, "pos"), (3, 9, "one"), (4, 10, "null"))
_INTERLEAVED = ((1, "pos"), (2, "neg"), (3, "pos"), (4, "neg"))
_CROSS_SEED = ((10, "neg"), (11, "one"))
_CROSS_NEW = ((1, "pos"),)
_FILLER = ((99, "two"),)
_PAIR_ROWS = tuple((i, k) for a, b, k in _PAIRS for i in (a, b))


def _value(kind: str, sql_type: str) -> str:
    text = _TEXT[kind]
    return f"CAST(NULL AS {sql_type})" if text is None else f"CAST('{text}' AS {sql_type})"


def _select(rows: tuple[tuple[int, str], ...]) -> str:
    values = ", ".join(
        f"(CAST({i} AS BIGINT), {_value(k, 'DOUBLE')}, {_value(k, 'FLOAT')})" for i, k in rows
    )
    return f"SELECT id, d, f FROM (VALUES {values}) AS q(id, d, f)"


def _props(version: int, fmt: str, mode: str, extra: str) -> str:
    return (
        f"TBLPROPERTIES ('format-version' = '{version}', 'write.format.default' = '{fmt}', "
        f"'write.update.mode' = '{mode}', 'write.delete.mode' = '{mode}', "
        f"'write.merge.mode' = '{mode}'{extra})"
    )


class _Door:
    def __init__(self, session: Any, table: str, col: str, version: int, fmt: str) -> None:
        self.session = session
        self.table = table
        self.col = col
        self.version = version
        self.fmt = fmt

    def sql(self, text: str) -> None:
        self.session.sql(text).collect()

    def create(self, mode: str = "copy-on-write", extra: str = "") -> None:
        self.sql(
            f"CREATE TABLE {self.table} (id BIGINT, d DOUBLE, f FLOAT) USING iceberg "
            f"PARTITIONED BY ({self.col}) {_props(self.version, self.fmt, mode, extra)}"
        )

    def seed(self, rows: tuple[tuple[int, str], ...]) -> None:
        for row in rows:
            self.sql(f"INSERT INTO {self.table} {_select((row,))}")

    def seed_pairs(self) -> None:
        for a, b, kind in _PAIRS:
            self.sql(f"INSERT INTO {self.table} {_select(((a, kind), (b, kind)))}")

    def call(self, procedure: str, options: str) -> None:
        name = self.table.split(".", 1)[1]
        self.sql(f"CALL {_RP}.system.{procedure}(table => '{name}'{options})")


def _insert_values(door: _Door) -> None:
    door.create()
    door.sql(f"INSERT INTO {door.table} {_select(_BASE)}")


def _ctas(door: _Door) -> None:
    props = _props(door.version, door.fmt, "copy-on-write", "")
    door.sql(
        f"CREATE TABLE {door.table} USING iceberg PARTITIONED BY ({door.col}) {props} "
        f"AS {_select(_BASE)}"
    )


def _wt_create(door: _Door) -> None:
    from repark import functions

    (
        door.session.sql(_select(_BASE))
        .writeTo(door.table)
        .using("iceberg")
        .tableProperty("format-version", str(door.version))
        .tableProperty("write.format.default", door.fmt)
        .partitionedBy(functions.col(door.col))
        .create()
    )


def _sat_overwrite(door: _Door) -> None:
    door.create()
    door.seed(_FILLER)
    frame = door.session.sql(_select(_BASE))
    frame.write.format("iceberg").mode("overwrite").partitionBy(door.col).saveAsTable(door.table)


def _merge_insert(door: _Door) -> None:
    door.create()
    door.sql(
        f"MERGE INTO {door.table} t USING ({_select(_BASE)}) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def _update_mor(door: _Door) -> None:
    door.create("merge-on-read")
    door.seed_pairs()
    door.sql(f"UPDATE {door.table} SET id = id WHERE id >= 7")


def _delete_cow(door: _Door) -> None:
    door.create()
    door.seed_pairs()
    door.sql(f"DELETE FROM {door.table} WHERE id >= 7")


def _rewrite_data_files(door: _Door) -> None:
    door.create()
    door.seed_pairs()
    door.call("rewrite_data_files", ", options => map('rewrite-all', 'true')")


def _dynamic_overwrite_cross(door: _Door) -> None:
    door.create()
    door.seed(_CROSS_SEED)
    door.session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        door.sql(f"INSERT OVERWRITE {door.table} {_select(_CROSS_NEW)}")
    finally:
        door.session.conf.set("spark.sql.sources.partitionOverwriteMode", "static")


def _overwrite_partitions_cross(door: _Door) -> None:
    door.create()
    door.seed(_CROSS_SEED)
    door.session.sql(_select(_CROSS_NEW)).writeTo(door.table).overwritePartitions()


def _branch_insert(door: _Door) -> None:
    door.create()
    door.seed(_FILLER)
    door.sql(f"ALTER TABLE {door.table} CREATE BRANCH b")
    door.sql(f"INSERT INTO {door.table}.branch_b {_select(_BASE)}")


def _partition_deletes(door: _Door) -> None:
    door.create("merge-on-read", ", 'write.delete.granularity' = 'partition'")
    door.seed_pairs()
    door.sql(f"DELETE FROM {door.table} WHERE id >= 7")


def _rewrite_position_deletes(door: _Door) -> None:
    door.create("merge-on-read")
    door.seed_pairs()
    door.sql(f"DELETE FROM {door.table} WHERE id = 7")
    door.sql(f"DELETE FROM {door.table} WHERE id = 8")
    door.call("rewrite_position_delete_files", ", options => map('rewrite-all', 'true')")


def _run_maintenance(door: _Door) -> None:
    door.create("merge-on-read", ", 'write.delete.granularity' = 'partition'")
    door.seed_pairs()
    door.sql(f"DELETE FROM {door.table} WHERE id = 7")
    door.sql(f"DELETE FROM {door.table} WHERE id = 8")
    door.call(
        "run_maintenance",
        ", dry_run => false, target_file_size_bytes => 536870912, position_delete_ratio => 0.0",
    )


def _write_ordered(door: _Door, extra: str) -> None:
    door.create("copy-on-write", extra)
    door.sql(f"ALTER TABLE {door.table} WRITE ORDERED BY id")
    door.sql(f"INSERT INTO {door.table} {_select(_INTERLEAVED)}")


_PAIR_SURVIVORS = tuple(row for row in _PAIR_ROWS if row[0] < 7)
_PAIR_MINUS_78 = tuple(row for row in _PAIR_ROWS if row[0] not in (7, 8))
_CELLS: dict[str, tuple[int, str, Callable[[_Door], None], tuple[tuple[int, str], ...], bool]] = {
    "insert_parquet": (2, "parquet", _insert_values, _BASE, False),
    "insert_orc": (2, "orc", _insert_values, _BASE, False),
    "insert_avro": (2, "avro", _insert_values, _BASE, False),
    "ctas": (2, "parquet", _ctas, _BASE, False),
    "wt_create": (3, "parquet", _wt_create, _BASE, False),
    "sat_overwrite": (2, "parquet", _sat_overwrite, _BASE, False),
    "merge_insert_v3": (3, "parquet", _merge_insert, _BASE, False),
    "update_mor_v3": (3, "parquet", _update_mor, _PAIR_ROWS, False),
    "delete_cow_v3": (3, "parquet", _delete_cow, _PAIR_SURVIVORS, False),
    "rewrite_data_files": (2, "parquet", _rewrite_data_files, _PAIR_ROWS, False),
    "dynamic_overwrite_cross": (
        2,
        "parquet",
        _dynamic_overwrite_cross,
        (*_CROSS_SEED, *_CROSS_NEW),
        False,
    ),
    "overwrite_partitions_cross": (
        2,
        "parquet",
        _overwrite_partitions_cross,
        (*_CROSS_SEED, *_CROSS_NEW),
        False,
    ),
    "branch_insert": (2, "parquet", _branch_insert, (*_BASE, *_FILLER), False),
    "partition_deletes": (2, "parquet", _partition_deletes, _PAIR_SURVIVORS, True),
    "rewrite_position_deletes": (2, "parquet", _rewrite_position_deletes, _PAIR_MINUS_78, True),
    "run_maintenance": (2, "parquet", _run_maintenance, _PAIR_MINUS_78, False),
    "write_ordered": (2, "parquet", lambda door: _write_ordered(door, ""), _INTERLEAVED, False),
    "write_ordered_clustered": (
        2,
        "parquet",
        lambda door: _write_ordered(door, ", 'write.datafusion.fanout.enabled' = 'false'"),
        _INTERLEAVED,
        False,
    ),
}
_COLUMNS = ("d", "f")
_WRITERS = ("1", "16")


def _bits(value: float | None, width: int) -> str | None:
    if value is None:
        return None
    return struct.pack(">d" if width == 8 else ">f", value).hex()


def _expected_rows(rows: tuple[tuple[int, str], ...]) -> dict[int, tuple[str | None, str | None]]:
    return {i: _BITS[k] for i, k in rows}


def _read(session: Any, table: str, branch: bool) -> dict[int, tuple[str | None, str | None]]:
    source = f"{table}.branch_b" if branch else table
    got = session.sql(f"SELECT id, d, f FROM {source}").collect()
    return {r[0]: (_bits(r[1], 8), _bits(r[2], 4)) for r in got}


def _zero_partitions(session: Any, table: str, col: str, branch: bool) -> dict[int, list[str]]:
    suffix = " VERSION AS OF 'b'" if branch else ""
    width = 8 if col == "d" else 4
    out: dict[int, set[str]] = {0: set(), 1: set()}
    for row in session.sql(f"SELECT content, partition FROM {table}.files{suffix}").collect():
        bits = _bits(row[1][col], width)
        if bits in (_NEG[col], _POS[col]):
            out[row[0]].add(bits)
    return {content: sorted(signs) for content, signs in out.items()}


def _want_zero_partitions(
    rows: tuple[tuple[int, str], ...], col: str, deletes: bool
) -> dict[int, list[str]]:
    signs = sorted({_NEG[col] if k == "neg" else _POS[col] for _, k in rows if k in ("neg", "pos")})
    return {0: signs, 1: sorted((_NEG[col], _POS[col])) if deletes else []}


def _name(catalog: str, cell: str, col: str, writers: str) -> str:
    return f"{catalog}.{_NS}.{cell}_{col}_w{writers}"


def _answers(session: Any, table: str, col: str, cell: str) -> dict[str, Any]:
    branch = cell == "branch_insert"
    return {
        "rows": _read(session, table, branch),
        "zeros": _zero_partitions(session, table, col, branch),
    }


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("zero_partition_merge_1")


@pytest.fixture(scope="module")
def repark_side(warehouse: Path) -> dict[Any, Any]:
    out: dict[Any, Any] = {}
    for writers in _WRITERS:
        session = (
            ReparkSession.builder.appName(f"zero-partition-merge-1-w{writers}")
            .config("repark.sql.allowCreateFormatVersion3", "true")
            .config("spark.sql.shuffle.partitions", writers)
            .config(f"spark.sql.catalog.{_RP}.type", "hadoop")
            .config(f"spark.sql.catalog.{_RP}.warehouse", str(warehouse / f"repark_w{writers}"))
            .getOrCreate()
        )
        try:
            session.sql(f"CREATE NAMESPACE IF NOT EXISTS {_RP}.{_NS}").collect()
            for cell, (version, fmt, run, _rows, _deletes) in _CELLS.items():
                for col in _COLUMNS:
                    table = _name(_RP, cell, col, writers)
                    try:
                        run(_Door(session, table, col, version, fmt))
                        out[(cell, col, writers)] = _answers(session, table, col, cell)
                    except Exception as error:
                        out[(cell, col, writers)] = {"error": f"{type(error).__name__}: {error}"}
        finally:
            session.stop()
    return out


_PARAMS = [(cell, col, writers) for cell in _CELLS for col in _COLUMNS for writers in _WRITERS]


@pytest.mark.parametrize(("cell", "col", "writers"), _PARAMS)
def test_repark_keeps_the_two_zero_partitions_apart(
    repark_side: dict[Any, Any], cell: str, col: str, writers: str
) -> None:
    _version, _fmt, _run, rows, deletes = _CELLS[cell]
    got = repark_side[(cell, col, writers)]
    assert "error" not in got, got
    assert got["rows"] == _expected_rows(rows)
    want = _want_zero_partitions(rows, col, deletes)
    assert got["zeros"][0] == want[0]
    if deletes:
        assert got["zeros"][1] == want[1]


def _live_spark(root: Path) -> tuple[Any, str]:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("zero-partition-merge-1-live")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        )
        jar = _v37_iceberg_runtime_jar()
        builder = (
            builder.config("spark.jars", jar)
            if jar is not None
            else builder.config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        )
        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
    catalog = f"zpm_sp_{uuid.uuid4().hex[:8]}"
    session.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
    session.conf.set(f"spark.sql.catalog.{catalog}.warehouse", str(root))
    return session, catalog


@pytest.fixture(scope="module")
def spark_reads(repark_side: dict[Any, Any], warehouse: Path) -> dict[Any, Any]:
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    out: dict[Any, Any] = {}
    for writers in _WRITERS:
        spark, catalog = _live_spark(warehouse / f"repark_w{writers}")
        for cell in _CELLS:
            for col in _COLUMNS:
                table = _name(catalog, cell, col, writers)
                try:
                    out[(cell, col, writers)] = _answers(spark, table, col, cell)
                except Exception as error:
                    out[(cell, col, writers)] = {"error": f"{type(error).__name__}: {error}"[:300]}
    return out


@pytest.mark.parametrize(("cell", "col", "writers"), _PARAMS)
def test_spark_reads_each_rows_own_zero_from_repark_tables(
    spark_reads: dict[Any, Any], cell: str, col: str, writers: str
) -> None:
    _version, _fmt, _run, rows, deletes = _CELLS[cell]
    got = spark_reads[(cell, col, writers)]
    assert "error" not in got, got
    assert got["rows"] == _expected_rows(rows)
    want = _want_zero_partitions(rows, col, deletes)
    assert got["zeros"][0] == want[0]
    if deletes:
        assert got["zeros"][1] == want[1]


@pytest.mark.parametrize("col", _COLUMNS)
def test_spark_refuses_interleaved_zeros_on_a_write_ordered_table(
    warehouse: Path, col: str
) -> None:
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    spark, catalog = _live_spark(warehouse / "spark_own")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{_NS}")
    door = _Door(spark, f"{catalog}.{_NS}.ordered_{col}", col, 2, "parquet")
    with pytest.raises(Exception, match="clustered"):
        _write_ordered(door, "")

"""ICE-V3-WRITE-DEFAULT-1 — omitted columns fill from ``write_default`` on every write path.

The Java-API-created format-v3 tables live in
``python/repark-parity/fixtures/torture/data/ice_v3_write_default_1`` with the
Spark-recorded truth beside them. Offline cells adopt the fixture on a fresh
copy per test and replay one shape each; the live cell rebuilds the tables on
live Spark and replays both engines.

pins: ice-v3-write-default-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
  C-008, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018, C-020,
  C-021, C-022
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

_REPOS_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE_SRC = (
    _REPOS_ROOT
    / "python"
    / "repark-parity"
    / "fixtures"
    / "torture"
    / "data"
    / "ice_v3_write_default_1"
)
_TABLE_ROOT = Path("/tmp/repark-ice-v3-write-default-1/ns")
_CATALOG = "ice_v3_write_default_1"
_NAMESPACE = "ns"
_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (CI is JVM-free)"
_UTC = UTC


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

    def __init__(self, path: Path) -> None:
        """Acquire the lock directory, waiting up to two minutes."""
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(
                        f"fixture lock {path} held for 2 minutes (no steal)"
                    ) from None
                time.sleep(0.025)

    def close(self) -> None:
        """Release the lock directory."""
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize() -> Iterator[Path]:
    """Copy the checked-in tables to their baked-in canonical location."""
    lock = _DirLock(Path(str(_TABLE_ROOT) + ".lock"))
    try:
        if _TABLE_ROOT.exists():
            shutil.rmtree(_TABLE_ROOT)
        _TABLE_ROOT.mkdir(parents=True, exist_ok=True)
        shutil.copytree(
            _FIXTURE_SRC / "ns", _TABLE_ROOT, dirs_exist_ok=True, copy_function=shutil.copy
        )
        yield _TABLE_ROOT
    finally:
        with suppress(OSError):
            if _TABLE_ROOT.exists():
                shutil.rmtree(_TABLE_ROOT)
        lock.close()


def _newest_metadata_file(table_root: Path) -> Path:
    """The highest-versioned vN.metadata.json under a table root."""
    versions = sorted(
        (table_root / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    if not versions:
        raise ValueError(f"no vN metadata files under {table_root}/metadata")
    return versions[-1]


def _truth() -> dict[str, Any]:
    """The recorded Spark oracle."""
    return json.loads((_FIXTURE_SRC / "truth.json").read_text(encoding="utf-8"))


def _json_value(value: Any) -> Any:
    """Convert one truth JSON scalar to the driver-side value."""
    if isinstance(value, str):
        for parse in (date.fromisoformat, datetime.fromisoformat, Decimal):
            try:
                return parse(value)
            except Exception:
                continue
        return value
    return value


def _seed_rows(table: str) -> list[tuple[Any, ...]]:
    """Recorded seed rows of a fixture table as driver values."""
    seed = _truth()["tables"][table]["seed"]
    assert seed["outcome"] == "ok", seed
    rows = seed["rows"]
    assert isinstance(rows, list)
    return [tuple(_json_value(value) for value in row) for row in rows]


def _cell_contains(shape: str, row: tuple[Any, ...]) -> None:
    """Fail unless the truth cell holds the written row."""
    cell = _truth()["cells"][shape]
    assert cell["outcome"] == "ok", cell
    assert [_json_value(value) for value in row] in (
        [_json_value(value) for value in recorded] for recorded in cell["rows"]
    ), f"{row} not in {shape}"


def _cell_errors(shape: str) -> None:
    """Fail unless the truth cell records a refused write."""
    cell = _truth()["cells"][shape]
    assert cell["outcome"] == "error", cell


def _expect(table: str, extra: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Seed rows plus written rows in the ``_rows`` sort order."""
    return sorted(_seed_rows(table) + extra, key=repr)


def _adopt(session: Any, catalog: str, table: str) -> None:
    """Adopt one fixture table into a memory catalog."""
    session.sql(
        f"CALL {catalog}.system.register_table("
        f"table => '{_NAMESPACE}.{table}', "
        f"metadata_file => '{_newest_metadata_file(_TABLE_ROOT / table)}')"
    )


def _rows(session: Any, catalog: str, table: str) -> list[tuple[Any, ...]]:
    """All columns of a table as tuples, sorted by id."""
    arrow_table = session.sql(
        f"SELECT * FROM {catalog}.{_NAMESPACE}.{table} ORDER BY id"
    ).to_arrow()
    cols = arrow_table.column_names
    return sorted(
        (tuple(row[col] for col in cols) for row in arrow_table.to_pylist()),
        key=repr,
    )


def _session(app: str) -> Any:
    """Build a facade session with a scratch memory catalog namespace."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName(app).getOrCreate()
    session.register_memory_catalog(_CATALOG, Path(f"/tmp/{app}-warehouse"))
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}")
    return session


def test_fixture_seed_matches_truth() -> None:
    """Adopted fixture tables read back the recorded seed rows."""
    session = _session("ice-v3-write-default-1-seed")
    try:
        with _materialize():
            for table in ("defaults", "strdef", "temporal", "differ", "nodefault", "required"):
                _adopt(session, _CATALOG, table)
            assert _rows(session, _CATALOG, "defaults") == _seed_rows("defaults")
            assert _rows(session, _CATALOG, "strdef") == _seed_rows("strdef")
            assert _rows(session, _CATALOG, "temporal") == _seed_rows("temporal")
            assert _rows(session, _CATALOG, "differ") == _seed_rows("differ")
            assert _rows(session, _CATALOG, "nodefault") == _seed_rows("nodefault")
            assert _rows(session, _CATALOG, "required") == _seed_rows("required")
            seed = _truth()["tables"]["decdef"]["seed"]
            assert seed["outcome"] == "error", seed
    finally:
        session.stop()


def test_insert_column_list_fills_write_default() -> None:
    """A column-list INSERT fills the omitted column from ``write_default``."""
    session = _session("ice-v3-write-default-1-insert")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults (id, name) VALUES (11, 'k')"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == _expect("defaults", [(11, "k", 5)])
            _cell_contains("insert_column_list", (11, "k", 5))
    finally:
        session.stop()


def test_merge_not_matched_fills_write_default() -> None:
    """A MERGE NOT MATCHED INSERT fills the omitted column from ``write_default``."""
    session = _session("ice-v3-write-default-1-merge")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            session.sql(
                f"MERGE INTO {_CATALOG}.{_NAMESPACE}.defaults t"
                " USING (SELECT 12 AS id, 'l' AS name) s"
                " ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == _expect("defaults", [(12, "l", 5)])
            _cell_contains("merge_not_matched", (12, "l", 5))
    finally:
        session.stop()


def test_dataframe_writers_fill_write_default() -> None:
    """``writeTo`` and ``saveAsTable`` appends fill a missing defaulted column."""
    session = _session("ice-v3-write-default-1-writers")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            session.createDataFrame([(13, "m")], "id int, name string").writeTo(
                f"{_CATALOG}.{_NAMESPACE}.defaults"
            ).append()
            assert _rows(session, _CATALOG, "defaults") == _expect("defaults", [(13, "m", 5)])
            _cell_contains("writeto_append", (13, "m", 5))
            session.createDataFrame([(14, "n")], "id int, name string").write.mode(
                "append"
            ).saveAsTable(f"{_CATALOG}.{_NAMESPACE}.defaults")
            assert _rows(session, _CATALOG, "defaults") == _expect(
                "defaults", [(13, "m", 5), (14, "n", 5)]
            )
            _cell_contains("saveas_append", (14, "n", 5))
    finally:
        session.stop()


def test_default_keyword_fills_write_default() -> None:
    """The DEFAULT keyword fills from ``write_default`` in VALUES and SELECT position."""
    session = _session("ice-v3-write-default-1-default-kw")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults VALUES (15, 'o', DEFAULT)"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == _expect("defaults", [(15, "o", 5)])
            _cell_contains("insert_values_default_kw", (15, "o", 5))
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults (id, name, c)"
                " SELECT 18, 'r', DEFAULT"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == _expect(
                "defaults", [(15, "o", 5), (18, "r", 5)]
            )
            _cell_contains("select_position_default", (18, "r", 5))
    finally:
        session.stop()


def test_short_inserts_refuse() -> None:
    """Positional-short and SELECT-short INSERT statements refuse without writing."""
    session = _session("ice-v3-write-default-1-short")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            _cell_errors("insert_positional_short")
            _cell_errors("insert_select_short")
            with pytest.raises(Exception, match="Inconsistent data length"):
                session.sql(
                    f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults VALUES (8, 'h')"
                ).collect()
            with pytest.raises(Exception, match="Column count doesn't match"):
                session.sql(f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults SELECT 9, 'i'").collect()
            assert _rows(session, _CATALOG, "defaults") == _seed_rows("defaults")
    finally:
        session.stop()


def test_insert_into_and_extra_column_refuse() -> None:
    """``insertInto`` with a missing column and ``writeTo`` with an extra column refuse."""
    session = _session("ice-v3-write-default-1-refuse")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            _cell_errors("insertInto_missing")
            _cell_errors("writeto_extra_col")
            with pytest.raises(Exception, match="Column count doesn't match"):
                session.createDataFrame([(19, "s")], "id int, name string").write.insertInto(
                    f"{_CATALOG}.{_NAMESPACE}.defaults"
                )
            with pytest.raises(Exception, match="extra in the DataFrame"):
                session.createDataFrame(
                    [(21, "u", "zzz")], "id int, name string, zzz string"
                ).writeTo(f"{_CATALOG}.{_NAMESPACE}.defaults").append()
            assert _rows(session, _CATALOG, "defaults") == _seed_rows("defaults")
    finally:
        session.stop()


def test_explicit_null_stays_null() -> None:
    """A listed NULL is user intent and never fills from ``write_default``."""
    session = _session("ice-v3-write-default-1-null")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults (id, name, c) VALUES (16, 'p', NULL)"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == _expect("defaults", [(16, "p", None)])
            _cell_contains("explicit_null_values", (16, "p", None))
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.defaults SELECT 17, 'q', NULL"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == _expect(
                "defaults", [(16, "p", None), (17, "q", None)]
            )
            _cell_contains("explicit_null_select", (17, "q", None))
    finally:
        session.stop()


def test_no_default_table_unchanged() -> None:
    """A table without defaults null-fills omitted columns on SQL and DataFrame doors."""
    session = _session("ice-v3-write-default-1-nodefault")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "nodefault")
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.nodefault (id, name) VALUES (2, 'b')"
            ).collect()
            assert _rows(session, _CATALOG, "nodefault") == _expect("nodefault", [(2, "b", None)])
            _cell_contains("nodefault_insert_column_list", (2, "b", None))
            session.createDataFrame([(3, "c")], "id int, name string").writeTo(
                f"{_CATALOG}.{_NAMESPACE}.nodefault"
            ).append()
            assert _rows(session, _CATALOG, "nodefault") == _expect(
                "nodefault", [(2, "b", None), (3, "c", None)]
            )
            _cell_contains("nodefault_writeto_append", (3, "c", None))
            session.sql(
                f"INSERT INTO {_CATALOG}.{_NAMESPACE}.nodefault VALUES (4, 'd', DEFAULT)"
            ).collect()
            assert _rows(session, _CATALOG, "nodefault") == _expect(
                "nodefault", [(2, "b", None), (3, "c", None), (4, "d", None)]
            )
            _cell_contains("nodefault_values_default_kw", (4, "d", None))
    finally:
        session.stop()


def test_string_temporal_differ_defaults() -> None:
    """String, date, timestamp and differing write/initial defaults fill exactly."""
    session = _session("ice-v3-write-default-1-types")
    try:
        with _materialize():
            for table in ("strdef", "temporal", "differ"):
                _adopt(session, _CATALOG, table)
            session.sql(f"INSERT INTO {_CATALOG}.{_NAMESPACE}.strdef (id) VALUES (12)").collect()
            assert _rows(session, _CATALOG, "strdef") == _expect("strdef", [(12, "hi")])
            _cell_contains("string_default_insert", (12, "hi"))
            session.sql(f"INSERT INTO {_CATALOG}.{_NAMESPACE}.temporal (id) VALUES (12)").collect()
            expected_temporal = (12, date(2024, 10, 4), datetime(2024, 10, 4, tzinfo=_UTC))
            assert _rows(session, _CATALOG, "temporal") == _expect("temporal", [expected_temporal])
            _cell_contains("temporal_default_insert", expected_temporal)
            session.sql(f"INSERT INTO {_CATALOG}.{_NAMESPACE}.differ (id) VALUES (12)").collect()
            assert _rows(session, _CATALOG, "differ") == _expect("differ", [(12, 7)])
            _cell_contains("differ_insert", (12, 7))
    finally:
        session.stop()


def test_decimal_default_fills_exact() -> None:
    """A decimal ``write_default`` fills bit-exact; Spark cannot read its own fill back."""
    session = _session("ice-v3-write-default-1-decimal")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "decdef")
            session.sql(f"INSERT INTO {_CATALOG}.{_NAMESPACE}.decdef (id) VALUES (2)").collect()
            answer = session.sql(
                f"SELECT id, d FROM {_CATALOG}.{_NAMESPACE}.decdef ORDER BY id"
            ).to_arrow()
            assert answer.column("d").to_pylist() == [Decimal("3.14"), Decimal("3.14")]
            assert answer.schema.field("d").type == pa.decimal128(10, 2)
            seed = _truth()["tables"]["decdef"]["seed"]
            assert seed["outcome"] == "error", seed
    finally:
        session.stop()


def test_required_missing_refuses() -> None:
    """An omitted required column with no default refuses on SQL and DataFrame doors."""
    session = _session("ice-v3-write-default-1-required")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "required")
            _cell_errors("required_missing_insert")
            _cell_errors("required_missing_writeto")
            with pytest.raises(Exception, match="non-nullable but contains null"):
                session.sql(
                    f"INSERT INTO {_CATALOG}.{_NAMESPACE}.required (id) VALUES (2)"
                ).collect()
            with pytest.raises(Exception, match="non-nullable but contains null"):
                session.createDataFrame([(3,)], "id int").writeTo(
                    f"{_CATALOG}.{_NAMESPACE}.required"
                ).append()
            assert _rows(session, _CATALOG, "required") == _seed_rows("required")
    finally:
        session.stop()


def test_overwrite_column_list_fills_write_default() -> None:
    """A Spark-door INSERT OVERWRITE column list fills the omitted defaulted column."""
    session = _session("ice-v3-write-default-1-overwrite")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "defaults")
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.defaults (id, name) SELECT 30, 'ov'"
            ).collect()
            assert _rows(session, _CATALOG, "defaults") == [(30, "ov", 5)]
            _cell_contains("overwrite_column_list", (30, "ov", 5))
    finally:
        session.stop()


def test_v2_table_without_defaults_unchanged() -> None:
    """A format-v2 table with no defaults keeps its plain append behavior."""
    session = _session("ice-v3-write-default-1-v2")
    try:
        session.register_memory_catalog(
            "ice_v3_write_default_1_v2", Path("/tmp/ice-v3-write-default-1-v2-wh")
        )
        session.sql("CREATE NAMESPACE ice_v3_write_default_1_v2.ns")
        session.sql(
            "CREATE TABLE ice_v3_write_default_1_v2.ns.plain (id INT, name STRING)"
            " USING iceberg TBLPROPERTIES ('format-version' = '2')"
        )
        session.sql("INSERT INTO ice_v3_write_default_1_v2.ns.plain VALUES (1, 'a')").collect()
        session.sql(
            "INSERT INTO ice_v3_write_default_1_v2.ns.plain (id, name) VALUES (2, 'b')"
        ).collect()
        answer = session.sql(
            "SELECT id, name FROM ice_v3_write_default_1_v2.ns.plain ORDER BY id"
        ).to_arrow()
        assert answer.column("id").to_pylist() == [1, 2]
        assert answer.column("name").to_pylist() == ["a", "b"]
    finally:
        session.stop()


def test_live_write_default_parity(tmp_path: Path) -> None:
    """Live Spark rebuilds the tables; both engines replay the oracle shapes."""
    if not _LIVE:
        pytest.skip(_LIVE_SKIP)
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    from repark import ReparkSession

    prior = SparkSession.getActiveSession()
    warehouse = tmp_path / "spark-warehouse"
    spark = live_parity.build_spark_iceberg_engine(warehouse, catalog="wd_live").session
    assert SparkSession.getActiveSession() is spark
    if prior is not None:
        assert spark is prior
    spark.sql("CREATE NAMESPACE IF NOT EXISTS wd_live.ns")
    spark.sql(
        "CREATE TABLE wd_live.ns.defaults (id INT, name STRING)"
        " USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql("INSERT INTO wd_live.ns.defaults VALUES (1, 'a'), (2, 'b')")
    jvm = spark._jvm
    integer = jvm.org.apache.iceberg.types.Types.IntegerType.get()
    live_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, "wd_live.ns.defaults"
    )
    live_table.updateSchema().addColumn(
        "c", integer, "doc", jvm.org.apache.iceberg.expressions.Literal.of(5)
    ).commit()
    spark.sql("REFRESH TABLE wd_live.ns.defaults")
    spark.sql("INSERT INTO wd_live.ns.defaults (id, name) VALUES (3, 'c')").collect()
    live_arrow = spark.sql("SELECT id, name, c FROM wd_live.ns.defaults ORDER BY id").toArrow()
    live_cols = live_arrow.column_names
    live_rows = sorted(tuple(row[col] for col in live_cols) for row in live_arrow.to_pylist())
    assert [list(row) for row in live_rows] == [[1, "a", 5], [2, "b", 5], [3, "c", 5]]
    with pytest.raises(Exception, match="NOT_ENOUGH_DATA_COLUMNS"):
        spark.sql("INSERT INTO wd_live.ns.defaults VALUES (8, 'h')").collect()
    repark = ReparkSession.builder.appName("ice-v3-write-default-1-live").getOrCreate()
    try:
        repark.register_memory_catalog("wd_live_rp", tmp_path / "repark-warehouse")
        repark.sql("CREATE NAMESPACE wd_live_rp.ns")
        newest = sorted(
            (warehouse / "ns" / "defaults" / "metadata").glob("v*.metadata.json"),
            key=lambda path: int(path.name[1:].split(".", 1)[0]),
        )[-1]
        repark.sql(
            "CALL wd_live_rp.system.register_table("
            f"table => 'ns.defaults', metadata_file => '{newest}')"
        )
        adopted_arrow = repark.sql(
            "SELECT id, name, c FROM wd_live_rp.ns.defaults ORDER BY id"
        ).to_arrow()
        adopted_cols = adopted_arrow.column_names
        adopted = sorted(
            tuple(row[col] for col in adopted_cols) for row in adopted_arrow.to_pylist()
        )
        assert [list(row) for row in adopted] == [[1, "a", 5], [2, "b", 5], [3, "c", 5]]
        repark.sql("INSERT INTO wd_live_rp.ns.defaults (id, name) VALUES (4, 'd')").collect()
        adopted_arrow = repark.sql(
            "SELECT id, name, c FROM wd_live_rp.ns.defaults ORDER BY id"
        ).to_arrow()
        adopted_cols = adopted_arrow.column_names
        adopted = sorted(
            tuple(row[col] for col in adopted_cols) for row in adopted_arrow.to_pylist()
        )
        assert [list(row) for row in adopted] == [
            [1, "a", 5],
            [2, "b", 5],
            [3, "c", 5],
            [4, "d", 5],
        ]
        _live_rollcall(spark, repark, warehouse)
        _live_overwrite_partitions(spark, repark, warehouse)
    finally:
        repark.stop()


def _live_rollcall(spark: Any, repark: Any, warehouse: Path) -> None:
    """Both engines accept a missing nullable no-default column and write NULL."""
    spark.sql(
        "CREATE TABLE wd_live.ns.nodef (id INT, name STRING, c INT)"
        " USING iceberg TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql("INSERT INTO wd_live.ns.nodef VALUES (1, 'a', 1)")
    spark.createDataFrame([(40, "t")], "id int, name string").writeTo("wd_live.ns.nodef").append()
    spark.createDataFrame([(41, "u")], "id int, name string").write.mode("append").format(
        "iceberg"
    ).saveAsTable("wd_live.ns.nodef")
    live = spark.sql("SELECT id, name, c FROM wd_live.ns.nodef ORDER BY id").toArrow()
    live_cols = live.column_names
    assert sorted([row[col] for col in live_cols] for row in live.to_pylist()) == [
        [1, "a", 1],
        [40, "t", None],
        [41, "u", None],
    ]
    newest = sorted(
        (warehouse / "ns" / "nodef" / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )[-1]
    repark.sql(
        f"CALL wd_live_rp.system.register_table(table => 'ns.nodef', metadata_file => '{newest}')"
    )
    repark.createDataFrame([(42, "v")], "id int, name string").writeTo(
        "wd_live_rp.ns.nodef"
    ).append()
    repark.createDataFrame([(43, "w")], "id int, name string").write.mode("append").saveAsTable(
        "wd_live_rp.ns.nodef"
    )
    mirrored = repark.sql("SELECT id, name, c FROM wd_live_rp.ns.nodef ORDER BY id").to_arrow()
    mirrored_cols = mirrored.column_names
    assert sorted([row[col] for col in mirrored_cols] for row in mirrored.to_pylist()) == [
        [1, "a", 1],
        [40, "t", None],
        [41, "u", None],
        [42, "v", None],
        [43, "w", None],
    ]


def _cell_rows(shape: str) -> list[tuple[Any, ...]]:
    """The recorded full-table rows of one truth cell as driver values."""
    cell = _truth()["cells"][shape]
    assert cell["outcome"] == "ok", cell
    return [tuple(_json_value(value) for value in row) for row in cell["rows"]]


def test_dynamic_partition_named_list_fills_write_default() -> None:
    """Dynamic PARTITION with a named column list fills the omitted defaulted column."""
    session = _session("ice-v3-write-default-1-ow-dyn")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "pdflt")
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.pdflt PARTITION (id) (name) VALUES ('x')"
            ).collect()
            assert _rows(session, _CATALOG, "pdflt") == _expect("pdflt", [(None, "x", 5)])
            _cell_contains("ow_dynamic_partition_named_list_short", (None, "x", 5))
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.pdflt PARTITION (id) (id, name)"
                " SELECT 10, 'x'"
            ).collect()
            assert _rows(session, _CATALOG, "pdflt") == _expect(
                "pdflt", [(None, "x", 5), (10, "x", 5)]
            )
            _cell_contains("ow_dynamic_partition_named_list_full", (10, "x", 5))
    finally:
        session.stop()


def test_static_partition_named_list_fills_write_default() -> None:
    """Static PARTITION (k=v) with a named column list fills the omitted defaulted column."""
    session = _session("ice-v3-write-default-1-ow-static")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "pdflt")
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.pdflt PARTITION (id = 10) (name)"
                " VALUES ('x')"
            ).collect()
            assert _rows(session, _CATALOG, "pdflt") == _expect("pdflt", [(10, "x", 5)])
            _cell_contains("ow_static_partition_named_list", (10, "x", 5))
    finally:
        session.stop()


def test_partitioned_whole_table_named_list_fills_write_default() -> None:
    """A whole-table INSERT OVERWRITE column list on a partitioned table fills the default."""
    session = _session("ice-v3-write-default-1-ow-whole")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "pdflt")
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.pdflt (id, name) VALUES (10, 'x')"
            ).collect()
            assert _rows(session, _CATALOG, "pdflt") == [(10, "x", 5)]
            assert _cell_rows("ow_partitioned_no_partition_clause_named_list") == [(10, "x", 5)]
    finally:
        session.stop()


def test_overwrite_partitions_api_replaces_source_partitions() -> None:
    """``writeTo().overwritePartitions()`` writes the supplied values on a defaulted table."""
    session = _session("ice-v3-write-default-1-ow-api")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "pdflt")
            session.createDataFrame([(11, "y", 9)], "id int, name string, c int").writeTo(
                f"{_CATALOG}.{_NAMESPACE}.pdflt"
            ).overwritePartitions()
            assert _rows(session, _CATALOG, "pdflt") == _expect("pdflt", [(11, "y", 9)])
            _cell_contains("ow_partitions_api", (11, "y", 9))
    finally:
        session.stop()


def test_overwrite_default_keyword_fills_write_default() -> None:
    """The DEFAULT keyword fills on INSERT OVERWRITE in VALUES and named-list position."""
    session = _session("ice-v3-write-default-1-ow-default-kw")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "dfltow")
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.dfltow VALUES (15, 'o', DEFAULT)"
            ).collect()
            assert _rows(session, _CATALOG, "dfltow") == [(15, "o", 5)]
            assert _cell_rows("ow_values_default_kw") == [(15, "o", 5)]
            session.sql(
                f"INSERT OVERWRITE {_CATALOG}.{_NAMESPACE}.dfltow (id, name, c)"
                " SELECT 18, 'r', DEFAULT"
            ).collect()
            assert _rows(session, _CATALOG, "dfltow") == [(18, "r", 5)]
            assert _cell_rows("ow_named_list_default_kw") == [(18, "r", 5)]
    finally:
        session.stop()


def test_saveastable_overwrite_is_insert_overwrite_not_replace() -> None:
    """``saveAsTable(overwrite)`` keeps the schema and fills; Spark replaces the table (F-002)."""
    session = _session("ice-v3-write-default-1-saveas-ow")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "dfltsat")
            session.createDataFrame([(30, "s")], "id int, name string").write.mode(
                "overwrite"
            ).saveAsTable(f"{_CATALOG}.{_NAMESPACE}.dfltsat")
            assert _rows(session, _CATALOG, "dfltsat") == [(30, "s", 5)]
            assert _cell_rows("saveastable_overwrite_missing_defaulted") == [(30, "s")]
            assert _truth()["schema_after"]["dfltsat"] == [["id", "int"], ["name", "string"]]
    finally:
        session.stop()


def test_missing_nullable_no_default_accepts_and_nulls() -> None:
    """A missing nullable column with no default is accepted and written NULL on both writers."""
    session = _session("ice-v3-write-default-1-rollcall")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "nodef")
            session.createDataFrame([(40, "t")], "id int, name string").writeTo(
                f"{_CATALOG}.{_NAMESPACE}.nodef"
            ).append()
            assert _rows(session, _CATALOG, "nodef") == _expect("nodef", [(40, "t", None)])
            _cell_contains("nodef_writeto_append_missing", (40, "t", None))
            session.createDataFrame([(41, "u")], "id int, name string").write.mode(
                "append"
            ).saveAsTable(f"{_CATALOG}.{_NAMESPACE}.nodef")
            assert _rows(session, _CATALOG, "nodef") == _expect(
                "nodef", [(40, "t", None), (41, "u", None)]
            )
            _cell_contains("nodef_saveas_append_missing", (41, "u", None))
            session.createDataFrame([(42, "v", 7)], "id int, name string, c int").writeTo(
                f"{_CATALOG}.{_NAMESPACE}.nodef"
            ).append()
            assert _rows(session, _CATALOG, "nodef") == _expect(
                "nodef", [(40, "t", None), (41, "u", None), (42, "v", 7)]
            )
            _cell_contains("nodef_writeto_append_full", (42, "v", 7))
            assert _truth()["schema_after"]["nodef"] == [
                ["id", "int"],
                ["name", "string"],
                ["c", "int"],
            ]
    finally:
        session.stop()


def _live_overwrite_partitions(spark: Any, repark: Any, warehouse: Path) -> None:
    """Both engines fill the omitted defaulted column on ``overwritePartitions()``."""
    spark.sql(
        "CREATE TABLE wd_live.ns.pdflt (id INT, name STRING) USING iceberg"
        " PARTITIONED BY (id) TBLPROPERTIES ('format-version'='3')"
    )
    spark.sql("INSERT INTO wd_live.ns.pdflt VALUES (1, 'a'), (2, 'b')")
    jvm = spark._jvm
    integer = jvm.org.apache.iceberg.types.Types.IntegerType.get()
    live_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, "wd_live.ns.pdflt"
    )
    live_table.updateSchema().addColumn(
        "c", integer, "doc", jvm.org.apache.iceberg.expressions.Literal.of(5)
    ).commit()
    spark.sql("REFRESH TABLE wd_live.ns.pdflt")
    spark.createDataFrame([(12, "y")], "id INT, name STRING").writeTo(
        "wd_live.ns.pdflt"
    ).overwritePartitions()
    live = spark.sql("SELECT id, name, c FROM wd_live.ns.pdflt").toArrow()
    live_cols = live.column_names
    live_rows = sorted(([row[col] for col in live_cols] for row in live.to_pylist()), key=repr)
    recorded = _truth()["cells"]["V01_overwrite_partitions_missing_defaulted_column"]["rows"]
    assert live_rows == recorded
    newest = sorted(
        (warehouse / "ns" / "pdflt" / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )[-1]
    repark.sql(
        f"CALL wd_live_rp.system.register_table(table => 'ns.pdflt', metadata_file => '{newest}')"
    )
    repark.createDataFrame([(13, "z")], "id INT, name STRING").writeTo(
        "wd_live_rp.ns.pdflt"
    ).overwritePartitions()
    mirrored = repark.sql("SELECT id, name, c FROM wd_live_rp.ns.pdflt").to_arrow()
    mirrored_cols = mirrored.column_names
    assert sorted(
        ([row[col] for col in mirrored_cols] for row in mirrored.to_pylist()), key=repr
    ) == sorted([*recorded, [13, "z", 5]], key=repr)


def test_overwrite_partitions_api_fills_missing_defaulted_column() -> None:
    """``writeTo().overwritePartitions()`` fills an omitted defaulted column (V-01)."""
    session = _session("ice-v3-write-default-1-ow-api-fill")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "pdflt")
            session.createDataFrame([(12, "y")], "id int, name string").writeTo(
                f"{_CATALOG}.{_NAMESPACE}.pdflt"
            ).overwritePartitions()
            assert _rows(session, _CATALOG, "pdflt") == _expect("pdflt", [(12, "y", 5)])
            _cell_contains("V01_overwrite_partitions_missing_defaulted_column", (12, "y", 5))
    finally:
        session.stop()


def test_default_outside_the_insert_list_refuses() -> None:
    """``DEFAULT`` under a ``WITH`` or inside a nested query refuses as Spark does (V-02)."""
    from repark.errors import AnalysisException

    table = f"{_CATALOG}.{_NAMESPACE}.dfltow"
    shapes = {
        "V02_default_inside_cte_body": (
            f"INSERT OVERWRITE {table} WITH x AS (SELECT 18 AS id, 'r' AS name, DEFAULT AS c)"
            " SELECT * FROM x"
        ),
        "V02_default_inside_subquery": (
            f"INSERT OVERWRITE {table}"
            " SELECT * FROM (SELECT 19 AS id, 'q' AS name, DEFAULT AS c)"
        ),
        "V02_control_default_outer_select_with_cte": (
            f"INSERT OVERWRITE {table} WITH x AS (SELECT 20 AS id, 'z' AS name)"
            " SELECT id, name, DEFAULT FROM x"
        ),
    }
    session = _session("ice-v3-write-default-1-default-nested")
    try:
        with _materialize():
            _adopt(session, _CATALOG, "dfltow")
            before = _rows(session, _CATALOG, "dfltow")
            for shape, sql in shapes.items():
                _cell_errors(shape)
                assert "42703" in _truth()["cells"][shape]["message"][0]
                with pytest.raises(AnalysisException, match="(?i)default"):
                    session.sql(sql).collect()
                assert _rows(session, _CATALOG, "dfltow") == before, shape
            with pytest.raises(AnalysisException, match="UNRESOLVED_COLUMN.*`DEFAULT`.*42703"):
                session.sql(shapes["V02_control_default_outer_select_with_cte"]).collect()
            with pytest.raises(AnalysisException, match="UNRESOLVED_COLUMN.*`DEFAULT`.*42703"):
                session.sql(
                    f"INSERT INTO {table} WITH x AS (SELECT 20 AS id, 'z' AS name)"
                    " SELECT id, name, DEFAULT FROM x"
                ).collect()
            assert _rows(session, _CATALOG, "dfltow") == before
    finally:
        session.stop()


def test_mixed_static_dynamic_partition_refuses() -> None:
    """Mixed static and dynamic PARTITION keys refuse loud; Spark accepts (OPEN, Q-21b-10)."""
    from repark.errors import AnalysisException

    table = f"{_CATALOG}.{_NAMESPACE}.p2"
    session = _session("ice-v3-write-default-1-mix")
    try:
        session.sql(
            f"CREATE TABLE {table} (id INT, cat STRING, payload STRING, c INT)"
            " USING iceberg PARTITIONED BY (id, cat)"
        )
        session.sql(
            f"INSERT INTO {table} VALUES"
            " (1, 'east', 'e', 5), (1, 'west', 'w', 5), (2, 'west', 'w2', 5)"
        ).collect()
        before = _rows(session, _CATALOG, "p2")
        shapes = {
            "MIX_static_dynamic_positional": (
                f"INSERT OVERWRITE {table} PARTITION (id=1, cat) SELECT 'west', 'p', 9"
            ),
            "MIX_static_dynamic_named_list_omits_default": (
                f"INSERT OVERWRITE {table} PARTITION (id=2, cat) (cat, payload) VALUES ('west','q')"
            ),
        }
        assert _cell_rows("MIX_static_dynamic_positional") == [
            (1, "west", "p", 9),
            (2, "west", "w2", 5),
        ]
        assert _cell_rows("MIX_static_dynamic_named_list_omits_default") == [
            (1, "west", "p", 9),
            (2, "west", "q", 5),
        ]
        for shape, sql in shapes.items():
            with pytest.raises(AnalysisException, match="cannot mix static assignments"):
                session.sql(sql).collect()
            assert _rows(session, _CATALOG, "p2") == before, shape
    finally:
        session.stop()

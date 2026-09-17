"""ICE-COLUMN-REORDER-1 — ALTER COLUMN FIRST/AFTER column-move pins.

Offline tier (JVM-free): RePark tables vs the checked-in Spark truth on the facade
SQL door and the native door. Live tier (REPARK_PARITY_LIVE=1): Spark replays the
truth (drift detector) and the engines cross-read each other's moved tables.

pins: ice-column-reorder-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _live_parity as lp
import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException

_HERE = Path(__file__).resolve().parent
_TRUTH: dict[str, Any] = json.loads(
    (_HERE / "test_ice_column_reorder_1_truth.json").read_text(encoding="utf-8")
)
_LIVE = lp.LIVE
_LIVE_SKIP = lp.LIVE_SKIP_REASON
_NAMESPACE = "ns"

_PLAIN_DDL = "CREATE TABLE {t} (id INT, a STRING, b STRING) USING iceberg"
_PLAIN_SEED = "INSERT INTO {t} VALUES (1, 'a1', 'b1')"
_NESTED_DDL = "CREATE TABLE {t} (id INT, s STRUCT<a: INT, b: STRING>) USING iceberg"
_NESTED_SEED = "INSERT INTO {t} VALUES (1, named_struct('a', 1, 'b', 'x'))"


def _catalog(test_name: str) -> str:
    """Per-test catalog name so moves never leak across tests."""
    return f"mv_{test_name}"


def _table_root(warehouse: Path, catalog: str, table: str) -> Path:
    """Memory-catalog table root for a moved table."""
    return warehouse / "repark_ctas" / catalog / _NAMESPACE / table


def _metadata_file_count(table_root: Path) -> int:
    """Number of metadata documents (a no-op move must not mint one)."""
    return len(list((table_root / "metadata").glob("*.metadata.json")))


def _flat_ids(fields: list[dict[str, Any]], prefix: str = "") -> list[tuple[int, str]]:
    """(id, dotted-name) pairs in order, descending into structs."""
    out: list[tuple[int, str]] = []
    for field in fields:
        name = f"{prefix}{field['name']}"
        out.append((field["id"], name))
        nested = field["type"]
        if isinstance(nested, dict) and nested.get("type") == "struct":
            out.extend(_flat_ids(nested["fields"], prefix=f"{name}."))
    return out


def _current_order_ids(table_root: Path) -> tuple[int, list[tuple[int, str]]]:
    """Current schema id plus ordered (id, dotted-name) pairs."""
    doc = _newest_metadata(table_root)
    current = doc["current-schema-id"]
    schema = next(item for item in doc["schemas"] if item["schema-id"] == current)
    return current, _flat_ids(schema["fields"])


def _truth_flat_ids(fields: list[dict[str, Any]]) -> list[tuple[int, str]]:
    """Ordered (id, dotted-name) pairs from a truth snapshot."""
    return _flat_ids(fields)


def _fresh_facade(
    tmp_path: Path, test_name: str, table: str, ddl: str, seed: str
) -> tuple[ReparkSession, Path]:
    """Facade session plus seeded table; caller stops the session."""
    catalog = _catalog(test_name)
    warehouse = tmp_path / "wh"
    session = ReparkSession.builder.appName(f"reorder-{test_name}").getOrCreate()
    session.register_memory_catalog(catalog, warehouse)
    session.sql(f"CREATE NAMESPACE {catalog}.{_NAMESPACE}")
    session.sql(ddl.format(t=f"{catalog}.{_NAMESPACE}.{table}"))
    session.sql(seed.format(t=f"{catalog}.{_NAMESPACE}.{table}"))
    return session, warehouse


def _assert_matches_truth_snapshot(
    session: ReparkSession, qualified: str, table_root: Path, snapshot: dict[str, Any]
) -> None:
    """Schema ids/order, SELECT * order and rows equal the recorded Spark answer."""
    schema_id, order_ids = _current_order_ids(table_root)
    assert schema_id == snapshot["schema_id"], (schema_id, snapshot["schema_id"])
    assert order_ids == _truth_flat_ids(snapshot["fields"]), (order_ids, snapshot["fields"])
    arrow = session.sql(f"SELECT * FROM {qualified} ORDER BY id").to_arrow()
    assert arrow.schema.names == snapshot["select_columns"], arrow.schema.names
    columns = {name: arrow.column(name).to_pylist() for name in arrow.schema.names}
    for position, name in enumerate(snapshot["select_columns"]):
        expected = [row[position] for row in snapshot["rows"]]
        assert columns[name] == expected, (name, columns[name], expected)


def _assert_native_matches_truth_snapshot(
    qualified: str, table_root: Path, snapshot: dict[str, Any]
) -> None:
    """Native-door SELECT * order and rows equal the recorded Spark answer."""
    arrow = repark.sql(f"SELECT * FROM {qualified} ORDER BY id").to_arrow()
    assert arrow.schema.names == snapshot["select_columns"], arrow.schema.names
    columns = {name: arrow.column(name).to_pylist() for name in arrow.schema.names}
    for position, name in enumerate(snapshot["select_columns"]):
        expected = [row[position] for row in snapshot["rows"]]
        assert columns[name] == expected, (name, columns[name], expected)
    schema_id, order_ids = _current_order_ids(table_root)
    assert schema_id == snapshot["schema_id"]
    assert order_ids == _truth_flat_ids(snapshot["fields"])


def test_move_first_matches_oracle(tmp_path: Path) -> None:
    """ALTER COLUMN b FIRST reorders with ids intact on both doors."""
    case = _TRUTH["cases"]["first_v2"]
    session, warehouse = _fresh_facade(tmp_path, "first", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("first")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b FIRST")
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
    finally:
        session.stop()
    session2, warehouse2 = _fresh_facade(tmp_path, "first_native", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog2 = _catalog("first_native")
        qualified2 = f"{catalog2}.{_NAMESPACE}.t"
        repark.sql(f"ALTER TABLE {qualified2} ALTER COLUMN b FIRST").to_arrow()
        _assert_native_matches_truth_snapshot(qualified2, _table_root(warehouse2, catalog2, "t"), case["after"])
    finally:
        session2.stop()


def test_move_after_matches_oracle(tmp_path: Path) -> None:
    """ALTER COLUMN b AFTER id reorders with ids intact on both doors."""
    case = _TRUTH["cases"]["after_v2"]
    session, warehouse = _fresh_facade(tmp_path, "after", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("after")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b AFTER id")
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
    finally:
        session.stop()
    session2, warehouse2 = _fresh_facade(tmp_path, "after_native", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog2 = _catalog("after_native")
        qualified2 = f"{catalog2}.{_NAMESPACE}.t"
        repark.sql(f"ALTER TABLE {qualified2} ALTER COLUMN b AFTER id").to_arrow()
        _assert_native_matches_truth_snapshot(qualified2, _table_root(warehouse2, catalog2, "t"), case["after"])
    finally:
        session2.stop()


def test_noop_moves_commit_nothing(tmp_path: Path) -> None:
    """Moves to the current position mint no schema and no metadata file."""
    for name, statement in [
        ("noop_first", "ALTER TABLE {t} ALTER COLUMN id FIRST"),
        ("noop_after", "ALTER TABLE {t} ALTER COLUMN a AFTER id"),
    ]:
        case = _TRUTH["cases"][f"{name}_v2"]
        assert case["after"]["schema_id"] == 0
        session, warehouse = _fresh_facade(tmp_path, name, "t", _PLAIN_DDL, _PLAIN_SEED)
        try:
            catalog = _catalog(name)
            qualified = f"{catalog}.{_NAMESPACE}.t"
            root = _table_root(warehouse, catalog, "t")
            files_before = _metadata_file_count(root)
            session.sql(statement.format(t=qualified))
            assert _metadata_file_count(root) == files_before
            _assert_matches_truth_snapshot(session, qualified, root, case["after"])
        finally:
            session.stop()


def test_first_after_last_matches_oracle(tmp_path: Path) -> None:
    """ALTER COLUMN id AFTER b moves the first column last."""
    case = _TRUTH["cases"]["first_after_last_v2"]
    session, warehouse = _fresh_facade(tmp_path, "last", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("last")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN id AFTER b")
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
    finally:
        session.stop()


def test_self_move_refuses(tmp_path: Path) -> None:
    """ALTER COLUMN b AFTER b refuses and leaves the table untouched."""
    truth_error = _TRUTH["cases"]["self_v2"]["error"]
    assert truth_error["java_class"] == "org.apache.spark.SparkException"
    session, warehouse = _fresh_facade(tmp_path, "self", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("self")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        root = _table_root(warehouse, catalog, "t")
        files_before = _metadata_file_count(root)
        with pytest.raises(PySparkException) as caught:
            session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b AFTER b")
        assert "Cannot move b after itself" in str(caught.value)
        assert _metadata_file_count(root) == files_before
        schema_id, order_ids = _current_order_ids(root)
        assert schema_id == 0
        assert [name for _, name in order_ids] == ["id", "a", "b"]
    finally:
        session.stop()


def test_after_unknown_column_refuses(tmp_path: Path) -> None:
    """AFTER an unknown column raises Spark's UNRESOLVED_COLUMN on both doors."""
    truth_error = _TRUTH["cases"]["badref_v2"]["error"]
    assert truth_error["python_class"] == "AnalysisException"
    session, warehouse = _fresh_facade(tmp_path, "badref", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("badref")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        with pytest.raises(AnalysisException) as caught:
            session.sql(f"ALTER TABLE {qualified} ALTER COLUMN a AFTER nope")
        message = str(caught.value)
        assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in message
        assert "`nope`" in message
        assert "SQLSTATE: 42703" in message
    finally:
        session.stop()
    session2, _ = _fresh_facade(tmp_path, "badref_native", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        qualified2 = f"{_catalog('badref_native')}.{_NAMESPACE}.t"
        with pytest.raises(AnalysisException) as caught2:
            repark.sql(f"ALTER TABLE {qualified2} ALTER COLUMN a AFTER nope").to_arrow()
        assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(caught2.value)
    finally:
        session2.stop()


def test_move_unknown_column_refuses(tmp_path: Path) -> None:
    """Moving an unknown column raises Spark's UNRESOLVED_COLUMN on both doors."""
    truth_error = _TRUTH["cases"]["badcol_v2"]["error"]
    assert truth_error["python_class"] == "AnalysisException"
    session, warehouse = _fresh_facade(tmp_path, "badcol", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("badcol")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        with pytest.raises(AnalysisException) as caught:
            session.sql(f"ALTER TABLE {qualified} ALTER COLUMN nope FIRST")
        message = str(caught.value)
        assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in message
        assert "`nope`" in message
        assert "SQLSTATE: 42703" in message
    finally:
        session.stop()
    session2, _ = _fresh_facade(tmp_path, "badcol_native", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        qualified2 = f"{_catalog('badcol_native')}.{_NAMESPACE}.t"
        with pytest.raises(AnalysisException) as caught2:
            repark.sql(f"ALTER TABLE {qualified2} ALTER COLUMN nope FIRST").to_arrow()
        assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(caught2.value)
    finally:
        session2.stop()


def test_nested_field_move_matches_oracle(tmp_path: Path) -> None:
    """ALTER COLUMN s.b FIRST reorders inside the struct with nested ids intact."""
    case = _TRUTH["cases"]["nested_v2"]
    before_ids = _truth_flat_ids(case["before"]["fields"])
    after_ids = _truth_flat_ids(case["after"]["fields"])
    assert sorted(before_ids) == sorted(after_ids)
    assert before_ids != after_ids
    session, warehouse = _fresh_facade(tmp_path, "nested", "t", _NESTED_DDL, _NESTED_SEED)
    try:
        catalog = _catalog("nested")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN s.b FIRST")
        root = _table_root(warehouse, catalog, "t")
        schema_id, order_ids = _current_order_ids(root)
        assert schema_id == case["after"]["schema_id"] == 1
        assert order_ids == after_ids
        arrow = session.sql(f"SELECT * FROM {qualified}").to_arrow()
        assert arrow.schema.names == ["id", "s"]
        struct_type = arrow.schema.field("s").type
        assert struct_type.names == ["b", "a"], struct_type
    finally:
        session.stop()


def test_positional_insert_and_select_after_move(tmp_path: Path) -> None:
    """INSERT in the new positional order lands rows both doors read back."""
    case = _TRUTH["cases"]["insert_v2"]
    session, warehouse = _fresh_facade(tmp_path, "insert", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("insert")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b FIRST")
        session.sql(f"INSERT INTO {qualified} VALUES ('b2', 2, 'a2')")
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
        native_arrow = repark.sql(f"SELECT * FROM {qualified} ORDER BY id").to_arrow()
        assert native_arrow.schema.names == case["after"]["select_columns"]
        assert native_arrow.column("b").to_pylist() == ["b1", "b2"]
    finally:
        session.stop()


def test_move_on_v3_matches_oracle(tmp_path: Path) -> None:
    """ALTER COLUMN b FIRST works on a format-v3 table."""
    case = _TRUTH["cases"]["first_v3"]
    ddl = _PLAIN_DDL + " TBLPROPERTIES ('format-version'='3')"
    session, warehouse = _fresh_facade(tmp_path, "v3", "t", ddl, _PLAIN_SEED)
    try:
        catalog = _catalog("v3")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b FIRST")
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
    finally:
        session.stop()


def test_move_partition_source_matches_oracle(tmp_path: Path) -> None:
    """Moving a partition-source column keeps reads and positional writes working."""
    case = _TRUTH["cases"]["part_v2"]
    ddl = (
        "CREATE TABLE {t} (id INT, a STRING, b STRING) "
        "USING iceberg PARTITIONED BY (b)"
    )
    session, warehouse = _fresh_facade(tmp_path, "part", "t", ddl, _PLAIN_SEED)
    try:
        catalog = _catalog("part")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b FIRST")
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
        session.sql(f"INSERT INTO {qualified} VALUES ('b3', 3, 'a3')")
        arrow = session.sql(f"SELECT * FROM {qualified} ORDER BY id").to_arrow()
        assert arrow.column("b").to_pylist() == ["b1", "b3"]
    finally:
        session.stop()


def test_dataframe_door_columns_and_append(tmp_path: Path) -> None:
    """table(t).columns follows the move; writeTo appends by name."""
    case = _TRUTH["cases"]["df_v2"]
    session, warehouse = _fresh_facade(tmp_path, "df", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        catalog = _catalog("df")
        qualified = f"{catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b AFTER id")
        assert session.table(qualified).columns == case["columns"]
        frame = session.createDataFrame([(9, "b9", "a9")], ["id", "b", "a"])
        frame.writeTo(qualified).append()
        _assert_matches_truth_snapshot(session, qualified, _table_root(warehouse, catalog, "t"), case["after"])
    finally:
        session.stop()


def _live_catalog(test_name: str) -> str:
    """Private live catalog per test (a catalog binds one warehouse for the session)."""
    return f"mvlive_{test_name}"


def _live_engine(tmp_path: Path, test_name: str) -> tuple[Any, Path]:
    """Live Spark Iceberg engine on a private catalog and warehouse; never stopped."""
    catalog = _live_catalog(test_name)
    warehouse = tmp_path / "spark-wh"
    engine = lp.build_spark_iceberg_engine(warehouse, catalog=catalog)
    engine.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{_NAMESPACE}")
    return engine, warehouse


def _live_fresh(engine: lp.Engine, catalog: str, table: str, ddl: str, seed: str) -> str:
    """Recreate one live table; return its qualified name."""
    qualified = f"{catalog}.{_NAMESPACE}.{table}"
    engine.session.sql(f"DROP TABLE IF EXISTS {qualified}")
    engine.session.sql(ddl.format(t=qualified))
    engine.session.sql(seed.format(t=qualified))
    return qualified


def _live_snapshot(engine: lp.Engine, qualified: str) -> dict[str, Any]:
    """Column order and ordered rows from the live engine."""
    arrow = engine.arrow_of(engine.session.sql(f"SELECT * FROM {qualified} ORDER BY id"))
    names = arrow.schema.names
    columns = [arrow.column(name).to_pylist() for name in names]
    return {
        "select_columns": names,
        "rows": [list(row) for row in zip(*columns, strict=True)],
    }


def _live_case_ddl(case_name: str) -> tuple[str, str]:
    """Recorded DDL and seed for one replay case."""
    shapes = _TRUTH["shapes"]
    nested = "nested" in case_name or "struct" in case_name
    ddl = shapes["nested_ddl"] if nested else shapes["plain_ddl"]
    seed = shapes["nested_seed"] if nested else shapes["plain_seed"]
    if case_name == "first_v3":
        ddl += " TBLPROPERTIES ('format-version'='3')"
    if case_name == "part_v2":
        ddl = (
            "CREATE TABLE {t} (id INT, a STRING, b STRING) "
            "USING iceberg PARTITIONED BY (b)"
        )
    return ddl, seed


_LIVE_MOVE_CASES = [
    "first_v2",
    "after_v2",
    "noop_first_v2",
    "noop_after_v2",
    "first_after_last_v2",
    "nested_v2",
    "struct_top_v2",
    "first_v3",
    "part_v2",
]


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
@pytest.mark.parametrize("case_name", _LIVE_MOVE_CASES)
def test_live_oracle_replays_truth(tmp_path: Path, case_name: str) -> None:
    """Live Spark re-derives the recorded move answer (oracle-drift detector)."""
    case = _TRUTH["cases"][case_name]
    engine, spark_warehouse = _live_engine(tmp_path, f"replay_{case_name}")
    catalog = _live_catalog(f"replay_{case_name}")
    ddl, seed = _live_case_ddl(case_name)
    qualified = _live_fresh(engine, catalog, "t", ddl, seed)
    engine.session.sql(case["statement"].format(t=qualified))
    after = case["after"]
    live = _live_snapshot(engine, qualified)
    assert live["select_columns"] == after["select_columns"]
    assert live["rows"] == after["rows"]
    live_meta = max(
        (spark_warehouse / _NAMESPACE / "t" / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    live_doc = json.loads(live_meta.read_text(encoding="utf-8"))
    assert live_doc["current-schema-id"] == after["schema_id"]
    engine.session.sql(f"DROP TABLE {qualified}")


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
@pytest.mark.parametrize("case_name", ["self_v2", "badref_v2", "badcol_v2"])
def test_live_oracle_refusals_match_truth(tmp_path: Path, case_name: str) -> None:
    """Live Spark re-raises the recorded refusal class and message."""
    case = _TRUTH["cases"][case_name]
    engine, _ = _live_engine(tmp_path, f"refuse_{case_name}")
    catalog = _live_catalog(f"refuse_{case_name}")
    shapes = _TRUTH["shapes"]
    qualified = _live_fresh(engine, catalog, "t", shapes["plain_ddl"], shapes["plain_seed"])
    with pytest.raises(Exception) as caught:
        engine.session.sql(case["statement"].format(t=qualified))
    assert type(caught.value).__name__ == case["error"]["python_class"]
    assert case["error"]["message_first_line"] in str(caught.value)
    engine.session.sql(f"DROP TABLE {qualified}")


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_cross_read_moved_tables(tmp_path: Path) -> None:
    """Spark reads RePark's moved table; RePark reads Spark's moved table."""
    engine, spark_warehouse = _live_engine(tmp_path, "xread")
    spark = engine.session
    catalog = _live_catalog("xread")
    session, warehouse = _fresh_facade(tmp_path, "xlive", "t", _PLAIN_DDL, _PLAIN_SEED)
    try:
        repark_catalog = _catalog("xlive")
        qualified = f"{repark_catalog}.{_NAMESPACE}.t"
        session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b FIRST")
        session.sql(f"INSERT INTO {qualified} VALUES ('b2', 2, 'a2')")
        root = _table_root(warehouse, repark_catalog, "t")
        newest = max(
            (root / "metadata").glob("*.metadata.json"),
            key=lambda path: path.stat().st_mtime_ns,
        )
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{_NAMESPACE}.xrepark")
        spark.sql(
            f"CALL {catalog}.system.register_table("
            f"table => '{_NAMESPACE}.xrepark', metadata_file => '{newest}')"
        )
        live = _live_snapshot(engine, f"{catalog}.{_NAMESPACE}.xrepark")
        assert live["select_columns"] == ["b", "id", "a"]
        assert live["rows"] == [["b1", 1, "a1"], ["b2", 2, "a2"]]
        spark.sql(f"DROP TABLE {catalog}.{_NAMESPACE}.xrepark")
    finally:
        session.stop()
    spark_table = _live_fresh(engine, catalog, "xspark", _PLAIN_DDL, _PLAIN_SEED)
    spark.sql(f"ALTER TABLE {spark_table} ALTER COLUMN b FIRST")
    spark_meta = max(
        (spark_warehouse / _NAMESPACE / "xspark" / "metadata").glob("v*.metadata.json"),
        key=lambda path: int(path.name[1:].split(".", 1)[0]),
    )
    repark2, _ = _fresh_facade(tmp_path, "xlive2", "t2", _PLAIN_DDL, _PLAIN_SEED)
    try:
        repark_catalog2 = _catalog("xlive2")
        repark2.sql(
            f"CALL {repark_catalog2}.system.register_table("
            f"table => '{_NAMESPACE}.xspark', metadata_file => '{spark_meta}')"
        )
        back = repark2.sql(
            f"SELECT * FROM {repark_catalog2}.{_NAMESPACE}.xspark ORDER BY id"
        ).to_arrow()
        assert back.schema.names == ["b", "id", "a"]
        assert back.column("b").to_pylist() == ["b1"]
    finally:
        repark2.stop()
    spark.sql(f"DROP TABLE {spark_table}")


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_dataframe_door_matches_truth(tmp_path: Path) -> None:
    """Live df.columns and writeTo-append replay the recorded DataFrame leg."""
    case = _TRUTH["cases"]["df_v2"]
    engine, _ = _live_engine(tmp_path, "df")
    catalog = _live_catalog("df")
    shapes = _TRUTH["shapes"]
    qualified = _live_fresh(engine, catalog, "t", shapes["plain_ddl"], shapes["plain_seed"])
    engine.session.sql(f"ALTER TABLE {qualified} ALTER COLUMN b AFTER id")
    assert engine.session.table(qualified).columns == case["columns"]
    engine.session.createDataFrame([(9, "b9", "a9")], ["id", "b", "a"]).writeTo(
        qualified
    ).append()
    live = _live_snapshot(engine, qualified)
    assert live["select_columns"] == case["after"]["select_columns"]
    assert live["rows"] == case["after"]["rows"]
    engine.session.sql(f"DROP TABLE {qualified}")

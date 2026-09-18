"""ICE-NESTED-EVO-1 — adopt a Spark-evolved nested table, and nested DDL, against Spark 4.1.2.

Every cell reads its expected answer from the recorded
`fixtures/torture/data/ice_nested_evo_1/oracle.json`. The adoption cells copy the committed
Spark-written tables to their baked root, `register_table` them, and read them on the SQL door
and the DataFrame door. The DDL cells replay Spark's own statements on RePark-created tables.

Cells whose id starts with `fork292` read a data file that lacks a nested child the table schema
has. They need fork PR #292 (F-NESTED-EVO-1), in the workspace fork pin since RP-25.

pins: ice-nested-evo-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
pins: ice-nested-evo-1/C-010, C-011, C-012, C-013
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import _record_ice_nested_evo_1 as recorder
import pyarrow as pa
import pytest

_ORACLE: dict[str, Any] = json.loads(recorder.ORACLE_FILE.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]
_ADOPTED_CATALOG = "ice_nested_evo_1_adopted"
_REQUIRED_CHILD_MESSAGE = _CELLS["v2_add_required_nested_child"]["error"]["message"]
_SPARK_SIMPLE_TYPES = {
    "integer": "int",
    "long": "bigint",
    "string": "string",
    "short": "smallint",
    "byte": "tinyint",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "date": "date",
    "binary": "binary",
}
_ARROW_SIMPLE_TYPES = {
    "int32": "int",
    "int64": "bigint",
    "string": "string",
    "large_string": "string",
    "string_view": "string",
    "int16": "smallint",
    "int8": "tinyint",
    "double": "double",
    "float": "float",
    "bool": "boolean",
    "date32": "date",
    "binary": "binary",
    "large_binary": "binary",
}
_ADOPTED_READS: tuple[tuple[str, str, str], ...] = (
    ("st_add_v2", "v2_struct_child_add_read", "struct"),
    ("st_add_v2", "v2_struct_child_add_leaf_read", "leaf"),
    ("st_add_v2", "v2_struct_child_add_filter_null", "filter_null"),
    ("st_add_v3", "v3_struct_child_add_read", "struct"),
    ("st_add_v3", "v3_struct_child_add_leaf_read", "leaf"),
    ("st_add_v3", "v3_struct_child_add_filter_null", "filter_null"),
    ("list_add_v3", "v3_list_element_child_add_read", "list"),
    ("map_add_v3", "v3_map_value_child_add_read", "map"),
)
_READ_SHAPES = {
    "SELECT id, s": "struct",
    "SELECT id, arrs": "list",
    "SELECT id, m": "map",
    "SELECT id, s.a, s.b": "leaf",
}
_FORK_292_DDL_LABELS = frozenset(
    {
        "struct_child_add_read",
        "struct_child_add_leaf_read",
        "struct_child_add_filter_null",
        "list_element_child_add_read",
        "map_value_child_add_read",
        "nested_create_then_add",
        "add_required_nested_child",
    }
)
_FORK_WRITE_DDL_LABELS = frozenset({"list_element_child_add_read"})
_FORK_WRITE_REASON = (
    "fork finding: an INSERT into a list column fails in the fork writer "
    "(`column types must match schema types … PARQUET:field_id`)"
)
_DDL_LABELS: tuple[str, ...] = tuple(
    label.removeprefix("v2_") for label in _CELLS if label.startswith("v2_")
)


class _DirLock:
    """Cross-process lock so concurrent facade workers do not clobber the baked table root."""

    def __init__(self, path: Path) -> None:
        """Take the lock by creating `path`, waiting up to two minutes.

        Args:
            path: The lock directory.
        """
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
        """Release the lock."""
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialized_adopted_tables() -> Iterator[Path]:
    """Copy the four committed Spark tables to their baked root and remove them on exit."""
    root = recorder.ADOPTED_WAREHOUSE
    lock = _DirLock(Path(str(root) + ".lock"))
    try:
        if root.exists():
            shutil.rmtree(root)
        for table in recorder.ADOPTED_TABLES:
            shutil.copytree(
                recorder.FIXTURE_DIR / table,
                root / recorder.ADOPTED_NAMESPACE / table,
                copy_function=shutil.copy,
            )
        yield root
    finally:
        with suppress(OSError):
            if root.exists():
                shutil.rmtree(root)
        lock.close()


def _session() -> Any:
    """Return the facade session with format-version 3 CREATE allowed."""
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("ice-nested-evo-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )


def _spark_type(data_type: Any) -> str:
    """Render a Spark schema-JSON data type as Spark's simple string."""
    if isinstance(data_type, str):
        return _SPARK_SIMPLE_TYPES.get(data_type, data_type)
    kind = data_type["type"]
    if kind == "struct":
        children = ",".join(
            f"{field['name']}:{_spark_type(field['type'])}" for field in data_type["fields"]
        )
        return f"struct<{children}>"
    if kind == "array":
        return f"array<{_spark_type(data_type['elementType'])}>"
    if kind == "map":
        return f"map<{_spark_type(data_type['keyType'])},{_spark_type(data_type['valueType'])}>"
    raise AssertionError(f"unmapped Spark type {data_type}")


def _arrow_type(data_type: pa.DataType) -> str:
    """Render an Arrow data type as Spark's simple string."""
    if pa.types.is_struct(data_type):
        children = ",".join(
            f"{data_type.field(index).name}:{_arrow_type(data_type.field(index).type)}"
            for index in range(data_type.num_fields)
        )
        return f"struct<{children}>"
    if pa.types.is_map(data_type):
        return f"map<{_arrow_type(data_type.key_type)},{_arrow_type(data_type.item_type)}>"
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return f"array<{_arrow_type(data_type.value_type)}>"
    return _ARROW_SIMPLE_TYPES.get(str(data_type), str(data_type))


def _python_value(value: Any, data_type: pa.DataType) -> Any:
    """Convert one `to_pylist` value so maps compare as dicts, the way PySpark returns them."""
    if value is None:
        return None
    if pa.types.is_map(data_type):
        return {key: _python_value(item, data_type.item_type) for key, item in value}
    if pa.types.is_struct(data_type):
        return {
            data_type.field(index).name: _python_value(
                value[data_type.field(index).name], data_type.field(index).type
            )
            for index in range(data_type.num_fields)
        }
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return [_python_value(item, data_type.value_type) for item in value]
    return value


def _answer_of(table: pa.Table) -> dict[str, Any]:
    """Return the rows (as PySpark dicts), column names and simple-string types of a result."""
    rows = [
        {field.name: _python_value(row[field.name], field.type) for field in table.schema}
        for row in table.to_pylist()
    ]
    return {
        "columns": list(table.column_names),
        "types": [_arrow_type(field.type) for field in table.schema],
        "rows": rows,
    }


def _expected_of(cell: dict[str, Any]) -> dict[str, Any]:
    """Return the oracle cell's rows, column names and simple-string types."""
    fields = cell["schema"]["fields"]
    return {
        "columns": [field["name"] for field in fields],
        "types": [_spark_type(field["type"]) for field in fields],
        "rows": cell["rows"],
    }


def _aliased_leaf_read(sql: str) -> str:
    """Alias the unaliased nested projection `s.a, s.b` to the leaf names Spark reports.

    Notes:
        RePark names an unaliased `s.a` projection `<table>.s[a]`; Spark names it `a`. That
        default-name divergence is registry row EX-COL-2 (BACKLOG) and is pinned on its own in
        `test_unaliased_nested_projection_names_like_spark`. Spark names `s.a AS a` `a` too, so
        the aliased read compares against the same recorded answer.
    """
    return sql.replace("SELECT id, s.a, s.b FROM", "SELECT id, s.a AS a, s.b AS b FROM")


def _retargeted(sql: str, catalog: str) -> str:
    """Point one recorded statement at a RePark catalog instead of Spark's `sc`."""
    return sql.replace(f"{recorder.SPARK_CATALOG}.{recorder.ADOPTED_NAMESPACE}.", f"{catalog}.ns.")


def _dataframe_read(frame: Any, shape: str) -> Any:
    """Build the DataFrame-door twin of one recorded read.

    Notes:
        Nested fields use `getField(...).alias(...)`: the dotted `col("s.a")` spelling is
        registry row COL-DOTTED-FIELD-1 (BACKLOG), outside this unit.
    """
    from repark.spark.sql import functions

    if shape in ("struct", "list", "map"):
        value_column = {"struct": "s", "list": "arrs", "map": "m"}[shape]
        return frame.select("id", value_column).orderBy("id")
    if shape == "leaf":
        struct = functions.col("s")
        return frame.select(
            "id", struct.getField("a").alias("a"), struct.getField("b").alias("b")
        ).orderBy("id")
    if shape == "filter_null":
        return frame.filter(functions.col("s").getField("b").isNull()).select("id").orderBy("id")
    raise AssertionError(f"unknown read shape {shape}")


def _adopted_id(table: str, label: str) -> str:
    """Return the test id for one adoption read, marked `fork292`."""
    return f"fork292-{label}-{table}"


@pytest.mark.parametrize(
    ("table", "label", "shape"),
    [
        pytest.param(table, label, shape, id=_adopted_id(table, label))
        for table, label, shape in _ADOPTED_READS
    ],
)
def test_adopted_spark_table_reads_match_spark(table: str, label: str, shape: str) -> None:
    """An adopted Spark table whose nested struct gained a child answers Spark on both doors."""
    spark = _session()
    expected = _expected_of(_CELLS[label])
    with _materialized_adopted_tables() as root:
        catalog = f"{_ADOPTED_CATALOG}_{table}"
        spark.register_memory_catalog(catalog, root)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
        metadata = root / recorder.ADOPTED_NAMESPACE / table / "metadata/v4.metadata.json"
        spark.sql(
            f"CALL {catalog}.system.register_table("
            f"table => 'ns.{table}', metadata_file => '{metadata}')"
        )
        query = _aliased_leaf_read(_retargeted(_CELLS[label]["query"], catalog))
        sql_answer = _answer_of(spark.sql(query).toArrow())
        frame_answer = _answer_of(
            _dataframe_read(spark.table(f"{catalog}.ns.{table}"), shape).toArrow()
        )
    assert sql_answer == expected, (label, "sql", sql_answer, expected)
    assert frame_answer == expected, (label, "dataframe", frame_answer, expected)


def _metadata_files(warehouse: Path) -> list[str]:
    """Return every metadata JSON file name under a warehouse."""
    return sorted(str(path) for path in warehouse.rglob("*.metadata.json"))


def _run_statements(spark: Any, statements: list[str]) -> Exception | None:
    """Run statements in order and return the first exception, or `None`."""
    for statement in statements:
        try:
            spark.sql(statement).collect()
        except Exception as exc:
            return exc
    return None


def _recorded_query(format_version: str, label: str) -> str | None:
    """Return the read the recorder attached to a cell, including one Spark never ran."""
    for recorded_label, _statements, query in recorder.build_cells(format_version):
        if recorded_label == label:
            return query
    return None


def _replay_format_version(
    spark: Any, format_version: str, warehouse: Path, with_inserts: bool
) -> dict[str, Any]:
    """Replay every recorded cell of one format version on a fresh RePark catalog.

    Args:
        spark: The facade session.
        format_version: `"2"` or `"3"`.
        warehouse: A fresh warehouse directory.
        with_inserts: `False` skips every `INSERT`, so the replay measures the DDL alone.

    Returns:
        One outcome per label without its `vN_` prefix: the statement error, the SQL answer,
        the DataFrame answer where the cell reads a whole table, and the metadata files
        before and after the statements.
    """
    catalog = f"ice_nested_evo_1_v{format_version}_{warehouse.name.replace('-', '_')}"
    spark.register_memory_catalog(catalog, warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    outcomes: dict[str, Any] = {}
    for label, cell in _CELLS.items():
        if not label.startswith(f"v{format_version}_"):
            continue
        before = _metadata_files(warehouse)
        statements = [
            _retargeted(statement, catalog)
            for statement in cell["statements"]
            if with_inserts or not statement.startswith("INSERT")
        ]
        error = _run_statements(spark, statements)
        outcome: dict[str, Any] = {
            "error": error,
            "before": before,
            "after": _metadata_files(warehouse),
        }
        query = cell.get("query") or _recorded_query(format_version, label)
        if query is not None:
            try:
                outcome["sql"] = _answer_of(
                    spark.sql(_aliased_leaf_read(_retargeted(query, catalog))).toArrow()
                )
            except Exception as exc:
                outcome["sql_error"] = exc
            shape = _READ_SHAPES.get(query.split(" FROM ")[0])
            if shape is not None:
                table = _retargeted(query, catalog).split(" FROM ")[1].split(" ")[0]
                try:
                    outcome["dataframe"] = _answer_of(
                        _dataframe_read(spark.table(table), shape).toArrow()
                    )
                except Exception as exc:
                    outcome["dataframe_error"] = exc
        outcomes[label.removeprefix(f"v{format_version}_")] = outcome
    return outcomes


_REPLAYS: dict[tuple[str, bool], dict[str, Any]] = {}


def _replayed(
    format_version: str, with_inserts: bool, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, Any]:
    """Replay one format version once per worker and mode, and cache the outcomes."""
    key = (format_version, with_inserts)
    if key not in _REPLAYS:
        mode = "rows" if with_inserts else "ddl"
        warehouse = tmp_path_factory.mktemp(f"nested-{mode}-v{format_version}")
        _REPLAYS[key] = _replay_format_version(_session(), format_version, warehouse, with_inserts)
    return _REPLAYS[key]


def _ddl_params() -> list[Any]:
    """Return every `(label, format_version)` replay cell except the required-child refusal."""
    return [
        pytest.param(label, format_version, id=f"{label}-v{format_version}")
        for format_version in recorder.FORMAT_VERSIONS
        for label in _DDL_LABELS
        if label != "add_required_nested_child"
    ]


def _rows_param(label: str, format_version: str) -> Any:
    """Return one row cell, marked `fork292` and `forkwrite` where each fork change is needed."""
    marker = "fork292-" if label in _FORK_292_DDL_LABELS else ""
    if label in _FORK_WRITE_DDL_LABELS:
        return pytest.param(
            label,
            format_version,
            id=f"forkwrite-{marker}{label}-v{format_version}",
            marks=pytest.mark.xfail(strict=True, reason=_FORK_WRITE_REASON),
        )
    return pytest.param(label, format_version, id=f"{marker}{label}-v{format_version}")


def _rows_params() -> list[Any]:
    """Return every replay cell that reads rows."""
    return [
        _rows_param(label, format_version)
        for format_version in recorder.FORMAT_VERSIONS
        for label in _DDL_LABELS
        if label != "add_required_nested_child" and not label.endswith("_describe")
    ]


def _describe_types(answer: dict[str, Any]) -> list[dict[str, str]]:
    """Return the `(col_name, data_type)` pairs of a DESCRIBE answer."""
    return [{"col_name": row["col_name"], "data_type": row["data_type"]} for row in answer["rows"]]


@pytest.mark.parametrize(("label", "format_version"), _ddl_params())
def test_nested_ddl_schema_matches_spark(
    label: str, format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Spark's nested CREATE / ADD / RENAME / DROP leave the schema Spark reports.

    Every `INSERT` is skipped, so each read answers an empty table with Spark's column names
    and types on both doors, and each `DESCRIBE` answers Spark's `data_type` strings.
    """
    outcome = _replayed(format_version, False, tmp_path_factory)[label]
    cell = _CELLS[f"v{format_version}_{label}"]
    assert outcome["error"] is None, (label, repr(outcome["error"]))
    assert "sql_error" not in outcome, (label, repr(outcome.get("sql_error")))
    if label.endswith("_describe"):
        got = _describe_types(outcome["sql"])
        assert got == _describe_types(cell), (label, got)
        return
    expected = {**_expected_of(cell), "rows": []}
    assert outcome["sql"] == expected, (label, "sql", outcome["sql"], expected)
    if "dataframe" in outcome or "dataframe_error" in outcome:
        assert "dataframe_error" not in outcome, (label, repr(outcome.get("dataframe_error")))
        assert outcome["dataframe"] == expected, (label, "dataframe", outcome["dataframe"])


@pytest.mark.parametrize(("label", "format_version"), _rows_params())
def test_nested_ddl_rows_match_spark(
    label: str, format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Spark's full nested cells — inserts included — answer Spark's rows on both doors.

    Notes:
        Ids starting `forkwrite` are strict-xfail: an `INSERT` into a list column fails in the
        fork writer on the pinned fork (hand-back fork finding). Ids with `fork292` read a
        file that lacks an added child.
    """
    outcome = _replayed(format_version, True, tmp_path_factory)[label]
    cell = _CELLS[f"v{format_version}_{label}"]
    assert outcome["error"] is None, (label, repr(outcome["error"]))
    assert "sql_error" not in outcome, (label, repr(outcome.get("sql_error")))
    expected = _expected_of(cell)
    assert outcome["sql"] == expected, (label, "sql", outcome["sql"], expected)
    if "dataframe" in outcome or "dataframe_error" in outcome:
        assert "dataframe_error" not in outcome, (label, repr(outcome.get("dataframe_error")))
        assert outcome["dataframe"] == expected, (label, "dataframe", outcome["dataframe"])


@pytest.mark.parametrize("format_version", recorder.FORMAT_VERSIONS)
def test_required_nested_child_refuses_like_spark(
    format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """`ADD COLUMN s.r INT NOT NULL` refuses as incompatible and leaves the table untouched."""
    from repark.errors import PySparkException

    outcome = _replayed(format_version, False, tmp_path_factory)["add_required_nested_child"]
    error = outcome["error"]
    assert isinstance(error, PySparkException), repr(error)
    assert "Incompatible change: cannot add required column" in str(error), str(error)
    assert outcome["after"] == outcome["before"]
    expected = {**_expected_of(_CELLS[f"v{format_version}_struct_child_add_read"]), "rows": []}
    assert outcome["sql"] == expected, (outcome["sql"], expected)


@pytest.mark.parametrize("format_version", recorder.FORMAT_VERSIONS)
def test_required_nested_child_message_matches_spark(
    format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """The required-child refusal carries Spark's whole first message line.

    Notes:
        Round 2 (2026-09-18): RePark raises Iceberg 1.11.0's `cannot add required column: r`
        with Spark's `Unsupported table change: ` prefix before the fork is asked, so the
        fork's own `… without a default value: s.r` text no longer surfaces.
    """
    outcome = _replayed(format_version, False, tmp_path_factory)["add_required_nested_child"]
    assert _REQUIRED_CHILD_MESSAGE in str(outcome["error"]), str(outcome["error"])


@pytest.mark.xfail(
    strict=True,
    reason="EX-COL-2 BACKLOG: RePark names an unaliased `s.a` projection `<table>.s[a]`",
)
def test_unaliased_nested_projection_names_like_spark() -> None:
    """`SELECT id, s.a, s.b` names its columns `id, a, b` as Spark does (EX-COL-2)."""
    spark = _session()
    label = "v2_struct_child_add_leaf_read"
    with _materialized_adopted_tables() as root:
        catalog = f"{_ADOPTED_CATALOG}_names"
        spark.register_memory_catalog(catalog, root)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
        metadata = root / recorder.ADOPTED_NAMESPACE / "st_add_v2" / "metadata/v4.metadata.json"
        spark.sql(
            f"CALL {catalog}.system.register_table("
            f"table => 'ns.st_add_v2', metadata_file => '{metadata}')"
        )
        answer = _answer_of(spark.sql(_retargeted(_CELLS[label]["query"], catalog)).toArrow())
    assert answer["columns"] == _expected_of(_CELLS[label])["columns"], answer["columns"]

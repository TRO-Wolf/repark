"""Facade-boundary container-shape corpus (H-2 gap G10) — pandas / Arrow interchange.

**Oracle.** Every Spark half below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``, ``spark.sql.session.timeZone=UTC``,
``spark.sql.execution.arrow.pyspark.enabled=true``) on 2026-08-12. One recipe per row runs
on BOTH engines — the recorded recipe and the asserted recipe are the same code.

**Home.** Sibling of ``test_interchange_parity.py`` (G-INT primitives, inline goldens) and of
X-5 ``test_nested_container_parity.py`` (engine VALUES via createDataFrame tuples / SQL).
These rows are **boundary SHAPES** (``toPandas`` dtypes + cell Python types, pandas timestamp
unit, Arrow list field naming on the pandas ingest path). They do not duplicate X-5's
tuple-roundtrip VALUES families.

**Why some rows are DISCLOSURES.** Where the engines already agree (binary bytes, array
ndarray cells, inbound ArrowDtype list field name, inbound object-dict inferred as struct)
the row is a plain equality. Where they honestly disagree the row pins BOTH halves:

* map ``toPandas`` cells are ``dict`` on Spark and list-of-pairs on repark
* struct ``toPandas`` Long fields can land as Python ``float`` on Spark (row-stable) and stay
  ``int`` on repark
* inbound pandas ``datetime64[us]`` exports as ``datetime64[ns]`` on Spark and ``datetime64[us]``
  on repark
* inbound pandas object-list arrays export Arrow ``list<element: …>`` on Spark and
  ``list<item: …>`` on repark (same type class as G18, **pandas ingest** entry)

A silent CONVERGENCE goes red and forces the disclosure to flip to equality, never delete.

**Rows assert value AND dtype/shape AND (Arrow surface) nullability** — never ``show``.
Pandas-surface rows pin ``str(dtype)`` + cell ``type.__name__`` + normalized values.
Arrow-surface rows use the parity comparator (name / type / nullability / values).

**Re-deriving the goldens (record mode).** The driver that recorded every Spark half is
committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_boundary_shapes_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe. Needs a JVM +
``pyspark`` (``uv sync --extra record``); never collected by pytest. CI stays JVM-free.

**Entry points.** Facade interchange only (``createDataFrame`` in / ``toPandas`` /
``to_arrow`` out). Claim scoped to the facade boundary. Arrow-Table ingest is not a
repark API (ledger finding); inbound is pandas.

**In-flight fix named by every disclosure** so a red row points at what flips it.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import math
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.session import ReparkSession

pd = pytest.importorskip("pandas")

FIX_G10 = (
    "the facade-boundary container-shape / pandas-timestamp-unit fix "
    "(briefs/v2-engine-hardening.md, gap G10)"
)

# Budget floors/ceilings pinned by test_boundary_shapes_row_set_covers_g10_budget.
G10_BUDGET_MIN = 8
G10_BUDGET_MAX = 10
MIN_EQUALITY_ROWS = 1
MIN_DISCLOSURE_ROWS = 3
# Name-gated so a control cannot satisfy a family (CP-2).
MIN_MAP_ROWS = 1  # *map_*
MIN_STRUCT_ROWS = 1  # *struct_*
MIN_BINARY_ROWS = 1  # *binary_*
MIN_ARRAY_ROWS = 2  # *array_*
MIN_PANDAS_TS_UNIT_ROWS = 1  # *pandas_timestamp_unit_*
MIN_FROM_PANDAS_ROWS = 2  # *_from_pandas_*
MIN_OUT_ROWS = 2  # *_topandas_* or *_sql_cast_*

Surface = Literal["arrow", "pandas"]


# ==================================================================================================
# Recorded pandas half
# ==================================================================================================


@dataclass(frozen=True)
class PandasShape:
    """One engine's recorded ``toPandas`` / ``to_pandas`` export.

    ``dtypes`` are ``str(series.dtype)`` (so ``datetime64[ns]`` vs ``datetime64[us]`` is
    load-bearing). ``cell_kinds`` are ``type(cell).__name__`` (``dict`` vs ``list``,
    ``ndarray`` vs ``list``, ``int`` vs ``float``). ``values`` are *normalized* cells
    (ndarray → tuple, Timestamp → naive datetime, NaT/None unchanged as None).
    """

    dtypes: tuple[tuple[str, str], ...]
    cell_kinds: tuple[tuple[str, tuple[str, ...]], ...]
    values: tuple[tuple[str, tuple[object, ...]], ...]


def _pandas_shape(
    dtypes: list[tuple[str, str]],
    cell_kinds: list[tuple[str, list[str]]],
    values: dict[str, list[object]],
) -> PandasShape:
    """Build a recorded pandas half from the lists a probe / emit prints."""
    return PandasShape(
        dtypes=tuple(dtypes),
        cell_kinds=tuple((name, tuple(kinds)) for name, kinds in cell_kinds),
        values=tuple((name, tuple(values[name])) for name, _ in dtypes),
    )


def _normalize_cell(cell: object) -> object:
    """Comparable form of one pandas cell (dtype/kind are pinned separately)."""
    if cell is None:
        return None
    if type(cell).__name__ == "NaTType":
        return None
    if isinstance(cell, float) and math.isnan(cell):
        return None
    if type(cell).__name__ == "ndarray":
        return tuple(cell.tolist())  # type: ignore[union-attr]
    if hasattr(cell, "to_pydatetime"):
        converted = cell.to_pydatetime()
        if getattr(converted, "tzinfo", None) is not None:
            converted = converted.astimezone(dt.UTC).replace(tzinfo=None)
        return converted
    if isinstance(cell, dict):
        return {key: _normalize_cell(val) for key, val in cell.items()}
    if isinstance(cell, (list, tuple)):
        return tuple(_normalize_cell(item) for item in cell)
    if hasattr(cell, "item") and type(cell).__module__ == "numpy":
        return cell.item()  # type: ignore[union-attr]
    return cell


def capture_pandas(frame_pandas: object) -> PandasShape:
    """Snapshot a live ``toPandas`` result into a :class:`PandasShape`."""
    pdf = frame_pandas
    dtypes: list[tuple[str, str]] = []
    cell_kinds: list[tuple[str, tuple[str, ...]]] = []
    values: list[tuple[str, tuple[object, ...]]] = []
    for name in list(pdf.columns):  # type: ignore[union-attr]
        series = pdf[name]  # type: ignore[index]
        cells = series.tolist()
        dtypes.append((str(name), str(series.dtype)))
        cell_kinds.append((str(name), tuple(type(cell).__name__ for cell in cells)))
        values.append((str(name), tuple(_normalize_cell(cell) for cell in cells)))
    return PandasShape(tuple(dtypes), tuple(cell_kinds), tuple(values))


def _values_match(left: object, right: object) -> bool:
    """Type-sensitive value equality so ``20`` and ``20.0`` stay a disclosure."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        assert isinstance(right, dict)
        if left.keys() != right.keys():
            return False
        return all(_values_match(left[key], right[key]) for key in left)
    if isinstance(left, tuple):
        assert isinstance(right, tuple)
        if len(left) != len(right):
            return False
        return all(_values_match(item, other) for item, other in zip(left, right, strict=True))
    return bool(left == right)


def assert_pandas_shapes_equal(actual: PandasShape, expected: PandasShape) -> None:
    """Equality on dtype strings, cell Python types, and type-sensitive values."""
    if actual.dtypes != expected.dtypes:
        raise FrameMismatchError(
            f"pandas dtype mismatch: actual={actual.dtypes} expected={expected.dtypes}"
        )
    if actual.cell_kinds != expected.cell_kinds:
        raise FrameMismatchError(
            f"pandas cell-type mismatch: actual={actual.cell_kinds} expected={expected.cell_kinds}"
        )
    if len(actual.values) != len(expected.values):
        raise FrameMismatchError(
            f"pandas value mismatch: actual={actual.values} expected={expected.values}"
        )
    for (actual_name, actual_cells), (expected_name, expected_cells) in zip(
        actual.values, expected.values, strict=True
    ):
        if actual_name != expected_name or len(actual_cells) != len(expected_cells):
            raise FrameMismatchError(
                f"pandas value mismatch: actual={actual.values} expected={expected.values}"
            )
        if any(
            not _values_match(actual_cell, expected_cell)
            for actual_cell, expected_cell in zip(actual_cells, expected_cells, strict=True)
        ):
            raise FrameMismatchError(
                f"pandas value mismatch: actual={actual.values} expected={expected.values}"
            )


# ==================================================================================================
# Arrow helpers
# ==================================================================================================


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


# ==================================================================================================
# Row shape
# ==================================================================================================


@dataclass(frozen=True)
class ShapeRow:
    """One G10 boundary-shape row: recipe + recorded Spark half + optional repark half.

    ``repark is None`` → EQUALITY (``repark == Spark`` on the chosen surface).
    ``repark is not None`` → DISCLOSURE (both halves pinned; classifier is reachable).
    """

    name: str
    family: str
    recipe: str
    surface: Surface
    spark: pa.Table | PandasShape | None
    repark: pa.Table | PandasShape | None
    note: str

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (no repark pin)."""
        return self.repark is None and self.spark is not None

    def is_disclosure(self) -> bool:
        """True when the row pins a known divergence (both halves recorded)."""
        return self.repark is not None and self.spark is not None


# ==================================================================================================
# Dual-engine helpers (shared with the record driver)
# ==================================================================================================


def _types_module(session: object) -> object:
    """The ``types`` module belonging to ``session``'s engine."""
    if session.__class__.__module__.split(".", maxsplit=1)[0] == "pyspark":
        from pyspark.sql import types as spark_types

        return spark_types
    from repark import types as repark_types

    return repark_types


def _to_arrow(frame: object) -> pa.Table:
    """Arrow export common to both engines (``to_arrow`` / ``toArrow``)."""
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow  # type: ignore[attr-defined]
    return to_arrow()  # type: ignore[no-any-return]


def _to_pandas(frame: object) -> object:
    """pandas export common to both engines (``to_pandas`` / ``toPandas``)."""
    to_pandas = getattr(frame, "to_pandas", None) or frame.toPandas  # type: ignore[attr-defined]
    return to_pandas()


def _map_schema(session: object) -> object:
    """``StructType([id Long, attrs MapType(String, Long)])`` on either engine."""
    types = _types_module(session)
    return types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("attrs", types.MapType(types.StringType(), types.LongType())),
        ]
    )


def _struct_schema(session: object) -> object:
    """``StructType([id Long, payload StructType(x Long, y String)])`` on either engine."""
    types = _types_module(session)
    return types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField(
                "payload",
                types.StructType(
                    [
                        types.StructField("x", types.LongType()),
                        types.StructField("y", types.StringType()),
                    ]
                ),
            ),
        ]
    )


def _array_schema(session: object) -> object:
    """``StructType([id Long, items ArrayType(Long)])`` on either engine."""
    types = _types_module(session)
    return types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("items", types.ArrayType(types.LongType())),
        ]
    )


def _binary_schema(session: object) -> object:
    """``StructType([id Long, blob BinaryType])`` on either engine."""
    types = _types_module(session)
    return types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("blob", types.BinaryType()),
        ]
    )


def recipe_out_map(session: object) -> object:
    """Typed map column via createDataFrame — outbound ``toPandas`` cell shape."""
    return session.createDataFrame(  # type: ignore[attr-defined]
        [(1, {"a": 1, "b": 2}), (2, {"c": 3}), (3, None)],
        _map_schema(session),
    )


def recipe_out_struct(session: object) -> object:
    """Typed struct column via createDataFrame — outbound ``toPandas`` cell shape."""
    return session.createDataFrame(  # type: ignore[attr-defined]
        [(1, (10, "a")), (2, (20, "b")), (3, None)],
        _struct_schema(session),
    )


def recipe_out_array(session: object) -> object:
    """Typed array column via createDataFrame — outbound ``toPandas`` ndarray shape."""
    return session.createDataFrame(  # type: ignore[attr-defined]
        [(1, [10, 20]), (2, [30]), (3, None)],
        _array_schema(session),
    )


def recipe_out_binary(session: object) -> object:
    """Typed binary column via createDataFrame — outbound bytes shape."""
    return session.createDataFrame(  # type: ignore[attr-defined]
        [(1, b"hello"), (2, bytes([0, 1, 255])), (3, None)],
        _binary_schema(session),
    )


def recipe_out_sql_cast_timestamp(session: object) -> object:
    """SQL CAST timestamp — outbound pandas unit (session TZ = UTC on the Spark half)."""
    return session.sql(  # type: ignore[attr-defined]
        "SELECT CAST(1 AS BIGINT) AS id, CAST('2024-01-15 12:30:00' AS TIMESTAMP) AS ts"
    )


def recipe_in_pandas_object_list(session: object) -> object:
    """Inbound pandas object-dtype lists → array column (Arrow list field name)."""
    pdf = pd.DataFrame({"id": [1, 2], "items": [[10, 20], [30]]})
    return session.createDataFrame(pdf)  # type: ignore[attr-defined]


def recipe_in_pandas_arrowdtype_list(session: object) -> object:
    """Inbound pandas ArrowDtype ``list<element: int64>`` — field name preserved."""
    array = pa.array([[10, 20], [30]], type=pa.list_(pa.field("element", pa.int64())))
    pdf = pd.DataFrame(
        {
            "id": pd.Series([1, 2], dtype="int64"),
            "items": pd.Series(array, dtype=pd.ArrowDtype(array.type)),
        }
    )
    return session.createDataFrame(pdf)  # type: ignore[attr-defined]


def recipe_in_pandas_bytes(session: object) -> object:
    """Inbound pandas object-dtype bytes → binary column."""
    pdf = pd.DataFrame({"id": [1, 2], "blob": [b"hello", bytes([0, 1, 255])]})
    return session.createDataFrame(pdf)  # type: ignore[attr-defined]


def recipe_in_pandas_object_dict(session: object) -> object:
    """Inbound pandas object-dtype dicts — both engines infer a struct, not a map."""
    pdf = pd.DataFrame({"id": [1, 2], "attrs": [{"a": 1, "b": 2}, {"c": 3}]})
    return session.createDataFrame(pdf)  # type: ignore[attr-defined]


def recipe_in_pandas_datetime64_us(session: object) -> object:
    """Inbound pandas ``datetime64[us]`` — outbound pandas timestamp unit."""
    pdf = pd.DataFrame(
        {
            "id": [1, 2],
            "ts": pd.Series(
                [pd.Timestamp("2024-01-15 12:30:00"), pd.Timestamp("2020-06-01 00:00:00")],
                dtype="datetime64[us]",
            ),
        }
    )
    return session.createDataFrame(pdf)  # type: ignore[attr-defined]


RECIPES: dict[str, Any] = {
    "out_map": recipe_out_map,
    "out_struct": recipe_out_struct,
    "out_array": recipe_out_array,
    "out_binary": recipe_out_binary,
    "out_sql_cast_timestamp": recipe_out_sql_cast_timestamp,
    "in_pandas_object_list": recipe_in_pandas_object_list,
    "in_pandas_arrowdtype_list": recipe_in_pandas_arrowdtype_list,
    "in_pandas_bytes": recipe_in_pandas_bytes,
    "in_pandas_object_dict": recipe_in_pandas_object_dict,
    "in_pandas_datetime64_us": recipe_in_pandas_datetime64_us,
}


def run_row(row: ShapeRow, session: object) -> pa.Table | PandasShape:
    """Run one row's recipe on a session (either engine) and capture the chosen surface.

    Shared with the record driver so the recipe the oracle ran and the recipe asserted
    here are the same code, not two copies.
    """
    recipe = RECIPES[row.recipe]
    frame = recipe(session)
    if row.surface == "arrow":
        return _to_arrow(frame)
    return capture_pandas(_to_pandas(frame))


# ==================================================================================================
# Gap G10 — boundary shapes (spark / repark halves filled after record mode)
# ==================================================================================================

ROWS: list[ShapeRow] = [
    # ----- pandas OUT: map / struct / binary / array cell shapes --------------------------------
    ShapeRow(
        "map_topandas_cell_shape",
        "map",
        "out_map",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("attrs", "object")],
            [("id", ["int", "int", "int"]), ("attrs", ["dict", "dict", "NoneType"])],
            {
                "id": [1, 2, 3],
                "attrs": [{"a": 1, "b": 2}, {"c": 3}, None],
            },
        ),
        _pandas_shape(
            [("id", "int64"), ("attrs", "object")],
            [("id", ["int", "int", "int"]), ("attrs", ["list", "list", "NoneType"])],
            {
                "id": [1, 2, 3],
                "attrs": [(("a", 1), ("b", 2)), (("c", 3),), None],
            },
        ),
        "toPandas of a typed map column: both engines use object dtype; Spark cells are "
        "dict, repark cells are list-of-pairs (raw Arrow map → pandas). "
        f"Flipped by {FIX_G10}.",
    ),
    ShapeRow(
        "struct_topandas_cell_shape",
        "struct",
        "out_struct",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("payload", "object")],
            [("id", ["int", "int", "int"]), ("payload", ["dict", "dict", "NoneType"])],
            {
                "id": [1, 2, 3],
                "payload": [{"x": 10, "y": "a"}, {"x": 20.0, "y": "b"}, None],
            },
        ),
        _pandas_shape(
            [("id", "int64"), ("payload", "object")],
            [("id", ["int", "int", "int"]), ("payload", ["dict", "dict", "NoneType"])],
            {
                "id": [1, 2, 3],
                "payload": [{"x": 10, "y": "a"}, {"x": 20, "y": "b"}, None],
            },
        ),
        "toPandas of a typed struct column: both engines use object+dict cells; Spark's "
        "Long field on the second row lands as Python float 20.0 (recorded, stable) while "
        f"repark keeps int 20. Flipped by {FIX_G10}.",
    ),
    ShapeRow(
        "binary_topandas_bytes_shape",
        "binary",
        "out_binary",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("blob", "object")],
            [("id", ["int", "int", "int"]), ("blob", ["bytes", "bytes", "NoneType"])],
            {
                "id": [1, 2, 3],
                "blob": [b"hello", bytes([0, 1, 255]), None],
            },
        ),
        None,
        "equality control: toPandas of a typed binary column is object + bytes cells with "
        "identical payloads on both engines (including a null row).",
    ),
    ShapeRow(
        "array_topandas_ndarray_shape",
        "array",
        "out_array",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("items", "object")],
            [("id", ["int", "int", "int"]), ("items", ["ndarray", "ndarray", "NoneType"])],
            {
                "id": [1, 2, 3],
                "items": [(10, 20), (30,), None],
            },
        ),
        None,
        "toPandas of a typed array column: object dtype + numpy ndarray cells (null row "
        "included). Pandas shape matches; Arrow list field name is X-5's family, not re-pinned "
        "here.",
    ),
    ShapeRow(
        "pandas_timestamp_unit_sql_cast_ns",
        "timestamp",
        "out_sql_cast_timestamp",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("ts", "datetime64[ns]")],
            [("id", ["int"]), ("ts", ["Timestamp"])],
            {
                "id": [1],
                "ts": [dt.datetime(2024, 1, 15, 12, 30, 0)],
            },
        ),
        None,
        "SQL CAST timestamp → toPandas is datetime64[ns] on both engines (wall-clock "
        "2024-01-15 12:30:00). Equality on the pandas unit; Arrow unit/tz is out of this "
        "row's surface.",
    ),
    # ----- pandas IN: createDataFrame from pandas -----------------------------------------------
    ShapeRow(
        "array_from_pandas_object",
        "array",
        "in_pandas_object_list",
        "arrow",
        _table(
            [
                ("id", pa.int64(), True),
                ("items", pa.list_(pa.field("element", pa.int64(), nullable=True)), True),
            ],
            {"id": [1, 2], "items": [[10, 20], [30]]},
        ),
        _table(
            [
                ("id", pa.int64(), True),
                ("items", pa.list_(pa.field("item", pa.int64(), nullable=True)), True),
            ],
            {"id": [1, 2], "items": [[10, 20], [30]]},
        ),
        "createDataFrame from a pandas object-dtype list column: VALUES match; TYPE "
        "diverges on the Arrow list field name (Spark element vs repark item) at the "
        f"pandas ingest boundary. Flipped by {FIX_G10}.",
    ),
    ShapeRow(
        "array_from_pandas_arrowdtype",
        "array",
        "in_pandas_arrowdtype_list",
        "arrow",
        _table(
            [
                ("id", pa.int64(), True),
                ("items", pa.list_(pa.field("element", pa.int64(), nullable=True)), True),
            ],
            {"id": [1, 2], "items": [[10, 20], [30]]},
        ),
        None,
        "createDataFrame from pandas ArrowDtype list<element: int64>: both engines keep "
        "the element field name. Equality twin of array_from_pandas_object.",
    ),
    ShapeRow(
        "binary_from_pandas",
        "binary",
        "in_pandas_bytes",
        "arrow",
        _table(
            [("id", pa.int64(), True), ("blob", pa.binary(), True)],
            {"id": [1, 2], "blob": [b"hello", bytes([0, 1, 255])]},
        ),
        None,
        "createDataFrame from pandas object-dtype bytes: both engines land binary with "
        "identical payloads (inbound twin of binary_topandas_bytes_shape).",
    ),
    ShapeRow(
        "map_from_pandas_object_dict",
        "map",
        "in_pandas_object_dict",
        "arrow",
        _table(
            [
                ("id", pa.int64(), True),
                (
                    "attrs",
                    pa.struct([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())]),
                    True,
                ),
            ],
            {
                "id": [1, 2],
                "attrs": [
                    {"a": 1, "b": 2, "c": None},
                    {"a": None, "b": None, "c": 3},
                ],
            },
        ),
        None,
        "createDataFrame from pandas object-dtype dicts (no MapType schema): both engines "
        "infer a struct over the key union with null-fill — not a map. Equality.",
    ),
    ShapeRow(
        "pandas_timestamp_unit_from_pandas_us",
        "timestamp",
        "in_pandas_datetime64_us",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("ts", "datetime64[ns]")],
            [("id", ["int", "int"]), ("ts", ["Timestamp", "Timestamp"])],
            {
                "id": [1, 2],
                "ts": [
                    dt.datetime(2024, 1, 15, 12, 30, 0),
                    dt.datetime(2020, 6, 1, 0, 0, 0),
                ],
            },
        ),
        _pandas_shape(
            [("id", "int64"), ("ts", "datetime64[us]")],
            [("id", ["int", "int"]), ("ts", ["Timestamp", "Timestamp"])],
            {
                "id": [1, 2],
                "ts": [
                    dt.datetime(2024, 1, 15, 12, 30, 0),
                    dt.datetime(2020, 6, 1, 0, 0, 0),
                ],
            },
        ),
        "createDataFrame from pandas datetime64[us] then toPandas: Spark promotes the "
        "export unit to datetime64[ns]; repark preserves datetime64[us]. Wall-clock "
        f"values match. Flipped by {FIX_G10}.",
    ),
]


# ==================================================================================================
# Session + classification helpers
# ==================================================================================================


def _repark_session() -> ReparkSession:
    """A plain repark session for interchange recipes."""
    import repark

    return repark.ReparkSession.builder.appName("boundary-shapes-parity").getOrCreate()


@pytest.fixture
def repark() -> Iterator[ReparkSession]:
    """Repark session for classifier tests. Yields then stops."""
    session = _repark_session()
    try:
        yield session
    finally:
        with contextlib.suppress(Exception):
            session.stop()


def _halves_differ(left: pa.Table | PandasShape, right: pa.Table | PandasShape) -> bool:
    """True when the two recorded / live halves are not equal on their surface."""
    if isinstance(left, PandasShape) or isinstance(right, PandasShape):
        if not isinstance(left, PandasShape) or not isinstance(right, PandasShape):
            return True
        try:
            assert_pandas_shapes_equal(left, right)
        except FrameMismatchError:
            return True
        return False
    try:
        assert_frames_equal(left, right)
    except FrameMismatchError:
        return True
    return False


def _assert_half_equal(actual: pa.Table | PandasShape, expected: pa.Table | PandasShape) -> None:
    """Dispatch Arrow vs pandas equality."""
    if isinstance(expected, PandasShape):
        assert isinstance(actual, PandasShape), (
            f"expected a pandas half, got {type(actual).__name__}"
        )
        assert_pandas_shapes_equal(actual, expected)
        return
    assert isinstance(actual, pa.Table), f"expected an Arrow table, got {type(actual).__name__}"
    assert_frames_equal(actual, expected)


# ==================================================================================================
# The differential rows
# ==================================================================================================


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_boundary_row_matches_spark_or_still_diverges(row: ShapeRow) -> None:
    """Every recorded row: value AND dtype/shape AND (Arrow) nullability.

    Equality rows assert ``repark == Spark``. Disclosure rows assert repark's pinned
    actual output — and when that assertion fails, the failure is CLASSIFIED:
    CONVERGED (flip-don't-delete) vs regression (re-derive both halves).
    """
    assert row.spark is not None, (
        f"{row.name}: spark golden is missing — run "
        "python/repark/tests/_record_boundary_shapes_goldens.py --emit and paste the halves"
    )

    session = _repark_session()
    try:
        actual = run_row(row, session)
    finally:
        session.stop()
        from repark.session import _reset_active_session_for_tests

        _reset_active_session_for_tests()

    if row.is_equality():
        _assert_half_equal(actual, row.spark)
        return

    assert row.repark is not None
    assert _halves_differ(row.repark, row.spark), (
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a "
        "disclosure at all - flip it to an equality row (repark=None) or re-record it. "
        f"{row.note}"
    )

    try:
        _assert_half_equal(actual, row.repark)
    except FrameMismatchError as mismatch:
        if not _halves_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED - repark now produces the "
                "RECORDED SPARK output, so this disclosure is stale. Do not delete the "
                "row: flip it to an equality row (repark=None) and record the "
                f"convergence. {row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the "
            "recorded Spark golden either - this is a regression, not a convergence. "
            "Re-derive both halves in record mode "
            "(python/repark/tests/_record_boundary_shapes_goldens.py --emit). "
            f"{row.note}\n{mismatch}"
        ) from mismatch


def test_boundary_shapes_row_set_covers_g10_budget() -> None:
    """Budget + name-gated family coverage pins (CP-2 / CP-10) — not incidental counts."""
    names = [row.name for row in ROWS]
    assert len(names) == len(set(names)), f"duplicate row names: {names}"
    assert G10_BUDGET_MIN <= len(ROWS) <= G10_BUDGET_MAX, (
        f"G10 budget {G10_BUDGET_MIN}-{G10_BUDGET_MAX} rows; got {len(ROWS)}"
    )

    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure()]
    assert all(row.spark is not None for row in ROWS), (
        "every row must have a recorded spark half before the budget pin can pass"
    )
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"G10 needs ≥{MIN_EQUALITY_ROWS} equality control; got {len(equalities)}"
    )
    assert len(disclosures) >= MIN_DISCLOSURE_ROWS, (
        f"G10 needs ≥{MIN_DISCLOSURE_ROWS} disclosures; got {len(disclosures)}"
    )

    map_rows = [row for row in ROWS if "map_" in row.name]
    struct_rows = [row for row in ROWS if "struct_" in row.name]
    binary_rows = [row for row in ROWS if "binary_" in row.name]
    array_rows = [row for row in ROWS if "array_" in row.name]
    ts_unit_rows = [row for row in ROWS if "pandas_timestamp_unit_" in row.name]
    from_pandas = [row for row in ROWS if "_from_pandas_" in row.name]
    outbound = [row for row in ROWS if "_topandas_" in row.name or "_sql_cast_" in row.name]
    assert len(map_rows) >= MIN_MAP_ROWS, (
        f"G10 must keep the map family (≥{MIN_MAP_ROWS} rows named *map_*); got {len(map_rows)}"
    )
    assert len(struct_rows) >= MIN_STRUCT_ROWS, (
        f"G10 must keep the struct family (≥{MIN_STRUCT_ROWS} rows named *struct_*); "
        f"got {len(struct_rows)}"
    )
    assert len(binary_rows) >= MIN_BINARY_ROWS, (
        f"G10 must keep the binary family (≥{MIN_BINARY_ROWS} rows named *binary_*); "
        f"got {len(binary_rows)}"
    )
    assert len(array_rows) >= MIN_ARRAY_ROWS, (
        f"G10 must keep the array family (≥{MIN_ARRAY_ROWS} rows named *array_*); "
        f"got {len(array_rows)}"
    )
    assert len(ts_unit_rows) >= MIN_PANDAS_TS_UNIT_ROWS, (
        f"G10 must keep the pandas timestamp-unit family "
        f"(≥{MIN_PANDAS_TS_UNIT_ROWS} rows named *pandas_timestamp_unit_*); "
        f"got {len(ts_unit_rows)}"
    )
    assert len(from_pandas) >= MIN_FROM_PANDAS_ROWS, (
        f"G10 must keep inbound createDataFrame-from-pandas rows "
        f"(≥{MIN_FROM_PANDAS_ROWS} named *_from_pandas_*); got {len(from_pandas)}"
    )
    assert len(outbound) >= MIN_OUT_ROWS, (
        f"G10 must keep outbound toPandas/sql-cast rows "
        f"(≥{MIN_OUT_ROWS} named *_topandas_* or *_sql_cast_*); got {len(outbound)}"
    )
    # A control equality cannot satisfy the timestamp-unit family (CP-2).
    assert all("pandas_timestamp_unit_" in row.name for row in ts_unit_rows)
    assert any(row.is_equality() for row in ROWS if "binary_" in row.name), (
        "at least one binary_* row must remain an equality control"
    )


# ==================================================================================================
# Classifier reachability (CP-1) — both arms proven by monkeypatch
# ==================================================================================================


def _disclosure(name: str) -> ShapeRow:
    """The named disclosure row (must exist — classifier is not optional)."""
    for row in ROWS:
        if row.name == name:
            assert row.is_disclosure(), f"{name} must be a disclosure for the classifier"
            return row
    raise AssertionError(f"missing disclosure {name}")


def test_disclosure_classifier_converged_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: disclosure actual matching the Spark golden → CONVERGED flip guidance."""
    import test_boundary_shapes_parity as shapes_mod

    row = _disclosure("map_topandas_cell_shape")

    def _fake_match(_row: ShapeRow, _session: object) -> pa.Table | PandasShape:
        assert row.spark is not None
        return row.spark

    monkeypatch.setattr(shapes_mod, "run_row", _fake_match)

    with pytest.raises(AssertionError, match="CONVERGED") as excinfo:
        test_boundary_row_matches_spark_or_still_diverges(row)
    message = str(excinfo.value)
    assert "flip it to an equality" in message
    assert "Do not delete" in message
    _ = repark


def test_disclosure_classifier_regression_arm(
    repark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CP-1: disclosure actual matching neither half → regression guidance."""
    import test_boundary_shapes_parity as shapes_mod

    row = _disclosure("map_topandas_cell_shape")
    third = _pandas_shape(
        [("id", "int64"), ("attrs", "object")],
        [("id", ["int", "int", "int"]), ("attrs", ["str", "str", "NoneType"])],
        {"id": [1, 2, 3], "attrs": ["nope", "nope", None]},
    )

    def _fake_third(_row: ShapeRow, _session: object) -> pa.Table | PandasShape:
        return third

    monkeypatch.setattr(shapes_mod, "run_row", _fake_third)

    with pytest.raises(AssertionError, match="regression") as excinfo:
        test_boundary_row_matches_spark_or_still_diverges(row)
    message = str(excinfo.value)
    assert "Re-derive" in message
    _ = repark

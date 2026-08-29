"""Facade-boundary container-shape corpus (H-2 gap G10) — pandas / Arrow interchange.

Every Spark half was recorded in record mode against live PySpark 4.1.2 (zulu-17,
``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``, ``spark.sql.session.timeZone=UTC``,
``spark.sql.execution.arrow.pyspark.enabled=true``). One recipe per row runs on BOTH
engines — the recorded recipe and the asserted recipe are the same code.

Rows assert value AND dtype/shape AND (Arrow surface) nullability — never ``show``.
Where the engines disagree the row pins BOTH halves as a disclosure; a silent
convergence goes red and forces a flip to equality, never delete.

Re-derive goldens with the committed record driver (needs a JVM + ``pyspark``,
``uv sync --extra record``; never collected by pytest)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_boundary_shapes_goldens.py

Entry points: facade interchange only (``createDataFrame`` in / ``toPandas`` /
``to_arrow`` out). Arrow-Table ingest is not a repark API; inbound is pandas.
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
    from repark.spark.session import ReparkSession

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
# Name-gated floors. Disclosure families are *also* semantics-gated in the budget
# pin so an equality control cannot satisfy them (CP-2 / Q-001 / Q-002 / L-001-L-004).
MIN_MAP_ROWS = 1  # typed Map (recipe out_map / Arrow map), not a map_ prefix
MIN_STRUCT_ROWS = 1  # *struct_*
MIN_BINARY_ROWS = 1  # *binary_*
MIN_ARRAY_ROWS = 2  # *array_* AND the item-vs-element ingest disclosure
MIN_PANDAS_TS_UNIT_ROWS = 1  # *pandas_timestamp_unit_* AND the us ingest disclosure
MIN_FROM_PANDAS_ROWS = 2  # *_from_pandas_* — must match every inbound row
MIN_OUT_ROWS = 2  # *_topandas_*

Surface = Literal["arrow", "pandas"]


# Recorded pandas half


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


def _is_pair_sequence(value: object) -> bool:
    """True when ``value`` is a non-empty tuple of 2-tuples (pandas map cells)."""
    if not isinstance(value, tuple) or not value:
        return False
    return all(isinstance(item, tuple) and len(item) == 2 for item in value)


def _pair_sequences_match(left: tuple[object, ...], right: tuple[object, ...]) -> bool:
    """Order-insensitive map-cell compare (X-5 sorts map keys; pair order is storage)."""
    if len(left) != len(right):
        return False
    left_keys = [pair[0] for pair in left]
    right_keys = [pair[0] for pair in right]
    if len(set(left_keys)) == len(left_keys) and len(set(right_keys)) == len(right_keys):
        left_map = {pair[0]: pair[1] for pair in left}
        right_map = {pair[0]: pair[1] for pair in right}
        return _values_match(left_map, right_map)
    # Duplicate keys: sort by repr(key) then compare positionally (no re-enter pair branch).
    left_sorted = tuple(sorted(left, key=lambda pair: repr(pair[0])))
    right_sorted = tuple(sorted(right, key=lambda pair: repr(pair[0])))
    return all(
        _values_match(item, other) for item, other in zip(left_sorted, right_sorted, strict=True)
    )


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
        if _is_pair_sequence(left) and _is_pair_sequence(right):
            return _pair_sequences_match(left, right)
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


# Arrow helpers


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


# Row shape


@dataclass(frozen=True)
class ShapeRow:
    """One G10 row: recipe + recorded Spark half; ``repark`` None → EQUALITY, set → DISCLOSURE."""

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


# Dual-engine helpers (shared with the record driver)


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


def recipe_in_pandas_datetime64_ns(session: object) -> object:
    """Inbound pandas ``datetime64[ns]`` — same-path twin of the us ingest disclosure."""
    pdf = pd.DataFrame(
        {
            "id": [1, 2],
            "ts": pd.Series(
                [
                    pd.Timestamp("2024-01-15 12:30:00.123456"),
                    pd.Timestamp("2020-06-01 00:00:00.654321"),
                ],
                dtype="datetime64[ns]",
            ),
        }
    )
    return session.createDataFrame(pdf)  # type: ignore[attr-defined]


RECIPES: dict[str, Any] = {
    "out_map": recipe_out_map,
    "out_struct": recipe_out_struct,
    "out_array": recipe_out_array,
    "out_binary": recipe_out_binary,
    "in_pandas_object_list": recipe_in_pandas_object_list,
    "in_pandas_arrowdtype_list": recipe_in_pandas_arrowdtype_list,
    "in_pandas_bytes": recipe_in_pandas_bytes,
    "in_pandas_object_dict": recipe_in_pandas_object_dict,
    "in_pandas_datetime64_us": recipe_in_pandas_datetime64_us,
    "in_pandas_datetime64_ns": recipe_in_pandas_datetime64_ns,
}


def run_row(row: ShapeRow, session: object) -> pa.Table | PandasShape:
    """Run one row's recipe on a session (either engine) and capture the chosen surface."""
    recipe = RECIPES[row.recipe]
    frame = recipe(session)
    if row.surface == "arrow":
        return _to_arrow(frame)
    return capture_pandas(_to_pandas(frame))


# Gap G10 boundary-shape rows

ROWS: list[ShapeRow] = [
    # pandas OUT: map / struct / binary / array cell shapes
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
        "Long field on the recorded second row is Python float 20.0 while the first row "
        "is int 10 (recorded live-Spark fact, not a uniform converter; L-005). repark "
        f"keeps int 20. Flipped by {FIX_G10}.",
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
    # pandas IN: createDataFrame from pandas
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
        "binary_from_pandas_bytes",
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
        "struct_from_pandas_object_dict",
        "struct",
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
        "infer a struct over the key union with null-fill — not a map. Named struct_* so "
        "this equality cannot green the typed-map family (Q-002 / L-003).",
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
        "values match. The timestamp-unit family disclosure (Q-001 / L-001). "
        f"Flipped by {FIX_G10}.",
    ),
    ShapeRow(
        "pandas_timestamp_unit_from_pandas_ns",
        "timestamp",
        "in_pandas_datetime64_ns",
        "pandas",
        _pandas_shape(
            [("id", "int64"), ("ts", "datetime64[ns]")],
            [("id", ["int", "int"]), ("ts", ["Timestamp", "Timestamp"])],
            {
                "id": [1, 2],
                "ts": [
                    dt.datetime(2024, 1, 15, 12, 30, 0, 123456),
                    dt.datetime(2020, 6, 1, 0, 0, 0, 654321),
                ],
            },
        ),
        None,
        "createDataFrame from pandas datetime64[ns] then toPandas: both engines export "
        "datetime64[ns] (inbound-ns twin of pandas_timestamp_unit_from_pandas_us). "
        "Ingest-always-us would red this equality (L-002). Values carry microseconds; "
        "the unit lives on str(dtype).",
    ),
]


# Session + classification helpers


def _repark_session() -> ReparkSession:
    """A plain repark session for interchange recipes."""
    import repark

    return repark.ReparkSession.builder.appName("boundary-shapes-parity").getOrCreate()


@pytest.fixture
def repark() -> Iterator[ReparkSession]:
    """Repark session for classifier tests."""
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


# The differential rows


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_boundary_row_matches_spark_or_still_diverges(row: ShapeRow) -> None:
    """Every recorded row: value AND dtype/shape AND (Arrow) nullability — never ``show``."""
    assert row.spark is not None, (
        f"{row.name}: spark golden is missing — run "
        "python/repark/tests/_record_boundary_shapes_goldens.py --emit and paste the halves"
    )

    session = _repark_session()
    try:
        actual = run_row(row, session)
    finally:
        session.stop()
        from repark.spark.session import _reset_active_session_for_tests

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


def _arrow_has_map(table: pa.Table) -> bool:
    """True when any top-level Arrow field is a Map type."""
    return any(pa.types.is_map(field.type) for field in table.schema)


def _is_typed_map_row(row: ShapeRow) -> bool:
    """True when the row exercises a real Map, not a ``map_``-prefixed struct."""
    if row.recipe == "out_map":
        return True
    if isinstance(row.spark, pa.Table) and _arrow_has_map(row.spark):
        return True
    return isinstance(row.repark, pa.Table) and _arrow_has_map(row.repark)


def test_boundary_shapes_row_set_covers_g10_budget() -> None:
    """Budget + semantics-gated family coverage (CP-2 / CP-10) — not incidental counts."""
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

    typed_map_rows = [row for row in ROWS if _is_typed_map_row(row)]
    struct_rows = [row for row in ROWS if "struct_" in row.name]
    binary_rows = [row for row in ROWS if "binary_" in row.name]
    array_rows = [row for row in ROWS if "array_" in row.name]
    ts_unit_rows = [row for row in ROWS if "pandas_timestamp_unit_" in row.name]
    inbound_rows = [row for row in ROWS if row.recipe.startswith("in_pandas_")]
    from_pandas = [row for row in ROWS if "_from_pandas_" in row.name]
    outbound = [row for row in ROWS if "_topandas_" in row.name]
    assert len(typed_map_rows) >= MIN_MAP_ROWS, (
        f"G10 must keep a typed-Map row (recipe out_map or Arrow map type, "
        f"≥{MIN_MAP_ROWS}); got {len(typed_map_rows)}"
    )
    assert any(row.is_disclosure() for row in typed_map_rows), (
        "G10 map family needs a typed-Map DISCLOSURE (outbound map-cell shape); "
        "a map_ prefix on a struct-inference equality does not count"
    )
    assert any("map_topandas" in row.name for row in typed_map_rows), (
        "G10 map family needs the outbound map-cell disclosure by name (map_topandas_*)"
    )
    assert len(struct_rows) >= MIN_STRUCT_ROWS, (
        f"G10 must keep the struct family (≥{MIN_STRUCT_ROWS} rows named *struct_*); "
        f"got {len(struct_rows)}"
    )
    assert any("_from_pandas_" in row.name for row in struct_rows), (
        "G10 struct family needs an inbound *_from_pandas_* row (not outbound-only)"
    )
    assert any("_topandas_" in row.name for row in struct_rows), (
        "G10 struct family needs an outbound *_topandas_* row"
    )
    assert len(binary_rows) >= MIN_BINARY_ROWS, (
        f"G10 must keep the binary family (≥{MIN_BINARY_ROWS} rows named *binary_*); "
        f"got {len(binary_rows)}"
    )
    assert any("_from_pandas_" in row.name for row in binary_rows), (
        "G10 binary family needs an inbound *_from_pandas_* row"
    )
    assert any("_topandas_" in row.name for row in binary_rows), (
        "G10 binary family needs an outbound *_topandas_* row"
    )
    assert len(array_rows) >= MIN_ARRAY_ROWS, (
        f"G10 must keep the array family (≥{MIN_ARRAY_ROWS} rows named *array_*); "
        f"got {len(array_rows)}"
    )
    assert any(
        row.name == "array_from_pandas_object" and row.is_disclosure() for row in array_rows
    ), (
        "G10 array family needs the pandas-ingest item-vs-element DISCLOSURE; "
        "two array equalities cannot satisfy the floor"
    )
    assert any("_topandas_" in row.name for row in array_rows), (
        "G10 array family needs an outbound *_topandas_* row"
    )
    assert len(ts_unit_rows) >= MIN_PANDAS_TS_UNIT_ROWS, (
        f"G10 must keep the pandas timestamp-unit family "
        f"(≥{MIN_PANDAS_TS_UNIT_ROWS} rows named *pandas_timestamp_unit_*); "
        f"got {len(ts_unit_rows)}"
    )
    assert any(row.is_disclosure() and "from_pandas_us" in row.name for row in ts_unit_rows), (
        "G10 pandas timestamp-unit family needs the inbound us DISCLOSURE "
        "(pandas_timestamp_unit_from_pandas_us); an ns equality cannot satisfy the family"
    )
    assert any(row.recipe == "in_pandas_datetime64_ns" for row in ts_unit_rows), (
        "G10 needs an inbound datetime64[ns] twin on the same createDataFrame(pandas) path"
    )
    assert inbound_rows, "G10 needs inbound createDataFrame-from-pandas rows"
    assert all("_from_pandas_" in row.name for row in inbound_rows), (
        "every inbound row must match *_from_pandas_*; "
        f"misses={[row.name for row in inbound_rows if '_from_pandas_' not in row.name]}"
    )
    assert {row.name for row in inbound_rows} == {row.name for row in from_pandas}, (
        "inbound glob *_from_pandas_* must match every inbound row and no others "
        f"(recipes={[row.name for row in inbound_rows]} "
        f"named={[row.name for row in from_pandas]})"
    )
    assert len(from_pandas) >= MIN_FROM_PANDAS_ROWS, (
        f"G10 must keep inbound createDataFrame-from-pandas rows "
        f"(≥{MIN_FROM_PANDAS_ROWS} named *_from_pandas_*); got {len(from_pandas)}"
    )
    assert len(outbound) >= MIN_OUT_ROWS, (
        f"G10 must keep outbound toPandas rows "
        f"(≥{MIN_OUT_ROWS} named *_topandas_*); got {len(outbound)}"
    )
    assert any(row.is_equality() for row in binary_rows), (
        "at least one binary_* row must remain an equality control"
    )


# Classifier reachability (CP-1)


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


def test_pandas_map_cell_pairs_are_order_insensitive() -> None:
    """Q-003: map pair storage order is not a regression (X-5 key-sort)."""
    left = _pandas_shape(
        [("id", "int64"), ("attrs", "object")],
        [("id", ["int"]), ("attrs", ["list"])],
        {"id": [1], "attrs": [(("a", 1), ("b", 2))]},
    )
    right = _pandas_shape(
        [("id", "int64"), ("attrs", "object")],
        [("id", ["int"]), ("attrs", ["list"])],
        {"id": [1], "attrs": [(("b", 2), ("a", 1))]},
    )
    assert_pandas_shapes_equal(left, right)

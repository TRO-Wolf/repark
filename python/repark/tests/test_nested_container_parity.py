"""Nested-container differential corpus — list / struct / map vs live Spark.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(``master("local[2]")``, ``spark.sql.ansi.enabled=true``, ``spark.sql.shuffle.partitions=2``).
One recipe per row runs on BOTH engines, so the recipe under test and the recipe the oracle ran
are the same code — nothing here is hand-computed.

**Why this corpus exists.** Outer row order is unordered (``GROUP BY`` + ``collect_list``) and
nested cells are not ``Table.sort_by``-able; the order-insensitive nested comparison in
``repark_parity.assert_frames_equal`` is exercised on every content assertion.

**Disclosures.** Struct and map round-trips match Spark on value AND Arrow type AND nullability
(equalities). Array-typed columns diverge on the list field name (repark ``item`` vs Spark
``element``) and sometimes nullability — honest TYPE disclosures with both halves pinned. A
silent CONVERGENCE goes red and forces the disclosure to be flipped to equality, never deleted.

**Rows assert on the Arrow path** (``to_arrow`` / ``toArrow``) through the parity comparator —
value AND type AND nullability; never ``show``; order-insensitive by default.

**Re-deriving goldens (record mode).** The committed driver
``python/repark/tests/_record_nested_container_goldens.py`` (needs a JVM + ``pyspark``; never
collected by pytest) imports ``ROWS`` from this module and runs each row's own recipe.

**Entry points.** Facade DataFrame API and facade ``sql()`` over a createDataFrame temp view;
the claim is scoped to the facade surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# Named so every disclosure's note can cite the same future work without inventing per-row fix IDs.
FIX_G18_LIST = (
    "the list field-name / nullability parity fix "
    "(repark list<item: …> vs Spark list<element: …> [not null]; "
    "briefs/v2-engine-hardening.md gap G18 follow-on / G10)"
)

# Budget floors/ceilings pinned by test_nested_row_set_covers_g18_budget (not incidental).
G18_BUDGET_MIN = 4
G18_BUDGET_MAX = 6
MIN_EQUALITY_ROWS = 2
MIN_DISCLOSURE_ROWS = 2
# Name-gated family coverage so a control equality cannot satisfy the pins.
MIN_STRUCT_ROWS = 1  # name-gated *struct*
MIN_MAP_ROWS = 1  # name-gated *map*
MIN_ARRAY_OR_LIST_ROWS = 2  # name-gated *array* or *collect_list*


# Arrow helpers
def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


# ==================================================================================================
# Row shape


@dataclass(frozen=True)
class NestedRow:
    """One differential nested-container row: recipe + recorded Spark half + optional repark half.

    ``repark is None`` → plain EQUALITY. Both halves present → DISCLOSURE: repark's actual output
    is pinned and a convergence onto the recorded Spark output is detected and reported.
    ``entry_point`` selects the facade spelling: ``"sql"`` runs ``session.sql(row.sql)`` after
    registering the seed view; ``"dataframe_api"`` runs the named :data:`DF_RECIPES` helper.
    """

    name: str
    family: str
    sql: str
    spark: pa.Table | None
    repark: pa.Table | None
    note: str
    entry_point: Literal["sql", "dataframe_api"] = "sql"
    df_recipe: str | None = None

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (no repark pin)."""
        return self.repark is None and self.spark is not None

    def is_disclosure(self) -> bool:
        """True when the row pins a known divergence (both halves recorded)."""
        return self.repark is not None and self.spark is not None


# Dual-engine helpers (shared with the record driver)
def _types_module(session: object) -> object:
    """The ``types`` module belonging to ``session``'s engine — PySpark's or repark's."""
    if session.__class__.__module__.split(".", maxsplit=1)[0] == "pyspark":
        from pyspark.sql import types as spark_types

        return spark_types
    from repark import types as repark_types

    return repark_types


def _functions_module(session: object) -> object:
    """The ``functions`` module belonging to ``session``'s engine."""
    if session.__class__.__module__.split(".", maxsplit=1)[0] == "pyspark":
        from pyspark.sql import functions as spark_functions

        return spark_functions
    from repark.spark.sql import functions as repark_functions

    return repark_functions


def _to_arrow(frame: object) -> pa.Table:
    """Arrow export common to both engines (``to_arrow`` / ``toArrow``)."""
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow  # type: ignore[attr-defined]
    return to_arrow()  # type: ignore[no-any-return]


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


def _map_schema(session: object) -> object:
    """``StructType([id Long, attrs MapType(String, Long)])`` on either engine."""
    types = _types_module(session)
    return types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("attrs", types.MapType(types.StringType(), types.LongType())),
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


def _array_of_struct_schema(session: object) -> object:
    """``StructType([id Long, items ArrayType(StructType(x,y))])`` on either engine."""
    types = _types_module(session)
    element = types.StructType(
        [
            types.StructField("x", types.LongType()),
            types.StructField("y", types.StringType()),
        ]
    )
    return types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("items", types.ArrayType(element)),
        ]
    )


def dataframe_api_struct_roundtrip(session: object) -> pa.Table:
    """Struct column via createDataFrame — multi-row with a duplicate nested payload.

    Outer row order is not pinned; values AND Arrow type AND nullability are part of the pin.
    """
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [
            (1, (10, "a")),
            (2, (20, "b")),
            (3, (10, "a")),  # duplicate nested value under a different id
        ],
        _struct_schema(session),
    )
    return _to_arrow(frame.select("id", "payload"))


def dataframe_api_map_roundtrip(session: object) -> pa.Table:
    """Map column via createDataFrame — multi-row map round-trip (entry order normalized)."""
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [
            (1, {"a": 1, "b": 2}),
            (2, {"c": 3}),
            (3, {"a": 1, "b": 2}),
        ],
        _map_schema(session),
    )
    return _to_arrow(frame.select("id", "attrs"))


def dataframe_api_array_roundtrip(session: object) -> pa.Table:
    """Array column via createDataFrame — TYPE disclosure (list field name item vs element)."""
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [
            (1, [10, 20]),
            (2, [30]),
            (3, [10, 20]),
            (4, None),
        ],
        _array_schema(session),
    )
    return _to_arrow(frame.select("id", "items"))


def dataframe_api_array_of_struct(session: object) -> pa.Table:
    """Array-of-struct via createDataFrame — nested list + struct TYPE disclosure."""
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [
            (1, [(10, "a"), (11, "b")]),
            (2, [(20, "c")]),
            (3, [(10, "a"), (11, "b")]),
        ],
        _array_of_struct_schema(session),
    )
    return _to_arrow(frame.select("id", "items"))


def dataframe_api_collect_list(session: object) -> pa.Table:
    """``groupBy.agg(collect_list)`` — outer rows unordered; the G18 enabler for nested groups.

    List element order within a group is part of the value; the seed is small and
    single-partition-friendly so both engines produce the same within-group order.
    """
    functions = _functions_module(session)
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(1, 10), (1, 20), (2, 30), (2, 40), (1, 15)],
        ["grp", "v"],
    )
    aggregated = frame.groupBy("grp").agg(functions.collect_list("v").alias("items"))
    return _to_arrow(aggregated)


def register_struct_seed_view(session: object) -> None:
    """Register ``nested_struct_seed`` for SQL-door struct projection."""
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [
            (1, (10, "a")),
            (2, (20, "b")),
            (3, (10, "a")),
        ],
        _struct_schema(session),
    )
    frame.createOrReplaceTempView("nested_struct_seed")


DF_RECIPES: dict[str, object] = {
    "struct_roundtrip": dataframe_api_struct_roundtrip,
    "map_roundtrip": dataframe_api_map_roundtrip,
    "array_roundtrip": dataframe_api_array_roundtrip,
    "array_of_struct": dataframe_api_array_of_struct,
    "collect_list": dataframe_api_collect_list,
}


def run_row(row: NestedRow, session: object) -> pa.Table:
    """Run one row's recipe on a session (either engine) and return its Arrow output.

    Shared with the record driver so the recipe the oracle ran and the recipe asserted here are
    the same code, not two copies.
    """
    if row.entry_point == "dataframe_api":
        assert row.df_recipe is not None, f"{row.name}: dataframe_api row needs df_recipe"
        recipe = DF_RECIPES[row.df_recipe]
        return recipe(session)  # type: ignore[operator, no-any-return]
    register_struct_seed_view(session)
    frame = session.sql(row.sql)  # type: ignore[attr-defined]
    return _to_arrow(frame)


# Gap G18 — nested containers
ROWS: list[NestedRow] = [
    # ----- Equalities: struct + map (types match Spark on the Arrow path) -----------------------
    NestedRow(
        "struct_column_roundtrip",
        "struct",
        "SELECT id, payload FROM nested_struct_seed",  # documentation; DF recipe runs
        _table(
            [
                ("id", pa.int64(), True),
                ("payload", pa.struct([("x", pa.int64()), ("y", pa.string())]), True),
            ],
            {
                "id": [1, 2, 3],
                "payload": [
                    {"x": 10, "y": "a"},
                    {"x": 20, "y": "b"},
                    {"x": 10, "y": "a"},
                ],
            },
        ),
        None,
        "Struct column createDataFrame round-trip: value AND struct<x: int64, y: string> "
        "type AND nullability match Spark. Multi-row multiset with a duplicate nested payload "
        "exercises the G18 order-insensitive nested comparator.",
        entry_point="dataframe_api",
        df_recipe="struct_roundtrip",
    ),
    NestedRow(
        "struct_sql_select",
        "struct",
        "SELECT id, payload FROM nested_struct_seed",
        _table(
            [
                ("id", pa.int64(), True),
                ("payload", pa.struct([("x", pa.int64()), ("y", pa.string())]), True),
            ],
            {
                "id": [1, 2, 3],
                "payload": [
                    {"x": 10, "y": "a"},
                    {"x": 20, "y": "b"},
                    {"x": 10, "y": "a"},
                ],
            },
        ),
        None,
        "SQL-door projection of a struct column registered via createDataFrame temp view "
        "(CP-11: sql entry point sibling of struct_column_roundtrip). Outer row order free.",
        entry_point="sql",
    ),
    NestedRow(
        "map_column_roundtrip",
        "map",
        "SELECT id, attrs FROM nested_map_seed",  # documentation; DF recipe runs
        _table(
            [
                ("id", pa.int64(), True),
                ("attrs", pa.map_(pa.string(), pa.int64()), True),
            ],
            {
                "id": [1, 2, 3],
                "attrs": [[("a", 1), ("b", 2)], [("c", 3)], [("a", 1), ("b", 2)]],
            },
        ),
        None,
        "Map column createDataFrame round-trip: value AND map<string, int64> type AND "
        "nullability match Spark. Map entry order is normalized by the G18 comparator so "
        "equal maps with different storage order still pass.",
        entry_point="dataframe_api",
        df_recipe="map_roundtrip",
    ),
    # ----- Disclosures: array field-name / collect_list nullability (TYPE) ---------------------
    NestedRow(
        "array_column_roundtrip",
        "array",
        "SELECT id, items FROM nested_array_seed",
        _table(
            [
                ("id", pa.int64(), True),
                (
                    "items",
                    pa.list_(pa.field("element", pa.int64(), nullable=True)),
                    True,
                ),
            ],
            {
                "id": [1, 2, 3, 4],
                "items": [[10, 20], [30], [10, 20], None],
            },
        ),
        _table(
            [
                ("id", pa.int64(), True),
                ("items", pa.list_(pa.field("item", pa.int64(), nullable=True)), True),
            ],
            {
                "id": [1, 2, 3, 4],
                "items": [[10, 20], [30], [10, 20], None],
            },
        ),
        "Array column createDataFrame: VALUES match; TYPE diverges — repark "
        "list<item: int64> vs Spark list<element: int64> (Arrow list field name). "
        f"Null row included. Flipped by {FIX_G18_LIST}.",
        entry_point="dataframe_api",
        df_recipe="array_roundtrip",
    ),
    NestedRow(
        "collect_list_grouped",
        "collect_list",
        "SELECT grp, collect_list(v) AS items FROM nested_list_seed GROUP BY grp",
        _table(
            [
                ("grp", pa.int64(), True),
                (
                    "items",
                    pa.list_(pa.field("element", pa.int64(), nullable=False)),
                    False,
                ),
            ],
            {
                "grp": [1, 2],
                "items": [[10, 20, 15], [30, 40]],
            },
        ),
        _table(
            [
                ("grp", pa.int64(), True),
                ("items", pa.list_(pa.field("item", pa.int64(), nullable=True)), False),
            ],
            {
                # Outer row order from repark at record time; comparator is order-insensitive.
                "grp": [2, 1],
                "items": [[30, 40], [10, 20, 15]],
            },
        ),
        "groupBy.agg(collect_list): outer rows unordered (G18 enabler). VALUES match under "
        "the recorded seed; the top-level flag converged non-null 2026-09-06 "
        "(NULLABILITY-2 round 2: the empty-array branch is non-null like Spark). TYPE still "
        "diverges on the value-field name and element flag — repark list<item: int64> "
        "(nullable elements) vs Spark list<element: int64 not null>. "
        f"Flipped by {FIX_G18_LIST}.",
        entry_point="dataframe_api",
        df_recipe="collect_list",
    ),
    NestedRow(
        "array_of_struct_roundtrip",
        "array_struct",
        "SELECT id, items FROM nested_aos_seed",
        _table(
            [
                ("id", pa.int64(), True),
                (
                    "items",
                    pa.list_(
                        pa.field(
                            "element",
                            pa.struct([("x", pa.int64()), ("y", pa.string())]),
                            nullable=True,
                        )
                    ),
                    True,
                ),
            ],
            {
                "id": [1, 2, 3],
                "items": [
                    [{"x": 10, "y": "a"}, {"x": 11, "y": "b"}],
                    [{"x": 20, "y": "c"}],
                    [{"x": 10, "y": "a"}, {"x": 11, "y": "b"}],
                ],
            },
        ),
        _table(
            [
                ("id", pa.int64(), True),
                (
                    "items",
                    pa.list_(
                        pa.field(
                            "item",
                            pa.struct([("x", pa.int64()), ("y", pa.string())]),
                            nullable=True,
                        )
                    ),
                    True,
                ),
            ],
            {
                "id": [1, 2, 3],
                "items": [
                    [{"x": 10, "y": "a"}, {"x": 11, "y": "b"}],
                    [{"x": 20, "y": "c"}],
                    [{"x": 10, "y": "a"}, {"x": 11, "y": "b"}],
                ],
            },
        ),
        "Array-of-struct createDataFrame: VALUES match; TYPE diverges on the list field name "
        "(item vs element) wrapping struct<x: int64, y: string>. Nested list+struct shape "
        f"the old comparator could not sort. Flipped by {FIX_G18_LIST}.",
        entry_point="dataframe_api",
        df_recipe="array_of_struct",
    ),
]


# Session + classification helpers
def _session() -> ReparkSession:
    """A plain repark session for nested SQL / DataFrame API."""
    import repark

    return repark.ReparkSession.builder.appName("nested-container-parity").getOrCreate()


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


# The differential rows
@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_nested_row_matches_spark_or_still_diverges(row: NestedRow) -> None:
    """Every recorded row, on the Arrow path (value AND exact Arrow type AND nullability).

    Equality rows assert ``repark == Spark``. Disclosure rows pin repark's recorded output; a
    mismatch is CLASSIFIED before it is raised: CONVERGED (flip-don't-delete) vs regression
    (re-derive both halves).
    """
    assert row.spark is not None, (
        f"{row.name}: spark golden is missing — run "
        "python/repark/tests/_record_nested_container_goldens.py --emit and paste the halves"
    )

    session = _session()
    try:
        actual = run_row(row, session)
    finally:
        session.stop()
        from repark.spark.session import _reset_active_session_for_tests

        _reset_active_session_for_tests()

    if row.is_equality():
        assert_frames_equal(actual, row.spark)
        return

    # Disclosure: both halves pinned; they must still differ (else the row is not a disclosure).
    assert row.repark is not None
    assert _frames_differ(row.repark, row.spark), (
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure "
        "at all - flip it to an equality row (repark=None) or re-record it. "
        f"{row.note}"
    )

    try:
        assert_frames_equal(actual, row.repark)
    except FrameMismatchError as mismatch:
        # Classify: did repark converge onto the recorded Spark golden?
        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED - repark now produces the "
                "RECORDED SPARK output, so this disclosure is stale. Do not delete the row: "
                "flip it to an equality row (repark=None) and record the convergence. "
                f"{row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the "
            "recorded Spark golden either - this is a regression, not a convergence. "
            "Re-derive both halves in record mode "
            "(python/repark/tests/_record_nested_container_goldens.py --emit). "
            f"{row.note}\n{mismatch}"
        ) from mismatch


def test_nested_row_set_covers_g18_budget() -> None:
    """Budget + name-gated family coverage pins — not incidental counts."""
    names = [row.name for row in ROWS]
    assert len(names) == len(set(names)), f"duplicate row names: {names}"
    assert G18_BUDGET_MIN <= len(ROWS) <= G18_BUDGET_MAX, (
        f"G18 budget {G18_BUDGET_MIN}-{G18_BUDGET_MAX} rows; got {len(ROWS)}"
    )

    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure()]
    # Rows still awaiting record (spark is None) count as neither — fail closed.
    assert all(row.spark is not None for row in ROWS), (
        "every row must have a recorded spark half before the budget pin can pass"
    )
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"G18 needs ≥{MIN_EQUALITY_ROWS} equalities; got {len(equalities)}"
    )
    assert len(disclosures) >= MIN_DISCLOSURE_ROWS, (
        f"G18 needs ≥{MIN_DISCLOSURE_ROWS} disclosures; got {len(disclosures)}"
    )

    struct_rows = [row for row in ROWS if "struct" in row.name]
    map_rows = [row for row in ROWS if "map" in row.name]
    array_or_list = [row for row in ROWS if "array" in row.name or "collect_list" in row.name]
    assert len(struct_rows) >= MIN_STRUCT_ROWS, (
        f"G18 must keep the struct family (≥{MIN_STRUCT_ROWS} rows named *struct*); "
        f"got {len(struct_rows)}"
    )
    assert len(map_rows) >= MIN_MAP_ROWS, (
        f"G18 must keep the map family (≥{MIN_MAP_ROWS} rows named *map*); got {len(map_rows)}"
    )
    assert len(array_or_list) >= MIN_ARRAY_OR_LIST_ROWS, (
        f"G18 must keep the array/list family (≥{MIN_ARRAY_OR_LIST_ROWS} rows named "
        f"*array* or *collect_list*); got {len(array_or_list)}"
    )

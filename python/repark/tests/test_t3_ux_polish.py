"""T3 UX polish: display_style, Column.round, default app name, H1 export naming.

Pins the four must-ship surfaces from charter-t3. Synthetic fixtures only — no brokerage
values.
"""

from __future__ import annotations

import sys
from io import StringIO

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark import ta as repark_ta
from repark.errors import IllegalArgumentException
from repark.spark.window import Window


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test (stop active so defaults re-resolve)."""
    existing = ReparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    session = ReparkSession.builder.getOrCreate()
    yield session
    session.stop()


# display_style — conf.set applies; wrong module surface refuses loud


def test_conf_set_display_style_applies_to_show(spark: ReparkSession) -> None:
    """conf.set('repark.display.style', …) must drive show() (not conf-only absorption)."""
    frame = spark.range(2)
    spark.conf.set("repark.display.style", "polars")
    assert spark.display_style == "polars"
    assert spark.conf.get("repark.display.style") == "polars"
    buffer = StringIO()
    old = sys.stdout
    sys.stdout = buffer
    try:
        frame.show()
    finally:
        sys.stdout = old
    rendered = buffer.getvalue()
    assert "shape:" in rendered
    assert "+----+" not in rendered


def test_display_style_property_syncs_conf(spark: ReparkSession) -> None:
    """session.display_style = … keeps conf.get in lockstep."""
    spark.display_style = "duckdb"
    assert spark.conf.get("repark.display.style") == "duckdb"
    assert spark.display_style == "duckdb"


def test_module_display_style_assignment_refuses_loud() -> None:
    """repark.display_style = … on the package must not silently absorb."""
    import repark

    with pytest.raises(AttributeError, match=r"not a module attribute"):
        repark.display_style = "polars"  # type: ignore[attr-defined]


def test_invalid_conf_display_style_refuses(spark: ReparkSession) -> None:
    """Invalid repark.display.style via conf.set fails loud (same as property)."""
    with pytest.raises(IllegalArgumentException):
        spark.conf.set("repark.display.style", "pandas")


def test_conf_unset_display_style_resets_to_spark(spark: ReparkSession) -> None:
    """conf.unset('repark.display.style') must not leave property/show on prior style.

    F-T3-001 regression: set polars → unset → get/property default spark; show spark-like.
    """
    frame = spark.range(2)
    spark.conf.set("repark.display.style", "polars")
    assert spark.display_style == "polars"
    spark.conf.unset("repark.display.style")
    # Live style + conf.get return the default (not the pre-unset polars).
    assert spark.conf.get("repark.display.style") == "spark"
    assert spark.display_style == "spark"
    # getAll omits the key after unset (tomb); default is not a stored override.
    assert "repark.display.style" not in spark.conf.getAll
    buffer = StringIO()
    old = sys.stdout
    sys.stdout = buffer
    try:
        frame.show()
    finally:
        sys.stdout = old
    rendered = buffer.getvalue()
    # Spark-style ASCII grid (not polars "shape:" table).
    assert "+----+" in rendered or "+---+" in rendered
    assert "shape:" not in rendered


# Default session / app name → repark


def test_default_app_name_is_repark(spark: ReparkSession) -> None:
    """When WE control the default (no builder.appName), conf surfaces 'repark'."""
    assert spark.conf.get("spark.app.name") == "repark"


def test_explicit_app_name_is_surfaced() -> None:
    """builder.appName overrides the repark default."""
    existing = ReparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    session = ReparkSession.builder.appName("etl-job").getOrCreate()
    try:
        assert session.conf.get("spark.app.name") == "etl-job"
    finally:
        session.stop()


# Column.round (repark-extra)


def test_column_round_delegates_to_f_round(spark: ReparkSession) -> None:
    """Column.round(n) matches F.round on Arrow path (value + type)."""
    frame = spark.createDataFrame([(1.2345,), (2.5,)], ["x"])
    via_method = frame.select(frame.x.round(2).alias("r")).to_arrow()
    via_function = frame.select(F.round("x", 2).alias("r")).to_arrow()
    assert via_method.column_names == ["r"]
    assert via_method.schema.field("r").type == via_function.schema.field("r").type
    assert via_method.to_pylist() == via_function.to_pylist()
    assert via_method.to_pylist() == [{"r": 1.23}, {"r": 2.5}]


def test_column_round_on_windowed_ta(spark: ReparkSession) -> None:
    """Column.round works on windowed TA outputs (charter chain)."""
    frame = spark.createDataFrame(
        [(1, 10.0), (2, 11.0), (3, 12.0), (4, 13.0)],
        ["ts", "close"],
    )
    window = Window.orderBy("ts")
    rounded = frame.select(
        repark_ta.sma("close", timeperiod=2).over(window).round(2).alias("sma2")
    ).to_arrow()
    assert rounded.column_names == ["sma2"]
    values = [row["sma2"] for row in rounded.to_pylist()]
    # SMA(2): null, 10.5, 11.5, 12.5 — round(2) preserves two decimals where present.
    assert values[0] is None or values[0] != values[0]  # null or NaN lookback
    assert values[-1] == pytest.approx(12.5)


# H1 residual — export display overlay (collect / to_arrow / to_polars / to_pandas)


def _bare_multi_name_join(spark: ReparkSession):
    """Condition join that retains Spark-legal duplicate display names (H1 bare join)."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    return left.join(frame, left.b == frame.b)


def test_h1_collect_uses_display_names(spark: ReparkSession) -> None:
    """collect Row field names = display names (no __repark_l_ leak)."""
    joined = _bare_multi_name_join(spark)
    assert joined.columns == ["aa", "b", "a", "b"]
    rows = joined.collect()
    assert len(rows) == 2
    assert rows[0].__fields__ == ["aa", "b", "a", "b"]
    assert not any(name.startswith("__repark_") for name in rows[0].__fields__)
    # Positional values preserved under dup display names.
    assert rows[0][0] == 1
    assert rows[0][1] == 2
    assert rows[0][2] == 1
    assert rows[0][3] == 2


def test_h1_to_arrow_uses_display_names(spark: ReparkSession) -> None:
    """to_arrow field names = display names (dup names positionally)."""
    joined = _bare_multi_name_join(spark)
    table = joined.to_arrow()
    assert list(table.column_names) == ["aa", "b", "a", "b"]
    assert not any(name.startswith("__repark_") for name in table.column_names)
    # Two columns both named "b" — values by index.
    assert table.column(1).to_pylist() == [2, 4]
    assert table.column(3).to_pylist() == [2, 4]


def test_h1_to_polars_uses_display_names(spark: ReparkSession) -> None:
    """to_polars prefers display names; dups disambiguated (polars unique-name constraint)."""
    pytest.importorskip("polars")
    joined = _bare_multi_name_join(spark)
    frame = joined.to_polars()
    # First "b" stays bare; second becomes b__1 — no engine __repark_* leak.
    assert list(frame.columns) == ["aa", "b", "a", "b__1"]
    assert not any(name.startswith("__repark_") for name in frame.columns)


def test_h1_to_pandas_uses_display_names(spark: ReparkSession) -> None:
    """to_pandas column names = display names (pandas allows duplicate labels)."""
    pytest.importorskip("pandas")
    joined = _bare_multi_name_join(spark)
    frame = joined.to_pandas()
    assert list(frame.columns) == ["aa", "b", "a", "b"]
    assert not any(str(name).startswith("__repark_") for name in frame.columns)


def test_h1_multi_name_row_pickle_preserves_values(spark: ReparkSession) -> None:
    """Pickle round-trip of multi-name collect Rows must not drop duplicate fields.

    F-T3-002 regression: __reduce__ via asDict/from_mapping collapsed dups (4→3);
    must use from_ordered_fields so names and values both survive.
    """
    import pickle

    joined = _bare_multi_name_join(spark)
    row = joined.collect()[0]
    assert row.__fields__ == ["aa", "b", "a", "b"]
    assert list(row) == [1, 2, 1, 2]
    restored = pickle.loads(pickle.dumps(row))
    assert restored.__fields__ == ["aa", "b", "a", "b"]
    assert list(restored) == [1, 2, 1, 2]
    assert restored == row
    # Positional access under dup display names.
    assert restored[1] == 2
    assert restored[3] == 2

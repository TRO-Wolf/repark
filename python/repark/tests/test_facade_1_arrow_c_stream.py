"""FACADE-1 Arrow C Stream boundary pins: capsule protocol, pyarrow optional, IPC fallback."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from repark import ReparkSession
from repark.spark.dataframe import DataFrame
from repark.spark.ml.ext._arrow_util import reenter_with_prediction

_REPO_ROOT = Path(__file__).resolve().parents[3]
_HIDE_PYARROW_PREFIX = """
import sys
class _BlockPyarrow:
    def find_spec(self, name, path=None, target=None):
        if name == "pyarrow" or name.startswith("pyarrow."):
            raise ModuleNotFoundError(name)
        return None
sys.meta_path.insert(0, _BlockPyarrow())
for key in list(sys.modules):
    if key == "pyarrow" or key.startswith("pyarrow."):
        del sys.modules[key]
"""


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Session for capsule-seam pins."""
    session = ReparkSession.builder.appName("facade-1-arrow-c-stream").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _hidden_pyarrow_env() -> dict[str, str]:
    """Child env with the facade src on PYTHONPATH."""
    env = os.environ.copy()
    src = str(_REPO_ROOT / "python" / "repark" / "src")
    previous = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src if not previous else os.pathsep.join([src, previous])
    return env


def _run_hidden_pyarrow(body: str) -> subprocess.CompletedProcess[str]:
    """Run ``body`` in a child interpreter with pyarrow hidden."""
    script = _HIDE_PYARROW_PREFIX + "\n" + textwrap.dedent(body).strip() + "\n"
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
        env=_hidden_pyarrow_env(),
        check=False,
        cwd=str(_REPO_ROOT),
    )


def test_package_imports_with_pyarrow_hidden() -> None:
    """Package import succeeds when pyarrow is hidden. pins: facade-1/C-002"""
    result = _run_hidden_pyarrow(
        "import repark\n"
        "from repark import ReparkSession, DataFrame, Column, Row\n"
        "print('imported', repark.__name__, ReparkSession.__name__, DataFrame.__name__)"
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "imported repark" in result.stdout


def test_polars_and_pandas_consume_arrow_c_stream_with_pyarrow_hidden() -> None:
    """Polars and pandas consume ``__arrow_c_stream__`` with pyarrow hidden. pins: facade-1/C-002"""
    result = _run_hidden_pyarrow(
        textwrap.dedent(
            """
            from repark import ReparkSession
            import polars as pl
            import pandas as pd
            session = ReparkSession.builder.appName("facade-1-hidden-consume").getOrCreate()
            try:
                frame = session.sql("SELECT 1 AS x")
                capsule = frame.__arrow_c_stream__()
                assert type(capsule).__name__ == "PyCapsule"
                polars_frame = pl.DataFrame(frame)
                assert polars_frame.to_dicts() == [{"x": 1}]
                pandas_frame = pd.DataFrame(polars_frame.to_dicts())
                assert list(pandas_frame["x"]) == [1]
                inbound = pl.DataFrame({"y": [3, 4]})
                round_trip = session.createDataFrame(inbound)
                assert sorted(pl.DataFrame(round_trip).to_dicts(), key=lambda row: row["y"]) == [
                    {"y": 3},
                    {"y": 4},
                ]
                print("consumed")
            finally:
                session.stop()
            """
        )
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "consumed" in result.stdout


def test_to_arrow_names_pyarrow_extra_when_hidden() -> None:
    """to_arrow fails loud naming the extra when pyarrow is hidden. pins: facade-1/C-002"""
    result = _run_hidden_pyarrow(
        textwrap.dedent(
            """
            from repark import ReparkSession
            session = ReparkSession.builder.appName("facade-1-hidden-to-arrow").getOrCreate()
            try:
                frame = session.sql("SELECT 1 AS x")
                try:
                    frame.to_arrow()
                except ImportError as error:
                    text = str(error)
                    assert "repark[pyarrow]" in text, text
                    print("named-extra")
                else:
                    raise SystemExit("to_arrow must raise ImportError when pyarrow is hidden")
            finally:
                session.stop()
            """
        )
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "named-extra" in result.stdout


def test_pandas_from_arrow_consumes_arrow_c_stream(spark: ReparkSession) -> None:
    """pandas.DataFrame.from_arrow consumes the export capsule. pins: facade-1/C-001"""
    frame = spark.sql("SELECT 1 AS x")
    pandas_frame = pd.DataFrame.from_arrow(frame)
    assert list(pandas_frame["x"]) == [1]


def test_polars_from_arrow_consumes_arrow_c_stream(spark: ReparkSession) -> None:
    """polars.DataFrame consumes the export capsule. pins: facade-1/C-001"""
    frame = spark.sql("SELECT 1 AS x")
    polars_frame = pl.DataFrame(frame)
    assert polars_frame.to_dicts() == [{"x": 1}]


def _identity_batches(batches: object) -> object:
    """Pass-through mapInArrow callback."""
    return batches


def test_mapinarrow_placeholder_registers_capsule_not_ipc(spark: ReparkSession) -> None:
    """mapInArrow construction registers a capsule exporter, not IPC. pins: facade-1/C-001"""
    events: list[str] = []
    frame = spark.createDataFrame([(1,)], "x INT")
    real = frame._session

    class _SessionProxy:
        def register_arrow_stream_as_temp_view(self, view_name: str, stream_obj: object) -> None:
            events.append(f"cstream:{view_name}")
            assert hasattr(stream_obj, "__arrow_c_stream__")
            real.register_arrow_stream_as_temp_view(view_name, stream_obj)

        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            events.append(f"ipc:{view_name}:{len(ipc_bytes)}")
            real.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

    frame._session = _SessionProxy()  # type: ignore[assignment]
    out = frame.mapInArrow(_identity_batches, "x INT")
    assert out.columns == ["x"]
    assert any(event.startswith("cstream:") for event in events), events
    assert not any(event.startswith("ipc:") for event in events), events


def test_ml_reenter_registers_capsule_not_ipc(spark: ReparkSession) -> None:
    """ML prediction re-entry registers a capsule exporter, not IPC. pins: facade-1/C-001"""
    import pyarrow as pa

    events: list[str] = []
    frame = spark.createDataFrame([(1.0,)], ["x"])
    table = pa.table({"x": pa.array([1.0], type=pa.float64())})
    real = frame._session

    class _SessionProxy:
        def register_arrow_stream_as_temp_view(self, view_name: str, stream_obj: object) -> None:
            events.append(f"cstream:{view_name}")
            assert hasattr(stream_obj, "__arrow_c_stream__")
            real.register_arrow_stream_as_temp_view(view_name, stream_obj)

        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            events.append(f"ipc:{view_name}:{len(ipc_bytes)}")
            real.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

    frame._session = _SessionProxy()  # type: ignore[assignment]
    result = reenter_with_prediction(frame, table, [2.5], "prediction")
    try:
        rows = result.collect()
        assert rows[0]["prediction"] == pytest.approx(2.5)
        assert any(event.startswith("cstream:") for event in events), events
        assert not any(event.startswith("ipc:") for event in events), events
    finally:
        del result


def test_create_dataframe_ipc_fallback_stays_when_capsule_hidden(
    spark: ReparkSession,
) -> None:
    """Version-skew: missing capsule register falls back to IPC. pins: facade-1/C-001"""
    real = spark._ensure_alive()
    ipc_calls = {"n": 0}

    class _SessionIpcOnly:
        def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
            ipc_calls["n"] += 1
            real.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

        def __getattr__(self, name: str) -> object:
            if name == "register_arrow_stream_as_temp_view":
                raise AttributeError(name)
            return getattr(real, name)

    spark._inner = _SessionIpcOnly()  # type: ignore[assignment]
    try:
        frame = spark.createDataFrame([(9, "z")], ["id", "label"])
        assert frame.collect()[0][0] == 9
    finally:
        spark._inner = real  # type: ignore[assignment]
    assert ipc_calls["n"] >= 1


def test_to_arrow_to_pandas_collect_signatures_unchanged(spark: ReparkSession) -> None:
    """Export doors keep their signatures and answers. pins: facade-1/C-005"""
    frame = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])
    table = frame.to_arrow()
    assert table.column("id").to_pylist() == [1, 2]
    pandas_frame = frame.toPandas()
    assert list(pandas_frame["id"]) == [1, 2]
    rows = frame.collect()
    assert [row.id for row in rows] == [1, 2]
    assert callable(frame.to_arrow)
    assert callable(frame.toPandas)
    assert callable(frame.collect)
    assert DataFrame.toPandas is DataFrame.to_pandas

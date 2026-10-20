from __future__ import annotations

from pathlib import Path

import _sm2_shared as sm2
import pyarrow as pa
import pytest

from repark.spark import functions as spark_functions


def test_arrow_c_stream_of_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupexport-self")
    frame = sm2._self_join(session)
    stream = pa.table(frame)
    assert stream.schema.names == ["id", "s", "v", "id", "s", "v"]
    assert stream.column(0).to_pylist() == [1, 2]
    assert stream.column(1).to_pylist() == ["a", "b"]
    assert stream.column(2).to_pylist() == [10, 20]
    assert stream.column(3).to_pylist() == [1, 2]
    assert stream.column(4).to_pylist() == ["a", "b"]
    assert stream.column(5).to_pylist() == [10, 20]
    session.stop()


def test_arrow_c_stream_of_mixed_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupexport-mixed")
    frame = sm2._mixed_join(session)
    stream = pa.table(frame)
    assert stream.schema.names == ["id", "s", "v", "id", "t"]
    session.stop()


def test_arrow_c_stream_of_empty_join_keeps_display_schema(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupexport-empty")
    frame = sm2._self_join(session).limit(0)
    stream = pa.table(frame)
    assert stream.schema.names == ["id", "s", "v", "id", "s", "v"]
    assert stream.num_rows == 0
    session.stop()


def test_arrow_c_stream_matches_to_arrow_on_duplicate_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupexport-parity")
    frame = sm2._self_join(session)
    stream = pa.table(frame)
    exported = frame.toArrow()
    assert stream.schema.names == exported.schema.names
    assert stream.to_pylist() == exported.to_pylist()
    session.stop()


def test_arrow_c_stream_of_plain_frame_is_unchanged(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupexport-plain")
    frame = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    stream = pa.table(frame)
    assert stream.schema.names == ["id", "s", "v"]
    assert stream.to_pylist() == [{"id": 1, "s": "a", "v": 10}]
    session.stop()


def test_raw_polars_consumers_of_duplicate_name_join_raise_duplicate_error(
    tmp_path: Path,
) -> None:
    pytest.importorskip("polars")
    import polars as pl

    session = sm2._open(tmp_path, "sm2-dupexport-rawpolars")
    frame = sm2._self_join(session)
    assert frame.columns == ["id", "s", "v", "id", "s", "v"]
    with pytest.raises(pl.exceptions.DuplicateError):
        pl.from_arrow(frame)
    with pytest.raises(pl.exceptions.DuplicateError):
        pl.DataFrame(frame)
    session.stop()


def test_to_polars_of_duplicate_name_join_disambiguates_with_suffix(
    tmp_path: Path,
) -> None:
    pytest.importorskip("polars")
    session = sm2._open(tmp_path, "sm2-dupexport-topolars")
    left = session.createDataFrame([(1, "x"), (2, "y")], ["id", "b"])
    frame = left.alias("l").join(
        left.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id")
    )
    converted = frame.to_polars()
    assert list(converted.columns) == ["id", "b", "id__1", "b__1"]
    assert converted.to_series(0).to_list() == [1, 2]
    assert converted.to_series(3).to_list() == ["x", "y"]
    session.stop()

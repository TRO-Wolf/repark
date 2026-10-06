from __future__ import annotations

from pathlib import Path

import _sm2_shared as sm2
import pyarrow as pa


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

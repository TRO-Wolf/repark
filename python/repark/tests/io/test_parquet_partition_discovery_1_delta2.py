from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark.errors import PySparkException
from repark.spark import ReparkSession

DOORS = ["parquet", "parquet_load"]
ANSI_MODES = ["true", "false"]
CASES = json.loads(
    (Path(__file__).parent / "parquet_partition_discovery_1_delta2_cases.json").read_text(
        encoding="utf-8"
    )
)
VIEW_FILTERS = {
    "V1_view_cast_outer_eq": ("CAST(p AS INT) = 1", "p = '1'"),
    "V2_view_eq_outer_cast": ("p = '1'", "CAST(p AS INT) = 1"),
    "V3_view_cast_outer_eq99": ("CAST(p AS INT) = 1", "p = '99'"),
    "V4_df_view_cast_outer_eq": ("CAST(p AS INT) = 1", "p = '1'"),
}


def _write_id_s(path: Path, rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.int64()),
            "s": pa.array([row[1] for row in rows], type=pa.string()),
        }
    )
    pq.write_table(table, path)


def _read_door(spark: ReparkSession, door: str, path: str) -> Any:
    if door == "parquet":
        return spark.read.parquet(path)
    return spark.read.format("parquet").load(path)


def _sorted_rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in frame.collect()), key=repr)


def _expected_rows(rows: list[list[Any]]) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in rows), key=repr)


def _where_all(frame: Any, predicates: list[str]) -> Any:
    for predicate in predicates:
        frame = frame.where(predicate)
    return frame


def _assert_case(frame_fn: Callable[[], Any], outcome: str, expected: Any) -> None:
    if outcome == "raises":
        with pytest.raises(PySparkException) as exc_info:
            frame_fn().collect()
        error = exc_info.value
        if expected is not None:
            assert error.getCondition() == expected or f"[{expected}]" in str(error)
        return
    assert _sorted_rows(frame_fn()) == _expected_rows(expected)


@pytest.fixture
def spark_factory() -> Iterator[list[ReparkSession]]:
    created: list[ReparkSession] = []
    yield created
    for session in created:
        session.stop()


def _make_session(created: list[ReparkSession], name: str, ansi: str) -> ReparkSession:
    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    session = (
        ReparkSession.builder.appName(name)
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    created.append(session)
    return session


@pytest.fixture(scope="module")
def d2_fixtures(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("parquet_partdisc_d2")
    for name, entries in CASES["layouts"].items():
        for rel, rows in entries:
            target = root / name / rel
            if rows is None:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"")
            else:
                _write_id_s(target, rows)
    return root


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(("layout", "expected_schema", "expected_rows"), CASES["layout_answers"])
def test_delta2_layouts_answer_as_spark(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_schema: str,
    expected_rows: list[list[Any]],
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-{door}-{ansi}-{layout}", ansi)
    df = _read_door(spark, door, str(d2_fixtures / layout))
    assert df.schema.simpleString() == expected_schema
    assert _sorted_rows(df) == _expected_rows(expected_rows)


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize(("layout", "expected_cond"), CASES["layout_refusals"])
def test_delta2_layouts_refuse_as_spark(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    ansi: str,
    layout: str,
    expected_cond: str,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-ref-{door}-{ansi}-{layout}", ansi)
    with pytest.raises(PySparkException) as exc_info:
        _read_door(spark, door, str(d2_fixtures / layout)).collect()
    assert exc_info.value.getCondition() == expected_cond


@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize(
    ("case", "layout", "predicates", "ansi", "outcome", "expected"), CASES["prune_cases"]
)
def test_delta2_constant_propagation_matrix(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    door: str,
    case: str,
    layout: str,
    predicates: list[str],
    ansi: str,
    outcome: str,
    expected: Any,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-cp-{door}-{ansi}-{case}", ansi)
    path = str(d2_fixtures / layout)
    _assert_case(lambda: _where_all(_read_door(spark, door, path), predicates), outcome, expected)


@pytest.mark.parametrize(("case", "layout", "ansi", "outcome", "expected"), CASES["view_cases"])
def test_delta2_constant_propagation_through_views(
    d2_fixtures: Path,
    spark_factory: list[ReparkSession],
    case: str,
    layout: str,
    ansi: str,
    outcome: str,
    expected: Any,
) -> None:
    spark = _make_session(spark_factory, f"partdisc-d2-view-{ansi}-{case}", ansi)
    source = spark.read.parquet(str(d2_fixtures / layout))
    inner, outer = VIEW_FILTERS[case]
    if case.startswith("V4"):
        source.where(inner).createOrReplaceTempView("w_d2")
        query = f"SELECT * FROM w_d2 WHERE {outer}"
    else:
        source.createOrReplaceTempView("t_d2")
        spark.sql(f"CREATE OR REPLACE TEMP VIEW v_d2 AS SELECT * FROM t_d2 WHERE {inner}")
        query = f"SELECT * FROM v_d2 WHERE {outer}"
    _assert_case(lambda: spark.sql(query), outcome, expected)

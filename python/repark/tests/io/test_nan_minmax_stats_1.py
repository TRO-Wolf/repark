from __future__ import annotations

import math
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark._native import PyReparkSession

_D_BITS = {"c": 0x7FF8000000000000, "s": 0xFFF8000000000000, "p": 0x7FF8DEADBEEF0001}
_F_BITS = {"c": 0x7FC00000, "s": 0xFFC00000, "p": 0x7FC00123}
_LAYOUTS = {"one": (1, 4, 4), "rg": (1, 6, 2), "many": (4, 2, 2)}
_SOURCES = [f"{layout}_{shape}" for layout in _LAYOUTS for shape in _D_BITS]
_ORDERED_SOURCES = [s for s in _SOURCES if not s.endswith("_s")]
_SIGN_SET_SOURCES = [s for s in _SOURCES if s.endswith("_s")]
_PART_2 = (
    "NAN-MINMAX-1 part 2: max and min order a sign-set NaN below every number "
    "(Arrow's total order); the statistics shortcut that hid it on main is gone"
)
_SELECTIVE_FILES = 100
_SELECTIVE_ROWS = 20
_SELECTIVE_GROUP = 10
_COMPARE = (
    "NAN-COMPARE-SPARK-1: a comparison orders a sign-set NaN below every number (Arrow's total "
    "order); the constant-column replacement that hid it on main is gone"
)
_NAN_GROUP_SETS = {
    "R_one": [["N", "N", 1.0, 2.0]],
    "R_many": [["N", "N", 1.0, 2.0], [3.0, 4.0, 5.5, 6.0]],
    "R_nullnan": [[None, "N", 1.0, 2.0]],
}
_NAN_GROUP_SPARK = {
    "R_one": {"gt5": [[1], [2]], "eqnan": [[2]]},
    "R_many": {"gt5": [[1], [2], [7], [8]], "eqnan": [[2]]},
    "R_nullnan": {"gt5": [[2]], "eqnan": [[1]]},
}
_NAN_GROUP_QUERIES = {
    "gt5": "SELECT id FROM nan_minmax_src WHERE d > 5.0 ORDER BY id",
    "eqnan": "SELECT count(*) FROM nan_minmax_src WHERE d = CAST('NaN' AS DOUBLE)",
}
_DYNAMIC_ROWS = 200_000
_RUNS = 10
_NULL_PARTITION_FILES = [[3.0, 4.0], [None, None, 1.0, 2.0], [5.0, 6.0]]
_NULL_GROUP_ROWS = 1_000_000
_NULL_GROUP_SIZE = 100_000
_NULL_PARTITION_CELLS = [
    ("three_files", "SELECT max(d), min(d) FROM nan_minmax_src", [[6.0, 1.0]]),
    ("three_files", "SELECT max(f), min(f) FROM nan_minmax_src", [[6.0, 1.0]]),
    ("three_files", "SELECT min(d), max(id) FROM nan_minmax_src", [[1.0, 8]]),
    ("one_file", "SELECT max(d), min(d) FROM nan_minmax_src", [[1000009.0, 100010.0]]),
    ("one_file", "SELECT max(f), min(f) FROM nan_minmax_src", [[1000009.0, 100010.0]]),
    ("one_file", "SELECT min(d), max(id) FROM nan_minmax_src", [[100010.0, 999999]]),
    ("one_file", "SELECT max(k), min(k) FROM nan_minmax_src WHERE id >= 0", [[1000009, 100010]]),
]
_AGGREGATE_KEY = "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown"
_UMBRELLA_KEY = "datafusion.optimizer.enable_dynamic_filter_pushdown"
_DYNAMIC_FILTER = "DynamicFilter"


def _column(count: int, shape: str, wide: bool) -> pa.Array:
    if wide:
        values = np.full(count, 1.0, dtype=np.float64)
        values[0::2] = np.array([_D_BITS[shape]], dtype=np.uint64).view(np.float64)[0]
        return pa.array(values, pa.float64())
    narrow = np.full(count, 1.0, dtype=np.float32)
    narrow[0::2] = np.array([_F_BITS[shape]], dtype=np.uint32).view(np.float32)[0]
    return pa.array(narrow, pa.float32())


def _write_source(root: Path, layout: str, shape: str) -> Path:
    files, rows, group = _LAYOUTS[layout]
    folder = root / f"{layout}_{shape}"
    folder.mkdir(parents=True)
    for index in range(files):
        first = 1 + index * rows
        table = pa.table(
            {
                "id": pa.array(range(first, first + rows), pa.int64()),
                "d": _column(rows, shape, True),
                "f": _column(rows, shape, False),
            }
        )
        pq.write_table(table, folder / f"part-{index}.parquet", row_group_size=group)
    return folder


@pytest.fixture(scope="module")
def sources(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("nan_minmax_sources")
    return {
        f"{layout}_{shape}": _write_source(root, layout, shape)
        for layout in _LAYOUTS
        for shape in _D_BITS
    }


@pytest.fixture
def native() -> Iterator[Any]:
    yield PyReparkSession.native()


def _token(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


def _rows(table: pa.Table) -> list[list[Any]]:
    return [[_token(v) for v in row.values()] for row in table.to_pylist()]


def _spark_rows(source: str) -> list[list[Any]]:
    files, rows, _ = _LAYOUTS[source.split("_")[0]]
    out = []
    for row_id in range(1, files * rows + 1):
        value = "NaN" if (row_id - 1) % 2 == 0 else 1.0
        out.append([row_id, value, value])
    return out


_SPARK_MINMAX = [["NaN", 1.0, "NaN", 1.0]]
_MINMAX = "SELECT max(d), min(d), max(f), min(f) FROM nan_minmax_src"


def _spark_answers(source: str) -> dict[str, list[list[Any]]]:
    rows = _spark_rows(source)
    nans = len(rows) // 2
    return {
        "rows": rows,
        "count": [[len(rows)]],
        "isnan_d": [[nans]],
        "isnan_f": [[nans]],
    }


def _queries(view: str) -> dict[str, str]:
    return {
        "rows": f"SELECT id, d, f FROM {view} ORDER BY id",
        "count": f"SELECT count(*) FROM {view}",
        "isnan_d": f"SELECT count(*) FROM {view} WHERE isnan(d)",
        "isnan_f": f"SELECT count(*) FROM {view} WHERE isnan(f)",
    }


def _spark_door(spark: ReparkSession, folder: Path, text: str) -> list[list[Any]]:
    spark.read.parquet(str(folder)).createOrReplaceTempView("nan_minmax_src")
    return _rows(spark.sql(text).toArrow())


def _native_door(native: Any, folder: Path, text: str) -> list[list[Any]]:
    native.create_or_replace_temp_view("nan_minmax_src", native.read_parquet(str(folder)))
    return _rows(pa.table(native.sql(text)))


@pytest.mark.parametrize("source", _SOURCES)
def test_spark_door_reads_equal_spark(
    spark: ReparkSession, sources: dict[str, Path], source: str
) -> None:
    answers = {
        name: _spark_door(spark, sources[source], text)
        for name, text in _queries("nan_minmax_src").items()
    }
    assert answers == _spark_answers(source)


@pytest.mark.parametrize("source", _SOURCES)
def test_native_door_reads_equal_spark(native: Any, sources: dict[str, Path], source: str) -> None:
    answers = {
        name: _native_door(native, sources[source], text)
        for name, text in _queries("nan_minmax_src").items()
    }
    assert answers == _spark_answers(source)


@pytest.mark.parametrize("source", _ORDERED_SOURCES)
def test_max_min_shortcut_equals_spark_on_both_doors(
    spark: ReparkSession, native: Any, sources: dict[str, Path], source: str
) -> None:
    assert _spark_door(spark, sources[source], _MINMAX) == _SPARK_MINMAX
    assert _native_door(native, sources[source], _MINMAX) == _SPARK_MINMAX


@pytest.mark.xfail(strict=True, reason=_PART_2)
@pytest.mark.parametrize("source", _SIGN_SET_SOURCES)
def test_max_min_over_a_sign_set_nan_equals_spark(
    spark: ReparkSession, sources: dict[str, Path], source: str
) -> None:
    assert _spark_door(spark, sources[source], _MINMAX) == _SPARK_MINMAX


@pytest.mark.xfail(strict=True, reason=_COMPARE)
def test_range_filter_over_a_constant_sign_set_file_equals_spark(
    spark: ReparkSession, sources: dict[str, Path]
) -> None:
    text = "SELECT id FROM nan_minmax_src WHERE d >= 1.0 ORDER BY id"
    assert _spark_door(spark, sources["one_s"], text) == [[1], [2], [3], [4]]


def _write_nan_group_set(root: Path, name: str) -> Path:
    folder = root / name
    folder.mkdir(parents=True)
    next_id = 1
    for index, values in enumerate(_NAN_GROUP_SETS[name]):
        d = pa.array(
            [math.nan if v == "N" else v for v in values],
            pa.float64(),
            mask=np.array([v is None for v in values]),
        )
        ids = pa.array(range(next_id, next_id + len(values)), pa.int64())
        next_id += len(values)
        table = pa.table({"id": ids, "d": d})
        pq.write_table(table, folder / f"part-{index:03d}.parquet", row_group_size=2)
    return folder


@pytest.mark.parametrize("name", list(_NAN_GROUP_SETS))
def test_a_row_group_without_footer_bounds_keeps_its_nan_rows_on_both_doors(
    spark: ReparkSession, native: Any, tmp_path: Path, name: str
) -> None:
    folder = _write_nan_group_set(tmp_path, name)
    first = pq.ParquetFile(folder / "part-000.parquet").metadata
    assert not first.row_group(0).column(1).statistics.has_min_max
    spark_door = {q: _spark_door(spark, folder, text) for q, text in _NAN_GROUP_QUERIES.items()}
    native_door = {q: _native_door(native, folder, text) for q, text in _NAN_GROUP_QUERIES.items()}
    assert spark_door == _NAN_GROUP_SPARK[name]
    assert native_door == _NAN_GROUP_SPARK[name]


def _write_spread_nan_set(root: Path) -> Path:
    folder = root / "dynamic"
    folder.mkdir()
    for index in range(9):
        d = 1000.0 + index + np.arange(_DYNAMIC_ROWS, dtype=np.float64) * 1e-6
        ids = np.arange(index * _DYNAMIC_ROWS, (index + 1) * _DYNAMIC_ROWS, dtype=np.int64)
        pq.write_table(pa.table({"id": ids, "d": d}), folder / f"high-{index}.parquet")
    low = np.arange(_DYNAMIC_ROWS, dtype=np.float64) / _DYNAMIC_ROWS
    low[-1] = math.nan
    ids = np.arange(9 * _DYNAMIC_ROWS, 10 * _DYNAMIC_ROWS, dtype=np.int64)
    pq.write_table(pa.table({"id": ids, "d": low}), folder / "low.parquet")
    return folder


def test_max_over_many_files_with_a_nan_in_a_spread_file_equals_spark_on_both_doors(
    spark: ReparkSession, native: Any, tmp_path: Path
) -> None:
    folder = _write_spread_nan_set(tmp_path)
    text = "SELECT max(d) FROM nan_minmax_src"
    spark_door = [_spark_door(spark, folder, text) for _ in range(_RUNS)]
    native_door = [_native_door(native, folder, text) for _ in range(_RUNS)]
    assert spark_door == [[["NaN"]]] * _RUNS
    assert native_door == [[["NaN"]]] * _RUNS


def _write_null_partition_sets(root: Path) -> dict[str, Path]:
    three = root / "three_files"
    three.mkdir()
    next_id = 1
    for index, values in enumerate(_NULL_PARTITION_FILES):
        mask = np.array([v is None for v in values])
        ids = pa.array(range(next_id, next_id + len(values)), pa.int64())
        next_id += len(values)
        table = pa.table(
            {
                "id": ids,
                "d": pa.array(values, pa.float64(), mask=mask),
                "f": pa.array(values, pa.float32(), mask=mask),
            }
        )
        pq.write_table(table, three / f"part-{index:03d}.parquet", row_group_size=2)
    one = root / "one_file"
    one.mkdir()
    values = np.arange(_NULL_GROUP_ROWS, dtype=np.int64) + 10
    mask = np.zeros(_NULL_GROUP_ROWS, dtype=bool)
    mask[:_NULL_GROUP_SIZE] = True
    table = pa.table(
        {
            "id": pa.array(np.arange(_NULL_GROUP_ROWS, dtype=np.int64)),
            "d": pa.array(values.astype(np.float64), mask=mask),
            "f": pa.array(values.astype(np.float32), mask=mask),
            "k": pa.array(values, mask=mask),
        }
    )
    pq.write_table(table, one / "part-000.parquet", row_group_size=_NULL_GROUP_SIZE)
    return {"three_files": three, "one_file": one}


@pytest.fixture(scope="module")
def null_partition_sets(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    return _write_null_partition_sets(tmp_path_factory.mktemp("nan_minmax_null_partition"))


@pytest.mark.parametrize(("name", "text", "expected"), _NULL_PARTITION_CELLS)
def test_an_all_null_row_group_keeps_max_and_min_equal_to_spark_on_both_doors(
    spark: ReparkSession,
    native: Any,
    null_partition_sets: dict[str, Path],
    name: str,
    text: str,
    expected: list[list[Any]],
) -> None:
    folder = null_partition_sets[name]
    spark_door = [_spark_door(spark, folder, text) for _ in range(_RUNS)]
    native_door = [_native_door(native, folder, text) for _ in range(_RUNS)]
    assert spark_door == [expected] * _RUNS
    assert native_door == [expected] * _RUNS


def _plan(rows: list[list[Any]]) -> str:
    return "\n".join(str(value) for row in rows for value in row)


def test_only_the_specific_key_turns_the_aggregate_dynamic_filter_on_on_both_doors(
    spark: ReparkSession, native: Any, null_partition_sets: dict[str, Path]
) -> None:
    folder = null_partition_sets["three_files"]
    explain = "EXPLAIN ANALYZE SELECT max(d), min(d) FROM nan_minmax_src"
    assert _DYNAMIC_FILTER not in _plan(_spark_door(spark, folder, explain))
    assert _DYNAMIC_FILTER not in _plan(_native_door(native, folder, explain))
    with pytest.raises(Exception, match=_AGGREGATE_KEY):
        spark.conf.set(_UMBRELLA_KEY, "true")
    with pytest.raises(Exception, match=_AGGREGATE_KEY):
        pa.table(native.sql(f"SET {_UMBRELLA_KEY} = true"))
    assert _DYNAMIC_FILTER not in _plan(_spark_door(spark, folder, explain))
    assert _DYNAMIC_FILTER not in _plan(_native_door(native, folder, explain))
    spark.conf.set(_UMBRELLA_KEY, "false")
    pa.table(native.sql(f"SET {_UMBRELLA_KEY} = false"))
    assert _DYNAMIC_FILTER not in _plan(_spark_door(spark, folder, explain))
    assert _DYNAMIC_FILTER not in _plan(_native_door(native, folder, explain))
    spark.conf.set(_AGGREGATE_KEY, "true")
    pa.table(native.sql(f"SET {_AGGREGATE_KEY} = true"))
    assert _DYNAMIC_FILTER in _plan(_spark_door(spark, folder, explain))
    assert _DYNAMIC_FILTER in _plan(_native_door(native, folder, explain))


def test_format_load_equals_spark(spark: ReparkSession, sources: dict[str, Path]) -> None:
    frame = spark.read.format("parquet").load(str(sources["one_c"])).orderBy("id")
    assert _rows(frame.toArrow()) == _spark_rows("one_c")


@pytest.mark.parametrize("source", ["one_c", "rg_p"])
def test_native_create_external_table_equals_spark(
    native: Any, sources: dict[str, Path], source: str
) -> None:
    native.register_memory_catalog("nan_minmax_wh", str(sources[source].parent))
    location = f"{sources[source]}/"
    pa.table(
        native.sql(f"CREATE EXTERNAL TABLE nan_minmax_ext STORED AS PARQUET LOCATION '{location}'")
    )
    table = pa.table(native.sql("SELECT id, d, f FROM nan_minmax_ext ORDER BY id"))
    assert _rows(table) == _spark_rows(source)
    top = pa.table(native.sql("SELECT max(d), min(d), max(f), min(f) FROM nan_minmax_ext"))
    assert _rows(top) == _SPARK_MINMAX


def _stored(folder: Path) -> list[list[Any]]:
    files = sorted(folder.rglob("*.parquet"))
    assert files, f"no parquet files under {folder}"
    merged = pa.concat_tables([pq.read_table(p).select(["id", "d", "f"]) for p in files])
    return _rows(merged.sort_by("id"))


@pytest.mark.parametrize("source", _SOURCES)
def test_round_trip_write_equals_spark(
    spark: ReparkSession, sources: dict[str, Path], tmp_path: Path, source: str
) -> None:
    out = tmp_path / "rt"
    spark.read.parquet(str(sources[source])).coalesce(1).write.parquet(str(out))
    assert _stored(out) == _spark_rows(source)
    back = spark.read.parquet(str(out)).orderBy("id")
    assert _rows(back.toArrow()) == _spark_rows(source)


@pytest.mark.parametrize("source", ["one_c", "rg_s", "many_p"])
def test_iceberg_ingestion_write_equals_spark(
    spark_with_warehouse: ReparkSession, sources: dict[str, Path], tmp_path: Path, source: str
) -> None:
    session = spark_with_warehouse
    session.sql(
        "CREATE TABLE mem.ns.nan_minmax_ingest (id BIGINT, d DOUBLE, f FLOAT) USING iceberg "
        "TBLPROPERTIES ('format-version' = '2')"
    )
    session.read.parquet(str(sources[source])).writeTo("mem.ns.nan_minmax_ingest").append()
    back = session.sql("SELECT id, d, f FROM mem.ns.nan_minmax_ingest ORDER BY id").toArrow()
    assert _rows(back) == _spark_rows(source)
    data = [p for p in tmp_path.rglob("*.parquet") if "nan_minmax_ingest" in str(p)]
    assert data
    stored = _rows(
        pa.concat_tables([pq.read_table(p).select(["id", "d", "f"]) for p in data]).sort_by("id")
    )
    assert stored == _spark_rows(source)


def _pruning_metrics(name: str, text: str) -> list[tuple[int, int]]:
    return [
        (int(total), int(matched))
        for total, matched in re.findall(rf"{name}=(\d+) total → (\d+) matched", text)
    ]


def test_selective_filter_over_many_files_scans_one_row_group(
    spark: ReparkSession, tmp_path: Path
) -> None:
    folder = tmp_path / "selective"
    folder.mkdir()
    for index in range(_SELECTIVE_FILES):
        first = index * _SELECTIVE_ROWS
        ids = np.arange(first, first + _SELECTIVE_ROWS, dtype=np.int64)
        table = pa.table({"id": ids, "d": ids.astype(np.float64)})
        pq.write_table(table, folder / f"part-{index:03d}.parquet", row_group_size=_SELECTIVE_GROUP)
    spark.read.parquet(str(folder)).createOrReplaceTempView("nan_minmax_many")
    last = _SELECTIVE_FILES * _SELECTIVE_ROWS - _SELECTIVE_GROUP
    query = f"SELECT count(*) FROM nan_minmax_many WHERE d >= {last}.0"
    assert _rows(spark.sql(query).toArrow()) == [[_SELECTIVE_GROUP]]
    plan = "\n".join(str(v) for row in spark.sql(f"EXPLAIN ANALYZE {query}").collect() for v in row)
    row_groups = _pruning_metrics("row_groups_pruned_statistics", plan)
    assert row_groups, plan
    assert sum(matched for _, matched in row_groups) == 1, plan
    files = _pruning_metrics("files_ranges_pruned_statistics", plan)
    assert files, plan
    assert sum(matched for _, matched in files) == 1, plan

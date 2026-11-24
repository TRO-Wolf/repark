from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark._native import PyReparkSession
from repark.spark import functions
from repark.spark.sql import DataFrame, Window

_MIN = -9223372036854775808
_MAX = 9223372036854775807
_ANSI = ["true", "false"]
_ID_SOURCES = {
    "leading_zero": ([["1", "2", "3", "04"]], None),
    "mixed_width": ([["10", "3", "25", "7"]], None),
    "wrong_range": ([["05", "10", "6", "055"]], None),
    "many": ([["9", "10", "100", "08"], ["011", "2", "30", "7"]], 2),
}
_FULL_SOURCES = {
    "full_one": ([([0, 1, 2, 3, 4], [0, _MAX, _MIN, 5, None])], None),
    "full_split": ([([0, 1], [_MIN, -1]), ([2, 3, 4], [0, 5, _MAX])], 2),
}
_DEDUP_SPARK = {
    "leading_zero": [[1, 1], [2, 2], [3, 3], [4, 4]],
    "mixed_width": [[3, 2], [7, 4], [10, 1], [25, 3]],
    "many": [[2, 2], [7, 4], [8, 4], [9, 1], [10, 2], [11, 1], [30, 3], [100, 3]],
}
_SQL_SPARK = {
    ("mixed_width", "SELECT id, v FROM cast_stats_src WHERE CAST(id AS INT) BETWEEN 5 AND 20"): [
        ["10", 1],
        ["7", 4],
    ],
    ("many", "SELECT id, v FROM cast_stats_src WHERE CAST(id AS INT) BETWEEN 8 AND 30"): [
        ["011", 1],
        ["08", 4],
        ["10", 2],
        ["30", 3],
        ["9", 1],
    ],
    ("wrong_range", "SELECT max(CAST(id AS INT)), min(CAST(id AS INT)) FROM cast_stats_src"): [
        [55, 5]
    ],
    ("wrong_range", "SELECT v FROM cast_stats_src WHERE CAST(id AS INT) = 10"): [[2]],
    ("wrong_range", "SELECT v FROM cast_stats_src WHERE CAST(id AS INT) > 8"): [[2], [4]],
    ("wrong_range", "SELECT v FROM cast_stats_src WHERE CAST(id AS INT) IN (10, 55)"): [[2], [4]],
    ("wrong_range", "SELECT v FROM cast_stats_src WHERE CAST(id AS INT) BETWEEN 9 AND 60"): [
        [2],
        [4],
    ],
}
_MINMAX_SPARK = {"mixed_width": [[25, 3]], "wrong_range": [[55, 5]], "many": [[100, 2]]}
_FULL_QUERIES = {
    "gt": "SELECT rid FROM cast_stats_full WHERE x > 0",
    "eq": "SELECT rid FROM cast_stats_full WHERE x = 5",
    "cast_gt": "SELECT rid FROM cast_stats_full WHERE CAST(x AS BIGINT) > 0",
}
_FULL_SPARK = {
    "full_one": {"gt": [[1], [3]], "eq": [[3]], "cast_gt": [[1], [3]]},
    "full_split": {"gt": [[3], [4]], "eq": [[3]], "cast_gt": [[3], [4]]},
}
_NUMBERS = {"n": pa.array([9, 10, 11], pa.int32()), "x": pa.array([-1, 0, 1], pa.int64())}
_FOLDED = "11 as max(w), 9 as min(w)"
_NAN_VALUES = [5.0, float("nan"), 10.0]
_NAN_QUERIES = {
    "int_eq": "SELECT id, CAST(d AS INT) c FROM cast_stats_nan WHERE CAST(d AS INT) = 0",
    "bigint_le": "SELECT id FROM cast_stats_nan WHERE CAST(d AS BIGINT) <= 0",
    "smallint_lt": "SELECT id FROM cast_stats_nan WHERE CAST(d AS SMALLINT) < 3",
    "int_in": "SELECT id FROM cast_stats_nan WHERE CAST(d AS INT) IN (0, 100)",
    "float_column": "SELECT id FROM cast_stats_nan WHERE CAST(f AS INT) = 0",
    "count": "SELECT count(*) FROM cast_stats_nan WHERE CAST(d AS INT) = 0",
}
_NAN_CELLS = [*sorted(_NAN_QUERIES), "dataframe"]
_NAN_SPARK_OFF = {
    "int_eq": [[2, 0]],
    "bigint_le": [[2]],
    "smallint_lt": [[2]],
    "int_in": [[2]],
    "float_column": [[2]],
    "count": [[1]],
    "dataframe": [[2]],
}
_NAN_CONTROLS = {
    "SELECT id FROM cast_stats_nan WHERE d > 100": [],
    "SELECT id FROM cast_stats_nan WHERE CAST(f AS DOUBLE) > 7": [[2], [3]],
}
_NARROWING_FLOAT = {
    ("nan", "CAST(d AS FLOAT) > 100"): [[2]],
    ("nan", "CAST(CAST(d AS FLOAT) AS DOUBLE) > 100"): [[2]],
    ("nan", "CAST(d AS FLOAT) >= 11"): [[2]],
    ("nan", "CAST(d AS FLOAT) = CAST('NaN' AS FLOAT)"): [[2]],
    ("nan_dictionary", "CAST(d AS FLOAT) > 100"): [[2]],
    ("nan_null", "CAST(d AS FLOAT) > 100"): [[1]],
}
_FLOAT_PUSHDOWN_CONTROLS = {
    ("nan", "CAST(f AS DOUBLE) > 100"): [],
    ("nan_dictionary", "d > 100"): [],
    ("nan_null", "d > 100"): [],
}


def _ids_table(ids: list[str]) -> pa.Table:
    return pa.table({"id": pa.array(ids), "v": pa.array(range(1, len(ids) + 1), pa.int64())})


def _full_table(rids: list[int], values: list[int | None]) -> pa.Table:
    return pa.table({"rid": pa.array(rids, pa.int32()), "x": pa.array(values, pa.int64())})


def _write(folder: Path, tables: list[pa.Table], row_group_size: int | None) -> Path:
    folder.mkdir(parents=True)
    for index, table in enumerate(tables):
        pq.write_table(table, folder / f"part-{index}.parquet", row_group_size=row_group_size)
    return folder


def _write_dictionary(folder: Path) -> Path:
    folder.mkdir(parents=True)
    values = [5.0, float("nan"), 10.0, 5.0]
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int32()),
            "d": pa.array(values, pa.float64()),
            "f": pa.array(values, pa.float32()),
        }
    )
    pq.write_table(table, folder / "part-0.parquet", row_group_size=100, use_dictionary=True)
    return folder


@pytest.fixture(scope="module")
def sources(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("cast_stats")
    folders = {
        name: _write(root / name, [_ids_table(ids) for ids in files], group)
        for name, (files, group) in _ID_SOURCES.items()
    }
    for name, (files, group) in _FULL_SOURCES.items():
        tables = [_full_table(rids, values) for rids, values in files]
        folders[name] = _write(root / name, tables, group)
    folders["numbers"] = _write(root / "numbers", [pa.table(_NUMBERS)], None)
    nan = {
        "id": pa.array([1, 2, 3], pa.int32()),
        "d": pa.array(_NAN_VALUES, pa.float64()),
        "f": pa.array(_NAN_VALUES, pa.float32()),
    }
    folders["nan"] = _write(root / "nan", [pa.table(nan)], 100)
    folders["nan_dictionary"] = _write_dictionary(root / "nan_dictionary")
    nan_null = [float("nan"), None, 3.0]
    nulls = {
        "id": pa.array([1, 2, 3], pa.int32()),
        "d": pa.array(nan_null, pa.float64()),
        "f": pa.array(nan_null, pa.float32()),
    }
    folders["nan_null"] = _write(root / "nan_null", [pa.table(nulls)], 100)
    return folders


@pytest.fixture
def native() -> Iterator[Any]:
    yield PyReparkSession.native()


def _rows(table: pa.Table) -> list[list[Any]]:
    return sorted(([list(row.values()) for row in table.to_pylist()]), key=repr)


def _sorted(rows: list[list[Any]]) -> list[list[Any]]:
    return sorted(rows, key=repr)


def _cast_ids(spark: ReparkSession, folder: Path) -> DataFrame:
    return spark.read.parquet(str(folder)).withColumn("id", functions.col("id").cast("int"))


def _dedup(frame: DataFrame) -> DataFrame:
    window = Window.partitionBy("id").orderBy(functions.col("v").desc())
    ranked = frame.withColumn("row_num", functions.row_number().over(window))
    return ranked.filter(functions.col("row_num") == 1).drop("row_num")


def _spark_sql(spark: ReparkSession, folder: Path, view: str, text: str) -> pa.Table:
    spark.read.parquet(str(folder)).createOrReplaceTempView(view)
    return spark.sql(text).toArrow()


def _plan(table: pa.Table) -> str:
    return "\n".join(str(value) for row in table.to_pylist() for value in row.values())


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("source", sorted(_DEDUP_SPARK))
def test_row_number_dedup_over_a_string_id_cast_to_int_equals_spark(
    spark: ReparkSession, sources: dict[str, Path], source: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    table = _dedup(_cast_ids(spark, sources[source])).toArrow()
    assert table.schema.field("id").type == pa.int32()
    assert _rows(table) == _sorted(_DEDUP_SPARK[source])


@pytest.mark.parametrize("ansi", _ANSI)
def test_equality_and_in_after_the_dedup_equal_spark(
    spark: ReparkSession, sources: dict[str, Path], ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    equal = _dedup(_cast_ids(spark, sources["leading_zero"])).filter(functions.col("id") == 4)
    member = _dedup(_cast_ids(spark, sources["many"])).filter(functions.col("id").isin(8, 100))
    assert _rows(equal.toArrow()) == [[4, 4]]
    assert _rows(member.toArrow()) == _sorted([[8, 4], [100, 3]])


@pytest.mark.parametrize("ansi", _ANSI)
def test_dataframe_between_on_the_cast_equals_spark(
    spark: ReparkSession, sources: dict[str, Path], ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    frame = spark.read.parquet(str(sources["mixed_width"]))
    table = frame.filter(functions.col("id").cast("int").between(5, 20)).toArrow()
    assert table.schema.field("id").type == pa.string()
    assert _rows(table) == _sorted([["10", 1], ["7", 4]])


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize(("source", "text"), sorted(_SQL_SPARK))
def test_sql_over_a_string_id_cast_to_int_equals_spark(
    spark: ReparkSession, sources: dict[str, Path], source: str, text: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    table = _spark_sql(spark, sources[source], "cast_stats_src", text)
    assert _rows(table) == _sorted(_SQL_SPARK[(source, text)])


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("source", sorted(_MINMAX_SPARK))
def test_max_and_min_of_a_string_id_cast_to_int_read_the_rows(
    spark: ReparkSession, sources: dict[str, Path], source: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    table = (
        _cast_ids(spark, sources[source]).agg(functions.max("id"), functions.min("id")).toArrow()
    )
    assert [field.type for field in table.schema] == [pa.int32(), pa.int32()]
    assert _rows(table) == _MINMAX_SPARK[source]


@pytest.mark.parametrize("ansi", _ANSI)
def test_max_and_min_of_casts_that_reorder_values_read_the_rows(
    spark: ReparkSession, sources: dict[str, Path], ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    frame = spark.read.parquet(str(sources["numbers"]))
    text = frame.select(functions.col("n").cast("string").alias("t")).agg(
        functions.max("t"), functions.min("t")
    )
    flag = frame.select(functions.col("x").cast("boolean").alias("b")).agg(
        functions.max("b"), functions.min("b")
    )
    assert _rows(text.toArrow()) == [["9", "10"]]
    assert _rows(flag.toArrow()) == [[True, False]]


@pytest.mark.parametrize("ansi", _ANSI)
def test_an_order_keeping_cast_keeps_its_statistics(
    spark: ReparkSession, sources: dict[str, Path], ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    folder = sources["numbers"]
    query = "SELECT max(w), min(w) FROM (SELECT CAST(n AS BIGINT) AS w FROM cast_stats_src)"
    assert _rows(_spark_sql(spark, folder, "cast_stats_src", query)) == [[11, 9]]
    plan = _plan(_spark_sql(spark, folder, "cast_stats_src", f"EXPLAIN {query}"))
    assert _FOLDED in plan
    reordered = "SELECT max(t) FROM (SELECT CAST(n AS STRING) AS t FROM cast_stats_src)"
    assert "AggregateExec" in _plan(
        _spark_sql(spark, folder, "cast_stats_src", f"EXPLAIN {reordered}")
    )


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("query", sorted(_FULL_QUERIES))
@pytest.mark.parametrize("source", sorted(_FULL_SPARK))
def test_filters_over_full_range_bigint_statistics_equal_spark(
    spark: ReparkSession, sources: dict[str, Path], source: str, query: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    table = _spark_sql(spark, sources[source], "cast_stats_full", _FULL_QUERIES[query])
    assert table.schema.field("rid").type == pa.int32()
    assert _rows(table) == _FULL_SPARK[source][query]


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("source", sorted(_FULL_SPARK))
def test_dataframe_filter_over_full_range_bigint_statistics_equals_spark(
    spark: ReparkSession, sources: dict[str, Path], source: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    frame = spark.read.parquet(str(sources[source])).filter(functions.col("x") > 0).select("rid")
    assert _rows(frame.toArrow()) == _FULL_SPARK[source]["gt"]


@pytest.mark.parametrize("source", sorted(_FULL_SPARK))
def test_native_door_reads_full_range_bigint_statistics(
    native: Any, sources: dict[str, Path], source: str
) -> None:
    native.create_or_replace_temp_view("cast_stats_full", native.read_parquet(str(sources[source])))
    for query, text in _FULL_QUERIES.items():
        assert _rows(pa.table(native.sql(text))) == _FULL_SPARK[source][query]


def test_native_door_reads_a_string_id_cast_to_int(native: Any, sources: dict[str, Path]) -> None:
    native.create_or_replace_temp_view("cast_stats_src", native.read_parquet(str(sources["many"])))
    text = "SELECT max(CAST(id AS INT)), min(CAST(id AS INT)) FROM cast_stats_src"
    assert _rows(pa.table(native.sql(text))) == [[100, 2]]
    between = "SELECT id FROM cast_stats_src WHERE CAST(id AS INT) BETWEEN 8 AND 30"
    assert _rows(pa.table(native.sql(between))) == _sorted([["011"], ["08"], ["10"], ["30"], ["9"]])


def _nan_frame(spark: ReparkSession, folder: Path, query: str) -> DataFrame:
    frame = spark.read.parquet(str(folder))
    if query == "dataframe":
        return frame.filter(functions.col("d").cast("int") == 0).select("id")
    frame.createOrReplaceTempView("cast_stats_nan")
    return spark.sql(_NAN_QUERIES[query])


@pytest.mark.parametrize("ansi", ("true",))
@pytest.mark.parametrize("query", _NAN_CELLS)
def test_a_float_cast_to_an_integer_reads_the_nan_row_its_footer_leaves_out(
    spark: ReparkSession, sources: dict[str, Path], query: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    with pytest.raises(Exception, match="NaN"):
        _nan_frame(spark, sources["nan"], query).toArrow()


@pytest.mark.parametrize("query", _NAN_CELLS)
def test_an_ansi_off_float_cast_to_an_integer_answers_spark_rows(
    spark: ReparkSession, sources: dict[str, Path], query: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    assert _rows(_nan_frame(spark, sources["nan"], query).toArrow()) == _NAN_SPARK_OFF[query]


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("text", sorted(_NAN_CONTROLS))
def test_uncast_and_float_to_double_filters_over_the_nan_file_equal_spark(
    spark: ReparkSession, sources: dict[str, Path], text: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    table = _spark_sql(spark, sources["nan"], "cast_stats_nan", text)
    assert _rows(table) == _NAN_CONTROLS[text]


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize(("source", "predicate"), sorted(_NARROWING_FLOAT))
def test_a_double_cast_to_float_reads_the_nan_row_its_footer_leaves_out(
    spark: ReparkSession, sources: dict[str, Path], source: str, predicate: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    text = f"SELECT id FROM cast_stats_float WHERE {predicate}"
    table = _spark_sql(spark, sources[source], "cast_stats_float", text)
    assert _rows(table) == _NARROWING_FLOAT[(source, predicate)]


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize(("source", "predicate"), sorted(_FLOAT_PUSHDOWN_CONTROLS))
def test_uncast_and_widening_float_filters_follow_spark_pushdown(
    spark: ReparkSession, sources: dict[str, Path], source: str, predicate: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    text = f"SELECT id FROM cast_stats_float WHERE {predicate}"
    table = _spark_sql(spark, sources[source], "cast_stats_float", text)
    assert _rows(table) == _FLOAT_PUSHDOWN_CONTROLS[(source, predicate)]

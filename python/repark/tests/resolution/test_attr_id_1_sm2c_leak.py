from __future__ import annotations

import json
from pathlib import Path

import _sm2_shared as sm2
import pyarrow.parquet as pa_pq
import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.spark import functions as spark_functions
from repark.spark.dataframe import DataFrame


def _self_drop(session: ReparkSession) -> DataFrame:
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    return (
        left.alias("l")
        .join(left.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id"))
        .drop(spark_functions.col("r.id"))
        .drop(spark_functions.col("r.s"))
        .drop(spark_functions.col("r.v"))
    )


def _join_drop(session: ReparkSession) -> DataFrame:
    left = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "s"])
    right = session.createDataFrame([(2, "x"), (3, "y"), (4, "z")], ["id", "t"])
    return left.join(right, left.id == right.id).drop(right.id)


def _part_file(root: Path, suffix: str) -> Path:
    parts = sorted(root.rglob(f"*{suffix}"))
    assert len(parts) == 1
    return parts[0]


def test_parquet_write_of_dropped_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-parquet")
    frame = _self_drop(session)
    assert frame.columns == ["id", "s", "v"]
    target = tmp_path / "drop_parquet"
    frame.write.mode("overwrite").parquet(str(target))
    assert pa_pq.read_schema(_part_file(target, ".parquet")).names == ["id", "s", "v"]
    rows = pa_pq.read_table(target).to_pylist()
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 1, "s": "a", "v": 10},
        {"id": 2, "s": "b", "v": 20},
    ]
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_csv_write_of_dropped_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-csv")
    frame = _self_drop(session)
    target = tmp_path / "drop_csv"
    frame.write.mode("overwrite").option("header", "true").csv(str(target))
    lines = _part_file(target, ".csv").read_text(encoding="utf-8").splitlines()
    assert lines[0] == "id,s,v"
    assert sorted(lines[1:]) == ["1,a,10", "2,b,20"]
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_json_write_of_dropped_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-json")
    frame = _self_drop(session)
    target = tmp_path / "drop_json"
    frame.write.mode("overwrite").json(str(target))
    lines = _part_file(target, ".json").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in sorted(lines)] == [
        {"id": 1, "s": "a", "v": 10},
        {"id": 2, "s": "b", "v": 20},
    ]
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_orc_write_of_dropped_self_join_still_refuses_the_format(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-orc")
    frame = _self_drop(session)
    target = tmp_path / "drop_orc"
    refused = sm2._refusal_of(lambda: frame.write.mode("overwrite").format("orc").save(str(target)))
    assert "NOT_IMPLEMENTED" in str(refused).splitlines()[0]
    assert not target.exists()
    session.stop()


def test_text_write_of_dropped_single_string_column_writes_values(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-text")
    frame = _self_drop(session).drop("id").drop("v")
    assert frame.columns == ["s"]
    target = tmp_path / "drop_text"
    frame.write.mode("overwrite").text(str(target))
    leaves = sorted(
        path
        for path in target.rglob("*")
        if path.is_file() and not path.name.startswith((".", "_"))
    )
    assert len(leaves) >= 1
    lines = sorted(
        line for leaf in leaves for line in leaf.read_text(encoding="utf-8").splitlines()
    )
    assert lines == ["a", "b"]
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_text_write_of_dropped_frame_reports_the_display_column_name(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-textmsg")
    frame = _self_drop(session).drop("s").drop("v")
    assert frame.columns == ["id"]
    refused = sm2._refusal_of(
        lambda: frame.write.mode("overwrite").text(str(tmp_path / "drop_text_int"))
    )
    assert "`id`" in str(refused).splitlines()[0]
    assert "__repark_" not in str(refused)
    session.stop()


def test_save_as_table_of_dropped_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-save")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    frame = _self_drop(session)
    frame.write.mode("overwrite").saveAsTable("sc.ns.dropt")
    assert session.table("sc.ns.dropt").columns == ["id", "s", "v"]
    rows = session.sql("SELECT * FROM sc.ns.dropt ORDER BY id").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10), (2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_save_as_table_append_of_dropped_self_join_matches_by_name(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-saveappend")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.dropapp (id INT, s STRING, v INT) USING iceberg").collect()
    frame = _self_drop(session)
    frame.write.mode("append").saveAsTable("sc.ns.dropapp")
    rows = session.sql("SELECT * FROM sc.ns.dropapp ORDER BY id").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10), (2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_write_to_create_of_dropped_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-v2create")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    frame = _self_drop(session)
    frame.writeTo("sc.ns.v2drop").create()
    assert session.table("sc.ns.v2drop").columns == ["id", "s", "v"]
    rows = session.sql("SELECT * FROM sc.ns.v2drop ORDER BY id").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10), (2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_write_to_replace_of_dropped_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-v2replace")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.v2decoy (x INT) USING iceberg").collect()
    frame = _self_drop(session)
    frame.writeTo("sc.ns.v2decoy").replace()
    assert session.table("sc.ns.v2decoy").columns == ["id", "s", "v"]
    rows = session.sql("SELECT * FROM sc.ns.v2decoy ORDER BY id").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10), (2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_write_to_append_of_dropped_self_join_matches_by_name(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-v2append")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.v2app (v INT, s STRING, id INT) USING iceberg").collect()
    frame = _self_drop(session)
    frame.writeTo("sc.ns.v2app").append()
    rows = session.sql("SELECT id, s, v FROM sc.ns.v2app ORDER BY id").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10), (2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_insert_into_of_dropped_self_join_writes_positionally_like_spark(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-insert")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.xyz (x INT, y STRING, z INT) USING iceberg").collect()
    frame = _self_drop(session)
    frame.write.mode("append").insertInto("sc.ns.xyz")
    rows = session.sql("SELECT * FROM sc.ns.xyz ORDER BY x").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10), (2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_temp_view_of_dropped_self_join_answers_select_id(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-view")
    frame = _self_drop(session)
    frame.createOrReplaceTempView("dropv")
    assert session.sql("SELECT * FROM dropv").columns == ["id", "s", "v"]
    rows = session.sql("SELECT id FROM dropv").collect()
    assert sorted(tuple(row) for row in rows) == [(1,), (2,)]
    session.stop()


def test_create_temp_view_of_dropped_self_join_answers_select_id(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-viewcreate")
    frame = _self_drop(session)
    frame.createTempView("dropvc")
    rows = session.sql("SELECT id FROM dropvc").collect()
    assert sorted(tuple(row) for row in rows) == [(1,), (2,)]
    session.stop()


def test_global_temp_views_of_dropped_self_join_still_unsupported(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-global")
    frame = _self_drop(session)
    with pytest.raises(UnsupportedOperationException, match="global_temp"):
        frame.createGlobalTempView("dropg")
    with pytest.raises(UnsupportedOperationException, match="global_temp"):
        frame.createOrReplaceGlobalTempView("dropg2")
    session.stop()


def test_parquet_write_of_dropped_join_key_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-s2parquet")
    frame = _join_drop(session)
    assert frame.columns == ["id", "s", "t"]
    target = tmp_path / "s2_parquet"
    frame.write.mode("overwrite").parquet(str(target))
    assert pa_pq.read_schema(_part_file(target, ".parquet")).names == ["id", "s", "t"]
    rows = pa_pq.read_table(target).to_pylist()
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 2, "s": "b", "t": "x"},
        {"id": 3, "s": "c", "t": "y"},
    ]
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_temp_view_of_dropped_join_key_answers_select_id(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-s2view")
    frame = _join_drop(session)
    frame.createOrReplaceTempView("s2v")
    assert session.sql("SELECT * FROM s2v").columns == ["id", "s", "t"]
    rows = session.sql("SELECT id FROM s2v").collect()
    assert sorted(tuple(row) for row in rows) == [(2,), (3,)]
    session.stop()


def test_save_as_table_of_dropped_join_key_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-s2save")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    frame = _join_drop(session)
    frame.write.mode("overwrite").saveAsTable("sc.ns.s2t")
    assert session.table("sc.ns.s2t").columns == ["id", "s", "t"]
    rows = session.sql("SELECT * FROM sc.ns.s2t ORDER BY id").collect()
    assert [tuple(row) for row in rows] == [(2, "b", "x"), (3, "c", "y")]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_parquet_write_of_filtered_dropped_join_carries_display_names(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2c-leak-filter")
    frame = _join_drop(session).filter("id > 0")
    assert frame.columns == ["id", "s", "t"]
    target = tmp_path / "filt_parquet"
    frame.write.mode("overwrite").parquet(str(target))
    assert pa_pq.read_schema(_part_file(target, ".parquet")).names == ["id", "s", "t"]
    rows = pa_pq.read_table(target).to_pylist()
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 2, "s": "b", "t": "x"},
        {"id": 3, "s": "c", "t": "y"},
    ]
    sm2._assert_no_twin_bytes(target)
    session.stop()

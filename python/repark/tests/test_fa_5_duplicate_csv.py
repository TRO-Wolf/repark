from __future__ import annotations

from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as spark_functions
from repark.spark.dataframe import DataFrame

SELF_HEADER = "id,s,v,id,s,v"
SELF_ROWS = ["1,a,10,1,a,10", "2,b,20,2,b,20"]


def _using_join(session: ReparkSession) -> DataFrame:
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    return left.alias("l").join(left.alias("r"), "id", "inner")


def _parts(target: Path) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for path in sorted(target.rglob("*.csv")):
        found.setdefault(str(path.parent.relative_to(target)), []).append(
            path.read_text(encoding="utf-8")
        )
    return found


def _assert_csv(parts: list[str], header: str | None, rows: list[str]) -> None:
    body: list[str] = []
    for content in parts:
        lines = content.splitlines()
        if header is not None:
            assert lines[:1] == [header], content
            lines = lines[1:]
        body.extend(lines)
    assert sorted(body) == sorted(rows)


def _ambiguous_message(written: str) -> str:
    return (
        f"[AMBIGUOUS_REFERENCE] Reference `{written}` is ambiguous, could be: "
        f"[`l`.`{written}`, `r`.`{written}`]. SQLSTATE: 42704"
    )


def test_csv_header_carries_the_duplicate_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-header")
    target = tmp_path / "out"
    sm2._self_join(session).write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS)
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_csv_single_part_is_byte_equal_to_spark(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-bytes")
    target = tmp_path / "out"
    sm2._self_join(session).orderBy(spark_functions.col("l.v")).coalesce(1).write.csv(
        str(target), header=True
    )
    assert _parts(target)["."] == ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]
    session.stop()


def test_csv_without_header_writes_the_rows_only(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-noheader")
    target = tmp_path / "out"
    sm2._self_join(session).write.option("header", "false").csv(str(target))
    _assert_csv(_parts(target)["."], None, SELF_ROWS)
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_csv_header_option_through_format_save(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-save")
    target = tmp_path / "out"
    sm2._self_join(session).write.option("header", "true").format("csv").save(str(target))
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS)
    session.stop()


def test_csv_mixed_join_header_keeps_unique_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-mixed")
    target = tmp_path / "out"
    sm2._mixed_join(session).write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,s,v,id,t", ["1,a,10,1,x"])
    session.stop()


def test_csv_using_join_header_repeats_the_shared_columns(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-using")
    target = tmp_path / "out"
    _using_join(session).write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,s,v,s,v", ["1,a,10,a,10", "2,b,20,b,20"])
    session.stop()


def test_csv_same_origin_duplicate_writes_both_columns(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-same-origin")
    target = tmp_path / "out"
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    left.select("id", "id", "s").write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,id,s", ["1,1,a", "2,2,b"])
    session.stop()


def test_csv_exact_duplicate_beside_a_case_twin_keeps_all_three(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-twin")
    target = tmp_path / "out"
    frame = sm2._self_join(session).select(
        spark_functions.col("l.id"),
        spark_functions.col("r.id"),
        spark_functions.col("l.s").alias("ID"),
    )
    frame.write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,id,ID", ["1,1,a", "2,2,b"])
    session.stop()


@pytest.mark.parametrize(
    ("options", "header", "rows"),
    [
        ({"sep": "|"}, "id|s|v|id|s|v", ["1|a|10|1|a|10", "2|b|20|2|b|20"]),
        (
            {"quoteAll": True},
            '"id","s","v","id","s","v"',
            ['"1","a","10","1","a","10"', '"2","b","20","2","b","20"'],
        ),
        ({"compression": "none"}, SELF_HEADER, SELF_ROWS),
    ],
)
def test_csv_options_apply_to_the_display_header(
    tmp_path: Path, options: dict[str, Any], header: str, rows: list[str]
) -> None:
    session = sm2._open(tmp_path, "fa5-options")
    target = tmp_path / "out"
    sm2._self_join(session).write.csv(str(target), header=True, **options)
    _assert_csv(_parts(target)["."], header, rows)
    session.stop()


@pytest.mark.parametrize("mode", ["overwrite", "append", "ignore", "error", "errorifexists"])
def test_csv_every_mode_writes_a_fresh_path(tmp_path: Path, mode: str) -> None:
    session = sm2._open(tmp_path, f"fa5-fresh-{mode}")
    target = tmp_path / "out"
    sm2._self_join(session).write.mode(mode).csv(str(target), header=True)
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS)
    session.stop()


def test_csv_overwrite_replaces_an_existing_path(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-overwrite")
    target = tmp_path / "out"
    frame = sm2._self_join(session)
    frame.write.csv(str(target), header=True)
    frame.write.mode("overwrite").csv(str(target), header=True)
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS)
    session.stop()


def test_csv_append_adds_parts_with_their_own_header(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-append")
    target = tmp_path / "out"
    frame = sm2._self_join(session)
    frame.write.csv(str(target), header=True)
    frame.write.mode("append").csv(str(target), header=True)
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS + SELF_ROWS)
    session.stop()


def test_csv_ignore_leaves_an_existing_path(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-ignore")
    target = tmp_path / "out"
    sm2._mixed_join(session).write.csv(str(target), header=True)
    sm2._self_join(session).write.mode("ignore").csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,s,v,id,t", ["1,a,10,1,x"])
    session.stop()


def test_csv_error_mode_refuses_an_existing_path(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-error")
    target = tmp_path / "out"
    frame = sm2._self_join(session)
    frame.write.csv(str(target), header=True)
    refused = sm2._refusal_of(lambda: frame.write.mode("error").csv(str(target), header=True))
    assert isinstance(refused, AnalysisException)
    assert str(refused).startswith("[PATH_ALREADY_EXISTS]")
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS)
    session.stop()


def test_csv_case_sensitive_session_writes_the_same_header(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-sensitive")
    session.conf.set("spark.sql.caseSensitive", "true")
    target = tmp_path / "out"
    sm2._self_join(session).write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], SELF_HEADER, SELF_ROWS)
    session.stop()


def _temporal_join(session: ReparkSession) -> DataFrame:
    frame = session.sql(
        "SELECT 1 AS id, TIMESTAMP'2024-01-02 03:04:05' AS ts, DATE'2024-01-02' AS d"
    )
    return frame.alias("l").join(
        frame.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id")
    )


def test_csv_temporal_columns_format_under_the_display_header(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-temporal")
    target = tmp_path / "out"
    _temporal_join(session).write.csv(str(target), header=True)
    assert _parts(target)["."] == [
        "id,ts,d,id,ts,d\n"
        "1,2024-01-02T03:04:05.000Z,2024-01-02,1,2024-01-02T03:04:05.000Z,2024-01-02\n"
    ]
    session.stop()


def test_csv_timestamp_format_applies_to_both_twins(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-temporal-format")
    target = tmp_path / "out"
    _temporal_join(session).write.csv(str(target), header=True, timestampFormat="yyyy/MM/dd HH")
    assert _parts(target)["."] == [
        "id,ts,d,id,ts,d\n1,2024/01/02 03,2024-01-02,1,2024/01/02 03,2024-01-02\n"
    ]
    session.stop()


def test_csv_empty_frame_writes_the_display_header_only(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-empty")
    target = tmp_path / "out"
    empty = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"]).filter("id > 99")
    frame = empty.alias("l").join(
        empty.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id")
    )
    frame.write.csv(str(target), header=True)
    assert _parts(target)["."] == ["id,s,v,id,s,v\n"]
    session.stop()


@pytest.mark.parametrize("written", ["s", "S"])
def test_csv_partition_by_a_duplicate_name_is_ambiguous(tmp_path: Path, written: str) -> None:
    session = sm2._open(tmp_path, f"fa5-part-dup-{written}")
    target = tmp_path / "out"
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(
        lambda: frame.write.partitionBy(written).csv(str(target), header=True)
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _ambiguous_message(written)
    assert not target.exists()
    session.stop()


def test_csv_partition_by_the_duplicate_of_a_mixed_join_is_ambiguous(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-part-dup-mixed")
    target = tmp_path / "out"
    frame = sm2._mixed_join(session)
    refused = sm2._refusal_of(lambda: frame.write.partitionBy("id").csv(str(target), header=True))
    assert str(refused).splitlines()[0] == _ambiguous_message("id")
    assert not target.exists()
    session.stop()


def test_csv_partition_by_a_unique_name_drops_it_from_the_header(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-part-unique")
    target = tmp_path / "out"
    sm2._mixed_join(session).write.partitionBy("t").csv(str(target), header=True)
    parts = _parts(target)
    assert list(parts) == ["t=x"]
    _assert_csv(parts["t=x"], "id,s,v,id", ["1,a,10,1"])
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_csv_partition_by_the_using_key_keeps_the_repeated_columns(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-part-using")
    target = tmp_path / "out"
    _using_join(session).write.partitionBy("id").csv(str(target), header=True)
    parts = _parts(target)
    assert sorted(parts) == ["id=1", "id=2"]
    _assert_csv(parts["id=1"], "s,v,s,v", ["a,10,a,10"])
    _assert_csv(parts["id=2"], "s,v,s,v", ["b,20,b,20"])
    session.stop()


def test_csv_partition_by_a_repeated_using_column_is_ambiguous(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-part-using-dup")
    target = tmp_path / "out"
    frame = _using_join(session)
    refused = sm2._refusal_of(lambda: frame.write.partitionBy("s").csv(str(target), header=True))
    assert str(refused).splitlines()[0] == _ambiguous_message("s")
    assert not target.exists()
    session.stop()


@pytest.mark.parametrize("door", ["parquet", "json"])
def test_other_file_doors_keep_refusing_duplicate_names(tmp_path: Path, door: str) -> None:
    session = sm2._open(tmp_path, f"fa5-control-{door}")
    target = tmp_path / "out"
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: getattr(frame.write, door)(str(target)))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert not target.exists()
    session.stop()


def test_csv_of_unique_names_is_unchanged(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-control-plain")
    target = tmp_path / "out"
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    left.write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,s,v", ["1,a,10", "2,b,20"])
    session.stop()


@pytest.mark.parametrize("name", ["__repark_dup_0_id", "__repark_dup_7_"])
def test_csv_keeps_a_unique_column_named_like_a_generated_one(tmp_path: Path, name: str) -> None:
    session = sm2._open(tmp_path, "fa5-prefix")
    target = tmp_path / "out"
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.select(spark_functions.col("id").alias(name), "s", "v")
    assert frame.columns == [name, "s", "v"]
    frame.write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], f"{name},s,v", ["1,a,10", "2,b,20"])
    session.stop()


def test_csv_repeat_beside_a_user_column_named_like_a_generated_one(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-prefix-mixed")
    target = tmp_path / "out"
    frame = sm2._self_join(session).select(
        spark_functions.col("l.id"),
        spark_functions.col("r.id"),
        spark_functions.col("l.v").alias("__repark_dup_0_id"),
    )
    frame.write.csv(str(target), header=True)
    _assert_csv(_parts(target)["."], "id,id,__repark_dup_0_id", ["1,1,10", "2,2,20"])
    session.stop()


def test_no_sql_or_view_door_rewrites_a_name_of_the_generated_shape(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-prefix-sql")
    literal = session.sql("SELECT 1 AS `__repark_dup_3_x`, 2 AS y")
    assert literal.columns == ["__repark_dup_3_x", "y"]
    assert literal._display_names is None
    assert [tuple(row) for row in literal.collect()] == [(1, 2)]
    left = session.createDataFrame([(1, "a")], ["id", "s"])
    left.select(spark_functions.col("id").alias("__repark_dup_0_id"), "s").createOrReplaceTempView(
        "pfx"
    )
    assert session.sql("SELECT * FROM pfx").columns == ["__repark_dup_0_id", "s"]
    assert session.sql("SELECT `__repark_dup_0_id` FROM pfx").columns == ["__repark_dup_0_id"]
    assert session.table("pfx").columns == ["__repark_dup_0_id", "s"]
    assert [row["col_name"] for row in session.sql("DESCRIBE pfx").collect()] == [
        "__repark_dup_0_id",
        "s",
    ]
    assert [column.name for column in session.catalog.listColumns("pfx")] == [
        "__repark_dup_0_id",
        "s",
    ]
    session.stop()


def test_csv_partition_candidates_are_sorted_as_spark_sorts_them(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-part-sorted")
    target = tmp_path / "out"
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.alias("z").join(
        left.alias("a"), spark_functions.col("z.id") == spark_functions.col("a.id")
    )
    refused = sm2._refusal_of(lambda: frame.write.partitionBy("s").csv(str(target), header=True))
    assert str(refused).splitlines()[0] == (
        "[AMBIGUOUS_REFERENCE] Reference `s` is ambiguous, could be: "
        "[`a`.`s`, `z`.`s`]. SQLSTATE: 42704"
    )
    assert not target.exists()
    session.stop()


def test_csv_partition_by_an_unmatched_spelling_is_not_found_when_case_sensitive(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "fa5-part-sensitive")
    session.conf.set("spark.sql.caseSensitive", "true")
    target = tmp_path / "out"
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.write.partitionBy("S").csv(str(target), header=True))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "_LEGACY_ERROR_TEMP_1155"
    assert str(refused).splitlines()[0] == (
        "Partition column `S` not found in schema "
        "struct<id:bigint,s:string,v:bigint,id:bigint,s:string,v:bigint>."
    )
    assert "_repark_" not in str(refused)
    assert not target.exists()
    exact = sm2._refusal_of(lambda: frame.write.partitionBy("s").csv(str(target), header=True))
    assert str(exact).splitlines()[0] == _ambiguous_message("s")
    session.stop()


def test_csv_partition_by_a_same_origin_repeat_refuses_cleanly_divergence(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa5-part-same-origin")
    target = tmp_path / "out"
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.select("id", "id", "s")
    refused = sm2._refusal_of(lambda: frame.write.partitionBy("id").csv(str(target), header=True))
    assert isinstance(refused, AnalysisException)
    assert str(refused).startswith("partitionBy column `id` is carried 2 times by the frame")
    assert "_repark_" not in str(refused)
    assert not target.exists()
    session.stop()


def test_s3_csv_route_hands_the_engine_recorded_display_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from repark import _native
    from repark.spark.dataframe import writer_s3

    session = sm2._open(tmp_path, "fa5-s3")
    handed: list[Any] = []
    monkeypatch.setattr(
        writer_s3,
        "_native",
        SimpleNamespace(session_write_path=lambda *arguments: handed.append(arguments)),
    )
    sm2._self_join(session).write.option("header", "true").csv("s3a://fa5-bucket/out")
    assert len(handed) == 1
    engine_session, frame, url, stored_as = handed[0][:4]
    assert (url, stored_as) == ("s3a://fa5-bucket/out", "csv")
    engine_names = list(_native.logical_column_names(frame))
    assert len(set(engine_names)) == 6
    _, resolved, options = _native.session_text_write_copy_parts(
        engine_session, frame, "v", {}, [], "CSV"
    )
    assert resolved == "repark_text_csv"
    assert "'repark.text.display_header_hex' '6964,73,76,6964,73,76'" in options
    session.stop()


def test_a_duplicate_name_view_stays_refused_so_no_bare_name_binds_beside_it(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "fa5-view-refused")
    session.createDataFrame([(1, "w")], ["id", "w"]).createOrReplaceTempView("plain")
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.createOrReplaceTempView("vj"))
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert session.catalog.tableExists("vj") is False
    missing = sm2._refusal_of(lambda: session.sql("SELECT id FROM vj CROSS JOIN plain").collect())
    assert isinstance(missing, AnalysisException)
    assert "vj" in str(missing)
    session.stop()

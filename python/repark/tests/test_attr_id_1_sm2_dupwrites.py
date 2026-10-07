from __future__ import annotations

from pathlib import Path

import _sm2_shared as sm2
import pytest

from repark.errors import AnalysisException
from repark.spark import functions as spark_functions


def test_parquet_write_of_duplicate_display_names_refuses_column_already_exists(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-parquet")
    frame = sm2._self_join(session)
    assert frame.columns == ["id", "s", "v", "id", "s", "v"]
    target = tmp_path / "dup_parquet"
    refused = sm2._refusal_of(lambda: frame.write.mode("overwrite").parquet(str(target)))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert not target.exists()
    session.stop()


def test_json_write_of_duplicate_display_names_refuses_column_already_exists(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-json")
    frame = sm2._self_join(session)
    target = tmp_path / "dup_json"
    refused = sm2._refusal_of(lambda: frame.write.mode("overwrite").json(str(target)))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert not target.exists()
    session.stop()


def test_csv_write_of_duplicate_display_names_refuses_as_ruled_divergence(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-csv")
    frame = sm2._self_join(session)
    target = tmp_path / "dup_csv"
    refused = sm2._refusal_of(
        lambda: frame.write.mode("overwrite").option("header", "true").csv(str(target))
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert not target.exists()
    session.stop()


def test_orc_write_of_duplicate_display_names_refuses_column_already_exists(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-orc")
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(
        lambda: frame.write.mode("overwrite").format("orc").save(str(tmp_path / "dup_orc"))
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    session.stop()


def test_orc_write_of_unique_names_still_refuses_the_format(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-orc-plain")
    frame = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    refused = sm2._refusal_of(
        lambda: frame.write.mode("overwrite").format("orc").save(str(tmp_path / "plain_orc"))
    )
    assert "NOT_IMPLEMENTED" in str(refused).splitlines()[0]
    session.stop()


def test_save_as_table_of_duplicate_display_names_refuses_column_already_exists(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-save")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.write.mode("overwrite").saveAsTable("sc.ns.dupt"))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert session.catalog.tableExists("sc.ns.dupt") is False
    sm2._assert_no_twin_bytes(tmp_path)
    session.stop()


def test_write_to_create_of_duplicate_display_names_refuses_column_already_exists(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-v2create")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.writeTo("sc.ns.v2dupt").create())
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert session.catalog.tableExists("sc.ns.v2dupt") is False
    session.stop()


def test_write_to_append_of_duplicate_display_names_refuses_column_already_exists(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-v2append")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.v2six (a INT, b STRING, c INT, d INT, e STRING, f INT) USING iceberg"
    ).collect()
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.writeTo("sc.ns.v2six").append())
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    session.stop()


def test_insert_into_of_duplicate_display_names_writes_positionally_like_spark(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-insert")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.six (a INT, b STRING, c INT, d INT, e STRING, f INT) USING iceberg"
    ).collect()
    frame = sm2._self_join(session)
    frame.write.mode("append").insertInto("sc.ns.six")
    rows = session.sql("SELECT * FROM sc.ns.six ORDER BY a").collect()
    assert [tuple(row) for row in rows] == [(1, "a", 10, 1, "a", 10), (2, "b", 20, 2, "b", 20)]
    sm2._assert_no_twin_bytes(tmp_path / "sc")
    session.stop()


def test_parquet_write_of_folded_duplicate_names_reports_the_folded_name(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-fold")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    folded = left.select(
        spark_functions.col("id").alias("ID"),
        spark_functions.col("s"),
        spark_functions.col("v"),
        spark_functions.col("id").alias("id"),
    )
    assert folded.columns == ["ID", "s", "v", "id"]
    target = tmp_path / "fold_parquet"
    refused = sm2._refusal_of(lambda: folded.write.mode("overwrite").parquet(str(target)))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "COLUMN_ALREADY_EXISTS"
    assert sm2._sql_state_of(refused) == "42711"
    assert str(refused).splitlines()[0] == sm2._expected_dup_message("id")
    assert not target.exists()
    session.stop()


def test_csv_write_of_case_twin_names_writes_the_raw_header(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-csvtwins")
    frame = session.sql("SELECT 1 AS T, 2 AS t")
    assert frame.columns == ["T", "t"]
    target = tmp_path / "twin_csv"
    frame.write.mode("overwrite").option("header", "true").csv(str(target))
    parts = sorted(target.rglob("*.csv"))
    assert len(parts) == 1
    assert parts[0].read_text(encoding="utf-8").splitlines()[0] == "T,t"
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_parquet_write_of_case_twin_names_writes_when_case_sensitive(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-sensitive")
    session.conf.set("spark.sql.caseSensitive", "true")
    try:
        frame = session.sql("SELECT 1 AS T, 2 AS t")
        target = tmp_path / "twin_parquet"
        frame.write.mode("overwrite").parquet(str(target))
        assert len(list(target.rglob("*.parquet"))) >= 1
        sm2._assert_no_twin_bytes(target)
    finally:
        session.conf.set("spark.sql.caseSensitive", "false")
    session.stop()


def test_parquet_write_of_unique_names_still_succeeds(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-plain")
    frame = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    target = tmp_path / "plain_parquet"
    frame.write.mode("overwrite").parquet(str(target))
    assert len(list(target.rglob("*.parquet"))) >= 1
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_save_as_table_append_of_duplicate_display_names_stays_loud(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-dupwrites-append")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.six (a INT, b STRING, c INT, d INT, e STRING, f INT) USING iceberg"
    ).collect()
    frame = sm2._self_join(session)
    with pytest.raises(AnalysisException, match=r"(?i)duplicate"):
        frame.write.mode("append").saveAsTable("sc.ns.six")
    session.stop()

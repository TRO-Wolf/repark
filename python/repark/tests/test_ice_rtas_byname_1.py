"""ICE-RTAS-BYNAME-1 — ``INSERT … BY NAME`` on the Spark door, RTAS operations as xfails.

Oracle: ``ice_rtas_byname_1_spark_oracle.json`` (live PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0, re-derived by
``_record_ice_rtas_byname_1_oracle.py``). RePark has no temp views, so the
oracle's ``swapped`` view is a same-shape source table ``sc.ns.sw``
(``last_name, first_name, n`` order — what BY NAME observes). The catalog is
``sc`` so refusal texts match the fixture byte-exact. Offline tier pins
RePark against the fixture; the live tier replays the generator and checks
the fixture.

pins: ice-rtas-byname-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException, ParseException
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_rtas_byname_1_spark_oracle.json").read_text(encoding="utf-8")
)
BN = "sc.ns.bn"
SW = "sc.ns.sw"
PQ = "sc.ns.pq"
BN_DDL = "(first_name STRING, last_name STRING, n INT)"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-rtas-byname-1").getOrCreate()
    session.register_memory_catalog("sc", tmp_path / "wh")
    session.sql("CREATE NAMESPACE sc.ns")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _seed_shapes(session: Any) -> None:
    session.sql(f"CREATE TABLE {BN} {BN_DDL} USING iceberg")
    session.sql(
        "CREATE TABLE sc.ns.sw (last_name STRING, first_name STRING, n INT) USING iceberg"
    )
    session.sql("INSERT INTO sc.ns.sw VALUES ('Smith', 'Ann', 1)")


def _seed_parquet(session: Any) -> None:
    session.sql(f"CREATE TABLE {PQ} {BN_DDL} USING parquet")


def _rows(session: Any, table: str) -> list[tuple[Any, ...]]:
    frame = session.sql(f"SELECT first_name, last_name, n FROM {table}").to_arrow()
    columns = [frame.column(name).to_pylist() for name in ("first_name", "last_name", "n")]
    return sorted(zip(*columns, strict=True), key=repr)


def _apply(session: Any, statements: list[str]) -> list[tuple[Any, ...]]:
    for statement in statements:
        session.sql(statement).collect()
    return _rows(session, BN)


def _cell_sql(section: str, cell: str) -> str:
    return str(FIXTURE[section][cell]["sql"]).replace("swapped", SW)


def _refusal(session: Any, section: str, cell: str) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        session.sql(_cell_sql(section, cell)).collect()
    assert str(excinfo.value) == "Error during planning: " + str(
        FIXTURE[section][cell]["error"]
    ), str(excinfo.value)


def test_by_name_reorders_source_columns(spark: Any) -> None:
    """by_name answers ('Ann','Smith',1) from swapped-order source. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    assert _apply(spark, [_cell_sql("insert_by_name", "by_name")]) == [
        tuple(row) for row in FIXTURE["insert_by_name"]["by_name"]["rows"]
    ]


def test_by_name_subset_null_fills_missing_nullable(spark: Any) -> None:
    """by_name_subset NULL-fills last_name. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    assert _apply(
        spark,
        [
            _cell_sql("insert_by_name", "by_name"),
            _cell_sql("insert_by_name", "by_name_subset"),
        ],
    ) == [tuple(row) for row in FIXTURE["insert_by_name"]["by_name_subset"]["rows"]]


def test_by_name_matches_case_insensitively(spark: Any) -> None:
    """by_name_case resolves FIRST_NAME/Last_Name/N. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    assert _apply(
        spark,
        [
            _cell_sql("insert_by_name", "by_name"),
            _cell_sql("insert_by_name", "by_name_subset"),
            _cell_sql("insert_by_name", "by_name_case"),
        ],
    ) == [tuple(row) for row in FIXTURE["insert_by_name"]["by_name_case"]["rows"]]


def test_by_name_extra_column_refused(spark: Any) -> None:
    """by_name_extra answers TOO_MANY_DATA_COLUMNS byte-exact. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    _refusal(spark, "insert_by_name", "by_name_extra")


def test_by_name_duplicate_source_name_refused(spark: Any) -> None:
    """by_name_dup answers TOO_MANY_DATA_COLUMNS byte-exact. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    _refusal(spark, "insert_by_name", "by_name_dup")


def test_by_name_case_duplicate_refused(spark: Any) -> None:
    """by_name_case_dup answers AMBIGUOUS_COLUMN_NAME byte-exact. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    _refusal(spark, "insert_by_name", "by_name_case_dup")


def test_positional_insert_stays_positional(spark: Any) -> None:
    """positional writes ('Smith','Ann',1) after the by-name rows. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    assert _apply(
        spark,
        [
            _cell_sql("insert_by_name", "by_name"),
            _cell_sql("insert_by_name", "by_name_subset"),
            _cell_sql("insert_by_name", "by_name_case"),
            _cell_sql("insert_by_name", "positional"),
        ],
    ) == [tuple(row) for row in FIXTURE["insert_by_name"]["positional"]["rows"]]


def test_by_name_values_refused(spark: Any) -> None:
    """by_name_values answers EXTRA_COLUMNS byte-exact. pins: ice-rtas-byname-1/C-003"""
    _seed_shapes(spark)
    _refusal(spark, "insert_by_name", "by_name_values")


def test_by_name_column_list_is_parse_error(spark: Any) -> None:
    """column list + BY NAME refuses PARSE_SYNTAX_ERROR naming BY NAME. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    with pytest.raises(ParseException) as excinfo:
        spark.sql(_cell_sql("insert_by_name", "by_name_column_list")).collect()
    message = str(excinfo.value)
    assert "PARSE_SYNTAX_ERROR" in message, message
    assert "BY NAME" in message, message
    assert _rows(spark, BN) == []


def test_insert_overwrite_by_name_replaces(spark: Any) -> None:
    """overwrite_by_name leaves only ('P','O',8). pins: ice-rtas-byname-1/C-002"""
    _seed_shapes(spark)
    assert _apply(
        spark,
        [
            _cell_sql("insert_by_name", "by_name"),
            _cell_sql("insert_by_name", "by_name_subset"),
            _cell_sql("insert_by_name", "overwrite_by_name"),
        ],
    ) == [tuple(row) for row in FIXTURE["insert_by_name"]["overwrite_by_name"]["rows"]]


def test_thin_native_door_parse_errors_loudly(spark: Any) -> None:
    """repark.sql BY NAME is a loud parse error like other Spark-isms. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    with pytest.raises(ParseException) as excinfo:
        repark.sql(f"INSERT INTO {BN} BY NAME SELECT * FROM {SW}").collect()
    assert "BY" in str(excinfo.value), str(excinfo.value)


@pytest.mark.xfail(strict=True, reason="BLOCKED-ON-FORK F-DML-FIELD-ID-1")
def test_dataframe_writeto_appends_by_name(spark: Any) -> None:
    """writeTo append maps the swapped-order frame by name. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    spark.table(SW).writeTo(BN).append()
    assert _rows(spark, BN) == [("Ann", "Smith", 1)]


def test_parquet_by_name_reorders(spark: Any) -> None:
    """pq_by_name answers ('Ann','Smith',1) on USING parquet. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    spark.sql(_cell_sql("parquet_by_name", "pq_by_name")).collect()
    assert _rows(spark, PQ) == [
        tuple(row) for row in FIXTURE["parquet_by_name"]["pq_by_name"]["rows"]
    ]


def test_parquet_subset_and_case_fill_and_fold(spark: Any) -> None:
    """pq_subset/pq_case NULL-fill and fold case on parquet. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    spark.sql(_cell_sql("parquet_by_name", "pq_by_name")).collect()
    spark.sql(_cell_sql("parquet_by_name", "pq_subset")).collect()
    spark.sql(_cell_sql("parquet_by_name", "pq_case")).collect()
    assert _rows(spark, PQ) == [
        tuple(row) for row in FIXTURE["parquet_by_name"]["pq_case"]["rows"]
    ]


def test_parquet_extra_answers_extra_columns(spark: Any) -> None:
    """pq_extra answers EXTRA_COLUMNS byte-exact. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    _refusal(spark, "parquet_by_name", "pq_extra")


def test_parquet_dup_answers_too_many(spark: Any) -> None:
    """pq_dup answers TOO_MANY_DATA_COLUMNS byte-exact. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    _refusal(spark, "parquet_by_name", "pq_dup")


def test_parquet_values_refused(spark: Any) -> None:
    """pq_values answers EXTRA_COLUMNS byte-exact. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    _refusal(spark, "parquet_by_name", "pq_values")


def test_parquet_positional_stays_positional(spark: Any) -> None:
    """pq_positional writes ('Smith','Ann',1) on parquet. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    for cell in ("pq_by_name", "pq_subset", "pq_case", "pq_positional"):
        spark.sql(_cell_sql("parquet_by_name", cell)).collect()
    assert _rows(spark, PQ) == [
        tuple(row) for row in FIXTURE["parquet_by_name"]["pq_positional"]["rows"]
    ]


def test_parquet_overwrite_by_name_replaces(spark: Any) -> None:
    """pq_overwrite leaves only ('P','O',8) on parquet. pins: ice-rtas-byname-1/C-004"""
    _seed_shapes(spark)
    _seed_parquet(spark)
    spark.sql(_cell_sql("parquet_by_name", "pq_by_name")).collect()
    spark.sql(_cell_sql("parquet_by_name", "pq_overwrite")).collect()
    assert _rows(spark, PQ) == [
        tuple(row) for row in FIXTURE["parquet_by_name"]["pq_overwrite"]["rows"]
    ]


def _seed_rtas(session: Any) -> None:
    session.sql("CREATE TABLE sc.ns.rsrc (id BIGINT) USING iceberg")
    session.sql("INSERT INTO sc.ns.rsrc VALUES (1), (2), (3), (4), (5)")


def _snapshot_ops(session: Any, table: str) -> list[str]:
    return [
        row["operation"]
        for row in session.sql(
            f"SELECT operation FROM {table}.snapshots ORDER BY committed_at"
        ).to_arrow().to_pylist()
    ]


def test_by_name_partitioned_table_reorders(spark: Any) -> None:
    """BY NAME resolves before the partition fanout. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    spark.sql(
        "CREATE TABLE sc.ns.pt (first_name STRING, n INT, p INT) USING iceberg PARTITIONED BY (p)"
    )
    spark.sql("INSERT INTO sc.ns.pt BY NAME SELECT 5 AS n, 'x' AS first_name, 0 AS p").collect()
    assert _rows_partitioned(spark) == [("x", 5, 0)]


def _rows_partitioned(session: Any) -> list[tuple[Any, ...]]:
    frame = session.sql("SELECT first_name, n, p FROM sc.ns.pt").to_arrow()
    columns = [frame.column(name).to_pylist() for name in ("first_name", "n", "p")]
    return sorted(zip(*columns, strict=True), key=repr)


def test_by_name_empty_insert_matches_positional_door(spark: Any) -> None:
    """An empty BY NAME source commits like the positional door. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    spark.sql(f"INSERT INTO {BN} BY NAME SELECT * FROM {SW} WHERE false").collect()
    spark.sql("CREATE TABLE sc.ns.cmp (first_name STRING, last_name STRING, n INT) USING iceberg")
    spark.sql("INSERT INTO sc.ns.cmp SELECT * FROM sc.ns.sw WHERE false").collect()
    assert _snapshot_ops(spark, BN) == _snapshot_ops(spark, "sc.ns.cmp") == ["append"]
    assert _rows(spark, BN) == []


def test_by_name_branch_append(spark: Any) -> None:
    """BY NAME appends to the branch and leaves main alone. pins: ice-rtas-byname-1/C-001"""
    _seed_shapes(spark)
    spark.sql(f"INSERT INTO {BN} BY NAME SELECT * FROM {SW}").collect()
    spark.sql(f"ALTER TABLE {BN} CREATE BRANCH b1")
    spark.sql(f"INSERT INTO {BN}.branch_b1 BY NAME SELECT * FROM {SW}").collect()
    assert _rows_branch(spark, f"{BN}.branch_b1") == [("Ann", "Smith", 1), ("Ann", "Smith", 1)]
    assert _rows(spark, BN) == [("Ann", "Smith", 1)]


def _rows_branch(session: Any, table: str) -> list[tuple[Any, ...]]:
    frame = session.sql(f"SELECT first_name, last_name, n FROM {table}").to_arrow()
    columns = [frame.column(name).to_pylist() for name in ("first_name", "last_name", "n")]
    return sorted(zip(*columns, strict=True), key=repr)


@pytest.mark.xfail(strict=True, reason="BLOCKED-ON-FORK F-RTAS-OPS-1")
def test_rtas_replace_records_overwrite(spark: Any) -> None:
    """CTAS then RTAS answers [append, overwrite]. pins: ice-rtas-byname-1/C-005"""
    _seed_rtas(spark)
    spark.sql("CREATE TABLE sc.ns.rt USING iceberg AS SELECT id FROM sc.ns.rsrc").collect()
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt USING iceberg AS SELECT id FROM sc.ns.rsrc WHERE id < 3"
    ).collect()
    assert _snapshot_ops(spark, "sc.ns.rt") == ["append", "overwrite"]


@pytest.mark.xfail(strict=True, reason="BLOCKED-ON-FORK F-RTAS-OPS-1")
def test_rtas_new_table_records_overwrite(spark: Any) -> None:
    """RTAS creating the table answers [overwrite]. pins: ice-rtas-byname-1/C-005"""
    _seed_rtas(spark)
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt2 USING iceberg AS SELECT id FROM sc.ns.rsrc WHERE id < 3"
    ).collect()
    assert _snapshot_ops(spark, "sc.ns.rt2") == ["overwrite"]


@pytest.mark.xfail(strict=True, reason="BLOCKED-ON-FORK F-RTAS-OPS-1")
def test_rtas_empty_new_records_delete(spark: Any) -> None:
    """RTAS of an empty SELECT answers [delete]. pins: ice-rtas-byname-1/C-005"""
    _seed_rtas(spark)
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt3 USING iceberg AS SELECT id FROM sc.ns.rsrc WHERE id < 0"
    ).collect()
    assert _snapshot_ops(spark, "sc.ns.rt3") == ["delete"]


@pytest.mark.xfail(strict=True, reason="BLOCKED-ON-FORK F-RTAS-OPS-1")
def test_rtas_empty_twice_records_two_deletes(spark: Any) -> None:
    """RTAS of an empty SELECT twice answers [delete, delete]. pins: ice-rtas-byname-1/C-005"""
    _seed_rtas(spark)
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt3 USING iceberg AS SELECT id FROM sc.ns.rsrc WHERE id < 0"
    ).collect()
    spark.sql(
        "CREATE OR REPLACE TABLE sc.ns.rt3 USING iceberg AS SELECT id FROM sc.ns.rsrc WHERE id < 0"
    ).collect()
    assert _snapshot_ops(spark, "sc.ns.rt3") == ["delete", "delete"]


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_fixture_reproduces(tmp_path: Path) -> None:
    """The generator re-derives the committed fixture on live Spark. pins: ice-rtas-byname-1/C-001"""
    sparkenv = Path("/tmp/sparkenv/bin/python")
    generator = Path(__file__).with_name("_record_ice_rtas_byname_1_oracle.py")
    environ = dict(os.environ)
    environ.setdefault("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64")
    environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    completed = subprocess.run(
        [
            str(sparkenv),
            str(generator),
            "--warehouse",
            str(tmp_path / "live-wh"),
            "check",
        ],
        capture_output=True,
        text=True,
        env=environ,
        timeout=1200,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr

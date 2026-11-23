from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _record_ansi_off_narrowing_overflow_1 import (
    ANSI_MODES,
    CARD_COMPARISON_SPECS,
    FIXTURE,
    PARQUET_BARRIER_QUERIES,
    REACH_QUERIES,
    SCALAR_SPECS,
    SEATS,
    WRITE_CASES,
    collect_table,
    run_card_comparisons,
    run_parquet_barrier,
    run_reach_queries,
    run_scalar,
    run_write_case,
    scalar_key,
    write_barrier_parquets,
)

from repark import ReparkSession
from repark._native import PyReparkSession
from repark.spark import functions
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import DecimalType, IntegerType, StructField, StructType

_ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
_CAST_SEATS: tuple[str, ...] = (
    "sql-literal",
    "sql-column",
    "dataframe",
    "expr",
    "selectexpr",
    "view",
)
_SCALAR_GROUPS = [
    (kind, src_type, dst_type, vals, seat, ansi)
    for kind, src_type, dst_type, vals in SCALAR_SPECS
    for seat in _CAST_SEATS
    for ansi in ANSI_MODES
]
_TRY_GROUPS = [
    (kind, src_type, dst_type, vals, ansi)
    for kind, src_type, dst_type, vals in SCALAR_SPECS
    for ansi in ANSI_MODES
]
_TRY_2_63_KINDS = frozenset({"float_to_bigint", "double_to_bigint"})
_WRITE_GROUPS = [(case, ansi) for case in WRITE_CASES for ansi in ANSI_MODES]


def _strip_engine_prefix(message: str) -> str:
    for prefix in ("datafusion engine error: Execution error: ", "Execution error: "):
        if message.startswith(prefix):
            return message[len(prefix) :]
    return message


def _open(warehouse: Path, ansi: str) -> ReparkSession:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("ansi-off-narrowing-overflow-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.narrow.type", "hadoop")
        .config("spark.sql.catalog.narrow.warehouse", str(warehouse / "narrow"))
        .getOrCreate()
    )
    session.conf.set("spark.sql.ansi.enabled", ansi)
    return session


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    built = _open(tmp_path, "false")
    yield built
    built.stop()
    _reset_active_session_for_tests()


def _scalar_miss(expected: dict[str, Any], got: dict[str, Any]) -> bool:
    if "value" in expected:
        return (
            got.get("value") != expected["value"]
            or got.get("type") != expected["type"]
            or got.get("nullable") != expected["nullable"]
        )
    return (
        got.get("error") != expected["error"]
        or got.get("sqlstate") != expected["sqlstate"]
        or _strip_engine_prefix(got.get("message", "")) != expected["message"]
    )


def _table_miss(expected: dict[str, Any], got: dict[str, Any]) -> bool:
    if "rows" in expected:
        return got.get("rows") != expected["rows"] or got.get("types") != expected["types"]
    return (
        got.get("error") != expected["error"]
        or got.get("sqlstate") != expected["sqlstate"]
        or _strip_engine_prefix(got.get("message", "")) != expected["message"]
    )


def _cold_read(warehouse: Path, table: str) -> dict[str, Any]:
    cold = _open(warehouse, "true")
    try:
        folder = warehouse / "narrow" / "ns" / table.rsplit(".", 1)[1]
        frame = (
            cold.read.format("iceberg")
            .load(str(folder))
            .select("id", "bi", "ft", "dd")
            .orderBy("id")
        )
        return collect_table(frame)
    finally:
        cold.stop()
        _reset_active_session_for_tests()


def test_the_fixture_holds_every_cell() -> None:
    assert _ORACLE["spark"] == "4.1.2"
    want_scalars = {
        scalar_key(kind, seat, raw)
        for kind, _s, _d, vals in SCALAR_SPECS
        for seat in SEATS
        for raw in vals
    }
    want_comparisons = {
        f"{name}|{pred}" for name, _s, _d, _v, preds in CARD_COMPARISON_SPECS for pred in preds
    }
    want_barrier = {key for key, _t, _sql in PARQUET_BARRIER_QUERIES}
    want_reach = {key for key, _sql in REACH_QUERIES}
    for ansi in ANSI_MODES:
        bucket = _ORACLE["modes"][ansi]
        assert set(bucket["scalars"]) == want_scalars
        assert set(bucket["card_comparisons"]) == want_comparisons
        assert set(bucket["parquet_barrier"]) == want_barrier
        assert set(bucket["reach"]) == want_reach
        assert set(bucket["writes"]) == set(WRITE_CASES)


@pytest.mark.parametrize(
    ("kind", "src_type", "dst_type", "vals", "seat", "ansi"),
    _SCALAR_GROUPS,
    ids=[f"{k}-{s}-ansi_{a}" for k, _st, _dt, _v, s, a in _SCALAR_GROUPS],
)
def test_every_narrowing_numeric_cast_answers_as_spark(
    session: ReparkSession,
    kind: str,
    src_type: str,
    dst_type: str,
    vals: tuple[str | None, ...],
    seat: str,
    ansi: str,
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    oracle = _ORACLE["modes"][ansi]["scalars"]
    misses: list[str] = []
    for raw in vals:
        key = scalar_key(kind, seat, raw)
        expected = oracle[key]
        got = run_scalar(session, functions, src_type, dst_type, seat, raw)
        if _scalar_miss(expected, got):
            misses.append(f"{key}: spark={expected} repark={got}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize(
    ("kind", "src_type", "dst_type", "vals", "ansi"),
    _TRY_GROUPS,
    ids=[f"{k}-ansi_{a}" for k, _st, _dt, _v, a in _TRY_GROUPS],
)
def test_try_cast_narrowing_answers_as_spark(
    session: ReparkSession,
    kind: str,
    src_type: str,
    dst_type: str,
    vals: tuple[str | None, ...],
    ansi: str,
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    oracle = _ORACLE["modes"][ansi]["scalars"]
    misses: list[str] = []
    for raw in vals:
        key = scalar_key(kind, "try-cast", raw)
        got = run_scalar(session, functions, src_type, dst_type, "try-cast", raw)
        if kind in _TRY_2_63_KINDS and raw == "9223372036854775808.0":
            assert got == {"value": None, "type": "bigint", "nullable": True}
            continue
        expected = oracle[key]
        if _scalar_miss(expected, got):
            misses.append(f"{key}: spark={expected} repark={got}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_card_comparisons_answer_as_spark(session: ReparkSession, ansi: str) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    oracle = _ORACLE["modes"][ansi]["card_comparisons"]
    found = run_card_comparisons(session)
    misses = [
        f"{key}: spark={oracle[key]} repark={found[key]}"
        for key in sorted(oracle)
        if _table_miss(oracle[key], found[key])
    ]
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_parquet_stats_barrier_answers_as_spark(
    session: ReparkSession, tmp_path: Path, ansi: str
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    paths = write_barrier_parquets(tmp_path / "pq")
    oracle = _ORACLE["modes"][ansi]["parquet_barrier"]
    found = run_parquet_barrier(session, paths)
    misses = [
        f"{key}: spark={oracle[key]} repark={found[key]}"
        for key in sorted(oracle)
        if _table_miss(oracle[key], found[key])
    ]
    assert not misses, "\n".join(misses)
    if ansi == "false":
        assert found["tiny_max_1_300"]["rows"] == [[44]]


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_reach_checklist_queries_answer_as_spark(session: ReparkSession, ansi: str) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    oracle = _ORACLE["modes"][ansi]["reach"]
    found = run_reach_queries(session)
    misses = [
        f"{key}: spark={oracle[key]} repark={found[key]}"
        for key in sorted(oracle)
        if _table_miss(oracle[key], found[key])
    ]
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize(
    ("case", "ansi"),
    _WRITE_GROUPS,
    ids=[f"{c}-ansi_{a}" for c, a in _WRITE_GROUPS],
)
def test_iceberg_write_seats_read_back_cold_as_spark(tmp_path: Path, case: str, ansi: str) -> None:
    warehouse = tmp_path / f"wh_{case}_{ansi}"
    writer = _open(warehouse, ansi)
    try:
        writer.sql("CREATE NAMESPACE IF NOT EXISTS narrow.ns").collect()
        got = run_write_case(
            writer,
            functions,
            "narrow",
            case,
            ansi,
            lambda table: _cold_read(warehouse, table),
        )
    finally:
        writer.stop()
        _reset_active_session_for_tests()
    expected = _ORACLE["modes"][ansi]["writes"][case]
    assert not _table_miss(expected, got), f"{case}/{ansi}: spark={expected} repark={got}"


def test_empty_insert_overwrite_unsafe_cast_guard_stays_armed(tmp_path: Path) -> None:
    session = _open(tmp_path, "true")
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS narrow.ns").collect()
        session.sql("CREATE TABLE narrow.ns.ow_i (c INT) USING iceberg").collect()
        session.sql("INSERT INTO narrow.ns.ow_i VALUES (7)").collect()
        session.sql("CREATE TABLE narrow.ns.ow_d (c DECIMAL(3,2)) USING iceberg").collect()
        session.sql("INSERT INTO narrow.ns.ow_d VALUES (CAST(1.25 AS DECIMAL(3,2)))").collect()
        with pytest.raises(Exception, match=r"CAST_OVERFLOW|empty"):
            session.sql(
                "INSERT OVERWRITE narrow.ns.ow_i "
                "SELECT CAST(b AS INT) FROM (SELECT CAST(4294967297 AS BIGINT) AS b) WHERE false"
            ).collect()
        assert session.sql("SELECT c FROM narrow.ns.ow_i").collect() == [(7,)]
        with pytest.raises(Exception, match=r"NUMERIC_VALUE_OUT_OF_RANGE|empty"):
            session.sql(
                "INSERT OVERWRITE narrow.ns.ow_d "
                "SELECT CAST(d AS DECIMAL(3,2)) FROM "
                "(SELECT CAST(10.00 AS DECIMAL(10,2)) AS d) WHERE false"
            ).collect()
        assert session.sql("SELECT c FROM narrow.ns.ow_d").collect() == [(Decimal("1.25"),)]
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_out_of_scope_store_assignment_reader_schema_and_native_door_stay_unchanged(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path, "false")
    try:
        session.sql("CREATE NAMESPACE IF NOT EXISTS narrow.ns").collect()
        session.sql("CREATE TABLE narrow.ns.store_i (id INT, bi INT) USING iceberg").collect()
        with pytest.raises(Exception, match=r"Can't cast value 4294967297"):
            session.sql(
                "INSERT INTO narrow.ns.store_i SELECT 1, CAST(4294967297 AS BIGINT)"
            ).collect()
        session.sql("CREATE TABLE narrow.ns.store_u (id INT, v BIGINT) USING iceberg").collect()
        session.sql("INSERT INTO narrow.ns.store_u VALUES (1, 0)").collect()
        with pytest.raises(Exception, match=r"CAST_OVERFLOW_IN_TABLE_INSERT"):
            session.sql("UPDATE narrow.ns.store_u SET v = 1e19").collect()

        json_int = tmp_path / "reader_int.json"
        json_int.write_text('{"b": 4294967297}\n', encoding="utf-8")
        with pytest.raises(Exception, match=r"Can't cast value 4294967297|Cast error"):
            session.read.schema(StructType([StructField("b", IntegerType(), True)])).json(
                str(json_int)
            ).collect()

        json_dec = tmp_path / "reader_dec.json"
        json_dec.write_text('{"d": 10.00}\n', encoding="utf-8")
        with pytest.raises(Exception, match=r"too large to store|Cast error"):
            session.read.schema(StructType([StructField("d", DecimalType(3, 2), True)])).json(
                str(json_dec)
            ).collect()

        safe_plan = " ".join(
            str(row[-1])
            for row in session.sql(
                "EXPLAIN SELECT CAST(d AS INT) AS i, CAST(d AS DECIMAL(10,1)) AS d1 "
                "FROM (SELECT CAST(1.25 AS DECIMAL(10,2)) AS d)"
            ).collect()
        )
        assert "__repark_narrow_to_" not in safe_plan
    finally:
        session.stop()
        _reset_active_session_for_tests()

    pq_multi = tmp_path / "native_multi.parquet"
    schema = pa.schema([pa.field("b", pa.int64())])
    with pq.ParquetWriter(pq_multi, schema) as writer:
        writer.write_table(pa.table({"b": pa.array([1, 2], type=pa.int64())}))
        writer.write_table(pa.table({"b": pa.array([100, 300], type=pa.int64())}))
    native = PyReparkSession.native()
    native.create_or_replace_temp_view("multi", native.read_parquet(str(pq_multi)))
    explain = pa.table(native.sql("EXPLAIN SELECT min(CAST(b AS TINYINT)) FROM multi")).to_pylist()
    assert "__repark_stats_barrier__" in " ".join(str(row) for row in explain)
    with pytest.raises(Exception, match=r"Can't cast value 300|overflow|out of range"):
        pa.table(native.sql("SELECT min(CAST(b AS TINYINT)) FROM multi"))
    with pytest.raises(Exception, match=r"Can't cast value 4294967297"):
        pa.table(native.sql("SELECT CAST(CAST(4294967297 AS BIGINT) AS INT)"))

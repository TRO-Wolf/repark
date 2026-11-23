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


def _assert_frame_rows_and_nullability(
    frame: Any,
    expected_rows: list[list[Any]],
    expected_types: list[str],
    expected_nullability: list[bool],
) -> None:
    rows = [list(row) for row in frame.collect()]
    types = [field.dataType.simpleString() for field in frame.schema.fields]
    nullability = [bool(field.nullable) for field in frame.schema.fields]
    assert rows == expected_rows
    assert types == expected_types
    assert nullability == expected_nullability


def test_nullable_operands_over_not_null_inputs_never_narrow_nullability(tmp_path: Path) -> None:
    session = _open(tmp_path / "a1_wh", "false")
    try:
        session.sql("CREATE NAMESPACE narrow.a1").collect()
        session.sql(
            "CREATE TABLE narrow.a1.nn "
            "(id INT NOT NULL, b BIGINT NOT NULL, s STRING NOT NULL) USING iceberg"
        ).collect()
        session.sql(
            "INSERT INTO narrow.a1.nn VALUES (1, 4294967297L, 'abc'), (2, 1L, '7')"
        ).collect()
        session.sql(
            "CREATE TABLE narrow.a1.rr (id INT NOT NULL, rb BIGINT NOT NULL) USING iceberg"
        ).collect()
        session.sql("INSERT INTO narrow.a1.rr VALUES (2, 10L)").collect()
        session.sql(
            "CREATE TABLE narrow.a1.upd "
            "(id INT NOT NULL, b BIGINT NOT NULL, big BIGINT, sm SMALLINT) USING iceberg"
        ).collect()
        session.sql(
            "INSERT INTO narrow.a1.upd VALUES "
            "(1, 4294967297L, 0L, CAST(0 AS SMALLINT)), (2, 1L, 0L, CAST(0 AS SMALLINT))"
        ).collect()

        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.ansi.enabled", ansi)
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT CAST(TRY_CAST(b AS INT) AS SMALLINT) AS r "
                    "FROM (SELECT CAST(4294967297 AS BIGINT) AS b) t"
                ),
                [[None]],
                ["smallint"],
                [True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(TRY_CAST(b AS INT) AS SMALLINT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, 1]],
                ["int", "smallint"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(TRY_CAST(s AS INT) AS SMALLINT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, 7]],
                ["int", "smallint"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(TRY_CAST(s AS BIGINT) AS INT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, 7]],
                ["int", "int"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.table("narrow.a1.nn")
                .select(
                    "id",
                    functions.col("b").try_cast("int").cast("smallint").alias("r"),
                )
                .orderBy("id"),
                [[1, None], [2, 1]],
                ["int", "smallint"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.table("narrow.a1.nn")
                .select(
                    "id",
                    functions.expr("CAST(TRY_CAST(b AS INT) AS SMALLINT)").alias("r"),
                )
                .orderBy("id"),
                [[1, None], [2, 1]],
                ["int", "smallint"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(-TRY_CAST(b AS INT) AS SMALLINT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, -1]],
                ["int", "smallint"],
                [False, True],
            )
            group_frame = session.sql(
                "SELECT CAST(TRY_CAST(b AS INT) AS SMALLINT) AS k, count(*) AS cnt "
                "FROM narrow.a1.nn GROUP BY CAST(TRY_CAST(b AS INT) AS SMALLINT)"
            )
            group_rows = sorted(
                [(row[0], row[1]) for row in group_frame.collect()],
                key=lambda item: (item[0] is not None, item[0] or 0),
            )
            assert group_rows == [(None, 1), (1, 1)]
            assert [bool(field.nullable) for field in group_frame.schema.fields] == [True, False]

            session.sql(
                f"CREATE TABLE narrow.a1.ctas_{ansi} USING iceberg AS "
                "SELECT id, CAST(TRY_CAST(b AS INT) AS SMALLINT) AS r FROM narrow.a1.nn"
            ).collect()
            _assert_frame_rows_and_nullability(
                session.sql(f"SELECT id, r FROM narrow.a1.ctas_{ansi} ORDER BY id"),
                [[1, None], [2, 1]],
                ["int", "int"],
                [True, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(CASE WHEN id = 2 THEN b END AS INT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, 1]],
                ["int", "int"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(NULLIF(b, 4294967297L) AS INT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, 1]],
                ["int", "int"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT l.id, CAST(r.rb AS INT) AS r "
                    "FROM narrow.a1.nn l LEFT JOIN narrow.a1.rr r ON l.id = r.id ORDER BY l.id"
                ),
                [[1, None], [2, 10]],
                ["int", "int"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST((SELECT rb FROM narrow.a1.rr WHERE id = 999) AS INT) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, None]],
                ["int", "int"],
                [False, True],
            )
            _assert_frame_rows_and_nullability(
                session.sql(
                    "SELECT id, CAST(TRY_CAST(b AS DECIMAL(10,2)) AS DECIMAL(3,2)) AS r "
                    "FROM narrow.a1.nn ORDER BY id"
                ),
                [[1, None], [2, Decimal("1.00")]],
                ["int", "decimal(3,2)"],
                [False, True],
            )
            if ansi == "false":
                _assert_frame_rows_and_nullability(
                    session.sql(
                        "SELECT id, CAST(TRY_CAST(s AS STRING) AS INT) AS r "
                        "FROM narrow.a1.nn ORDER BY id"
                    ),
                    [[1, None], [2, 7]],
                    ["int", "int"],
                    [False, True],
                )
            session.sql(
                "UPDATE narrow.a1.upd SET big = CAST(TRY_CAST(b AS INT) AS BIGINT)"
            ).collect()
            session.sql(
                "UPDATE narrow.a1.upd SET sm = CAST(TRY_CAST(b AS INT) AS SMALLINT)"
            ).collect()
            assert [
                list(row)
                for row in session.sql(
                    "SELECT id, big, sm FROM narrow.a1.upd ORDER BY id"
                ).collect()
            ] == [[1, None, None], [2, 1, 1]]
    finally:
        session.stop()
        _reset_active_session_for_tests()

    native = PyReparkSession.native()
    for sql in (
        "SELECT CAST(TRY_CAST(b AS INT) AS SMALLINT) AS r "
        "FROM (SELECT CAST(4294967297 AS BIGINT) AS b) t",
        "SELECT CAST(-TRY_CAST(b AS INT) AS SMALLINT) AS r "
        "FROM (SELECT CAST(4294967297 AS BIGINT) AS b) t",
    ):
        table = pa.table(native.sql(sql))
        assert table.to_pydict() == {"r": [None]}
        assert table.schema.field(0).nullable is True


def test_unary_plus_suffixed_literals_narrow_as_spark(tmp_path: Path) -> None:
    session = _open(tmp_path / "a2_wh", "false")
    try:
        legacy_cases: list[tuple[str, list[list[Any]], str, bool]] = [
            ("SELECT CAST(+4294967297L AS INT) AS r", [[1]], "int", False),
            ("SELECT CAST((+4294967297L) AS INT) AS r", [[1]], "int", False),
            ("SELECT CAST(+128S AS TINYINT) AS r", [[-128]], "tinyint", False),
            (
                "SELECT CAST(+10.00BD AS DECIMAL(3,2)) AS r",
                [[None]],
                "decimal(3,2)",
                True,
            ),
            ("SELECT CAST(+128.00BD AS TINYINT) AS r", [[-128]], "tinyint", True),
            ("SELECT CAST(+1e10F AS TINYINT) AS r", [[-1]], "tinyint", True),
            (
                "SELECT CAST(+9007199254740992.0D AS INT) AS r",
                [[2147483647]],
                "int",
                True,
            ),
        ]
        for sql, expected_rows, expected_type, expected_null in legacy_cases:
            _assert_frame_rows_and_nullability(
                session.sql(sql),
                expected_rows,
                [expected_type],
                [expected_null],
            )

        session.conf.set("spark.sql.ansi.enabled", "true")
        ansi_cases: list[tuple[str, str, str]] = [
            ("SELECT CAST(+4294967297L AS INT) AS r", "[CAST_OVERFLOW]", "4294967297L"),
            ("SELECT CAST((+4294967297L) AS INT) AS r", "[CAST_OVERFLOW]", "4294967297L"),
            ("SELECT CAST(+128S AS TINYINT) AS r", "[CAST_OVERFLOW]", "128S"),
            (
                "SELECT CAST(+10.00BD AS DECIMAL(3,2)) AS r",
                "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]",
                "10.00",
            ),
            ("SELECT CAST(+128.00BD AS TINYINT) AS r", "[CAST_OVERFLOW]", "128.00BD"),
            ("SELECT CAST(+1e10F AS TINYINT) AS r", "[CAST_OVERFLOW]", "1.0E10"),
            (
                "SELECT CAST(+9007199254740992.0D AS INT) AS r",
                "[CAST_OVERFLOW]",
                "9.007199254740992E15D",
            ),
        ]
        for sql, condition, value_text in ansi_cases:
            with pytest.raises(Exception) as exc_info:
                session.sql(sql).collect()
            text = str(exc_info.value)
            assert condition in text, f"{sql}: {text}"
            assert value_text in text, f"{sql}: {text}"
            assert "SQLSTATE: 22003" in text, f"{sql}: {text}"
    finally:
        session.stop()
        _reset_active_session_for_tests()

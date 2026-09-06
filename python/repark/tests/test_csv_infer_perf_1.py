"""CSV inferSchema plan-time cost and Spark-equal promotion shapes."""

from __future__ import annotations

import datetime
import statistics
import time
from pathlib import Path
from typing import Any

import _live_parity as lp
import pytest

from repark.spark.dataframe import DataFrame

_CSV_OFFSET_INSTANT = datetime.datetime(2020, 6, 1, 10, 0, tzinfo=datetime.UTC)
_CSV_NOON_UTC = datetime.datetime(2020, 6, 1, 12, 0, tzinfo=datetime.UTC)
_CSV_DATE = datetime.date(2020, 6, 1)
_BENCH_ROWS = 300_000
_BENCH_REPEATS = 5


def _session(zone: str = "UTC") -> Any:
    from repark import ReparkSession

    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    return (
        ReparkSession.builder.appName("csv-infer-perf-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )


def _read_inferred(
    session: Any,
    path: Path,
    *,
    null_value: str | None = None,
    infer_schema: bool = True,
    header: bool = True,
) -> Any:
    reader = session.read.option("header", header).option("inferSchema", infer_schema)
    if null_value is not None:
        reader = reader.option("nullValue", null_value)
    return reader.csv(str(path))


def _write_text(path: Path, text: str) -> Path:
    path.write_text(text)
    return path


_MATERIALIZE_HOOK: _MaterializeCounter | None = None


class _MaterializeCounter:
    """Count DataFrame.to_arrow and collect while a CSV inferSchema plan is built."""

    def __init__(self) -> None:
        self.to_arrow = 0
        self.collect = 0
        self._original_to_arrow = DataFrame.to_arrow
        self._original_collect = DataFrame.collect


def _hooked_to_arrow(frame: DataFrame) -> Any:
    hook = _MATERIALIZE_HOOK
    if hook is None:
        raise RuntimeError("materialize hook is not installed")
    hook.to_arrow += 1
    return hook._original_to_arrow(frame)


def _hooked_collect(frame: DataFrame) -> Any:
    hook = _MATERIALIZE_HOOK
    if hook is None:
        raise RuntimeError("materialize hook is not installed")
    hook.collect += 1
    return hook._original_collect(frame)


_TO_ARROW_ATTR = "to_arrow"
_COLLECT_ATTR = "collect"


def _plan_time_materializations(session: Any, path: Path, **read_kwargs: Any) -> dict[str, int]:
    global _MATERIALIZE_HOOK
    counter = _MaterializeCounter()
    _MATERIALIZE_HOOK = counter
    setattr(DataFrame, _TO_ARROW_ATTR, _hooked_to_arrow)
    setattr(DataFrame, _COLLECT_ATTR, _hooked_collect)
    try:
        _ = _read_inferred(session, path, **read_kwargs)
    finally:
        setattr(DataFrame, _TO_ARROW_ATTR, counter._original_to_arrow)
        setattr(DataFrame, _COLLECT_ATTR, counter._original_collect)
        _MATERIALIZE_HOOK = None
    return {"to_arrow": counter.to_arrow, "collect": counter.collect}


def _write_repeated(path: Path, header: str, body: str, last: str, rows: int = 1001) -> Path:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(header + "\n")
        for _ in range(rows - 1):
            handle.write(body + "\n")
        handle.write(last + "\n")
    return path


def _assert_both_doors(
    session: Any, frame: Any, dtypes: list[tuple[str, str]], rows: list[dict[str, Any]]
) -> None:
    frame.createOrReplaceTempView("csv_inferred")
    sql_frame = session.sql("SELECT * FROM csv_inferred")
    assert frame.dtypes == dtypes
    assert sql_frame.dtypes == dtypes
    assert frame.to_arrow().to_pylist() == rows
    assert sql_frame.to_arrow().to_pylist() == rows


def _write_bench_csv(path: Path, rows: int = _BENCH_ROWS) -> Path:
    with path.open("w", encoding="utf-8") as handle:
        handle.write("i,d,nts,ots,dt,s,ni,b\n")
        for index in range(rows):
            na_cell = "NA" if index % 17 == 0 else str(index)
            flag = "true" if index % 2 == 0 else "false"
            handle.write(
                f"{index},{index * 0.5},2020-06-01 12:00:00,2020-06-01T12:00:00+02:00,"
                f"2020-06-01,hello{index},{na_cell},{flag}\n"
            )
    return path


def _median_read_seconds(session: Any, path: Path, *, infer_schema: bool) -> float:
    samples: list[float] = []
    for _ in range(_BENCH_REPEATS):
        started = time.monotonic()
        frame = _read_inferred(session, path, infer_schema=infer_schema)
        frame.to_arrow()
        samples.append(time.monotonic() - started)
    return statistics.median(samples)


def test_infer_schema_trial_path_does_not_materialize_per_column(tmp_path: Path) -> None:
    path = _write_text(
        tmp_path / "mixed.csv",
        "i,d,nts,ots,dt,s,ni,b\n"
        "1,1.5,2020-06-01 12:00:00,2020-06-01T12:00:00+02:00,2020-06-01,hello,1,true\n"
        "2,2.5,2020-06-01 13:00:00,2020-06-01T12:00:00Z,2020-06-02,world,NA,false\n",
    )
    session = _session()
    try:
        counts = _plan_time_materializations(session, path)
        assert counts["collect"] == 0
        assert counts["to_arrow"] <= 1
        false_counts = _plan_time_materializations(session, path, infer_schema=False)
        assert false_counts == {"to_arrow": 0, "collect": 0}
        ints = _write_text(tmp_path / "ints.csv", "id,v\n1,1\n2,2\n")
        int_counts = _plan_time_materializations(session, ints)
        assert int_counts["collect"] == 0
        assert int_counts["to_arrow"] <= 1
        null_path = _write_text(tmp_path / "na.csv", "id,ts\nA,2020-06-01 12:00:00\nB,NA\n")
        null_counts = _plan_time_materializations(session, null_path, null_value="NA")
        assert null_counts["collect"] == 0
        assert null_counts["to_arrow"] <= 1
    finally:
        session.stop()


def test_infer_schema_true_stays_within_twice_false(tmp_path: Path) -> None:
    import repark._native as native

    if native.__debug_assertions__:
        pytest.skip("wall pins run on release modules only")
    path = _write_bench_csv(tmp_path / "bench_300k_x8.csv")
    session = _session()
    try:
        false_median = _median_read_seconds(session, path, infer_schema=False)
        true_median = _median_read_seconds(session, path, infer_schema=True)
        assert true_median <= 2 * false_median, (
            f"inferSchema=True {true_median:.3f}s over 2x inferSchema=False {false_median:.3f}s"
        )
        frame = _read_inferred(session, path)
        assert frame.dtypes == [
            ("i", "bigint"),
            ("d", "double"),
            ("nts", "timestamp"),
            ("ots", "timestamp"),
            ("dt", "date"),
            ("s", "string"),
            ("ni", "string"),
            ("b", "boolean"),
        ]
    finally:
        session.stop()


@pytest.mark.parametrize(
    ("name", "text", "null_value", "dtypes", "rows"),
    [
        (
            "int_then_double",
            "id,v\n1,1\n2,1.5\n",
            None,
            [("id", "bigint"), ("v", "double")],
            [{"id": 1, "v": 1.0}, {"id": 2, "v": 1.5}],
        ),
        (
            "late_bad_int",
            "id,v\n1,1\n2,2\n3,abc\n",
            None,
            [("id", "bigint"), ("v", "string")],
            [{"id": 1, "v": "1"}, {"id": 2, "v": "2"}, {"id": 3, "v": "abc"}],
        ),
        (
            "padded_int",
            "id,v\n1,007\n2,008\n",
            None,
            [("id", "bigint"), ("v", "bigint")],
            [{"id": 1, "v": 7}, {"id": 2, "v": 8}],
        ),
        (
            "na_without_null_value",
            "id,v\n1,1\n2,NA\n",
            None,
            [("id", "bigint"), ("v", "string")],
            [{"id": 1, "v": "1"}, {"id": 2, "v": "NA"}],
        ),
        (
            "bool",
            "id,v\n1,true\n2,false\n",
            None,
            [("id", "bigint"), ("v", "boolean")],
            [{"id": 1, "v": True}, {"id": 2, "v": False}],
        ),
        (
            "string_stays_string",
            "id,v\n1,hello\n2,world\n",
            None,
            [("id", "bigint"), ("v", "string")],
            [{"id": 1, "v": "hello"}, {"id": 2, "v": "world"}],
        ),
        (
            "offset",
            "id,ts\nA,2020-06-01T12:00:00+02:00\n",
            None,
            [("id", "string"), ("ts", "timestamp")],
            [{"id": "A", "ts": _CSV_OFFSET_INSTANT}],
        ),
        (
            "zulu",
            "id,ts\nA,2020-06-01T12:00:00Z\n",
            None,
            [("id", "string"), ("ts", "timestamp")],
            [{"id": "A", "ts": _CSV_NOON_UTC}],
        ),
        (
            "date_only",
            "id,d\nA,2020-06-01\n",
            None,
            [("id", "string"), ("d", "date")],
            [{"id": "A", "d": _CSV_DATE}],
        ),
        (
            "null_value_date",
            "id,d\nA,2020-06-01\nB,NA\n",
            "NA",
            [("id", "string"), ("d", "date")],
            [{"id": "A", "d": _CSV_DATE}, {"id": "B", "d": None}],
        ),
        (
            "null_value_timestamp",
            "id,ts\nA,2020-06-01 12:00:00\nB,NA\n",
            "NA",
            [("id", "string"), ("ts", "timestamp")],
            [{"id": "A", "ts": _CSV_NOON_UTC}, {"id": "B", "ts": None}],
        ),
    ],
)
def test_infer_schema_shapes_match_spark_answers(
    tmp_path: Path,
    name: str,
    text: str,
    null_value: str | None,
    dtypes: list[tuple[str, str]],
    rows: list[dict[str, Any]],
) -> None:
    path = _write_text(tmp_path / f"{name}.csv", text)
    session = _session()
    try:
        frame = _read_inferred(session, path, null_value=null_value)
        frame.createOrReplaceTempView("csv_inferred")
        sql_frame = session.sql("SELECT * FROM csv_inferred")
        assert frame.dtypes == dtypes
        assert sql_frame.dtypes == dtypes
        assert frame.to_arrow().to_pylist() == rows
        assert sql_frame.to_arrow().to_pylist() == rows
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_infer_schema_shapes_match_oracle(tmp_path: Path, spark_engine: lp.Engine) -> None:
    cases: list[tuple[str, str, str | None]] = [
        ("int_then_double", "id,v\n1,1\n2,1.5\n", None),
        ("late_bad_int", "id,v\n1,1\n2,2\n3,abc\n", None),
        ("padded_int", "id,v\n1,007\n2,008\n", None),
        ("na_without_null_value", "id,v\n1,1\n2,NA\n", None),
        ("bool", "id,v\n1,true\n2,false\n", None),
        ("string_stays_string", "id,v\n1,hello\n2,world\n", None),
        ("offset", "id,ts\nA,2020-06-01T12:00:00+02:00\n", None),
        ("zulu", "id,ts\nA,2020-06-01T12:00:00Z\n", None),
        ("date_only", "id,d\nA,2020-06-01\n", None),
        ("null_value_date", "id,d\nA,2020-06-01\nB,NA\n", "NA"),
        ("null_value_timestamp", "id,ts\nA,2020-06-01 12:00:00\nB,NA\n", "NA"),
    ]
    session = _session()
    try:
        for name, text, null_value in cases:
            path = _write_text(tmp_path / f"live_{name}.csv", text)
            repark_csv = _read_inferred(session, path, null_value=null_value)
            spark_csv = _read_inferred(spark_engine.session, path, null_value=null_value)
            repark_csv.createOrReplaceTempView("csv_inferred_live")
            sql_frame = session.sql("SELECT * FROM csv_inferred_live")
            repark_rows = repark_csv.to_arrow().to_pylist()
            spark_rows = spark_engine.arrow_of(spark_csv).to_pylist()
            sql_rows = sql_frame.to_arrow().to_pylist()
            assert len(repark_csv.dtypes) == len(spark_csv.dtypes)
            for repark_field, spark_field in zip(repark_csv.dtypes, spark_csv.dtypes, strict=True):
                assert repark_field[0] == spark_field[0]
                if repark_field[1] != spark_field[1]:
                    assert (repark_field[1], spark_field[1]) == ("bigint", "int")
            assert repark_rows == spark_rows == sql_rows
    finally:
        session.stop()


@pytest.mark.parametrize(
    ("name", "header", "body", "last", "dtypes", "last_row"),
    [
        (
            "late_double_1001",
            "id,v",
            "1,1",
            "1,1.5",
            [("id", "bigint"), ("v", "double")],
            {"id": 1, "v": 1.5},
        ),
        (
            "late_bad_int_1001",
            "id,v",
            "1,1",
            "1,abc",
            [("id", "bigint"), ("v", "string")],
            {"id": 1, "v": "abc"},
        ),
        (
            "late_true_in_int_1001",
            "id,v",
            "1,1",
            "1,true",
            [("id", "bigint"), ("v", "string")],
            {"id": 1, "v": "true"},
        ),
        (
            "late_na_in_int_1001",
            "id,v",
            "1,1",
            "1,NA",
            [("id", "bigint"), ("v", "string")],
            {"id": 1, "v": "NA"},
        ),
        (
            "late_bad_double_1001",
            "id,v",
            "1,1.5",
            "1,abc",
            [("id", "bigint"), ("v", "string")],
            {"id": 1, "v": "abc"},
        ),
        (
            "late_bad_date_1001",
            "id,d",
            "A,2020-06-01",
            "A,not-a-date",
            [("id", "string"), ("d", "string")],
            {"id": "A", "d": "not-a-date"},
        ),
        (
            "late_slash_date_1001",
            "id,d",
            "A,2020-06-01",
            "A,12/31/2020",
            [("id", "string"), ("d", "string")],
            {"id": "A", "d": "12/31/2020"},
        ),
        (
            "late_usdate_in_ts_1001",
            "id,ts",
            "A,2020-06-01 12:00:00",
            "A,12/31/2020",
            [("id", "string"), ("ts", "string")],
            {"id": "A", "ts": "12/31/2020"},
        ),
        (
            "late_ts_in_date_1001",
            "id,d",
            "A,2020-06-01",
            "A,2020-06-01 12:00:00",
            [("id", "string"), ("d", "timestamp")],
            {"id": "A", "d": _CSV_NOON_UTC},
        ),
    ],
)
def test_late_type_conflict_past_sample_is_spark_equal(
    tmp_path: Path,
    name: str,
    header: str,
    body: str,
    last: str,
    dtypes: list[tuple[str, str]],
    last_row: dict[str, Any],
) -> None:
    path = _write_repeated(tmp_path / f"{name}.csv", header, body, last)
    session = _session()
    try:
        frame = _read_inferred(session, path)
        frame.createOrReplaceTempView("csv_inferred")
        sql_frame = session.sql("SELECT * FROM csv_inferred")
        assert frame.dtypes == dtypes
        assert sql_frame.dtypes == dtypes
        assert frame.to_arrow().to_pylist()[-1] == last_row
        assert sql_frame.to_arrow().to_pylist()[-1] == last_row
    finally:
        session.stop()


def test_date_bad_day_stays_string(tmp_path: Path) -> None:
    path = _write_text(tmp_path / "bad_day.csv", "id,d\nA,2020-06-01\nB,2020-13-45\n")
    session = _session()
    try:
        _assert_both_doors(
            session,
            _read_inferred(session, path),
            [("id", "string"), ("d", "string")],
            [{"id": "A", "d": "2020-06-01"}, {"id": "B", "d": "2020-13-45"}],
        )
    finally:
        session.stop()


def test_header_false_offset_timestamp_keeps_instant_in_new_york(tmp_path: Path) -> None:
    path = _write_text(tmp_path / "hfalse.csv", "1,2020-06-01T12:00:00+02:00,2.5\n")
    session = _session("America/New_York")
    try:
        frame = _read_inferred(session, path, header=False)
        frame.createOrReplaceTempView("csv_inferred")
        sql_frame = session.sql("SELECT * FROM csv_inferred")
        assert frame.columns == ["_c0", "_c1", "_c2"]
        assert sql_frame.columns == ["_c0", "_c1", "_c2"]
        assert frame.dtypes == [("_c0", "bigint"), ("_c1", "timestamp"), ("_c2", "double")]
        assert sql_frame.dtypes == frame.dtypes
        assert frame.to_arrow().to_pylist()[0]["_c1"] == _CSV_OFFSET_INSTANT
        assert sql_frame.to_arrow().to_pylist()[0]["_c1"] == _CSV_OFFSET_INSTANT
    finally:
        session.stop()


def test_numeric_grammar_tokens_follow_spark(tmp_path: Path) -> None:
    session = _session()
    try:
        inf_path = _write_text(tmp_path / "inf.csv", "v\nInf\n-Inf\nNaN\n")
        inf_frame = _read_inferred(session, inf_path)
        assert inf_frame.dtypes == [("v", "double")]
        inf_rows = inf_frame.to_arrow().to_pylist()
        assert inf_rows[0]["v"] == float("inf")
        assert inf_rows[1]["v"] == float("-inf")
        assert inf_rows[2]["v"] != inf_rows[2]["v"]
        infy = _write_text(tmp_path / "infy.csv", "v,x\nInfinity,1.5\n")
        infy_frame = _read_inferred(session, infy)
        assert infy_frame.dtypes == [("v", "double"), ("x", "double")]
        plus = _write_text(tmp_path / "plus.csv", "v\n+5\n")
        plus_frame = _read_inferred(session, plus)
        assert plus_frame.dtypes == [("v", "bigint")]
        assert plus_frame.to_arrow().to_pylist() == [{"v": 5}]
        long_path = _write_text(tmp_path / "long.csv", "v\n1.5\n12345678901234567890123\n")
        long_frame = _read_inferred(session, long_path)
        assert long_frame.dtypes == [("v", "double")]
    finally:
        session.stop()


def test_utf8_columns_user_option_does_not_force_string(tmp_path: Path) -> None:
    path = _write_text(tmp_path / "u.csv", "v\n2\n")
    session = _session()
    try:
        frame = (
            session.read.option("utf8_columns", "v")
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(path))
        )
        assert frame.dtypes == [("v", "bigint")]
        assert frame.to_arrow().to_pylist() == [{"v": 2}]
    finally:
        session.stop()


def test_csv_path_argument_is_not_stored_on_the_reader(tmp_path: Path) -> None:
    path = _write_text(tmp_path / "p.csv", "v\n1\n")
    session = _session()
    try:
        reader = session.read.option("header", True).option("inferSchema", True)
        _ = reader.csv(str(path))
        with pytest.raises(Exception, match="path") as raised:
            reader.load()
        assert raised.value is not None
    finally:
        session.stop()


def test_empty_csv_columns_are_string_not_void(tmp_path: Path) -> None:
    session = _session()
    try:
        empty = _write_text(tmp_path / "empty.csv", "1,\n2,\n")
        empty_frame = _read_inferred(session, empty, header=False)
        assert empty_frame.dtypes == [("_c0", "bigint"), ("_c1", "string")]
        header_only = _write_text(tmp_path / "hdr.csv", "a,b\n")
        header_frame = _read_inferred(session, header_only)
        assert header_frame.dtypes == [("a", "string"), ("b", "string")]
    finally:
        session.stop()


def test_twenty_digit_integer_infers_double_not_decimal(tmp_path: Path) -> None:
    path = _write_text(tmp_path / "wide.csv", "v\n12345678901234567890\n")
    session = _session()
    try:
        frame = _read_inferred(session, path)
        assert frame.dtypes == [("v", "double")]
        late = _write_repeated(tmp_path / "late_wide.csv", "id,v", "1,1", "1,12345678901234567890")
        late_frame = _read_inferred(session, late)
        assert late_frame.dtypes == [("id", "bigint"), ("v", "double")]
    finally:
        session.stop()


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_late_and_grammar_shapes_match_oracle(tmp_path: Path, spark_engine: lp.Engine) -> None:
    session = _session()
    try:
        late_path = _write_repeated(tmp_path / "live_late.csv", "id,v", "1,1", "1,1.5")
        repark_late = _read_inferred(session, late_path)
        spark_late = _read_inferred(spark_engine.session, late_path)
        assert repark_late.dtypes[1][1] == "double"
        assert spark_late.dtypes[1][1] == "double"
        assert (
            repark_late.to_arrow().to_pylist()[-1]
            == spark_engine.arrow_of(spark_late).to_pylist()[-1]
        )
        ts_path = _write_repeated(
            tmp_path / "live_ts_date.csv",
            "id,d",
            "A,2020-06-01",
            "A,2020-06-01 12:00:00",
        )
        repark_ts = _read_inferred(session, ts_path)
        spark_ts = _read_inferred(spark_engine.session, ts_path)
        assert repark_ts.dtypes[1][1] == "timestamp"
        assert spark_ts.dtypes[1][1] == "timestamp"
        assert (
            repark_ts.to_arrow().to_pylist()[-1] == spark_engine.arrow_of(spark_ts).to_pylist()[-1]
        )
        hpath = _write_text(tmp_path / "live_hfalse.csv", "1,2020-06-01T12:00:00+02:00,2.5\n")
        repark_h = _read_inferred(session, hpath, header=False)
        spark_h = _read_inferred(spark_engine.session, hpath, header=False)
        assert repark_h.columns == ["_c0", "_c1", "_c2"]
        assert (
            repark_h.to_arrow().to_pylist()[0]["_c1"]
            == spark_engine.arrow_of(spark_h).to_pylist()[0]["_c1"]
        )
        inf_path = _write_text(tmp_path / "live_inf.csv", "v\nInf\n-Inf\n")
        repark_inf = _read_inferred(session, inf_path)
        spark_inf = _read_inferred(spark_engine.session, inf_path)
        assert repark_inf.dtypes == spark_inf.dtypes
        wide = _write_text(tmp_path / "live_wide.csv", "v\n12345678901234567890\n")
        repark_wide = _read_inferred(session, wide)
        spark_wide = _read_inferred(spark_engine.session, wide)
        assert repark_wide.dtypes[0][1] == "double"
        assert spark_wide.dtypes[0][1].startswith("decimal")
    finally:
        session.stop()

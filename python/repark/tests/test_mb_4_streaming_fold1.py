import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from repark import _native
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkNotImplementedError,
    PySparkTypeError,
    StreamingQueryException,
)
from repark.spark.dataframe import DataFrame
from repark.spark.functions import col, pandas_udf, udf
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming import DataStreamReader, DataStreamWriter
from repark.spark.streaming.query import StreamingQueryManager

_MBE8_TEXT = (
    "[REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED] streaming needs a shared catalog; "
    "sc is a local filesystem catalog. Use Glue, S3 Tables, the Postgres catalog or REST"
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-fold1").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    session.stop()


@pytest.fixture
def stream_table(spark: ReparkSession, tmp_path: Path) -> str:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.mb4")
    spark.sql("CREATE TABLE sc.mb4.orders (id BIGINT, k STRING)")
    return "sc.mb4.orders"


def _reader(spark: ReparkSession) -> DataStreamReader:
    return DataStreamReader(spark)


_DM3_TEXT = (
    "Queries with streaming sources must be executed with writeStream.start(), "
    "or from a streaming table or flow definition within a Spark Declarative Pipeline.;\niceberg"
)

_MBE18_TEXT = "[NOT_IMPLEMENTED] Python UDF over a streaming DataFrame is not implemented."
_MBE18_PARAMS = {"feature": "Python UDF over a streaming DataFrame"}


def _udf_frames(stream: DataFrame) -> dict[str, DataFrame]:
    identity = udf(lambda value: value, "long")(col("id"))
    vectorized = pandas_udf(lambda series: series, "long")(col("id"))
    return {
        "udf": stream.withColumn("id", identity),
        "pandas_udf": stream.withColumn("id", vectorized),
        "mapInArrow": stream.mapInArrow(lambda batches: batches, "id long"),
        "mapInPandas": stream.mapInPandas(lambda frames: frames, "id long"),
        "applyInPandas": stream.groupBy("id").applyInPandas(lambda key, pdf: pdf, "id long"),
    }


def _udf_start_doors(
    frame: DataFrame, sink: str, checkpoint: str
) -> dict[str, Callable[[], object]]:
    return {
        "toTable": lambda: (
            frame.writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(sink)
        ),
        "start": lambda: (
            frame.writeStream.format("iceberg")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(sink)
        ),
        "foreach": lambda: (
            frame.writeStream.foreachBatch(lambda batch, epoch: None)
            .option("repark.cdc.sink", sink)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        ),
    }


def _batch_action_doors(frame: DataFrame) -> dict[str, Callable[[], object]]:
    return {
        "collect": frame.collect,
        "take": lambda: frame.take(1),
        "head": lambda: frame.head(1),
        "first": frame.first,
        "tail": lambda: frame.tail(1),
        "isEmpty": frame.isEmpty,
        "toLocalIterator": lambda: list(frame.toLocalIterator()),
        "count": frame.count,
        "show": lambda: frame.show(2),
        "to_arrow": frame.to_arrow,
        "to_arrow_batches": lambda: list(frame.to_arrow_batches()),
        "to_pandas": frame.to_pandas,
        "to_numpy": frame.to_numpy,
    }


_SIGINT_CHILD = """
import os
import signal
import sys
import threading
import time

from repark import _native
from repark.spark.session.session_core import ReparkSession

warehouse, checkpoint, variant = sys.argv[1], sys.argv[2], sys.argv[3]
session = ReparkSession.builder.appName("pytest-mb-4-fold1-sigint").getOrCreate()
_native._streaming_tests_allow_local_catalog(session._ensure_alive())
session.register_memory_catalog("sc", warehouse)
session.sql("CREATE NAMESPACE sc.mb4")
session.sql("CREATE TABLE sc.mb4.src (id BIGINT)")
session.sql("CREATE TABLE sc.mb4.snk (id BIGINT)")
session.sql("INSERT INTO sc.mb4.src VALUES (1), (2), (3)")
query = (
    session.readStream.table("sc.mb4.src")
    .writeStream.option("checkpointLocation", checkpoint)
    .trigger(processingTime="1 second")
    .toTable("sc.mb4.snk")
)
threading.Timer(2.0, lambda: os.kill(os.getpid(), signal.SIGINT)).start()
started = time.time()
try:
    if variant == "await":
        query.awaitTermination()
    elif variant == "await_timeout":
        query.awaitTermination(60.0)
    elif variant == "await_any":
        session.streams.awaitAnyTermination()
    else:
        session.streams.awaitAnyTermination(60.0)
except KeyboardInterrupt:
    print(
        f"WAIT-INTERRUPTED active={query.isActive} elapsed={time.time() - started:.1f}",
        flush=True,
    )
    assert query.isActive
    query.stop()
    session.stop()
else:
    print("WAIT-RETURNED", flush=True)
    raise SystemExit(3)
"""


def _memory_session(app: str, warehouse: Path) -> ReparkSession:
    session = ReparkSession.builder.appName(app).getOrCreate()
    session.register_memory_catalog("sc", str(warehouse))
    return session


def _hadoop_session(app: str, warehouse: Path) -> ReparkSession:
    return (
        ReparkSession.builder.appName(app)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .getOrCreate()
    )


def _stream_tables(spark: ReparkSession) -> tuple[str, str]:
    spark.sql("CREATE NAMESPACE sc.mb4")
    spark.sql("CREATE TABLE sc.mb4.orders (id BIGINT, k STRING)")
    spark.sql("CREATE TABLE sc.mb4.silver (id BIGINT, k STRING)")
    spark.sql("INSERT INTO sc.mb4.orders VALUES (1, 'k1'), (2, 'k0'), (3, 'k1')")
    return ("sc.mb4.orders", "sc.mb4.silver")


def _start_doors(
    spark: ReparkSession, source: str, sink: str, checkpoint: str
) -> dict[str, Callable[[], object]]:
    return {
        "toTable": lambda: (
            spark.readStream.table(source)
            .writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(sink)
        ),
        "start(path)": lambda: (
            spark.readStream.table(source)
            .writeStream.format("iceberg")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start(sink)
        ),
        "foreachBatch": lambda: (
            spark.readStream.table(source)
            .writeStream.foreachBatch(lambda frame, epoch: None)
            .option("repark.cdc.sink", sink)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        ),
    }


def _assert_mbe8(run: Callable[[], object], door: str) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        run()
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED", door
    assert excinfo.value.getSqlState() is None, door
    assert str(excinfo.value) == _MBE8_TEXT, door


def test_public_doors_refuse_memory_catalog_mbe8(tmp_path: Path) -> None:
    spark = _memory_session("pytest-mb-4-fold1-mbe8-memory", tmp_path / "wh")
    try:
        source, sink = _stream_tables(spark)
        for door, run in _start_doors(spark, source, sink, str(tmp_path / "ck")).items():
            _assert_mbe8(run, door)
    finally:
        spark.stop()


def test_public_doors_refuse_hadoop_catalog_mbe8(tmp_path: Path) -> None:
    spark = _hadoop_session("pytest-mb-4-fold1-mbe8-hadoop", tmp_path / "wh")
    try:
        source, sink = _stream_tables(spark)
        for door, run in _start_doors(spark, source, sink, str(tmp_path / "ck")).items():
            _assert_mbe8(run, door)
    finally:
        spark.stop()


def test_test_seam_runs_on_memory_catalog(tmp_path: Path) -> None:
    spark = _memory_session("pytest-mb-4-fold1-mbe8-seam", tmp_path / "wh")
    try:
        _native._streaming_tests_allow_local_catalog(spark._ensure_alive())
        source, sink = _stream_tables(spark)
        query = (
            spark.readStream.table(source)
            .writeStream.option("checkpointLocation", str(tmp_path / "ck"))
            .trigger(availableNow=True)
            .toTable(sink)
        )
        assert query.awaitTermination() is None
        assert spark.table(sink).count() == 3
    finally:
        spark.stop()


def test_python_udf_doors_keep_streaming_true(spark: ReparkSession, stream_table: str) -> None:
    stream = spark.readStream.table(stream_table)
    for name, frame in _udf_frames(stream).items():
        assert frame.isStreaming is True, name
        assert isinstance(frame.writeStream, DataStreamWriter), name
        with pytest.raises(PySparkNotImplementedError) as watermarked:
            frame.withWatermark("id", "1 second")
        assert watermarked.value.getErrorClass() == "NOT_IMPLEMENTED", name


def test_python_udf_over_stream_start_doors_refuse_mbe18(
    spark: ReparkSession, stream_table: str, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.mb4.silver_udf (id BIGINT)")
    stream = spark.readStream.table(stream_table)
    checkpoint = str(tmp_path / "ck")
    for name, frame in _udf_frames(stream).items():
        for door, run in _udf_start_doors(frame, "sc.mb4.silver_udf", checkpoint).items():
            with pytest.raises(PySparkNotImplementedError) as excinfo:
                run()
            assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED", (name, door)
            assert excinfo.value.getMessageParameters() == _MBE18_PARAMS, (name, door)
            assert excinfo.value.getSqlState() is None, (name, door)
            assert str(excinfo.value) == _MBE18_TEXT, (name, door)


def test_python_udf_over_stream_batch_actions_refuse_dm3(
    spark: ReparkSession, stream_table: str
) -> None:
    stream = spark.readStream.table(stream_table)
    frames = _udf_frames(stream)
    for name, frame in frames.items():
        with pytest.raises(AnalysisException) as excinfo:
            frame.collect()
        assert (
            excinfo.value.getCondition(),
            excinfo.value.getSqlState(),
            str(excinfo.value),
        ) == ("_LEGACY_ERROR_TEMP_3102", None, _DM3_TEXT), name
    for name in ("udf", "mapInArrow"):
        for door, run in _batch_action_doors(frames[name]).items():
            with pytest.raises(AnalysisException) as excinfo:
                run()
            assert (
                excinfo.value.getCondition(),
                excinfo.value.getSqlState(),
                str(excinfo.value),
            ) == ("_LEGACY_ERROR_TEMP_3102", None, _DM3_TEXT), (name, door)


def test_test_seam_not_referenced_by_public_package() -> None:
    assert callable(_native._streaming_tests_allow_local_catalog)
    package = Path(__file__).resolve().parents[1] / "src" / "repark"
    hits = sorted(
        str(path.relative_to(package))
        for path in package.rglob("*.py")
        if "_streaming_tests_allow_local_catalog" in path.read_text(encoding="utf-8")
    )
    assert hits == []


@pytest.mark.parametrize("variant", ["await", "await_timeout", "await_any", "await_any_timeout"])
def test_await_variants_honor_sigint_and_leave_query_running(tmp_path: Path, variant: str) -> None:
    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    checkpoint = tmp_path / "ck"
    checkpoint.mkdir()
    started = time.time()
    completed = subprocess.run(
        [sys.executable, "-c", _SIGINT_CHILD, str(warehouse), str(checkpoint), variant],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert time.time() - started < 10.0, completed.stdout + completed.stderr
    assert completed.returncode == 0, completed.stdout + completed.stderr[-2000:]
    assert "WAIT-INTERRUPTED active=True" in completed.stdout


def _fail_batch(frame: DataFrame, batch_id: int) -> None:
    raise RuntimeError("mb4 injected failure at batch")


def test_microbatch_no_credential_on_any_surface_1(spark: ReparkSession, stream_table: str) -> None:
    checkpoint = "s3://ckptuser:hunter2ckpt9x@example.com/mb4-cred-ckpt"
    secret_option = "s3://optuser:hunter2opt9x@example.com/mb4-cred-opt"
    masked_option = "s3://optuser:***@example.com/mb4-cred-opt"
    spark.sql(f"INSERT INTO {stream_table} VALUES (1, 'k1'), (2, 'k0')")
    spark.sql("CREATE TABLE sc.mb4.silver_cred1 (id BIGINT, k STRING)")
    frame = _reader(spark).format("iceberg").option("unrelated", secret_option).load(stream_table)
    query = (
        DataStreamWriter(frame)
        .format("iceberg")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable("sc.mb4.silver_cred1")
    )
    assert query.awaitTermination() is None
    spark.sql("CREATE TABLE sc.mb4.silver_cred2 (id BIGINT, k STRING)")
    failing_frame = (
        _reader(spark).format("iceberg").option("unrelated", secret_option).load(stream_table)
    )
    failing = (
        DataStreamWriter(failing_frame)
        .foreachBatch(_fail_batch)
        .option("repark.cdc.sink", "sc.mb4.silver_cred2")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    with pytest.raises(StreamingQueryException):
        failing.awaitTermination()
    failure_text = str(failing.exception())
    with pytest.raises(AnalysisException) as refused:
        (
            _reader(spark)
            .format("iceberg")
            .option("stream-from-timestamp", secret_option)
            .load(stream_table)
        )
    value_refusal = str(refused.value)
    assert "***" in value_refusal
    timeout_doors = [
        lambda: (
            DataStreamWriter(frame)
            .format("iceberg")
            .option("repark.cdc.catalog-timeout", secret_option)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable("sc.mb4.silver_cred1")
        ),
        lambda: (
            DataStreamWriter(frame)
            .foreachBatch(_fail_batch)
            .option("repark.cdc.sink", "sc.mb4.silver_cred2")
            .option("repark.cdc.catalog-timeout", secret_option)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        ),
    ]
    timeout_refusals = []
    for start in timeout_doors:
        with pytest.raises(IllegalArgumentException) as bad:
            start()
        assert bad.value.getCondition() == "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"
        assert bad.value.getMessageParameters() == {"input": masked_option, "number": masked_option}
        assert masked_option in str(bad.value)
        timeout_refusals.append(str(bad.value))
    with pytest.raises(IllegalArgumentException) as mismatch:
        (
            DataStreamWriter(frame)
            .option("repark.cdc.sink", secret_option)
            .option("checkpointLocation", checkpoint)
            .toTable("sc.mb4.silver_cred1")
        )
    sink_refusal = str(mismatch.value)
    assert sink_refusal == (
        'option "repark.cdc.sink" names "s3://optuser:***@example.com/mb4-cred-opt" '
        'but toTable names "sc.mb4.silver_cred1"; pass one sink'
    )
    with pytest.raises(PySparkNotImplementedError) as reader_format:
        _reader(spark).format(secret_option).load(stream_table)
    with pytest.raises(PySparkNotImplementedError) as writer_format:
        DataStreamWriter(frame).format(secret_option).start(
            "sc.mb4.silver_cred1", checkpointLocation=checkpoint
        )
    format_refusals = [str(reader_format.value), str(writer_format.value)]
    assert format_refusals[0] == (
        "[NOT_IMPLEMENTED] readStream.format(s3://optuser:***@example.com/mb4-cred-opt) "
        "is not implemented."
    )
    assert format_refusals[1] == (
        "[NOT_IMPLEMENTED] writeStream.format(s3://optuser:***@example.com/mb4-cred-opt) "
        "is not implemented."
    )
    assert reader_format.value.getMessageParameters() == {
        "feature": f"readStream.format({masked_option})"
    }
    assert writer_format.value.getMessageParameters() == {
        "feature": f"writeStream.format({masked_option})"
    }
    summaries = str(
        [
            (row.operation, dict(row.summary))
            for row in spark.sql(
                "SELECT operation, summary FROM sc.mb4.silver_cred1.snapshots ORDER BY committed_at"
            ).collect()
        ]
    )
    properties = str(
        [
            (row.key, row.value)
            for row in spark.sql("SHOW TBLPROPERTIES sc.mb4.silver_cred1").collect()
        ]
    )
    surfaces = {
        "status": str(query.status),
        "last_progress": str(query.lastProgress),
        "repr": repr(query),
        "error": failure_text,
        "summaries": summaries,
        "properties": properties,
        "value_refusal": value_refusal,
        "timeout_totable": timeout_refusals[0],
        "timeout_foreach": timeout_refusals[1],
        "sink_refusal": sink_refusal,
        "format_reader": format_refusals[0],
        "format_writer": format_refusals[1],
    }
    for name, text in surfaces.items():
        assert checkpoint not in text, name
        assert secret_option not in text, name
        assert "hunter2ckpt9x" not in text, name
        assert "hunter2opt9x" not in text, name


def test_reader_folded_path_option_loads_the_stream(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _reader(spark).format("iceberg").option("PATH", stream_table).load()
    assert frame.isStreaming is True


def test_writer_folded_path_option_feeds_plain_start(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _reader(spark).format("iceberg").load(stream_table)
    with pytest.raises(AnalysisException) as excinfo:
        (
            frame.writeStream.format("iceberg")
            .option("PATH", "sc.mb4.silver_folded_path")
            .trigger(availableNow=True)
            .start()
        )
    assert excinfo.value.getErrorClass() == "_LEGACY_ERROR_TEMP_1298"


_SPARK_EMPTY_STATEMENT_TEXT = (
    "\n[PARSE_EMPTY_STATEMENT] Syntax error, unexpected empty statement. SQLSTATE: 42617 "
    "(line 1, pos 0)\n\n== SQL ==\n\n^^^\n"
)


def test_totable_empty_name_refuses_parse_empty_statement_like_spark(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _reader(spark).format("iceberg").load(stream_table)
    with pytest.raises(ParseException) as excinfo:
        (
            frame.writeStream.option("checkpointLocation", "/tmp/mb4-totable-empty")
            .trigger(availableNow=True)
            .toTable("")
        )
    assert excinfo.value.getErrorClass() == "PARSE_EMPTY_STATEMENT"
    assert str(excinfo.value) == _SPARK_EMPTY_STATEMENT_TEXT
    assert excinfo.value.getSqlState() == "42617"
    assert excinfo.value.getMessageParameters() == {}


def test_totable_blank_name_echoes_the_input_in_parse_empty_statement(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _reader(spark).format("iceberg").load(stream_table)
    with pytest.raises(ParseException) as excinfo:
        (
            frame.writeStream.option("checkpointLocation", "/tmp/mb4-totable-blank")
            .trigger(availableNow=True)
            .toTable("   ")
        )
    assert excinfo.value.getErrorClass() == "PARSE_EMPTY_STATEMENT"
    assert str(excinfo.value) == _SPARK_EMPTY_STATEMENT_TEXT.replace("\n\n^^^\n", "\n   \n^^^\n")


def test_totable_non_str_name_refuses_not_str(spark: ReparkSession, stream_table: str) -> None:
    frame = _reader(spark).format("iceberg").load(stream_table)
    for bad in (5, None):
        with pytest.raises(PySparkTypeError) as excinfo:
            frame.writeStream.toTable(bad)
        assert excinfo.value.getErrorClass() == "NOT_STR"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "tableName",
            "arg_type": type(bad).__name__,
        }


def test_manager_get_non_str_id_refuses_not_str(spark: ReparkSession) -> None:
    manager = StreamingQueryManager(spark)
    for bad in (5, None):
        with pytest.raises(PySparkTypeError) as excinfo:
            manager.get(bad)
        assert excinfo.value.getErrorClass() == "NOT_STR"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "id",
            "arg_type": type(bad).__name__,
        }

import pytest

from repark.errors import (
    AnalysisException,
    ArithmeticException,
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming import (
    DataStreamReader,
    DataStreamWriter,
    StreamingQuery,
    StreamingQueryManager,
)

_W8_TEXT = (
    'checkpointLocation must be specified either through option("checkpointLocation", ...) '
    'or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...).'
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-surface").getOrCreate()
    yield session
    session.stop()


def _batch_frame(spark: ReparkSession) -> DataFrame:
    return spark.sql("SELECT 1 AS id")


def _reader(spark: ReparkSession) -> DataStreamReader:
    return DataStreamReader(spark)


def _writer(spark: ReparkSession) -> DataStreamWriter:
    return DataStreamWriter(_batch_frame(spark))


def _terminal_type(excinfo: pytest.ExceptionInfo[BaseException]) -> None:
    assert type(excinfo.value) is NotImplementedError


def _ignore_batch(frame: DataFrame, batch_id: int) -> None:
    raise AssertionError("the stub never runs a batch body")


def test_reader_format_returns_self_for_chaining(spark: ReparkSession) -> None:
    reader = _reader(spark)
    assert reader.format("iceberg") is reader


def test_reader_option_overwrites_case_insensitively(spark: ReparkSession) -> None:
    reader = _reader(spark).option("Max-Rows", "1").option("max-rows", "2")
    assert reader._options == {"max-rows": "2"}


def test_reader_option_converts_bool_none_like_pyspark_to_str(spark: ReparkSession) -> None:
    reader = _reader(spark).option("flag", True).option("nothing", None).option("count", 3)
    assert reader._options == {"flag": "true", "nothing": None, "count": "3"}


def test_reader_options_sets_many(spark: ReparkSession) -> None:
    reader = _reader(spark).options(first="1", second="2")
    assert reader._options == {"first": "1", "second": "2"}


def test_reader_load_path_must_be_non_empty_str(spark: ReparkSession) -> None:
    for bad in (123, "", "   "):
        with pytest.raises(PySparkValueError) as excinfo:
            _reader(spark).format("iceberg").load(bad)
        assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "path",
            "arg_value": str(bad),
        }


def test_reader_table_name_must_be_str(spark: ReparkSession) -> None:
    with pytest.raises(PySparkTypeError) as excinfo:
        _reader(spark).format("iceberg").table(123)
    assert excinfo.value.getErrorClass() == "NOT_STR"
    assert excinfo.value.getMessageParameters() == {"arg_name": "tableName", "arg_type": "int"}


def test_reader_format_must_be_iceberg_mbe7(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _reader(spark).format("parquet").load("ice.bronze.orders")
    assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "readStream.format(parquet)"}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "[NOT_IMPLEMENTED] readStream.format(parquet) is not implemented."


def test_reader_format_match_is_case_sensitive_mbe7(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _reader(spark).format("Iceberg").load("ice.bronze.orders")
    assert excinfo.value.getMessageParameters() == {"feature": "readStream.format(Iceberg)"}


def test_reader_missing_format_defaults_to_parquet_refusal_mbe7(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _reader(spark).load("ice.bronze.orders")
    assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "readStream.format(parquet)"}


def test_reader_skip_overwrite_true_refuses_mbe3_mb0_r3(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-skip-overwrite-snapshots", "true")
            .load("ice.bronze.orders")
        )
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.SKIP_OPTION_REFUSED"
    assert excinfo.value.getMessageParameters() == {"option": "streaming-skip-overwrite-snapshots"}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == (
        "[REPARK_MICROBATCH.SKIP_OPTION_REFUSED] "
        "streaming-skip-overwrite-snapshots is not accepted: Bronze is append-only (O-5)"
    )


def test_reader_skip_delete_true_refuses_mbe3_mb0_r6(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-skip-delete-snapshots", True)
            .load("ice.bronze.orders")
        )
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.SKIP_OPTION_REFUSED"
    assert excinfo.value.getMessageParameters() == {"option": "streaming-skip-delete-snapshots"}
    assert excinfo.value.getSqlState() is None


def test_reader_skip_false_passes_to_stub(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-skip-overwrite-snapshots", "false")
            .option("streaming-skip-delete-snapshots", "false")
            .load("ice.bronze.orders")
        )
    _terminal_type(excinfo)


def test_reader_unknown_prefixed_option_refuses_mbe17(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-something-new", "1")
            .load("ice.bronze.orders")
        )
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.UNKNOWN_OPTION"
    assert excinfo.value.getMessageParameters() == {"key": "streaming-something-new"}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == (
        "[REPARK_MICROBATCH.UNKNOWN_OPTION] unknown streaming option streaming-something-new; "
        "remove it or fix the spelling"
    )


def test_reader_plain_unknown_option_passes_to_stub(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _reader(spark).format("iceberg").option("unrelated", "1").load("ice.bronze.orders")
    _terminal_type(excinfo)


def test_reader_option_max_files_passes_to_stub(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-max-files-per-micro-batch", "1")
            .load("ice.bronze.orders")
        )
    _terminal_type(excinfo)


def test_reader_option_max_rows_passes_to_stub(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-max-rows-per-micro-batch", 1)
            .table("ice.bronze.orders")
        )
    _terminal_type(excinfo)


def test_reader_option_from_timestamp_passes_to_stub(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("stream-from-timestamp", "0")
            .load("ice.bronze.orders")
        )
    _terminal_type(excinfo)


def test_reader_option_start_after_snapshot_passes_to_stub(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("repark.cdc.start-after-snapshot-id", "7")
            .table("ice.bronze.orders")
        )
    _terminal_type(excinfo)


def test_reader_none_option_value_drops_before_validation(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("streaming-something-new", None)
            .load("ice.bronze.orders")
        )
    _terminal_type(excinfo)


def test_reader_load_without_path_or_option_refuses(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        _reader(spark).format("iceberg").load()
    assert str(excinfo.value) == "Iceberg streaming load requires a table identifier argument"


def test_reader_load_uses_path_option_when_arg_omitted(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _reader(spark).format("iceberg").option("path", "ice.bronze.orders").load()
    _terminal_type(excinfo)


def test_writer_builders_return_self_for_chaining(spark: ReparkSession) -> None:
    writer = _writer(spark)
    assert writer.format("iceberg") is writer
    assert writer.outputMode("append") is writer
    assert writer.queryName("q") is writer
    assert writer.option("checkpointLocation", "/tmp/x") is writer
    assert writer.options(extra="1") is writer
    assert writer.trigger(availableNow=True) is writer
    assert writer.foreachBatch(_ignore_batch) is writer


def test_writer_outputmode_rejects_blank_value(spark: ReparkSession) -> None:
    for bad in ("", "   ", 123, None):
        with pytest.raises(PySparkValueError) as excinfo:
            _writer(spark).outputMode(bad)
        assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "outputMode",
            "arg_value": str(bad),
        }
        assert excinfo.value.getSqlState() is None


def test_writer_queryname_rejects_blank_value(spark: ReparkSession) -> None:
    for bad in ("", "  ", 123, None):
        with pytest.raises(PySparkValueError) as excinfo:
            _writer(spark).queryName(bad)
        assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "queryName",
            "arg_value": str(bad),
        }


def test_writer_trigger_requires_exactly_one(spark: ReparkSession) -> None:
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).trigger()
    assert excinfo.value.getErrorClass() == "ONLY_ALLOW_SINGLE_TRIGGER"
    assert excinfo.value.getMessageParameters() == {}
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).trigger(processingTime="5 seconds", once=True)
    assert excinfo.value.getErrorClass() == "ONLY_ALLOW_SINGLE_TRIGGER"


def test_writer_trigger_validates_interval_strings(spark: ReparkSession) -> None:
    for kwargs in ({"processingTime": 5}, {"processingTime": "  "}, {"continuous": ""}):
        with pytest.raises(PySparkValueError) as excinfo:
            _writer(spark).trigger(**kwargs)
        assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"


def test_writer_trigger_validates_true_flags(spark: ReparkSession) -> None:
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).trigger(once=False)
    assert excinfo.value.getErrorClass() == "VALUE_NOT_TRUE"
    assert excinfo.value.getMessageParameters() == {"arg_name": "once", "arg_value": "False"}
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).trigger(availableNow=0)
    assert excinfo.value.getErrorClass() == "VALUE_NOT_TRUE"
    assert excinfo.value.getMessageParameters() == {
        "arg_name": "availableNow",
        "arg_value": "0",
    }


def test_writer_trigger_stores_single_trigger(spark: ReparkSession) -> None:
    writer = _writer(spark).trigger(processingTime="  5 seconds  ")
    assert writer._trigger_kind == "processingTime"
    assert writer._trigger_interval == "5 seconds"
    writer = _writer(spark).trigger(once=True)
    assert (writer._trigger_kind, writer._trigger_interval) == ("once", None)
    writer = _writer(spark).trigger(availableNow=True)
    assert (writer._trigger_kind, writer._trigger_interval) == ("availableNow", None)
    writer = _writer(spark).trigger(continuous="1 minute")
    assert (writer._trigger_kind, writer._trigger_interval) == ("continuous", "1 minute")


def test_writer_foreachbatch_stores_callable_without_validation(spark: ReparkSession) -> None:
    assert _writer(spark).foreachBatch(_ignore_batch)._foreach is _ignore_batch
    assert _writer(spark).foreachBatch("not-a-callable")._foreach == "not-a-callable"


def test_writer_start_merges_kwargs_in_pyspark_order(spark: ReparkSession) -> None:
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).format("iceberg").start(outputMode="")
    assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).format("iceberg").start(queryName="  ")
    assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _writer(spark).outputMode("append").start(
            format="iceberg", outputMode="complete", checkpointLocation="/tmp/x"
        )
    assert excinfo.value.getMessageParameters() == {"feature": "outputMode(complete)"}


def test_writer_format_must_be_iceberg_mbe7(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _writer(spark).format("parquet").start(checkpointLocation="/tmp/x")
    assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "writeStream.format(parquet)"}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "[NOT_IMPLEMENTED] writeStream.format(parquet) is not implemented."


def test_writer_missing_format_defaults_to_parquet_refusal_mbe7(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _writer(spark).start(checkpointLocation="/tmp/x")
    assert excinfo.value.getMessageParameters() == {"feature": "writeStream.format(parquet)"}


def test_writer_complete_mode_refuses_mbe6_mb0_w3(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _writer(spark).format("iceberg").outputMode("complete").start(checkpointLocation="/tmp/x")
    assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "outputMode(complete)"}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "[NOT_IMPLEMENTED] outputMode(complete) is not implemented."


def test_writer_update_mode_refuses_mbe6(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        (
            _writer(spark)
            .format("iceberg")
            .outputMode("Update")
            .toTable("ice.sales.silver", checkpointLocation="/tmp/x")
        )
    assert excinfo.value.getMessageParameters() == {"feature": "outputMode(update)"}


def test_writer_append_mode_passes(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").outputMode("Append").start(checkpointLocation="/tmp/x")
    _terminal_type(excinfo)


def test_writer_unknown_mode_passes_to_wireup(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").outputMode("sideways").start(checkpointLocation="/tmp/x")
    _terminal_type(excinfo)


def test_writer_continuous_trigger_refuses_mbe7(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        (
            _writer(spark)
            .format("iceberg")
            .trigger(continuous="5 seconds")
            .start(checkpointLocation="/tmp/x")
        )
    assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "trigger(continuous)"}
    assert excinfo.value.getSqlState() is None


def test_writer_missing_checkpoint_refuses_mbe4_mb0_w8(spark: ReparkSession) -> None:
    for run in (
        lambda: _writer(spark).format("iceberg").start(),
        lambda: _writer(spark).format("iceberg").toTable("ice.sales.silver"),
    ):
        with pytest.raises(AnalysisException) as excinfo:
            run()
        assert excinfo.value.getCondition() == "_LEGACY_ERROR_TEMP_1298"
        assert excinfo.value.getMessageParameters() == {}
        assert excinfo.value.getSqlState() is None
        assert str(excinfo.value) == _W8_TEXT


def test_writer_checkpoint_option_or_conf_passes(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").start(checkpointLocation="/tmp/x")
    _terminal_type(excinfo)
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/conf-check")
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").toTable("ice.sales.silver")
    _terminal_type(excinfo)


def test_writer_foreach_without_sink_refuses_mbe10_mb0_w4(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _writer(spark)
            .format("iceberg")
            .foreachBatch(_ignore_batch)
            .start(checkpointLocation="/tmp/x")
        )
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.SINK_UNDECLARED"
    assert excinfo.value.getMessageParameters() == {}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == (
        "[REPARK_MICROBATCH.SINK_UNDECLARED] no sink declared for this streaming query; "
        'pass .option("repark.cdc.sink", "<table>")'
    )


def test_writer_foreach_with_sink_passes(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _writer(spark)
            .format("iceberg")
            .foreachBatch(_ignore_batch)
            .option("repark.cdc.sink", "ice.sales.silver")
            .start(checkpointLocation="/tmp/x")
        )
    _terminal_type(excinfo)


def test_writer_start_without_foreach_needs_no_sink(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").start(checkpointLocation="/tmp/x")
    _terminal_type(excinfo)


def test_writer_unknown_prefixed_option_refuses_mbe17(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _writer(spark)
            .format("iceberg")
            .option("streaming-zzz", "1")
            .start(checkpointLocation="/tmp/x")
        )
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.UNKNOWN_OPTION"
    assert excinfo.value.getMessageParameters() == {"key": "streaming-zzz"}
    assert excinfo.value.getSqlState() is None


def test_writer_partitionby_accepted_and_ignored(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").start(checkpointLocation="/tmp/x", partitionBy="dt")
    _terminal_type(excinfo)
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").toTable(
            "ice.sales.silver", checkpointLocation="/tmp/x", partitionBy=["a", "b"]
        )
    _terminal_type(excinfo)


def test_writer_path_passes_through_opaquely(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").start(path="/tmp/x", checkpointLocation="/tmp/y")
    _terminal_type(excinfo)


def test_totable_merges_kwargs_like_start(spark: ReparkSession) -> None:
    with pytest.raises(PySparkValueError) as excinfo:
        _writer(spark).format("iceberg").toTable("ice.sales.silver", outputMode="")
    assert excinfo.value.getErrorClass() == "VALUE_NOT_NON_EMPTY_STR"
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).toTable(
            "ice.sales.silver",
            format="iceberg",
            outputMode="append",
            queryName="q",
            checkpointLocation="/tmp/x",
        )
    _terminal_type(excinfo)


def test_totable_needs_no_sink_option(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _writer(spark).format("iceberg").toTable("ice.sales.silver", checkpointLocation="/tmp/x")
    _terminal_type(excinfo)


def test_manager_active_is_empty_on_idle_session(spark: ReparkSession) -> None:
    assert StreamingQueryManager(spark).active == []


def test_manager_get_returns_none_without_queries(spark: ReparkSession) -> None:
    assert StreamingQueryManager(spark).get("no-such-query") is None


def test_manager_await_any_termination_validates_timeout(spark: ReparkSession) -> None:
    for bad in (-1, "x"):
        with pytest.raises(PySparkValueError) as excinfo:
            StreamingQueryManager(spark).awaitAnyTermination(bad)
        assert excinfo.value.getErrorClass() == "VALUE_NOT_POSITIVE"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "timeout",
            "arg_value": type(bad).__name__,
        }
    with pytest.raises(NotImplementedError) as excinfo:
        StreamingQueryManager(spark).awaitAnyTermination(0)
    _terminal_type(excinfo)
    with pytest.raises(NotImplementedError) as excinfo:
        StreamingQueryManager(spark).awaitAnyTermination()
    _terminal_type(excinfo)


def test_manager_methods_refuse_on_stopped_session() -> None:
    session = ReparkSession.builder.appName("pytest-mb-4-stopped").getOrCreate()
    manager = StreamingQueryManager(session)
    session.stop()
    with pytest.raises(RuntimeError):
        _ = manager.active
    with pytest.raises(RuntimeError):
        manager.get("x")
    with pytest.raises(RuntimeError):
        manager.awaitAnyTermination()


def test_query_await_termination_validates_timeout() -> None:
    for bad in (-1, 0, "x"):
        with pytest.raises(PySparkValueError) as excinfo:
            StreamingQuery().awaitTermination(bad)
        assert excinfo.value.getErrorClass() == "VALUE_NOT_POSITIVE"
        assert excinfo.value.getMessageParameters() == {
            "arg_name": "timeout",
            "arg_value": type(bad).__name__,
        }
    with pytest.raises(NotImplementedError) as excinfo:
        StreamingQuery().awaitTermination()
    _terminal_type(excinfo)
    with pytest.raises(NotImplementedError) as excinfo:
        StreamingQuery().awaitTermination(5)
    _terminal_type(excinfo)


def test_query_surface_raises_stub_terminal() -> None:
    query = StreamingQuery()
    for probe in (
        lambda: query.id,
        lambda: query.runId,
        lambda: query.name,
        lambda: query.isActive,
        lambda: query.status,
        lambda: query.lastProgress,
        lambda: query.recentProgress,
        query.stop,
        query.exception,
    ):
        with pytest.raises(NotImplementedError) as excinfo:
            probe()
        _terminal_type(excinfo)


def test_writer_trigger_accepts_five_seconds_mb0c_t1(spark: ReparkSession) -> None:
    writer = _writer(spark).trigger(processingTime="5 seconds")
    assert writer._trigger_kind == "processingTime"
    assert writer._trigger_interval == "5 seconds"


def test_writer_trigger_accepts_padded_interval_mb0c_t1(spark: ReparkSession) -> None:
    writer = _writer(spark).trigger(processingTime=" 5 seconds ")
    assert writer._trigger_kind == "processingTime"
    assert writer._trigger_interval == "5 seconds"


def test_writer_trigger_bogus_refuses_unrecognized_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Unrecognized number bogus") as excinfo:
        _writer(spark).trigger(processingTime="bogus")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'bogus' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Unrecognized number bogus. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "bogus", "number": "bogus"}


def test_writer_trigger_padded_bogus_echoes_stripped_mb0c_t1c(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Unrecognized number bogus") as excinfo:
        _writer(spark).trigger(processingTime="  bogus")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'bogus' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Unrecognized number bogus. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "bogus", "number": "bogus"}


def test_writer_trigger_missing_unit_refuses_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="after 5 but hit EOL") as excinfo:
        _writer(spark).trigger(processingTime="5")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.MISSING_UNIT"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '5' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Expect a unit name after 5 but hit EOL. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "5", "word": "5"}


def test_writer_trigger_invalid_unit_refuses_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Invalid unit secs") as excinfo:
        _writer(spark).trigger(processingTime="5 secs")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.INVALID_UNIT"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 secs' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Invalid unit secs. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "5 secs", "unit": "secs"}


def test_writer_trigger_invalid_value_refuses_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Invalid value 5s") as excinfo:
        _writer(spark).trigger(processingTime="5s")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.INVALID_VALUE"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '5s' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Invalid value 5s. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "5s", "value": "5s"}


def test_writer_trigger_fraction_refuses_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="cannot have fractional part") as excinfo:
        _writer(spark).trigger(processingTime="1.5 minutes")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.INVALID_FRACTION"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 minutes' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "minutes cannot have fractional part. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "1.5 minutes", "unit": "minutes"}


def test_writer_trigger_prefix_refuses_mb0c_t1c(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Invalid interval prefix") as excinfo:
        _writer(spark).trigger(processingTime="interval5 seconds")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.INVALID_PREFIX"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.INVALID_PREFIX] Error parsing 'interval5 seconds' to "
        "interval. Please ensure that the value provided is in a valid format for defining "
        "an interval. You can reference the documentation for the correct format. "
        "Invalid interval prefix interval5. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {
        "input": "interval5 seconds",
        "prefix": "interval5",
    }


def test_writer_trigger_missing_number_refuses_mb0c_t1c(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Expect a number after") as excinfo:
        _writer(spark).trigger(processingTime="5 seconds -")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.MISSING_NUMBER"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.MISSING_NUMBER] Error parsing '5 seconds -' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Expect a number after - but hit EOL. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "5 seconds -", "word": "-"}


def test_writer_trigger_wrapped_arithmetic_refuses_mb0c_t1b(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Uncaught arithmetic exception") as excinfo:
        _writer(spark).trigger(processingTime="2147483648 days")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '2147483648 days' to "
        "interval. Please ensure that the value provided is in a valid format for defining "
        "an interval. You can reference the documentation for the correct format. "
        "Uncaught arithmetic exception while parsing '2147483648 days'. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "2147483648 days"}


def test_writer_trigger_raw_overflow_refuses_mb0c_t1b(spark: ReparkSession) -> None:
    with pytest.raises(ArithmeticException, match="long overflow") as excinfo:
        _writer(spark).trigger(processingTime="2147483647 days")
    assert excinfo.value.getCondition() is None
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "long overflow"
    assert excinfo.value.getMessageParameters() is None


def test_writer_trigger_months_refuse_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="month or year interval") as excinfo:
        _writer(spark).trigger(processingTime="1 month")
    assert excinfo.value.getCondition() == "_LEGACY_ERROR_TEMP_3262"
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "Doesn't support month or year interval: 1 month"
    assert excinfo.value.getMessageParameters() == {"interval": "1 month"}


def test_writer_trigger_negative_refuses_mb0c_t1(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="should not be negative") as excinfo:
        _writer(spark).trigger(processingTime="-1 seconds")
    assert excinfo.value.getCondition() is None
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == (
        "requirement failed: the interval of trigger should not be negative"
    )
    assert excinfo.value.getMessageParameters() is None


def test_writer_trigger_interval_alone_refuses_empty_mb0c_t1c(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="cannot be empty") as excinfo:
        _writer(spark).trigger(processingTime="interval")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY"
    assert excinfo.value.getSqlState() == "22006"
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY] Error parsing 'interval' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Interval string cannot be empty. SQLSTATE: 22006"
    )
    assert excinfo.value.getMessageParameters() == {"input": "interval"}


def test_writer_trigger_continuous_bogus_refuses_at_trigger_mb0c_t3(spark: ReparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="Unrecognized number bogus") as excinfo:
        _writer(spark).trigger(continuous="bogus")
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"
    assert excinfo.value.getSqlState() == "22006"
    assert excinfo.value.getMessageParameters() == {"input": "bogus", "number": "bogus"}

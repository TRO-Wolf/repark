from __future__ import annotations

import errno
import shutil
import traceback
from collections.abc import Callable
from pathlib import Path

import pytest

from repark.errors import AnalysisException, IllegalArgumentException, PySparkException
from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark._secrets import scrub_exception
from repark.spark.dataframe.export_errors import _export_engine_error
from repark.spark.session.reader_support import _parse_snapshot_id_option

MARK = "S3cr3tPw"

SHUFFLE = "spark.sql.shuffle.partitions"
MAX_ARRAY = "repark.sql.maxArrayElements"
DF_BATCH = "datafusion.execution.batch_size"
WRITE_FILES = "repark.write.max-concurrent-files"

KNOBS = [SHUFFLE, MAX_ARRAY, DF_BATCH, WRITE_FILES]

SHAPES = ["jdbc", "dsn", "odbc"]

DOORS = ["builder", "toml", "confset", "sqlset"]

GUARD_KNOBS = [
    SHUFFLE,
    "repark.memory.limit.gb",
    "spark.sql.execution.arrow.maxRecordsPerBatch",
]

USERINFO = "u" + ":pw@"


def _guard_value(kind: str) -> str:
    if kind == "backslash":
        return f"Driver=x;Uid=u;Pwd={MARK}\\9;"
    return f"Driver=x;Uid=u;Pwd={MARK};" + "Database=" + "d" * 300 + ";"


EXPECTED: dict[tuple[str, str], type[BaseException] | None] = {
    ("builder", SHUFFLE): IllegalArgumentException,
    ("builder", MAX_ARRAY): AnalysisException,
    ("builder", DF_BATCH): IllegalArgumentException,
    ("builder", WRITE_FILES): IllegalArgumentException,
    ("toml", SHUFFLE): IllegalArgumentException,
    ("toml", MAX_ARRAY): AnalysisException,
    ("toml", DF_BATCH): IllegalArgumentException,
    ("toml", WRITE_FILES): IllegalArgumentException,
    ("confset", SHUFFLE): None,
    ("confset", MAX_ARRAY): None,
    ("confset", DF_BATCH): IllegalArgumentException,
    ("confset", WRITE_FILES): None,
    ("sqlset", SHUFFLE): IllegalArgumentException,
    ("sqlset", MAX_ARRAY): None,
    ("sqlset", DF_BATCH): PySparkException,
    ("sqlset", WRITE_FILES): None,
}


def _shape_value(shape: str, token: str) -> str:
    if shape == "jdbc":
        return f"jdbc:postgresql://h/db?user=u&password={token}"
    if shape == "dsn":
        return f"host=h user=u password={token}"
    return f"Driver=x;Server=h;Uid=u;Pwd={token};"


def _attempt_builder(key: str, value: str) -> None:
    session = SparkSession.builder.config(key, value).getOrCreate()
    try:
        session.sql("SELECT 1").collect()
    finally:
        session.stop()


def _attempt_toml(key: str, value: str, tmp_path: Path) -> None:
    path = tmp_path / "repark.toml"
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    path.write_text(f'[default.conf]\n"{key}" = "{escaped}"\n', encoding="utf-8")
    session = SparkSession.builder.configFile(str(path)).getOrCreate()
    try:
        session.sql("SELECT 1").collect()
    finally:
        session.stop()


def _attempt_conf_set(key: str, value: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        session.conf.set(key, value)
        session.sql("SELECT array(1) AS a").collect()
    finally:
        session.stop()


def _attempt_sql_set(key: str, value: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        session.sql(f"SET {key} = '{value}'").collect()
        session.sql("SELECT array(1) AS a").collect()
    finally:
        session.stop()


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("knob", KNOBS)
@pytest.mark.parametrize("door", DOORS)
def test_config_value_never_echoes_a_credential(
    door: str, knob: str, shape: str, tmp_path: Path
) -> None:
    token = f"{MARK}{DOORS.index(door)}{KNOBS.index(knob)}{SHAPES.index(shape)}"
    value = _shape_value(shape, token)
    expected = EXPECTED[(door, knob)]
    try:
        if door == "builder":
            _attempt_builder(knob, value)
        elif door == "toml":
            _attempt_toml(knob, value, tmp_path)
        elif door == "confset":
            _attempt_conf_set(knob, value)
        else:
            _attempt_sql_set(knob, value)
    except BaseException as error:
        assert expected is not None
        assert type(error) is expected
        rendered = "".join(traceback.format_exception(error))
        assert MARK not in rendered
        assert "***" in rendered
    else:
        assert expected is None


@pytest.mark.parametrize("knob", GUARD_KNOBS)
@pytest.mark.parametrize("kind", ["backslash", "long"])
def test_credential_shaped_integer_refusal_cuts_the_chain(knob: str, kind: str) -> None:
    try:
        SparkSession.builder.config(knob, _guard_value(kind)).getOrCreate()
    except BaseException as error:
        assert type(error) is IllegalArgumentException
        assert error.__cause__ is None
        rendered = "".join(traceback.format_exception(error))
        assert MARK not in rendered
        assert "***" in rendered
    else:
        pytest.fail(f"{knob} accepted a non-integer")


@pytest.mark.parametrize("scheme", ["file", "hdfs"])
def test_orc_message_parameters_mask_url_userinfo(scheme: str) -> None:
    host = "localhost/tmp/nope/x" if scheme == "file" else "h/x"
    url = scheme + "://" + USERINFO + host
    session = SparkSession.builder.getOrCreate()
    try:
        session.read.orc(url).collect()
    except BaseException as error:
        assert type(error) is AnalysisException
        params = error.getMessageParameters()
        assert set(params) == {"path"}
        assert USERINFO not in repr(params)
    else:
        pytest.fail(f"orc read of a {scheme} userinfo path unexpectedly succeeded")
    finally:
        session.stop()


def test_rest_catalog_uri_chain_carries_no_userinfo() -> None:
    uri = "http://" + USERINFO + "127.0.0.1:9/"
    builder = (
        SparkSession.builder.config("spark.sql.catalog.c", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.c.type", "rest")
        .config("spark.sql.catalog.c.uri", uri)
    )
    try:
        session = builder.getOrCreate()
    except BaseException as error:
        assert USERINFO not in "".join(traceback.format_exception(error))
        return
    try:
        session.sql("SHOW NAMESPACES IN c").collect()
    except BaseException as error:
        assert USERINFO not in "".join(traceback.format_exception(error))
    else:
        pytest.fail("rest catalog read unexpectedly succeeded")
    finally:
        session.stop()


def test_scrub_exception_masks_cause_chain_args() -> None:
    cause = ValueError("http://" + USERINFO + "127.0.0.1:9/refused")
    context = RuntimeError("during catalog read")
    context.__cause__ = cause
    outer = AnalysisException("listNamespaces failed")
    outer.__context__ = context
    scrubbed = scrub_exception(outer)
    assert USERINFO in repr(cause.args)
    assert USERINFO in "".join(traceback.format_exception(outer))
    assert USERINFO not in "".join(traceback.format_exception(scrubbed))


def test_reader_path_chain_carries_no_userinfo() -> None:
    url = "file://" + USERINFO + "localhost/tmp/nope/x"
    session = SparkSession.builder.getOrCreate()
    try:
        session.read.parquet(url).collect()
    except BaseException as error:
        rendered = "".join(traceback.format_exception(error))
        assert USERINFO not in rendered
    else:
        pytest.fail("parquet read unexpectedly succeeded")
    finally:
        session.stop()


def test_writer_option_value_never_echoes(tmp_path: Path) -> None:
    value = f"host=h user=u password={MARK}W4"
    session = SparkSession.builder.getOrCreate()
    try:
        session.range(2).write.option("compression", value).parquet(str(tmp_path / "w"))
    except BaseException as error:
        rendered = "".join(traceback.format_exception(error))
        assert MARK not in rendered
        assert "***" in rendered
    else:
        pytest.fail("writer option refusal unexpectedly succeeded")
    finally:
        session.stop()


UDF_USER_TEXT = "lookup failed for key: customer_id (token: abc123 expired)"


def _boom_user_text(value: object) -> str:
    raise ValueError(UDF_USER_TEXT)


def _boom_user_url(value: object) -> str:
    raise ValueError("fetch failed for " + "http://" + USERINFO + "127.0.0.1:9/x")


def test_sql_udf_user_text_is_byte_identical() -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        session.udf.register("boom", _boom_user_text, "string")
        with pytest.raises(PySparkException) as caught:
            session.sql("SELECT boom(id) FROM range(2)").collect()
        assert UDF_USER_TEXT in str(caught.value)
    finally:
        session.stop()


def test_dataframe_udf_user_text_is_byte_identical() -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        frame = session.range(2).select(F.udf(_boom_user_text, "string")("id"))
        with pytest.raises(PySparkException) as caught:
            frame.collect()
        assert UDF_USER_TEXT in str(caught.value)
    finally:
        session.stop()


def test_sql_udf_user_url_is_masked() -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        session.udf.register("boom_url", _boom_user_url, "string")
        with pytest.raises(PySparkException) as caught:
            session.sql("SELECT boom_url(id) FROM range(2)").collect()
        rendered = str(caught.value)
        assert USERINFO not in rendered
        assert "***" in rendered
        assert USERINFO not in "".join(traceback.format_exception(caught.value))
    finally:
        session.stop()


def test_scrub_exception_returns_copies_and_keeps_originals() -> None:
    cause = ValueError("http://" + USERINFO + "127.0.0.1:9/refused")
    outer = AnalysisException("listNamespaces failed")
    outer.__cause__ = cause
    scrubbed = scrub_exception(outer)
    assert scrubbed is not outer
    assert type(scrubbed) is AnalysisException
    assert str(scrubbed) == "listNamespaces failed"
    assert isinstance(scrubbed.__cause__, ValueError)
    assert USERINFO in repr(cause.args)
    assert USERINFO not in "".join(traceback.format_exception(scrubbed))


def test_scrub_exception_keeps_identity_without_secrets() -> None:
    error = ValueError(UDF_USER_TEXT)
    assert scrub_exception(error) is error


def test_scrub_exception_leaves_decode_error_untouched() -> None:
    try:
        b"\xff".decode("utf-8")
    except UnicodeDecodeError as error:
        assert scrub_exception(error) is error
    else:
        pytest.fail("invalid utf-8 unexpectedly decoded")


def test_reader_option_mode_value_is_masked(tmp_path: Path) -> None:
    value = f"host=h user=u password={MARK}M1"
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(AnalysisException) as caught:
            session.read.option("mode", value).csv(str(tmp_path / "zz")).collect()
        rendered = str(caught.value)
        assert MARK not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_reader_format_value_is_masked(tmp_path: Path) -> None:
    value = f"host=h user=u password={MARK}F1"
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(AnalysisException) as caught:
            session.read.format(value).load(str(tmp_path / "zz")).collect()
        rendered = str(caught.value)
        assert MARK not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_reader_snapshot_id_value_is_masked() -> None:
    with pytest.raises(AnalysisException) as caught:
        _parse_snapshot_id_option(f"host=h user=u password={MARK}S1")
    rendered = str(caught.value)
    assert MARK not in rendered
    assert "***" in rendered


def test_reader_jdbc_int_option_value_is_masked() -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        reader = (
            session.read.format("postgres")
            .option("url", "postgresql://h/db")
            .option("dbtable", "t")
            .option("partitionColumn", "id")
            .option("lowerBound", f"host=h user=u password={MARK}J1")
            .option("upperBound", "10")
            .option("numPartitions", "2")
        )
        with pytest.raises(IllegalArgumentException) as caught:
            reader.load()
        rendered = str(caught.value)
        assert MARK not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_reader_orc_merge_schema_value_is_masked(tmp_path: Path) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(IllegalArgumentException) as caught:
            session.read.orc(str(tmp_path / "zz"), mergeSchema=f"host=h user=u password={MARK}O1")
        rendered = str(caught.value)
        assert MARK not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_reader_text_encoding_value_is_masked(tmp_path: Path) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(AnalysisException) as caught:
            session.read.option("encoding", f"host=h user=u password={MARK}T1").text(
                str(tmp_path / "zz")
            ).collect()
        rendered = str(caught.value)
        assert MARK not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_writer_format_value_is_masked(tmp_path: Path) -> None:
    value = f"host=h user=u password={MARK}D1"
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(AnalysisException) as caught:
            session.range(2).write.format(value).save(str(tmp_path / "h"))
        rendered = str(caught.value)
        assert MARK.lower() not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_scrub_exception_rebuilds_os_error() -> None:
    original = FileNotFoundError(2, "No such file", "http://" + USERINFO + "h/x")
    scrubbed = scrub_exception(original)
    assert scrubbed is not original
    assert type(scrubbed) is FileNotFoundError
    assert scrubbed.errno == 2
    assert USERINFO in str(original.filename)
    assert USERINFO not in str(scrubbed)
    assert USERINFO not in str(scrubbed.filename)
    assert "***" in str(scrubbed.filename)


def test_scrub_exception_rebuilds_os_error_strerror() -> None:
    original = OSError(2, "connect " + "http://" + USERINFO + "h/db failed")
    scrubbed = scrub_exception(original)
    assert scrubbed is not original
    assert type(scrubbed) is type(original)
    assert scrubbed.errno == 2
    assert USERINFO in str(original)
    assert USERINFO not in str(scrubbed)
    assert "***" in str(scrubbed)


def test_scrub_exception_masks_os_error_filename2() -> None:
    original = OSError(2, "No such file", "/tmp/plain")
    original.filename2 = "http://" + USERINFO + "h/y"
    scrubbed = scrub_exception(original)
    assert scrubbed is not original
    assert USERINFO in str(original.filename2)
    assert USERINFO not in str(scrubbed.filename2)
    assert "***" in str(scrubbed.filename2)


_REAL_RMTREE = shutil.rmtree


def _raise_permission_denied_for_out(path: object, *args: object, **kwargs: object) -> None:
    if Path(str(path)).name == "out":
        raise OSError(13, "Permission denied", "http://" + USERINFO + "127.0.0.1:9/x")
    _REAL_RMTREE(path, *args, **kwargs)


def test_writer_overwrite_os_error_is_masked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "out"
    target.mkdir()
    (target / "seed").write_text("x", encoding="utf-8")
    monkeypatch.setattr(shutil, "rmtree", _raise_permission_denied_for_out)
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(AnalysisException) as caught:
            session.range(2).write.mode("overwrite").parquet(str(target))
        rendered = "".join(traceback.format_exception(caught.value))
        assert USERINFO not in rendered
        assert "***" in rendered
        assert str(target) in rendered
    finally:
        session.stop()


def test_text_write_overwrite_os_error_is_masked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "out"
    target.mkdir()
    (target / "seed").write_text("x", encoding="utf-8")
    monkeypatch.setattr(shutil, "rmtree", _raise_permission_denied_for_out)
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(AnalysisException) as caught:
            session.sql("SELECT 'a' AS v").write.mode("overwrite").text(str(target))
        rendered = "".join(traceback.format_exception(caught.value))
        assert USERINFO not in rendered
        assert "***" in rendered
        assert str(target) in rendered
    finally:
        session.stop()


_REST_CHAIN_URI = "http://" + USERINFO + "127.0.0.1:9/v1"
_REST_CHAIN_CAUSE = ConnectionError("connect failed for " + _REST_CHAIN_URI)
_REST_CHAIN_FAILURE = RuntimeError("catalog read failed")
_REST_CHAIN_FAILURE.__cause__ = _REST_CHAIN_CAUSE


def _raise_rest_chain(self: object, sql: object) -> object:
    raise _REST_CHAIN_FAILURE


def test_list_databases_scrubs_cause_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        monkeypatch.setattr(type(session), "_sql_built", _raise_rest_chain)
        with pytest.raises(AnalysisException) as caught:
            session.catalog.listDatabases()
        rendered = "".join(traceback.format_exception(caught.value))
        assert USERINFO not in rendered
        assert "***" in rendered
        assert USERINFO in str(_REST_CHAIN_CAUSE)
    finally:
        session.stop()


def _raise_materialize_url_error(self: object) -> None:
    raise IllegalArgumentException("read failed for " + "http://" + USERINFO + "h/x")


def _raise_materialize_user_text(self: object) -> None:
    raise IllegalArgumentException(UDF_USER_TEXT)


def test_eager_materialize_refusal_masks_url(monkeypatch: pytest.MonkeyPatch) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        frame = session.range(2)
        monkeypatch.setattr(
            type(frame), "_materialize_cache_if_needed", _raise_materialize_url_error
        )
        with pytest.raises(IllegalArgumentException) as caught:
            frame.eager()
        rendered = "".join(traceback.format_exception(caught.value))
        assert USERINFO not in rendered
        assert "***" in rendered
    finally:
        session.stop()


def test_eager_materialize_refusal_keeps_user_text(monkeypatch: pytest.MonkeyPatch) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        frame = session.range(2)
        monkeypatch.setattr(
            type(frame), "_materialize_cache_if_needed", _raise_materialize_user_text
        )
        with pytest.raises(IllegalArgumentException) as caught:
            frame.eager()
        assert UDF_USER_TEXT in str(caught.value)
    finally:
        session.stop()


def test_export_engine_error_masks_url() -> None:
    error = RuntimeError("export failed for " + "http://" + USERINFO + "h/x")
    mapped = _export_engine_error(error)
    assert USERINFO not in str(mapped)
    assert "***" in str(mapped)
    assert USERINFO in str(error)
    assert USERINFO not in "".join(traceback.format_exception(mapped))


def test_export_engine_error_keeps_user_text() -> None:
    mapped = _export_engine_error(RuntimeError(UDF_USER_TEXT))
    assert UDF_USER_TEXT in str(mapped)


CAST_ACTIONS = ["collect", "toArrow", "toPandas", "to_arrow_batches"]


def _run_export_action(frame: object, action: str) -> None:
    if action == "to_arrow_batches":
        list(frame.to_arrow_batches())
        return
    getattr(frame, action)()


@pytest.mark.parametrize("action", CAST_ACTIONS)
def test_collect_cast_error_traceback_is_masked(action: str) -> None:
    if action == "toPandas":
        pytest.importorskip("pandas")
    session = SparkSession.builder.config("spark.sql.ansi.enabled", "true").getOrCreate()
    try:
        rows = [("1",), ("http://u:" + MARK + "@h/x",)]
        session.createDataFrame(rows, ["v"]).createOrReplaceTempView("t")
        frame = session.sql("SELECT CAST(v AS INT) FROM t")
        with pytest.raises(PySparkException) as caught:
            _run_export_action(frame, action)
        assert MARK not in "".join(traceback.format_exception(caught.value))
    finally:
        session.stop()


def test_registered_dsn_traceback_is_masked() -> None:
    secret = "RegPw" + "0042"
    dsn = "host=h user=u password=" + secret
    builder = SparkSession.builder.config("spark.sql.ansi.enabled", "true")
    session = builder.config("spark.repark.test.dsn", dsn).getOrCreate()
    try:
        session.createDataFrame([("1",), (dsn,)], ["v"]).createOrReplaceTempView("t")
        with pytest.raises(PySparkException) as caught:
            session.sql("SELECT CAST(v AS INT) FROM t").collect()
        assert secret not in "".join(traceback.format_exception(caught.value))
    finally:
        session.stop()


SECRET_URL = "http://u:" + MARK + "@h/x"


class _TwoArgError(Exception):
    def __init__(self, url: str, code: int) -> None:
        super().__init__(f"connect {url} failed code={code}")


class _PathOSError(OSError):
    def __init__(self, path: str) -> None:
        super().__init__(errno.ENOENT, "nope", path)


class _KeywordOnlyError(Exception):
    def __new__(cls, *, url: str) -> _KeywordOnlyError:
        return super().__new__(cls, url)

    def __init__(self, *, url: str) -> None:
        super().__init__(f"connect {url} failed")


def _raised(factory: Callable[[], BaseException]) -> BaseException:
    try:
        raise factory()
    except BaseException as error:
        return error


def _raise_wrapped_two_arg() -> BaseException:
    try:
        raise _TwoArgError(SECRET_URL, 7)
    except _TwoArgError as inner:
        try:
            raise RuntimeError("wrapped") from inner
        except RuntimeError as outer:
            return outer


@pytest.mark.parametrize(
    "factory",
    [
        lambda: _raised(lambda: _TwoArgError(SECRET_URL, 7)),
        lambda: _raised(lambda: _PathOSError("/tmp/" + SECRET_URL)),
        _raise_wrapped_two_arg,
        lambda: _raised(lambda: _KeywordOnlyError(url=SECRET_URL)),
    ],
    ids=["two_arg", "os_subclass", "wrapped_two_arg", "keyword_only"],
)
def test_scrub_exception_copy_failure_never_returns_the_original(
    factory: Callable[[], BaseException],
) -> None:
    original = factory()
    links = [original, original.__cause__]
    before = [
        (link, link.args, link.__cause__, link.__context__, link.__traceback__)
        for link in links
        if link is not None
    ]
    scrubbed = scrub_exception(original)
    assert scrubbed is not original
    assert MARK not in "".join(traceback.format_exception(scrubbed))
    for link, args, cause, context, trace in before:
        assert link.args is args
        assert link.__cause__ is cause
        assert link.__context__ is context
        assert link.__traceback__ is trace


def test_scrub_exception_copy_failure_keeps_type_and_os_fields() -> None:
    original = _raised(lambda: _PathOSError("/tmp/" + SECRET_URL))
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is _PathOSError
    assert scrubbed.errno == errno.ENOENT
    assert scrubbed.strerror == "nope"
    assert MARK not in str(scrubbed.filename)
    assert type(scrub_exception(_TwoArgError(SECRET_URL, 7))) is _TwoArgError


def test_scrub_exception_unbuildable_link_becomes_masked_stand_in() -> None:
    scrubbed = scrub_exception(_KeywordOnlyError(url=SECRET_URL))
    assert type(scrubbed) is PySparkException
    assert MARK not in str(scrubbed)
    assert "connect http://u:" in str(scrubbed)

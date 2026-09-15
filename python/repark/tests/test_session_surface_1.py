"""SESSION-SURFACE-1 — the 19 SparkSession names against the PySpark 4.1.2 oracle.

Oracle cells live in ``facade_session_oracle.json`` (recorded on live PySpark
4.1.2 by the orchestrator). Implemented names pin columns + schema simpleString
+ Row reprs; declared names pin today's refusal so they go red when the engine
gains the feature.

pins: session-surface-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

import repark.functions as F  # noqa: N812
from repark import ReparkSession
from repark.errors import (
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkRuntimeError,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.session import _reset_active_session_for_tests

CELLS: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_session_oracle.json").read_text()
)["cells"]


def _cell(name: str) -> dict[str, Any]:
    return CELLS[name]


def _frame_pin(frame: Any, cell: str) -> None:
    expected = _cell(cell)["result"]
    assert frame.columns == expected["columns"]
    assert frame.schema.simpleString() == expected["schema"]
    assert [repr(row) for row in frame.collect()] == expected["rows"]


@pytest.fixture
def spark() -> Any:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-session-surface-1").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def test_tags_round_trip(spark: ReparkSession) -> None:
    """cells addTag / getTags / removeTag / clearTags. pins: session-surface-1/C-001"""
    assert spark.addTag("t1") is None
    assert spark.addTag("t2") is None
    assert spark.getTags() == {"t1", "t2"}
    assert spark.removeTag("t1") is None
    assert spark.getTags() == {"t2"}
    assert spark.removeTag("never") is None
    assert spark.removeTag("") is None
    assert spark.getTags() == {"t2"}
    assert spark.clearTags() is None
    assert spark.getTags() == set()


def test_get_tags_returns_fresh_set(spark: ReparkSession) -> None:
    """cell getTags_type_initial — a set whose mutation cannot leak.

    pins: session-surface-1/C-001
    """
    assert type(spark.getTags()) is set
    spark.addTag("keep")
    leaked = spark.getTags()
    leaked.discard("keep")
    assert spark.getTags() == {"keep"}


def test_add_tag_validation(spark: ReparkSession) -> None:
    """cells addTag_empty / addTag_comma — Spark's messages. pins: session-surface-1/C-001"""
    with pytest.raises(IllegalArgumentException) as raised:
        spark.addTag("")
    assert str(raised.value) == _cell("addTag_empty")["error"]["message"]
    with pytest.raises(IllegalArgumentException) as raised:
        spark.addTag("a,b")
    assert str(raised.value) == _cell("addTag_comma")["error"]["message"]


def test_add_tag_not_str_declared(spark: ReparkSession) -> None:
    """cell addTag_not_str — R-1: NOT_STR where Spark leaks a Py4JError (SES-TAG-1).

    pins: session-surface-1/C-001
    """
    with pytest.raises(PySparkTypeError) as raised:
        spark.addTag(5)  # type: ignore[arg-type]
    assert raised.value.getCondition() == "NOT_STR"
    assert raised.value.getMessageParameters() == {"arg_name": "tag", "arg_type": "int"}


def test_interrupts_answer_empty_lists(spark: ReparkSession) -> None:
    """cells interruptAll / interruptTag / interruptOperation_numeric — nothing running.

    pins: session-surface-1/C-002
    """
    assert spark.interruptAll() == []
    assert spark.interruptTag("t2") == []
    assert spark.interruptOperation("123") == []


def test_interrupt_operation_validation(spark: ReparkSession) -> None:
    """cell interruptOperation — non-numeric id refuses. pins: session-surface-1/C-002"""
    with pytest.raises(IllegalArgumentException) as raised:
        spark.interruptOperation("abc")
    assert str(raised.value) == _cell("interruptOperation")["error"]["message"]


_CONNECT_ONLY = (
    ("client", lambda spark: spark.client),
    ("copyFromLocalToFs", lambda spark: spark.copyFromLocalToFs("/tmp/x", "/tmp/y")),
    ("registerProgressHandler", lambda spark: spark.registerProgressHandler(lambda **kw: None)),
    ("removeProgressHandler", lambda spark: spark.removeProgressHandler(lambda **kw: None)),
    ("clearProgressHandlers", lambda spark: spark.clearProgressHandlers()),
)


@pytest.mark.parametrize("cell,call", _CONNECT_ONLY, ids=[name for name, _ in _CONNECT_ONLY])
def test_connect_only_refusals(spark: ReparkSession, cell: str, call: Any) -> None:
    """The five Connect-only names match classic's refusal byte-for-byte.

    pins: session-surface-1/C-003
    """
    expected = _cell(cell)["error"]
    with pytest.raises(PySparkRuntimeError) as raised:
        call(spark)
    assert raised.value.getCondition() == expected["condition"]
    assert raised.value.getMessageParameters() == expected["params"]
    assert str(raised.value) == expected["message"]


def test_read_stream_declared(spark: ReparkSession) -> None:
    """SES-DECL-readStream — no streaming engine. pins: session-surface-1/C-004"""
    with pytest.raises(PySparkNotImplementedError) as raised:
        _ = spark.readStream
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "readStream"}


def test_streams_declared(spark: ReparkSession) -> None:
    """SES-DECL-streams — no StreamingQueryManager. pins: session-surface-1/C-004"""
    with pytest.raises(PySparkNotImplementedError) as raised:
        _ = spark.streams
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "streams"}


def test_data_source_declared(spark: ReparkSession) -> None:
    """SES-DECL-dataSource — the Python data source API is deferred.

    pins: session-surface-1/C-004
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        _ = spark.dataSource
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "dataSource"}


def test_add_artifacts_two_flags(spark: ReparkSession) -> None:
    """cell addArtifacts_two_flags — one flag max; Spark's formatter crashes rendering it.

    pins: session-surface-1/C-005
    """
    with pytest.raises(PySparkValueError) as raised:
        spark.addArtifacts("/tmp/nope.py", pyfile=True, archive=True)
    assert raised.value.getCondition() == "INVALID_MULTIPLE_ARGUMENT_CONDITIONS"
    assert raised.value.getMessageParameters() == {
        "arg_names": "'pyfile', 'archive' and/or 'file'",
        "condition": "True together",
    }


def test_add_artifact_missing_path(spark: ReparkSession) -> None:
    """cells addArtifact / addArtifacts — a missing path names itself.

    pins: session-surface-1/C-005
    """
    for call in (spark.addArtifact, spark.addArtifacts):
        with pytest.raises(FileNotFoundError, match=r"/nonexistent-dir/nope\.py"):
            call("/nonexistent-dir/nope.py", pyfile=True)


def test_add_artifact_pyfile_lands_on_sys_path(spark: ReparkSession, tmp_path: Path) -> None:
    """cell addArtifact_real — driver-local copy plus one sys.path prepend.

    pins: session-surface-1/C-005
    """
    module = tmp_path / "ses_surface_mod_a1.py"
    module.write_text("MARKER = 71\n")
    sys.modules.pop("ses_surface_mod_a1", None)
    try:
        assert spark.addArtifact(str(module), pyfile=True) is None
        loaded = importlib.import_module("ses_surface_mod_a1")
        assert loaded.MARKER == 71
        assert spark.addArtifact(str(module), pyfile=True) is None
    finally:
        sys.modules.pop("ses_surface_mod_a1", None)


def test_add_artifact_no_flags_is_a_noop(spark: ReparkSession) -> None:
    """Spark no-flag addArtifact returns None without touching the filesystem.

    pins: session-surface-1/C-005
    """
    assert spark.addArtifact("/nonexistent-dir/nope.py") is None
    assert spark.addArtifacts("/nonexistent-dir/nope.py") is None


def test_add_artifacts_archive_and_file_declared(spark: ReparkSession, tmp_path: Path) -> None:
    """SES-ARTIFACT-1 — archive/file kinds are declared, driver-local pyfile only.

    pins: session-surface-1/C-005
    """
    real = tmp_path / "blob.bin"
    real.write_bytes(b"x")
    for flag, feature in (
        ({"archive": True}, "addArtifacts(archive=True)"),
        ({"file": True}, "addArtifacts(file=True)"),
    ):
        with pytest.raises(PySparkNotImplementedError) as raised:
            spark.addArtifacts(str(real), **flag)  # type: ignore[arg-type]
        assert raised.value.getCondition() == "NOT_IMPLEMENTED"
        assert raised.value.getMessageParameters() == {"feature": feature}


def test_profile_surface(spark: ReparkSession) -> None:
    """cells profile_type / profile_methods — the facade Profile object.

    pins: session-surface-1/C-006
    """
    assert type(spark.profile).__name__ == _cell("profile_type")["result"]["value"]
    expected = [item["value"] for item in _cell("profile_methods")["result"]["items"]]
    assert sorted(m for m in dir(spark.profile) if not m.startswith("_")) == expected


def test_profile_show_dump_clear_noop(spark: ReparkSession, tmp_path: Path, capsys: Any) -> None:
    """cell profile_show — nothing collected: nothing printed, memory warning kept.

    pins: session-surface-1/C-006
    """
    with pytest.warns(UserWarning, match="memory_profiler"):
        assert spark.profile.show() is None
    assert capsys.readouterr().out == ""
    with pytest.warns(UserWarning, match="memory_profiler"):
        assert spark.profile.show(id=3) is None
    dump_dir = tmp_path / "profiles"
    with pytest.warns(UserWarning, match="memory_profiler"):
        assert spark.profile.dump(str(dump_dir)) is None
    assert not dump_dir.exists()
    assert spark.profile.clear() is None
    assert spark.profile.clear(id=3, type="perf") is None


def test_profile_render_declared(spark: ReparkSession) -> None:
    """SES-PROFILE-1 — render is declared, no UDF profiles exist to render.

    pins: session-surface-1/C-006
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.profile.render(0)
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "profile.render"}


def test_profile_type_validation(spark: ReparkSession) -> None:
    """Spark's VALUE_NOT_ALLOWED on an unknown profiler type. pins: session-surface-1/C-006"""
    for call in (
        lambda: spark.profile.show(type="bogus"),
        lambda: spark.profile.dump("/tmp/x", type="bogus"),
        lambda: spark.profile.clear(type="bogus"),
    ):
        with pytest.raises(PySparkValueError) as raised:
            call()
        assert raised.value.getCondition() == "VALUE_NOT_ALLOWED"
        assert raised.value.getMessageParameters() == {
            "arg_name": "type",
            "allowed_values": "['perf', 'memory']",
        }


def test_tvf_type_and_methods(spark: ReparkSession) -> None:
    """cells tvf_type / tvf_methods_full — exact public method set. pins: session-surface-1/C-007"""
    assert type(spark.tvf).__name__ == _cell("tvf_type")["result"]["value"]
    expected = [item["value"] for item in _cell("tvf_methods_full")["result"]["items"]]
    assert sorted(m for m in dir(spark.tvf) if not m.startswith("_")) == expected


def test_tvf_range(spark: ReparkSession) -> None:
    """cell tvf_range — tvf.range delegates to spark.range. pins: session-surface-1/C-007"""
    _frame_pin(spark.tvf.range(3), "tvf_range")


def test_tvf_explode(spark: ReparkSession) -> None:
    """cell tvf_explode — the generator select answers Spark. pins: session-surface-1/C-007"""
    _frame_pin(spark.tvf.explode(F.array(F.lit(1), F.lit(2))), "tvf_explode")


def test_tvf_explode_outer_empty(spark: ReparkSession) -> None:
    """cell tvf_explode_outer_empty — empty int array yields one null row.

    pins: session-surface-1/C-007
    """
    _frame_pin(
        spark.tvf.explode_outer(F.slice(F.array(F.lit(1), F.lit(2)), 1, 0)),
        "tvf_explode_outer_empty",
    )


def test_tvf_stack(spark: ReparkSession) -> None:
    """cell tvf_stack — StackCall lowers in select. pins: session-surface-1/C-007"""
    _frame_pin(
        spark.tvf.stack(F.lit(2), F.lit(1), F.lit(2), F.lit(3), F.lit(4)),
        "tvf_stack",
    )


def test_tvf_posexplode_declared_today(spark: ReparkSession) -> None:
    """cell tvf_posexplode — today's refusal; RED when the ordinal unnest lands (run 15a).

    pins: session-surface-1/C-007
    """
    with pytest.raises(UnsupportedOperationException, match="posexplode"):
        spark.tvf.posexplode(F.array(F.lit("a"), F.lit("b")))
    with pytest.raises(UnsupportedOperationException, match="posexplode"):
        spark.tvf.posexplode_outer(F.array(F.lit("a")))


def test_tvf_json_tuple_declared_today(spark: ReparkSession) -> None:
    """cell tvf_json_tuple — the json_tuple kernel refusal rides through.

    pins: session-surface-1/C-007
    """
    with pytest.raises(UnsupportedOperationException, match="json_tuple"):
        spark.tvf.json_tuple(F.lit('{"a":1,"b":2}'), F.lit("a"), F.lit("b"))


def test_tvf_inline_and_variant_declared_today(spark: ReparkSession) -> None:
    """cells tvf_inline / tvf_variant_explode — no F.* generator exists yet.

    pins: session-surface-1/C-007
    """
    for name, arg in (
        ("inline", F.array(F.struct(F.lit(1).alias("a"), F.lit("x").alias("b")))),
        ("inline_outer", F.array(F.struct(F.lit(1).alias("a")))),
        ("variant_explode", F.col("x")),
        ("variant_explode_outer", F.col("x")),
    ):
        with pytest.raises(PySparkNotImplementedError) as raised:
            getattr(spark.tvf, name)(arg)
        assert raised.value.getCondition() == "NOT_IMPLEMENTED"
        assert raised.value.getMessageParameters() == {"feature": f"tvf.{name}"}


def test_tvf_sql_functions_declared_today(spark: ReparkSession) -> None:
    """cells tvf_sql_keywords / tvf_collations — the SQL door has no such table function.

    pins: session-surface-1/C-007
    """
    for name in ("sql_keywords", "collations", "python_worker_logs"):
        with pytest.raises(PySparkNotImplementedError) as raised:
            getattr(spark.tvf, name)()
        assert raised.value.getCondition() == "NOT_IMPLEMENTED"
        assert raised.value.getMessageParameters() == {"feature": f"tvf.{name}"}


def test_tvf_non_column_argument_refuses(spark: ReparkSession) -> None:
    """Ruling: non-Column arguments raise NOT_COLUMN. pins: session-surface-1/C-007"""
    with pytest.raises(PySparkTypeError) as raised:
        spark.tvf.explode(5)  # type: ignore[arg-type]
    assert raised.value.getCondition() == "NOT_COLUMN"
    assert raised.value.getMessageParameters() == {
        "arg_name": "collection",
        "arg_type": "int",
    }


def test_tvf_json_tuple_requires_a_field(spark: ReparkSession) -> None:
    """Spark's CANNOT_BE_EMPTY on an empty field list. pins: session-surface-1/C-007"""
    with pytest.raises(PySparkValueError) as raised:
        spark.tvf.json_tuple(F.lit("{}"))
    assert raised.value.getCondition() == "CANNOT_BE_EMPTY"
    assert raised.value.getMessageParameters() == {"item": "field"}

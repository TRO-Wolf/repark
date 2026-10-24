"""Execute the live cross-read function bodies without AWS or the native engine.

pins: aws-ephemeral-runs-1/C-009
"""

from __future__ import annotations

import ast
import io
import sys
from collections.abc import Iterator
from functools import partial
from pathlib import Path
from typing import Any, NamedTuple, NoReturn
from unittest.mock import Mock, call

import pytest

_SOURCE = (
    Path(__file__).resolve().parents[2] / "repark/tests/parity/test_aws_acceptance_cross_read.py"
)
_PRIVATE_METADATA = "s3://private-metadata-must-not-be-printed"


class _ProgressStream(io.StringIO):
    """Keep flushed output separate so callbacks can inspect visible progress."""

    def __init__(self) -> None:
        """Initialize an empty output stream and its flushed view."""
        super().__init__()
        self.flushed: str = ""

    def flush(self) -> None:
        """Expose the output written before the caller flushes the stream."""
        self.flushed = self.getvalue()


def _load_namespace(prefix: str = "testing_") -> dict[str, Any]:
    """Compile the exact source functions while excluding live module imports."""
    tree = ast.parse(_SOURCE.read_text(encoding="utf-8"), filename=str(_SOURCE))
    names = {"_discover_tables", "_cross_read_namespace"}
    functions = [
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    assert {node.name for node in functions} == names
    imports = [
        node
        for node in tree.body
        if (isinstance(node, ast.ImportFrom) and node.module == "__future__")
        or (isinstance(node, ast.Import) and any(alias.name == "time" for alias in node.names))
    ]
    namespace: dict[str, Any] = {"run_table_prefix": lambda: prefix}
    module = ast.Module(body=[*imports, *functions], type_ignores=[])
    exec(compile(module, str(_SOURCE), "exec"), namespace)
    return namespace


def _listing_session(names: list[str], temporary: list[bool]) -> Mock:
    """Return a session whose SHOW TABLES result contains the supplied rows."""
    columns = {
        "tableName": Mock(to_pylist=Mock(return_value=names)),
        "isTemporary": Mock(to_pylist=Mock(return_value=temporary)),
    }
    arrow = Mock(column=Mock(side_effect=columns.__getitem__))
    return Mock(sql=Mock(return_value=Mock(to_arrow=Mock(return_value=arrow))))


def _record_metadata(
    stream: _ProgressStream,
    checkpoints: list[tuple[str, str]],
    repark: Mock,
    table: str,
) -> str:
    """Record visible progress at metadata lookup and return a private sentinel."""
    checkpoints.append((f"metadata:{table}", stream.flushed))
    return _PRIVATE_METADATA


def _record_pair(
    stream: _ProgressStream,
    checkpoints: list[tuple[str, str]],
    outcomes: Iterator[Mock],
    repark: Mock,
    repark_table: str,
    spark: Mock,
    spark_table: str,
    metadata_file: str,
) -> Mock:
    """Record visible progress at engine-pair entry and return the next outcome."""
    checkpoints.append((f"pair:{repark_table}", stream.flushed))
    return next(outcomes)


def _fail_after_snapshot(stream: _ProgressStream, snapshots: list[str], *args: Any) -> NoReturn:
    """Record visible progress before a controlled external-call failure."""
    snapshots.append(stream.flushed)
    raise RuntimeError("controlled external-call failure")


def test_discovery_keeps_only_the_run_prefix_and_never_the_bench_table() -> None:
    """Another run's tables, unprefixed names and the bench table are not discovered."""
    namespace = _load_namespace("testing_r77_")
    session = _listing_session(
        [
            "testing_r77_b",
            "testing_r770_x",
            "testing_r78_a",
            "testing_r77_a",
            "testing_old_a",
            "ice_read_perf_bench",
            "testing_r77_tmp",
        ],
        [False, False, False, False, False, False, True],
    )
    assert namespace["_discover_tables"](session, "catalog", "scratch") == [
        "testing_r77_a",
        "testing_r77_b",
    ]


@pytest.mark.parametrize("first_status", ["EQUAL", "DIFFERENT"])
def test_progress_preserves_sorted_selection_and_asserts_every_outcome(
    monkeypatch: pytest.MonkeyPatch, first_status: str
) -> None:
    """A first mismatch cannot hide later tables or bypass the final assertion."""
    namespace = _load_namespace()
    repark = _listing_session(
        ["testing_z", "unrelated", "testing_temporary", "testing_a", "testing_middle"],
        [False, False, True, False, False],
    )
    spark = Mock()
    stream = _ProgressStream()
    monkeypatch.setattr(sys, "stdout", stream)
    checkpoints: list[tuple[str, str]] = []
    outcomes = [Mock(status=first_status), Mock(status="EQUAL"), Mock(status="EQUAL")]
    metadata = Mock(side_effect=partial(_record_metadata, stream, checkpoints))
    pair = Mock(side_effect=partial(_record_pair, stream, checkpoints, iter(outcomes)))
    formatter = Mock(return_value="FINAL_OUTCOMES")
    assertion = Mock()
    if first_status == "DIFFERENT":
        assertion.side_effect = AssertionError("cross-read mismatch")
    namespace.update(
        _metadata_file_or_none=metadata,
        cross_read_table_pair=pair,
        format_outcome_table=formatter,
        assert_outcomes_expected=assertion,
        time=Mock(monotonic=Mock(side_effect=[100.0, 101.25, 200.0, 202.5, 300.0, 305.0])),
    )
    if first_status == "DIFFERENT":
        with pytest.raises(AssertionError, match="cross-read mismatch"):
            namespace["_cross_read_namespace"](repark, spark, "catalog", "scratch")
    else:
        namespace["_cross_read_namespace"](repark, spark, "catalog", "scratch")

    tables = [f"catalog.scratch.{name}" for name in ("testing_a", "testing_middle", "testing_z")]
    repark.sql.assert_called_once_with("SHOW TABLES IN catalog.scratch")
    assert metadata.call_args_list == [call(repark, table) for table in tables]
    assert pair.call_args_list == [
        call(repark, table, spark, table, _PRIVATE_METADATA) for table in tables
    ]
    assert formatter.call_args_list == [
        *[call([outcome]) for outcome in outcomes],
        call(outcomes),
    ]
    assertion.assert_called_once_with(outcomes)

    visible = "Cross-read catalog.scratch: discovered 3 tables\n"
    expected_checkpoints: list[tuple[str, str]] = []
    for number, (table, status, elapsed) in enumerate(
        zip(tables, [first_status, "EQUAL", "EQUAL"], [1.25, 2.5, 5.0], strict=True), start=1
    ):
        visible += f"Cross-read [{number}/3] {table}: metadata lookup\n"
        expected_checkpoints.append((f"metadata:{table}", visible))
        visible += f"Cross-read {table}: engine pair\n"
        expected_checkpoints.append((f"pair:{table}", visible))
        visible += f"Cross-read {table}: {status} ({elapsed:.3f}s)\nFINAL_OUTCOMES\n"
    assert checkpoints == expected_checkpoints
    assert stream.flushed == visible
    assert stream.getvalue() == visible + "FINAL_OUTCOMES\n"
    assert _PRIVATE_METADATA not in stream.getvalue()
    assert namespace["time"].monotonic.call_count == 6


@pytest.mark.parametrize("phase", ["metadata", "pair"])
def test_progress_is_flushed_before_failure_without_false_completion(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """A failing call leaves its table and current phase visible without a result."""
    namespace = _load_namespace()
    repark = _listing_session(["testing_a"], [False])
    stream = _ProgressStream()
    monkeypatch.setattr(sys, "stdout", stream)
    snapshots: list[str] = []
    metadata = Mock(return_value=_PRIVATE_METADATA)
    pair = Mock(return_value=Mock(status="EQUAL"))
    failing_call = metadata if phase == "metadata" else pair
    failing_call.side_effect = partial(_fail_after_snapshot, stream, snapshots)
    formatter = Mock()
    assertion = Mock()
    namespace.update(
        _metadata_file_or_none=metadata,
        cross_read_table_pair=pair,
        format_outcome_table=formatter,
        assert_outcomes_expected=assertion,
        time=Mock(monotonic=Mock(return_value=100.0)),
    )
    with pytest.raises(RuntimeError, match="controlled external-call failure"):
        namespace["_cross_read_namespace"](repark, Mock(), "catalog", "scratch")
    visible = (
        "Cross-read catalog.scratch: discovered 1 tables\n"
        "Cross-read [1/1] catalog.scratch.testing_a: metadata lookup\n"
    )
    if phase == "pair":
        visible += "Cross-read catalog.scratch.testing_a: engine pair\n"
        pair.assert_called_once()
    else:
        pair.assert_not_called()
    assert snapshots == [visible]
    assert stream.getvalue() == visible
    formatter.assert_not_called()
    assertion.assert_not_called()


def test_empty_discovery_reports_zero_and_retains_the_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty scratch selection still fails before metadata or pair calls."""
    namespace = _load_namespace()
    repark = _listing_session(["unrelated", "testing_temporary"], [False, True])
    stream = _ProgressStream()
    monkeypatch.setattr(sys, "stdout", stream)
    external = Mock()
    namespace.update(
        _metadata_file_or_none=external.metadata,
        cross_read_table_pair=external.pair,
        format_outcome_table=external.formatter,
        assert_outcomes_expected=external.check_outcomes,
    )
    with pytest.raises(
        AssertionError, match=r"no testing_ tables under catalog\.scratch to cross-read"
    ):
        namespace["_cross_read_namespace"](repark, Mock(), "catalog", "scratch")
    assert stream.flushed == "Cross-read catalog.scratch: discovered 0 tables\n"
    assert external.mock_calls == []


def _load_reporting_namespace() -> dict[str, Any]:
    """Load the exact outcome types, formatter and assertion without live imports."""
    source = _SOURCE.with_name("_acceptance_cross_read.py")
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    names = {
        "CrossReadColumn",
        "CrossReadOutcome",
        "format_outcome_table",
        "assert_outcomes_expected",
        "spark_error_first_line",
    }
    constants = {"EQUAL", "DIFFERENT", "SPARK_CANNOT_READ", "_MAX_ERROR_LINE_CHARS"}
    nodes = [
        node
        for node in tree.body
        if (isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names)
        or (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id in constants for target in node.targets
            )
        )
        or (isinstance(node, ast.ImportFrom) and node.module == "__future__")
    ]
    loaded = {node.name for node in nodes if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    assert loaded == names
    namespace: dict[str, Any] = {"NamedTuple": NamedTuple, "__name__": __name__}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    return namespace


@pytest.mark.parametrize("phase", ["complete", "metadata", "pair"])
@pytest.mark.parametrize(
    ("status", "has_ns_columns", "condition"),
    [
        ("EQUAL", False, None),
        ("DIFFERENT", False, None),
        ("SPARK-CANNOT-READ", True, "UNSUPPORTED_TYPE"),
        ("SPARK-CANNOT-READ", False, "UNSUPPORTED_TYPE"),
        ("SPARK-CANNOT-READ", True, None),
        ("SPARK-CANNOT-READ", False, None),
    ],
)
def test_completed_details_flush_before_next_call_and_preserve_final_report(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    status: str,
    has_ns_columns: bool,
    condition: str | None,
) -> None:
    """Stored details stay visible through later failure and the real final assertion."""
    reporting = _load_reporting_namespace()
    error_line = "Spark read failed: " + "x" * 320
    error = RuntimeError(error_line + "\nprivate second line")
    first_line = reporting["spark_error_first_line"](error)
    assert first_line == error_line[:300]
    table = "catalog.scratch.testing_a"
    spark_counts = {"EQUAL": 2, "DIFFERENT": 3, "SPARK-CANNOT-READ": None}
    outcome = reporting["CrossReadOutcome"](
        table=table,
        status=status,
        repark_columns=[],
        spark_columns=[],
        schema_equal=status == "EQUAL",
        rows_equal=status == "EQUAL" if status != "SPARK-CANNOT-READ" else None,
        repark_row_count=2,
        spark_row_count=spark_counts[status],
        first_differences=["schema differs", "row differs", "third difference"],
        spark_error_class="RuntimeError" if status == "SPARK-CANNOT-READ" else None,
        spark_error_condition=condition,
        spark_error_first_line=first_line if status == "SPARK-CANNOT-READ" else None,
        has_ns_columns=has_ns_columns,
    )
    if status == "EQUAL":
        detail, spark_rows = "row-equal", "2"
    elif status == "DIFFERENT":
        detail, spark_rows = "schema differs; row differs", "3"
    else:
        label = "RuntimeError UNSUPPORTED_TYPE" if condition else "RuntimeError"
        detail, spark_rows = f"{label}: {first_line}", "-"
    if has_ns_columns:
        detail = f"ns-columns; {detail}"
    rendered = (
        "table | status | repark_rows | spark_rows | detail\n"
        f"{table} | {status} | 2 | {spark_rows} | {detail}"
    )
    assert reporting["format_outcome_table"]([outcome]) == rendered
    namespace = _load_namespace()
    names = ["testing_a"] if phase == "complete" else ["testing_a", "testing_z"]
    repark = _listing_session(names, [False] * len(names))
    stream = _ProgressStream()
    monkeypatch.setattr(sys, "stdout", stream)
    metadata = Mock(return_value=_PRIVATE_METADATA)
    pair = Mock(side_effect=[outcome, RuntimeError("controlled next-call failure")])
    if phase == "metadata":
        metadata.side_effect = [_PRIVATE_METADATA, RuntimeError("controlled next-call failure")]
    formatter = Mock(wraps=reporting["format_outcome_table"])
    assertion = Mock(wraps=reporting["assert_outcomes_expected"])
    namespace.update(
        _metadata_file_or_none=metadata,
        cross_read_table_pair=pair,
        format_outcome_table=formatter,
        assert_outcomes_expected=assertion,
        time=Mock(monotonic=Mock(side_effect=[100.0, 101.25, 200.0])),
    )
    is_unexpected = status == "DIFFERENT" or (status == "SPARK-CANNOT-READ" and not has_ns_columns)
    if phase != "complete":
        with pytest.raises(RuntimeError, match="controlled next-call failure"):
            namespace["_cross_read_namespace"](repark, Mock(), "catalog", "scratch")
    elif is_unexpected:
        with pytest.raises(AssertionError, match="cross-read found 1 unexpected outcome"):
            namespace["_cross_read_namespace"](repark, Mock(), "catalog", "scratch")
    else:
        namespace["_cross_read_namespace"](repark, Mock(), "catalog", "scratch")
    visible = (
        f"Cross-read catalog.scratch: discovered {len(names)} tables\n"
        f"Cross-read [1/{len(names)}] {table}: metadata lookup\n"
        f"Cross-read {table}: engine pair\n"
        f"Cross-read {table}: {status} (1.250s)\n{rendered}\n"
    )
    if phase == "complete":
        assert stream.flushed == visible
        assert stream.getvalue() == visible + rendered + "\n"
        assert formatter.call_args_list == [call([outcome]), call([outcome])]
        assertion.assert_called_once_with([outcome])
    else:
        visible += "Cross-read [2/2] catalog.scratch.testing_z: metadata lookup\n"
        if phase == "pair":
            visible += "Cross-read catalog.scratch.testing_z: engine pair\n"
        assert stream.flushed == visible
        assert stream.getvalue() == visible
        formatter.assert_called_once_with([outcome])
        assertion.assert_not_called()
    assert "private second line" not in stream.getvalue()
    assert "third difference" not in stream.getvalue()

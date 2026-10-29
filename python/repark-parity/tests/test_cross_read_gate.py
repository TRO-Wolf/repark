"""Pin the cross-read gate: only the recorded unsupported-type failure is expected.

pins: cross-read-gate-class-1/C-001, C-002, C-003
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pyarrow as pa
import pytest

_HELPER_PATH = Path(__file__).resolve().parents[2] / "repark/tests/parity/_acceptance_cross_read.py"


def _no_metadata_file(*args: Any) -> None:
    """Stand-in metadata lookup reporting no metadata file."""
    return None


def _load_helper() -> ModuleType:
    """Load the real cross-read helper with the engine-metadata module stubbed."""
    stub = ModuleType("_acceptance_v3")
    stub.current_metadata_location = _no_metadata_file
    sys.modules["_acceptance_v3"] = stub
    spec = importlib.util.spec_from_file_location("_cross_read_gate_helper", _HELPER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_cross_read_gate_helper"] = module
    spec.loader.exec_module(module)
    return module


_HELPER = _load_helper()


class UnsupportedOperationException(Exception):  # noqa: N818 - Spark names the class
    """Stand-in for the Spark error the pins name."""


def _ns_table() -> pa.Table:
    """Two-row table with a nanosecond timestamp column."""
    return pa.table(
        {
            "id": [1, 2],
            "ts": pa.array(
                [datetime(2024, 1, 15, 10, 20, 30), datetime(2023, 6, 30, 12, 34, 56)],
                type=pa.timestamp("ns"),
            ),
        }
    )


def _us_table() -> pa.Table:
    """Two-row table with a microsecond timestamp column."""
    return pa.table(
        {
            "id": [1, 2],
            "ts": pa.array(
                [datetime(2024, 1, 15, 10, 20, 30), datetime(2023, 6, 30, 12, 34, 56)],
                type=pa.timestamp("us"),
            ),
        }
    )


def _changed_table() -> pa.Table:
    """Same schema as the nanosecond table with one row changed."""
    return pa.table(
        {
            "id": [1, 3],
            "ts": pa.array(
                [datetime(2024, 1, 15, 10, 20, 30), datetime(2023, 6, 30, 12, 34, 56)],
                type=pa.timestamp("ns"),
            ),
        }
    )


def _repark_session(table: pa.Table) -> Mock:
    """Stand-in RePark session answering one in-memory Arrow table."""
    return Mock(sql=Mock(return_value=Mock(to_arrow=Mock(return_value=table))))


def _raising_spark(error: BaseException) -> Mock:
    """Stand-in Spark session failing every read with the given error."""
    result = Mock()
    result.toArrow.side_effect = error
    return Mock(sql=Mock(return_value=result))


def _reading_spark(table: pa.Table) -> Mock:
    """Stand-in Spark session answering one in-memory Arrow table."""
    return Mock(sql=Mock(return_value=Mock(toArrow=Mock(return_value=table))))


def test_connection_failure_on_a_nanosecond_table_is_rejected() -> None:
    """The audit probe: a Spark connection failure is not the expected limitation."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(ConnectionError("simulated oracle connection unavailable")),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    with pytest.raises(AssertionError, match="ConnectionError"):
        _HELPER.assert_outcomes_expected([outcome])


def test_timestamp_ns_condition_on_a_nanosecond_table_is_accepted() -> None:
    """The recorded timestamp_ns condition stays the expected limitation."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(
            UnsupportedOperationException("Cannot convert unsupported type to Spark: timestamp_ns")
        ),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    _HELPER.assert_outcomes_expected([outcome])


def test_timestamptz_ns_condition_on_a_nanosecond_table_is_accepted() -> None:
    """The recorded timestamptz_ns condition stays the expected limitation."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(
            UnsupportedOperationException(
                "Cannot convert unsupported type to Spark: timestamptz_ns"
            )
        ),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    _HELPER.assert_outcomes_expected([outcome])


def test_multiline_message_matching_on_its_first_line_is_accepted() -> None:
    """A stack trace after the condition line does not change the verdict."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(
            UnsupportedOperationException(
                "Cannot convert unsupported type to Spark: timestamp_ns\n"
                "at org.apache.spark.sql.execution.SparkRead.run"
            )
        ),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    _HELPER.assert_outcomes_expected([outcome])


def test_condition_only_on_the_second_line_is_rejected() -> None:
    """The gate reads the first line, so a later condition line is rejected."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(
            UnsupportedOperationException(
                "Spark read failed: no executor\n"
                "Cannot convert unsupported type to Spark: timestamp_ns"
            )
        ),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    with pytest.raises(AssertionError, match="Spark read failed"):
        _HELPER.assert_outcomes_expected([outcome])


def test_authorization_failure_on_a_nanosecond_table_is_rejected() -> None:
    """An authorization failure on a nanosecond table is rejected."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(PermissionError("PERMISSION_DENIED: not authorized")),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    with pytest.raises(AssertionError, match="PermissionError"):
        _HELPER.assert_outcomes_expected([outcome])


def test_variant_condition_on_a_nanosecond_table_is_rejected() -> None:
    """The unsupported-type prefix with another type name is rejected."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _raising_spark(
            UnsupportedOperationException("Cannot convert unsupported type to Spark: variant")
        ),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert outcome.has_ns_columns
    with pytest.raises(AssertionError, match="variant"):
        _HELPER.assert_outcomes_expected([outcome])


def test_connection_failure_on_a_microsecond_table_is_rejected() -> None:
    """The non-nanosecond control keeps rejecting a connection failure."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_us_table()),
        "catalog.scratch.probe",
        _raising_spark(ConnectionError("simulated oracle connection unavailable")),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert not outcome.has_ns_columns
    with pytest.raises(AssertionError, match="ConnectionError"):
        _HELPER.assert_outcomes_expected([outcome])


def test_unsupported_condition_text_on_a_microsecond_table_is_rejected() -> None:
    """The condition text without nanosecond columns is rejected."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_us_table()),
        "catalog.scratch.probe",
        _raising_spark(
            UnsupportedOperationException("Cannot convert unsupported type to Spark: timestamp_ns")
        ),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.SPARK_CANNOT_READ
    assert not outcome.has_ns_columns
    with pytest.raises(AssertionError, match="timestamp_ns"):
        _HELPER.assert_outcomes_expected([outcome])


def test_equal_pair_is_accepted() -> None:
    """A row-equal pair stays accepted."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _reading_spark(_ns_table()),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.EQUAL
    _HELPER.assert_outcomes_expected([outcome])


def test_different_pair_is_rejected() -> None:
    """A row mismatch stays rejected."""
    outcome = _HELPER.cross_read_table_pair(
        _repark_session(_ns_table()),
        "catalog.scratch.probe",
        _reading_spark(_changed_table()),
        "catalog.scratch.probe",
        None,
    )
    assert outcome.status == _HELPER.DIFFERENT
    with pytest.raises(AssertionError, match="DIFFERENT"):
        _HELPER.assert_outcomes_expected([outcome])

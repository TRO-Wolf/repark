"""LIVE-v3: the Glue / S3 Tables v3 leg body, pinned against the local catalog.

pins: live-v3-aws-legs/C-002
MUTATION: change any expected count in ``_acceptance_v3`` (data files per partition, DV
counts, rewrite triple, lineage triples, snapshot counts) → this REDs; drop the ``adopt_with``
argument → the adopted-table assertion REDs.
"""

from __future__ import annotations

from pathlib import Path

from _acceptance_v3 import (
    S3T_V3_REFUSED_AT_CREATE,
    S3T_V3_SUPPORTED,
    S3T_V3_UNCLASSIFIED,
    V3_ADOPTED_SUFFIX,
    V3_ALLOW_CREATE_KEY,
    V3_EXPECTED_ADDED_DATA_FILES,
    V3_EXPECTED_DELETE_FILES_AFTER_DELETE,
    V3_EXPECTED_DELETE_FILES_AFTER_MERGE,
    V3_EXPECTED_REMOVED_DELETE_FILES,
    V3_EXPECTED_REWRITTEN_DATA_FILES,
    V3_EXPECTED_SNAPSHOTS_AFTER_EXPIRE,
    V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE,
    V3_FILES_PER_PARTITION,
    assert_v3_acceptance_outcome,
    classify_v3_create_outcome,
    format_v3_refusal_record,
    run_v3_acceptance,
    v3_acceptance_expected_rows,
)

from repark import ReparkSession

_CATALOG = "ice"
_NAMESPACE = "sales"
_TABLE = "v3acc"


def _second_session(spark: ReparkSession, warehouse: Path) -> ReparkSession:
    """A distinct engine handle over the same warehouse — the live legs' second session."""
    session = spark.newSession()
    session.register_memory_catalog(_CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE} LOCATION '{warehouse / _NAMESPACE}'")
    return session


def test_v3_acceptance_leg_body_against_the_local_catalog(tmp_path: Path) -> None:
    """5+5 files, one DV after DELETE, two after MERGE, 12→2 rewrite, 14→1 expire, adopt."""
    spark = (
        ReparkSession.builder.appName("live-v3-local")
        .config(V3_ALLOW_CREATE_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog(_CATALOG, tmp_path)
        spark.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE} LOCATION '{tmp_path / _NAMESPACE}'")
        outcome = run_v3_acceptance(
            spark,
            _CATALOG,
            _NAMESPACE,
            _TABLE,
            adopt_with=lambda: _second_session(spark, tmp_path),
        )
        assert_v3_acceptance_outcome(outcome)
        assert outcome.data_files_per_partition == [
            (0, V3_FILES_PER_PARTITION),
            (1, V3_FILES_PER_PARTITION),
        ]
        assert len(outcome.delete_files_after_delete) == V3_EXPECTED_DELETE_FILES_AFTER_DELETE
        assert len(outcome.delete_files_after_merge) == V3_EXPECTED_DELETE_FILES_AFTER_MERGE
        assert outcome.delete_files_after_rewrite == []
        assert outcome.rewritten_data_files_count == V3_EXPECTED_REWRITTEN_DATA_FILES
        assert outcome.added_data_files_count == V3_EXPECTED_ADDED_DATA_FILES
        assert outcome.removed_delete_files_count == V3_EXPECTED_REMOVED_DELETE_FILES
        assert outcome.snapshots_before_expire == V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE
        assert outcome.snapshots_after_expire == V3_EXPECTED_SNAPSHOTS_AFTER_EXPIRE
        assert outcome.rows_after_rewrite == v3_acceptance_expected_rows()
        assert outcome.adopted_table == f"{_CATALOG}.{_NAMESPACE}.{_TABLE}{V3_ADOPTED_SUFFIX}"
        assert outcome.rows_after_adopt == outcome.rows_after_rewrite
    finally:
        spark.stop()


def test_v3_create_refusal_classification_is_the_s3_tables_decision_table() -> None:
    """S3T-V3-1: a service format-version refusal classifies; the opt-in guard never does."""
    assert classify_v3_create_outcome(None) == S3T_V3_SUPPORTED
    refusal = RuntimeError("BadRequestException: format-version 3 is not supported")
    assert classify_v3_create_outcome(refusal) == S3T_V3_REFUSED_AT_CREATE
    opt_in = RuntimeError(f"{V3_ALLOW_CREATE_KEY} is false; format-version 3 is not supported")
    assert classify_v3_create_outcome(opt_in) == S3T_V3_UNCLASSIFIED
    assert classify_v3_create_outcome(RuntimeError("AccessDenied")) == S3T_V3_UNCLASSIFIED
    assert classify_v3_create_outcome(RuntimeError("format-version 2 is fine")) == (
        S3T_V3_UNCLASSIFIED
    )


def test_v3_refusal_record_masks_the_account_id() -> None:
    """The recorded S3T-V3-1 disposition names the row and never leaks a 12-digit account."""
    record = format_v3_refusal_record(
        RuntimeError("arn:aws:s3tables:us-east-2:123456789012:bucket/b — format-version 3 invalid")
    )
    assert record.startswith(f"S3T-V3-1 {S3T_V3_REFUSED_AT_CREATE}")
    assert "<ACCOUNT>" in record
    assert "123456789012" not in record

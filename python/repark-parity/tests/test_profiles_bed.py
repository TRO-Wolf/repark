"""Profiles bed pins. pins: profiles-1/C-001, C-002, C-003, C-004, C-005"""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))

from profiles import datasets as bed_datasets
from profiles import harness as bed_harness
from profiles import queries as bed_queries
from profiles import run_profiles as bed_runner


def test_bed_builds_three_datasets() -> None:
    """Pin C-001: futures, tpch SF10 via dbgen, iceberg with 200 files."""
    assert bed_datasets.DATASETS == ("futures", "tpch", "iceberg")
    assert bed_datasets.FUTURES_NAME == "test_futures.parquet"
    assert bed_datasets.FULL_TPCH_SF == 10
    assert bed_datasets.FULL_ICEBERG_FILES == 200
    assert callable(bed_datasets.ensure_parquet_sf)


def test_bed_runs_five_reads_and_three_writes() -> None:
    """Pin C-002: five read shapes and three write shapes by name."""
    assert tuple(sorted(bed_queries.READ_QUERIES)) == (
        "group_by",
        "hash_join",
        "scan_filter",
        "sort_merge_join",
        "window",
    )
    assert tuple(sorted(bed_queries.WRITE_QUERIES)) == (
        "append_files",
        "merge_updates",
        "overwrite_partition",
    )
    assert bed_queries.APPEND_FILE_COUNT == 8
    assert bed_queries.MERGE_UPDATE_FRACTION == 0.10


def test_harness_writes_one_csv_row_per_cell() -> None:
    """Pin C-003: one row per repetition plus the median of three."""
    assert bed_harness.csv_header() == (
        "dataset",
        "query",
        "knob",
        "value",
        "repetition",
        "seconds",
    )
    assert bed_harness.median_seconds([3.0, 1.0, 2.0]) == 2.0
    assert bed_harness.median_seconds([1.0, 2.0, 3.0, 4.0]) == 2.5


def test_harness_writes_rows_for_a_matrix(tmp_path: Path) -> None:
    """Pin C-003: a two-query one-value matrix lands two row sets."""
    out = tmp_path / "cells.csv"
    bed_harness.write_cell_rows(out, "tpch", "scan_filter", "some.knob", "v", [0.5, 0.4, 0.6])
    with out.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [(row["dataset"], row["query"], row["knob"], row["value"]) for row in rows] == [
        ("tpch", "scan_filter", "some.knob", "v")
    ] * 3
    assert [row["repetition"] for row in rows] == ["1", "2", "3"]


def test_harness_refuses_when_a_jvm_is_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin C-004: a live JVM refuses the timed run naming pgrep."""
    found = subprocess.CompletedProcess(["pgrep", "-f", "java"], 0, stdout="1234\n", stderr="")
    monkeypatch.setattr(bed_harness.subprocess, "run", lambda *a, **k: found)
    with pytest.raises(bed_harness.BoxBusyError, match="pgrep -f java"):
        bed_harness.require_quiet_box()
    quiet = subprocess.CompletedProcess(["pgrep", "-f", "java"], 1, stdout="", stderr="")
    monkeypatch.setattr(bed_harness.subprocess, "run", lambda *a, **k: quiet)
    assert bed_harness.require_quiet_box() is None


def test_runner_lands_write_properties_on_the_bed_table() -> None:
    """Pin: write.* knobs reach the rebuilt bed table as TBLPROPERTIES."""
    table = "cat.ns.bed"
    assert bed_runner.table_property_alter("write.distribution-mode", "none", table) == (
        "ALTER TABLE cat.ns.bed SET TBLPROPERTIES ('write.distribution-mode'='none')"
    )
    assert bed_runner.table_property_alter("write.target-file-size-bytes", "1024", table) == (
        "ALTER TABLE cat.ns.bed SET TBLPROPERTIES ('write.target-file-size-bytes'='1024')"
    )
    assert bed_runner.table_property_alter("write.distribution-mode", "@default", table) is None
    assert bed_runner.table_property_alter("datafusion.execution.batch_size", "x", table) is None


def test_smoke_completes_at_tiny_scale() -> None:
    """Pin C-005: smoke runs once per cell below full scale."""
    config = bed_harness.smoke_config()
    assert config.repeats == 1
    assert config.tpch_sf < bed_datasets.FULL_TPCH_SF
    assert config.iceberg_files < bed_datasets.FULL_ICEBERG_FILES
    assert config.futures_rows < 1_000_000

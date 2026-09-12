"""Profiles bed pins. pins: profiles-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008"""

from __future__ import annotations

import csv
import re
import subprocess
import sys
from collections import defaultdict
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


DOC_PATH = Path(__file__).resolve().parents[3] / "docs" / "perf" / "config-profiles-2026-09-12.md"
CSV_DIR = DOC_PATH.parent / "config-profiles-2026-09-12"

DOC_ROW = re.compile(
    r"^\| ([^|]+?) \| ([a-z]+) \| ([a-z_]+) \| ([0-9.]+) \| ([0-9.]+) \|$",
    re.MULTILINE,
)
ARGMAX_ROW = re.compile(
    r"^\*\*argmax:\*\* `?([^`\s]+)`? on ([a-z]+)/([a-z_]+) \(ratio ([0-9.]+)\)$",
    re.MULTILINE,
)
NO_EFFECT_ROW = re.compile(r"^\| `([^`]+)` \| ([0-9.]+) \|$", re.MULTILINE)
BASELINE_ROW = re.compile(r"^\| ([a-z]+) \| ([a-z_]+) \| ([0-9.]+) \|$", re.MULTILINE)

READ_CELLS = {
    ("futures", "scan_filter"),
    ("tpch", "scan_filter"),
    ("futures", "group_by"),
    ("tpch", "group_by"),
    ("tpch", "hash_join"),
    ("tpch", "sort_merge_join"),
    ("futures", "window"),
}
WRITE_CELLS = {
    ("iceberg", "append_files"),
    ("iceberg", "overwrite_partition"),
    ("iceberg", "merge_updates"),
}
MERGE_CELL = ("iceberg", "merge_updates")

AFFECTED_CELLS = {
    "datafusion.optimizer.prefer_hash_join": {
        ("tpch", "hash_join"),
        ("tpch", "sort_merge_join"),
        MERGE_CELL,
    },
    "datafusion.execution.target_partitions": READ_CELLS | WRITE_CELLS,
    "datafusion.execution.batch_size": READ_CELLS | WRITE_CELLS,
    "datafusion.optimizer.repartition_joins": {
        ("tpch", "hash_join"),
        ("tpch", "sort_merge_join"),
        MERGE_CELL,
    },
    "datafusion.optimizer.repartition_aggregations": {
        ("futures", "group_by"),
        ("tpch", "group_by"),
        ("tpch", "hash_join"),
    },
    "datafusion.optimizer.repartition_file_scans": READ_CELLS | {MERGE_CELL},
    "datafusion.execution.parquet.pushdown_filters": READ_CELLS | {MERGE_CELL},
    "datafusion.execution.parquet.enable_page_index": READ_CELLS | {MERGE_CELL},
    "datafusion.execution.parquet.bloom_filter_on_read": READ_CELLS | {MERGE_CELL},
    "repark.scan.concurrency_limit": {MERGE_CELL},
    "repark.batch.size": READ_CELLS | WRITE_CELLS,
    "datafusion.execution.parquet.compression": WRITE_CELLS,
    "datafusion.execution.parquet.max_row_group_size": WRITE_CELLS,
    "datafusion.execution.parquet.bloom_filter_on_write": WRITE_CELLS,
    "datafusion.execution.parquet.write_batch_size": WRITE_CELLS,
    "write.target-file-size-bytes": WRITE_CELLS,
    "write.distribution-mode": WRITE_CELLS,
    "repark.merge.file_scoped_rewrite": {MERGE_CELL},
    "repark.merge.scan_pruning": {MERGE_CELL},
}

CONTROL_VALUES = {
    "datafusion.optimizer.prefer_hash_join": {"true"},
    "datafusion.execution.target_partitions": set(),
    "datafusion.execution.batch_size": set(),
    "datafusion.optimizer.repartition_joins": {"true"},
    "datafusion.optimizer.repartition_aggregations": {"true"},
    "datafusion.optimizer.repartition_file_scans": {"true"},
    "datafusion.execution.parquet.pushdown_filters": {"false"},
    "datafusion.execution.parquet.enable_page_index": {"true"},
    "datafusion.execution.parquet.bloom_filter_on_read": {"true"},
    "repark.scan.concurrency_limit": set(),
    "repark.batch.size": set(),
    "datafusion.execution.parquet.compression": {"zstd(3)"},
    "datafusion.execution.parquet.max_row_group_size": set(),
    "datafusion.execution.parquet.bloom_filter_on_write": {"false"},
    "datafusion.execution.parquet.write_batch_size": set(),
    "write.target-file-size-bytes": set(),
    "write.distribution-mode": {"hash", "range"},
    "repark.merge.file_scoped_rewrite": {"true"},
    "repark.merge.scan_pruning": {"true"},
}


def load_cells(path: Path) -> dict[tuple[str, str, str, str], list[float]]:
    cells: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            key = (row["dataset"], row["query"], row["knob"], row["value"])
            cells[key].append(float(row["seconds"]))
    return cells


def cell_medians(path: Path) -> dict[tuple[str, str, str], float]:
    return {
        (dataset, query, value): bed_harness.median_seconds(seconds)
        for (dataset, query, _knob, value), seconds in load_cells(path).items()
    }


def knob_csvs() -> dict[str, Path]:
    return {
        path.name[: -len(".csv")]: path
        for path in sorted(CSV_DIR.glob("*.csv"))
        if path.name != "baseline.csv"
    }


def doc_sections() -> dict[str, str]:
    text = DOC_PATH.read_text()
    parts = re.split(r"^## ", text, flags=re.MULTILINE)
    return {part.splitlines()[0].strip(): part for part in parts[1:]}


def test_sweep_csvs_carry_three_reps_per_cell() -> None:
    """Pin C-006/C-007: every CSV cell is three repetitions; baseline is full scale."""
    baseline = load_cells(CSV_DIR / "baseline.csv")
    assert baseline, "baseline.csv is missing or empty"
    for seconds in baseline.values():
        assert len(seconds) == 3
    assert {dataset for dataset, _, _, _ in baseline} == {"futures", "tpch", "iceberg"}
    for knob, path in knob_csvs().items():
        cells = load_cells(path)
        values = {value for _, _, _, value in cells}
        assert "@default" in values, f"{knob}: no @default cell"
        assert len(values) >= 3, f"{knob}: fewer than three swept values"
        for key, seconds in cells.items():
            assert len(seconds) == 3, f"{knob} {key}: {len(seconds)} repetitions"


def test_doc_tables_equal_csv_medians() -> None:
    """Pin C-008: every doc table cell equals the committed CSV median."""
    sections = doc_sections()
    baseline_section = sections.get("Baseline")
    assert baseline_section is not None
    baseline_medians = cell_medians(CSV_DIR / "baseline.csv")
    for dataset, query, median in BASELINE_ROW.findall(baseline_section):
        key = (dataset, query, "@default")
        assert f"{baseline_medians[key]:.3f}" == median, key
    for knob, path in knob_csvs().items():
        heading = f"`{knob}`"
        section = next(
            body for title, body in sections.items() if title.split(" ", 1)[0] == heading
        )
        medians = cell_medians(path)
        rows = DOC_ROW.findall(section)
        assert len(rows) == len(medians), f"{knob}: doc rows {len(rows)} != cells {len(medians)}"
        seen: dict[tuple[str, str, str], float] = {}
        for value, dataset, query, median, ratio in rows:
            key = (dataset, query, value)
            assert key in medians, f"{knob}: doc row {key} not in CSV"
            assert f"{medians[key]:.3f}" == median, f"{knob} {key}: {median}"
            expected = medians[key] / medians[(dataset, query, "@default")]
            assert f"{expected:.4f}" == ratio, f"{knob} {key}: ratio {ratio}"
            if value != "@default":
                seen[key] = expected
        argmax = ARGMAX_ROW.search(section)
        assert argmax is not None, f"{knob}: no argmax row"
        (best_dataset, best_query, best_value), best_ratio = min(
            seen.items(), key=lambda item: item[1]
        )
        assert argmax.group(1) == best_value, f"{knob}: argmax {argmax.group(1)}"
        assert argmax.group(2) == best_dataset and argmax.group(3) == best_query
        assert argmax.group(4) == f"{best_ratio:.4f}"
    no_effect = sections.get("No effect measured")
    assert no_effect is not None
    listed = dict(NO_EFFECT_ROW.findall(no_effect))
    computed: dict[str, float] = {}
    for knob, path in knob_csvs().items():
        medians = cell_medians(path)
        affected = AFFECTED_CELLS[knob]
        controls = CONTROL_VALUES[knob] | {"@default"}
        worst = max(
            abs(medians[(dataset, query, value)] / medians[(dataset, query, "@default")] - 1.0)
            for dataset, query, value in medians
            if value not in controls and (dataset, query) in affected
        )
        if worst < 0.05:
            computed[knob] = worst
    assert set(listed) == set(computed), (
        f"no-effect mismatch: doc {sorted(listed)} vs csv {sorted(computed)}"
    )
    for knob, deviation in listed.items():
        assert f"{computed[knob]:.4f}" == deviation, knob

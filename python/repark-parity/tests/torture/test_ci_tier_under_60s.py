"""The CI-tier workload pin: both families generate and read on both doors under 60 seconds."""

from __future__ import annotations

import time
from pathlib import Path

from _support import (
    assert_loud_rows,
    family_output,
    read_frame_door,
    read_frame_door_loud,
    read_sql_door,
)

from repark import ReparkSession
from repark_parity.torture import FAMILIES, FamilyOutput
from repark_parity.torture.tiers import CI_ROWS

BUDGET_SECONDS = 60.0


def test_ci_tier_workload_under_60_seconds(spark: ReparkSession, tmp_path: Path) -> None:
    """Both families at CI rows generate and read on both doors inside the 60-second budget."""
    outputs: dict[str, FamilyOutput] = {}
    started = time.perf_counter()
    for name in ("nested", "inference"):
        outputs[name] = family_output(FAMILIES[name], rows=CI_ROWS, seed=7, out=tmp_path / name)
    nested_table = read_frame_door(spark, outputs["nested"].parquet_path, "parquet")
    assert nested_table.num_rows == CI_ROWS
    read_sql_door(spark, outputs["nested"].parquet_path, "parquet", "torture_timer_nested")
    nested_csv = read_frame_door_loud(spark, outputs["nested"].csv_path, "csv")
    assert_loud_rows(nested_csv, CI_ROWS, "dataframe")
    inference_table = read_frame_door(spark, outputs["inference"].csv_path, "csv")
    assert inference_table.num_rows == CI_ROWS
    read_sql_door(spark, outputs["inference"].csv_path, "csv", "torture_timer_inference")
    elapsed = time.perf_counter() - started
    assert elapsed < BUDGET_SECONDS, elapsed


def test_ci_tier_step2_families_under_60_seconds(spark: ReparkSession, tmp_path: Path) -> None:
    """The four step-2 families at CI rows generate and read on both doors in the budget."""
    outputs: dict[str, FamilyOutput] = {}
    started = time.perf_counter()
    for name in ("extreme_types", "smartcsv", "temporal", "decimal_overflow"):
        outputs[name] = family_output(FAMILIES[name], rows=CI_ROWS, seed=7, out=tmp_path / name)
    for name, output in outputs.items():
        table = read_frame_door(spark, output.parquet_path, "parquet")
        assert table.num_rows == CI_ROWS, name
        read_sql_door(spark, output.parquet_path, "parquet", f"torture_timer_{name}_parquet")
        csv_outcome = read_frame_door_loud(spark, output.csv_path, "csv")
        assert_loud_rows(csv_outcome, CI_ROWS, "dataframe")
    elapsed = time.perf_counter() - started
    assert elapsed < BUDGET_SECONDS, elapsed

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _acceptance_cross_release import (
    INDEX_WHEEL,
    KINDS,
    LOCAL_BUILD,
    PREVIOUS_PYTHON_ENV,
    PREVIOUS_RELEASE,
    REFUSED_EXIT,
    Side,
    compare_tables,
    cross_cell,
    format_cells,
    mixed_cell,
    run_side,
)

import repark

PREVIOUS_PYTHON = os.environ.get(PREVIOUS_PYTHON_ENV)
needs_previous = pytest.mark.skipif(
    not PREVIOUS_PYTHON,
    reason=(
        f"X-L is a local control: set {PREVIOUS_PYTHON_ENV} to a hash-checked "
        f"repark=={PREVIOUS_RELEASE} interpreter; the live leg is "
        "test_aws_acceptance_cross_release.py"
    ),
)
HEAD = Side("head", sys.executable, repark.__version__, LOCAL_BUILD)
FORMATS = (2, 3)
DIRECTIONS = ("previous_writes", "head_writes")


def previous() -> Side:
    assert PREVIOUS_PYTHON is not None
    return Side("previous", PREVIOUS_PYTHON, PREVIOUS_RELEASE, INDEX_WHEEL)


def local_spec(warehouse: Path, kind: str, format_version: int, table: str) -> dict[str, Any]:
    prefix = "spark.sql.catalog.loc"
    return {
        "catalog": "loc",
        "namespace": "ns",
        "table": table,
        "kind": kind,
        "format": format_version,
        "catalog_config": {
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.type": "hadoop",
            f"{prefix}.warehouse": str(warehouse),
        },
        "adopt_dir": str(warehouse / "ns" / table),
    }


def ordered(direction: str) -> tuple[Side, Side]:
    return (previous(), HEAD) if direction == "previous_writes" else (HEAD, previous())


@needs_previous
@pytest.mark.parametrize("direction", DIRECTIONS)
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("format_version", FORMATS)
def test_x_l_each_release_reads_the_other_releases_table(
    format_version: int, kind: str, direction: str, tmp_path: Path
) -> None:
    writer, reader = ordered(direction)
    spec = local_spec(tmp_path / "warehouse", kind, format_version, f"xrel_{kind}")
    label = f"X-L-{kind} v{format_version} {direction}"
    cell = cross_cell(writer=writer, reader=reader, spec=spec, workdir=tmp_path, label=label)
    print(format_cells([cell]))
    assert not cell.differences, f"H-X1 {label}: {cell.differences}"


@needs_previous
@pytest.mark.parametrize("direction", DIRECTIONS)
@pytest.mark.parametrize("format_version", FORMATS)
def test_x_l_the_other_release_appends_and_the_writer_reads_it_back(
    format_version: int, direction: str, tmp_path: Path
) -> None:
    writer, reader = ordered(direction)
    spec = local_spec(tmp_path / "warehouse", "plain", format_version, "xrel_mixed")
    label = f"X-L-mixed v{format_version} {direction}"
    cell = mixed_cell(writer=writer, reader=reader, spec=spec, workdir=tmp_path, label=label)
    print(format_cells([cell]))
    assert not cell.differences, f"H-X1 {label}: {cell.differences}"


@needs_previous
@pytest.mark.parametrize("direction", DIRECTIONS)
def test_x_ns_is_printed_not_asserted(direction: str, tmp_path: Path) -> None:
    writer, reader = ordered(direction)
    spec = local_spec(tmp_path / "warehouse", "ns", 3, "xrel_ns")
    written = run_side(writer, spec, "create", tmp_path, "ns-writer")
    read = run_side(reader, spec, "read", tmp_path, "ns-reader")
    print(f"X-NS {direction}: writer exit {written.returncode} reader exit {read.returncode}")
    for run in (written, read):
        if run.table is not None:
            print(f"  {run.label}: {run.table.schema} {run.table.to_pylist()}")
        else:
            print(f"  {run.label}: {run.stderr.strip().splitlines()[-1:]}")


def test_x_c0_the_runner_adds_nothing_head_writes_and_head_reads(tmp_path: Path) -> None:
    spec = local_spec(tmp_path / "warehouse", "plain", 2, "xrel_c0")
    cell = cross_cell(writer=HEAD, reader=HEAD, spec=spec, workdir=tmp_path, label="X-C0")
    print(format_cells([cell]))
    assert not cell.differences
    assert cell.reader.report["rows"] == 1000


def test_x_c1_the_runner_refuses_a_release_its_role_does_not_name(tmp_path: Path) -> None:
    impostor = Side("previous", sys.executable, PREVIOUS_RELEASE, INDEX_WHEEL)
    spec = local_spec(tmp_path / "warehouse", "plain", 2, "xrel_c1")
    run = run_side(impostor, spec, "create", tmp_path, "X-C1")
    assert run.returncode == REFUSED_EXIT
    assert run.report == {
        "refused": True,
        "role": "previous",
        "version": repark.__version__,
        "build": LOCAL_BUILD,
        "expected": {"version": PREVIOUS_RELEASE, "build": INDEX_WHEEL},
    }
    assert run.table is None
    assert not (tmp_path / "warehouse").exists()


def test_x_c1_a_cell_whose_sides_run_one_release_fails(tmp_path: Path) -> None:
    impostor = Side("previous", sys.executable, repark.__version__, LOCAL_BUILD)
    spec = local_spec(tmp_path / "warehouse", "plain", 2, "xrel_c1_same")
    with pytest.raises(AssertionError, match="both sides would run"):
        cross_cell(writer=impostor, reader=HEAD, spec=spec, workdir=tmp_path, label="X-C1b")


def test_x_compare_sees_an_ntz_column_read_as_an_instant() -> None:
    values = [0, 3_600_000_000]
    writer = pa.table({"ntz": pa.array(values, pa.timestamp("us"))})
    reader = pa.table({"ntz": pa.array(values, pa.timestamp("us", tz="UTC"))})
    differences = compare_tables(writer, reader)
    assert any("ntz" in difference for difference in differences)


def test_x_compare_sees_a_decimal_scale() -> None:
    writer = pa.table({"amount": pa.array([1], pa.decimal128(10, 2))})
    reader = pa.table({"amount": pa.array([1], pa.decimal128(10, 3))})
    assert compare_tables(writer, reader)


def test_x_compare_sees_a_string_read_as_a_large_string() -> None:
    writer = pa.table({"name": pa.array(["a", "b"], pa.string())})
    reader = pa.table({"name": pa.array(["a", "b"], pa.large_string())})
    differences = compare_tables(writer, reader)
    assert differences == ["column name: writer=string reader=large_string"]

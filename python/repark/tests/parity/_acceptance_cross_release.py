from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
from _acceptance_cross_read import columns_of, compare_row_sets, compare_schemas

RUNNER = Path(__file__).with_name("_acceptance_cross_release_runner.py")
PREVIOUS_RELEASE = "1.5.5"
PREVIOUS_PYTHON_ENV = "REPARK_PREVIOUS_RELEASE_PYTHON"
REFUSED_EXIT = 3
KINDS = ("plain", "partitioned", "deletes", "evolved")
KINDS_WITH_DELETES = frozenset({"deletes"})
RUNNER_TIMEOUT_S = 900


INDEX_WHEEL = "index-wheel"
LOCAL_BUILD = "local-build"


@dataclass(frozen=True)
class Side:
    role: str
    python: str
    version: str
    build: str

    def identity(self) -> tuple[str, str]:
        return self.version, self.build


@dataclass
class Run:
    label: str
    returncode: int
    report: dict[str, Any]
    table: pa.Table | None
    stderr: str = ""


@dataclass
class Cell:
    label: str
    writer: Run
    reader: Run
    differences: list[str] = field(default_factory=list)


def exact_type_differences(writer: pa.Schema, reader: pa.Schema) -> list[str]:
    left = [(item.name, item.type) for item in writer]
    right = [(item.name, item.type) for item in reader]
    if left == right:
        return []
    return [
        f"column {name}: writer={left_type} reader={right_type}"
        for (name, left_type), (_, right_type) in zip(left, right, strict=False)
        if left_type != right_type
    ] or [f"columns differ: writer={left} reader={right}"]


def compare_tables(writer: pa.Table, reader: pa.Table) -> list[str]:
    return (
        compare_schemas(columns_of(writer.schema), columns_of(reader.schema))
        + exact_type_differences(writer.schema, reader.schema)
        + compare_row_sets(writer, reader)
    )


def run_side(side: Side, spec: dict[str, Any], op: str, workdir: Path, label: str) -> Run:
    full = {
        **spec,
        "role": side.role,
        "expect_version": side.version,
        "expect_build": side.build,
        "op": op,
        "out": str(workdir / f"{label}.arrow"),
    }
    spec_path = workdir / f"{label}.json"
    spec_path.write_text(json.dumps(full), encoding="utf-8")
    completed = subprocess.run(
        [side.python, "-I", str(RUNNER), str(spec_path)],
        capture_output=True,
        text=True,
        timeout=RUNNER_TIMEOUT_S,
        check=False,
    )
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    report = json.loads(lines[-1]) if lines else {}
    table = None
    if completed.returncode == 0:
        with pa.OSFile(full["out"], "rb") as source:
            table = ipc.open_file(source).read_all()
    return Run(label, completed.returncode, report, table, completed.stderr[-4000:])


def require_ran(run: Run, side: Side) -> None:
    assert run.returncode == 0, f"{run.label}: the {side.role} runner failed\n{run.stderr}"
    printed = (run.report.get("version"), run.report.get("build"))
    assert printed == side.identity(), (
        f"{run.label}: the {side.role} side printed {printed!r}, not {side.identity()!r}"
    )
    assert run.table is not None


def cross_cell(
    *, writer: Side, reader: Side, spec: dict[str, Any], workdir: Path, label: str
) -> Cell:
    assert writer.identity() != reader.identity() or writer.role == reader.role, (
        f"{label}: both sides would run {writer.identity()}"
    )
    written = run_side(writer, spec, "create", workdir, f"{label}-writer")
    require_ran(written, writer)
    read = run_side(reader, spec, "read", workdir, f"{label}-reader")
    require_ran(read, reader)
    assert written.table is not None
    assert read.table is not None
    cell = Cell(label, written, read, compare_tables(written.table, read.table))
    cell.differences += summary_differences(spec["kind"], written, read)
    return cell


def summary_differences(kind: str, written: Run, read: Run) -> list[str]:
    differences: list[str] = []
    for key in ("snapshot_id", "total_records", "delete_files"):
        if written.report[key] != read.report[key]:
            differences.append(f"{key}: writer={written.report[key]!r} reader={read.report[key]!r}")
    if kind not in KINDS_WITH_DELETES:
        for run in (written, read):
            if run.report["total_records"] != run.report["rows"]:
                differences.append(
                    f"{run.label}: total-records {run.report['total_records']} "
                    f"is not the {run.report['rows']} rows read"
                )
    elif not written.report["delete_files"]:
        differences.append(f"{written.label}: the deletes kind wrote no delete file")
    return differences


def mixed_cell(
    *, writer: Side, reader: Side, spec: dict[str, Any], workdir: Path, label: str
) -> Cell:
    created = run_side(writer, spec, "create", workdir, f"{label}-writer")
    require_ran(created, writer)
    appended = run_side(reader, spec, "append", workdir, f"{label}-appender")
    require_ran(appended, reader)
    reread = run_side(writer, spec, "read", workdir, f"{label}-rereader")
    require_ran(reread, writer)
    assert appended.table is not None
    assert reread.table is not None
    assert created.table is not None
    cell = Cell(label, appended, reread, compare_tables(appended.table, reread.table))
    if appended.table.num_rows != created.table.num_rows + 3:
        cell.differences.append(
            f"{label}: the appender read {appended.table.num_rows} rows, "
            f"not {created.table.num_rows} + 3"
        )
    cell.differences += summary_differences(spec["kind"], appended, reread)
    return cell


def format_cells(cells: list[Cell]) -> str:
    lines = ["cell | writer | reader | rows | outcome"]
    for cell in cells:
        rows = cell.reader.report.get("rows")
        outcome = (
            "EQUAL" if not cell.differences else "DIFFERENT: " + "; ".join(cell.differences[:3])
        )
        writer = f"{cell.writer.report.get('version')} {cell.writer.report.get('build')}"
        reader = f"{cell.reader.report.get('version')} {cell.reader.report.get('build')}"
        lines.append(f"{cell.label} | {writer} | {reader} | {rows} | {outcome}")
    return "\n".join(lines)

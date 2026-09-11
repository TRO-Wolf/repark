"""Shared helpers for the torture suite: tier-aware generation and both-door reads."""

from __future__ import annotations

import json
import re
import shutil
import time
from contextlib import suppress
from pathlib import Path

import pyarrow as pa

from repark import ReparkSession
from repark_parity.torture import Family, FamilyOutput
from repark_parity.torture.family import CSV_NAME, PARQUET_NAME

LOUD_READ = pa.Table | Exception

MANIFEST_NAME = "manifest.json"


def family_output(family: Family, rows: int, seed: int, out: Path) -> FamilyOutput:
    """Generate the family under out, or reuse a manifest-matching earlier run."""
    manifest_path = out / MANIFEST_NAME
    parquet_path = out / PARQUET_NAME
    csv_path = out / CSV_NAME
    if manifest_path.is_file() and parquet_path.is_file() and csv_path.is_file():
        recorded = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = {"family": family.name, "rows": rows, "seed": seed}
        if recorded == expected:
            return FamilyOutput(rows=rows, parquet_path=parquet_path, csv_path=csv_path)
    output = family.generate(rows=rows, seed=seed, out=out)
    manifest_path.write_text(
        json.dumps({"family": family.name, "rows": rows, "seed": seed}), encoding="utf-8"
    )
    return output


def _load_frame(session: ReparkSession, path: Path, fmt: str) -> object:
    """Open one generated file through the DataFrame door."""
    if fmt == "parquet":
        return session.read.parquet(str(path))
    if fmt == "csv":
        return session.read.csv(str(path), header=True, inferSchema=True)
    raise ValueError(f"unsupported torture format: {fmt}")


def read_frame_door(session: ReparkSession, path: Path, fmt: str) -> pa.Table:
    """Read one generated file through the DataFrame door."""
    frame = _load_frame(session, path, fmt)
    return frame.to_arrow()  # type: ignore[attr-defined]


def read_sql_door(session: ReparkSession, path: Path, fmt: str, view: str) -> pa.Table:
    """Read one generated file through the spark.sql door over a temp view."""
    frame = _load_frame(session, path, fmt)
    frame.createOrReplaceTempView(view)  # type: ignore[attr-defined]
    return session.sql(f"SELECT * FROM {view}").to_arrow()


def read_frame_door_loud(session: ReparkSession, path: Path, fmt: str) -> LOUD_READ:
    """Run the DataFrame door, returning its table or the loud refusal it raised."""
    try:
        return read_frame_door(session, path, fmt)
    except Exception as exc:
        return exc


def read_sql_door_loud(session: ReparkSession, path: Path, fmt: str, view: str) -> LOUD_READ:
    """Run the spark.sql door, returning its table or the loud refusal it raised."""
    try:
        return read_sql_door(session, path, fmt, view)
    except Exception as exc:
        return exc


def assert_loud_rows(outcome: LOUD_READ, rows: int, door: str) -> None:
    """Assert one loud outcome: the door read the row count or refused with a message."""
    if isinstance(outcome, Exception):
        assert str(outcome) != "", door
        return
    assert outcome.num_rows == rows, door


class _DirLock:
    """Cross-process lock guarding a shared absolute fixture-materialization path."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(f"fixture lock {path} held for 2 minutes") from None
                time.sleep(0.025)

    def close(self) -> None:
        """Release the lock directory."""
        with suppress(OSError):
            self.path.rmdir()


def _metadata_version(path: Path) -> int:
    """Order metadata files by leading version digits (vN.metadata.json or NNNNN-uuid)."""
    match = re.match(r"v?(\d+)", path.name)
    if match is None:
        raise ValueError(f"unrecognized metadata file name: {path.name}")
    return int(match.group(1))


def materialize_table(src: Path, dest: Path) -> str:
    """Copy a Hadoop-layout table tree onto dest under a lock; return the newest metadata file."""
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
        versions = sorted((dest / "metadata").glob("*.metadata.json"), key=_metadata_version)
        if not versions:
            raise ValueError(f"no metadata files under {dest}/metadata")
        return str(versions[-1])
    finally:
        lock.close()

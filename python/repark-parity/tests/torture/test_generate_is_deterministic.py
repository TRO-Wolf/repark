"""Byte-identity determinism pins for the committed generate CLI."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from _support import family_output

from repark_parity.torture import FAMILIES, FamilyOutput
from repark_parity.torture.family import CSV_NAME, PARQUET_NAME

_SRC_DIR = Path(__file__).resolve().parents[2] / "src"


def _run_generate(
    family: str, rows: int, seed: int, out: Path, extra: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the committed CLI once with the checkout package on PYTHONPATH."""
    command = [
        sys.executable,
        "-m",
        "repark_parity.torture",
        "generate",
        family,
        "--rows",
        str(rows),
        "--seed",
        str(seed),
        "--out",
        str(out),
    ]
    if extra is not None:
        command.extend(extra)
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(_SRC_DIR)},
    )


def test_cli_same_seed_is_byte_identical(tmp_path: Path) -> None:
    """Two same-seed CLI runs per family write byte-identical Parquet and CSV."""
    for family in sorted(FAMILIES):
        first = tmp_path / f"{family}-a"
        second = tmp_path / f"{family}-b"
        first_run = _run_generate(family, 64, 7, first)
        second_run = _run_generate(family, 64, 7, second)
        assert first_run.returncode == 0, first_run.stderr
        assert second_run.returncode == 0, second_run.stderr
        for name in (PARQUET_NAME, CSV_NAME):
            assert (first / name).read_bytes() == (second / name).read_bytes(), f"{family}/{name}"


def test_cli_different_seed_moves_bytes(tmp_path: Path) -> None:
    """A different seed moves the written bytes for every family."""
    for family in sorted(FAMILIES):
        first = tmp_path / f"{family}-seed7"
        second = tmp_path / f"{family}-seed8"
        assert _run_generate(family, 64, 7, first).returncode == 0
        assert _run_generate(family, 64, 8, second).returncode == 0
        assert (first / PARQUET_NAME).read_bytes() != (second / PARQUET_NAME).read_bytes()
        assert (first / CSV_NAME).read_bytes() != (second / CSV_NAME).read_bytes()


def test_cli_refuses_unknown_family_and_bad_rows(tmp_path: Path) -> None:
    """Unknown families refuse at the parser and nonpositive rows refuse loudly."""
    unknown = _run_generate("not-a-family", 8, 7, tmp_path / "unknown")
    assert unknown.returncode != 0
    assert "invalid choice" in unknown.stderr
    bad_rows = _run_generate("nested", 0, 7, tmp_path / "rows")
    assert bad_rows.returncode != 0
    assert "rows" in bad_rows.stderr


class _StubFamily:
    """A family double that counts generate calls and writes marker files."""

    name = "stub"

    def __init__(self) -> None:
        self.calls = 0

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the marker files and count one generate call."""
        self.calls += 1
        (out / PARQUET_NAME).write_bytes(b"parquet")
        (out / CSV_NAME).write_bytes(b"csv")
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)


def test_full_tier_reuses_a_matching_manifest(tmp_path: Path) -> None:
    """A manifest-matching directory is reused; a rows mismatch regenerates."""
    family = _StubFamily()
    out = tmp_path / "stub"
    out.mkdir()
    first = family_output(family, rows=10, seed=7, out=out)
    assert first.rows == 10
    assert family.calls == 1
    second = family_output(family, rows=10, seed=7, out=out)
    assert second.rows == 10
    assert family.calls == 1
    third = family_output(family, rows=11, seed=7, out=out)
    assert third.rows == 11
    assert family.calls == 2


def test_generators_refuse_repository_output(tmp_path: Path) -> None:
    """Every registered family refuses an output directory inside the repository."""
    repo_root = Path(__file__).resolve().parents[4]
    assert (repo_root / "AGENTS.md").is_file()
    for family in sorted(FAMILIES):
        with pytest.raises(ValueError, match="outside the repository"):
            FAMILIES[family].generate(rows=8, seed=7, out=repo_root / "target" / "torture-refuse")

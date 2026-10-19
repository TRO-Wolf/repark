"""Unit pins for the write-bench harness (no SF1 wall; pure helpers + tiny smoke).

Never touches AWS. The full SF1 matrix is measurement-night only
(``bench/write/run_write_bench.py --sf 1``).
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

# bench/write is not an installed package — load as a named package for tests.
_WRITE_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench" / "write"
_PACKAGE = "repark_write_bench"


def _load_write_package() -> Any:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    return importlib.import_module(f"{_PACKAGE}.runner")


def _load_datagen() -> Any:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    return importlib.import_module(f"{_PACKAGE}.datagen")


def test_parse_file_size_mib_and_raw() -> None:
    runner = _load_write_package()
    assert runner.parse_file_size("64MiB") == 64 * 1024 * 1024
    assert runner.parse_file_size("256MiB") == 256 * 1024 * 1024
    assert runner.parse_file_size("512MiB") == 512 * 1024 * 1024
    assert runner.parse_file_size("134217728") == 134_217_728
    assert runner.parse_file_size("1G") == 1024 * 1024 * 1024
    # Decimal MB (1000-based) vs binary MiB — harness documents both.
    assert runner.parse_file_size("64MB") == 64 * 1000 * 1000
    assert runner.parse_file_size("1GiB") == 1024 * 1024 * 1024


def test_parse_file_size_rejects_empty() -> None:
    runner = _load_write_package()
    with pytest.raises(ValueError, match="empty"):
        runner.parse_file_size("  ")


def test_format_bytes() -> None:
    runner = _load_write_package()
    assert "MiB" in runner.format_bytes(64 * 1024 * 1024)
    assert runner.format_bytes(100) == "100 B"
    assert "GiB" in runner.format_bytes(2 * 1024 * 1024 * 1024)
    assert "KiB" in runner.format_bytes(2048)


def test_warehouse_stats_counts_parquet_only(tmp_path: Path) -> None:
    runner = _load_write_package()
    warehouse = tmp_path / "wh"
    data_dir = warehouse / "ns" / "t" / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "f1.parquet").write_bytes(b"abc")
    (data_dir / "f2.parquet").write_bytes(b"defg")
    (warehouse / "metadata.json").write_bytes(b"meta")
    total, data_files = runner._warehouse_stats(warehouse)
    assert data_files == 2
    assert total == 3 + 4 + 4  # parquet bytes + metadata


def test_compute_verdict_no_data() -> None:
    runner = _load_write_package()
    board = runner.MatrixBoard(
        scale_factor=0.01,
        source_table="lineitem",
        source_parquet="/x",
        source_bytes=1,
        k_values=[1],
        file_size_bytes=[1024],
        cells=[
            runner.CellResult(
                concurrency=1,
                target_file_size_bytes=1024,
                stages=[],
                wall_total_s=0.0,
                rss_peak_kb=0,
                warehouse_bytes=0,
                data_file_count=0,
                row_count_after_append=None,
                error="boom",
            )
        ],
    )
    verdict = runner.compute_verdict(board)
    assert "NO_DATA" in verdict


def test_compute_verdict_no_k_benefit() -> None:
    runner = _load_write_package()
    cells = []
    for k_value in (1, 2, 4, 8, 16):
        cells.append(
            runner.CellResult(
                concurrency=k_value,
                target_file_size_bytes=64 * 1024 * 1024,
                stages=[
                    runner.StageTiming("seed_parquet_view", 0.1),
                    runner.StageTiming("ctas", 10.0),  # flat across K
                    runner.StageTiming("append", 9.5),
                ],
                wall_total_s=19.6,
                rss_peak_kb=100_000,
                warehouse_bytes=1_000_000,
                data_file_count=2,
                row_count_after_append=100,
            )
        )
    board = runner.MatrixBoard(
        scale_factor=1.0,
        source_table="lineitem",
        source_parquet="/x",
        source_bytes=1,
        k_values=[1, 2, 4, 8, 16],
        file_size_bytes=[64 * 1024 * 1024],
        cells=cells,
    )
    verdict = runner.compute_verdict(board)
    assert "NO_K_BENEFIT_ON_LOCAL_FS" in verdict


def test_compute_verdict_clear_scaling() -> None:
    runner = _load_write_package()
    # Perfect 1/K-ish CTAS walls for K=1..16
    walls = {1: 16.0, 2: 8.5, 4: 4.5, 8: 2.5, 16: 1.5}
    cells = []
    for k_value, ctas in walls.items():
        cells.append(
            runner.CellResult(
                concurrency=k_value,
                target_file_size_bytes=64 * 1024 * 1024,
                stages=[
                    runner.StageTiming("seed_parquet_view", 0.1),
                    runner.StageTiming("ctas", ctas),
                    runner.StageTiming("append", ctas * 0.95),
                ],
                wall_total_s=ctas * 2,
                rss_peak_kb=100_000 + k_value,
                warehouse_bytes=1_000_000,
                data_file_count=k_value,
                row_count_after_append=100,
            )
        )
    board = runner.MatrixBoard(
        scale_factor=1.0,
        source_table="lineitem",
        source_parquet="/x",
        source_bytes=1,
        k_values=list(walls.keys()),
        file_size_bytes=[64 * 1024 * 1024],
        cells=cells,
    )
    verdict = runner.compute_verdict(board)
    assert "NO_STALL_ON_LOCAL_FS" in verdict


def test_compute_verdict_partial_scaling_plateau() -> None:
    """Some K gain then high-K regression → PARTIAL_SCALING_PLATEAU."""
    runner = _load_write_package()
    # K=1 slow; K=2/4 clear gain (>=15%); K=16 regresses past best*1.05
    walls = {1: 10.0, 2: 7.0, 4: 6.5, 8: 6.6, 16: 9.0}
    cells = []
    for k_value, ctas in walls.items():
        cells.append(
            runner.CellResult(
                concurrency=k_value,
                target_file_size_bytes=64 * 1024 * 1024,
                stages=[
                    runner.StageTiming("seed_parquet_view", 0.1),
                    runner.StageTiming("ctas", ctas),
                    runner.StageTiming("append", 5.0),
                ],
                wall_total_s=ctas + 5.1,
                rss_peak_kb=100_000,
                warehouse_bytes=1_000_000,
                data_file_count=k_value,
                row_count_after_append=100,
            )
        )
    board = runner.MatrixBoard(
        scale_factor=1.0,
        source_table="lineitem",
        source_parquet="/x",
        source_bytes=1,
        k_values=list(walls.keys()),
        file_size_bytes=[64 * 1024 * 1024],
        cells=cells,
    )
    verdict = runner.compute_verdict(board)
    assert "PARTIAL_SCALING_PLATEAU" in verdict


def test_compute_verdict_mixed_file_sizes_not_full_stall_clear() -> None:
    """Clear gain on one file-size only must not whole-matrix NO_STALL."""
    runner = _load_write_package()
    cells = []
    # 64 MiB: strong scaling
    for k_value, ctas in {1: 16.0, 2: 8.0, 4: 4.5, 8: 2.5, 16: 1.5}.items():
        cells.append(
            runner.CellResult(
                concurrency=k_value,
                target_file_size_bytes=64 * 1024 * 1024,
                stages=[
                    runner.StageTiming("ctas", ctas),
                    runner.StageTiming("append", 1.0),
                ],
                wall_total_s=ctas + 1.0,
                rss_peak_kb=100_000,
                warehouse_bytes=1_000_000,
                data_file_count=k_value,
                row_count_after_append=100,
            )
        )
    # 256 MiB: flat (no K benefit)
    for k_value in (1, 2, 4, 8, 16):
        cells.append(
            runner.CellResult(
                concurrency=k_value,
                target_file_size_bytes=256 * 1024 * 1024,
                stages=[
                    runner.StageTiming("ctas", 10.0),
                    runner.StageTiming("append", 1.0),
                ],
                wall_total_s=11.0,
                rss_peak_kb=100_000,
                warehouse_bytes=1_000_000,
                data_file_count=2,
                row_count_after_append=100,
            )
        )
    board = runner.MatrixBoard(
        scale_factor=1.0,
        source_table="lineitem",
        source_parquet="/x",
        source_bytes=1,
        k_values=[1, 2, 4, 8, 16],
        file_size_bytes=[64 * 1024 * 1024, 256 * 1024 * 1024],
        cells=cells,
    )
    verdict = runner.compute_verdict(board)
    assert "PARTIAL_SCALING_PLATEAU" in verdict
    assert "NO_STALL_ON_LOCAL_FS" not in verdict.split("VERDICT:")[-1]


def test_probe_release_build_default_unverified(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _load_write_package()
    monkeypatch.delenv("REPARK_WRITE_BENCH_RELEASE", raising=False)
    disclosed, reason = runner.probe_release_build(assert_release=False)
    assert disclosed is False
    assert "UNVERIFIED" in reason


def test_probe_release_build_assert_flag() -> None:
    runner = _load_write_package()
    disclosed, reason = runner.probe_release_build(assert_release=True)
    assert disclosed is True
    assert "assert-release" in reason


def test_probe_release_build_env(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _load_write_package()
    monkeypatch.setenv("REPARK_WRITE_BENCH_RELEASE", "1")
    disclosed, reason = runner.probe_release_build(assert_release=False)
    assert disclosed is True
    assert "REPARK_WRITE_BENCH_RELEASE" in reason


def test_expected_rows_after_ctas_append() -> None:
    runner = _load_write_package()
    assert runner.expected_rows_after_ctas_append(6_001_215) == 12_002_430
    assert runner.expected_rows_after_ctas_append(1) == 2


def test_render_markdown_discloses_scale_and_local_fs() -> None:
    runner = _load_write_package()
    board = runner.MatrixBoard(
        scale_factor=0.1,
        source_table="lineitem",
        source_parquet="/cache/sf0.1/lineitem.parquet",
        source_bytes=1024,
        k_values=[1],
        file_size_bytes=[64 * 1024 * 1024],
        cells=[
            runner.CellResult(
                concurrency=1,
                target_file_size_bytes=64 * 1024 * 1024,
                stages=[
                    runner.StageTiming("seed_parquet_view", 0.01),
                    runner.StageTiming("ctas", 0.5),
                    runner.StageTiming("append", 0.4),
                    runner.StageTiming("count_verify", 0.05),
                ],
                wall_total_s=0.96,
                rss_peak_kb=50_000,
                warehouse_bytes=2048,
                data_file_count=1,
                row_count_after_append=20,
            )
        ],
        findings=["Local-fs object store stands in for S3 — BOUNDED."],
        verdict="VERDICT: NO_K_BENEFIT_ON_LOCAL_FS — synthetic",
        environment={"machine": "test", "platform": "linux", "python": "3.12"},
    )
    md = runner.render_markdown_report(board)
    assert "SF0.1" in md
    assert "local" in md.lower() or "Local" in md
    assert "NO_K_BENEFIT" in md
    assert "maturin develop --release" in md
    assert "fork TableProvider passthrough" in md or "INSERT INTO K effect" in md


def test_datagen_rejects_unknown_table() -> None:
    datagen = _load_datagen()
    with pytest.raises(ValueError, match="unknown TPC-H table"):
        datagen.ensure_source_parquet(0.01, table="not_a_table")


def test_cli_usage_empty_k(capsys: pytest.CaptureFixture[str]) -> None:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    cli = importlib.import_module(f"{_PACKAGE}.run_write_bench")
    code = cli.main(["--k", "", "--file-sizes", "64MiB", "--sf", "0.01"])
    assert code == 2


def test_cli_usage_k_zero(capsys: pytest.CaptureFixture[str]) -> None:
    """K=0 must be usage error (exit 2), not an uncaught traceback."""
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    cli = importlib.import_module(f"{_PACKAGE}.run_write_bench")
    code = cli.main(["--k", "0", "--file-sizes", "64MiB", "--sf", "0.01"])
    assert code == 2
    captured = capsys.readouterr()
    assert "usage error" in captured.err.lower() or "usage error" in captured.out.lower()


def _load_schemas() -> Any:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    return importlib.import_module(f"{_PACKAGE}.schemas")


def _load_merge_runner() -> Any:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    return importlib.import_module(f"{_PACKAGE}.merge_runner")


def _load_overwrite_runner() -> Any:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    return importlib.import_module(f"{_PACKAGE}.overwrite_runner")


def test_merge_source_plan_and_expected_rows() -> None:
    schemas = _load_schemas()
    source_rows, id_start = schemas.merge_source_plan(1_000_000, source_fraction=0.10)
    assert source_rows == 100_000
    assert id_start == 1_000_000 - 50_000
    expected = schemas.expected_rows_after_merge(
        target_rows=1_000_000,
        source_rows=source_rows,
        id_start_source=id_start,
    )
    # half overlap + half new inserts → target + source/2
    assert expected == 1_000_000 + 50_000


def test_expected_rows_after_merge_full_overlap() -> None:
    schemas = _load_schemas()
    # source entirely inside target → no growth
    assert (
        schemas.expected_rows_after_merge(target_rows=100, source_rows=10, id_start_source=0) == 100
    )
    # source entirely past target → full append
    assert (
        schemas.expected_rows_after_merge(target_rows=100, source_rows=10, id_start_source=100)
        == 110
    )


def test_write_synthetic_parquet_narrow_wide(tmp_path: Path) -> None:
    # Polars is optional (repark[polars]); skip the write-I/O pin when absent so the default
    # facade env stays green.
    pytest.importorskip("polars")
    schemas = _load_schemas()
    narrow = schemas.write_synthetic_parquet(tmp_path / "n.parquet", rows=100, width="narrow")
    wide = schemas.write_synthetic_parquet(tmp_path / "w.parquet", rows=50, width="wide")
    assert narrow.is_file() and narrow.stat().st_size > 0
    assert wide.is_file() and wide.stat().st_size > 0
    import pyarrow.parquet as pq

    assert pq.read_metadata(narrow).num_rows == 100
    assert pq.read_metadata(wide).num_rows == 50
    assert schemas.bytes_per_row_estimate("narrow") == 16
    assert schemas.bytes_per_row_estimate("wide") == 8 + schemas.WIDE_FLOAT_COLS * 8


def test_write_synthetic_rejects_bad_width(tmp_path: Path) -> None:
    """Width is validated before the optional polars import (no ModuleNotFoundError)."""
    schemas = _load_schemas()
    with pytest.raises(ValueError, match="width"):
        schemas.write_synthetic_parquet(tmp_path / "x.parquet", rows=10, width="medium")  # type: ignore[arg-type]


def test_write_synthetic_rejects_nonpositive_rows(tmp_path: Path) -> None:
    schemas = _load_schemas()
    with pytest.raises(ValueError, match="rows"):
        schemas.write_synthetic_parquet(tmp_path / "x.parquet", rows=0, width="narrow")


def test_render_merge_markdown_discloses_local_and_knobs() -> None:
    merge = _load_merge_runner()
    board = merge.MergeBoard(
        row_counts=[1_000_000],
        widths=["narrow"],
        k_values=[1, 2],
        source_fraction=0.10,
        cells=[
            merge.MergeCellResult(
                target_rows=1_000_000,
                source_rows=100_000,
                width="narrow",
                concurrency=1,
                stages=[
                    merge.StageTiming("seed_parquet_views", 0.1),
                    merge.StageTiming("ctas_target_mor", 1.0),
                    merge.StageTiming("merge_mor", 2.5),
                    merge.StageTiming("merge_cow", 3.0),
                ],
                wall_total_s=6.6,
                rss_peak_kb=100_000,
                warehouse_bytes=1_000,
                data_file_count=2,
                row_count_after_merge=1_050_000,
                expected_rows=1_050_000,
            )
        ],
        findings=["Local-fs Iceberg MERGE"],
        environment={"machine": "test", "platform": "linux", "python": "3.12"},
        release_build_disclosed=True,
        pinned_knobs={"spark.sql.shuffle.partitions": "8"},
    )
    md = merge.render_merge_markdown(board)
    assert "MERGE" in md
    assert "local" in md.lower() or "Local" in md
    assert "1.000.000" in md.replace(",", "") or "1000000" in md
    assert "rule 10" in md.lower() or "Pinned knobs" in md
    assert "No AWS" in md


def test_render_overwrite_markdown_oth004() -> None:
    ow = _load_overwrite_runner()
    board = ow.OverwriteBoard(
        row_counts=[1_000_000, 10_000_000],
        widths=["narrow"],
        cells=[
            ow.OverwriteCellResult(
                source_rows=1_000_000,
                width="narrow",
                baseline_target_rows=1000,
                stages=[
                    ow.StageTiming("seed_parquet_views", 0.1),
                    ow.StageTiming("ctas_baseline", 0.2),
                    ow.StageTiming("insert_overwrite", 1.5),
                ],
                wall_total_s=1.8,
                rss_before_overwrite_kb=50_000,
                rss_peak_kb=200_000,
                rss_delta_kb=150_000,
                source_parquet_bytes=8_000_000,
                warehouse_bytes=9_000_000,
                data_file_count=1,
                row_count_after=1_000_000,
            ),
            ow.OverwriteCellResult(
                source_rows=10_000_000,
                width="narrow",
                baseline_target_rows=1000,
                stages=[
                    ow.StageTiming("seed_parquet_views", 0.2),
                    ow.StageTiming("ctas_baseline", 0.2),
                    ow.StageTiming("insert_overwrite", 12.0),
                ],
                wall_total_s=12.4,
                rss_before_overwrite_kb=200_000,
                rss_peak_kb=1_500_000,
                rss_delta_kb=1_300_000,
                source_parquet_bytes=80_000_000,
                warehouse_bytes=90_000_000,
                data_file_count=2,
                row_count_after=10_000_000,
            ),
        ],
        findings=["OTH-004 evidence"],
        environment={"machine": "test", "platform": "linux", "python": "3.12"},
        release_build_disclosed=True,
        pinned_knobs={"spark.sql.shuffle.partitions": "8"},
    )
    md = ow.render_overwrite_markdown(board)
    assert "OTH-004" in md
    assert "rss_delta" in md.lower() or "rss_delta_KiB" in md
    assert "1000000" in md or "1_000_000" in md or "1,000,000" in md


def test_cli_extension_mode_help_accepts() -> None:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    cli = importlib.import_module(f"{_PACKAGE}.run_write_bench")
    # bad width must exit 2 without running engine
    code = cli.main(["--mode", "merge", "--width", "medium", "--rows", "100"])
    assert code == 2


def test_cli_merge_k_zero() -> None:
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_WRITE_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    cli = importlib.import_module(f"{_PACKAGE}.run_write_bench")
    code = cli.main(["--mode", "merge", "--k", "0", "--rows", "100"])
    assert code == 2


def test_pinned_knobs_constants() -> None:
    """Shuffle partitions pinned at 8, not the OOM-prone 128."""
    merge = _load_merge_runner()
    ow = _load_overwrite_runner()
    assert merge.PINNED_SHUFFLE_PARTITIONS == 8
    assert ow.PINNED_SHUFFLE_PARTITIONS == 8
    assert merge.PINNED_SHUFFLE_PARTITIONS != 128
    assert ow.PINNED_K == 4

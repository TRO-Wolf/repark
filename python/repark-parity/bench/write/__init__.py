"""R-WRITE-BENCH harness (local-fs CTAS + append + MERGE + OVERWRITE).

Measurement only - no product-engine changes. See ``map.md``.
"""

from __future__ import annotations

from .datagen import DEFAULT_SOURCE_TABLE, ensure_source_parquet
from .merge_runner import (
    DEFAULT_MERGE_K,
    MergeBoard,
    MergeCellResult,
    run_merge_matrix,
)
from .overwrite_runner import (
    OverwriteBoard,
    OverwriteCellResult,
    run_overwrite_matrix,
)
from .runner import (
    DEFAULT_FILE_SIZE_BYTES,
    DEFAULT_K_VALUES,
    CellResult,
    MatrixBoard,
    StageTiming,
    compute_verdict,
    expected_rows_after_ctas_append,
    format_bytes,
    max_rss_kb,
    parse_file_size,
    probe_release_build,
    render_markdown_report,
    run_matrix,
    run_one_cell,
    source_row_count,
)
from .schemas import (
    DEFAULT_SOURCE_FRACTION,
    WIDE_FLOAT_COLS,
    bytes_per_row_estimate,
    expected_rows_after_merge,
    merge_source_plan,
    write_synthetic_parquet,
)

__all__ = [
    "DEFAULT_FILE_SIZE_BYTES",
    "DEFAULT_K_VALUES",
    "DEFAULT_MERGE_K",
    "DEFAULT_SOURCE_FRACTION",
    "DEFAULT_SOURCE_TABLE",
    "WIDE_FLOAT_COLS",
    "CellResult",
    "MatrixBoard",
    "MergeBoard",
    "MergeCellResult",
    "OverwriteBoard",
    "OverwriteCellResult",
    "StageTiming",
    "bytes_per_row_estimate",
    "compute_verdict",
    "ensure_source_parquet",
    "expected_rows_after_ctas_append",
    "expected_rows_after_merge",
    "format_bytes",
    "max_rss_kb",
    "merge_source_plan",
    "parse_file_size",
    "probe_release_build",
    "render_markdown_report",
    "run_matrix",
    "run_merge_matrix",
    "run_one_cell",
    "run_overwrite_matrix",
    "source_row_count",
    "write_synthetic_parquet",
]

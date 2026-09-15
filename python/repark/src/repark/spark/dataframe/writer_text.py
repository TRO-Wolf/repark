"""Text writer: one string column to part files with save-mode staging."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repark.spark.dataframe.writer_readwriter import DataFrameWriter


def text(
    self: DataFrameWriter,
    path: str,
    compression: str | None = None,
    lineSep: str | None = None,  # noqa: N803 — PySpark param name
) -> None:
    """Write one string column as text files (PySpark text). pins: io-text-1/C-002"""
    if compression is not None:
        self.option("compression", compression)
    if lineSep is not None:
        self.option("lineSep", lineSep)
    self._format = "text"
    write_text_path(self, path)


def write_text_path(writer: Any, path: str) -> None:
    """Stage Rust part files through save-mode handling into place. pins: io-text-1/C-002"""
    from repark import _native
    from repark.errors import AnalysisException

    writer._dataframe._ensure_alive()
    _refuse_text_compression(writer)
    if writer._partition_columns:
        raise AnalysisException(
            "DataFrameWriter.text with partitionBy(...) is not supported yet "
            "(partitioned text layout is a future seed)"
        )
    normalized_mode = "error" if writer._mode == "errorifexists" else writer._mode
    if normalized_mode not in writer._PATH_MODES:
        raise AnalysisException(
            f"path write mode must be one of {writer._PATH_MODES}, got {writer._mode!r}"
        )
    destination = Path(path)
    if destination.exists() and normalized_mode == "error":
        raise AnalysisException(
            f"[PATH_ALREADY_EXISTS] Path {path} already exists. "
            'Set mode as "overwrite" to overwrite the existing path.'
        )
    if destination.exists() and normalized_mode == "ignore":
        return
    if (
        normalized_mode == "append"
        and destination.exists()
        and (destination.is_file() or (destination.is_symlink() and not destination.is_dir()))
    ):
        raise AnalysisException(
            f"[PATH_ALREADY_EXISTS] Path {path} is a file (or non-directory symlink); "
            "path mode('append') requires a directory of part files. "
            'Use mode("overwrite") to replace the path, or write to a directory path.'
        )
    linesep = _text_line_sep(writer)
    staging = destination.parent / (
        f"repark-staging-{uuid.uuid4().hex}-{destination.name or 'out'}"
    )
    native = writer._dataframe._native_for_registration()
    try:
        _native.write_text_frame(native, str(staging), linesep)
        if normalized_mode == "append" and destination.exists():
            from repark.spark.dataframe.writer_readwriter import _merge_path_write_tree

            try:
                _merge_path_write_tree(staging, destination)
            except (FileExistsError, OSError, shutil.Error) as exc:
                raise AnalysisException(f"path mode('append') failed for {path!r}: {exc}") from exc
            if staging.exists():
                if staging.is_dir():
                    shutil.rmtree(staging)
                else:
                    staging.unlink()
            return
        if destination.exists():
            if destination.is_symlink():
                raise AnalysisException(
                    f"cannot overwrite path {path!r}: destination is a symbolic link "
                    "(refuse-loud; repark will not rmtree/unlink a symlink destination)"
                )
            try:
                if destination.is_dir():
                    shutil.rmtree(destination)
                else:
                    destination.unlink()
            except OSError as exc:
                raise AnalysisException(f"cannot overwrite path {path!r}: {exc}") from exc
        staging.rename(destination)
    except AnalysisException:
        if staging.exists() and destination.exists():
            if staging.is_dir() and not staging.is_symlink():
                shutil.rmtree(staging)
            elif staging.is_file() or staging.is_symlink():
                staging.unlink()
        raise
    except Exception:
        if staging.exists() and destination.exists():
            if staging.is_dir() and not staging.is_symlink():
                shutil.rmtree(staging)
            elif staging.is_file() or staging.is_symlink():
                staging.unlink()
        raise


def _refuse_text_compression(writer: Any) -> None:
    """Refuse compressed text writes loud (no vendored compressor). pins: io-text-1/C-003"""
    from repark.errors import AnalysisException

    for key, value in writer._options.items():
        if key.lower() == "compression" and str(value).strip().lower() not in {
            "",
            "none",
            "uncompressed",
        }:
            raise AnalysisException(
                f"DataFrameWriter.text option {key!r} is not supported yet "
                "(compressed text would silently diverge if ignored or passed raw — refuse-loud)"
            )


def _text_line_sep(writer: Any) -> str:
    """Resolve the text row separator (newline default). pins: io-text-1/C-002"""
    for key, value in writer._options.items():
        if key.lower() == "linesep":
            return str(value)
    return "\n"

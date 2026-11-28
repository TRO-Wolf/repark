from __future__ import annotations

import bz2
import gzip
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


def datafiles(path: str | Path) -> list[Path]:
    """List the data files under a written path, skipping hidden and underscore entries."""
    root = Path(path)
    return sorted(
        item
        for item in root.rglob("*")
        if item.is_file()
        and all(not part.startswith((".", "_")) for part in item.relative_to(root).parts)
    )


def text_content(path: str | Path) -> list[bytes]:
    """Return the sorted lines of every written text part, after gzip or bzip2 decoding."""
    values: list[bytes] = []
    for file in datafiles(path):
        raw = file.read_bytes()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        elif raw[:3] == b"BZh":
            raw = bz2.decompress(raw)
        values.extend(raw.splitlines(keepends=True))
    return sorted(values)


def read_written(path: str | Path, fmt: str) -> list[Any]:
    """Return written rows: Parquet through PyArrow, every other format as text lines."""
    if fmt != "parquet":
        return text_content(path)
    values = []
    for part in datafiles(path):
        table = pq.ParquetFile(part, coerce_int96_timestamp_unit="us").read()
        values.extend(sorted(item.items()) for item in table.to_pylist())
    return sorted(values, key=repr)


def codec_written(path: str | Path) -> list[str]:
    """Return the compression codecs of the first column of each written Parquet part."""
    return sorted(
        {
            pq.ParquetFile(file).metadata.row_group(0).column(0).compression
            for file in datafiles(path)
        }
    )


def compression_header(path: str | Path) -> list[str]:
    """Return whether each written part starts with a gzip header."""
    return sorted(
        {
            "gzip" if file.read_bytes()[:2] == b"\x1f\x8b" else "uncompressed"
            for file in datafiles(path)
        }
    )

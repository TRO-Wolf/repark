"""Project a parquet scan's hidden file-source struct through DataFrame.metadataColumn.

pins: df-metadata-col-1/M-4
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.metadataColumn"]


def main() -> None:
    """Read back two parquet rows and check the file name and row indices."""
    repark = ReparkSession.builder.appName("ex-df-metadata").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            frame = repark.createDataFrame([(1, "a"), (2, "b")], ["i", "s"])
            frame.coalesce(1).write.parquet(str(Path(workdir) / "p"))
            read = repark.read.parquet(str(Path(workdir) / "p"))
            scoped = read.select(
                "i", read.metadataColumn("_metadata").getField("file_name").alias("name")
            )
            names = sorted(row[1] for row in scoped.collect())
            if len(names) != 2 or not all(name.endswith(".parquet") for name in names):
                raise SystemExit(f"DataFrame.metadataColumn names {names!r} mismatch")
            indexed = read.select(
                "i", read.metadataColumn("_metadata").getField("row_index").alias("ri")
            )
            rows = sorted(tuple(row) for row in indexed.collect())
            if rows != [(1, 0), (2, 1)]:
                raise SystemExit(f"DataFrame.metadataColumn rows {rows!r} != [(1, 0), (2, 1)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()

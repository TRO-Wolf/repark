"""List source files and hash the logical plan.

pins: df-plan-introspect-1/C-003
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.inputFiles", "DataFrame.semanticHash"]


def main() -> None:
    """Run the measured inputFiles and semanticHash answers on one local frame."""
    repark = ReparkSession.builder.appName("ex-df-plan-introspect").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("x", 1, 2), ("y", 3, 4)], ["key", "a", "b"])
        local_files = frame.inputFiles()
        if local_files != []:
            raise SystemExit(f"DataFrame.inputFiles local {local_files!r} != []")
        with tempfile.TemporaryDirectory() as workdir:
            target = str(Path(workdir) / "pq")
            frame.coalesce(1).write.parquet(target)
            stored = repark.read.parquet(target)
            files = stored.inputFiles()
            if len(files) != 1 or not files[0].startswith("file://"):
                raise SystemExit(f"DataFrame.inputFiles parquet {files!r} != one file:// URI")
            if not files[0].endswith(".parquet"):
                raise SystemExit(f"DataFrame.inputFiles suffix {files[0]!r} != *.parquet")
        first = repark.range(3).filter("id > 1")
        second = repark.range(3).filter("id > 1")
        first_hash = first.semanticHash()
        second_hash = second.semanticHash()
        if first_hash != second_hash:
            raise SystemExit(f"DataFrame.semanticHash {first_hash!r} != {second_hash!r}")
        if not -(2**31) <= first_hash <= 2**31 - 1:
            raise SystemExit(f"DataFrame.semanticHash {first_hash!r} outside int range")
        left_alias = repark.range(3).select(F.col("id").alias("a"))
        right_alias = repark.range(3).select(F.col("id").alias("b"))
        if left_alias.semanticHash() != right_alias.semanticHash():
            raise SystemExit("DataFrame.semanticHash alias spelling changed the hash")
        narrow = repark.range(3)
        wide = repark.range(4)
        if narrow.semanticHash() == wide.semanticHash():
            raise SystemExit("DataFrame.semanticHash range(3) == range(4)")
        first_sql = repark.sql("SELECT 1 AS a")
        second_sql = repark.sql("SELECT 1 AS b")
        if first_sql.semanticHash() != second_sql.semanticHash():
            raise SystemExit("DataFrame.semanticHash SQL alias spelling changed the hash")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()

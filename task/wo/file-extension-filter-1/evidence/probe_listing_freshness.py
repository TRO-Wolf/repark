from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_ids(path: Path, ident: int) -> None:
    pq.write_table(pa.table({"id": pa.array([ident], pa.int64())}), str(path))


def main() -> None:
    root = Path(sys.argv[1])
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    write_ids(root / "a.parquet", 10)
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("listing-freshness").getOrCreate()
    frame = spark.read.parquet(str(root))
    print("first", frame.count())
    spark.createDataFrame([(20,)], "id bigint").write.mode("append").parquet(str(root))
    print("same frame", frame.count(), "fresh read", spark.read.parquet(str(root)).count())
    write_ids(root / "b", 30)
    print("after b", frame.count(), "fresh read", spark.read.parquet(str(root)).count())


if __name__ == "__main__":
    main()

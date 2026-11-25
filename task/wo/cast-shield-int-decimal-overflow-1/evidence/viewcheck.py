import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark import ReparkSession

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
pq.write_table(
    pa.table({"id": pa.array([1, 2], pa.int32()), "bi": pa.array([1, 123456], pa.int64())}),
    str(root / "v.parquet"),
)
spark = ReparkSession.builder.appName("v").getOrCreate()
spark.read.parquet(str(root / "v.parquet")).createOrReplaceTempView("tp")
spark.createDataFrame([(1, 1)], "id INT, bi BIGINT").createOrReplaceTempView("tm")
for src in ("tp", "tm"):
    try:
        spark.sql(
            f"CREATE OR REPLACE TEMP VIEW vv_{src} AS SELECT id FROM {src} WHERE bi = 1"
        ).collect()
        print(src, spark.sql(f"SELECT * FROM vv_{src}").collect())
    except Exception as exc:
        print(src, "ERR", str(exc)[:200])
    try:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW ww_{src} AS SELECT id FROM {src} WHERE bi = 1")
        print(src, "lazy", spark.sql(f"SELECT * FROM ww_{src}").collect())
    except Exception as exc:
        print(src, "lazy ERR", str(exc)[:200])

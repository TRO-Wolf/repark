from __future__ import annotations

import shutil
import sys
from pathlib import Path

from repark.spark import types as st
from repark.spark.session import ReparkSession


def main() -> int:
    root = Path(sys.argv[1]) / "repark"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    spark = (
        ReparkSession.builder.appName("partdisc-repark-gen")
        .config("spark.sql.session.timeZone", "UTC")
        .config("repark.local_fs.allow_ddl", "true")
        .getOrCreate()
    )

    base_schema = st.StructType(
        [
            st.StructField("id", st.LongType(), True),
            st.StructField("p", st.IntegerType(), True),
            st.StructField("s", st.StringType(), True),
        ]
    )
    base_df = spark.createDataFrame(
        [(1, 1, "a"), (2, 1, "b"), (3, 2, "c"), (4, 2, "d")],
        schema=base_schema,
    )
    two_schema = st.StructType(
        [
            st.StructField("id", st.LongType(), True),
            st.StructField("year", st.IntegerType(), True),
            st.StructField("month", st.IntegerType(), True),
            st.StructField("s", st.StringType(), True),
        ]
    )
    two_df = spark.createDataFrame(
        [(1, 2024, 1, "a"), (2, 2024, 2, "b"), (3, 2025, 1, "c")],
        schema=two_schema,
    )
    text_schema = st.StructType(
        [
            st.StructField("value", st.StringType(), True),
            st.StructField("p", st.IntegerType(), True),
        ]
    )
    text_df = spark.createDataFrame(
        [("a", 1), ("b", 1), ("c", 2), ("d", 2)],
        schema=text_schema,
    )

    base_df.write.mode("overwrite").partitionBy("p").parquet(str(root / "parquet" / "one_level"))
    two_df.write.mode("overwrite").partitionBy("year", "month").parquet(
        str(root / "parquet" / "two_level")
    )
    base_df.write.mode("overwrite").option("header", "true").partitionBy("p").csv(
        str(root / "csv_header" / "one_level")
    )
    base_df.write.mode("overwrite").partitionBy("p").json(str(root / "json" / "one_level"))
    text_df.write.mode("overwrite").partitionBy("p").text(str(root / "text" / "one_level"))

    spark.stop()
    print("REPARK-FIXTURES-GENERATED-OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

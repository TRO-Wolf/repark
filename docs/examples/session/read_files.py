"""Read CSV, JSON, and Parquet files written by this example.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.read_csv",
    "SparkSession.read_json",
    "SparkSession.read_parquet",
]


def main() -> None:
    """Run the measured reader answers: one local file per format, rows as Spark answers them."""
    repark = ReparkSession.builder.appName("ex21-ses-read").master("local[1]").getOrCreate()
    try:
        csv_path = Path("ex21_read.csv")
        csv_path.write_text("k,v\na,1\nb,2\n", encoding="utf-8")
        csv_rows = [
            tuple(row)
            for row in repark.read_csv(str(csv_path), options={"header": "true"}).collect()
        ]
        csv_rows_expected = [("a", 1), ("b", 2)]
        if csv_rows != csv_rows_expected:
            raise SystemExit(f"read_csv rows {csv_rows!r} != {csv_rows_expected!r}")

        json_path = Path("ex21_read.json")
        json_path.write_text('{"k":"a","v":1}\n{"k":"b","v":2}\n', encoding="utf-8")
        json_rows = [tuple(row) for row in repark.read_json(str(json_path)).collect()]
        json_rows_expected = [("a", 1), ("b", 2)]
        if json_rows != json_rows_expected:
            raise SystemExit(f"read_json rows {json_rows!r} != {json_rows_expected!r}")

        parquet_dir = Path("ex21_read_pq")
        source = repark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        source.write.mode("overwrite").parquet(str(parquet_dir))
        parquet_rows = sorted(
            tuple(row) for row in repark.read_parquet(str(parquet_dir)).collect()
        )
        parquet_rows_expected = [(1, "a"), (2, "b")]
        if parquet_rows != parquet_rows_expected:
            raise SystemExit(f"read_parquet rows {parquet_rows!r} != {parquet_rows_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()

from __future__ import annotations

import datetime
import os
import shutil
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as st


def _build_spark(warehouse: Path) -> SparkSession:
    jar = os.environ["SPARK_ICEBERG_JAR"]
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("partdisc-gen")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _write_format(df: Any, fmt: str, out: Path, part_cols: list[str] | None = None) -> None:
    writer = df.coalesce(1).write.mode("overwrite")
    if part_cols:
        writer = writer.partitionBy(*part_cols)
    if fmt == "parquet":
        writer.parquet(str(out))
    elif fmt == "csv_header":
        writer.option("header", "true").csv(str(out))
    elif fmt == "csv_noheader":
        writer.option("header", "false").csv(str(out))
    elif fmt == "json":
        writer.json(str(out))
    elif fmt == "orc":
        writer.orc(str(out))
    elif fmt == "text":
        writer.text(str(out))
    else:
        raise ValueError(fmt)


def _first_data_file(directory: Path, ext: str) -> Path:
    for path in sorted(directory.rglob(f"*{ext}")):
        if path.is_file() and not path.name.startswith((".", "_")):
            return path
    raise FileNotFoundError(f"no {ext} under {directory}")


def _place_part(src: Path, dst_dir: Path, filename: str) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / filename)


def main() -> int:
    root = Path(sys.argv[1])
    if root.exists():
        shutil.rmtree(root)
    spark_root = root / "spark"
    warehouse = root / "iceberg_wh"
    spark_root.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)

    spark = _build_spark(warehouse)

    base_schema = st.StructType(
        [
            st.StructField("id", st.LongType(), True),
            st.StructField("p", st.IntegerType(), True),
            st.StructField("s", st.StringType(), True),
        ]
    )
    base_rows = [(1, 1, "a"), (2, 1, "b"), (3, 2, "c"), (4, 2, "d")]
    base_df = spark.createDataFrame(base_rows, schema=base_schema)

    text_schema = st.StructType(
        [
            st.StructField("value", st.StringType(), True),
            st.StructField("p", st.IntegerType(), True),
        ]
    )
    text_rows = [("a", 1), ("b", 1), ("c", 2), ("d", 2)]
    text_df = spark.createDataFrame(text_rows, schema=text_schema)

    two_schema = st.StructType(
        [
            st.StructField("id", st.LongType(), True),
            st.StructField("year", st.IntegerType(), True),
            st.StructField("month", st.IntegerType(), True),
            st.StructField("s", st.StringType(), True),
        ]
    )
    two_rows = [(1, 2024, 1, "a"), (2, 2024, 2, "b"), (3, 2025, 1, "c")]
    two_df = spark.createDataFrame(two_rows, schema=two_schema)
    two_text_schema = st.StructType(
        [
            st.StructField("value", st.StringType(), True),
            st.StructField("year", st.IntegerType(), True),
            st.StructField("month", st.IntegerType(), True),
        ]
    )
    two_text_df = spark.createDataFrame(
        [("a", 2024, 1), ("b", 2024, 2), ("c", 2025, 1)], schema=two_text_schema
    )

    formats = [
        ("parquet", ".parquet"),
        ("csv_header", ".csv"),
        ("csv_noheader", ".csv"),
        ("json", ".json"),
        ("orc", ".orc"),
        ("text", ".txt"),
    ]

    for fmt, ext in formats:
        froot = spark_root / fmt
        df_one = text_df if fmt == "text" else base_df
        df_two = two_text_df if fmt == "text" else two_df
        _write_format(df_one, fmt, froot / "one_level", ["p"])
        _write_format(df_two, fmt, froot / "two_level", ["year", "month"])

        sample_a_df = (
            spark.createDataFrame([("x",)], ["value"])
            if fmt == "text"
            else spark.createDataFrame(
                [(1, "x")],
                st.StructType(
                    [
                        st.StructField("id", st.LongType(), True),
                        st.StructField("s", st.StringType(), True),
                    ]
                ),
            )
        )
        sample_b_df = (
            spark.createDataFrame([("y",)], ["value"])
            if fmt == "text"
            else spark.createDataFrame(
                [(2, "y")],
                st.StructType(
                    [
                        st.StructField("id", st.LongType(), True),
                        st.StructField("s", st.StringType(), True),
                    ]
                ),
            )
        )
        tmp_a = froot / "_tmp_a"
        tmp_b = froot / "_tmp_b"
        _write_format(sample_a_df, fmt, tmp_a)
        _write_format(sample_b_df, fmt, tmp_b)
        file_a = _first_data_file(tmp_a, ext)
        file_b = _first_data_file(tmp_b, ext)

        layouts: dict[str, list[tuple[str, Path]]] = {
            "type_int": [("p=1", file_a), ("p=-5", file_b)],
            "type_bigint": [("p=1", file_a), ("p=3000000000", file_b)],
            "type_double": [("p=1.5", file_a), ("p=2.25", file_b)],
            "type_decimal_looking": [("p=1.50", file_a), ("p=2.50", file_b)],
            "type_decimal_huge": [
                ("p=123456789012345678901234567890", file_a),
                ("p=1.23", file_b),
            ],
            "type_decimal_only_huge": [("p=123456789012345678901234567890", file_a)],
            "type_date": [("p=2024-01-02", file_a), ("p=2024-07-15", file_b)],
            "type_timestamp_raw": [
                ("p=2024-01-02 03:04:05", file_a),
                ("p=2024-07-15 12:30:00", file_b),
            ],
            "type_timestamp_iso": [("p=2024-01-02T03:04:05", file_a)],
            "type_timestamp_frac": [("p=2024-01-02 03:04:05.5", file_a)],
            "type_timestamp_frac2": [("p=2024-01-02 03:04:05.12", file_a)],
            "type_string": [("p=alpha", file_a), ("p=beta", file_b)],
            "type_bool": [("p=true", file_a), ("p=false", file_b)],
            "type_leading_zero": [("p=01", file_a), ("p=02", file_b)],
            "type_leading_zero_mixed": [("p=01", file_a), ("p=10", file_b)],
            "type_int_string_mix": [("p=1", file_a), ("p=abc", file_b)],
            "type_int_double_mix": [("p=1", file_a), ("p=1.5", file_b)],
            "special_hive_default": [
                ("p=__HIVE_DEFAULT_PARTITION__", file_a),
                ("p=1", file_b),
            ],
            "special_hive_default_only": [("p=__HIVE_DEFAULT_PARTITION__", file_a)],
            "special_empty_val": [("p=", file_a), ("p=1", file_b)],
            "special_empty_val_only": [("p=", file_a)],
            "special_url_escaped": [("p=a%20b", file_a), ("p=a%2Fb", file_b)],
            "conflict_names": [("a=1", file_a), ("b=2", file_b)],
            "mixed_depth": [("a=1", file_a), ("a=1/b=2", file_b)],
            "root_and_part": [("", file_a), ("p=1", file_b)],
            "root_with_equals_flat/tag=foo": [("", file_a)],
            "root_with_equals_part/tag=foo": [("p=1", file_a), ("p=2", file_b)],
            "case_dir_upper_no_data": [("P=1", file_a), ("P=2", file_b)],
            "case_dir_mixed": [("p=1", file_a), ("P=2", file_b)],
        }
        for rel_name, entries in layouts.items():
            target = froot / rel_name
            for idx, (sub, src) in enumerate(entries):
                dst_dir = target / sub if sub else target
                _place_part(src, dst_dir, f"part-{idx:05d}{ext}")

        ts_schema = (
            st.StructType(
                [
                    st.StructField("value", st.StringType(), True),
                    st.StructField("p", st.TimestampType(), True),
                ]
            )
            if fmt == "text"
            else st.StructType(
                [
                    st.StructField("id", st.LongType(), True),
                    st.StructField("s", st.StringType(), True),
                    st.StructField("p", st.TimestampType(), True),
                ]
            )
        )
        ts_rows = (
            [
                ("x", datetime.datetime(2024, 1, 2, 3, 4, 5)),
                ("y", datetime.datetime(2024, 7, 15, 12, 30, 0)),
            ]
            if fmt == "text"
            else [
                (1, "x", datetime.datetime(2024, 1, 2, 3, 4, 5)),
                (2, "y", datetime.datetime(2024, 7, 15, 12, 30, 0)),
            ]
        )
        _write_format(
            spark.createDataFrame(ts_rows, schema=ts_schema),
            fmt,
            froot / "type_timestamp_spark",
            ["p"],
        )

        esc_schema = (
            st.StructType(
                [
                    st.StructField("value", st.StringType(), True),
                    st.StructField("p", st.StringType(), True),
                ]
            )
            if fmt == "text"
            else st.StructType(
                [
                    st.StructField("id", st.LongType(), True),
                    st.StructField("s", st.StringType(), True),
                    st.StructField("p", st.StringType(), True),
                ]
            )
        )
        esc_rows = (
            [("x", "a b"), ("y", "a/b"), ("z", "50%"), ("w", "c=d")]
            if fmt == "text"
            else [
                (1, "x", "a b"),
                (2, "y", "a/b"),
                (3, "z", "50%"),
                (4, "w", "c=d"),
            ]
        )
        _write_format(
            spark.createDataFrame(esc_rows, schema=esc_schema),
            fmt,
            froot / "special_url_spark_written",
            ["p"],
        )

        noise = froot / "noise_hidden"
        shutil.copytree(froot / "one_level", noise)
        (noise / "_SUCCESS").write_text("", encoding="utf-8")
        _place_part(file_a, noise / "_temporary" / "0", f"part-temp{ext}")
        _place_part(file_a, noise, f".hidden{ext}")
        (noise / f".part-00000{ext}.crc").write_bytes(b"crc")
        _place_part(file_a, noise / "p=1", f".dot{ext}")
        _place_part(file_a, noise / "p=1", f"_under{ext}")

        if fmt != "text":
            with_p_same = spark.createDataFrame(
                [(1, 99, "x")],
                st.StructType(
                    [
                        st.StructField("id", st.LongType(), True),
                        st.StructField("p", st.IntegerType(), True),
                        st.StructField("s", st.StringType(), True),
                    ]
                ),
            )
            tmp_p_same = froot / "_tmp_p_same"
            _write_format(with_p_same, fmt, tmp_p_same)
            fp_same = _first_data_file(tmp_p_same, ext)
            _place_part(fp_same, froot / "col_in_file_and_dir_same" / "p=10", f"part-00000{ext}")
            _place_part(fp_same, froot / "case_dir_upper_data_lower" / "P=10", f"part-00000{ext}")

            with_p_str = spark.createDataFrame(
                [(1, "orig", "x")],
                st.StructType(
                    [
                        st.StructField("id", st.LongType(), True),
                        st.StructField("p", st.StringType(), True),
                        st.StructField("s", st.StringType(), True),
                    ]
                ),
            )
            tmp_p_str = froot / "_tmp_p_str"
            _write_format(with_p_str, fmt, tmp_p_str)
            fp_str = _first_data_file(tmp_p_str, ext)
            _place_part(
                fp_str, froot / "col_in_file_and_dir_diff_type" / "p=10", f"part-00000{ext}"
            )

            evo_a = spark.createDataFrame(
                [(1, "a1")],
                st.StructType(
                    [
                        st.StructField("id", st.LongType(), True),
                        st.StructField("a", st.StringType(), True),
                    ]
                ),
            )
            evo_b = spark.createDataFrame(
                [(2, "b2")],
                st.StructType(
                    [
                        st.StructField("id", st.LongType(), True),
                        st.StructField("b", st.StringType(), True),
                    ]
                ),
            )
            tmp_ea = froot / "_tmp_ea"
            tmp_eb = froot / "_tmp_eb"
            _write_format(evo_a, fmt, tmp_ea)
            _write_format(evo_b, fmt, tmp_eb)
            _place_part(
                _first_data_file(tmp_ea, ext), froot / "evolved" / "p=1", f"part-00000{ext}"
            )
            _place_part(
                _first_data_file(tmp_eb, ext), froot / "evolved" / "p=2", f"part-00000{ext}"
            )

            shutil.rmtree(tmp_p_same)
            shutil.rmtree(tmp_p_str)
            shutil.rmtree(tmp_ea)
            shutil.rmtree(tmp_eb)

        shutil.rmtree(tmp_a)
        shutil.rmtree(tmp_b)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
    spark.sql("DROP TABLE IF EXISTS ice.db.part_ice")
    spark.sql(
        "CREATE TABLE ice.db.part_ice (id BIGINT, s STRING, p INT) USING iceberg PARTITIONED BY (p)"
    )
    spark.sql(
        "INSERT INTO ice.db.part_ice VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2), (4, 'd', 2)"
    )

    spark.stop()
    print("FIXTURES-GENERATED-OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Local MoR MERGE phase decomposition (LOCAL filesystem only).

Memory catalog + LocalFs warehouse. NEVER sets AWS env / REPARK_* / TABLE_BUCKET_ARN.

Usage::

    python python/repark-parity/bench/bench_mor_merge.py \\
        [--rows 200000] [--source 20000] [--concurrency N] [--seed parquet] \\
        [--codec zstd|uncompressed]

``--seed parquet``: polars → parquet → ``read_parquet`` (fast seed; avoids VALUES re-plan).
``--concurrency N``: sets ``repark.write.max-concurrent-files`` at builder time (default engine 4).
``--codec``: sets Iceberg ``write.parquet.compression-codec`` on CTAS tables (default ``zstd`` —
engine default when the property is absent; ``uncompressed`` is the old-behavior escape hatch).
Local FS will NOT show the S3 wall-clock win — the latency-injection Rust pin is the evidence;
live S3 before/after is orchestrator-owned. Record local bytes + wall in the unit ledger.
"""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path


def _seed_frame(spark: object, rows: int, *, seed: str, tmp: Path, view: str) -> None:
    """Build a temp view of ``rows`` id/v pairs via list VALUES or parquet seed."""
    if seed == "parquet":
        import polars as pl

        path = tmp / f"{view}.parquet"
        frame = pl.DataFrame(
            {
                "id": list(range(rows)),
                "v": [float(index % 100) for index in range(rows)],
            }
        )
        frame.write_parquet(path)
        spark.read.parquet(str(path)).createOrReplaceTempView(view)  # type: ignore[attr-defined]
        return
    data = [(index, float(index % 100)) for index in range(rows)]
    spark.createDataFrame(data, schema=["id", "v"]).createOrReplaceTempView(view)  # type: ignore[attr-defined]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--source", type=int, default=5_000)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="repark.write.max-concurrent-files (omit = engine default 4)",
    )
    parser.add_argument(
        "--seed",
        choices=("values", "parquet"),
        default="values",
        help="how to materialize the seed frame (parquet is the fast path)",
    )
    parser.add_argument(
        "--codec",
        choices=("zstd", "uncompressed", "snappy", "gzip", "lz4"),
        default="zstd",
        help="write.parquet.compression-codec on CTAS tables (default zstd)",
    )
    args = parser.parse_args()

    from repark import ReparkSession

    rows = args.rows
    source_rows = args.source
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        warehouse = (tmp_path / "wh").resolve()
        warehouse.mkdir()
        builder = ReparkSession.builder.appName("bench-mor")
        if args.concurrency is not None:
            builder = builder.config(
                "repark.write.max-concurrent-files",
                str(args.concurrency),
            )
        spark = builder.getOrCreate()
        spark.register_memory_catalog("cat", str(warehouse))
        spark.sql("CREATE NAMESPACE cat.ns")

        phases: list[tuple[str, float]] = []

        t0 = time.perf_counter()
        _seed_frame(spark, rows, seed=args.seed, tmp=tmp_path, view="seed")
        phases.append(("build_seed_view", time.perf_counter() - t0))

        codec = args.codec
        t0 = time.perf_counter()
        spark.sql(
            "CREATE TABLE cat.ns.target USING iceberg "
            "TBLPROPERTIES ("
            "  'write.delete.mode'='merge-on-read',"
            "  'write.update.mode'='merge-on-read',"
            "  'write.merge.mode'='merge-on-read',"
            f"  'write.parquet.compression-codec'='{codec}'"
            ") AS SELECT * FROM seed"
        )
        phases.append(("ctas_target_mor", time.perf_counter() - t0))

        t0 = time.perf_counter()
        if args.seed == "parquet":
            import polars as pl

            src_path = tmp_path / "src.parquet"
            pl.DataFrame(
                {
                    "id": list(range(source_rows)),
                    "v": [float(index % 100) + 0.5 for index in range(source_rows)],
                }
            ).write_parquet(src_path)
            spark.read.parquet(str(src_path)).createOrReplaceTempView("src")
        else:
            source_data = [(index, float(index % 100) + 0.5) for index in range(source_rows)]
            spark.createDataFrame(source_data, schema=["id", "v"]).createOrReplaceTempView("src")
        phases.append(("build_source_view", time.perf_counter() - t0))

        t0 = time.perf_counter()
        spark.sql(
            "MERGE INTO cat.ns.target AS t USING src AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.v = s.v "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        phases.append(("merge_mor_upsert", time.perf_counter() - t0))

        t0 = time.perf_counter()
        count = spark.sql("SELECT count(*) AS c FROM cat.ns.target").collect()[0][0]
        phases.append(("count_after_merge", time.perf_counter() - t0))

        t0 = time.perf_counter()
        spark.sql(
            "CREATE TABLE cat.ns.target_cow USING iceberg "
            f"TBLPROPERTIES ('write.parquet.compression-codec'='{codec}') "
            "AS SELECT * FROM seed"
        )
        phases.append(("ctas_target_cow", time.perf_counter() - t0))

        t0 = time.perf_counter()
        spark.sql(
            "MERGE INTO cat.ns.target_cow AS t USING src AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.v = s.v "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        phases.append(("merge_cow_upsert", time.perf_counter() - t0))

        concurrency_note = str(args.concurrency) if args.concurrency is not None else "default(4)"
        # Local warehouse bytes (all files under the warehouse root).
        warehouse_bytes = sum(
            path.stat().st_size for path in warehouse.rglob("*") if path.is_file()
        )
        print(
            f"rows={rows} source={source_rows} count_after={count} "
            f"seed={args.seed} concurrency={concurrency_note} codec={codec} "
            f"warehouse_bytes={warehouse_bytes}"
        )
        print(f"{'phase':28s}  seconds")
        total = 0.0
        for name, seconds in phases:
            print(f"{name:28s}  {seconds:8.3f}")
            total += seconds
        print(f"{'TOTAL_TIMED':28s}  {total:8.3f}")
        by_name = dict(phases)
        merge_mor = by_name.get("merge_mor_upsert", 0.0)
        merge_cow = by_name.get("merge_cow_upsert", 0.0)
        if merge_cow > 0:
            print(f"mor/cow merge ratio = {merge_mor / merge_cow:.2f}")
        print(
            "NOTE: local FS does not show S3 transport win; "
            "latency-injection Rust pin is the concurrency evidence."
        )
        spark.stop()


if __name__ == "__main__":
    main()

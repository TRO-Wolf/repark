"""Regenerate the PERF-ICE-SCAN-1 read bed: seeds, CTAS tables, layouts."""

import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark import ReparkSession, _native


def lane_root() -> Path:
    """The lane checkout this probe runs from."""
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True
    )
    return Path(out.stdout.strip())


def refuse_unless_release(lane: Path) -> None:
    """Refuse anything but the lane release module."""
    import repark

    path = Path(repark.__file__).resolve()
    assert lane in path.parents, path
    assert _native.__debug_assertions__ is False


def wide_rows(start: int, stop: int) -> pa.Table:
    """The analysis seven-column bed, deterministic from the row index."""
    return pa.table(
        {
            "id": pa.array(range(start, stop), type=pa.int64()),
            "ts": pa.array(
                [1_600_000_000 + index for index in range(start, stop)], type=pa.int64()
            ),
            "v": pa.array([float(index) for index in range(start, stop)], type=pa.float64()),
            "vi": pa.array([index % 1000 for index in range(start, stop)], type=pa.int32()),
            "s": pa.array([f"s{index:015d}" for index in range(start, stop)], type=pa.string()),
            "cat": pa.array(
                [f"c{index % 100:02d}" for index in range(start, stop)], type=pa.string()
            ),
            "part": pa.array([index % 8 for index in range(start, stop)], type=pa.int32()),
        }
    )


def write_seed(directory: Path, rows: int, files: int, row_group_size: int) -> Path:
    """A fixed files-way zstd seed of the wide bed, reused when complete."""
    directory.mkdir(parents=True, exist_ok=True)
    if len(list(directory.glob("part-*.parquet"))) == files:
        return directory
    per_file = rows // files
    for index in range(files):
        start = index * per_file
        stop = rows if index == files - 1 else start + per_file
        pq.write_table(
            wide_rows(start, stop),
            directory / f"part-{index}.parquet",
            compression="zstd",
            row_group_size=row_group_size,
        )
    return directory


def build(engine: ReparkSession, bed: Path) -> None:
    """Seed and CTAS the read bed on an engine with the bed catalog."""
    seed1e6 = write_seed(bed / "synth_1e6", 1_000_000, 8, 100_000)
    seed1e7 = write_seed(bed / "synth_1e7", 10_000_000, 8, 1_000_000)
    engine.sql("CREATE NAMESPACE IF NOT EXISTS bed.ns")
    engine.read.parquet(str(seed1e6)).createOrReplaceTempView("src1e6")
    engine.read.parquet(str(seed1e7)).createOrReplaceTempView("src1e7")
    engine.sql("CREATE TABLE bed.ns.t_plain USING iceberg AS SELECT * FROM src1e6").collect()
    engine.sql(
        "CREATE TABLE bed.ns.t_part USING iceberg PARTITIONED BY (part) AS SELECT * FROM src1e6"
    ).collect()
    engine.sql("CREATE TABLE bed.ns.t_plain7 USING iceberg AS SELECT * FROM src1e7").collect()
    engine.sql(
        "CREATE TABLE bed.ns.t_dv (id BIGINT, ts BIGINT, v DOUBLE, vi INT, s STRING, "
        "cat STRING, part INT) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')"
    ).collect()
    engine.sql("INSERT INTO bed.ns.t_dv SELECT * FROM src1e6").collect()
    engine.sql("DELETE FROM bed.ns.t_dv WHERE id % 100 = 0").collect()


def print_layouts(engine: ReparkSession) -> None:
    """Print every bed table file layout and count."""
    for table in ("t_plain", "t_part", "t_plain7", "t_dv"):
        files = engine.sql(
            f"SELECT file_path, record_count FROM bed.ns.{table}.files ORDER BY file_path"
        ).to_arrow()
        print(f"== bed.ns.{table}: {files.num_rows} files")
        for path, count in zip(
            files.column("file_path").to_pylist(),
            files.column("record_count").to_pylist(),
            strict=True,
        ):
            print(f"   {Path(path).name} {count}")
        count = engine.sql(f"SELECT count(*) AS n FROM bed.ns.{table}").to_arrow()
        print(f"   count(*) = {count.column('n')[0].as_py()}")


def main() -> None:
    """Build seeds and CTAS the read bed, then print every table layout."""
    lane = lane_root()
    refuse_unless_release(lane)
    bed = Path(sys.argv[1])
    bed.mkdir(parents=True, exist_ok=True)
    engine = (
        ReparkSession.builder.appName("scan-bed")
        .config("spark.sql.shuffle.partitions", "8")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    try:
        engine.register_memory_catalog("bed", bed / "wh")
        build(engine, bed)
        print_layouts(engine)
    finally:
        engine.stop()


if __name__ == "__main__":
    main()

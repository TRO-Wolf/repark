import json
import os
import resource
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

from repark import ReparkSession, _native

if _native.__debug_assertions__:
    raise SystemExit("refusing to measure a debug module")

SRC = str(Path(sys.argv[1]).resolve())
CELL = sys.argv[2]
THREADS = sys.argv[3] if len(sys.argv) > 3 else "8"
REPS = int(sys.argv[4]) if len(sys.argv) > 4 else 5

root = Path(tempfile.mkdtemp(prefix=f"repark-cell-{CELL}-"))
engine = (
    ReparkSession.builder.appName(f"cell-{CELL}")
    .config("spark.sql.shuffle.partitions", THREADS)
    .getOrCreate()
)
engine.register_memory_catalog("ice", root)
engine.sql("CREATE NAMESPACE IF NOT EXISTS ice.perf")
engine.read.parquet(SRC).createOrReplaceTempView("src")


def setup(index: int) -> None:
    """Build the target the timed statement writes; untimed."""
    if CELL in ("insert_overwrite", "insert_overwrite_ordered"):
        engine.sql(
            f"CREATE TABLE ice.perf.t{index} USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM src"
        ).collect()
    if CELL == "insert_overwrite_ordered":
        engine.sql(f"ALTER TABLE ice.perf.t{index} WRITE ORDERED BY (id)").collect()


def run(index: int) -> None:
    """Run the selected cell once against a fresh table."""
    if CELL == "ctas":
        engine.sql(f"CREATE TABLE ice.perf.t{index} USING iceberg AS SELECT * FROM src").collect()
    elif CELL == "ctas_partitioned8":
        engine.sql(
            f"CREATE TABLE ice.perf.t{index} USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM src"
        ).collect()
    elif CELL in ("insert_overwrite", "insert_overwrite_ordered"):
        engine.sql(f"INSERT OVERWRITE ice.perf.t{index} SELECT * FROM src").collect()
    elif CELL == "df_write_parquet_zstd":
        engine.sql("SELECT * FROM src").write.parquet(
            str(root / f"pq{index}"), mode="overwrite", compression="zstd"
        )
    else:
        raise SystemExit(f"unknown cell {CELL}")


load_start = os.getloadavg()[0]
setup(0)
run(0)
samples = []
for index in range(1, REPS + 1):
    setup(index)
    started = time.perf_counter()
    run(index)
    samples.append((time.perf_counter() - started) * 1000.0)
files = None
if CELL != "df_write_parquet_zstd":
    files = engine.sql(f"SELECT count(*) AS n FROM ice.perf.t{REPS}.files").to_arrow()
    files = files.column("n")[0].as_py()
payload = {
    "cell": f"iceberg_write/1000000/{CELL}",
    "threads": THREADS,
    "samples_ms": [round(v, 2) for v in samples],
    "median_ms": round(statistics.median(samples), 2),
    "min_ms": round(min(samples), 2),
    "spread_ms": round(max(samples) - min(samples), 2),
    "load1_start": round(load_start, 2),
    "load1_end": round(os.getloadavg()[0], 2),
    "files": files,
    "max_rss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
}
engine.stop()
shutil.rmtree(root, ignore_errors=True)
print(json.dumps(payload))

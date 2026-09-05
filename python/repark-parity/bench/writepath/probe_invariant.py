import json
import shutil
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark import ReparkSession

SIZES = [5000, 10000, 20000, 40000, 7000, 3000, 60000, 1000]
TOTAL = sum(SIZES)
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
PARTS = sys.argv[2] if len(sys.argv) > 2 else "4"
V3 = "'format-version' = '3'"

root = Path(tempfile.mkdtemp(prefix="repark-invariant-"))
source = root / "seed"
source.mkdir(parents=True)
start = 0
for index, size in enumerate(SIZES):
    pq.write_table(
        pa.table(
            {
                "id": pa.array(range(start, start + size), type=pa.int64()),
                "label": pa.array([f"r{v:07d}" for v in range(start, start + size)]),
            }
        ),
        source / f"part-{index}.parquet",
    )
    start += size

engine = (
    ReparkSession.builder.appName(f"invariant-{PARTS}")
    .config("spark.sql.shuffle.partitions", PARTS)
    .config("repark.sql.allowCreateFormatVersion3", "true")
    .config("repark.write.max-concurrent-files", "4")
    .getOrCreate()
)
engine.register_memory_catalog("ice", root / "wh")
engine.sql("CREATE NAMESPACE IF NOT EXISTS ice.i")
engine.read.parquet(str(source)).createOrReplaceTempView("src")

ok_sorted = ok_contig = ok_rows = 0
groupings = set()
for run in range(RUNS):
    table = f"ice.i.t{run}"
    engine.sql(f"DROP TABLE IF EXISTS {table}")
    engine.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({V3}) AS SELECT * FROM src"
    ).collect()
    files = engine.sql(
        f"SELECT record_count, first_row_id, readable_metrics FROM {table}.files"
    ).to_arrow()
    counts = [int(v.as_py()) for v in files.column("record_count")]
    firsts = [int(v.as_py()) for v in files.column("first_row_id")]
    lows = [v.as_py()["id"]["lower_bound"] for v in files.column("readable_metrics")]
    sorted_ok = lows == sorted(lows)
    contiguous = firsts == [sum(counts[:i]) for i in range(len(counts))] and sum(counts) == TOTAL
    total = engine.sql(f"SELECT count(*) AS n, sum(id) AS s FROM {table}").to_arrow()
    rows_ok = (
        total.column("n")[0].as_py() == TOTAL
        and total.column("s")[0].as_py() == TOTAL * (TOTAL - 1) // 2
    )
    ok_sorted += sorted_ok
    ok_contig += contiguous
    ok_rows += rows_ok
    groupings.add(json.dumps(counts))
    print(
        f"run {run}: files={len(counts)} lows_sorted={sorted_ok} "
        f"rowid_contiguous={contiguous} rows_ok={rows_ok} seq={counts}"
    )

engine.stop()
shutil.rmtree(root, ignore_errors=True)
print(
    f"PARTS={PARTS} runs={RUNS} distinct_groupings={len(groupings)} "
    f"lows_sorted={ok_sorted}/{RUNS} rowid_contiguous={ok_contig}/{RUNS} rows_ok={ok_rows}/{RUNS}"
)

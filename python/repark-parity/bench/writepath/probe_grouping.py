import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from repark import ReparkSession

SIZES = [5000, 10000, 20000, 40000, 7000, 3000, 60000, 1000]
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
PARTS = sys.argv[2] if len(sys.argv) > 2 else "4"
V3 = "'format-version' = '3'"


def seed(directory: Path) -> Path:
    """Write the eight unequal source files the grouping refutation runs on."""
    directory.mkdir(parents=True, exist_ok=True)
    start = 0
    for index, size in enumerate(SIZES):
        pq.write_table(
            pa.table(
                {
                    "id": pa.array(range(start, start + size), type=pa.int64()),
                    "part": pa.array([v % 8 for v in range(start, start + size)], pa.int32()),
                    "label": pa.array([f"r{v:07d}" for v in range(start, start + size)]),
                }
            ),
            directory / f"part-{index}.parquet",
        )
        start += size
    return directory


root = Path(tempfile.mkdtemp(prefix="repark-grouping-"))
source = seed(root / "seed")
engine = (
    ReparkSession.builder.appName(f"grouping-{PARTS}")
    .config("spark.sql.shuffle.partitions", PARTS)
    .config("repark.sql.allowCreateFormatVersion3", "true")
    .config("repark.write.max-concurrent-files", "4")
    .getOrCreate()
)
engine.register_memory_catalog("ice", root / "wh")
engine.sql("CREATE NAMESPACE IF NOT EXISTS ice.g")
engine.read.parquet(str(source)).createOrReplaceTempView("src")

sequences, lineages = [], []
for run in range(RUNS):
    engine.sql(f"DROP TABLE IF EXISTS ice.g.t{run}")
    engine.sql(
        f"CREATE TABLE ice.g.t{run} USING iceberg TBLPROPERTIES ({V3}) AS SELECT * FROM src"
    ).collect()
    files = engine.sql(f"SELECT record_count FROM ice.g.t{run}.files").to_arrow()
    sequences.append([int(v.as_py()) for v in files.column("record_count")])
    rows = engine.sql(f"SELECT id, _row_id FROM ice.g.t{run} ORDER BY id").to_arrow()
    pairs = list(
        zip(
            [int(v.as_py()) for v in rows.column("id")],
            [int(v.as_py()) for v in rows.column("_row_id")],
            strict=True,
        )
    )
    lineages.append(hashlib.sha256(json.dumps(pairs).encode()).hexdigest()[:16])
    identity = all(i == r for i, r in pairs)
    print(
        f"run {run}: files={len(sequences[-1])} seq={sequences[-1]} "
        f"lineage={lineages[-1]} rowid_eq_id={identity}"
    )

engine.stop()
shutil.rmtree(root, ignore_errors=True)
distinct_seq = {json.dumps(s) for s in sequences}
distinct_lin = set(lineages)
print(
    f"PARTS={PARTS} RUNS={RUNS} distinct_sequences={len(distinct_seq)} "
    f"distinct_lineage={len(distinct_lin)}"
)

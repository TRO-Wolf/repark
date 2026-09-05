import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

rows = int(float(sys.argv[1])) if len(sys.argv) > 1 else 1000000
out = sys.argv[2] if len(sys.argv) > 2 else f"scratch/synth_{rows}.parquet"
rng = np.random.default_rng(42)
table = pa.table(
    {
        "id": pa.array(np.arange(rows, dtype=np.int64)),
        "ts": pa.array(rng.integers(1_600_000_000, 1_700_000_000, rows, dtype=np.int64)),
        "v": pa.array(rng.random(rows)),
        "vi": pa.array(rng.integers(0, 1000, rows, dtype=np.int32)),
        "s": pa.array([f"s{i:015d}" for i in rng.integers(0, 10**14, rows)]),
        "cat": pa.array([f"c{i:02d}" for i in rng.integers(0, 100, rows)]),
        "part": pa.array(rng.integers(0, 8, rows, dtype=np.int32)),
    }
)
pq.write_table(table, out, compression="zstd", row_group_size=100000)
print(out, table.num_rows, table.num_columns)

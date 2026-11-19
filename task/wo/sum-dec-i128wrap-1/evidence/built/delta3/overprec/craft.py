import shutil
import struct
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(sys.argv[1])
A = 10**38 - 1
R = 2**128 + 5 - 3 * A
SETS = {"wrap5": [A, A, A, R], "neg": [-A, -A, -A, -R], "fit": [A, A, -A, 7], "filler": [0]}


def craft(path: Path, values: list[int], precision: int) -> None:
    table = pa.table(
        {
            "c": pa.array([Decimal(v) for v in values], pa.decimal128(38, 0)),
            "g": pa.array([1] * len(values), pa.int64()),
        }
    )
    pq.write_table(table, path, store_schema=False, write_statistics=False)
    data = bytearray(path.read_bytes())
    size = struct.unpack("<I", data[-8:-4])[0]
    start = len(data) - 8 - size
    footer = bytes(data[start:-8])
    patched = footer.replace(b"\x15\x4c", bytes([0x15, precision * 2]))
    assert len(patched) == len(footer)
    data[start:-8] = patched
    path.write_bytes(bytes(data))
    back = pq.read_table(path)
    print(
        path, back.schema.field("c").type, [int(x) for x in back.column("c").to_pylist()] == values
    )


for name, values in SETS.items():
    for precision in (19, 20):
        craft(OUT / f"{name}_p{precision}.parquet", values, precision)
for name in ("wrap5", "neg", "fit"):
    for precision in (19, 20):
        two = OUT / f"{name}_p{precision}_two"
        two.mkdir(parents=True, exist_ok=True)
        shutil.copy(OUT / f"{name}_p{precision}.parquet", two / "a.parquet")
        shutil.copy(OUT / f"filler_p{precision}.parquet", two / "b.parquet")

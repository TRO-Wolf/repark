import struct
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)


def d(bits: int) -> float:
    return struct.unpack(">d", struct.pack(">Q", bits))[0]


def f(bits: int) -> float:
    return struct.unpack(">f", struct.pack(">I", bits))[0]


NZ, PZ = d(0x8000000000000000), 0.0
NAN, NNAN, P1, P2 = (
    d(0x7FF8000000000000),
    d(0xFFF8000000000000),
    d(0x7FF8000000000001),
    d(0xFFF8000000000002),
)
FNZ, FNAN, FNNAN, FP1 = f(0x80000000), f(0x7FC00000), f(0xFFC00000), f(0x7FC00001)

rows_c = [NZ, PZ, NNAN, NAN, P1, 1.5]
rows_f = [FNZ, 0.0, FNNAN, FNAN, FP1, 1.5]
rows_k = [NZ, NZ, PZ, PZ, NNAN, NAN]
table = pa.table(
    {
        "i": pa.array(list(range(1, 7)), pa.int64()),
        "c": pa.array(rows_c, pa.float64()),
        "f": pa.array(rows_f, pa.float32()),
        "k": pa.array(rows_k, pa.float64()),
        "v": pa.array(["a", "b", "c", "d", "e", "g"]),
    }
)
pq.write_table(table, out / "r.parquet")

sets = [
    ("s1-two-canon", [NAN, NAN]),
    ("s2-canon-signed", [NAN, NNAN]),
    ("s3-payloads", [P1, P2, NAN]),
    ("s4-neg-first", [NZ, PZ]),
    ("s5-pos-first", [PZ, NZ]),
    ("s6-all", [NZ, PZ, NNAN, NAN, P1, 1.5]),
    ("s7-signed-twice", [NNAN, NNAN]),
]
sid, val = [], []
for name, values in sets:
    for value in values:
        sid.append(name)
        val.append(value)
cs = pa.table({"s": pa.array(sid), "x": pa.array(val, pa.float64())})
pq.write_table(cs, out / "cs.parquet")
parts = out / "cs4"
parts.mkdir(exist_ok=True)
for index in range(4):
    pq.write_table(cs.take(list(range(index, cs.num_rows, 4))), parts / f"part-{index}.parquet")
print("ok", table.num_rows, cs.num_rows)

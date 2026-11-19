import hashlib
import json
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "probes"))
import cells

M = "9" * 38
FABLE = [("11", "0", M), ("11", "1", "1"), ("11", "2", "-1"), ("12", "3", "5")]


def arrow_type(kind: str) -> pa.DataType:
    if kind == "INT":
        return pa.int32()
    p, s = kind[len("DECIMAL(") : -1].split(",")
    return pa.decimal128(int(p), int(s))


def convert(text: str | None, kind: str) -> object:
    if text is None:
        return None
    return int(text) if kind == "INT" else Decimal(text)


def write(root: Path, name: str, names: list, rows: list, types: dict, files: int) -> list:
    folder = root / name
    folder.mkdir(parents=True)
    schema = pa.schema([(n, arrow_type(types[n])) for n in names])
    written = []
    for k in range(files):
        part = [r for r in rows if files == 1 or int(r[names.index("f")]) == k]
        columns = [[convert(r[i], types[n]) for r in part] for i, n in enumerate(names)]
        path = folder / f"part-{k}.parquet"
        pq.write_table(pa.table(columns, schema=schema), path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
        written.append({"table": name, "file": path.name, "rows": len(part), "sha256": digest})
    return written


root = Path(sys.argv[1])
sn, sr = cells.s_rows()
p2_names, p2_data = cells.p2_rows()
record = write(root, "s", sn, sr, cells.s_types(), 1)
record += write(root, "s4", sn, sr, cells.s_types(), 4)
record += write(root, "p2", p2_names, p2_data, cells.p2_types(), 2)
fable_types = {"g": "INT", "f": "INT", "c": "DECIMAL(38,0)"}
record += write(root, "fq", ["g", "f", "c"], [list(r) for r in FABLE], fable_types, 4)
(root / "files.json").write_text(json.dumps(record, indent=1) + "\n")
print(json.dumps(record))

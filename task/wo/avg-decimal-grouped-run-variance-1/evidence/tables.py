import json
import shutil
import struct
import sys
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = [(10, 2), (18, 0), (19, 0), (20, 0), (28, 4), (38, 0)]
FILES = 4
FILLER = 300
BIG_ROWS, BIG_GROUPS = 300000, 3000
M38 = 10**38 - 1
SPLIT_SHAPES = {
    21: [["M"], ["1"], ["-1"]],
    22: [["M"], ["1"]],
    23: [["M", "5"], ["-M"]],
    24: [["M", "-M"], ["M", "-M", "5"]],
    25: [["M"], ["-M"], ["5"]],
}
OVER_A = 10**38 - 1
OVER_R = 2**128 + 5 - 3 * OVER_A
OVER_SETS = {
    "wrap5": [OVER_A, OVER_A, OVER_A, OVER_R],
    "neg": [-OVER_A, -OVER_A, -OVER_A, -OVER_R],
    "fit": [OVER_A, OVER_A, -OVER_A, 7],
}


def scenarios(m: int) -> dict[int, list[list[int | None]]]:
    return {
        1: [[m, m, m], [-m, -m, -m], [5], [m, -m]],
        2: [[m, m], [], [m], []],
        3: [[-m], [-m], [], [-m]],
        4: [[m], [0], [0], [0]],
        5: [[m], [1], [], []],
        6: [[m, m], [-m], [-m, 1], []],
        7: [[m] * 4, [m] * 4, [-m] * 8, []],
        8: [[-m] * 5, [m] * 5, [-3], []],
        9: [[m] * 7, [-m] * 6, [-m] * 1, [m // 2]],
        10: [[-m, m] * 20, [m, -m] * 20, [7, -m, m], [-7]],
        11: [[1, 2, 3], [-4], [5], [6, None]],
        12: [[None], [None], [], []],
    }


def scaled(value: int, s: int) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 200
        return Decimal(value).scaleb(-s)


def write_t(root: Path) -> list[dict[str, object]]:
    per_file: list[list[dict[str, object]]] = [[] for _ in range(FILES)]
    for g in scenarios(1):
        for k in range(FILES):
            for i in range(len(scenarios(1)[g][k])):
                row: dict[str, object] = {"g": g}
                for p, s in COLUMNS:
                    row[f"c{p}_{s}"] = scenarios(10**p - 1)[g][k][i]
                per_file[k].append(row)
    for k in range(FILES):
        for i in range(FILLER):
            row = {"g": 0} | {f"c{p}_{s}": (i % 7) - 3 for p, s in COLUMNS}
            per_file[k].append(row)
    rows: list[dict[str, object]] = []
    path = root / "t"
    path.mkdir(parents=True, exist_ok=True)
    next_id = 0
    for k in range(FILES):
        for row in per_file[k]:
            row["id"] = next_id
            row["f"] = k
            next_id += 1
            rows.append(row)
        cols = {
            "g": pa.array([r["g"] for r in per_file[k]], pa.int64()),
            "id": pa.array([r["id"] for r in per_file[k]], pa.int64()),
        }
        for p, s in COLUMNS:
            name = f"c{p}_{s}"
            vals = [None if r[name] is None else scaled(r[name], s) for r in per_file[k]]
            cols[name] = pa.array(vals, pa.decimal128(p, s))
        pq.write_table(pa.table(cols), path / f"part-{k}.parquet", row_group_size=64)
    return rows


def write_big(root: Path) -> None:
    path = root / "big"
    path.mkdir(parents=True, exist_ok=True)
    rounds = BIG_ROWS // BIG_GROUPS
    special = {"a": str(M38), "b": "5", "c": "-" + str(M38)}
    for k, (name, value) in enumerate(special.items()):
        g = [i % BIG_GROUPS for i in range(BIG_ROWS)]
        c = [
            value
            if (i // BIG_GROUPS) == ((i % BIG_GROUPS) * (k + 3) * 2654435761 >> 7) % rounds
            else "0"
            for i in range(BIG_ROWS)
        ]
        cast = pa.array(c, pa.string()).cast(pa.decimal128(38, 0))
        table = pa.table({"g": pa.array(g, pa.int32()), "c": cast})
        pq.write_table(table, path / f"{name}.parquet")


def write_t4(root: Path) -> list[dict[str, object]]:
    path = root / "t4"
    path.mkdir(parents=True, exist_ok=True)
    tokens = {"M": M38, "-M": -M38}
    rows: list[dict[str, object]] = []
    for part in range(3):
        mine = []
        for g, files in SPLIT_SHAPES.items():
            for token in files[part] if part < len(files) else []:
                mine.append({"g": g, "id": len(rows) + 100, "c38_0": tokens.get(token)})
                if mine[-1]["c38_0"] is None:
                    mine[-1]["c38_0"] = int(token)
                rows.append(mine[-1])
        cols = {
            "g": pa.array([r["g"] for r in mine], pa.int32()),
            "id": pa.array([r["id"] for r in mine], pa.int32()),
            "c38_0": pa.array([Decimal(r["c38_0"]) for r in mine], pa.decimal128(38, 0)),
        }
        pq.write_table(pa.table(cols), path / f"part-{part}.parquet")
    return rows


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
    data[start:-8] = patched
    path.write_bytes(bytes(data))


def write_over(root: Path, source: Path) -> None:
    path = root / "over"
    path.mkdir(parents=True, exist_ok=True)
    for name in OVER_SETS:
        for p in (19, 20):
            one = path / f"{name}_p{p}"
            one.mkdir(exist_ok=True)
            shutil.copy(source / f"{name}_p{p}.parquet", one / "a.parquet")
            two = path / f"{name}_p{p}_two"
            two.mkdir(exist_ok=True)
            shutil.copy(source / f"{name}_p{p}.parquet", two / "a.parquet")
            craft(two / "b.parquet", [0], p)


def main() -> None:
    root, source = Path(sys.argv[1]), Path(sys.argv[2])
    rows = write_t(root)
    write_big(root)
    t4 = write_t4(root)
    write_over(root, source)
    exact = {
        "columns": COLUMNS,
        "t": [{k: v for k, v in r.items() if k != "f"} for r in rows],
        "t4": t4,
        "over": {name: [str(v) for v in values] for name, values in OVER_SETS.items()},
    }
    (root / "exact.json").write_text(json.dumps(exact, default=str))
    print("tables", root, len(rows), len(t4), flush=True)


if __name__ == "__main__":
    main()

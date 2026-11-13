from __future__ import annotations

import json
import math
import os
import struct
import sys
from pathlib import Path
from typing import Any

CAT = "sc"
NS = "ns"
JAR = os.environ.get("ICEBERG_SPARK_JAR", "iceberg-spark-runtime-4.1_2.13-1.11.0.jar")

D_BITS = {
    "c": 0x7FF8000000000000,
    "s": 0xFFF8000000000000,
    "p": 0x7FF8DEADBEEF0001,
    "q": 0xFFF8DEADBEEF0001,
}
F_BITS = {"c": 0x7FC00000, "s": 0xFFC00000, "p": 0x7FC00123, "q": 0xFFC00123}
SHAPES = ("c", "s", "p", "q")


def d_from_bits(word: int) -> float:
    return struct.unpack("<d", struct.pack("<Q", word))[0]


def f_from_bits(word: int) -> float:
    return struct.unpack("<f", struct.pack("<I", word))[0]


CONTROLS = {
    1: 1.5,
    3: math.inf,
    4: -math.inf,
    5: -0.0,
    6: 0.0,
    7: None,
    8: -2.5,
}
NAN_IDS = (2, 9)
ALL_IDS = (1, 2, 3, 4, 5, 6, 7, 8, 9)
LAYOUTS = {
    "l1": [ALL_IDS],
    "l5": [(1, 8), (2, 5, 7), (3, 6), (9,), (4,)],
}
WIDE_COLUMNS = ("dc", "ds", "dp", "dq", "fc", "fs", "fp", "fq")
WIDE_DDL = "id BIGINT, " + ", ".join(
    f"{c} {'DOUBLE' if c[0] == 'd' else 'FLOAT'}" for c in WIDE_COLUMNS
)
PART_DDL = "id BIGINT, pk DOUBLE, v DOUBLE"
PART_ROWS = (
    (1, 1.5),
    (2, ("d", "c")),
    (3, ("d", "s")),
    (4, ("d", "p")),
    (5, math.inf),
    (6, -math.inf),
    (7, -0.0),
    (8, 0.0),
    (9, None),
)


def wide_value(row_id: int, column: str) -> float | None:
    if row_id in NAN_IDS:
        word = (D_BITS if column[0] == "d" else F_BITS)[column[1]]
        return d_from_bits(word) if column[0] == "d" else f_from_bits(word)
    return CONTROLS[row_id]


def wide_rows(ids: tuple[int, ...]) -> list[tuple[Any, ...]]:
    return [(i, *[wide_value(i, c) for c in WIDE_COLUMNS]) for i in ids]


def part_rows() -> list[tuple[Any, ...]]:
    out = []
    for row_id, value in PART_ROWS:
        if isinstance(value, tuple):
            value = d_from_bits(D_BITS[value[1]])
        out.append((row_id, value, value))
    return out


def arrow_wide(ids: tuple[int, ...]) -> Any:
    import pyarrow as pa

    rows = wide_rows(ids)
    arrays = [pa.array([r[0] for r in rows], pa.int64())]
    for index, column in enumerate(WIDE_COLUMNS, start=1):
        kind = pa.float64() if column[0] == "d" else pa.float32()
        arrays.append(pa.array([r[index] for r in rows], kind))
    return pa.table(arrays, names=["id", *WIDE_COLUMNS])


def arrow_part() -> Any:
    import pyarrow as pa

    rows = part_rows()
    return pa.table(
        [
            pa.array([r[0] for r in rows], pa.int64()),
            pa.array([r[1] for r in rows], pa.float64()),
            pa.array([r[2] for r in rows], pa.float64()),
        ],
        names=["id", "pk", "v"],
    )


def token(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if value == 0.0:
            return "-0.0" if math.copysign(1.0, value) < 0 else "0.0"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return repr(value)
    if isinstance(value, list):
        return [token(v) for v in value]
    if isinstance(value, dict):
        return {k: token(v) for k, v in value.items()}
    return value


def nan_bits(array: Any) -> list[str]:
    import pyarrow as pa

    kind = array.type
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        return nan_bits(array.flatten())
    if pa.types.is_float64(kind):
        view = array.view(pa.uint64()).to_pylist()
        width = 16
    elif pa.types.is_float32(kind):
        view = array.view(pa.uint32()).to_pylist()
        width = 8
    else:
        return []
    values = array.to_pylist()
    return [f"{w:0{width}x}" for v, w in zip(values, view, strict=True) if v is not None and v != v]


def collect(frame: Any) -> dict[str, Any]:
    table = frame.toArrow()
    table = table.combine_chunks()
    rows = [[token(v) for v in row.values()] for row in table.to_pylist()]
    bits: list[str] = []
    for i in range(table.num_columns):
        column = table.column(i)
        for chunk in column.chunks:
            bits.extend(nan_bits(chunk))
    return {"rows": rows, "nan_bits": sorted(set(bits))}


def attempt(action: Any) -> dict[str, Any]:
    try:
        return {"ok": True, **action()}
    except Exception as error:
        text = f"{type(error).__name__}: {error!s}"
        return {"ok": False, "error": text.splitlines()[0][:240] if text else ""}


def header(engine: str) -> dict[str, Any]:
    info: dict[str, Any] = {"record": "leg", "engine": engine, "python": sys.version.split()[0]}
    if engine == "repark":
        import repark

        info["native_file"] = repark._native.__file__
        built = Path(repark.__file__).parent.parent / "BUILT_FROM"
        info["built_from"] = built.read_text().strip() if built.exists() else "nwc/pkg-base"
    return info


def dump(sink: Any, record: dict[str, Any]) -> None:
    sink.write(json.dumps(record, sort_keys=True) + "\n")
    sink.flush()

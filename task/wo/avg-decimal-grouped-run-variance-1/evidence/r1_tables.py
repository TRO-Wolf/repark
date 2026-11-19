import sys
from decimal import Context, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

M38 = 10**38 - 1
M18 = 10**18 - 1
M9 = 999999999
SPILL_GROUPS = 200_000


def dec(unscaled: int, scale: int) -> Decimal:
    return Decimal(unscaled).scaleb(-scale, context=Context(prec=100))


Rows = list[list[tuple[int, int, int]]]


def write(root: Path, name: str, parts: Rows, ctype: pa.DataType) -> None:
    folder = root / name
    folder.mkdir(parents=True, exist_ok=True)
    scale = ctype.scale
    for i, rows in enumerate(parts):
        table = pa.table(
            {
                "g": pa.array([r[0] for r in rows], pa.int32()),
                "id": pa.array([r[1] for r in rows], pa.int32()),
                "c": pa.array([dec(r[2], scale) for r in rows], ctype),
            }
        )
        pq.write_table(table, folder / f"part-{i}.parquet", row_group_size=64)


def even(files: int, rows: int, value: int) -> Rows:
    return [[(1, f * rows + i, value) for i in range(rows)] for f in range(files)]


def class_tables(root: Path) -> None:
    write(root, "m_w34", even(4, 5000, 9 * 10**33), pa.decimal128(38, 0))
    write(root, "m_n33", even(4, 10000, -3 * 10**33), pa.decimal128(38, 0))
    write(root, "m_s2", even(4, 5000, 9 * 10**33), pa.decimal128(38, 2))
    mix = [
        [(1, f * 5000 + i, 9 * 10**33 if i % 2 == 0 else 7 * 10**33) for i in range(5000)]
        for f in range(4)
    ]
    write(root, "m_mix", mix, pa.decimal128(38, 0))
    write(root, "m_w8", even(8, 2500, 9 * 10**33), pa.decimal128(38, 0))


def narrow_tables(root: Path) -> None:
    d32 = [
        [(1, 0, M9), (1, 1, M9), (1, 2, M9)],
        [(1, 3, -M9), (1, 4, -M9), (1, 5, -M9)],
        [(1, 6, 500)],
        [(1, 7, 0)],
    ]
    write(root, "m_d32", d32, pa.decimal32(9, 2))
    d64 = [
        [(1, i, M18) for i in range(10)],
        [(1, 10 + i, -M18) for i in range(10)],
        [(1, 20, 5)],
    ]
    write(root, "m_d64", d64, pa.decimal64(18, 0))
    big = 2**256
    v = big // 12
    w = big + 5 - 11 * v
    d256 = [[(1, i, v) for i in range(6)], [(1, 6 + i, v) for i in range(5)] + [(1, 11, w)]]
    write(root, "m_d256", d256, pa.decimal256(76, 0))


def slide_table(root: Path) -> None:
    parts = [
        [(1, i, M38 if i % 4 < 2 else -M38) for i in range(f * 8, f * 8 + 8)] + [(1, 100 + f, 5)]
        for f in range(4)
    ]
    write(root, "m_slide", parts, pa.decimal128(38, 0))


def spill_table(root: Path) -> None:
    groups: list[int] = []
    values: list[Decimal] = []
    for g in range(SPILL_GROUPS):
        groups += [g, g]
        values += [dec(M38, 0), dec(M38, 0)]
    for g in range(SPILL_GROUPS):
        groups += [g, g, g]
        values += [dec(-M38, 0), dec(-M38, 0), dec(5, 0)]
    folder = root / "sp"
    folder.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {"g": pa.array(groups, pa.int32()), "c": pa.array(values, pa.decimal128(38, 0))}
    )
    pq.write_table(table, folder / "part-0.parquet", row_group_size=65536)


def main() -> None:
    root = Path(sys.argv[1])
    class_tables(root)
    narrow_tables(root)
    slide_table(root)
    spill_table(root)
    print("written", sorted(p.name for p in root.iterdir()))


if __name__ == "__main__":
    main()

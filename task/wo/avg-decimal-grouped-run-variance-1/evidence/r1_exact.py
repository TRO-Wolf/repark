import json
import sys
from collections import defaultdict
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow.parquet as pq
import r1_cells

AO = "ERR ARITHMETIC_OVERFLOW"
NVOOR = "ERR NUMERIC_VALUE_OUT_OF_RANGE"
M38 = 10**38 - 1
Row = tuple[int, int, int | None]


def unscaled(value: Decimal, scale: int) -> int:
    with localcontext() as ctx:
        ctx.prec = 200
        return int(value.scaleb(scale))


def scaled(value: int, scale: int) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 200
        return Decimal(value).scaleb(-scale)


def load(folder: Path, column: str) -> tuple[list[Row], int, int]:
    rows: list[Row] = []
    precision = scale = 0
    for path in sorted(folder.glob("*.parquet")):
        table = pq.read_table(path)
        kind = table.schema.field(column).type
        precision, scale = kind.precision, kind.scale
        for r in table.to_pylist():
            value = r[column]
            rows.append((r["g"], r.get("id", 0), None if value is None else unscaled(value, scale)))
    return rows, precision, scale


def bounds(precision: int, scale: int) -> tuple[int, int, int]:
    top = 76 if precision > 38 else 38
    return min(precision + 10, top), min(precision + 4, top), min(scale + 4, top)


def average(values: list[int | None], p: int, s: int, raises: bool, window: bool) -> object:
    present = [v for v in values if v is not None]
    if not present:
        return None
    total = sum(present)
    sum_p, result_p, result_s = bounds(p, s)
    if not window and abs(total) >= 10**sum_p:
        return AO if raises else None
    numerator = abs(total) * 10 ** (result_s - s)
    quotient, rest = divmod(numerator, len(present))
    quotient += 1 if 2 * rest >= len(present) else 0
    if abs(quotient) >= 10**result_p:
        return NVOOR if raises else None
    return scaled(quotient if total >= 0 else -quotient, result_s)


def is_error(value: object) -> bool:
    return isinstance(value, str) and value.startswith("ERR")


def answer(rows: list[tuple[object, ...]]) -> str:
    errors = sorted({v for r in rows for v in r if is_error(v)})
    if errors:
        return json.dumps(errors[0] if len(errors) == 1 else " | ".join(errors))
    return json.dumps(r1_cells.shape(rows))


def by_group(rows: list[Row]) -> dict[int, list[int | None]]:
    out: dict[int, list[int | None]] = defaultdict(list)
    for g, _, value in rows:
        out[g].append(value)
    return out


def table_cells(name: str, rows: list[Row], p: int, s: int, where: str, ansi: bool) -> dict:
    limit = int(where.split("<")[1]) if where else None
    keep = [r for r in rows if limit is None or r[1] < limit]
    tag = "_M" if where else ""
    groups = by_group(keep)
    out = {}
    for fn in r1_cells.FNS:
        raises = ansi and fn == "avg"
        out[f"{name}/u/{fn}{tag}"] = answer([(average([r[2] for r in keep], p, s, raises, False),)])
        out[f"{name}/g/{fn}{tag}"] = answer(
            [(g, average(v, p, s, raises, False)) for g, v in groups.items()]
        )
    out[f"{name}/win/avg{tag}"] = answer(
        [(g, average(v, p, s, ansi, True)) for g, v in groups.items()]
    )
    return out


def step0_cells(step0: Path, ansi: bool) -> dict:
    out = {}
    t, p, s = load(step0 / "t", "c38_0")
    groups = by_group(t)
    for k in (2, 3, 5):
        value = average(groups[k], p, s, ansi, True)
        out[f"t/c38_0/win/avg/g{k}"] = answer([(k, value)] * len(groups[k]))
    for fn in r1_cells.FNS:
        raises = ansi and fn == "avg"
        out[f"t/c38_0/g/{fn}/g6"] = answer([(6, average(groups[6], p, s, raises, False))])
        out[f"t/c38_0/u/{fn}/g6"] = answer([(average(groups[6], p, s, raises, False),)])
    t4, p, s = load(step0 / "t4", "c38_0")
    t4_groups = by_group(t4)
    for fn in r1_cells.FNS:
        raises = ansi and fn == "avg"
        out[f"t4/c38_0/g/{fn}/g21"] = answer([(21, average(t4_groups[21], p, s, raises, False))])
    out["t4/corr/avg"] = answer([(g, average(v, p, s, ansi, False)) for g, v in t4_groups.items()])
    out |= iceberg_cells(t4_groups, p, s, ansi)
    big, p, s = load(step0 / "big", "c")
    big_groups = by_group(big)
    per_group = [(g, average(v, p, s, ansi, False)) for g, v in big_groups.items()]
    whole = average([r[2] for r in big], p, s, ansi, False)
    out["big/gsets/avg"] = answer([*per_group, (None, whole)])
    out |= count_cells("big", big_groups, p, s, ansi)
    out["big/table/g/avg"] = answer(per_group)
    out["big/table/g/try_avg"] = answer(
        [(g, average(v, p, s, False, False)) for g, v in big_groups.items()]
    )
    out["big/table/u/avg"] = answer([(whole,)])
    out["t4/table/g/avg"] = answer(
        [(g, average(v, p, s, ansi, False)) for g, v in t4_groups.items()]
    )
    return out


def iceberg_cells(groups: dict[int, list[int | None]], p: int, s: int, ansi: bool) -> dict:
    merged = dict(groups) | {26: [M38, -M38, 5]}
    rows = [(g, average(v, p, s, ansi, False)) for g, v in merged.items()]
    try_rows = [(g, average(v, p, s, False, False)) for g, v in merged.items()]
    return {
        "ice/sql/g/avg": answer(rows),
        "ice/table/g/avg": answer(rows),
        "ice/sql/g/try_avg": answer(try_rows),
        "ice/sql/u26/avg": answer([(average(merged[26], p, s, ansi, False),)]),
    }


def count_cells(name: str, groups: dict, p: int, s: int, ansi: bool) -> dict:
    out = {}
    for fn in r1_cells.FNS:
        raises = ansi and fn == "avg"
        values = [average(v, p, s, raises, False) for v in groups.values()]
        errors = [v for v in values if is_error(v)]
        if errors:
            out[f"{name}/g/{fn}/count"] = json.dumps(errors[0])
            continue
        present = [v for v in values if v is not None]
        row = (len(values), len(present), min(present, default=None), max(present, default=None))
        out[f"{name}/g/{fn}/count"] = answer([row])
    return out


def frame_cells(mine: Path) -> dict:
    out = {}
    for name in ("m_w34", "m_n33"):
        rows, p, s = load(mine / name, "c")
        values = [r[2] for r in rows]
        mean = average(values, p, s, False, True)
        for shape in ("run", "slide15k"):
            for fn in r1_cells.FNS:
                out[f"{name}/{shape}/{fn}"] = answer([(len(rows), len(rows), mean, mean)])
    return out


def main() -> None:
    step0, mine, out_path = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    exact: dict[str, dict[str, str]] = {}
    for ansi in ("false", "true"):
        on = ansi == "true"
        cells: dict[str, str] = {}
        for name in (*r1_cells.CLASS, "m_d256", *r1_cells.NARROW):
            rows, p, s = load(mine / name, "c")
            cells |= table_cells(name, rows, p, s, "", on)
            if name in r1_cells.NARROW:
                cells |= table_cells(name, rows, p, s, r1_cells.NARROW[name], on)
        rows, p, s = load(mine / "sp", "c")
        cells |= count_cells("sp", by_group(rows), p, s, on)
        cells |= step0_cells(step0, on)
        cells |= frame_cells(mine)
        exact[ansi] = cells
    out_path.write_text(json.dumps(exact, indent=1, sort_keys=True))
    print("cells", len(exact["false"]), len(exact["true"]))


if __name__ == "__main__":
    main()

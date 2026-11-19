import json
from decimal import Decimal, localcontext
from pathlib import Path

import cells

M38 = 10**38 - 1
BIG_GROUPS = 3000


def half_up(a: int, b: int) -> int:
    sign = -1 if (a < 0) != (b < 0) else 1
    return sign * ((2 * abs(a) + abs(b)) // (2 * abs(b)))


def scaled(value: int, s: int) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 200
        return Decimal(value).scaleb(-s)


class OverflowAnswerError(Exception):
    pass


def avg_value(values: list[int], p: int, s: int, raise_on: bool) -> Decimal | None:
    kept = [v for v in values if v is not None]
    if not kept:
        return None
    total, n = sum(kept), len(kept)
    if abs(total) >= 10 ** min(38, p + 10):
        if raise_on:
            raise OverflowAnswerError("ARITHMETIC_OVERFLOW")
        return None
    rs, rp = min(s + 4, 38), min(p + 4, 38)
    q = half_up(total * 10 ** (rs - s), n)
    if abs(q) >= 10**rp:
        if raise_on:
            raise OverflowAnswerError("NUMERIC_VALUE_OUT_OF_RANGE")
        return None
    return scaled(q, rs)


def sum_value(values: list[int], p: int, s: int, raise_on: bool) -> Decimal | None:
    kept = [v for v in values if v is not None]
    if not kept:
        return None
    total = sum(kept)
    if abs(total) >= 10 ** min(38, p + 10):
        if raise_on:
            raise OverflowAnswerError("ARITHMETIC_OVERFLOW")
        return None
    return scaled(total, s)


def value(fn: str, values: list[int], p: int, s: int, ansi: str) -> Decimal | None:
    raise_on = ansi == "true" and fn in ("avg", "sum")
    if fn in ("avg", "try_avg"):
        return avg_value(values, p, s, raise_on)
    return sum_value(values, p, s, raise_on)


class Model:
    def __init__(self, root: Path) -> None:
        exact = json.loads((root / "exact.json").read_text())
        self.t = exact["t"]
        self.t4 = exact["t4"]
        self.over = {k: [int(v) for v in vs] for k, vs in exact["over"].items()}
        self.memo: dict[tuple[str, str], object] = {}

    def rows_of(self, table: str, c: str) -> list[dict[str, object]]:
        if table == "t":
            return [{"g": r["g"], "id": r["id"], "v": r[c]} for r in self.t]
        if table == "t4":
            return [{"g": r["g"], "id": r["id"], "v": r["c38_0"]} for r in self.t4]
        if table == "big":
            out: list[dict[str, object]] = []
            for g in range(BIG_GROUPS):
                out.extend({"g": g, "id": 0, "v": v} for v in (M38, 5, -M38))
                out.extend({"g": g, "id": 0, "v": 0} for _ in range(297))
            return out
        stem, p_text = table.split("_p")
        values = list(self.over[stem]) + ([0] if p_text.endswith("_two") else [])
        return [{"g": 1, "id": i, "v": v} for i, v in enumerate(values)]

    def precision(self, table: str, c: str) -> tuple[int, int]:
        if table in ("t", "t4"):
            p, s = c[1:].split("_")
            return int(p), int(s)
        if table == "big":
            return 38, 0
        return int(table.split("_p")[1][:2]), 0

    def answer(self, key: str, ansi: str) -> object:
        if (key, ansi) not in self.memo:
            self.memo[(key, ansi)] = self.compute(key, ansi)
        return self.memo[(key, ansi)]

    def compute(self, key: str, ansi: str) -> object:
        table, c, family, fn, scope = key.split("/")
        if table == "over":
            table = c
        if family == "ice":
            inner = self.answer(f"{table}/{c}/g/avg/all", ansi)
            if isinstance(inner, str):
                return {"statement": inner, "rows": "ERR"}
            return {"statement": "ok", "rows": inner}
        rows = self.rows_of(table, c)
        p, s = self.precision(table, c)
        if scope != "all":
            rows = [r for r in rows if r["g"] == int(scope[1:])]
        try:
            over = table not in ("t", "t4", "big")
            return cells.shape(self.rows(family, fn, rows, p, s, ansi, over))
        except OverflowAnswerError as error:
            return "ERR " + str(error)

    def rows(
        self,
        family: str,
        fn: str,
        rows: list[dict[str, object]],
        p: int,
        s: int,
        ansi: str,
        over: bool,
    ) -> list[tuple[object, ...]]:
        per: dict[object, list[dict[str, object]]] = {}
        for r in rows:
            per.setdefault(r["g"], []).append(r)
        groups = sorted(per)

        def of(sel: list[dict[str, object]]) -> Decimal | None:
            return value(fn, [r["v"] for r in sel], p, s, ansi)

        if family == "u":
            return [(of(rows),)]
        if family in ("g", "expr"):
            out = [(g, of(per[g])) for g in groups]
            if family == "expr":
                out = [(g, None if v is None else v.copy_abs()) for g, v in out]
            return out
        if family == "having":
            return [(g, v) for g, v in ((g, of(per[g])) for g in groups) if v is not None and v > 0]
        if family == "rollup":
            return [(g, of(per[g])) for g in groups] + [(None, of(rows))]
        if family == "filter":
            return [(g, of([r for r in per[g] if r["id"] % 2 == 0])) for g in groups]
        if family == "distinct":
            seen = sorted({r["v"] for r in rows if r["v"] is not None})
            return [(value(fn, seen, p, s, ansi),)]
        if family == "win":
            return [(r["g"], of(per[r["g"]])) for r in rows]
        if family == "slide" and over:
            ordered = sorted(rows, key=lambda r: r["v"])
            return [(r["g"], of(ordered[max(0, i - 1) : i + 1])) for i, r in enumerate(ordered)]
        if family == "slide":
            out = []
            for g in groups:
                ordered = sorted(per[g], key=lambda r: r["id"])
                for i, r in enumerate(ordered):
                    out.append((r["id"], of(ordered[max(0, i - 2) : i + 1])))
            return out
        raise ValueError(family)

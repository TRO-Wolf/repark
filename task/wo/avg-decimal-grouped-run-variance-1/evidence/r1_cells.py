import hashlib
import json
import re
from collections import Counter
from collections.abc import Callable
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

CONDITION = re.compile(r"\[([A-Z][A-Z_]+)")
FULL = 40
STEP0 = ("t", "big", "t4")
CLASS = ("m_w34", "m_n33", "m_s2", "m_mix", "m_w8")
MINE = (*CLASS, "m_d32", "m_d64", "m_d256", "m_slide", "sp")
NARROW = {"m_d32": "id < 3", "m_d64": "id < 10"}
FNS = ("avg", "try_avg")


def canon(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        with localcontext() as ctx:
            ctx.prec = 200
            text = format(value.normalize(), "f")
        return "0" if text in ("-0", "0") else text
    return str(value)


def shape(rows: list[tuple[object, ...]]) -> object:
    got = sorted((tuple(canon(v) for v in row) for row in rows), key=repr)
    if len(got) <= FULL:
        return [list(r) for r in got]
    nulls = sum(1 for r in got if r[-1] is None)
    digest = hashlib.sha1(repr(got).encode()).hexdigest()[:10]
    top = Counter(r[-1] for r in got).most_common(2)
    return {"rows": len(got), "nulls": nulls, "sha": digest, "top": top}


def label(error: Exception) -> str:
    text = " ".join(str(error).split())
    found = CONDITION.search(text)
    return "ERR " + (found.group(1) if found else text[:90])


def count(run: Callable[[], list[Any]], runs: int) -> dict[str, int]:
    seen: Counter[str] = Counter()
    for _ in range(runs):
        try:
            seen[json.dumps(shape([tuple(r) for r in run()]))] += 1
        except Exception as error:
            seen[json.dumps(label(error))] += 1
    return dict(seen)


def register(session: Any, step0: Path, mine: Path) -> dict[str, str]:
    refused: dict[str, str] = {}
    folders = [step0 / name for name in STEP0] + [mine / name for name in MINE]
    for folder in folders:
        try:
            session.read.parquet(str(folder)).createOrReplaceTempView(folder.name)
        except Exception as error:
            refused[folder.name] = label(error)
    return refused


def family(tb: str, where: str = "") -> dict[str, str]:
    cond = f" WHERE {where}" if where else ""
    tag = "_M" if where else ""
    out: dict[str, str] = {}
    for fn in FNS:
        out[f"{tb}/u/{fn}{tag}"] = f"SELECT {fn}(c) FROM {tb}{cond}"
        out[f"{tb}/g/{fn}{tag}"] = f"SELECT g, {fn}(c) FROM {tb}{cond} GROUP BY g"
    out[f"{tb}/win/avg{tag}"] = f"SELECT DISTINCT g, avg(c) OVER (PARTITION BY g) FROM {tb}{cond}"
    return out


def t_window(fn: str) -> dict[str, str]:
    return {
        f"t/c38_0/win/{fn}/g{k}": (
            f"SELECT g, {fn}(c38_0) OVER (PARTITION BY g) FROM t WHERE g = {k}"
        )
        for k in (2, 3, 5)
    }


def shared() -> dict[str, str]:
    out: dict[str, str] = {}
    for tb in (*CLASS, "m_d256"):
        out |= family(tb)
    for tb, where in NARROW.items():
        out |= family(tb) | family(tb, where)
        out[f"{tb}/type"] = f"SELECT typeof(avg(c)) FROM {tb}"
    out |= t_window("avg")
    out["big/gsets/avg"] = "SELECT g, avg(c) FROM big GROUP BY GROUPING SETS ((g), ())"
    out["t4/corr/avg"] = (
        "SELECT o.g, (SELECT avg(i.c38_0) FROM t4 i WHERE i.g = o.g) AS a "
        "FROM (SELECT DISTINCT g FROM t4) o"
    )
    return out


def repark_main() -> dict[str, str]:
    out = shared()
    for fn in FNS:
        out[f"t/c38_0/g/{fn}/g6"] = f"SELECT g, {fn}(c38_0) FROM t WHERE g = 6 GROUP BY g"
        out[f"t/c38_0/u/{fn}/g6"] = f"SELECT {fn}(c38_0) FROM t WHERE g = 6"
        out[f"t4/c38_0/g/{fn}/g21"] = f"SELECT g, {fn}(c38_0) FROM t4 WHERE g = 21 GROUP BY g"
    return out


def sums() -> dict[str, str]:
    out = t_window("sum")
    out["m_w34/win/sum"] = "SELECT DISTINCT g, sum(c) OVER (PARTITION BY g) FROM m_w34"
    out["m_w34/u/sum"] = "SELECT sum(c) FROM m_w34"
    out["m_w34/g/sum"] = "SELECT g, sum(c) FROM m_w34 GROUP BY g"
    return out


def spark_only() -> dict[str, str]:
    out = shared() | sums()
    out["big/c/u/avg/all"] = "SELECT avg(c) FROM big"
    out["big/c/g/avg/count"] = (
        "SELECT count(*), count(a) FROM (SELECT g, avg(c) AS a FROM big GROUP BY g)"
    )
    out["m_slide/g/avg"] = "SELECT g, avg(c) FROM m_slide GROUP BY g"
    out["m_slide/g/avg_objhash"] = (
        "SELECT g, avg(c), size(collect_list(id)) FROM m_slide GROUP BY g"
    )
    out["t4/g/avg"] = "SELECT g, avg(c38_0) FROM t4 GROUP BY g"
    out["t4/g/avg_objhash"] = "SELECT g, avg(c38_0), size(collect_list(id)) FROM t4 GROUP BY g"
    out["big/g/avg_objhash"] = (
        "SELECT count(*), count(a) FROM "
        "(SELECT g, avg(c) AS a, size(collect_list(c)) AS n FROM big GROUP BY g)"
    )
    out["m_w34/u/avg_objhash"] = "SELECT avg(c), size(collect_list(id)) FROM m_w34"
    out["big/u/avg_objhash"] = "SELECT avg(c), size(collect_list(id)) FROM big"
    return out


def grouped_counts(tb: str) -> dict[str, str]:
    return {
        f"{tb}/g/{fn}/count": (
            "SELECT count(*), count(a), min(a), max(a) "
            f"FROM (SELECT g, {fn}(c) AS a FROM {tb} GROUP BY g)"
        )
        for fn in FNS
    }


def frames() -> dict[str, str]:
    out: dict[str, str] = {}
    shapes = {"run": "UNBOUNDED PRECEDING", "slide15k": "14999 PRECEDING"}
    for tb in ("m_w34", "m_n33"):
        for name, start in shapes.items():
            for fn in FNS:
                frame = f"ROWS BETWEEN {start} AND CURRENT ROW"
                window = f"{fn}(c) OVER (PARTITION BY g ORDER BY id {frame})"
                out[f"{tb}/{name}/{fn}"] = (
                    "SELECT count(*), count(a), min(a), max(a) "
                    f"FROM (SELECT {window} AS a FROM {tb})"
                )
    return out


SETS: dict[str, Callable[[], dict[str, str]]] = {
    "frames": frames,
    "frames_w34": lambda: {k: v for k, v in frames().items() if k.startswith("m_w34/")},
    "main": repark_main,
    "sum": sums,
    "spark": spark_only,
    "spill": lambda: grouped_counts("sp"),
    "skip": lambda: grouped_counts("big"),
}

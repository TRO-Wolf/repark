import hashlib
import json
import re
import time
from collections import Counter
from collections.abc import Callable
from decimal import Decimal, localcontext
from typing import Any

COLUMNS = [(10, 2), (18, 0), (19, 0), (20, 0), (28, 4), (38, 0)]
GROUPS = range(13)
T4_GROUPS = (21, 22, 23, 24, 25)
OVER = [f"{n}_p{p}{t}" for n in ("wrap5", "neg", "fit") for p in (19, 20) for t in ("", "_two")]
CONDITION = re.compile(r"\[([A-Z][A-Z_]+)")
FULL = 40


def col(p: int, s: int) -> str:
    return f"c{p}_{s}"


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


def error_label(error: Exception) -> str:
    text = " ".join(str(error).split())
    found = CONDITION.search(text)
    if found:
        return "ERR " + found.group(1)
    return "ERR " + text[:90]


def attempt(run: Callable[[], list[Any]]) -> object:
    try:
        return shape([tuple(r) for r in run()])
    except Exception as error:
        return error_label(error)


def grouped_family(c: str, fns: tuple[str, ...]) -> dict[str, str]:
    out: dict[str, str] = {}
    for fn in fns:
        out[f"t/{c}/u/{fn}/all"] = f"SELECT {fn}({c}) FROM t"
        out[f"t/{c}/g/{fn}/all"] = f"SELECT g, {fn}({c}) FROM t GROUP BY g"
        for k in GROUPS:
            out[f"t/{c}/u/{fn}/g{k}"] = f"SELECT {fn}({c}) FROM t WHERE g = {k}"
            out[f"t/{c}/g/{fn}/g{k}"] = f"SELECT g, {fn}({c}) FROM t WHERE g = {k} GROUP BY g"
    return out


def queries(twins: bool) -> dict[str, str]:
    out: dict[str, str] = {}
    for p, s in COLUMNS:
        c = col(p, s)
        out.update(grouped_family(c, ("avg", "try_avg")))
        if twins:
            out.update(grouped_family(c, ("sum", "try_sum")))
        out[f"t/{c}/having/avg/all"] = f"SELECT g, avg({c}) FROM t GROUP BY g HAVING avg({c}) > 0"
        out[f"t/{c}/rollup/avg/all"] = f"SELECT g, avg({c}) FROM t GROUP BY ROLLUP(g)"
        out[f"t/{c}/filter/avg/all"] = (
            f"SELECT g, avg({c}) FILTER (WHERE id % 2 = 0) FROM t GROUP BY g"
        )
        for k in GROUPS:
            where = f"FROM t WHERE g = {k}"
            out[f"t/{c}/expr/avg/g{k}"] = f"SELECT g, abs(avg({c})) {where} GROUP BY g"
            out[f"t/{c}/win/avg/g{k}"] = f"SELECT g, avg({c}) OVER (PARTITION BY g) {where}"
            out[f"t/{c}/slide/avg/g{k}"] = (
                f"SELECT id, avg({c}) OVER (PARTITION BY g ORDER BY id "
                f"ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) {where}"
            )
            out[f"t/{c}/distinct/avg/g{k}"] = f"SELECT avg(DISTINCT {c}) {where}"
    fns = ("avg", "try_avg", "sum", "try_sum") if twins else ("avg", "try_avg")
    for fn in fns:
        out[f"big/c/g/{fn}/all"] = f"SELECT g, {fn}(c) FROM big GROUP BY g"
        out[f"big/c/u/{fn}/all"] = f"SELECT {fn}(c) FROM big"
        out[f"big/c/u/{fn}/g7"] = f"SELECT {fn}(c) FROM big WHERE g = 7"
        for k in T4_GROUPS:
            where = f"FROM t4 WHERE g = {k}"
            out[f"t4/c38_0/g/{fn}/g{k}"] = f"SELECT g, {fn}(c38_0) {where} GROUP BY g"
            out[f"t4/c38_0/u/{fn}/g{k}"] = f"SELECT {fn}(c38_0) {where}"
        for name in OVER:
            view = "o_" + name
            out[f"over/{name}/u/{fn}/all"] = f"SELECT {fn}(c) FROM {view}"
            out[f"over/{name}/g/{fn}/all"] = f"SELECT g, {fn}(c) FROM {view} GROUP BY g"
    for name in OVER:
        view = "o_" + name
        out[f"over/{name}/win/avg/all"] = f"SELECT g, avg(c) OVER (PARTITION BY g) FROM {view}"
        out[f"over/{name}/slide/avg/all"] = (
            f"SELECT g, avg(c) OVER (PARTITION BY g ORDER BY c "
            f"ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {view}"
        )
    return out


def register(session: Any, root: str) -> None:
    session.read.parquet(f"{root}/t").createOrReplaceTempView("t")
    session.read.parquet(f"{root}/big").createOrReplaceTempView("big")
    session.read.parquet(f"{root}/t4").createOrReplaceTempView("t4")
    for name in OVER:
        try:
            session.read.parquet(f"{root}/over/{name}").createOrReplaceTempView("o_" + name)
        except Exception as error:
            print("over view", name, error_label(error), flush=True)


def iceberg(session: Any, catalog: str, c: str, tag: str) -> object:
    name = f"{catalog}.avgns.ice_{c}_{tag}"
    statement = f"CREATE TABLE {name} USING iceberg AS SELECT g, avg({c}) AS m FROM t GROUP BY g"
    try:
        session.sql(statement).collect()
        outcome = "ok"
    except Exception as error:
        outcome = error_label(error)
    rows = attempt(lambda: session.sql(f"SELECT g, m FROM {name}").collect())
    return {"statement": outcome, "rows": rows}


def selected(key: str, only: str) -> bool:
    if only == "all":
        return True
    if only == "twins":
        return "/sum/" in key or "/try_sum/" in key
    return any(key.startswith(prefix) for prefix in only.split(","))


def measure(
    session: Any, catalog: str, runs: int, twins: bool, record: Callable, only: str = "all"
) -> None:
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.avgns").collect()
    for key, sql in queries(twins).items():
        if not selected(key, only):
            continue
        seen: Counter[str] = Counter()
        start = time.perf_counter()
        for _ in range(runs):
            seen[json.dumps(attempt(lambda sql=sql: session.sql(sql).collect()))] += 1
        record(key, sql, seen, ms=round((time.perf_counter() - start) * 1000))
    for p, s in COLUMNS:
        c = col(p, s)
        key = f"t/{c}/ice/avg/all"
        if not selected(key, only):
            continue
        seen = Counter()
        start = time.perf_counter()
        for run in range(runs):
            seen[json.dumps(iceberg(session, catalog, c, f"r{run}"))] += 1
        record(key, "CTAS", seen, ms=round((time.perf_counter() - start) * 1000))

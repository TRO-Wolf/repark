from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SHAPES: dict[str, str | None] = {
    "date": "2024-01-01",
    "full": "2024-01-01 03:04:05",
    "tsep": "2024-06-01T23:30:00",
    "frac": "2024-01-01 03:04:05.123456",
    "off": "2024-01-01T03:04:05+05:30",
    "zulu": "2024-01-01T03:04:05Z",
    "lead": "  2024-01-01 03:04:05",
    "trail": "2024-01-01 03:04:05  ",
    "empty": "",
    "null": None,
    "bad": "not a time",
    "nosec": "2024-01-01 03:04",
    "tonly": "03:04:05",
    "badtime": "2024-01-01 25:00:00",
    "gap": "2024-03-10 02:30:00",
    "yonly": "2024",
}

G_SHAPES = [
    "date",
    "full",
    "tsep",
    "frac",
    "off",
    "zulu",
    "lead",
    "trail",
    "null",
    "nosec",
    "gap",
    "yonly",
]

CONFIGS: dict[str, tuple[str, str, str]] = {
    "a0-utc": ("false", "UTC", "TIMESTAMP_LTZ"),
    "a0-la": ("false", "America/Los_Angeles", "TIMESTAMP_LTZ"),
    "a1-utc": ("true", "UTC", "TIMESTAMP_LTZ"),
    "a1-la": ("true", "America/Los_Angeles", "TIMESTAMP_LTZ"),
    "a1-utc-ntz": ("true", "UTC", "TIMESTAMP_NTZ"),
    "a1-la-ntz": ("true", "America/Los_Angeles", "TIMESTAMP_NTZ"),
}

HMS = ("hour", "minute", "second")
P_DOORS = ("sql", "fcol", "sexpr", "fexpr")
G_DOORS = ("where_sql", "group_sql", "order_sql", "where_f", "group_f", "order_f")
G_WANT = {"hour": 3, "minute": 4, "second": 5}

N8: dict[str, str] = {}

COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return found.group(1) if found else text[:160]


def text(value: Any) -> str:
    return "None" if value is None else str(value)


def sql_text(value: str | None) -> str:
    if value is None:
        return "CAST(NULL AS STRING)"
    return "'" + value + "'"


def make_sources(session: Any, root: Path) -> None:
    session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    for sid, value in SHAPES.items():
        session.sql(f"CREATE TABLE lc.db.s_{sid} (k INT, c STRING) USING iceberg").collect()
        session.sql(f"INSERT INTO lc.db.s_{sid} VALUES (1, {sql_text(value)})").collect()
    session.sql("CREATE TABLE lc.db.g (k INT, c STRING) USING iceberg").collect()
    rows = ", ".join(f"({i}, {sql_text(SHAPES[sid])})" for i, sid in enumerate(G_SHAPES))
    session.sql(f"INSERT INTO lc.db.g VALUES {rows}").collect()
    (root / "pq").mkdir(parents=True, exist_ok=True)
    for sid, value in SHAPES.items():
        write_parquet(root / "pq" / f"{sid}.parquet", [(1, value)])
    write_parquet(root / "pq" / "g.parquet", g_rows())


def explain(session: Any, fn: str) -> str:
    rows = session.sql(f"EXPLAIN SELECT {fn}(c) FROM lc.db.s_full").collect()
    return "\n".join(str(v) for row in rows for v in row)


def g_rows() -> list[tuple[int, str | None]]:
    return [(i, SHAPES[sid]) for i, sid in enumerate(G_SHAPES)]


def write_parquet(path: Path, rows: list[tuple[int, str | None]]) -> None:
    table = pa.table(
        {
            "k": pa.array([k for k, _ in rows], pa.int32()),
            "c": pa.array([c for _, c in rows], pa.string()),
        }
    )
    pq.write_table(table, path)


def configure(session: Any, cfg: str) -> None:
    ansi, zone, ttype = CONFIGS[cfg]
    session.sql(f"SET spark.sql.ansi.enabled={ansi}").collect()
    session.sql(f"SET spark.sql.session.timeZone={zone}").collect()
    session.sql(f"SET spark.sql.timestampType={ttype}").collect()


def sources(session: Any, root: Path, sid: str) -> dict[str, tuple[str, Any]]:
    rows = g_rows() if sid == "g" else [(1, SHAPES[sid])]
    view = session.createDataFrame(rows, "k INT, c STRING")
    view.createOrReplaceTempView(f"fv_{sid}")
    frame = session.read.parquet(str(root / "pq" / f"{sid}.parquet"))
    frame.createOrReplaceTempView(f"pv_{sid}")
    table = "lc.db.g" if sid == "g" else f"lc.db.s_{sid}"
    return {"I": (table, session.table(table)), "P": (f"pv_{sid}", frame), "V": (f"fv_{sid}", view)}


def project(session: Any, sf: Any, spec: tuple[Any, ...], fns: list[str]) -> dict[str, Any]:
    door, src, df, ref_sql, ref_col = spec
    names = [f"a{i}" for i in range(len(fns))]
    if door == "sql":
        cols = ", ".join(f"{fn}({ref_sql}) AS {name}" for fn, name in zip(fns, names, strict=True))
        frame = session.sql(f"SELECT {cols}" + (f" FROM {src}" if src else ""))
    elif door == "fcol":
        frame = df.select(
            *[getattr(sf, fn)(ref_col).alias(name) for fn, name in zip(fns, names, strict=True)]
        )
    elif door == "sexpr":
        frame = df.selectExpr(
            *[f"{fn}({ref_sql}) AS {name}" for fn, name in zip(fns, names, strict=True)]
        )
    else:
        frame = df.select(
            *[sf.expr(f"{fn}({ref_sql})").alias(name) for fn, name in zip(fns, names, strict=True)]
        )
    rows = frame.collect()
    if len(rows) != 1:
        return {fn: {"rows": len(rows)} for fn in fns}
    return {fn: {"v": text(rows[0][i])} for i, fn in enumerate(fns)}


def answer(session: Any, sf: Any, spec: tuple[Any, ...], fns: list[str]) -> dict[str, Any]:
    try:
        return project(session, sf, spec, fns)
    except Exception as error:
        if len(fns) == 1:
            return {fns[0]: {"err": condition(error)}}
    out: dict[str, Any] = {}
    for fn in fns:
        out.update(answer(session, sf, spec, [fn]))
    return out


def literal_spec(session: Any, sf: Any, door: str, value: str | None) -> tuple[Any, ...]:
    ref_col = sf.lit(value) if value is not None else sf.lit(None).cast("string")
    return (door, None, session.sql("SELECT 1 AS k"), sql_text(value), ref_col)


def g_frame(session: Any, sf: Any, door: str, fn: str, src: str, df: Any) -> tuple[Any, bool]:
    want = G_WANT[fn]
    col = getattr(sf, fn)(sf.col("c"))
    if door == "where_sql":
        return session.sql(f"SELECT k FROM {src} WHERE {fn}(c) = {want}"), False
    if door == "group_sql":
        query = f"SELECT CAST({fn}(c) AS STRING) AS g, count(*) AS n FROM {src}"
        return session.sql(f"{query} GROUP BY {fn}(c)"), False
    if door == "order_sql":
        return session.sql(f"SELECT k FROM {src} ORDER BY {fn}(c) ASC NULLS FIRST, k"), True
    if door == "where_f":
        return df.filter(col == sf.lit(want)).select("k"), False
    if door == "group_f":
        return df.groupBy(col.cast("string").alias("g")).count(), False
    return df.orderBy(col.asc_nulls_first(), sf.col("k")).select("k"), True


def g_answer(session: Any, sf: Any, door: str, fn: str, src: str, df: Any) -> dict[str, str]:
    try:
        frame, ordered = g_frame(session, sf, door, fn, src, df)
        rows = [[text(v) for v in row] for row in frame.collect()]
    except Exception as error:
        return {"err": condition(error)}
    return {"v": json.dumps(rows if ordered else sorted(rows))}


def groups(keys: list[str]) -> dict[tuple[str, str, str, str], list[str]]:
    out: dict[tuple[str, str, str, str], list[str]] = {}
    for key in keys:
        cfg, inp, sid, door, fn = key.split("|")
        out.setdefault((cfg, inp, sid, door), []).append(fn)
    return out


def measure_all(session: Any, sf: Any, root: Path, keys: list[str]) -> dict[str, Any]:
    found: dict[str, Any] = {}
    plan = groups(keys)
    for cfg in CONFIGS:
        configure(session, cfg)
        for sid in [*SHAPES, "g"]:
            wanted = {k: v for k, v in plan.items() if k[0] == cfg and k[2] == sid}
            if not wanted:
                continue
            srcs = sources(session, root, sid)
            for (_, inp, _, door), fns in sorted(wanted.items()):
                if sid == "g":
                    src, df = srcs[inp]
                    for fn in fns:
                        found[f"{cfg}|{inp}|g|{door}|{fn}"] = g_answer(
                            session, sf, door, fn, src, df
                        )
                    continue
                if inp == "L":
                    spec = literal_spec(session, sf, door, SHAPES[sid])
                else:
                    src, df = srcs[inp]
                    spec = (door, src, df, "c", sf.col("c"))
                for fn, got in answer(session, sf, spec, fns).items():
                    found[f"{cfg}|{inp}|{sid}|{door}|{fn}"] = got
    return found

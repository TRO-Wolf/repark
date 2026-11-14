import json
import sys
import tempfile
from pathlib import Path

from common import error_record, show, write_jsonl

engine, ansi, wh_parent, values_path, out, jar = sys.argv[1:7]
only = set(sys.argv[7].split(",")) if len(sys.argv) > 7 else None
V = json.loads(Path(values_path).read_text())
SCALES = V["scales"]
SRC = {"DOUBLE": V["double"], "FLOAT": V["float"]}
FNS = ["round", "bround"]
wh = tempfile.mkdtemp(prefix=f"rx-{engine}-{ansi}-", dir=wh_parent)
if engine == "repark":
    import repark
    import repark._native as native_module
    from repark import ReparkSession as Builder
    from repark import functions as sf
    from repark._native import PyReparkSession

    b = Builder.builder.appName("rx")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native_module.__file__.split("/scratchpad/", 1)[-1],
    }
    DOORS = ["sql", "fcol", "fexpr", "native"]
else:
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name}
    DOORS = ["sql", "fcol", "fexpr"]
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
native = None
if engine == "repark":
    native = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
    native.register_memory_catalog("ice", wh + "/ice")
    native.create_namespace("ice", "ns", wh + "/ice/ns")
    repark._ANSI_NATIVE = native
records = [dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, **prov)]


def lit(text: str, src: str) -> str:
    if src == "FLOAT":
        return f"CAST(CAST('{text}' AS DOUBLE) AS FLOAT)"
    return f"CAST('{text}' AS DOUBLE)"


def nq(sql: str) -> list:
    return repark.sql(sql).to_arrow().to_pylist()


def ntype(sql: str) -> tuple:
    t = repark.sql(sql).to_arrow()
    return t.to_pylist(), [str(f.type) for f in t.schema]


TAB = {}
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns")
for src, vals in SRC.items():
    t = f"lc.ns.src_{src[0].lower()}"
    TAB[("spark", src)] = t
    s.sql(f"CREATE TABLE {t} (id INT, v {src}, sc INT) USING iceberg").collect()
    for i in range(0, len(vals), 100):
        rows = ", ".join(f"({r['id']}, {lit(r['text'], src)}, 2)" for r in vals[i : i + 100])
        s.sql(f"INSERT INTO {t} VALUES {rows}").collect()
    if native is not None:
        nt = f"ice.ns.src_{src[0].lower()}"
        TAB[("native", src)] = nt
        nq(f"CREATE TABLE {nt} (id INT, v {src}, sc INT)")
        for i in range(0, len(vals), 100):
            rows = ", ".join(f"({r['id']}, {lit(r['text'], src)}, 2)" for r in vals[i : i + 100])
            nq(f"INSERT INTO {nt} VALUES {rows}")


def by_id(rows: list) -> dict:
    return {str(r[0]): show(r[1]) for r in rows}


def run_frame(door: str, src: str, sql_tmpl: str, fcol_fn: object, fexpr_tmpl: str) -> object:
    if door == "sql":
        f = s.sql(sql_tmpl.format(t=TAB[("spark", src)]))
        return [tuple(r) for r in f.collect()], [x.dataType.simpleString() for x in f.schema.fields]
    if door == "native":
        rows, types = ntype(sql_tmpl.format(t=TAB[("native", src)]))
        return [tuple(r.values()) for r in rows], types
    base = s.table(TAB[("spark", src)])
    f = fcol_fn(base) if door == "fcol" else fexpr_tmpl(base)
    return [tuple(r) for r in f.collect()], [x.dataType.simpleString() for x in f.schema.fields]


def fcall(fn: str, c: object, scale: int) -> object:
    return sf.round(c, scale) if fn == "round" else sf.bround(c, scale)


def guarded(rec: dict, thunk: object) -> dict:
    try:
        thunk(rec)
    except Exception as e:
        rec.update(error_record(e))
    return rec


def sel_cells(door: str, src: str, fn: str, sc: int) -> dict:
    rec = {
        "key": f"sel/{door}/{src}/{fn}/{sc}",
        "ctx": "sel",
        "door": door,
        "src": src,
        "fn": fn,
        "scale": sc,
    }

    def body(r: object) -> None:
        rows, types = run_frame(
            door,
            src,
            f"SELECT id, {fn}(v, {sc}) AS r FROM {{t}}",
            lambda d: d.select("id", fcall(fn, sf.col("v"), sc).alias("r")),
            lambda d: d.select("id", sf.expr(f"{fn}(v, {sc})").alias("r")),
        )
        r["type"] = types[1]
        r["rows"] = by_id(rows)

    return guarded(rec, body)


def lit_cells(door: str, src: str, fn: str, chunk: list, ci: int) -> dict:
    rec = {"key": f"lit/{door}/{src}/{fn}/{ci}", "ctx": "lit", "door": door, "src": src, "fn": fn}
    names = [(r["id"], sc) for r in chunk for sc in SCALES]

    def body(rr: object) -> None:
        if door in ("sql", "native", "fexpr"):
            exprs = [f"{fn}({lit(r['text'], src)}, {sc})" for r in chunk for sc in SCALES]
            if door == "fexpr":
                f = s.range(1).select(*[sf.expr(e).alias(f"c{k}") for k, e in enumerate(exprs)])
                row = f.collect()[0]
                types = [x.dataType.simpleString() for x in f.schema.fields]
            elif door == "sql":
                f = s.sql("SELECT " + ", ".join(f"{e} AS c{k}" for k, e in enumerate(exprs)))
                row = f.collect()[0]
                types = [x.dataType.simpleString() for x in f.schema.fields]
            else:
                rows, types = ntype(
                    "SELECT " + ", ".join(f"{e} AS c{k}" for k, e in enumerate(exprs))
                )
                row = list(rows[0].values())
        else:
            cols = []
            for r in chunk:
                c = sf.lit(float(r["text"]))
                if src == "FLOAT":
                    c = c.cast("float")
                for sc in SCALES:
                    cols.append(fcall(fn, c, sc))
            f = s.range(1).select(*[c.alias(f"c{k}") for k, c in enumerate(cols)])
            row = f.collect()[0]
            types = [x.dataType.simpleString() for x in f.schema.fields]
        rr["rows"] = {f"{i}@{sc}": show(row[k]) for k, (i, sc) in enumerate(names)}
        rr["types"] = sorted(set(types))

    return guarded(rec, body)


def where_cells(door: str, src: str, fn: str, sc: int, op: str) -> dict:
    rec = {
        "key": f"where/{door}/{src}/{fn}/{sc}/{op}",
        "ctx": "where",
        "door": door,
        "src": src,
        "fn": fn,
        "scale": sc,
        "op": op,
    }

    def body(r: object) -> None:
        rows, _ = run_frame(
            door,
            src,
            f"SELECT id FROM {{t}} WHERE {fn}(v, {sc}) {op} v",
            lambda d: d.filter(
                {
                    ">": fcall(fn, sf.col("v"), sc) > sf.col("v"),
                    "=": fcall(fn, sf.col("v"), sc) == sf.col("v"),
                    "<": fcall(fn, sf.col("v"), sc) < sf.col("v"),
                }[op]
            ).select("id"),
            lambda d: d.filter(sf.expr(f"{fn}(v, {sc}) {op} v")).select("id"),
        )
        r["ids"] = sorted(int(x[0]) for x in rows)

    return guarded(rec, body)


def group_cells(door: str, src: str, fn: str, sc: int) -> dict:
    rec = {
        "key": f"group/{door}/{src}/{fn}/{sc}",
        "ctx": "group",
        "door": door,
        "src": src,
        "fn": fn,
        "scale": sc,
    }

    def body(r: object) -> None:
        rows, types = run_frame(
            door,
            src,
            f"SELECT {fn}(v, {sc}) AS k, COUNT(*) AS n, MIN(id) AS lo FROM {{t}} GROUP BY "
            f"{fn}(v, {sc})",
            lambda d: d.groupBy(fcall(fn, sf.col("v"), sc).alias("k")).agg(
                sf.count("*").alias("n"), sf.min("id").alias("lo")
            ),
            lambda d: d.groupBy(sf.expr(f"{fn}(v, {sc})").alias("k")).agg(
                sf.count("*").alias("n"), sf.min("id").alias("lo")
            ),
        )
        r["type"] = types[0]
        r["groups"] = sorted([str(x[2]), show(x[0]), int(x[1])] for x in rows)

    return guarded(rec, body)


STORES = [
    ("DOUBLE", "round", 2, None),
    ("DOUBLE", "bround", 2, None),
    ("DOUBLE", "round", 18, None),
    ("DOUBLE", "round", -2, None),
    ("DECIMAL(10,2)", "round", 2, 10**7),
    ("DECIMAL(10,2)", "bround", 2, 10**7),
    ("DECIMAL(38,18)", "round", 18, 10**19),
]


def store_cells(door: str, src: str, target: str, fn: str, sc: int, cap: object) -> dict:
    col_t = src if target == "DOUBLE" else target
    tag = col_t.replace("(", "").replace(")", "").replace(",", "_").lower()
    t_name = f"st_{door}_{src[0].lower()}_{tag}_{fn}_{str(sc).replace('-', 'm')}"
    rec = {
        "key": f"store/{door}/{src}/{col_t}/{fn}/{sc}",
        "ctx": "store",
        "door": door,
        "src": src,
        "fn": fn,
        "scale": sc,
        "target": col_t,
    }
    safe = "true" if cap is None else f"abs(v) < {cap} AND NOT isnan(v)"
    if cap is not None:
        rec["filter"] = safe

    def body(r: object) -> None:
        if door == "native":
            t = f"ice.ns.{t_name}"
            nq(f"CREATE TABLE {t} (id INT, r {col_t})")
            nq(f"INSERT INTO {t} SELECT id, {fn}(v, {sc}) FROM {TAB[('native', src)]} WHERE {safe}")
            rows = [tuple(x.values()) for x in nq(f"SELECT id, r FROM {t}")]
        else:
            t = f"lc.ns.{t_name}"
            s.sql(f"CREATE TABLE {t} (id INT, r {col_t}) USING iceberg").collect()
            if door == "sql":
                s.sql(
                    f"INSERT INTO {t} SELECT id, {fn}(v, {sc}) FROM {TAB[('spark', src)]} "
                    f"WHERE {safe}"
                ).collect()
            else:
                base = s.table(TAB[("spark", src)]).filter(sf.expr(safe))
                if door == "fcol":
                    f = base.select("id", fcall(fn, sf.col("v"), sc).alias("r"))
                else:
                    f = base.select("id", sf.expr(f"{fn}(v, {sc})").alias("r"))
                f.writeTo(t).append()
            rows = [tuple(x) for x in s.sql(f"SELECT id, r FROM {t}").collect()]
        r["rows"] = by_id(rows)

    return guarded(rec, body)


def source_cells() -> None:
    for src in SRC:
        rec = {"key": f"source/{src}", "ctx": "source", "src": src}

        def body(r: object, src: object = src) -> None:
            rows = s.sql(
                "SELECT id, v, CAST(v AS STRING) AS t, CAST(CAST(v AS DOUBLE) AS STRING) "
                f"AS dt FROM {TAB[('spark', src)]}"
            ).collect()
            r["rows"] = {str(x[0]): show(x[1]) for x in rows}
            r["text"] = {str(x[0]): x[2] for x in rows}
            r["dtext"] = {str(x[0]): x[3] for x in rows}
            if native is not None:
                nrows = nq(f"SELECT id, v FROM {TAB[('native', src)]}")
                r["native_rows"] = {str(x["id"]): show(x["v"]) for x in nrows}

        records.append(guarded(rec, body))


def want(ctx: str) -> bool:
    return only is None or ctx in only


if want("source"):
    source_cells()
for src in SRC:
    for door in DOORS:
        for fn in FNS:
            if want("sel"):
                for sc in SCALES:
                    records.append(sel_cells(door, src, fn, sc))
            if want("lit"):
                vals = SRC[src]
                for ci, k in enumerate(range(0, len(vals), 20)):
                    records.append(lit_cells(door, src, fn, vals[k : k + 20], ci))
            if want("where"):
                for sc in SCALES:
                    for op in (">", "=", "<"):
                        records.append(where_cells(door, src, fn, sc, op))
            if want("group"):
                for sc in SCALES:
                    records.append(group_cells(door, src, fn, sc))
        if want("store"):
            for target, fn, sc, cap in STORES:
                records.append(store_cells(door, src, target, fn, sc, cap))
        print(engine, ansi, src, door, len(records), flush=True)
write_jsonl(out, records)
print(engine, ansi, Path(wh).name, len(records))

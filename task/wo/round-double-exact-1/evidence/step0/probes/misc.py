import json
import sys
import tempfile
from pathlib import Path

from common import error_record, show, write_jsonl

engine, ansi, wh_parent, values_path, out, jar = sys.argv[1:7]
V = json.loads(Path(values_path).read_text())
EXTREME = [-400, -309, -308, -20, 19, 20, 22, 23, 30, 308, 309, 400]
wh = tempfile.mkdtemp(prefix=f"rxm-{engine}-{ansi}-", dir=wh_parent)
if engine == "repark":
    import repark
    import repark._native as native_module
    from repark import ReparkSession as Builder
    from repark import functions as sf
    from repark._native import PyReparkSession

    b = Builder.builder.appName("rxm")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native_module.__file__.split("/scratchpad/", 1)[-1],
    }
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


def nq(sql: str) -> object:
    t = repark.sql(sql).to_arrow()
    return [tuple(r.values()) for r in t.to_pylist()], [str(f.type) for f in t.schema]


TAB = {}
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns")
for src, key in (("DOUBLE", "double"), ("FLOAT", "float")):
    vals = V[key]
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


def collect(f: object) -> object:
    return [tuple(r) for r in f.collect()], [x.dataType.simpleString() for x in f.schema.fields]


def cell(key: str, door: str, thunk: object) -> None:
    rec = {"key": f"{key}/{door}", "door": door}
    try:
        rows, types = thunk()
        rec["types"] = types
        rec["rows"] = [[show(x) for x in r] for r in rows]
    except Exception as e:
        rec.update(error_record(e))
    records.append(rec)


def sql_doors(key: str, sql: str, src: str | None = None) -> None:
    text = sql if src is None else sql.format(t=TAB[("spark", src)])
    cell(key, "sql", lambda: collect(s.sql(text)))
    if native is not None:
        ntext = sql if src is None else sql.format(t=TAB[("native", src)])
        cell(key, "native", lambda: nq(ntext))


def frame_door(key: str, door: str, build: object) -> None:
    cell(key, door, lambda: collect(build()))


for src in ("DOUBLE", "FLOAT"):
    t_order = " ORDER BY id"
    for fn in ("round", "bround"):
        sql_doors(f"nullscale/{src}/{fn}", f"SELECT id, {fn}(v, NULL) FROM {{t}}{t_order}", src)
        sql_doors(
            f"nullscale_typed/{src}/{fn}",
            f"SELECT id, {fn}(v, CAST(NULL AS INT)) FROM {{t}}{t_order}",
            src,
        )
        frame_door(
            f"nullscale_typed/{src}/{fn}",
            "fexpr",
            lambda fn=fn, src=src: (
                s.table(TAB[("spark", src)])
                .select("id", sf.expr(f"{fn}(v, CAST(NULL AS INT))"))
                .orderBy("id")
            ),
        )
        frame_door(
            f"nullscale_typed/{src}/{fn}",
            "fcol",
            lambda fn=fn, src=src: (
                s.table(TAB[("spark", src)])
                .select(
                    "id",
                    (sf.round if fn == "round" else sf.bround)(
                        sf.col("v"), sf.lit(None).cast("int")
                    ),
                )
                .orderBy("id")
            ),
        )
        sql_doors(f"colscale/{src}/{fn}", f"SELECT id, {fn}(v, sc) FROM {{t}}{t_order}", src)
        frame_door(
            f"colscale/{src}/{fn}",
            "fexpr",
            lambda fn=fn, src=src: (
                s.table(TAB[("spark", src)]).select("id", sf.expr(f"{fn}(v, sc)")).orderBy("id")
            ),
        )
        frame_door(
            f"colscale/{src}/{fn}",
            "fcol",
            lambda fn=fn, src=src: (
                s.table(TAB[("spark", src)])
                .select("id", (sf.round if fn == "round" else sf.bround)(sf.col("v"), sf.col("sc")))
                .orderBy("id")
            ),
        )
        sql_doors(f"noscale/{src}/{fn}", f"SELECT id, {fn}(v) FROM {{t}}{t_order}", src)
        frame_door(
            f"noscale/{src}/{fn}",
            "fcol",
            lambda fn=fn, src=src: (
                s.table(TAB[("spark", src)])
                .select("id", (sf.round if fn == "round" else sf.bround)(sf.col("v")))
                .orderBy("id")
            ),
        )
        frame_door(
            f"noscale/{src}/{fn}",
            "fexpr",
            lambda fn=fn, src=src: (
                s.table(TAB[("spark", src)]).select("id", sf.expr(f"{fn}(v)")).orderBy("id")
            ),
        )
        sql_doors(
            f"bigintscale/{src}/{fn}",
            f"SELECT id, {fn}(v, CAST(2 AS BIGINT)) FROM {{t}}{t_order}",
            src,
        )
        for sc in EXTREME:
            sql_doors(
                f"extreme/{src}/{fn}/{sc}", f"SELECT id, {fn}(v, {sc}) FROM {{t}}{t_order}", src
            )
            frame_door(
                f"extreme/{src}/{fn}/{sc}",
                "fcol",
                lambda fn=fn, src=src, sc=sc: (
                    s.table(TAB[("spark", src)])
                    .select("id", (sf.round if fn == "round" else sf.bround)(sf.col("v"), sc))
                    .orderBy("id")
                ),
            )
INTS = {
    "TINYINT": [0, 25, -25, 127, -128],
    "SMALLINT": [125, -125, 32767, -32768],
    "INT": [0, 5, 15, 25, -25, 125, -125, 2147483647, -2147483648],
    "BIGINT": [15, -15, 1234567890123, 9223372036854775807, -9223372036854775808],
}
CTL_SCALES = [-3, -2, -1, 0, 1, 2]
for typ, xs in INTS.items():
    for x in xs:
        for fn in ("round", "bround"):
            exprs = [f"{fn}(CAST({x} AS {typ}), {sc})" for sc in CTL_SCALES]
            sql = "SELECT " + ", ".join(f"{e} AS a{k}" for k, e in enumerate(exprs))
            sql_doors(f"ctl_int/{typ}/{x}/{fn}", sql)
            cell(
                f"ctl_int/{typ}/{x}/{fn}",
                "fexpr",
                lambda exprs=exprs: collect(
                    s.range(1).select(*[sf.expr(e).alias(f"a{k}") for k, e in enumerate(exprs)])
                ),
            )
DECS = [
    ("1.005", 10, 3),
    ("-1.005", 10, 3),
    ("123.456", 6, 3),
    ("99.95", 4, 2),
    ("-22.129547339983247", 38, 18),
    ("0.5", 3, 1),
    ("2.5", 2, 1),
    ("-2.5", 2, 1),
    ("99999999.995", 11, 3),
    ("1.255", 38, 18),
]
DEC_SCALES = [-3, -1, 0, 1, 2, 3, 18]
for text, p, q in DECS:
    for fn in ("round", "bround"):
        sql = "SELECT " + ", ".join(
            f"{fn}(CAST('{text}' AS DECIMAL({p},{q})), {sc}) AS a{k}"
            for k, sc in enumerate(DEC_SCALES)
        )
        sql_doors(f"ctl_dec/{text}/{p},{q}/{fn}", sql)
for text in ("1.005", "2.675", "1.255", "0.285"):
    for fn in ("round", "bround"):
        sql_doors(
            f"ctl_declit/{text}/{fn}",
            f"SELECT {fn}({text}, 2) AS a0, {fn}({text}D, 2) AS a1, {fn}({text}E0, 2) AS a2",
        )
CARD = [
    (
        "card_float_direct",
        "SELECT ROUND(CAST('1.255' AS FLOAT), 2) AS a0, BROUND(CAST('1.255' AS FLOAT), 2) AS a1",
    ),
    ("card_string_arg", "SELECT ROUND('1.005', 2) AS a0"),
    ("card_where_eq", "SELECT id FROM {t} WHERE ROUND(v, 2) = CAST('1.01' AS DOUBLE) ORDER BY id"),
    (
        "card_where_eq_bround",
        "SELECT id FROM {t} WHERE BROUND(v, 2) = CAST('2.68' AS DOUBLE) ORDER BY id",
    ),
    (
        "card_typeof",
        "SELECT typeof(ROUND(CAST(1 AS FLOAT), 2)) AS a0, typeof(BROUND(CAST(1 AS FLOAT), "
        "2)) AS a1, "
        "typeof(ROUND(CAST(1 AS DOUBLE), 2)) AS a2, typeof(ROUND(CAST(1 AS INT), -1)) AS a3",
    ),
    (
        "card_nested",
        "SELECT ROUND(ROUND(CAST('1.0049999999999999' AS DOUBLE), 3), 2) AS a0, "
        "ROUND(CAST('1.005' AS DOUBLE) * 1, 2) AS a1, ROUND(-CAST('1.005' AS DOUBLE), 2) AS a2",
    ),
]
for name, sql in CARD:
    sql_doors(f"card/{name}", sql, "DOUBLE" if "{t}" in sql else None)
write_jsonl(out, records)
print(engine, ansi, Path(wh).name, len(records))

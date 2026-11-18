import sys
import tempfile
import time
from collections.abc import Callable

import cells
import lib

engine, start, wh_parent, jar, out = sys.argv[1:6]
only = set(sys.argv[6].split(",")) if len(sys.argv) > 6 and sys.argv[6] else set()
wh = tempfile.mkdtemp(prefix="wh-" + engine + "-", dir=wh_parent)
s, prov = lib.build(engine, wh, jar, start)
log = lib.Log(out, dict(prov, start=start, only=sorted(only)))
held: dict = {}


def put(cell: str, meta: dict, run: object) -> None:
    t0 = time.time()
    res = lib.attempt(run)
    log.put(dict(meta, cell=cell, result=res, ms=int((time.time() - t0) * 1000)))


def run_sql(sql: str) -> dict:
    s.sql(sql).collect()
    return {"rows": [], "types": []}


def read(sql: str) -> Callable[[], dict]:
    return lambda: lib.answer(s.sql(sql))


def setv(fam: dict, value: str) -> None:
    if fam["key"] == "USE":
        s.sql("USE " + value).collect()
    else:
        s.sql("SET " + fam["key"] + "=" + value).collect()


def fixtures() -> None:
    for name, sql in cells.FIXTURES.items():
        path = wh + "/fx-" + name
        s.sql(sql).write.mode("overwrite").parquet(path)
        s.read.parquet(path).createOrReplaceTempView(name)
    s.sql("SET spark.sql.caseSensitive=true").collect()
    name, sql = cells.CASE_TWIN_FIXTURE
    put(
        "fixture/" + name,
        {"family": "fixture"},
        lambda: s.sql(sql).write.mode("overwrite").parquet(wh + "/fx-" + name),
    )
    put(
        "fixture/" + name + "/view",
        {"family": "fixture"},
        lambda: s.read.parquet(wh + "/fx-" + name).createOrReplaceTempView(name),
    )
    s.sql("SET spark.sql.caseSensitive=false").collect()
    for stmt in ["CREATE NAMESPACE IF NOT EXISTS lc.ns", "CREATE NAMESPACE IF NOT EXISTS mc.ns"]:
        put("fixture/" + stmt, {"family": "fixture"}, lambda stmt=stmt: run_sql(stmt))
    for local, table in cells.CATALOG_TABLES.items():
        put(
            "fixture/" + table,
            {"family": "fixture"},
            lambda local=local, table=table: run_sql(
                "CREATE TABLE " + table + " USING iceberg AS SELECT * FROM " + local
            ),
        )
    for table, value in cells.NAMESPACE_TABLES.items():
        ns = table.rsplit(".", 1)[0]
        put(
            "fixture/" + ns,
            {"family": "fixture"},
            lambda ns=ns: run_sql("CREATE NAMESPACE IF NOT EXISTS " + ns),
        )
        put(
            "fixture/" + table,
            {"family": "fixture"},
            lambda table=table, value=value: run_sql(
                "CREATE TABLE "
                + table
                + " USING iceberg AS SELECT CAST("
                + str(value)
                + " AS INT) AS x"
            ),
        )


def create(fname: str, fam: dict, tag: str, bodies: dict) -> None:
    for b, body in bodies.items():
        meta = {"family": fname, "body": b, "created": tag, "phase": "create"}
        for door in sorted(fam["doors"], key=lambda d: 1 if d == "tdff" else 0):
            m = dict(meta, door=door)
            if door in ("heldf", "tsqlf", "tdff"):
                source = {"heldf": None, "tsqlf": "sql_", "tdff": "dff_"}[door]
                put(
                    "create/" + door + "/" + tag + "/" + b,
                    m,
                    lambda b=b, body=body, door=door, source=source: held.__setitem__(
                        (door, tag, b),
                        s.sql(body) if source is None else s.table(source + tag + "_" + b),
                    ),
                )
            elif door == "dff":
                put(
                    "create/dff/" + tag + "/" + b,
                    m,
                    lambda b=b, body=body: s.sql(body).createOrReplaceTempView(
                        "dff_" + tag + "_" + b
                    ),
                )
            elif door == "vdsf":
                put(
                    "create/vdsf/" + tag + "/" + b,
                    m,
                    lambda b=b: s.sql("SELECT m FROM sql_" + tag + "_" + b).createOrReplaceTempView(
                        "vdsf_" + tag + "_" + b
                    ),
                )
            elif door == "sql":
                put(
                    "create/sql/" + tag + "/" + b,
                    m,
                    lambda b=b, body=body: run_sql(
                        "CREATE OR REPLACE TEMP VIEW sql_" + tag + "_" + b + " AS " + body
                    ),
                )
            elif door == "df":
                put(
                    "create/df/" + tag + "/" + b,
                    m,
                    lambda b=b, body=body: s.sql(body).createOrReplaceTempView(
                        "df_" + tag + "_" + b
                    ),
                )
            elif door == "held":
                put(
                    "create/held/" + tag + "/" + b,
                    m,
                    lambda b=b, body=body: held.__setitem__(("held", tag, b), s.sql(body)),
                )
            elif door == "tsql":
                put(
                    "create/tsql/" + tag + "/" + b,
                    m,
                    lambda b=b: held.__setitem__(("tsql", tag, b), s.table("sql_" + tag + "_" + b)),
                )
            elif door == "tdf":
                put(
                    "create/tdf/" + tag + "/" + b,
                    m,
                    lambda b=b: held.__setitem__(("tdf", tag, b), s.table("df_" + tag + "_" + b)),
                )
            elif door == "vss":
                put(
                    "create/vss/" + tag + "/" + b,
                    m,
                    lambda b=b: run_sql(
                        "CREATE OR REPLACE TEMP VIEW vss_"
                        + tag
                        + "_"
                        + b
                        + " AS SELECT m FROM sql_"
                        + tag
                        + "_"
                        + b
                    ),
                )
            elif door == "vsd":
                put(
                    "create/vsd/" + tag + "/" + b,
                    m,
                    lambda b=b: run_sql(
                        "CREATE OR REPLACE TEMP VIEW vsd_"
                        + tag
                        + "_"
                        + b
                        + " AS SELECT m FROM df_"
                        + tag
                        + "_"
                        + b
                    ),
                )
            elif door == "vds":
                put(
                    "create/vds/" + tag + "/" + b,
                    m,
                    lambda b=b: s.sql("SELECT m FROM sql_" + tag + "_" + b).createOrReplaceTempView(
                        "vds_" + tag + "_" + b
                    ),
                )
            elif door == "ren":
                put(
                    "create/ren/" + tag + "/" + b,
                    m,
                    lambda b=b, body=body: run_sql(
                        "CREATE OR REPLACE TEMP VIEW rn_" + tag + "_" + b + " AS " + body
                    ),
                )
        if b in fam["catalog"]:
            put(
                "create/cat/" + tag + "/" + b,
                dict(meta, door="cat"),
                lambda b=b, body=body: run_sql(
                    "CREATE OR REPLACE VIEW mc.ns.cv_"
                    + tag
                    + "_"
                    + b
                    + " AS "
                    + cells.catalog_body(body)
                ),
            )


def rename(fname: str, fam: dict, tag: str, bodies: dict) -> None:
    if "ren" not in fam["doors"]:
        return
    for b in bodies:
        old, new = "rn_" + tag + "_" + b, "ren_" + tag + "_" + b

        def go(old: str = old, new: str = new) -> dict:
            try:
                s.sql("ALTER VIEW " + old + " RENAME TO " + new).collect()
                return {"rows": ["ALTER VIEW"], "types": []}
            except Exception as first:
                try:
                    s.sql("ALTER TABLE " + old + " RENAME TO " + new).collect()
                except Exception:
                    raise first from None
                return {"rows": ["ALTER TABLE"], "types": []}

        put(
            "rename/" + tag + "/" + b,
            {"family": fname, "body": b, "created": tag, "phase": "rename"},
            go,
        )


def view_name(door: str, tag: str, b: str) -> str:
    if door == "ren":
        return "ren_" + tag + "_" + b
    if door == "cat":
        return "mc.ns.cv_" + tag + "_" + b
    return door + "_" + tag + "_" + b


def read_all(fname: str, fam: dict, tag: str, reader: str, bodies: dict) -> None:
    for b in bodies:
        doors = list(fam["doors"]) + (["cat"] if b in fam["catalog"] else [])
        for door in doors:
            if door in cells.FRESH and tag == reader:
                continue
            meta = {
                "family": fname,
                "body": b,
                "door": door,
                "created": tag,
                "read": reader,
                "phase": "read",
            }
            if door in ("held", "tsql", "tdf", "heldf", "tsqlf", "tdff"):
                key = (door, tag, b)
                put(
                    "read/" + door + "/plain/" + tag + "/" + reader + "/" + b,
                    dict(meta, pos="plain"),
                    lambda key=key: lib.answer(held[key]),
                )
                continue
            name = view_name(door, tag, b)
            positions = (
                cells.POSITIONS
                if fam["positions"] and door in ("sql", "df", "cat")
                else {"plain": cells.POSITIONS["plain"]}
            )
            for pos, form in positions.items():
                put(
                    "read/" + door + "/" + pos + "/" + tag + "/" + reader + "/" + b,
                    dict(meta, pos=pos),
                    read(form.format(V=name)),
                )


def direct(fname: str, reader: str, bodies: dict) -> None:
    for b, body in bodies.items():
        put(
            "direct/" + reader + "/" + b,
            {"family": fname, "body": b, "door": "direct", "read": reader, "phase": "direct"},
            read(body),
        )


def writes(fname: str, fam: dict, tag: str, writer: str, bodies: dict) -> None:
    for b, sqltype in fam["writes"].items():
        if b not in bodies:
            continue
        meta = {"family": fname, "body": b, "created": tag, "read": writer, "phase": "write"}
        suffix = tag + "_" + b
        for door, src in (("sql", "sql_"), ("df", "df_")):
            table = "lc.ns.wc" + door + "_" + suffix

            def ctas(table: str = table, src: str = src, suffix: str = suffix) -> dict:
                s.sql(
                    "CREATE TABLE " + table + " USING iceberg AS SELECT m FROM " + src + suffix
                ).collect()
                return lib.answer(s.sql("SELECT m FROM " + table))

            put(
                "write/ctas-" + door + "/" + tag + "/" + writer + "/" + b,
                dict(meta, door="w-ctas-" + door),
                ctas,
            )
        table = "lc.ns.wi_" + suffix

        def insert(table: str = table, sqltype: str = sqltype, suffix: str = suffix) -> dict:
            s.sql("CREATE TABLE " + table + " (m " + sqltype + ") USING iceberg").collect()
            s.sql("INSERT INTO " + table + " SELECT m FROM sql_" + suffix).collect()
            return lib.answer(s.sql("SELECT m FROM " + table))

        put(
            "write/insert-sql/" + tag + "/" + writer + "/" + b,
            dict(meta, door="w-insert-sql"),
            insert,
        )
        for kind in ("held", "tsql"):
            if (kind, tag, b) not in held:
                continue
            table = "lc.ns.wt" + kind + "_" + suffix

            def write_to(table: str = table, kind: str = kind, b: str = b) -> dict:
                held[(kind, tag, b)].writeTo(table).using("iceberg").create()
                return lib.answer(s.sql("SELECT m FROM " + table))

            put(
                "write/writeto-" + kind + "/" + tag + "/" + writer + "/" + b,
                dict(meta, door="w-writeto-" + kind),
                write_to,
            )
        path = wh + "/pq-" + suffix

        def parquet(path: str = path, b: str = b) -> dict:
            held[("held", tag, b)].write.mode("overwrite").parquet(path)
            return lib.answer(s.read.parquet(path))

        if ("held", tag, b) in held:
            put(
                "write/parquet-held/" + tag + "/" + writer + "/" + b,
                dict(meta, door="w-parquet-held"),
                parquet,
            )


def nest(fname: str, fam: dict) -> None:
    if fname not in cells.NEST:
        return
    (t1, v1), (t2, v2) = fam["values"]
    inner_body, outer_body = cells.NEST[fname]
    for di, do in cells.NEST_DOORS:
        inner, outer = "ni_" + di + do, "no_" + di + do
        meta = {
            "family": fname,
            "door": "nest-" + di + "-" + do,
            "phase": "nest",
            "created": t1 + ">" + t2,
        }
        setv(fam, v1)
        if di == "sql":
            put(
                "nest/create-inner/" + di + do,
                meta,
                lambda inner=inner: run_sql(
                    "CREATE OR REPLACE TEMP VIEW " + inner + " AS " + inner_body
                ),
            )
        else:
            put(
                "nest/create-inner/" + di + do,
                meta,
                lambda inner=inner: s.sql(inner_body).createOrReplaceTempView(inner),
            )
        setv(fam, v2)
        text = outer_body.format(I=inner)
        if do == "sql":
            put(
                "nest/create-outer/" + di + do,
                meta,
                lambda outer=outer, text=text: run_sql(
                    "CREATE OR REPLACE TEMP VIEW " + outer + " AS " + text
                ),
            )
        else:
            put(
                "nest/create-outer/" + di + do,
                meta,
                lambda outer=outer, text=text: s.sql(text).createOrReplaceTempView(outer),
            )
        for reader, value in ((t2, v2), (t1, v1)):
            setv(fam, value)
            for col in ("m", "m2"):
                put(
                    "nest/read/" + di + do + "/" + reader + "/" + col,
                    dict(meta, read=reader, body=col),
                    read("SELECT " + col + " FROM " + outer),
                )
    setv(fam, v1)


def reader_side(fname: str, fam: dict) -> None:
    if fname not in cells.READER:
        return
    spec = cells.READER[fname]
    for (tag, value), (rtag, rvalue) in (tuple(fam["values"]), tuple(reversed(fam["values"]))):
        setv(fam, value)
        put(
            "reader/create-sql/" + tag,
            {"family": fname, "phase": "reader"},
            lambda tag=tag: run_sql(
                "CREATE OR REPLACE TEMP VIEW rs_sql_" + tag + " AS " + spec["body"]
            ),
        )
        put(
            "reader/create-df/" + tag,
            {"family": fname, "phase": "reader"},
            lambda tag=tag: s.sql(spec["body"]).createOrReplaceTempView("rs_df_" + tag),
        )
        held[("rs", tag)] = s.sql(spec["body"])
        setv(fam, rvalue)
        for form_name, form in spec["forms"].items():
            for door in ("sql", "df"):
                sql = form.format(V="rs_" + door + "_" + tag, H=cells.READER_HOUR.get(rtag, 0))
                meta = {
                    "family": fname,
                    "phase": "reader",
                    "door": door,
                    "body": form_name,
                    "created": tag,
                    "read": rtag,
                }
                put("reader/" + door + "/" + tag + "/" + rtag + "/" + form_name, meta, read(sql))
            direct_sql = form.format(V="(" + spec["body"] + ") d", H=cells.READER_HOUR.get(rtag, 0))
            put(
                "reader/direct/" + tag + "/" + rtag + "/" + form_name,
                {
                    "family": fname,
                    "phase": "reader",
                    "door": "direct",
                    "body": form_name,
                    "created": tag,
                    "read": rtag,
                },
                read(direct_sql),
            )
    setv(fam, fam["values"][0][1])


def now_cells(fname: str, fam: dict) -> None:
    if fname != "tz":
        return
    for (tag, value), (rtag, rvalue) in (tuple(fam["values"]), tuple(reversed(fam["values"]))):
        setv(fam, value)
        put(
            "now/create-sql/" + tag,
            {"family": fname, "phase": "now"},
            lambda tag=tag: run_sql(
                "CREATE OR REPLACE TEMP VIEW now_sql_" + tag + " AS " + cells.NOW_BODY
            ),
        )
        put(
            "now/create-df/" + tag,
            {"family": fname, "phase": "now"},
            lambda tag=tag: s.sql(cells.NOW_BODY).createOrReplaceTempView("now_df_" + tag),
        )
        setv(fam, rvalue)
        for door in ("sql", "df"):
            meta = {
                "family": fname,
                "phase": "now",
                "door": door,
                "body": "now",
                "created": tag,
                "read": rtag,
            }
            put(
                "now/" + door + "/" + tag + "/" + rtag,
                meta,
                read(cells.NOW_READ.format(V="now_" + door + "_" + tag)),
            )
    setv(fam, fam["values"][0][1])


def family(fname: str, fam: dict) -> None:
    bodies = {k: v for k, v in fam["bodies"].items() if not only or k in only}
    if not bodies:
        return
    (t1, v1), (t2, v2) = fam["values"]
    setv(fam, v1)
    create(fname, fam, t1, bodies)
    read_all(fname, fam, t1, t1, bodies)
    direct(fname, t1, bodies)
    setv(fam, v2)
    rename(fname, fam, t1, bodies)
    read_all(fname, fam, t1, t2, bodies)
    direct(fname, t2, bodies)
    writes(fname, fam, t1, t2, bodies)
    create(fname, fam, t2, bodies)
    read_all(fname, fam, t2, t2, bodies)
    setv(fam, v1)
    rename(fname, fam, t2, bodies)
    read_all(fname, fam, t2, t1, bodies)
    writes(fname, fam, t2, t1, bodies)
    if not only:
        nest(fname, fam)
        reader_side(fname, fam)
        now_cells(fname, fam)
    setv(fam, v1)


def unset_run() -> None:
    meta = {"family": "unset", "phase": "unset"}
    put("unset/ansi_at_start", meta, read("SET spark.sql.ansi.enabled"))
    for b, body in cells.UNSET_BODIES.items():
        put(
            "unset/create-sql/" + b,
            dict(meta, body=b),
            lambda b=b, body=body: run_sql(
                "CREATE OR REPLACE TEMP VIEW u_sql_" + b + " AS " + body
            ),
        )
        put(
            "unset/create-df/" + b,
            dict(meta, body=b),
            lambda b=b, body=body: s.sql(body).createOrReplaceTempView("u_df_" + b),
        )
        held[("u", b)] = s.sql(body)
    for reader, stmt in (("unset", None), ("off", "SET spark.sql.ansi.enabled=false")):
        if stmt:
            s.sql(stmt).collect()
        for b, body in cells.UNSET_BODIES.items():
            m = dict(meta, body=b, created="unset", read=reader)
            put(
                "unset/read/sql/" + reader + "/" + b,
                dict(m, door="sql"),
                read("SELECT m FROM u_sql_" + b),
            )
            put(
                "unset/read/df/" + reader + "/" + b,
                dict(m, door="df"),
                read("SELECT m FROM u_df_" + b),
            )
            put(
                "unset/read/held/" + reader + "/" + b,
                dict(m, door="held"),
                lambda b=b: lib.answer(held[("u", b)]),
            )
            put("unset/direct/" + reader + "/" + b, dict(m, door="direct"), read(body))


put("conf/start/ansi", {"family": "conf"}, read("SET spark.sql.ansi.enabled"))
fixtures()
if start == "unset":
    unset_run()
else:
    for fname, fam in cells.FAMILIES.items():
        family(fname, fam)
log.close()
s.stop()
print("WROTE " + out, flush=True)

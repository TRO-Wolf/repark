import sys
import tempfile
from pathlib import Path

from cells import (
    CARD,
    EXTRA_DOORS,
    READ_DOORS,
    SELECTS,
    SOURCES,
    TARGETS,
    TEXTS,
    WRITE_DOORS,
    ids,
    lit,
)
from common import error_record, rows_by_id, write_jsonl

engine, ansi, wh_parent, out, jar = sys.argv[1:6]
only = sys.argv[6].split(",") if len(sys.argv) > 6 else None
wh = tempfile.mkdtemp(prefix=f"ddr-{engine}-{ansi}-", dir=wh_parent)
if engine == "repark":
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    b = Builder.builder.appName("ddr")
    print(native.__file__, flush=True)
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
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
records = [dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, texts=TEXTS, **prov)]
for src in SOURCES:
    rows = ", ".join(f"({i}, '{t}')" for i, t in enumerate(TEXTS))
    s.sql(
        f"SELECT id, CAST(t AS {src}) AS v FROM VALUES {rows} AS x(id, t)"
    ).createOrReplaceTempView(f"lit_{src[0].lower()}")
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns")
for src in SOURCES:
    tab = f"lc.ns.src_{src[0].lower()}"
    s.sql(f"CREATE TABLE {tab} (id INT, v {src}) USING iceberg").collect()
    s.sql(f"INSERT INTO {tab} SELECT id, v FROM lit_{src[0].lower()}").collect()


def attempt(fn: object) -> dict | None:
    try:
        fn()
        return None
    except Exception as e:
        return error_record(e)


def batched(all_fn: object, one_fn: object) -> dict:
    rec = {}
    err = attempt(all_fn)
    rec["batch"] = "ok" if err is None else err
    if err is not None:
        rec["row_errors"] = {}
        for i in ids():
            e = attempt(lambda i=i: one_fn(i))
            if e is not None:
                rec["row_errors"][str(i)] = e
    return rec


def source_cells(src: str, view: str) -> list:
    out_recs = []
    try:
        frame = s.sql(f"SELECT id, v FROM {view} ORDER BY id")
        out_recs.append({"key": f"source/frame/{src}", "rows": rows_by_id(frame)})
    except Exception as e:
        out_recs.append({"key": f"source/frame/{src}", **error_record(e)})
    literal_rows = {}
    for i, t in enumerate(TEXTS):
        try:
            literal_rows[str(i)] = rows_by_id(s.sql(f"SELECT {i} AS id, {lit(t, src)} AS v"))[
                str(i)
            ]
        except Exception as e:
            literal_rows[str(i)] = error_record(e)
    out_recs.append({"key": f"source/lit/{src}", "rows": literal_rows})
    return out_recs


def read_door(door: str, src: str, view: str, p: int, sc: int) -> dict:
    dec = f"DECIMAL({p},{sc})"
    rec = {"key": f"{door}/{src}/{p},{sc}", "door": door, "src": src, "target": dec}
    got = {}
    if door == "sel_cast_lit":
        for i, t in enumerate(TEXTS):
            try:
                frame = s.sql(f"SELECT {i} AS id, CAST({lit(t, src)} AS {dec}) AS v")
                got.update(rows_by_id(frame))
                rec["type"] = frame.schema.fields[1].dataType.simpleString()
            except Exception as e:
                got[str(i)] = error_record(e)
        rec["rows"] = got
        return rec
    if door == "sel_cast_col":

        def frame_of(cond: str) -> object:
            return s.sql(f"SELECT id, CAST(v AS {dec}) AS v FROM {view} WHERE {cond} ORDER BY id")
    else:

        def frame_of(cond: str) -> object:
            return s.sql(f"SELECT id, v FROM {view} WHERE {cond}").select(
                "id", sf.col("v").cast(f"decimal({p},{sc})").alias("v")
            )

    try:
        frame = frame_of("true")
        rec["type"] = frame.schema.fields[1].dataType.simpleString()
        got.update(rows_by_id(frame))
        rec["batch"] = "ok"
    except Exception as e:
        rec["batch"] = error_record(e)
        for i in ids():
            try:
                got.update(rows_by_id(frame_of(f"id = {i}")))
            except Exception as e2:
                got[str(i)] = error_record(e2)
    rec["rows"] = got
    return rec


SEPARATE = ("ctas", "ins_overwrite", "ins_overwrite_part", "df_overwrite")


def write_action(w: dict, i: int | None) -> None:
    door, t, view, src = w["door"], w["table"], w["view"], w["src"]
    cond = "true" if i is None else f"id = {i}"
    frame = s.sql(f"SELECT id, v FROM {view} WHERE {cond}")
    if door in SEPARATE and i is not None:
        t = f"{t}_r{i}"
        if door != "ctas":
            s.sql(f"CREATE TABLE {t} {w['ddl']}").collect()
    if door == "ins_values":
        picked = list(enumerate(TEXTS)) if i is None else [(i, TEXTS[i])]
        rows = ", ".join(f"({j}, {lit(x, src)})" for j, x in picked)
        s.sql(f"INSERT INTO {t} VALUES {rows}").collect()
    elif door == "ins_select":
        s.sql(f"INSERT INTO {t} SELECT id, v FROM {view} WHERE {cond}").collect()
    elif door == "ins_union":
        s.sql(
            f"INSERT INTO {t} SELECT id, v FROM {view} WHERE {cond} AND id % 2 = 0 "
            f"UNION ALL SELECT id, v FROM {view} WHERE {cond} AND id % 2 = 1"
        ).collect()
    elif door == "ctas":
        q = f"SELECT id, CAST(v AS {w['dec']}) AS v FROM {view}"
        tail = "" if i is None else f" WHERE id = {i}"
        s.sql(f"CREATE TABLE {t} USING iceberg AS {q}{tail}").collect()
    elif door == "update":
        if i is None:
            case = " ".join(f"WHEN {j} THEN {lit(x, src)}" for j, x in enumerate(TEXTS))
            s.sql(f"UPDATE {t} SET v = CASE id {case} END").collect()
        else:
            s.sql(f"UPDATE {t} SET v = {lit(TEXTS[i], src)} WHERE id = {i}").collect()
    elif door in ("merge_update", "merge_insert"):
        if door == "merge_update":
            clause = "WHEN MATCHED THEN UPDATE SET v = s.v"
        else:
            clause = "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        using = view if i is None else f"(SELECT id, v FROM {view} WHERE id = {i})"
        s.sql(f"MERGE INTO {t} t USING {using} s ON t.id = s.id {clause}").collect()
    elif door == "df_overwrite":
        frame.writeTo(t).overwritePartitions()
    elif door in ("ins_overwrite", "ins_overwrite_part"):
        head = "PARTITION (p = 1) " if door == "ins_overwrite_part" else ""
        s.sql(f"INSERT OVERWRITE {t} {head}SELECT id, v FROM {view} WHERE {cond}").collect()
    elif door == "df_insertinto":
        frame.write.insertInto(t)
    elif door == "df_writeto":
        frame.writeTo(t).append()
    else:
        frame.write.mode("append").saveAsTable(t)
    if door in SEPARATE and i is not None:
        w["tables"][str(i)] = t


def write_door(door: str, src: str, view: str, p: int, sc: int) -> dict:
    dec = f"DECIMAL({p},{sc})"
    t = f"lc.ns.{door}_{src[0].lower()}_{p}_{sc}"
    rec = {"key": f"{door}/{src}/{p},{sc}", "door": door, "src": src, "target": dec, "table": t}
    if door == "ins_overwrite_part":
        ddl = f"(id INT, v {dec}, p INT) USING iceberg PARTITIONED BY (p)"
    else:
        ddl = f"(id INT, v {dec}) USING iceberg"
    if door != "ctas":
        s.sql(f"CREATE TABLE {t} {ddl}").collect()
    if door in ("update", "merge_update"):
        s.sql(f"INSERT INTO {t} SELECT id, CAST(NULL AS {dec}) FROM {view}").collect()
    w = {"door": door, "table": t, "view": view, "src": src, "dec": dec, "ddl": ddl, "tables": {}}
    rec.update(batched(lambda: write_action(w, None), lambda i: write_action(w, i)))
    got = {}
    try:
        if w["tables"]:
            for tt in w["tables"].values():
                rb = s.sql(f"SELECT id, v FROM {tt} ORDER BY id")
                rec["type"] = rb.schema.fields[1].dataType.simpleString()
                got.update(rows_by_id(rb))
        elif door not in SEPARATE or rec["batch"] == "ok":
            rb = s.sql(f"SELECT id, v FROM {t} ORDER BY id")
            rec["type"] = rb.schema.fields[1].dataType.simpleString()
            got.update(rows_by_id(rb))
    except Exception as e:
        rec["readback"] = error_record(e)
    for i, e in rec.get("row_errors", {}).items():
        got[i] = e
    rec["rows"] = got
    return rec


for src in SOURCES:
    view = f"lc.ns.src_{src[0].lower()}"
    if only is None or "source" in only:
        records.extend(source_cells(src, view))
    for p, sc in TARGETS:
        for door in READ_DOORS + WRITE_DOORS + EXTRA_DOORS:
            if only is not None and door not in only:
                continue
            try:
                if door in READ_DOORS:
                    records.append(read_door(door, src, view, p, sc))
                else:
                    records.append(write_door(door, src, view, p, sc))
            except Exception as e:
                records.append({"key": f"{door}/{src}/{p},{sc}", "setup": error_record(e)})
            print(door, src, p, sc, flush=True)
if only is None or "selects" in only:
    for name, sql in SELECTS:
        rec = {"key": f"select/{name}", "sql": sql}
        try:
            frame = s.sql(sql)
            rec["types"] = [f.dataType.simpleString() for f in frame.schema.fields]
            rec["values"] = [str(x) for x in frame.collect()[0]]
        except Exception as e:
            rec.update(error_record(e))
        records.append(rec)
if only is None or "card" in only:
    for name, sql in CARD:
        t = f"lc.ns.card_{name}"
        rec = {"key": f"card/{name}", "sql": sql.format(t=t)}
        try:
            s.sql(f"CREATE TABLE {t} (id INT, v DECIMAL(10,2)) USING iceberg").collect()
            s.sql(sql.format(t=t)).collect()
            rec["write"] = "ok"
        except Exception as e:
            rec["write"] = error_record(e)
        try:
            rec["rows"] = rows_by_id(s.sql(f"SELECT id, v FROM {t} ORDER BY id"))
        except Exception as e:
            rec["readback"] = error_record(e)
        records.append(rec)
write_jsonl(out, records)
print(engine, ansi, Path(wh).name, len(records))

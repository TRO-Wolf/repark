import json
import sys
from pathlib import Path

import repark
from repark import ReparkSession

ROWS = (
    "(1, CAST(0.0 AS {ty})), (2, CAST(-0.0 AS {ty})), (3, CAST('NaN' AS {ty})), "
    "(4, CAST(NULL AS {ty})), (5, CAST(1.5 AS {ty}))"
)

warehouse = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2])
if warehouse.exists() and any(warehouse.iterdir()):
    raise SystemExit("warehouse not empty")
warehouse.mkdir(parents=True, exist_ok=True)
records = [{"native": repark._native.__file__}]
session = (
    ReparkSession.builder.appName("nan-partition-step0")
    .config("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hd.type", "hadoop")
    .config("spark.sql.catalog.hd.warehouse", str(warehouse))
    .getOrCreate()
)


def run(cid: str, text: str) -> None:
    try:
        rows = [list(r) for r in session.sql(text).collect()]
        rec = {
            "cell": cid,
            "sql": text,
            "rows": json.loads(
                json.dumps(rows, default=str)
                .replace("-Infinity", '"-Infinity"')
                .replace("NaN", '"NaN"')
                .replace(" Infinity", ' "Infinity"')
                .replace("[Infinity", '["Infinity"')
            ),
        }
    except Exception as exc:
        rec = {"cell": cid, "sql": text, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    records.append(rec)
    print(json.dumps(rec, default=str))


session.sql("CREATE NAMESPACE IF NOT EXISTS hd.ns")
for ty in ("DOUBLE", "FLOAT"):
    vals = ROWS.format(ty=ty)
    for kind, part in (("part", " PARTITIONED BY (d)"), ("flat", "")):
        name = f"hd.ns.rp_{ty.lower()}_{kind}"
        session.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg{part}")
        session.sql(f"INSERT INTO {name} VALUES {vals}")
        run(f"rows|{ty}|{kind}", f"SELECT id, d FROM {name} ORDER BY id")
        run(f"isnan|{ty}|{kind}", f"SELECT id FROM {name} WHERE isnan(d) ORDER BY id")
        run(
            f"eqnan|{ty}|{kind}", f"SELECT id FROM {name} WHERE d = CAST('NaN' AS {ty}) ORDER BY id"
        )
        run(f"gt1|{ty}|{kind}", f"SELECT id FROM {name} WHERE d > 1.0 ORDER BY id")
        run(f"isnull|{ty}|{kind}", f"SELECT id FROM {name} WHERE d IS NULL ORDER BY id")
        run(f"count|{ty}|{kind}", f"SELECT count(*) FROM {name}")
        if kind == "part":
            run(f"files|{ty}|{kind}", f"SELECT partition, record_count FROM {name}.files")
            run(
                f"partitions|{ty}|{kind}",
                f"SELECT partition, record_count, file_count FROM {name}.partitions",
            )
for ty in ("DOUBLE", "FLOAT"):
    vals = ROWS.format(ty=ty)
    select = f"SELECT CAST(id AS BIGINT) AS id, d FROM (VALUES {vals}) AS t(id, d)"
    for door in ("ctas", "writeto", "saveastable"):
        name = f"hd.ns.rp_{ty.lower()}_{door}"
        try:
            if door == "ctas":
                session.sql(f"CREATE TABLE {name} USING iceberg PARTITIONED BY (d) AS {select}")
            else:
                session.sql(
                    f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg PARTITIONED BY (d)"
                )
                frame = session.sql(select)
                if door == "writeto":
                    frame.writeTo(name).append()
                else:
                    frame.write.format("iceberg").mode("append").saveAsTable(name)
        except Exception as exc:
            records.append(
                {"cell": f"write|{ty}|{door}", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
            )
            print(records[-1])
            continue
        run(f"isnan|{ty}|{door}", f"SELECT id FROM {name} WHERE isnan(d) ORDER BY id")
        run(f"files|{ty}|{door}", f"SELECT partition, record_count FROM {name}.files")
name = "hd.ns.rp_double_inf"
session.sql(f"CREATE TABLE {name} (id BIGINT, d DOUBLE) USING iceberg PARTITIONED BY (d)")
session.sql(
    f"INSERT INTO {name} VALUES (6, CAST('Infinity' AS DOUBLE)), (7, CAST('-Infinity' AS DOUBLE))"
)
run("rows|DOUBLE|inf", f"SELECT id, d FROM {name} ORDER BY id")
run("files|DOUBLE|inf", f"SELECT partition, record_count FROM {name}.files")
for ty, vals in (("INT", "(1, 7), (2, NULL)"), ("STRING", "(1, 'a'), (2, NULL)")):
    name = f"hd.ns.rp_{ty.lower()}_part"
    session.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg PARTITIONED BY (d)")
    session.sql(f"INSERT INTO {name} VALUES {vals}")
    run(f"rows|{ty}|part", f"SELECT id, d FROM {name} ORDER BY id")
    run(f"isnull|{ty}|part", f"SELECT id FROM {name} WHERE d IS NULL ORDER BY id")
    run(f"files|{ty}|part", f"SELECT partition, record_count FROM {name}.files")
out.write_text("".join(json.dumps(r, default=str) + "\n" for r in records))
session.stop()

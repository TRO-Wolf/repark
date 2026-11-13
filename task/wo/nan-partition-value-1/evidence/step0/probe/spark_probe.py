import json
import math
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

ROWS = (
    "(1, CAST(0.0 AS {ty})), (2, CAST(-0.0 AS {ty})), (3, CAST('NaN' AS {ty})), "
    "(4, CAST(NULL AS {ty})), (5, CAST(1.5 AS {ty}))"
)

warehouse = Path(sys.argv[1]).resolve()
cross = Path(sys.argv[2]).resolve()
out = Path(sys.argv[3])
jar = sys.argv[4]
if warehouse.exists() and any(warehouse.iterdir()):
    raise SystemExit("warehouse not empty")
warehouse.mkdir(parents=True, exist_ok=True)
session = (
    SparkSession.builder.master("local[1]")
    .appName("nan-partition-step0-spark")
    .config("spark.jars", jar)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.sp", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.sp.type", "hadoop")
    .config("spark.sql.catalog.sp.warehouse", str(warehouse))
    .config("spark.sql.catalog.rp", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rp.type", "hadoop")
    .config("spark.sql.catalog.rp.warehouse", str(cross))
    .getOrCreate()
)
session.sparkContext.setLogLevel("ERROR")
records = [{"spark": session.version}]


def norm(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, float) and math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if hasattr(value, "asDict"):
        return {k: norm(v) for k, v in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [norm(v) for v in value]
    return value


def run(cid: str, text: str) -> None:
    try:
        rows = [norm(list(r)) for r in session.sql(text).collect()]
        rec = {"cell": cid, "sql": text, "rows": rows}
    except Exception as exc:
        rec = {"cell": cid, "sql": text, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    records.append(rec)
    print(json.dumps(rec, default=str))


def checks(prefix: str, name: str, ty: str, kind: str) -> None:
    run(f"rows|{ty}|{kind}|{prefix}", f"SELECT id, d FROM {name} ORDER BY id")
    run(f"isnan|{ty}|{kind}|{prefix}", f"SELECT id FROM {name} WHERE isnan(d) ORDER BY id")
    run(
        f"eqnan|{ty}|{kind}|{prefix}",
        f"SELECT id FROM {name} WHERE d = CAST('NaN' AS {ty}) ORDER BY id",
    )
    run(f"gt1|{ty}|{kind}|{prefix}", f"SELECT id FROM {name} WHERE d > 1.0 ORDER BY id")
    run(f"isnull|{ty}|{kind}|{prefix}", f"SELECT id FROM {name} WHERE d IS NULL ORDER BY id")
    run(f"count|{ty}|{kind}|{prefix}", f"SELECT count(*) FROM {name}")
    if kind != "flat":
        run(
            f"files|{ty}|{kind}|{prefix}",
            f"SELECT partition, record_count FROM {name}.files ORDER BY record_count, partition",
        )


session.sql("CREATE NAMESPACE IF NOT EXISTS sp.ns")
for ty in ("DOUBLE", "FLOAT"):
    vals = ROWS.format(ty=ty)
    for kind, part in (("part", " PARTITIONED BY (d)"), ("flat", "")):
        name = f"sp.ns.sp_{ty.lower()}_{kind}"
        session.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg{part}")
        session.sql(f"INSERT INTO {name} VALUES {vals}")
        checks("spark-own", name, ty, kind)
name = "sp.ns.sp_double_inf"
session.sql(f"CREATE TABLE {name} (id BIGINT, d DOUBLE) USING iceberg PARTITIONED BY (d)")
session.sql(
    f"INSERT INTO {name} VALUES (6, CAST('Infinity' AS DOUBLE)), (7, CAST('-Infinity' AS DOUBLE))"
)
run("rows|DOUBLE|inf|spark-own", f"SELECT id, d FROM {name} ORDER BY id")
run(
    "files|DOUBLE|inf|spark-own",
    f"SELECT partition, record_count FROM {name}.files ORDER BY record_count, partition",
)
for ty, vals in (("INT", "(1, 7), (2, NULL)"), ("STRING", "(1, 'a'), (2, NULL)")):
    name = f"sp.ns.sp_{ty.lower()}_part"
    session.sql(f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg PARTITIONED BY (d)")
    session.sql(f"INSERT INTO {name} VALUES {vals}")
    run(f"rows|{ty}|part|spark-own", f"SELECT id, d FROM {name} ORDER BY id")
    run(
        f"files|{ty}|part|spark-own",
        f"SELECT partition, record_count FROM {name}.files ORDER BY record_count, partition",
    )
if (cross / "ns").is_dir():
    for table in sorted(p.name for p in (cross / "ns").iterdir()):
        ty = table.split("_")[1].upper()
        kind = table.split("_", 2)[2]
        if kind == "inf":
            run(
                f"rows|{ty}|{kind}|spark-reads-repark",
                f"SELECT id, d FROM rp.ns.{table} ORDER BY id",
            )
            run(
                f"files|{ty}|{kind}|spark-reads-repark",
                f"SELECT partition, record_count FROM rp.ns.{table}.files "
                "ORDER BY record_count, partition",
            )
        elif ty in ("DOUBLE", "FLOAT"):
            checks("spark-reads-repark", f"rp.ns.{table}", ty, kind)
        else:
            run(
                f"rows|{ty}|{kind}|spark-reads-repark",
                f"SELECT id, d FROM rp.ns.{table} ORDER BY id",
            )
            run(
                f"files|{ty}|{kind}|spark-reads-repark",
                f"SELECT partition, record_count FROM rp.ns.{table}.files "
                "ORDER BY record_count, partition",
            )
jvm = session._jvm
types = jvm.org.apache.iceberg.types.Types
parser = jvm.org.apache.iceberg.SingleValueParser
for label, value in (
    ("NaN", float("nan")),
    ("Infinity", float("inf")),
    ("-Infinity", float("-inf")),
):
    rec = {"cell": f"java-json|DOUBLE|{label}"}
    try:
        text = parser.toJson(types.DoubleType.get(), value)
        rec["toJson"] = text
        try:
            back = parser.fromJson(types.DoubleType.get(), text)
            rec["fromJson"] = (
                norm(back) if not (isinstance(back, float) and back == float("inf")) else "Infinity"
            )
            if isinstance(back, float) and back == float("-inf"):
                rec["fromJson"] = "-Infinity"
        except Exception as exc:
            rec["fromJson_error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    except Exception as exc:
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    records.append(rec)
    print(json.dumps(rec, default=str))
for token in ('"NaN"', "NaN"):
    rec = {"cell": f"java-json-parse|DOUBLE|{token}"}
    try:
        back = parser.fromJson(types.DoubleType.get(), token)
        rec["fromJson"] = norm(back)
    except Exception as exc:
        rec["fromJson_error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    records.append(rec)
    print(json.dumps(rec, default=str))
out.write_text("".join(json.dumps(r, default=str) + "\n" for r in records))
session.stop()

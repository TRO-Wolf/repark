import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

ENGINE, WHP, OUT, ANSI = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
DOORS = sys.argv[5].split(",") if len(sys.argv) > 5 and sys.argv[5] else None
JAR = os.environ.get("ICEBERG_SPARK_JAR", "")
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
SCRATCH = re.compile(r"_+repark_[a-z0-9_]*?_\d+|__rp_[a-z]+_\d+")
TARGETS = ["DATE", "BOOLEAN", "BIGINT", "DECIMAL(10,2)", "TIMESTAMP", "STRING"]
VALUES = ["1", "NULL", "-NULL", "CAST(NULL AS {T})", "-1.5D"]
SHAPES = ["lit", "derived", "frame", "overview", "cdf"]
PER_SOURCE = {"create", "createOrReplace", "parquet", "saveAsTable_ow"}

wh = tempfile.mkdtemp(prefix="ws-" + ENGINE + "-", dir=WHP)
if ENGINE == "spark":
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    builder = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", JAR)
        .config("spark.sql.extensions", EXT)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", wh + "/sw")
    )
else:
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark.spark import functions as sf

    print("NATIVE", native.__file__, flush=True)
    builder = Builder.builder.appName("wsweep")
s = (
    builder.config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.sc.type", "hadoop")
    .config("spark.sql.catalog.sc.warehouse", wh + "/sc")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ANSI)
    .getOrCreate()
)
if ENGINE == "spark":
    s.sparkContext.setLogLevel("ERROR")
s.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
records: list[dict[str, Any]] = []
counter = [0]


def fresh(target: str | None) -> str:
    counter[0] += 1
    name = f"sc.ns.t{counter[0]}"
    if target is not None:
        s.sql(f"CREATE TABLE {name} (id INT, c {target}) USING iceberg").collect()
    return name


def cdf_frame(value: str, target: str) -> Any:
    if value == "-NULL":
        return None
    if value == "1":
        return s.createDataFrame([(127, 1)], "id INT, c INT")
    if value == "NULL":
        return s.createDataFrame([(127, None)], "id INT, c STRING")
    if value == "-1.5D":
        return s.createDataFrame([(127, -1.5)], "id INT, c DOUBLE")
    return s.createDataFrame([(127, None)], f"id INT, c {target}")


def source(shape: str, value: str, target: str) -> Any:
    v = value.replace("{T}", target)
    if shape == "lit":
        return s.sql(f"SELECT 127 AS id, {v} AS c")
    if shape == "derived":
        return s.sql(f"SELECT id, c FROM (SELECT 127 AS id, {v} AS c) d")
    if shape == "frame":
        return s.range(1).select(sf.lit(127).cast("int").alias("id")).withColumn("c", sf.expr(v))
    if shape == "overview":
        s.sql(f"SELECT 127 AS id, {v} AS c").createOrReplaceTempView("wv")
        return s.table("wv")
    return cdf_frame(value, target)


def merge_into(df: Any, t: str) -> None:
    if ENGINE == "spark":
        df.alias("src").mergeInto(
            t, sf.expr(f"{t}.id = src.id")
        ).whenNotMatched().insertAll().merge()
    else:
        df.mergeInto(t, "id").whenNotMatched().insertAll().merge()


def sql_insert_view(df: Any, t: str) -> None:
    df.createOrReplaceTempView("wsrc")
    s.sql(f"INSERT INTO {t} SELECT * FROM wsrc").collect()


def sql_merge_view(df: Any, t: str) -> None:
    df.createOrReplaceTempView("wsrc")
    s.sql(f"MERGE INTO {t} x USING wsrc ON x.id = wsrc.id WHEN NOT MATCHED THEN INSERT *").collect()


ALL_DOORS: dict[str, Any] = {
    "append": lambda df, t: df.writeTo(t).append(),
    "overwrite": lambda df, t: df.writeTo(t).overwrite(sf.lit(True)),
    "overwritePartitions": lambda df, t: df.writeTo(t).overwritePartitions(),
    "create": lambda df, t: df.writeTo(t).using("iceberg").create(),
    "createOrReplace": lambda df, t: df.writeTo(t).using("iceberg").createOrReplace(),
    "insertInto": lambda df, t: df.write.insertInto(t),
    "insertInto_ow": lambda df, t: df.write.insertInto(t, overwrite=True),
    "saveAsTable_append": lambda df, t: df.write.mode("append").saveAsTable(t),
    "saveAsTable_ow": lambda df, t: df.write.mode("overwrite").saveAsTable(t),
    "mergeInto": merge_into,
    "sql_insert_view": sql_insert_view,
    "sql_merge_view": sql_merge_view,
    "parquet": None,
}


def text_of(error: Exception) -> tuple[str, str]:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return (found.group(1) if found else "-"), SCRATCH.sub("_SCRATCH_", text)[:300]


def read_back(t: str | None, path: str | None) -> dict[str, Any]:
    df = s.read.parquet(path) if path else s.table(t)
    rows = sorted(repr(tuple(r)) for r in df.collect())
    return {"rows": rows, "types": [ty for _, ty in df.dtypes]}


def cell(door: str, shape: str, value: str, target: str | None) -> None:
    key = "/".join([door, shape, value.replace("{T}", "T"), target or "-", ANSI])
    try:
        df = source(shape, value, target or "STRING")
    except Exception as error:
        cls, text = text_of(error)
        records.append({"cell": key, "source_error": cls, "text": text})
        return
    if df is None:
        records.append({"cell": key, "na": True})
        return
    path = None
    t = None
    if door == "parquet":
        counter[0] += 1
        path = f"{wh}/pq{counter[0]}"
    elif door == "create":
        t = fresh(None)
    else:
        t = fresh(target or "STRING")
    try:
        if door == "parquet":
            df.write.parquet(path)
        else:
            ALL_DOORS[door](df, t)
        records.append({"cell": key, "wrote": read_back(t, path)})
    except Exception as error:
        cls, text = text_of(error)
        records.append({"cell": key, "error": cls, "text": text})


for door in ALL_DOORS:
    if DOORS and door not in DOORS:
        continue
    for shape in SHAPES:
        for value in VALUES:
            if door not in PER_SOURCE:
                for target in TARGETS:
                    cell(door, shape, value, target)
            elif "{T}" in value:
                for typed in TARGETS:
                    cell(door, shape, value.replace("{T}", typed), None)
            else:
                cell(door, shape, value, None)
records.append({"cell": "done"})
Path(OUT).write_text("".join(json.dumps(record) + "\n" for record in records))
s.stop()

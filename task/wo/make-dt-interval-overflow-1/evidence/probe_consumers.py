from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any


def render(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return "NaN" if math.isnan(value) else repr(value)
    if isinstance(value, dt.timedelta):
        return f"td({value.days},{value.seconds},{value.microseconds})"
    if isinstance(value, Decimal):
        return f"Dec({value})"
    if isinstance(value, (list, tuple)):
        return [render(item) for item in value]
    if isinstance(value, dict):
        return {str(k): render(v) for k, v in sorted(value.items())}
    return f"{type(value).__name__}:{value}"


def first(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return f"{type(exc).__name__}: {text.splitlines()[0][:200]}"


def frame(fn: Any) -> dict[str, Any]:
    try:
        df = fn()
        types = [[f.name, f.dataType.simpleString(), bool(f.nullable)] for f in df.schema.fields]
        rows = [render(list(row)) for row in df.collect()]
    except Exception as exc:
        return {"kind": "error", "first": first(exc)}
    return {"kind": "ok", "types": types, "rows": rows}


def value(fn: Any) -> dict[str, Any]:
    try:
        return {"kind": "ok", "value": render(fn())}
    except Exception as exc:
        return {"kind": "error", "first": first(exc)}


def session(engine: str, ansi: str) -> tuple[Any, Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as sf
        from pyspark.sql import types as st

        spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark, sf, st
    from repark import ReparkSession
    from repark.spark import functions as sf
    from repark.spark import types as st

    spark = (
        ReparkSession.builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    return spark, sf, st


BASE = (
    "SELECT * FROM VALUES (1, 'a', 1.5D, TIMESTAMP'2024-01-02 02:03:04.5', '1 02:03:04.5'), "
    "(2, 'b', 2.5D, TIMESTAMP'2024-01-01 00:00:00', 'x') AS v(id, s, x, ts, txt)"
)


def cells(spark: Any, sf: Any, st: Any, work: Path) -> dict[str, Any]:
    spark.sql(BASE).createOrReplaceTempView("v")
    df = spark.sql(
        "SELECT id, s, x, ts - TIMESTAMP'2024-01-01 00:00:00' AS t, "
        "make_dt_interval(id, 2, 3, 4.5) AS m, txt FROM v"
    )
    dti = st.DayTimeIntervalType()
    out: dict[str, Any] = {}
    out["schema|simpleString"] = value(lambda: df.schema.simpleString())
    out["schema|json"] = value(lambda: df.schema.json())
    out["schema|fromJson(json) == schema"] = value(
        lambda: st.StructType.fromJson(json.loads(df.schema.json())) == df.schema
    )
    out["schema|fromJson(jsonValue) == schema"] = value(
        lambda: st.StructType.fromJson(df.schema.jsonValue()) == df.schema
    )
    out["types|fromDDL(simpleString)"] = value(
        lambda: st.DataType.fromDDL(df.schema.simpleString()).simpleString()
    )
    for ddl in (
        "interval day to second",
        "INTERVAL DAY TO SECOND",
        "t interval day to second",
        "struct<t:interval day to second>",
        "array<interval day to second>",
        "interval hour to minute",
        "interval day",
        "interval year to month",
    ):
        out[f"types|fromDDL({ddl})"] = value(lambda d=ddl: repr(st.DataType.fromDDL(d)))
    for name in ("interval day to second", "interval second", "interval year to month"):
        doc = {
            "type": "struct",
            "fields": [{"name": "t", "type": name, "nullable": True, "metadata": {}}],
        }
        out[f"types|fromJson(field {name})"] = value(lambda d=doc: repr(st.StructType.fromJson(d)))
    out["types|DayTimeIntervalType().simpleString"] = value(lambda: dti.simpleString())
    out["types|DayTimeIntervalType(1,2).simpleString"] = value(
        lambda: st.DayTimeIntervalType(1, 2).simpleString()
    )
    casts = {
        "lit(None).cast(schema[t])": lambda: df.select(
            "id", sf.lit(None).cast(df.schema["t"].dataType).alias("n")
        ),
        "t.cast(DayTimeIntervalType())": lambda: df.select("id", sf.col("t").cast(dti).alias("n")),
        "t.cast('interval day to second')": lambda: df.select(
            "id", sf.col("t").cast("interval day to second").alias("n")
        ),
        "t.cast('INTERVAL DAY TO SECOND')": lambda: df.select(
            "id", sf.col("t").cast("INTERVAL DAY TO SECOND").alias("n")
        ),
        "m.cast(DayTimeIntervalType())": lambda: df.select("id", sf.col("m").cast(dti).alias("n")),
        "id.cast(DayTimeIntervalType())": lambda: df.select(
            "id", sf.col("id").cast(dti).alias("n")
        ),
        "x.cast(DayTimeIntervalType())": lambda: df.select("id", sf.col("x").cast(dti).alias("n")),
        "txt.cast(DayTimeIntervalType())": lambda: df.select(
            "id", sf.col("txt").cast(dti).alias("n")
        ),
        "ts.cast(DayTimeIntervalType())": lambda: spark.table("v").select(
            "id", sf.col("ts").cast(dti).alias("n")
        ),
        "t.cast('string')": lambda: df.select("id", sf.col("t").cast("string").alias("n")),
        "t.try_cast(DayTimeIntervalType())": lambda: df.select(
            "id", sf.col("t").try_cast(dti).alias("n")
        ),
        "selectExpr CAST(t AS INTERVAL DAY TO SECOND)": lambda: df.selectExpr(
            "id", "CAST(t AS INTERVAL DAY TO SECOND) AS n"
        ),
        "sql CAST(NULL AS INTERVAL DAY TO SECOND)": lambda: spark.sql(
            "SELECT 1 AS id, CAST(NULL AS INTERVAL DAY TO SECOND) AS n"
        ),
    }
    for label, fn in casts.items():
        out[f"cast|{label}"] = frame(fn)
    td = dt.timedelta(days=1, seconds=7384, microseconds=500000)
    schema = st.StructType(
        [st.StructField("id", st.IntegerType(), True), st.StructField("t", dti, True)]
    )
    out["create|rows+df.schema"] = frame(
        lambda: spark.createDataFrame(df.select("id", "t").collect(), df.select("id", "t").schema)
    )
    out["create|timedelta+StructType"] = frame(
        lambda: spark.createDataFrame([(1, td), (2, None)], schema)
    )
    out["create|timedelta+DDL"] = frame(
        lambda: spark.createDataFrame([(1, td)], "id int, t interval day to second")
    )
    out["create|timedelta+fromJson"] = frame(
        lambda: spark.createDataFrame(
            [(1, td)], st.StructType.fromJson(json.loads(df.select("id", "t").schema.json()))
        )
    )
    out["describe|()"] = frame(lambda: df.describe())
    out["describe|(t)"] = frame(lambda: df.describe("t"))
    out["describe|(id, t)"] = frame(lambda: df.describe("id", "t"))
    out["summary|()"] = frame(lambda: df.select("id", "t").summary())
    out["summary|(count, min)"] = frame(lambda: df.select("id", "t").summary("count", "min"))
    out["na|fill('z')"] = frame(lambda: df.na.fill("z"))
    out["na|fill(0)"] = frame(lambda: df.na.fill(0))
    out["na|fillna('z', [t])"] = frame(lambda: df.fillna("z", ["t"]))
    out["na|drop()"] = frame(lambda: df.na.drop())
    out["toPandas|dtypes"] = value(lambda: [str(d) for d in df.toPandas().dtypes])
    out["toPandas|t"] = value(lambda: [str(v) for v in df.toPandas()["t"].tolist()])
    out["dtypes"] = value(lambda: df.dtypes)
    other = spark.sql("SELECT 3 AS id, 'c' AS s")
    out["union|unionByName(missing)"] = frame(
        lambda: df.select("id", "s", "t").unionByName(other, allowMissingColumns=True)
    )
    out["union|other.unionByName(df, missing)"] = frame(
        lambda: other.unionByName(df.select("id", "s", "t"), allowMissingColumns=True)
    )
    out["union|union(self)"] = frame(lambda: df.select("t").union(df.select("t")))
    out["udf|returnType=DayTimeIntervalType"] = frame(
        lambda: df.select("id", sf.udf(lambda i: dt.timedelta(days=i), dti)("id").alias("u"))
    )
    out["udf|returnType=schema[t]"] = frame(
        lambda: df.select(
            "id", sf.udf(lambda i: dt.timedelta(days=i), df.schema["t"].dataType)("id").alias("u")
        )
    )
    out["udf|returnType='interval day to second'"] = frame(
        lambda: df.select(
            "id", sf.udf(lambda i: dt.timedelta(days=i), "interval day to second")("id").alias("u")
        )
    )
    out["udf|input t"] = frame(
        lambda: df.select("id", sf.udf(lambda v: str(v), "string")("t").alias("u"))
    )
    pq_dir = work / "w.parquet"
    shutil.rmtree(pq_dir, ignore_errors=True)
    out["write|parquet+read"] = frame(
        lambda: (df.select("id", "t").write.parquet(str(pq_dir)), spark.read.parquet(str(pq_dir)))[
            1
        ]
    )
    out["write|parquet+read(schema)"] = frame(
        lambda: spark.read.schema(df.select("id", "t").schema).parquet(str(pq_dir))
    )
    js_dir = work / "w.json"
    shutil.rmtree(js_dir, ignore_errors=True)
    out["write|json+read(schema)"] = frame(
        lambda: (
            df.select("id", "t").write.json(str(js_dir)),
            spark.read.schema("id int, t interval day to second").json(str(js_dir)),
        )[1]
    )
    out["fn|from_json(schema with t)"] = frame(
        lambda: spark.sql("SELECT '{\"t\": null}' AS j").select(
            sf.from_json("j", "t interval day to second").alias("p")
        )
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("repark", "spark"), required=True)
    parser.add_argument("--ansi", choices=("false", "true"), required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    spark, sf, st = session(args.engine, args.ansi)
    payload: dict[str, Any] = {"engine": args.engine, "ansi": args.ansi}
    if args.engine == "repark":
        import repark._native

        payload["native_file"] = repark._native.__file__
    payload["cells"] = cells(spark, sf, st, args.work)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"cells={len(payload['cells'])}")


if __name__ == "__main__":
    main()

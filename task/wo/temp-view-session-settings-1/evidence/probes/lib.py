import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")


def build(engine: str, wh: str, jar: str, ansi: str) -> tuple:
    if engine == "spark":
        import pyspark
        from pyspark.sql import SparkSession as Builder

        builder = (
            Builder.builder.master("local[1]")
            .appName("vs-step0")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.extensions", EXT)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", wh + "/spark-warehouse")
            .config("spark.sql.catalog.mc", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog.mc.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog"
            )
            .config("spark.sql.catalog.mc.warehouse", wh + "/mc")
        )
        prov = {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name}
    else:
        import repark
        import repark._native as native
        from repark import ReparkSession as Builder
        from repark.spark.session import _reset_active_session_for_tests

        print("NATIVE " + native.__file__, flush=True)
        _reset_active_session_for_tests()
        builder = Builder.builder.appName("vs-step0")
        prov = {
            "engine": "repark",
            "version": getattr(repark, "__version__", "?"),
            "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
        }
    builder = (
        builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if ansi != "unset":
        builder = builder.config("spark.sql.ansi.enabled", ansi)
    session = builder.getOrCreate()
    if engine != "spark":
        session.register_memory_catalog("mc", wh + "/mc")
    return session, prov


def norm(v: object) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, dict):
        return (
            "{"
            + ",".join(
                norm(k) + ":" + norm(x) for k, x in sorted(v.items(), key=lambda p: str(p[0]))
            )
            + "}"
        )
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(norm(x) for x in v) + "]"
    return str(v)


def answer(frame: Any) -> dict:
    rows = sorted("|".join(norm(x) for x in r) for r in frame.collect())
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"rows": rows, "types": types}


def error(e: BaseException) -> dict:
    text = " ".join(str(e).split())
    found = COND.search(text)
    return {"error": found.group(1) if found else "", "text": text[:260]}


def attempt(run: Any) -> dict:
    try:
        return run()
    except Exception as e:
        return error(e)


class Log:
    def __init__(self, path: str, prov: dict) -> None:
        self.path = Path(path)
        self.path.write_text("")
        self.put({"cell": "provenance", **prov})

    def put(self, record: dict) -> None:
        with self.path.open("a") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def close(self) -> None:
        return

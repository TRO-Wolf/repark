import contextlib
import io
import os
import re
import tempfile
from typing import Any

JAR = os.environ.get("ICEBERG_JAR", "")
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")


def session(engine: str, parent: str, ansi: str, wh: str | None = None) -> tuple[Any, str, Any]:
    if wh is None:
        wh = tempfile.mkdtemp(prefix="ih-" + engine + "-", dir=parent)
    if engine == "spark":
        import pyspark.sql.functions as functions
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", JAR)
            .config("spark.sql.extensions", EXT)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", wh + "/sw")
        )
    else:
        import repark._native as native
        import repark.spark.functions as functions
        from repark import ReparkSession

        print("NATIVE", native.__file__, flush=True)
        builder = ReparkSession.builder.appName("ice-held").config("repark.display.style", "spark")
    s = (
        builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    if engine == "spark":
        s.sparkContext.setLogLevel("ERROR")
    return s, wh, functions


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return found.group(1) if found else text[:160]


def rows(frame: Any) -> list[str]:
    return sorted(repr(tuple(row)) for row in frame.collect())


def shown(frame: Any) -> list[str]:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.show(50, False)
    lines = [line for line in buffer.getvalue().splitlines() if line.startswith("|")]
    return sorted("".join(line.split()) for line in lines[1:])

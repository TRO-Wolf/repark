import json
import os
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

JAR = os.environ.get("ICEBERG_JAR", "")
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")


def session(engine: str, parent: str) -> tuple[Any, str]:
    wh = tempfile.mkdtemp(prefix="rp-" + engine + "-", dir=parent)
    if engine == "spark":
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
        from repark import ReparkSession

        print("NATIVE", native.__file__, flush=True)
        builder = ReparkSession.builder.appName("reader-parse")
    s = (
        builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    if engine == "spark":
        s.sparkContext.setLogLevel("ERROR")
    return s, wh


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return found.group(1) if found else text[:140]


class Rec:
    def __init__(self, s: Any, out: str) -> None:
        self.s = s
        self.out = Path(out)
        self.out.write_text("")

    def conf(
        self, zone: str | None = None, ansi: str | None = None, ttype: str | None = None
    ) -> None:
        if zone is not None:
            self.s.sql("SET spark.sql.session.timeZone=" + zone).collect()
        if ansi is not None:
            self.s.sql("SET spark.sql.ansi.enabled=" + ansi).collect()
        if ttype is not None:
            self.s.sql("SET spark.sql.timestampType=" + ttype).collect()

    def emit(self, record: dict[str, Any]) -> None:
        with self.out.open("a") as handle:
            handle.write(json.dumps(record) + "\n")

    def fail(self, tag: str, error: BaseException) -> None:
        self.emit({"tag": tag, "create_error": condition(error)})

    def run(self, tag: str, build: Callable[[], Any]) -> None:
        try:
            frame = build()
            rows = frame.collect()
            got: dict[str, Any] = {"rows": sorted(repr(tuple(row)) for row in rows)}
            got["names"] = list(frame.columns)
            got["cols"] = sorted([str(value) for value in row] for row in rows)
            got["types"] = [kind for _, kind in frame.dtypes]
        except Exception as error:
            got = {"error": condition(error)}
        self.emit({"tag": tag, **got})

    def sql(self, tag: str, query: str) -> None:
        self.run(tag, lambda: self.s.sql(query))

from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def session(engine: str, ansi: str, wh: str, jar: str, extra: dict) -> tuple:
    if engine == "spark":
        import pyspark
        from pyspark.sql import SparkSession as Builder
        from pyspark.sql import Window
        from pyspark.sql import functions as sf

        builder = (
            Builder.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.extensions", EXT)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", str(Path(wh) / "spark-warehouse"))
        )
        prov = {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name}
    else:
        import repark
        import repark._native as native
        from repark import ReparkSession as Builder
        from repark import functions as sf
        from repark.spark.window import Window

        print("native", native.__file__, flush=True)
        builder = Builder.builder.appName("sum-step0")
        prov = {
            "engine": "repark",
            "version": getattr(repark, "__version__", "?"),
            "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
        }
    builder = (
        builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
    )
    for k, v in extra.items():
        builder = builder.config(k, v)
    return builder.getOrCreate(), sf, Window, prov


def err(e: Exception) -> str:
    cls = None
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(e, attr, None)
        if fn is not None:
            try:
                cls = fn()
            except Exception:
                cls = None
            if cls:
                break
    return f"ERR:{type(e).__name__}:{cls}:{' '.join(str(e).split())[:300]}"


def norm(v: object) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(norm(x) for x in v) + "]"
    return str(v)


def sort_key(row: list) -> tuple:
    out = []
    for x in row:
        try:
            out.append((0, Decimal(x), ""))
        except (InvalidOperation, ValueError):
            out.append((1, Decimal(0), x))
    return tuple(out)


def answer_of(frame: Any) -> dict:
    rows = [[norm(x) for x in r] for r in frame.collect()]
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"rows": sorted(rows, key=sort_key), "types": types}


def attempt(run: Callable[[], object]) -> object:
    try:
        return run()
    except Exception as e:
        return err(e)

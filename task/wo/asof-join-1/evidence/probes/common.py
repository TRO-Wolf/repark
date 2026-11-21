import json
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
TYPES = ("TIMESTAMP", "TIMESTAMP_NTZ", "DATE", "BIGINT", "INT", "DOUBLE", "DECIMAL(10,2)")


def session(engine: str, ansi: str, partitions: int = 1) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", str(partitions))
        )
    else:
        import repark._native as native
        from repark import ReparkSession

        print("NATIVE", native.__file__, flush=True)
        builder = ReparkSession.builder.appName("asof-join")
    s = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    if engine == "spark":
        s.sparkContext.setLogLevel("FATAL")
    return s


def modules(engine: str) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import functions
        from pyspark.sql.window import Window
    else:
        from repark.spark import functions
        from repark.spark.window import Window
    return functions, Window


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return type(error).__name__ + " " + (found.group(1) if found else text[:160])


def text_of(value: Any) -> str:
    if isinstance(value, float) and value != value:
        return "NaN"
    return str(value)


class Rec:
    def __init__(self, out: str) -> None:
        self.out = Path(out)
        self.out.write_text("")

    def emit(self, record: dict[str, Any]) -> None:
        with self.out.open("a") as handle:
            handle.write(json.dumps(record) + "\n")

    def run(self, key: dict[str, Any], build: Callable[[], Any]) -> None:
        start = time.perf_counter()
        try:
            frame = build()
            rows = frame.collect()
            got: dict[str, Any] = {
                "rows": sorted(json.dumps([text_of(v) for v in row]) for row in rows),
                "names": list(frame.columns),
                "types": [kind for _, kind in frame.dtypes],
            }
        except Exception as error:
            got = {"error": condition(error)}
        got["ms"] = round((time.perf_counter() - start) * 1000, 1)
        self.emit({**key, **got})

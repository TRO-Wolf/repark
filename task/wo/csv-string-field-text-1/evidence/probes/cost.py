import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import repark._native as native
from repark import ReparkSession
from repark.spark.session import reader as reader_module

case, variant, label, data, out = sys.argv[1:6]
if case == "gen":
    with Path(data, "is.csv").open("w") as handle:
        handle.writelines(f"{i},s{i % 1000:03d}\n" for i in range(2_000_000))
    sys.exit(0)
s = (
    ReparkSession.builder.appName("csv-string-cost")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
K = str(Path(data, "k.csv"))
IS = str(Path(data, "is.csv"))
wrap: Callable[[Any], Any] = getattr(reader_module, "_reader_parse", lambda frame: frame)


def read(path: str, schema: str | None, opts: dict[str, str]) -> Any:
    reader = s.read
    if schema is not None:
        reader = reader.schema(schema)
    for key, value in opts.items():
        reader = reader.option(key, value)
    if variant == "base":
        return reader.csv(path)
    reader._format = "csv"
    frame = s.read_csv(path, {"header": "false", "utf8_columns": "column_1"})
    shaped = reader._apply_reader_schema_semantics(
        frame, infer_schema=False, header=False, path=path
    )
    return wrap(shaped)


def k0_count() -> object:
    return read(K, "n INT, ts TIMESTAMP", {}).count()


def k0_hour() -> object:
    frame = read(K, "n INT, ts TIMESTAMP", {}).filter("n > 1000000")
    return frame.selectExpr("max(hour(ts))").collect()


def is_count() -> object:
    return read(IS, "n INT, s STRING", {}).count()


def is_sum() -> object:
    return read(IS, "n INT, s STRING", {}).selectExpr("sum(n)", "max(s)").collect()


def is_none() -> object:
    return read(IS, None, {}).selectExpr("count(_c0)", "max(_c1)").collect()


def is_infer() -> object:
    frame = s.read.option("inferSchema", "true").csv(IS)
    return frame.selectExpr("sum(_c0)", "max(_c1)").collect()


CASES = {
    "k0_count": k0_count,
    "k0_hour": k0_hour,
    "is_count": is_count,
    "is_sum": is_sum,
    "is_none": is_none,
    "is_infer": is_infer,
}
action = CASES[case]
got = action()
reps = 5
start = time.perf_counter()
for _ in range(reps):
    got = action()
seconds = (time.perf_counter() - start) / reps
record = {
    "case": case,
    "variant": variant,
    "label": label,
    "seconds": seconds,
    "answer": str(got)[:200],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as handle:
    handle.write(json.dumps(record) + "\n")

import json
import sys
from pathlib import Path
from typing import Any

engine, data, out = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
if engine == "spark":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    import repark._native as native
    from repark import ReparkSession

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("csv-wiring-fix1")
spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
if engine == "spark":
    spark.sparkContext.setLogLevel("ERROR")


def put(name: str, text: str) -> str:
    path = data / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))
    return str(path)


case_dup = put("case_dup.csv", "x,X,k\n1,2,3\n")
unicode_case = put("unicode_case.csv", "ä,Ä,k\n1,2,3\n")
triple = put("triple.csv", "a,A,a\n1,2,3\n")
c0_clash = put("c0_clash.csv", "_c1,,k\n1,2,3\n")
x0_clash = put("x0_clash.csv", "x,X,x0\n1,2,3\n")
modes = put("modes.csv", "a,b\n1,x\nabc,y\n3,z\n")
HEADER = {"header": "true"}
CLEAN = "a STRING, b STRING, _corrupt_record STRING"
OTHER_CASE = "a STRING, b STRING, _CORRUPT_RECORD STRING"
CELLS: dict[str, tuple[str, dict[str, str], Any]] = {
    "names/case_dup": ("false", {}, lambda r: r.options(**HEADER).csv(case_dup)),
    "names/case_dup/sensitive": ("true", {}, lambda r: r.options(**HEADER).csv(case_dup)),
    "names/case_dup/sensitive_select": (
        "true",
        {},
        lambda r: r.options(**HEADER).csv(case_dup).select("x"),
    ),
    "names/case_dup/sensitive_schema": (
        "true",
        {},
        lambda r: r.schema("x STRING, X STRING, k STRING").options(**HEADER).csv(case_dup),
    ),
    "names/unicode_case": ("false", {}, lambda r: r.options(**HEADER).csv(unicode_case)),
    "names/unicode_case/sensitive": ("true", {}, lambda r: r.options(**HEADER).csv(unicode_case)),
    "names/triple/sensitive_schema": (
        "true",
        {},
        lambda r: r.schema("p STRING, q STRING, k STRING").options(**HEADER).csv(triple),
    ),
    "names/c0_clash/schema": (
        "false",
        {},
        lambda r: r.schema("p STRING, q STRING").options(**HEADER).csv(c0_clash),
    ),
    "names/c0_clash/schema/sensitive": (
        "true",
        {},
        lambda r: r.schema("p STRING, q STRING").options(**HEADER).csv(c0_clash),
    ),
    "names/c0_clash/schema3": (
        "false",
        {},
        lambda r: r.schema("p INT, q INT, k INT").options(**HEADER).csv(c0_clash),
    ),
    "names/x0_clash/schema": (
        "false",
        {},
        lambda r: r.schema("p STRING, q STRING, k STRING").options(**HEADER).csv(x0_clash),
    ),
    "names/x0_clash/schema/sensitive": (
        "true",
        {},
        lambda r: r.schema("p STRING, q STRING, k STRING").options(**HEADER).csv(x0_clash),
    ),
    "corrupt/first": (
        "false",
        {},
        lambda r: r.schema("_corrupt_record STRING, a INT, b STRING").options(**HEADER).csv(modes),
    ),
    "corrupt/middle": (
        "false",
        {},
        lambda r: r.schema("a INT, _corrupt_record STRING, b STRING").options(**HEADER).csv(modes),
    ),
    "corrupt/last": (
        "false",
        {},
        lambda r: r.schema("a INT, b STRING, _corrupt_record STRING").options(**HEADER).csv(modes),
    ),
    "corrupt/last_noheader": (
        "false",
        {},
        lambda r: r.schema("a INT, b STRING, _corrupt_record STRING").csv(modes),
    ),
    "corrupt/int_typed": (
        "false",
        {},
        lambda r: r.schema("a INT, b STRING, _corrupt_record INT").options(**HEADER).csv(modes),
    ),
    "corrupt/clean_rows": (
        "false",
        {},
        lambda r: r.schema(CLEAN).options(**HEADER).csv(modes),
    ),
    "corrupt/option_default": (
        "false",
        {},
        lambda r: (
            r.schema("a INT, b STRING, _corrupt_record STRING")
            .options(columnNameOfCorruptRecord="_corrupt_record", **HEADER)
            .csv(modes)
        ),
    ),
    "corrupt/option_named": (
        "false",
        {},
        lambda r: (
            r.schema("a INT, b STRING, bad STRING")
            .options(columnNameOfCorruptRecord="bad", **HEADER)
            .csv(modes)
        ),
    ),
    "corrupt/conf_named": (
        "false",
        {"spark.sql.columnNameOfCorruptRecord": "bad"},
        lambda r: r.schema("a INT, b STRING, bad STRING").options(**HEADER).csv(modes),
    ),
    "corrupt/conf_named_default_field": (
        "false",
        {"spark.sql.columnNameOfCorruptRecord": "bad"},
        lambda r: r.schema(CLEAN).options(**HEADER).csv(modes),
    ),
    "corrupt/other_case": (
        "false",
        {},
        lambda r: r.schema(OTHER_CASE).options(**HEADER).csv(modes),
    ),
    "corrupt/other_case_int": (
        "false",
        {},
        lambda r: r.schema("a INT, b STRING, _CORRUPT_RECORD STRING").options(**HEADER).csv(modes),
    ),
    "corrupt/no_schema": ("false", {}, lambda r: r.options(**HEADER).csv(modes)),
}


def answer(frame: Any) -> dict[str, Any]:
    rows = sorted(repr(tuple(row)) for row in frame.collect())
    return {"names": list(frame.columns), "types": [kind for _, kind in frame.dtypes], "rows": rows}


def cell(build: Any) -> dict[str, Any]:
    try:
        return answer(build(spark.read))
    except Exception as error:
        return {"error": type(error).__name__, "text": " ".join(str(error).split())[:300]}


with Path(out).open("w") as handle:
    for ansi in ("true", "false"):
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        for tag, (sensitive, conf, build) in CELLS.items():
            spark.conf.set("spark.sql.caseSensitive", sensitive)
            for key, value in conf.items():
                spark.conf.set(key, value)
            record = {"ansi": ansi, "tag": tag, "caseSensitive": sensitive, **cell(build)}
            for key in conf:
                spark.conf.unset(key)
            handle.write(json.dumps(record) + "\n")
            handle.flush()
print("CELLS", len(CELLS) * 2, flush=True)

import json
import sys
from pathlib import Path
from typing import Any

engine, data, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
if engine == "spark":
    from pyspark.sql import SparkSession, functions, types

    builder = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    import repark._native as native
    from repark import ReparkSession
    from repark.spark import functions, types

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("csv-wiring-delta2-callers")
spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
if engine == "spark":
    spark.sparkContext.setLogLevel("ERROR")

AS_B = "\u0391\u03a3 \u0392"
AS_B_MEDIAL = "\u03b1\u03c3 \u03b2"
AS_B_FINAL = "\u03b1\u03c2 \u03b2"
HEADERS = {
    "space_medial": f"{AS_B},{AS_B_MEDIAL},k",
    "space": f"{AS_B},{AS_B_FINAL},k",
    "cjk": "\u4e2d\u03a3,\u4e2d\u03c3,k",
    "word": "\u039f\u0394\u039f\u03a3,\u03bf\u03b4\u03bf\u03c2,k",
    "digit": "\u0391\u03a31,\u03b1\u03c21,k",
    "hyphen": "\u0391\u03a3-\u0392,\u03b1\u03c2-\u03b2,k",
    "mark": "\u0391\u03a3\u0301\u0392,\u03b1\u03c2\u0301\u03b2,k",
    "dot": "\u0391.\u03a3,\u03b1.\u03c3,k",
}
paths: dict[str, str] = {}
for shape, line in HEADERS.items():
    path = data / f"h_{shape}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((line + "\n1,2,3\n").encode("utf-8"))
    paths[shape] = str(path)


def frame() -> Any:
    fields = [
        types.StructField(AS_B, types.IntegerType()),
        types.StructField("k", types.IntegerType()),
    ]
    return spark.createDataFrame([(None, 1), (2, 3)], types.StructType(fields))


def map_lookup() -> Any:
    key = functions.struct(functions.lit(1).alias(AS_B))
    probe = functions.struct(functions.lit(1).alias(AS_B_FINAL))
    return frame().select(functions.create_map(key, functions.lit("x"))[probe].alias("v"))


CELLS: dict[str, Any] = {}
for sensitive in ("true", "false"):
    conf = {"spark.sql.caseSensitive": sensitive}
    for shape in HEADERS:
        CELLS[f"csv_header/{shape}/cs_{sensitive}"] = (
            conf,
            lambda p=paths[shape]: spark.read.option("header", "true").csv(p),
        )
    CELLS[f"with_columns/distinct/cs_{sensitive}"] = (
        conf,
        lambda: frame().withColumns({AS_B: functions.lit(5), AS_B_MEDIAL: functions.lit(6)}),
    )
    CELLS[f"with_columns/repeat/cs_{sensitive}"] = (
        conf,
        lambda: frame().withColumns({AS_B: functions.lit(5), AS_B_FINAL: functions.lit(6)}),
    )
    CELLS[f"with_columns/cjk/cs_{sensitive}"] = (
        conf,
        lambda: frame().withColumns(
            {"\u4e2d\u03a3": functions.lit(5), "\u4e2d\u03c3": functions.lit(6)}
        ),
    )
    CELLS[f"fold_a/fillna_final/cs_{sensitive}"] = (
        conf,
        lambda: frame().fillna(0, subset=[AS_B_FINAL]),
    )
    CELLS[f"fold_a/fillna_medial/cs_{sensitive}"] = (
        conf,
        lambda: frame().fillna(0, subset=[AS_B_MEDIAL]),
    )
    CELLS[f"fold_a/dropna_final/cs_{sensitive}"] = (
        conf,
        lambda: frame().dropna(subset=[AS_B_FINAL]),
    )
    CELLS[f"fold_a/drop_column_final/cs_{sensitive}"] = (
        conf,
        lambda: frame().drop(functions.col(AS_B_FINAL)),
    )
    CELLS[f"fold_b/drop_name_final/cs_{sensitive}"] = (conf, lambda: frame().drop(AS_B_FINAL))
    CELLS[f"fold_b/drop_duplicates_final/cs_{sensitive}"] = (
        conf,
        lambda: frame().dropDuplicates([AS_B_FINAL]),
    )
    CELLS[f"fold_b/map_struct_key/cs_{sensitive}"] = (conf, map_lookup)


def answer(result: Any) -> dict[str, Any]:
    rows = sorted(repr(tuple(row)) for row in result.collect())
    kinds = [kind for _, kind in result.dtypes]
    return {"names": list(result.columns), "types": kinds, "rows": rows}


def run(build: Any) -> dict[str, Any]:
    try:
        return answer(build())
    except Exception as error:
        text = " ".join(str(error).split())
        return {"error": type(error).__name__, "text": text[:300]}


with out.open("w") as handle:
    for ansi in ("true", "false"):
        for tag, (conf, build) in CELLS.items():
            spark.conf.set("spark.sql.ansi.enabled", ansi)
            for key, value in conf.items():
                spark.conf.set(key, value)
            record = {"ansi": ansi, "tag": tag, **run(build)}
            spark.conf.set("spark.sql.caseSensitive", "false")
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
print("CELLS", len(CELLS) * 2, flush=True)

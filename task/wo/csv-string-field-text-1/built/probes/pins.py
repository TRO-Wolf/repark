import gzip
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

engine, out = sys.argv[1], sys.argv[2]
if engine == "spark":
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", sys.argv[3])
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )
    from pyspark.sql import types
else:
    import repark._native as native
    from repark import ReparkSession

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("pins")
    from repark.spark import types
root = Path(tempfile.mkdtemp(prefix="pins-" + engine + "-"))
spark = (
    builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
if engine == "spark":
    spark.sparkContext.setLogLevel("ERROR")


def put(name: str, text: str, raw: bytes | None = None) -> str:
    path = root / "files" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw if raw is not None else text.encode("utf-8"))
    return str(path)


card = put("card.csv", "007,1.50,1e3,TRUE\n")
card_h = put("card_h.csv", "a,b,c,d\n007,1.50,1e3,TRUE\n")
infer = put("infer.csv", "x,k\n007,abc\n")
n1 = put("n1.csv", "1e3,2.0\n1.5,7\n3,4\n")
mis = put("mis.csv", "abc,k1\n2147483648,k2\n42,k3\n 7 ,k4\n1e3,k5\n")
swap = put("swap.csv", "k,x\nabc,007\n")
part = put("part.csv", "x,z\n007,abc\n")
emptyname = put("emptyname.csv", ",k\n007,k1\n")
case = put("case.csv", "X,K\n007,abc\n")
w3h = put("w3h.csv", "a,b,c\n007,abc,zz\n")
w2h = put("w2h.csv", "x,k\n007,abc\n")
w3 = put("w3.csv", "007,abc,zz\n")
w2 = put("w2.csv", "007,abc\n")
late = put("late.csv", "".join(str(i) + ",k\n" for i in range(1, 1201)) + "abc,k\n1.5,k\n")
nullname = put("nullname.csv", "NA,k\n007,NA\n")
untrimmed = put("untrimmed.csv", " x , k \n007,abc\n")
dup = put("dup.csv", "x,X,k\n1,2,3\n")
gz = put("gz.csv.gz", "", gzip.compress(b"x,k\n007,1e3\n"))
semi = put("semi.csv", "x;k\n007;1e3\n")
bom = put("bom.csv", "", b"\xef\xbb\xbfx,k\n007,1e3\n")
put("glob/a.csv", "x,k\n007,k1\n")
put("glob/b.csv", "x,k\n1.50,k2\n")
put("two/a.csv", "x,k\n007,k1\n")
put("two/b.csv", "k,x\nk2,1e3\nk3,TRUE\n")
glob = str(root / "files" / "glob" / "*.csv")
two = str(root / "files" / "two")
dec25 = types.StructType(
    [types.StructField("x", types.DecimalType(2, 5)), types.StructField("k", types.StringType())]
)
dec5m2 = types.StructType(
    [types.StructField("x", types.DecimalType(5, -2)), types.StructField("k", types.StringType())]
)
S4 = "a STRING, b STRING, c STRING, d STRING"
LATE = "count(*) AS n", "count(x) AS nx", "max(CAST(x AS STRING)) AS mx"
CELLS = {
    "card/default": lambda r: r.csv(card),
    "card/infer_false": lambda r: r.option("inferSchema", "false").csv(card),
    "card/string": lambda r: r.schema(S4).csv(card),
    "card/load": lambda r: r.format("csv").schema(S4).load(card),
    "card/header_infer_true": lambda r: (
        r.schema(S4).option("header", "true").option("inferSchema", "true").csv(card_h)
    ),
    "infer/string": lambda r: (
        r.schema("x STRING, k STRING")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(infer)
    ),
    "infer/int": lambda r: (
        r.schema("x INT, k INT").option("header", "true").option("inferSchema", "true").csv(infer)
    ),
    "n1": lambda r: r.schema("a INT, b BIGINT").csv(n1),
    "misfit/int": lambda r: r.schema("x INT, k STRING").csv(mis),
    "misfit/boolean": lambda r: r.schema("x BOOLEAN, k STRING").csv(mis),
    "misfit/double": lambda r: r.schema("x DOUBLE, k STRING").csv(mis),
    "misfit/decimal": lambda r: r.schema("x DECIMAL(10,2), k STRING").csv(mis),
    "pair/swap_int": lambda r: r.schema("x INT, k STRING").option("header", "true").csv(swap),
    "pair/swap_string": lambda r: r.schema("x STRING, k STRING").option("header", "true").csv(swap),
    "pair/part_int": lambda r: r.schema("x INT, k STRING").option("header", "true").csv(part),
    "pair/part_string": lambda r: r.schema("x STRING, k STRING").option("header", "true").csv(part),
    "pair/emptyname_schema": lambda r: (
        r.schema("x STRING, k STRING").option("header", "true").csv(emptyname)
    ),
    "pair/case_string": lambda r: r.schema("x STRING, k STRING").option("header", "true").csv(case),
    "width/narrow_hdr": lambda r: r.schema("x STRING, k STRING").option("header", "true").csv(w3h),
    "width/narrow_hdr_int": lambda r: r.schema("x INT, k STRING").option("header", "true").csv(w3h),
    "width/wide_hdr": lambda r: (
        r.schema("x STRING, k STRING, z STRING").option("header", "true").csv(w2h)
    ),
    "width/narrow_off": lambda r: r.schema("x STRING, k STRING").csv(w3),
    "width/wide_off": lambda r: r.schema("x STRING, k STRING, z STRING").csv(w2),
    "width/wide_off_int": lambda r: r.schema("x INT, k STRING, z INT").csv(w2),
    "late/string": lambda r: r.schema("x STRING, k STRING").csv(late).selectExpr(*LATE),
    "late/int": lambda r: r.schema("x INT, k STRING").csv(late).selectExpr(*LATE),
    "late/none": lambda r: r.csv(late).withColumnRenamed("_c0", "x").selectExpr(*LATE),
    "names/empty": lambda r: r.option("header", "true").csv(emptyname),
    "names/null_token": lambda r: (
        r.option("header", "true").option("nullValue", "NA").csv(nullname)
    ),
    "names/duplicate": lambda r: r.option("header", "true").csv(dup),
    "names/untrimmed": lambda r: r.option("header", "true").csv(untrimmed),
    "names/untrimmed_schema": lambda r: (
        r.schema("x STRING, k STRING").option("header", "true").csv(untrimmed)
    ),
    "topo/file_url": lambda r: r.option("header", "true").csv(Path(infer).absolute().as_uri()),
    "topo/file_url_schema": lambda r: (
        r.schema("x STRING, k STRING").option("header", "true").csv(Path(infer).absolute().as_uri())
    ),
    "topo/gzip": lambda r: r.option("header", "true").option("compression", "gzip").csv(gz),
    "topo/sep": lambda r: r.option("header", "true").option("sep", ";").csv(semi),
    "topo/bom": lambda r: r.option("header", "true").csv(bom),
    "topo/glob": lambda r: r.option("header", "true").csv(glob),
    "topo/glob_schema": lambda r: r.schema("x STRING, k STRING").option("header", "true").csv(glob),
    "topo/two_schema": lambda r: r.schema("x STRING, k STRING").option("header", "true").csv(two),
    "topo/two_values": lambda r: r.option("header", "true").csv(two).toDF("p", "q"),
    "empty_value/empty": lambda r: r.option("header", "true").option("emptyValue", "").csv(infer),
    "empty_value/e": lambda r: r.option("header", "true").option("emptyValue", "E").csv(infer),
    "refuse/decimal_2_5": lambda r: r.schema(dec25).option("header", "true").csv(infer),
    "refuse/decimal_5_m2": lambda r: r.schema(dec5m2).option("header", "true").csv(infer),
    "refuse/char": lambda r: r.schema("x CHAR(5), k STRING").option("header", "true").csv(infer),
    "user_text_scan": lambda r: (
        r.option("header", "true")
        .option("inferSchema", "true")
        .option("text_scan", "true")
        .csv(infer)
    ),
}


def answer(frame: Any) -> dict[str, Any]:
    rows = sorted(repr(tuple(row)) for row in frame.collect())
    return {"names": list(frame.columns), "types": [kind for _, kind in frame.dtypes], "rows": rows}


def cell(tag: str, build: Any) -> dict[str, Any]:
    try:
        got = build()
    except Exception as error:
        got = {"error": type(error).__name__ + ": " + " ".join(str(error).split())[:240]}
    return {"tag": tag, **got}


records = []
for ansi in ("true", "false"):
    spark.sql("SET spark.sql.ansi.enabled=" + ansi).collect()
    for tag, build in CELLS.items():
        records.append({"ansi": ansi, **cell(tag, lambda build=build: answer(build(spark.read)))})
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lc.w").collect()
    for kind in ("STRING", "INT", "DECIMAL(10,2)"):
        name = "t" + kind.split("(")[0].lower() + ansi
        ddl = ", ".join(c + " " + kind for c in "abcd")
        source = spark.read.schema(ddl).csv(card)

        def write_to(source: Any = source, name: str = name) -> dict[str, Any]:
            source.writeTo("lc.w." + name + "_wt").tableProperty("format-version", "2").create()
            return answer(spark.sql("SELECT * FROM lc.w." + name + "_wt"))

        def insert(source: Any = source, name: str = name, ddl: str = ddl) -> dict[str, Any]:
            source.createOrReplaceTempView("src")
            spark.sql(
                "CREATE TABLE lc.w."
                + name
                + "_in ("
                + ddl
                + ") USING iceberg TBLPROPERTIES ('format-version'='2')"
            ).collect()
            spark.sql("INSERT INTO lc.w." + name + "_in SELECT * FROM src").collect()
            return answer(spark.sql("SELECT * FROM lc.w." + name + "_in"))

        records.append({"ansi": ansi, **cell("label/write_to/" + kind, write_to)})
        records.append({"ansi": ansi, **cell("label/insert/" + kind, insert)})
Path(out).write_text("".join(json.dumps(record) + "\n" for record in records))

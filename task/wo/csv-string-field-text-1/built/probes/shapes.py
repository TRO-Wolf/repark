import json
import sys
import tempfile
from pathlib import Path

engine, out = sys.argv[1], sys.argv[2]
if engine == "spark":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
else:
    import repark._native as native
    from repark import ReparkSession

    print("NATIVE", native.__file__, flush=True)
    spark = (
        ReparkSession.builder.appName("empty")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
root = Path(tempfile.mkdtemp(prefix="empty-"))
(root / "empty.csv").write_text("")
(root / "hdr.csv").write_text("x,k\n")
(root / "dir").mkdir()
(root / "dir" / "a.csv").write_text("")
(root / "dir" / "b.csv").write_text("x,k\n007,k1\n")
(root / "ml.csv").write_text('x,k\n"a\nb",007\n')
(root / "dup.csv").write_text("x,X,k\n1,2,3\n")
(root / "dup2.csv").write_text("x,x,k\n1,2,3\n")
(root / "ws.csv").write_text("x,k\n1,2\n3\n")
cells = {
    "empty/schema": lambda: spark.read.schema("x INT, k STRING").csv(str(root / "empty.csv")),
    "empty/none": lambda: spark.read.csv(str(root / "empty.csv")),
    "hdr/schema": lambda: (
        spark.read.schema("x INT, k STRING").option("header", "true").csv(str(root / "hdr.csv"))
    ),
    "hdr/none": lambda: spark.read.option("header", "true").csv(str(root / "hdr.csv")),
    "dir_first_empty/schema": lambda: (
        spark.read.schema("x STRING, k STRING").option("header", "true").csv(str(root / "dir"))
    ),
    "dir_first_empty/none": lambda: spark.read.option("header", "true").csv(str(root / "dir")),
    "multiline/schema": lambda: (
        spark.read.schema("x STRING, k INT")
        .option("header", "true")
        .option("multiLine", "true")
        .csv(str(root / "ml.csv"))
    ),
    "multiline/none": lambda: (
        spark.read.option("header", "true").option("multiLine", "true").csv(str(root / "ml.csv"))
    ),
    "dup_case/none": lambda: spark.read.option("header", "true").csv(str(root / "dup.csv")),
    "dup_exact/none": lambda: spark.read.option("header", "true").csv(str(root / "dup2.csv")),
    "short_row/schema": lambda: (
        spark.read.schema("x INT, k INT").option("header", "true").csv(str(root / "ws.csv"))
    ),
    "void/schema": lambda: (
        spark.read.schema("x VOID, k STRING").option("header", "true").csv(str(root / "hdr.csv"))
    ),
    "array/schema": lambda: (
        spark.read.schema("x ARRAY<INT>, k STRING")
        .option("header", "true")
        .csv(str(root / "hdr.csv"))
    ),
    "interval/schema": lambda: (
        spark.read.schema("x INTERVAL DAY TO SECOND, k STRING")
        .option("header", "true")
        .csv(str(root / "hdr.csv"))
    ),
    "time/schema": lambda: (
        spark.read.schema("x TIME, k STRING").option("header", "true").csv(str(root / "hdr.csv"))
    ),
}
with Path(out).open("w") as handle:
    for tag, build in cells.items():
        try:
            frame = build()
            got = {
                "names": list(frame.columns),
                "types": [k for _, k in frame.dtypes],
                "rows": sorted(repr(tuple(r)) for r in frame.collect()),
            }
        except Exception as error:
            got = {"error": type(error).__name__ + ": " + " ".join(str(error).split())[:200]}
        handle.write(json.dumps({"ansi": "true", "tag": tag, **got}) + "\n")

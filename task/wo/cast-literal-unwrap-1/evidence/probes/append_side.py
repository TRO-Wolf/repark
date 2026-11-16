import sys
import tempfile
from pathlib import Path

engine, parent = sys.argv[1:3]
root = Path(tempfile.mkdtemp(prefix=f"ap-{engine}-", dir=parent))
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    print("native", "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1])
    b = Builder.builder.appName("append-side")
s = b.config("spark.sql.session.timeZone", "UTC").getOrCreate()
for label, select in (
    ("string", "SELECT 1 AS id, 'a' AS c"),
    ("cast_string", "SELECT 1 AS id, CAST('1' AS STRING) AS c"),
    ("int", "SELECT 1 AS id, 7 AS c"),
):
    path = str(root / label)
    for attempt in (1, 2):
        try:
            s.sql(select).write.mode("append").parquet(path)
            print(label, "append", attempt, "ok")
        except Exception as error:
            print(
                label,
                "append",
                attempt,
                "RAISE",
                type(error).__name__,
                " ".join(str(error).split())[:220],
            )
    try:
        print(label, "rows", s.read.parquet(path).count())
    except Exception as error:
        print(label, "read RAISE", " ".join(str(error).split())[:160])

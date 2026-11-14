import json
import sys
from pathlib import Path

engine, ansi, out = sys.argv[1:4]
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
    print("pyspark", pyspark.__version__)
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("small")
    print(native.__file__)
s = (
    b.config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
if engine == "spark":
    jvm = s.sparkContext._jvm
    print("java", jvm.java.lang.System.getProperty("java.version"))
TS = "CAST(TIMESTAMP '2024-01-01 00:00:00.123455' AS TIMESTAMP)"
TS2 = "TIMESTAMP '2026-11-13 15:42:03.005'"
cells = {
    "round_1.005_2": "ROUND(CAST('1.005' AS DOUBLE), 2)",
    "round_0.285_2": "ROUND(CAST('0.285' AS DOUBLE), 2)",
    "round_1.255_2": "ROUND(CAST('1.255' AS DOUBLE), 2)",
    "round_2.675_2": "ROUND(CAST('2.675' AS DOUBLE), 2)",
    "round_-22.129547339983247_18": "ROUND(CAST('-22.129547339983247' AS DOUBLE), 18)",
    "round_float_1.255_2": "ROUND(CAST('1.255' AS FLOAT), 2)",
    "trycast_ts_dec20_0": f"TRY_CAST({TS2} AS DECIMAL(20,0))",
    "trycast_ts_dec38_18": f"TRY_CAST({TS} AS DECIMAL(38,18))",
    "trycast_ts_double": f"TRY_CAST({TS} AS DOUBLE)",
    "trycast_ts_bigint": f"TRY_CAST({TS} AS BIGINT)",
    "trycast_ts_int": f"TRY_CAST({TS} AS INT)",
    "cast_ts_dec20_0": f"CAST({TS2} AS DECIMAL(20,0))",
    "cast_ts_dec38_18": f"CAST({TS} AS DECIMAL(38,18))",
    "cast_ts_double": f"CAST({TS} AS DOUBLE)",
    "cast_ts_bigint": f"CAST({TS} AS BIGINT)",
}
res = {}
for name, expr in cells.items():
    try:
        res[name] = str(s.sql(f"SELECT {expr} AS c").collect()[0][0])
    except Exception as e:
        t = str(e).strip().splitlines()
        res[name] = "ERR " + (t[0][:160] if t else type(e).__name__)
    print(name, res[name])
Path(out).write_text(json.dumps(res, indent=1))

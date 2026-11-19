import sys
import repark._native as native
from repark import ReparkSession
from repark.spark.types import DataType

print("NATIVE", native.__file__)
s = ReparkSession.builder.appName("q2").config("spark.sql.session.timeZone", "America/Los_Angeles").getOrCreate()
p = sys.argv[1]
s.sql("SET spark.sql.timestampType=TIMESTAMP_NTZ").collect()
print("ddl", DataType.fromDDL("n INT, ts TIMESTAMP"))
for name, frame in (("csv", s.read.schema("n INT, ts TIMESTAMP").csv(p + "/csv")), ("json", s.read.schema("n INT, ts TIMESTAMP").json(p + "/json"))):
    frame.createOrReplaceTempView("x")
    print(name, frame.dtypes, s.sql("SELECT unix_timestamp(ts), hour(ts), CAST(ts AS STRING) FROM x").collect())

import math
import sys
from pathlib import Path

from pyspark.sql import SparkSession

root = Path(sys.argv[1])
spark = SparkSession.builder.master("local[4]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("spark", spark.version)
cells = [
    ("three_files", "SELECT max(d), min(d) FROM nan_minmax_src"),
    ("three_files", "SELECT max(f), min(f) FROM nan_minmax_src"),
    ("three_files", "SELECT min(d), max(id) FROM nan_minmax_src"),
    ("one_file", "SELECT max(d), min(d) FROM nan_minmax_src"),
    ("one_file", "SELECT max(f), min(f) FROM nan_minmax_src"),
    ("one_file", "SELECT min(d), max(id) FROM nan_minmax_src"),
    ("one_file", "SELECT max(k), min(k) FROM nan_minmax_src WHERE id >= 0"),
    ("dynamic", "SELECT max(d) FROM nan_minmax_src"),
]
for name, text in cells:
    spark.read.parquet(str(root / name)).createOrReplaceTempView("nan_minmax_src")
    answers = set()
    for _ in range(3):
        row = spark.sql(text).collect()[0]
        answers.add(tuple("NaN" if isinstance(v, float) and math.isnan(v) else v for v in row))
    print(name, "|", text, "|", sorted(answers), flush=True)

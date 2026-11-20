import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

root = Path(sys.argv[1])
out = Path(sys.argv[2])
files = root / "files"
files.mkdir(parents=True, exist_ok=True)

TYPES = [
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL(10,2)",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "STRING",
    "BINARY",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL DAY TO SECOND",
    "VARIANT",
    "DECIMAL(38,10)",
    "DECIMAL(3,0)",
    "DECIMAL(4,4)",
]
TEXTS = [
    " ",
    ".",
    "+NaN",
    "-NaN",
    "NaN ",
    " Inf",
    "+Infinity",
    "1D",
    "1e3f",
    "\u00017",
    "\u00a07",
    "\u20037",
    "fal\u017fe",
    "FAL\u017fE",
    "\u0661\u0662\u0663",
    "\uff11\uff12",
    "1e-400",
    "0x1.8p1",
    "-0x1p-2",
    "0X10P0",
    "0x1p3d",
    "1e",
    "-",
    "+",
    "1.2.3",
    "--1",
    ",",
    "1,,000",
    "-1,5",
    "2024-02-30",
    "2024-02-29",
    "GMT2024-01-15",
    "2024-01-15GMT",
    "2024-01-15 10:30:00GMT",
    "2024-01-15 10:30:00UTC",
    "2024-01-15T10:30:00.1234567",
    "+2024-01-15",
    "-0001-01-01",
    "12024-01-15",
    "1234567-01-01",
    "2024-1-5 garbage",
    "2024-01-15  10:30:00",
    "2024-01-15\t",
    "1e10",
    "-1e-3",
    "0.005",
    "-0.005",
    "0.0049999",
    "1E+2",
    "00.50",
    "1.0e0",
    "1e-2147483648",
    "1e2147483648",
    "1e9999999999",
    "-128",
    "-32768",
    "-2147483648",
    "-9223372036854775808",
    "0000000000000000000000000000007",
    "99999999999999999999999999999999999999",
    "999999999999999999999999999999999999999",
    "0.99995",
    "-0",
    "32767",
    "32768",
    "40000",
]


def field(text: str) -> str:
    if any(char in text for char in ',"'):
        return '"' + text.replace('"', '""') + '"'
    return text


s = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
s.sparkContext.setLogLevel("ERROR")
with out.open("w") as handle:
    for index, text in enumerate(TEXTS):
        path = files / f"a{index}.csv"
        path.write_text(field(text) + ",k\n", encoding="utf-8")
        for kind in TYPES:
            record = {"index": index, "text": text, "type": kind}
            try:
                frame = s.read.schema(f"x {kind}, k STRING").csv(str(path))
                rows = frame.selectExpr("CAST(x AS STRING) AS xs", "x IS NULL AS isnull").collect()
                record["xs"] = rows[0]["xs"]
                record["isnull"] = rows[0]["isnull"]
                try:
                    value = frame.select("x").collect()[0]["x"]
                    record["py"] = None if value is None else str(value)
                except Exception as error:
                    record["py_error"] = " ".join(str(error).split())[:120]
            except Exception as error:
                record["error"] = " ".join(str(error).split())[:160]
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
            handle.flush()
s.stop()
print("PROBE-DONE", flush=True)

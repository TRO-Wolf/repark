import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

ROWS = 2_000_000
QUERIES = {
    "K1 hour(c) STRING": "SELECT sum(hour(c)) FROM lc.db.big",
    "K1x hour(CAST(c AS TIMESTAMP))": "SELECT sum(hour(CAST(c AS TIMESTAMP))) FROM lc.db.big",
    "K2 hour(ts) TIMESTAMP": "SELECT sum(hour(ts)) FROM lc.db.big",
    "K3 year(c) STRING": "SELECT sum(year(c)) FROM lc.db.big",
    "K4 extract(HOUR FROM c)": "SELECT sum(extract(HOUR FROM c)) FROM lc.db.big",
    "K5 minute(c) STRING": "SELECT sum(minute(c)) FROM lc.db.big",
}


def main() -> None:
    parent, out_path, runs = sys.argv[1], sys.argv[2], int(sys.argv[3])
    print("NATIVE", native.__file__, flush=True)
    wh = tempfile.mkdtemp(prefix="cost-", dir=parent)
    s = (
        ReparkSession.builder.appName("string-hms-cost")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    s.sql("CREATE TABLE lc.db.big (k INT, c STRING, ts TIMESTAMP) USING iceberg").collect()
    s.sql(
        "INSERT INTO lc.db.big SELECT CAST(id AS INT) AS k, "
        "date_format(timestamp_seconds(1704067200 + id * 37), 'yyyy-MM-dd HH:mm:ss') AS c, "
        "timestamp_seconds(1704067200 + id * 37) AS ts FROM range(" + str(ROWS) + ")"
    ).collect()
    answers = {name: str(s.sql(q).collect()[0][0]) for name, q in QUERIES.items()}
    times: dict[str, list[float]] = {name: [] for name in QUERIES}
    for _ in range(runs):
        for name, query in QUERIES.items():
            started = time.perf_counter()
            s.sql(query).collect()
            times[name].append(time.perf_counter() - started)
    record = {
        "rows": ROWS,
        "runs": runs,
        "answers": answers,
        "median_s": {name: round(statistics.median(v), 4) for name, v in times.items()},
        "all_s": {name: [round(x, 4) for x in v] for name, v in times.items()},
    }
    Path(out_path).write_text(json.dumps(record, indent=1) + "\n")
    print(json.dumps(record["median_s"]), flush=True)


main()

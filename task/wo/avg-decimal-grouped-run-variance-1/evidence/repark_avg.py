import json
import os
import sys
from collections import Counter
from pathlib import Path

import cells

import repark._native as native
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests


def main() -> None:
    root, out_path, wh = sys.argv[1], Path(sys.argv[2]), sys.argv[3]
    steal, tp, runs, twins = sys.argv[4], int(sys.argv[5]), int(sys.argv[6]), sys.argv[7] == "1"
    only = sys.argv[8] if len(sys.argv) > 8 else "all"
    build = native.__file__.split("/")[-3]
    cpus = len(os.sched_getaffinity(0))
    print("native", native.__file__, "cpus", cpus, "steal", steal, "tp", tp, flush=True)
    with out_path.open("a") as out:
        for ansi in ("false", "true"):
            _reset_active_session_for_tests()
            session = (
                ReparkSession.builder.appName("avg-variance")
                .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.lc.type", "hadoop")
                .config("spark.sql.catalog.lc.warehouse", f"{wh}/{ansi}")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", ansi)
                .getOrCreate()
            )
            if steal == "off":
                session.sql(
                    "SET datafusion.execution.enable_file_stream_work_stealing = false"
                ).collect()
            if tp:
                session.sql(f"SET datafusion.execution.target_partitions = {tp}").collect()
            cells.register(session, root)

            def record(
                key: str, sql: str, seen: Counter[str], ansi: str = ansi, ms: int = 0
            ) -> None:
                row = {"engine": "repark", "build": build, "cpus": cpus, "steal": steal, "tp": tp}
                row |= {
                    "ansi": ansi,
                    "query": key,
                    "distinct": len(seen),
                    "answers": dict(seen),
                    "ms": ms,
                }
                out.write(json.dumps(row) + "\n")
                out.flush()

            cells.measure(session, "lc", runs, twins, record, only)
            session.stop()
    _reset_active_session_for_tests()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()

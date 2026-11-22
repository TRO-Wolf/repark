from __future__ import annotations

import json
import sys
from typing import Any

from _acceptance_held_frame import WRITER_TAG, spark_session


def reply(**fields: Any) -> None:
    print(WRITER_TAG, json.dumps(fields, sort_keys=True, default=str), flush=True)


def write(spark: Any, request: dict[str, str]) -> None:
    table = request["table"]
    try:
        spark.sql(f"INSERT INTO {table} VALUES {request['values']}").collect()
        count = spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0]
    except Exception as error:
        reply(kind=type(error).__name__, message=str(error)[:2000])
        return
    reply(count=count)


def main() -> int:
    spark = spark_session(json.loads(sys.stdin.readline())["conf"])
    reply(ready=True)
    for line in sys.stdin:
        if line.strip():
            write(spark, json.loads(line))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

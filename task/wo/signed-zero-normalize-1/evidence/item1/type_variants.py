import json
import sys

import pyarrow as pa

import repark
from repark import ReparkSession, functions

session = ReparkSession.builder.appName("sz-types").getOrCreate()
values = [1.5, None, float("nan"), -0.0, 2.5, 0.0, None, 1.5]
ids = list(range(1, 9))
arrays = {
    "float64": pa.array(values, type=pa.float64()),
    "float32": pa.array(values, type=pa.float32()),
    "float16": None,
    "dict_float64": pa.array(
        [1.5, 7.0, float("nan"), -0.0, 2.5, 0.0, 7.0, 1.5], type=pa.float64()
    ).dictionary_encode(),
    "ree_float64": None,
    "polars_float64": pa.array(values, type=pa.float64()),
    "polars_float32": pa.array(values, type=pa.float32()),
}
try:
    import numpy as np

    arrays["float16"] = pa.array(
        [None if v is None else np.float16(v) for v in values], type=pa.float16()
    )
except BaseException as exc:
    arrays["float16"] = repr(exc)[:120]
try:
    arrays["ree_float64"] = pa.compute.run_end_encode(pa.array(values, type=pa.float64()))
except BaseException as exc:
    arrays["ree_float64"] = repr(exc)[:120]
out = []
for name, array in arrays.items():
    for pred, typed in (
        ("d = 0.0", lambda c: c == 0.0),
        ("d = -0.0", lambda c: c == -0.0),
        ("d = 0", lambda c: c == 0),
    ):
        for route in ("filter-str", "typed", "sql"):
            rec = {"variant": name, "pred": pred, "route": route}
            try:
                if isinstance(array, str) or array is None:
                    raise RuntimeError(f"input not built: {array}")
                table = pa.table({"id": pa.array(ids, type=pa.int64()), "d": array})
                if name.startswith("polars"):
                    import polars as pl

                    frame = session.createDataFrame(pl.from_arrow(table))
                else:
                    frame = session.createDataFrame(table.to_pandas())
                rec["dtype"] = str(frame.schema["d"].dataType)
                if route == "filter-str":
                    res = frame.filter(pred).select("id")
                elif route == "typed":
                    res = frame.filter(typed(functions.col("d"))).select("id")
                else:
                    frame.createOrReplaceTempView("tv_types")
                    res = session.sql(f"SELECT id FROM tv_types WHERE {pred}")
                rec["rows"] = sorted(r[0] for r in res.collect())
                rec["outcome"] = "ANSWERED"
            except BaseException as exc:
                rec["outcome"] = "REFUSED"
                rec["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
            out.append(rec)
for rec in out:
    print(json.dumps(rec))
print("repark", repark.__file__, file=sys.stderr)

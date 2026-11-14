from __future__ import annotations

import collections
import sys
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa

import repark
from repark import ReparkSession
from repark._native import PyReparkSession

folder = sys.argv[1]
print("native", Path(repark._native.__file__).parent.parent.name, flush=True)
spark = ReparkSession.builder.appName("umbrella").getOrCreate()
native = PyReparkSession.native()
Q = "SELECT max(d), min(d) FROM t"
A = "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown"
U = "datafusion.optimizer.enable_dynamic_filter_pushdown"


def spark_state(label: str) -> None:
    spark.read.parquet(folder).createOrReplaceTempView("t")
    c = collections.Counter(str(spark.sql(Q).toArrow().to_pylist()[0]) for _ in range(10))
    plan = "\n".join(str(v) for r in spark.sql("EXPLAIN ANALYZE " + Q).collect() for v in r)
    shows = {k: spark.conf.get(k, None) for k in (A, U)}
    print("spark", label, dict(c), "DynamicFilter" in plan, shows, flush=True)


def native_state(label: str) -> None:
    native.create_or_replace_temp_view("t", native.read_parquet(folder))
    c = collections.Counter(str(pa.table(native.sql(Q)).to_pylist()[0]) for _ in range(10))
    plan = "\n".join(
        str(v) for r in pa.table(native.sql("EXPLAIN ANALYZE " + Q)).to_pylist() for v in r.values()
    )
    print("native", label, dict(c), "DynamicFilter" in plan, flush=True)


def attempt(fn: Callable[[], object], label: str) -> None:
    try:
        fn()
        print(label, "accepted", flush=True)
    except Exception as exc:
        print(label, "REFUSED", str(exc).splitlines()[0][:300], flush=True)


spark_state("default")
native_state("default")
attempt(lambda: spark.conf.set(A, "false"), "spark set A=false")
spark_state("A=false")
attempt(lambda: spark.conf.set(U, "true"), "spark set U=true")
spark_state("then U=true")
attempt(lambda: spark.conf.set(U, "false"), "spark set U=false")
spark_state("then U=false")
attempt(lambda: spark.conf.set(A, "true"), "spark set A=true")
spark_state("then A=true")
attempt(lambda: spark.conf.unset(A), "spark unset A")
spark_state("then unset A")
attempt(lambda: spark.conf.set(A, "false"), "spark set A=false")
attempt(lambda: spark.sql(f"RESET {A}").collect(), "spark RESET A")
spark_state("then RESET A")
attempt(lambda: pa.table(native.sql(f"SET {A} = false")), "native SET A=false")
native_state("A=false")
attempt(lambda: pa.table(native.sql(f"SET {U} = true")), "native SET U=true")
native_state("then U=true")
attempt(lambda: pa.table(native.sql(f"SET {A} = false")), "native SET A=false")
attempt(lambda: pa.table(native.sql(f"RESET {U}")), "native RESET U")
native_state("then RESET U")
attempt(lambda: pa.table(native.sql(f"RESET {A}")), "native RESET A")
native_state("then RESET A")

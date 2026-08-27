"""MW-6 CRITIC: the engine's own manifest byte totals on the MANIFEST-3 fixtures."""
from __future__ import annotations
from pathlib import Path
import pytest
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

V2 = "'format-version' = '2', 'commit.manifest.target-size-bytes' = '4096'"


@pytest.fixture(autouse=True)
def _isolate() -> None:
    _reset_active_session_for_tests(); yield; _reset_active_session_for_tests()


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    s = ReparkSession.builder.appName("critic-bytes").getOrCreate()
    s.register_memory_catalog("mem", tmp_path)
    s.sql("CREATE NAMESPACE mem.ns")
    return s


def totals(spark, table):
    t = spark.sql(f"SELECT length FROM {table}.manifests").to_arrow()
    lens = t.column("length").to_pylist()
    return len(lens), sum(lens)


@pytest.mark.parametrize("n", [5, 12])
def test_engine_manifest_byte_totals(spark: ReparkSession, n: int) -> None:
    t = f"mem.ns.t{n}"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, n + 1):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    print(f"\nENGINE n={n} before: count/total_bytes =", totals(spark, t))
    r = spark.sql(f"CALL mem.system.rewrite_manifests(table => 'ns.t{n}')").to_arrow()
    print(f"ENGINE n={n} result =", (r.column("rewritten_manifests_count")[0].as_py(),
                                     r.column("added_manifests_count")[0].as_py()))
    print(f"ENGINE n={n} after: count/total_bytes =", totals(spark, t))

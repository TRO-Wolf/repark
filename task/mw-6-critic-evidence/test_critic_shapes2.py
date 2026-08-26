"""MW-6 CRITIC — round 2 shapes matched to the oracle's Q / N / P runs."""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

V2 = "'format-version' = '2'"


@pytest.fixture(autouse=True)
def _isolate() -> None:
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("critic-mw6-2").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def manifests(spark, table):
    t = spark.sql(f"SELECT content, length FROM {table}.manifests").to_arrow()
    return list(zip(t.column("content").to_pylist(), t.column("length").to_pylist()))


def call(spark, sql):
    t = spark.sql(sql).to_arrow()
    return (
        t.column("rewritten_manifests_count")[0].as_py(),
        t.column("added_manifests_count")[0].as_py(),
    )


def test_q_twelve_manifests_tiny_target(spark: ReparkSession) -> None:
    t = "mem.ns.q"
    spark.sql(
        f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES "
        f"({V2}, 'commit.manifest.target-size-bytes' = '4096')"
    )
    for i in range(1, 13):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    print("\nQ before n=", len(manifests(spark, t)), manifests(spark, t)[:3])
    before = spark.sql(f"SELECT * FROM {t} ORDER BY id").to_arrow().to_pylist()
    print("Q result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.q')"))
    after = manifests(spark, t)
    print("Q after n=", len(after), after)
    print("Q rows equal:", spark.sql(f"SELECT * FROM {t} ORDER BY id").to_arrow().to_pylist() == before)


def test_n_use_caching_int(spark: ReparkSession) -> None:
    t = "mem.ns.n"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, 5):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    for label, sql in [
        ("use_caching => 1 (INT)", "CALL mem.system.rewrite_manifests(table => 'ns.n', use_caching => 1)"),
        ("use_caching => 'true' (STRING)", "CALL mem.system.rewrite_manifests(table => 'ns.n', use_caching => 'true')"),
    ]:
        try:
            print(f"\nN {label}: OK", spark.sql(sql).to_arrow().to_pylist())
        except Exception as exc:  # noqa: BLE001
            print(f"\nN {label}: {type(exc).__name__} {str(exc).splitlines()[0][:250]}")


def test_p_missing_table_other_procedures(spark: ReparkSession) -> None:
    for label, sql in [
        ("rewrite_manifests", "CALL mem.system.rewrite_manifests(table => 'ns.nope')"),
        ("rewrite_data_files", "CALL mem.system.rewrite_data_files(table => 'ns.nope')"),
        ("expire_snapshots", "CALL mem.system.expire_snapshots(table => 'ns.nope')"),
    ]:
        try:
            print(f"\nP {label}: OK", spark.sql(sql).to_arrow().to_pylist())
        except Exception as exc:  # noqa: BLE001
            print(f"\nP {label}: {type(exc).__name__} {str(exc).splitlines()[0][:250]}")

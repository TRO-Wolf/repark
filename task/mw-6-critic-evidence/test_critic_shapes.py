"""MW-6 CRITIC — novel shapes through the repark facade door. Read-only on the worktree."""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException
from repark.spark.session import _reset_active_session_for_tests

V2 = "'format-version' = '2'"
MOR = "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"


@pytest.fixture(autouse=True)
def _isolate() -> None:
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("critic-mw6").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def manifests(spark: ReparkSession, table: str) -> list[tuple]:
    t = spark.sql(
        f"SELECT content, partition_spec_id, length FROM {table}.manifests"
    ).to_arrow()
    return list(
        zip(
            t.column("content").to_pylist(),
            t.column("partition_spec_id").to_pylist(),
            t.column("length").to_pylist(),
        )
    )


def call(spark: ReparkSession, sql: str) -> tuple:
    t = spark.sql(sql).to_arrow()
    return (
        t.column("rewritten_manifests_count")[0].as_py(),
        t.column("added_manifests_count")[0].as_py(),
    )


def snaps(spark: ReparkSession, table: str) -> int:
    return spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").to_arrow().num_rows


def rows(spark: ReparkSession, table: str) -> list:
    return spark.sql(f"SELECT * FROM {table} ORDER BY id").to_arrow().to_pylist()


def test_a_two_data_one_delete(spark: ReparkSession) -> None:
    t = "mem.ns.a"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({MOR})")
    spark.sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b'),(3,'c')")
    spark.sql(f"DELETE FROM {t} WHERE id = 1")
    spark.sql(f"INSERT INTO {t} VALUES (4,'d')")
    print("\nA before", manifests(spark, t))
    before = rows(spark, t)
    print("A result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.a')"))
    print("A after", manifests(spark, t))
    print("A rows equal:", rows(spark, t) == before, len(before))


def test_b_twentyfour_appends(spark: ReparkSession) -> None:
    t = "mem.ns.b"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, 25):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    print("\nB before n=", len(manifests(spark, t)))
    before = rows(spark, t)
    print("B result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.b')"))
    print("B after", manifests(spark, t))
    print("B rows equal:", rows(spark, t) == before, len(before))


def test_c_after_expire_snapshots(spark: ReparkSession) -> None:
    t = "mem.ns.c"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, 6):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    print("\nC snaps before", snaps(spark, t))
    exp = spark.sql(
        "CALL mem.system.expire_snapshots(table => 'ns.c', "
        "older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)"
    ).to_arrow()
    print("C expire", exp.to_pylist())
    print("C snaps after", snaps(spark, t), "manifests", manifests(spark, t))
    before = rows(spark, t)
    print("C result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.c')"))
    print("C after", manifests(spark, t))
    print("C rows equal:", rows(spark, t) == before, len(before))


def test_d_after_add_column(spark: ReparkSession) -> None:
    t = "mem.ns.d"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, 3):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    spark.sql(f"ALTER TABLE {t} ADD COLUMN extra STRING")
    for i in range(3, 6):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}', 'e{i}')")
    print("\nD before", manifests(spark, t))
    before = rows(spark, t)
    print("D result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.d')"))
    print("D after", manifests(spark, t))
    print("D rows", rows(spark, t) == before, rows(spark, t))


def test_e_tiny_target_five_manifests(spark: ReparkSession) -> None:
    t = "mem.ns.e"
    spark.sql(
        f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES "
        f"({V2}, 'commit.manifest.target-size-bytes' = '4096')"
    )
    for i in range(1, 6):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    print("\nE before", manifests(spark, t))
    before = rows(spark, t)
    print("E result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.e')"))
    print("E after", manifests(spark, t))
    print("E rows equal:", rows(spark, t) == before)


def test_f_one_manifest_over_target(spark: ReparkSession) -> None:
    t = "mem.ns.f"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    spark.sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    print("\nF before", manifests(spark, t))
    spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('commit.manifest.target-size-bytes' = '1024')")
    before = rows(spark, t)
    snaps_before = snaps(spark, t)
    print("F result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.f')"))
    print("F after", manifests(spark, t), "snaps", snaps_before, "->", snaps(spark, t))
    print("F rows equal:", rows(spark, t) == before)


def test_g_positional(spark: ReparkSession) -> None:
    t = "mem.ns.g"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, 4):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    print("\nG result", call(spark, "CALL mem.system.rewrite_manifests('ns.g')"))
    print("G after", manifests(spark, t))


def test_h_nonexistent_table(spark: ReparkSession) -> None:
    try:
        out = spark.sql("CALL mem.system.rewrite_manifests(table => 'ns.nope')").to_arrow()
        print("\nH no error:", out.to_pylist())
    except Exception as exc:  # noqa: BLE001
        print("\nH ERROR", type(exc).__name__, str(exc).splitlines()[0][:300])


def test_i_rewrite_merge_rewrite(spark: ReparkSession) -> None:
    t = "mem.ns.i"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({MOR})")
    spark.sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b'),(3,'c')")
    spark.sql(f"INSERT INTO {t} VALUES (4,'d')")
    print("\nI before", manifests(spark, t))
    print("I first", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.i')"))
    print("I after first", manifests(spark, t))
    spark.sql(
        f"MERGE INTO {t} AS t USING (SELECT 2 AS id, 'm2' AS v) AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.v = s.v"
    )
    print("I after merge", manifests(spark, t))
    before = rows(spark, t)
    try:
        print("I second", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.i')"))
    except Exception as exc:  # noqa: BLE001
        print("I second ERROR", type(exc).__name__, str(exc).splitlines()[0][:300])
    print("I after second", manifests(spark, t))
    print("I rows equal:", rows(spark, t) == before, before)


def test_j_partitioned(spark: ReparkSession) -> None:
    t = "mem.ns.j"
    spark.sql(
        f"CREATE TABLE {t} (id INT, grp STRING) USING iceberg PARTITIONED BY (grp) "
        f"TBLPROPERTIES ({V2})"
    )
    for i in range(1, 7):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'g{i % 3}')")
    print("\nJ before", manifests(spark, t))
    before = rows(spark, t)
    print("J result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.j')"))
    print("J after", manifests(spark, t))
    print("J rows equal:", rows(spark, t) == before, len(before))


def test_k_argument_refusals(spark: ReparkSession) -> None:
    t = "mem.ns.k"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
    for i in range(1, 4):
        spark.sql(f"INSERT INTO {t} VALUES ({i}, 'v{i}')")
    for label, sql in [
        ("bogus named", "CALL mem.system.rewrite_manifests(table => 'ns.k', bogus => 1)"),
        ("use_caching str", "CALL mem.system.rewrite_manifests(table => 'ns.k', use_caching => 'yes')"),
        ("4 positional", "CALL mem.system.rewrite_manifests('ns.k', true, 0, 9)"),
        ("no args", "CALL mem.system.rewrite_manifests()"),
        ("positional use_caching only", "CALL mem.system.rewrite_manifests('ns.k', false)"),
    ]:
        try:
            out = spark.sql(sql).to_arrow()
            print(f"\nK {label}: OK {out.to_pylist()}")
        except Exception as exc:  # noqa: BLE001
            print(f"\nK {label}: {type(exc).__name__} {str(exc).splitlines()[0][:250]}")


def test_l_exactly_one_delete_manifest(spark: ReparkSession) -> None:
    """The refusal boundary's other side: one delete manifest must NOT refuse."""
    t = "mem.ns.l"
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ({MOR})")
    spark.sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    spark.sql(f"DELETE FROM {t} WHERE id = 1")
    print("\nL before", manifests(spark, t))
    snaps_before = snaps(spark, t)
    try:
        print("L result", call(spark, "CALL mem.system.rewrite_manifests(table => 'ns.l')"))
    except Exception as exc:  # noqa: BLE001
        print("L ERROR", type(exc).__name__, str(exc).splitlines()[0][:300])
    print("L after", manifests(spark, t), "snaps", snaps_before, "->", snaps(spark, t))

from __future__ import annotations

import itertools
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as sf
from repark._native import PyReparkSession
from repark.spark.session import _reset_active_session_for_tests

COW = (
    "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
)
MOR = (
    "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
)
MODES = {"cow": COW, "mor": MOR}
TABLES = {
    "t": (
        "DECIMAL(10,2)",
        "(1, CAST(1.00 AS DECIMAL(10,2)), CAST('1.0000000000000004' AS DOUBLE)), "
        "(2, CAST(1.47 AS DECIMAL(10,2)), CAST('1.4700000000000004' AS DOUBLE)), "
        "(3, CAST(2.23 AS DECIMAL(10,2)), CAST('2.2300000000000004' AS DOUBLE)), "
        "(4, CAST(1.50 AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE))",
    ),
    "u": (
        "DECIMAL(38,18)",
        "(1, CAST('-22.129547339983247' AS DECIMAL(38,18)), CAST('-22.129547339983247' AS "
        "DOUBLE)), "
        "(2, CAST(1.5 AS DECIMAL(38,18)), CAST('1.5' AS DOUBLE)), "
        "(3, CAST('1.0000000000000004' AS DECIMAL(38,18)), CAST('1.0000000000000004' AS DOUBLE))",
    ),
    "z": (
        "DECIMAL(10,2)",
        "(1, CAST(0 AS DECIMAL(10,2)), CAST('-0.0' AS DOUBLE)), "
        "(2, CAST(0.50 AS DECIMAL(10,2)), CAST('0.5' AS DOUBLE)), "
        "(3, CAST(0 AS DECIMAL(10,2)), CAST('1e-20' AS DOUBLE))",
    ),
}
STATEMENTS = {
    "select_eq": "SELECT id FROM {t} WHERE d = v",
    "select_lt": "SELECT id FROM {t} WHERE d < v",
    "select_ns": "SELECT id FROM {t} WHERE d <=> v",
    "select_in": "SELECT id FROM {t} WHERE v IN (d)",
    "select_between": "SELECT id FROM {t} WHERE v BETWEEN d AND d",
    "delete_eq": "DELETE FROM {t} WHERE d = v",
    "update_eq": "UPDATE {t} SET id = id + 100 WHERE d = v",
    "update_in": "UPDATE {t} SET id = id + 100 WHERE v IN (d)",
    "merge_on_dv": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.d = s.sv "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100"
    ),
    "merge_on_vd": (
        "MERGE INTO {t} t USING (SELECT id AS sid, d AS sd FROM {s}) s ON t.v = s.sd "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100"
    ),
    "merge_on_key_and": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.id = s.sid AND t.d = "
        "s.sv "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100 "
        "WHEN NOT MATCHED THEN INSERT (id, d, v) VALUES (s.sid + 50, 0, 0)"
    ),
    "merge_delete": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.d = s.sv "
        "WHEN MATCHED THEN DELETE"
    ),
    "merge_by_source": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.d = s.sv "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    ),
}
SPARK_DML = {
    "t/cow/delete_eq": [1, 2, 3],
    "t/cow/merge_by_source": [4],
    "t/cow/merge_delete": [1, 2, 3],
    "t/cow/merge_on_dv": [1, 2, 3, 104],
    "t/cow/merge_on_key_and": [1, 2, 3, 51, 52, 53, 104],
    "t/cow/merge_on_vd": [1, 2, 3, 104],
    "t/cow/select_between": [4],
    "t/cow/select_eq": [4],
    "t/cow/select_in": [4],
    "t/cow/select_lt": [1, 2, 3],
    "t/cow/select_ns": [4],
    "t/cow/update_eq": [1, 2, 3, 104],
    "t/cow/update_in": [1, 2, 3, 104],
    "t/mor/delete_eq": [1, 2, 3],
    "t/mor/merge_by_source": [4],
    "t/mor/merge_delete": [1, 2, 3],
    "t/mor/merge_on_dv": [1, 2, 3, 104],
    "t/mor/merge_on_key_and": [1, 2, 3, 51, 52, 53, 104],
    "t/mor/merge_on_vd": [1, 2, 3, 104],
    "t/mor/select_between": [4],
    "t/mor/select_eq": [4],
    "t/mor/select_in": [4],
    "t/mor/select_lt": [1, 2, 3],
    "t/mor/select_ns": [4],
    "t/mor/update_eq": [1, 2, 3, 104],
    "t/mor/update_in": [1, 2, 3, 104],
    "u/cow/delete_eq": [],
    "u/cow/merge_by_source": [1, 2, 3],
    "u/cow/merge_delete": [],
    "u/cow/merge_on_dv": [101, 102, 103],
    "u/cow/merge_on_key_and": [101, 102, 103],
    "u/cow/merge_on_vd": [101, 102, 103],
    "u/cow/select_between": [1, 2, 3],
    "u/cow/select_eq": [1, 2, 3],
    "u/cow/select_in": [1, 2, 3],
    "u/cow/select_lt": [],
    "u/cow/select_ns": [1, 2, 3],
    "u/cow/update_eq": [101, 102, 103],
    "u/cow/update_in": [101, 102, 103],
    "u/mor/delete_eq": [],
    "u/mor/merge_by_source": [1, 2, 3],
    "u/mor/merge_delete": [],
    "u/mor/merge_on_dv": [101, 102, 103],
    "u/mor/merge_on_key_and": [101, 102, 103],
    "u/mor/merge_on_vd": [101, 102, 103],
    "u/mor/select_between": [1, 2, 3],
    "u/mor/select_eq": [1, 2, 3],
    "u/mor/select_in": [1, 2, 3],
    "u/mor/select_lt": [],
    "u/mor/select_ns": [1, 2, 3],
    "u/mor/update_eq": [101, 102, 103],
    "u/mor/update_in": [101, 102, 103],
    "z/cow/delete_eq": [3],
    "z/cow/merge_by_source": [1, 2, 3],
    "z/cow/merge_delete": [],
    "z/cow/merge_on_dv": [101, 102, 103],
    "z/cow/merge_on_key_and": [3, 53, 101, 102],
    "z/cow/merge_on_vd": "MERGE_CARDINALITY_VIOLATION",
    "z/cow/select_between": [1, 2],
    "z/cow/select_eq": [1, 2],
    "z/cow/select_in": [1, 2],
    "z/cow/select_lt": [3],
    "z/cow/select_ns": [1, 2],
    "z/cow/update_eq": [3, 101, 102],
    "z/cow/update_in": [3, 101, 102],
    "z/mor/delete_eq": [3],
    "z/mor/merge_by_source": [1, 2, 3],
    "z/mor/merge_delete": [],
    "z/mor/merge_on_dv": [101, 102, 103],
    "z/mor/merge_on_key_and": [3, 53, 101, 102],
    "z/mor/merge_on_vd": "MERGE_CARDINALITY_VIOLATION",
    "z/mor/select_between": [1, 2],
    "z/mor/select_eq": [1, 2],
    "z/mor/select_in": [1, 2],
    "z/mor/select_lt": [3],
    "z/mor/select_ns": [1, 2],
    "z/mor/update_eq": [3, 101, 102],
    "z/mor/update_in": [3, 101, 102],
}
SPARK_FLIPS = {
    "flip/c1/-22.129547339983247": "true",
    "flip/c1/1.0000000000000004": "true",
    "flip/c1/2.2300000000000004": "true",
    "flip/c3/-22.129547339983247": "true",
    "flip/c3/1.0000000000000004": "true",
    "flip/c3/2.2300000000000004": "true",
    "flip/c5/-22.129547339983247": "true",
    "flip/c5/1.0000000000000004": "true",
    "flip/c5/2.2300000000000004": "true",
    "flip/c6/-22.129547339983247": "false",
    "flip/c6/1.0000000000000004": "false",
    "flip/c6/2.2300000000000004": "false",
    "flip/c7/-22.129547339983247": "true",
    "flip/c7/1.0000000000000004": "true",
    "flip/c7/2.2300000000000004": "true",
}
SIGNED_ZERO_UPDATE_IN_LIST = {"z/cow/update_in", "z/mor/update_in"}
FLIP_SHAPES = {
    "c1": "ROUND({d}, 18) = {d}",
    "c3": "CAST({d} AS DECIMAL(38,18)) = {d}",
    "c5": "CAST('{v}' AS DECIMAL(38,18)) = {d}",
    "c6": "CAST(ROUND({d}, 2) AS DECIMAL(10,2)) = {d}",
    "c7": "CAST(ROUND({d}, 18) AS DECIMAL(38,18)) = {d}",
}
ZSRC = "(SELECT CAST(0 AS DECIMAL(10,2)) AS d, CAST('-0.0' AS DOUBLE) AS v)"
ZD = "CAST(0 AS DECIMAL(10,2))"
NZ = "CAST('-0.0' AS DOUBLE)"
ZERO_SHAPES = {
    "zero_eq": (f"SELECT v = d AS c FROM {ZSRC}", "True"),
    "zero_join": (
        f"SELECT count(*) AS c FROM (SELECT {ZD} AS k) a JOIN (SELECT {NZ} AS k) b ON a.k = b.k",
        "1",
    ),
    "zero_intersect": (f"SELECT count(*) AS c FROM (SELECT {ZD} AS k INTERSECT SELECT {NZ})", "1"),
    "zero_except": (f"SELECT count(*) AS c FROM (SELECT {ZD} AS k EXCEPT SELECT {NZ})", "0"),
    "zero_union_distinct": (f"SELECT count(*) AS c FROM (SELECT {ZD} AS k UNION SELECT {NZ})", "1"),
    "zero_group_by": (
        f"SELECT count(*) AS c FROM (SELECT k FROM (SELECT {ZD} AS k UNION ALL SELECT {NZ}) "
        f"GROUP BY k)",
        "1",
    ),
    "zero_in_subquery": (
        f"SELECT count(*) AS c FROM (SELECT {NZ} AS v) WHERE v IN (SELECT {ZD})",
        "1",
    ),
}
FUZZ = [
    ("eq_round_d38_18", "-22.129547339983247", "true"),
    ("eq_round_d38_18", "0.07464000000000004", "true"),
    ("eq_round_d38_18", "0.8017300000000002", "true"),
    ("eq_round_d38_18", "24677.77000000001", "true"),
    ("eq_round_d38_18", "5000.999999999997", "true"),
    ("eq_round_d38_18", "73.24096726660397", "true"),
    ("eq_round_d38_18", "99.11000000000004", "true"),
    ("eqf_round_d38_18", "0.062272000000000015", "true"),
    ("eqf_round_d38_18", "0.48138000000000014", "true"),
    ("eqf_round_d38_18", "11371.400000000005", "true"),
    ("eqf_round_d38_18", "518.7", "true"),
    ("eqf_round_d38_18", "6946.2300000000005", "true"),
    ("in_d38_18", "-77.21431654625898", "true"),
    ("in_d38_18", "0.09219999999999998", "true"),
    ("in_d38_18", "0.9270000000000002", "true"),
    ("in_d38_18", "2837.9600000000005", "true"),
    ("in_d38_18", "5316687.999999997", "true"),
    ("in_d38_18", "765.0818000000003", "true"),
    ("le_round_d38_18", "0.0059099999999999995", "true"),
    ("le_round_d38_18", "0.9370999999999998", "true"),
    ("le_round_d38_18", "518.6999999999999", "true"),
    ("lt_round_d38_18", "-77.8755931943731", "false"),
    ("lt_round_d38_18", "0.8392000000000001", "false"),
    ("lt_round_d38_18", "5.6488380000000005", "false"),
    ("ns_round_d38_18", "-25.33391257474416", "true"),
    ("ns_round_d38_18", "0.07672999999999999", "true"),
    ("ns_round_d38_18", "0.8192499999999998", "true"),
    ("ns_round_d38_18", "25.271839999999994", "true"),
    ("ns_round_d38_18", "5000.999999999998", "true"),
    ("ns_round_d38_18", "7325.4299999999985", "true"),
    ("coalesce_d38_18", "0.002387999999999999", "0.002387999999999999"),
    ("eq_round_d38_18", "0.00591", "true"),
    ("eq_round_d38_18", "0.08106999999999998", "true"),
    ("eq_round_d38_18", "0.7699999999999999", "true"),
    ("eq_round_d38_18", "1e-05", "true"),
    ("eq_round_d38_18", "378.3999999999998", "true"),
    ("eq_round_d38_18", "552498.0999999999", "true"),
    ("eq_round_d38_18", "765.0817999999998", "true"),
    ("eq_round_d38_18", "93.69750468807183", "true"),
    ("eqf_round_d38_18", "0.00915", "true"),
    ("eqf_round_d38_18", "0.214669", "true"),
    ("eqf_round_d38_18", "0.9700000000000001", "true"),
    ("eqf_round_d38_18", "2237700.7", "true"),
    ("eqf_round_d38_18", "4094.3000000000015", "true"),
    ("eqf_round_d38_18", "6.250000000000001", "true"),
    ("coalesce_d10_2", "-1.9382652774403653", "-1.9382652774403653"),
    ("coalesce_d10_2", "6541.334000000002", "6541.334000000002"),
    ("coalesce_d10_3", "3.8970000000000007", "3.8970000000000007"),
    ("coalesce_d16_4", "0.6993299999999999", "0.6993299999999999"),
    ("coalesce_d20_6", "0.008000000000000005", "0.008000000000000005"),
    ("coalesce_d20_6", "9.250000000000005", "9.250000000000005"),
    ("eq_round_d20_6", "0.9370999999999998", "false"),
    ("eqf_round_d20_6", "0.09645200000000004", "false"),
    ("greatest_d10_2", "24.785500000000003", "24.785500000000003"),
    ("greatest_d10_3", "0.392077", "0.392077"),
    ("greatest_d16_4", "0.0038219999999999986", "0.0038219999999999986"),
    ("greatest_d16_4", "82881.03", "82881.03"),
    ("greatest_d20_6", "4704.488799999999", "4704.488799999999"),
    ("if_d10_2", "13596.740000000003", "13596.740000000003"),
    ("if_d10_3", "0.08106999999999999", "0.08106999999999999"),
]
COUNTER = itertools.count()


def shape_sql(shape: str) -> str:
    family, _, decimal_key = shape.rpartition("_d")
    precision, scale = decimal_key.split("_")
    d = f"DECIMAL({precision},{scale})"
    r = int(scale)
    return {
        "eq_round": f"CAST(CAST(ROUND(v, {r}) AS {d}) = v AS STRING)",
        "lt_round": f"CAST(CAST(ROUND(v, {r}) AS {d}) < v AS STRING)",
        "le_round": f"CAST(v <= CAST(ROUND(v, {r}) AS {d}) AS STRING)",
        "ns_round": f"CAST(CAST(ROUND(v, {r}) AS {d}) <=> v AS STRING)",
        "eqf_round": f"CAST(CAST(ROUND(f, {r}) AS {d}) = f AS STRING)",
        "coalesce": f"CAST(coalesce(CAST(NULL AS {d}), v) AS STRING)",
        "if": f"CAST(IF(id < 0, CAST(0 AS {d}), v) AS STRING)",
        "greatest": f"CAST(greatest(CAST(-1000 AS {d}), v) AS STRING)",
        "in": f"CAST(v IN (CAST(ROUND(v, {r}) AS {d}), CAST(0 AS {d})) AS STRING)",
    }[family]


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-decimal-double-coercion-1")
        .config("spark.sql.ansi.enabled", request.param)
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def table(spark: Any, name: str, mode: str, statement: str) -> tuple[str, str]:
    dtype, rows = TABLES[name]
    target, source = f"ice.ns.x{next(COUNTER)}", f"ice.ns.s{next(COUNTER)}"
    spark.sql(
        f"CREATE TABLE {target} (id INT, d {dtype}, v DOUBLE) USING iceberg TBLPROPERTIES "
        f"({MODES[mode]})"
    ).collect()
    spark.sql(f"INSERT INTO {target} VALUES {rows}").collect()
    if "{s}" in statement:
        spark.sql(f"CREATE TABLE {source} (id INT, d {dtype}, v DOUBLE) USING iceberg").collect()
        spark.sql(f"INSERT INTO {source} VALUES {rows}").collect()
    return target, source


def dml_answer(spark: Any, name: str, mode: str, key: str) -> Any:
    statement = STATEMENTS[key]
    target, source = table(spark, name, mode, statement)
    text = statement.format(t=target, s=source)
    try:
        if key.startswith("select"):
            got = spark.sql(text).collect()
        else:
            spark.sql(text).collect()
            got = spark.sql(f"SELECT id FROM {target}").collect()
    except Exception as error:
        if "MERGE_CARDINALITY_VIOLATION" in str(error):
            return "MERGE_CARDINALITY_VIOLATION"
        raise
    return sorted(int(row[0]) for row in got)


@pytest.mark.parametrize(
    "cell",
    [
        pytest.param(
            cell,
            marks=pytest.mark.xfail(
                strict=True,
                reason=(
                    "SIGNED-ZERO-UPDATE-IN-LIST-1: an UPDATE IN-list over FLOAT columns misses "
                    "-0.0 against 0.0"
                ),
            ),
        )
        if cell in SIGNED_ZERO_UPDATE_IN_LIST
        else cell
        for cell in sorted(SPARK_DML)
    ],
)
def test_a_decimal_beside_a_double_in_dml_answers_as_spark(spark: Any, cell: str) -> None:
    name, mode, key = cell.split("/")
    assert dml_answer(spark, name, mode, key) == SPARK_DML[cell]


@pytest.mark.parametrize("cell", sorted(SPARK_FLIPS))
def test_the_flip_casts_answer_as_spark(spark: Any, cell: str) -> None:
    _, shape, value = cell.split("/")
    d = f"CAST('{value}' AS DOUBLE)"
    expr = FLIP_SHAPES[shape].format(d=d, v=value)
    got = spark.sql(f"SELECT CAST({expr} AS STRING) AS c").collect()[0][0]
    assert str(got) == SPARK_FLIPS[cell]


@pytest.mark.parametrize("shape", sorted(ZERO_SHAPES))
def test_the_signed_zero_shapes_answer_as_spark(spark: Any, shape: str) -> None:
    sql, want = ZERO_SHAPES[shape]
    assert str(spark.sql(sql).collect()[0][0]) == want


def zero_table(spark: Any, mode: str) -> str:
    name = f"ice.ns.z{next(COUNTER)}"
    spark.sql(
        f"CREATE TABLE {name} (id INT, v DOUBLE, d DECIMAL(10,2)) USING iceberg TBLPROPERTIES "
        f"({MODES[mode]})"
    ).collect()
    spark.sql(
        f"INSERT INTO {name} VALUES (1, CAST('-0.0' AS DOUBLE), CAST(0 AS DECIMAL(10,2)))"
    ).collect()
    return name


@pytest.mark.parametrize("mode", sorted(MODES))
def test_delete_zero_answers_as_spark(spark: Any, mode: str) -> None:
    name = zero_table(spark, mode)
    spark.sql(f"DELETE FROM {name} WHERE v = d").collect()
    assert spark.sql(f"SELECT count(*) FROM {name}").collect()[0][0] == 0


@pytest.mark.parametrize("mode", sorted(MODES))
def test_merge_on_zero_keeps_mains_match_as_q8_rules(spark: Any, mode: str) -> None:
    name = zero_table(spark, mode)
    spark.sql(
        f"MERGE INTO {name} t USING (SELECT CAST(0 AS DECIMAL(10,2)) AS k) s ON t.v = s.k "
        "WHEN MATCHED THEN UPDATE SET id = 99"
    ).collect()
    assert [row[0] for row in spark.sql(f"SELECT id FROM {name}").collect()] == [99]


def test_the_coercion_fuzz_sample_answers_as_spark(spark: Any) -> None:
    texts = sorted({value for _, value, _ in FUZZ})
    spark.createDataFrame(list(enumerate(texts)), "id INT, t STRING").createOrReplaceTempView("raw")
    spark.sql(
        "SELECT id, CAST(t AS DOUBLE) AS v, CAST(CAST(t AS DOUBLE) AS FLOAT) AS f FROM raw"
    ).createOrReplaceTempView("x")
    index = {text: i for i, text in enumerate(texts)}
    misses = []
    for shape in sorted({shape for shape, _, _ in FUZZ}):
        got = {
            row[0]: row[1] for row in spark.sql(f"SELECT id, {shape_sql(shape)} FROM x").collect()
        }
        for cell_shape, value, want in FUZZ:
            if cell_shape == shape and got.get(index[value]) != want:
                misses.append(f"{shape} {value}: {got.get(index[value])}, Spark {want}")
    assert misses == []


def union_frame(spark: Any) -> Any:
    left = spark.sql("SELECT 1 AS id, CAST(0.1 AS DECIMAL(10,2)) AS v")
    right = spark.sql("SELECT 2 AS id, CAST(1e-20 AS DOUBLE) AS v")
    return left.union(right)


def stored(spark: Any, name: str) -> tuple[str, dict[int, str]]:
    frame = spark.sql(f"SELECT id, v FROM {name} ORDER BY id")
    return dict(frame.dtypes)["v"], {int(row[0]): str(row[1]) for row in frame.collect()}


UNION_STORED = ("double", {1: "0.1", 2: "1e-20"})


def test_save_as_table_of_a_union_stores_sparks_double(spark: Any) -> None:
    union_frame(spark).write.format("iceberg").saveAsTable("ice.ns.sat")
    assert stored(spark, "ice.ns.sat") == UNION_STORED


def test_write_to_append_of_a_union_stores_sparks_double(spark: Any) -> None:
    spark.sql("CREATE TABLE ice.ns.wta (id INT, v DOUBLE) USING iceberg").collect()
    union_frame(spark).writeTo("ice.ns.wta").append()
    assert stored(spark, "ice.ns.wta") == UNION_STORED


def test_create_or_replace_of_a_union_creates_sparks_double(spark: Any) -> None:
    spark.sql("CREATE TABLE ice.ns.cor (id INT, v STRING) USING iceberg").collect()
    union_frame(spark).writeTo("ice.ns.cor").using("iceberg").createOrReplace()
    assert stored(spark, "ice.ns.cor") == UNION_STORED


def test_replace_where_of_a_union_stores_sparks_double_values(spark: Any) -> None:
    spark.sql("CREATE TABLE ice.ns.rw (id INT, v DOUBLE) USING iceberg").collect()
    spark.sql("INSERT INTO ice.ns.rw VALUES (1, 5.0), (2, 6.0)").collect()
    union_frame(spark).write.format("iceberg").mode("overwrite").option(
        "replaceWhere", "id < 3"
    ).insertInto("ice.ns.rw")
    assert stored(spark, "ice.ns.rw") == UNION_STORED


def test_a_v3_delete_compares_in_double_as_spark(spark: Any) -> None:
    spark.sql(
        "CREATE TABLE ice.ns.v3 (id INT, d DECIMAL(10,2), v DOUBLE) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    ).collect()
    spark.sql(
        "INSERT INTO ice.ns.v3 VALUES (1, CAST(1.00 AS DECIMAL(10,2)), CAST('1.0000000000000004' "
        "AS DOUBLE)), "
        "(2, CAST(0 AS DECIMAL(10,2)), CAST('1e-20' AS DOUBLE)), (3, CAST(1.50 AS "
        "DECIMAL(10,2)), CAST('1.5' AS DOUBLE))"
    ).collect()
    spark.sql("DELETE FROM ice.ns.v3 WHERE d = v").collect()
    assert sorted(row[0] for row in spark.sql("SELECT id FROM ice.ns.v3").collect()) == [1, 2]


def test_a_non_ascii_union_unifies_to_double(spark: Any) -> None:
    frame = spark.sql("SELECT 0.1 AS `名前` UNION ALL SELECT CAST(1e-20 AS DOUBLE)")
    assert frame.dtypes == [("名前", "double")]
    assert sorted(str(row[0]) for row in frame.collect()) == ["0.1", "1e-20"]
    filtered = spark.sql(
        "SELECT * FROM (SELECT 0.1 AS `名前` UNION ALL SELECT CAST(1e-20 AS DOUBLE)) "
        "WHERE `名前` > 0.05"
    )
    assert [str(row[0]) for row in filtered.collect()] == ["0.1"]


def test_a_struct_comparison_of_a_decimal_and_a_double_leaf_still_refuses(spark: Any) -> None:
    with pytest.raises(Exception, match="DATATYPE_MISMATCH"):
        spark.sql(
            "SELECT named_struct('a', CAST(0.1 AS DECIMAL(10,2))) = "
            "named_struct('a', CAST(1e-20 AS DOUBLE))"
        ).collect()


LITERAL_SHAPES = {
    "SELECT id FROM lt WHERE v = 0.1": ["1"],
    "SELECT id FROM lt WHERE v IN (0.1, 0.3)": ["1"],
    "SELECT id FROM lt WHERE v BETWEEN 0.1 AND 0.2": ["1", "3"],
    "SELECT CASE v WHEN 0.1 THEN 'a' WHEN 0.3 THEN 'b' ELSE 'c' END FROM lt": ["a", "c", "c"],
}


@pytest.mark.parametrize("sql", sorted(LITERAL_SHAPES))
def test_the_four_literal_shapes_answer_as_spark_with_both_rules(spark: Any, sql: str) -> None:
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW lt AS SELECT * FROM VALUES (1, CAST('0.1' AS DOUBLE)), "
        "(2, CAST('0.30000000000000004' AS DOUBLE)), (3, CAST('0.2' AS DOUBLE)) AS t(id, v)"
    )
    assert sorted(str(row[0]) for row in spark.sql(sql).collect()) == LITERAL_SHAPES[sql]


def test_written_casts_round_once_as_spark(spark: Any) -> None:
    row = spark.sql(
        "SELECT CAST(CAST('-22.129547339983247' AS DECIMAL(38,18)) AS DOUBLE), "
        "CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2)), "
        "CAST('-22.129547339983247' AS DECIMAL(38,18)) + CAST(0 AS DOUBLE)"
    ).collect()[0]
    assert [str(value) for value in row] == ["-22.129547339983247", "1.01", "-22.129547339983247"]


def test_a_dataframe_map_of_a_decimal_and_a_double_value_is_double(spark: Any) -> None:
    frame = spark.sql(
        "SELECT CAST(1.25 AS DECIMAL(10,2)) AS dc, CAST('1e-20' AS DOUBLE) AS dd"
    ).select(sf.create_map(sf.lit("a"), sf.col("dc"), sf.lit("b"), sf.col("dd")).alias("c"))
    assert frame.dtypes == [("c", "map<string,double>")]
    assert frame.collect()[0][0] == {"a": 1.25, "b": 1e-20}


def test_a_dataframe_column_comparison_widens_as_spark(spark: Any) -> None:
    frame = spark.sql(
        "SELECT CAST(1.00 AS DECIMAL(10,2)) AS d, CAST('1.0000000000000004' AS DOUBLE) AS v"
    )
    assert frame.select((sf.col("d") == sf.col("v")).alias("c")).collect()[0][0] is False
    assert frame.filter("d = v").count() == 0


VALUES_AT_MAIN = {
    "SELECT c FROM VALUES (0.1), (CAST(1e-20 AS DOUBLE)) AS v(c)": (
        "decimal(30,15)",
        ["0.100000000000000", "0E-15"],
    ),
    "SELECT c FROM VALUES (1, 0.1), (2, CAST(1e-20 AS DOUBLE)) AS v(id, c)": (
        "decimal(30,15)",
        ["0.100000000000000", "0E-15"],
    ),
}


@pytest.mark.parametrize("sql", sorted(VALUES_AT_MAIN))
def test_a_values_cell_keeps_mains_answer_until_seat_v(spark: Any, sql: str) -> None:
    frame = spark.sql(sql)
    data_type, values = VALUES_AT_MAIN[sql]
    assert frame.dtypes[-1][1] == data_type
    assert sorted(str(row[-1]) for row in frame.collect()) == sorted(values)


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_the_native_door_keeps_datafusions_answers(ansi: str, tmp_path: Path) -> None:
    import repark

    session = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
    previous = getattr(repark, "_ANSI_NATIVE", None)
    repark._ANSI_NATIVE = session
    try:
        cast = repark.sql(
            "SELECT CAST(CAST('-22.129547339983247' AS DECIMAL(38,18)) AS DOUBLE) AS c"
        ).collect()
        assert str(cast[0][0]) == "-22.129547339983244"
        compared = repark.sql(
            "SELECT CAST(1.00 AS DECIMAL(10,2)) = CAST('1.0000000000000004' AS DOUBLE) AS c"
        ).collect()
        assert compared[0][0] is False
    finally:
        repark._ANSI_NATIVE = previous

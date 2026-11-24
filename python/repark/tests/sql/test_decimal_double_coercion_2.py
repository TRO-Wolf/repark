from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as sf
from repark._native import PyReparkSession
from repark.spark.session import _reset_active_session_for_tests

VALS = "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"
DEC = "CAST(0.1 AS DECIMAL(10,2))"
TINY = "CAST('1e-20' AS DOUBLE)"
NEG = "CAST('-1e-20' AS DOUBLE)"
ZROWS = {
    "tiny": TINY,
    "negtiny": NEG,
    "round16": "CAST('0.10000000000000002' AS DOUBLE)",
    "negzero": "CAST('-0.0' AS DOUBLE)",
    "zero": "CAST(0.0 AS DOUBLE)",
    "e16_2": "CAST('2e-16' AS DOUBLE)",
    "e16_1": "CAST('1e-16' AS DOUBLE)",
    "big": "CAST('1e30' AS DOUBLE)",
}
SHAPES = {
    "in": "SELECT 1 AS id WHERE {z} IN (SELECT c FROM VALUES " + VALS + " AS t(c))",
    "in_cte": "WITH v AS (SELECT c FROM VALUES " + VALS + " AS t(c)) SELECT 1 AS id WHERE {z} IN "
    "(SELECT c FROM v)",
    "exists": "SELECT 1 AS id WHERE EXISTS (SELECT 1 FROM VALUES " + VALS + " AS t(c) WHERE t.c = "
    "{z})",
    "join": "SELECT a.id FROM (SELECT 1 AS id, {z} AS z) a JOIN (SELECT c FROM VALUES "
    + VALS
    + " AS t(c)) b ON a.z = b.c",
    "intersect": "SELECT {z} AS c INTERSECT SELECT c FROM VALUES " + VALS + " AS t(c)",
    "view_in": "SELECT id FROM (SELECT 1 AS id, {z} AS z) WHERE z IN (SELECT c FROM vv)",
    "view_join": "SELECT a.id FROM (SELECT 1 AS id, {z} AS z) a JOIN vv b ON a.z = b.c",
}
COLLISION = {"intersect": [["1e-20"]]}
M1_Z = f"z IN (SELECT c FROM VALUES {VALS} AS t(c))"
M1_EXISTS = f"EXISTS (SELECT 1 FROM VALUES {VALS} AS t(c) WHERE t.c = z)"
B_DEC = "SELECT 1 AS id, 0.1 AS c"
B_DBL = [f"SELECT 2, {TINY}", f"SELECT 3, {NEG}", "SELECT 4, CAST('2e-20' AS DOUBLE)"]
UNIONS = {
    "all3_dec_first": " UNION ALL ".join([B_DEC, *B_DBL[:2]]),
    "all4_dec_first": " UNION ALL ".join([B_DEC, *B_DBL]),
    "distinct3_dec_first": " UNION ".join([B_DEC, *B_DBL[:2]]),
    "right_deep3": B_DEC + " UNION ALL (" + " UNION ALL ".join(B_DBL[:2]) + ")",
    "mixed_nest4": B_DEC + " UNION ALL " + B_DBL[0] + " UNION (" + B_DBL[1] + " UNION ALL "
    "SELECT 4, CAST(0.25 AS DECIMAL(10,2)))",
    "all5_dec_first": " UNION ALL ".join([B_DEC, *B_DBL, "SELECT 5, CAST(0.25 AS DECIMAL(10,2))"]),
    "cte3": "WITH u AS (" + " UNION ALL ".join([B_DEC, *B_DBL[:2]]) + ") SELECT id, c FROM u",
    "view3": "SELECT id, c FROM uv3",
}
UNION_ROWS = {
    "all3_dec_first": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"]],
    "all4_dec_first": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"], ["4", "2e-20"]],
    "distinct3_dec_first": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"]],
    "right_deep3": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"]],
    "mixed_nest4": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"], ["4", "0.25"]],
    "all5_dec_first": [
        ["1", "0.1"],
        ["2", "1e-20"],
        ["3", "-1e-20"],
        ["4", "2e-20"],
        ["5", "0.25"],
    ],
    "cte3": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"]],
    "view3": [["1", "0.1"], ["2", "1e-20"], ["3", "-1e-20"]],
}
RANGE_VALUES = (
    "FROM VALUES (DATE '2024-01-01', CAST(0.1 AS DECIMAL(10,2))), "
    "(DATE '2024-01-02', CAST('1e-20' AS DOUBLE)), (DATE '2024-01-04', CAST('-0.0' AS DOUBLE)) "
    "AS t(dt, c)"
)
SEAT_V = {
    "values/standalone": (f"SELECT c FROM VALUES {VALS} AS t(c)", "double", [["0.1"], ["1e-20"]]),
    "values/two_cols": (
        f"SELECT id, c FROM VALUES (1, {DEC}), (2, {TINY}) AS t(id, c)",
        "double",
        [["1", "0.1"], ["2", "1e-20"]],
    ),
    "values/null_first": (
        f"SELECT c FROM VALUES (NULL), (0.1), ({TINY}) AS t(c)",
        "double",
        [["None"], ["0.1"], ["1e-20"]],
    ),
    "values/int_dec_double": (
        f"SELECT c FROM VALUES (1), (0.5), ({TINY}) AS t(c)",
        "double",
        [["1.0"], ["0.5"], ["1e-20"]],
    ),
    "values/float": (
        "SELECT c FROM VALUES (0.1), (CAST('1e-20' AS FLOAT)) AS t(c)",
        "double",
        [["0.1"], ["9.999999682655225e-21"]],
    ),
    "values/cte": (
        f"WITH x AS (SELECT * FROM VALUES (0.1), ({TINY}) AS t(c)) SELECT c FROM x",
        "double",
        [["0.1"], ["1e-20"]],
    ),
    "values/scalar_min": (
        f"SELECT (SELECT min(c) FROM VALUES {VALS} AS t(c)) AS m",
        "double",
        [["1e-20"]],
    ),
    "range/bare_days": (
        "SELECT dt, c, count(*) OVER (ORDER BY dt RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS n "
        + RANGE_VALUES
        + " ORDER BY dt",
        "double",
        [["2024-01-01", "0.1", "1"], ["2024-01-02", "1e-20", "2"], ["2024-01-04", "-0.0", "1"]],
    ),
    "range/interval_days": (
        "SELECT dt, c, count(*) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND "
        "CURRENT ROW) AS n " + RANGE_VALUES + " ORDER BY dt",
        "double",
        [["2024-01-01", "0.1", "1"], ["2024-01-02", "1e-20", "2"], ["2024-01-04", "-0.0", "1"]],
    ),
    "range/bare_days_sum_c": (
        "SELECT dt, sum(c) OVER (ORDER BY dt RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS n "
        + RANGE_VALUES
        + " ORDER BY dt",
        "double",
        [["2024-01-01", "0.1"], ["2024-01-02", "0.1"], ["2024-01-04", "0.0"]],
    ),
    "range/no_frame_control": (
        "SELECT dt, c " + RANGE_VALUES + " ORDER BY dt",
        "double",
        [["2024-01-01", "0.1"], ["2024-01-02", "1e-20"], ["2024-01-04", "-0.0"]],
    ),
}
MAIN_ANSWERS = {
    "trip/values_user_cast_both": (
        "SELECT c FROM VALUES (CAST(0.1 AS DECIMAL(30,15))), "
        "(CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(30,15))) AS t(c)",
        "decimal(30,15)",
        [["0.100000000000000"], ["0E-15"]],
    ),
    "trip/values_user_cast_beside_a_decimal": (
        "SELECT c FROM VALUES (CAST(0.5 AS DECIMAL(10,2))), "
        "(CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(10,2))), (1.5) AS t(c)",
        "decimal(10,2)",
        [["0.50"], ["0.00"], ["1.50"]],
    ),
    "trip/values_all_double": (
        f"SELECT c FROM VALUES (CAST(0.1 AS DOUBLE)), ({TINY}) AS t(c)",
        "double",
        [["0.1"], ["1e-20"]],
    ),
    "trip/values_all_decimal": (
        "SELECT c FROM VALUES (0.1), (CAST(2 AS DECIMAL(10,2))) AS t(c)",
        "decimal(10,2)",
        [["0.10"], ["2.00"]],
    ),
    "trip/values_single_row": (
        f"SELECT c, e FROM VALUES (0.1, {TINY}) AS t(c, e)",
        "double",
        [["0.1", "1e-20"]],
    ),
    "trip/values_int_double": (
        f"SELECT c FROM VALUES (1), ({TINY}) AS t(c)",
        "double",
        [["1.0"], ["1e-20"]],
    ),
    "trip/values_10k_one_double": (
        "SELECT count(*), count(DISTINCT b) FROM VALUES "
        + ", ".join(f"({i}, 0.5)" for i in range(999))
        + f", (999, {TINY}) AS x(a, b)",
        "bigint",
        [["1000", "2"]],
    ),
}
RECURSIVE = (
    "WITH RECURSIVE r(n, c) AS (SELECT * FROM VALUES (1, CAST(0.5 AS DECIMAL(10,2))), "
    "(2, CAST(1.5 AS DECIMAL(10,2))) AS t(n, c) UNION ALL SELECT n + 2, c FROM r WHERE n < 3) "
    "SELECT n, c FROM r ORDER BY n"
)
COW = (
    "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
)
MOR = (
    "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
)
SEEDED = [["1", "0.10", "0.1", "1"], ["2", "0.00", "1e-20", "2"], ["3", "0.00", "0.0", "3"]]
SEEDED_ROW4 = ["4", "0.10", "0.10000000000000002", "4"]
MERGED = [
    ["1", "0.10", "0.1", "1"],
    ["2", "0.00", "1e-20", "2"],
    ["3", "0.00", "-1e-20", "3"],
    SEEDED_ROW4,
]
DML = {
    "merge_union_src": (
        "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS c UNION ALL SELECT 2, "
        + TINY
        + " UNION ALL SELECT 3, "
        + NEG
        + ") s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.c",
        MERGED,
    ),
    "merge_union_right_deep": (
        "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS c UNION ALL (SELECT 2, "
        + TINY
        + " UNION ALL SELECT 3, "
        + NEG
        + ")) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.c",
        MERGED,
    ),
    "merge_union_view": (
        "MERGE INTO {t} t USING uv3 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.c",
        MERGED,
    ),
    "merge_values_src": (
        "MERGE INTO {t} t USING (SELECT * FROM VALUES (1, 0.1), (2, "
        + TINY
        + "), (3, "
        + NEG
        + ") AS s0(id, c)) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.c",
        MERGED,
    ),
    "merge_values_view": (
        "MERGE INTO {t} t USING sv s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.c",
        MERGED,
    ),
    "merge_values_insert": (
        "MERGE INTO {t} t USING (SELECT * FROM VALUES (7, 0.1), (8, "
        + TINY
        + ") AS s0(id, c)) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, d, v, w) "
        "VALUES (s.id, 0.0, s.c, 0)",
        [*SEEDED, SEEDED_ROW4, ["7", "0.00", "0.1", "0"], ["8", "0.00", "1e-20", "0"]],
    ),
    "merge_values_on": (
        "MERGE INTO {t} t USING (SELECT * FROM VALUES (0.1), ("
        + TINY
        + ") AS s0(c)) s ON t.v = s.c WHEN MATCHED THEN DELETE",
        [["3", "0.00", "0.0", "3"], SEEDED_ROW4],
    ),
    "delete_in_table": (
        "DELETE FROM {t} WHERE id IN (SELECT id FROM ice.ns.src)",
        [["1", "0.10", "0.1", "1"], SEEDED_ROW4],
    ),
    "delete_exists_table": (
        "DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM ice.ns.src s WHERE s.id = {t}.id)",
        [["1", "0.10", "0.1", "1"], SEEDED_ROW4],
    ),
}
DML_REFUSES = {
    "delete_in_values": "DELETE FROM {t} WHERE v IN (SELECT c FROM VALUES " + VALS + " AS x(c))",
    "update_in_values": "UPDATE {t} SET w = 9 WHERE v IN (SELECT c FROM VALUES "
    + VALS
    + " AS x(c))",
    "delete_exists_values": "DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM VALUES "
    + VALS
    + " AS x(c) WHERE x.c = {t}.v)",
}
WRITES = {
    "insert_dec38": (
        "DECIMAL(38,18)",
        f"INSERT INTO {{t}} VALUES (0.1), ({TINY})",
        ["0E-18", "0.100000000000000000"],
    ),
    "insert_string": ("STRING", f"INSERT INTO {{t}} VALUES (0.1), ({TINY})", ["0.1", "1.0E-20"]),
    "insert_double": ("DOUBLE", f"INSERT INTO {{t}} VALUES (0.1), ({TINY})", ["1e-20", "0.1"]),
    "insert_dec10": (
        "DECIMAL(10,2)",
        "INSERT INTO {t} VALUES (1.255), (CAST(1.255 AS DOUBLE))",
        ["1.26", "1.26"],
    ),
    "insert_select_values": (
        "STRING",
        f"INSERT INTO {{t}} SELECT c FROM VALUES (0.1), ({TINY}) AS v(c)",
        ["0.1", "1.0E-20"],
    ),
    "overwrite_values": (
        "DOUBLE",
        f"INSERT OVERWRITE {{t}} VALUES (0.1), ({TINY})",
        ["1e-20", "0.1"],
    ),
    "overwrite_empty_values": (
        "DOUBLE",
        f"INSERT OVERWRITE {{t}} SELECT c FROM VALUES (0.1), ({TINY}) AS v(c) WHERE c > 1",
        [],
    ),
    "user_cast_into_dec": (
        "DECIMAL(10,2)",
        "INSERT INTO {t} VALUES (CAST(CAST(1.255 AS DOUBLE) AS DECIMAL(10,2))), (1.5)",
        ["1.26", "1.50"],
    ),
}
LITERAL_ROWS = (
    "SELECT * FROM VALUES (1, CAST('0.1' AS DOUBLE)), (2, CAST('0.30000000000000004' AS DOUBLE)), "
    "(3, CAST('0.2' AS DOUBLE)) AS t(id, v)"
)
LITERAL_SHAPES = {
    "cmp": ("v = 0.1", ["True", "False", "False"]),
    "in": ("v IN (0.1, 0.3)", ["True", "False", "False"]),
    "between": ("v BETWEEN 0.1 AND 0.2", ["True", "False", "True"]),
    "case": ("CASE v WHEN 0.1 THEN 'a' WHEN 0.3 THEN 'b' ELSE 'c' END", ["a", "c", "c"]),
    "cmp_typed": ("v = 0.1BD", ["True", "False", "False"]),
    "in_typed": ("v IN (0.1BD, 0.3BD)", ["True", "False", "False"]),
}


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-decimal-double-coercion-2")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    session.sql(f"SELECT c FROM VALUES {VALS} AS t(c)").createOrReplaceTempView("vv")
    session.sql(" UNION ALL ".join([B_DEC, *B_DBL[:2]])).createOrReplaceTempView("uv3")
    session.sql(
        f"SELECT * FROM VALUES (1, 0.1), (2, {TINY}), (3, {NEG}) AS s0(id, c)"
    ).createOrReplaceTempView("sv")
    yield session
    session.stop()


@pytest.fixture(params=["true", "false"])
def native(request: pytest.FixtureRequest) -> Iterator[Any]:
    import repark

    session = PyReparkSession.native(config={"spark.sql.ansi.enabled": request.param})
    previous = getattr(repark, "_ANSI_NATIVE", None)
    repark._ANSI_NATIVE = session
    try:
        yield repark
    finally:
        repark._ANSI_NATIVE = previous


def shown(frame: Any) -> list[list[str]]:
    return [[str(value) for value in row] for row in frame.collect()]


def collision_cells() -> list[str]:
    return [f"{shape}/{z}" for shape in SHAPES for z in ZROWS]


@pytest.mark.parametrize("cell", collision_cells())
def test_a_double_whose_decimal_30_15_image_collides_with_a_truncated_values_cell_answers_as_spark(
    spark: Any, cell: str
) -> None:
    shape, z = cell.split("/")
    expected = COLLISION.get(shape, [["1"]]) if z == "tiny" else []
    assert shown(spark.sql(SHAPES[shape].format(z=ZROWS[z]))) == expected


FRAGMENTS: dict[str, Callable[[Any], Any]] = {
    "fexpr_filter_z_in_values": lambda frame: frame.filter(sf.expr(M1_Z)).select("id"),
    "fexpr_filter_exists_values": lambda frame: frame.filter(sf.expr(M1_EXISTS)).select("id"),
    "filter_str_z_in_values": lambda frame: frame.filter(M1_Z).select("id"),
}


@pytest.mark.parametrize("cell", sorted(FRAGMENTS))
def test_a_fragment_over_a_values_collision_cell_answers_as_spark(spark: Any, cell: str) -> None:
    frame = spark.sql(f"SELECT 1 AS id, {TINY} AS z")
    assert shown(FRAGMENTS[cell](frame)) == [["1"]]


def test_a_dataframe_join_over_a_values_frame_answers_as_spark(spark: Any) -> None:
    base = spark.sql(f"SELECT 1 AS id, {TINY} AS z")
    values = spark.sql(f"SELECT c FROM VALUES {VALS} AS t(c)")
    assert shown(base.join(values, base.z == values.c).select("id")) == [["1"]]
    assert shown(values.filter(sf.col("c") == sf.lit(1e-20))) == [["1e-20"]]
    assert shown(values.filter("c = CAST('1e-20' AS DOUBLE)")) == [["1e-20"]]
    assert values.dtypes == [("c", "double")]
    assert shown(values.select((sf.col("c") * 2).alias("c2"))) == [["0.2"], ["2e-20"]]


def test_a_scalar_fragment_over_values_is_sparks_double(spark: Any) -> None:
    frame = spark.sql("SELECT 1 AS id")
    scalar = f"(SELECT min(c) FROM VALUES {VALS} AS t(c))"
    assert shown(frame.select(sf.expr(scalar).alias("c"))) == [["1e-20"]]
    assert shown(frame.selectExpr(scalar + " AS c")) == [["1e-20"]]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (f"SELECT {TINY} AS c INTERSECT SELECT c FROM VALUES {VALS} AS t(c)", [["1e-20"]]),
        (f"SELECT c FROM VALUES {VALS} AS t(c) INTERSECT SELECT {TINY}", [["1e-20"]]),
        (f"SELECT c FROM VALUES {VALS} AS t(c) EXCEPT SELECT {TINY} AS c", [["0.1"]]),
    ],
)
def test_a_set_operation_with_a_mixed_values_leg_answers_as_spark(
    spark: Any, sql: str, expected: list[list[str]]
) -> None:
    assert shown(spark.sql(sql)) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (f"SELECT c FROM VALUES {VALS} AS t(c) INTERSECT ALL SELECT {TINY} AS c", [["1e-20"]]),
        (f"SELECT c FROM VALUES {VALS} AS t(c) EXCEPT ALL SELECT {TINY} AS c", [["0.1"]]),
        (f"SELECT {DEC} AS c INTERSECT ALL SELECT c FROM VALUES {VALS} AS t(c)", [["0.1"]]),
        (f"SELECT {DEC} AS c EXCEPT ALL SELECT c FROM VALUES {VALS} AS t(c)", []),
        (
            f"SELECT c FROM VALUES ({DEC}), ({DEC}), ({DEC}) AS t(c) EXCEPT ALL "
            f"SELECT c FROM VALUES {VALS} AS t(c)",
            [["0.1"], ["0.1"]],
        ),
    ],
)
def test_the_all_set_operations_over_a_mixed_values_leg_answer_as_spark(
    spark: Any, sql: str, expected: list[list[str]]
) -> None:
    frame = spark.sql(sql)
    assert frame.schema.simpleString() == "struct<c:double>"
    assert shown(frame) == expected


def explained(spark: Any, sql: str) -> str:
    return "\n".join(str(value) for row in spark.sql(sql).collect() for value in row)


def test_explain_of_a_mixed_values_plans_and_shows_double(spark: Any) -> None:
    text = explained(spark, f"EXPLAIN SELECT c FROM VALUES {VALS} AS t(c)")
    assert "Float64(" in text, text
    assert "Decimal128(Some(0),30,15)" not in text, text
    assert "__repark_values_cell" not in text, text
    control = explained(spark, f"EXPLAIN SELECT {DEC} AS c")
    assert "Decimal128(Some(10),10,2)" in control, control


def test_explain_extended_still_refuses_at_the_parser(spark: Any) -> None:
    with pytest.raises(Exception, match="EXTENDED"):
        spark.sql(f"EXPLAIN EXTENDED SELECT c FROM VALUES {VALS} AS t(c)").collect()


@pytest.mark.parametrize("cell", sorted(UNIONS))
def test_a_union_of_three_or_more_branches_with_the_decimal_first_is_double(
    spark: Any, cell: str
) -> None:
    frame = spark.sql(UNIONS[cell] + (" ORDER BY 1" if cell not in ("cte3", "view3") else ""))
    assert frame.dtypes[-1] == ("c", "double")
    assert sorted(shown(frame)) == sorted(UNION_ROWS[cell])


def test_a_union_in_a_subquery_compares_in_double(spark: Any) -> None:
    sql = "SELECT count(*) FROM (" + " UNION ALL ".join([B_DEC, *B_DBL[:2]]) + ") WHERE c <> 0"
    assert shown(spark.sql(sql)) == [["3"]]


@pytest.mark.parametrize("cell", sorted(SEAT_V))
def test_seat_v_types_a_mixed_values_as_sparks_double(spark: Any, cell: str) -> None:
    sql, data_type, rows = SEAT_V[cell]
    frame = spark.sql(sql)
    column = 1 if cell.startswith("range/") else -1
    assert frame.dtypes[column][1] == data_type
    assert (
        shown(frame) == rows if cell.startswith("range/") else sorted(shown(frame)) == sorted(rows)
    )


@pytest.mark.parametrize("cell", sorted(MAIN_ANSWERS))
def test_the_tripwire_refuses_nothing_main_answers_as_spark(spark: Any, cell: str) -> None:
    sql, data_type, rows = MAIN_ANSWERS[cell]
    frame = spark.sql(sql)
    assert frame.dtypes[-1][1] == data_type
    assert shown(frame) == rows


def test_a_recursive_cte_over_values_keeps_mains_refusal(spark: Any) -> None:
    with pytest.raises(Exception, match="Cannot project plan column"):
        spark.sql(RECURSIVE).collect()


def seeded_table(spark: Any, mode: str, name: str) -> str:
    table = f"ice.ns.{name}_{mode}"
    props = COW if mode == "cow" else MOR
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, d DECIMAL(10,2), v DOUBLE, w BIGINT) USING iceberg "
        f"TBLPROPERTIES ({props})"
    ).collect()
    spark.sql(
        f"INSERT INTO {table} VALUES (1, 0.10, CAST(0.1 AS DOUBLE), 1), (2, 0.00, {TINY}, 2), "
        "(3, 0.00, CAST(0.0 AS DOUBLE), 3), (4, 0.10, CAST('0.10000000000000002' AS DOUBLE), 4)"
    ).collect()
    return table


@pytest.mark.parametrize("mode", ["cow", "mor"])
@pytest.mark.parametrize("cell", sorted(DML))
def test_a_dml_over_a_values_or_union_source_stores_sparks_rows(
    spark: Any, mode: str, cell: str
) -> None:
    spark.sql("CREATE TABLE IF NOT EXISTS ice.ns.src (id BIGINT) USING iceberg").collect()
    if not spark.sql("SELECT * FROM ice.ns.src").collect():
        spark.sql("INSERT INTO ice.ns.src VALUES (2), (3)").collect()
    template, rows = DML[cell]
    table = seeded_table(spark, mode, cell)
    spark.sql(template.format(t=table)).collect()
    assert shown(spark.sql(f"SELECT id, d, v, w FROM {table} ORDER BY id")) == rows


@pytest.mark.parametrize("mode", ["cow", "mor"])
@pytest.mark.parametrize("cell", sorted(DML_REFUSES))
def test_a_dml_where_subquery_over_values_still_refuses(spark: Any, mode: str, cell: str) -> None:
    table = seeded_table(spark, mode, cell)
    with pytest.raises(Exception, match="subquery in its WHERE clause is refused"):
        spark.sql(DML_REFUSES[cell].format(t=table)).collect()
    assert len(spark.sql(f"SELECT id FROM {table}").collect()) == 4


@pytest.mark.parametrize("cell", sorted(WRITES))
def test_a_values_write_stores_sparks_values(spark: Any, cell: str) -> None:
    column_type, template, values = WRITES[cell]
    table = f"ice.ns.w_{cell}"
    spark.sql(f"CREATE TABLE {table} (c {column_type}) USING iceberg").collect()
    if cell == "overwrite_empty_values":
        spark.sql(f"INSERT INTO {table} VALUES (CAST(7 AS DOUBLE))").collect()
    spark.sql(template.format(t=table)).collect()
    got = [str(row[0]) for row in spark.sql(f"SELECT c FROM {table}").collect()]
    assert sorted(got) == sorted(values)


def test_a_ctas_over_a_mixed_values_creates_double(spark: Any) -> None:
    spark.sql(
        f"CREATE TABLE ice.ns.ctas_v USING iceberg AS SELECT c FROM VALUES {VALS} AS v(c)"
    ).collect()
    frame = spark.sql("SELECT c FROM ice.ns.ctas_v")
    assert frame.dtypes == [("c", "double")]
    assert sorted(str(row[0]) for row in frame.collect()) == ["0.1", "1e-20"]


@pytest.mark.parametrize("shape", sorted(LITERAL_SHAPES))
def test_the_four_literal_shapes_through_fexpr_answer_as_spark_on_the_spark_door(
    spark: Any, shape: str
) -> None:
    text, expected = LITERAL_SHAPES[shape]
    frame = spark.sql(LITERAL_ROWS).select("id", sf.expr(text).alias("p"))
    assert [str(row[1]) for row in frame.collect()] == expected


NATIVE_FRAGMENTS: dict[str, tuple[Callable[[Any], Any], list[list[str]]]] = {
    "fexpr_in_values": (lambda frame: frame.filter(sf.expr(M1_Z)).select("id"), [["1"]]),
    "filter_str_in_values": (lambda frame: frame.filter(M1_Z).select("id"), [["1"]]),
    "fexpr_exists_values": (lambda frame: frame.filter(sf.expr(M1_EXISTS)).select("id"), [["1"]]),
    "fexpr_scalar_values": (
        lambda frame: frame.select(
            sf.expr(f"(SELECT min(c) FROM VALUES {VALS} AS t(c))").alias("m")
        ),
        [["0E-15"]],
    ),
}


@pytest.mark.parametrize("cell", sorted(NATIVE_FRAGMENTS))
def test_a_native_frame_over_a_fragment_values_subquery_plans_and_keeps_mains_answer(
    native: Any, cell: str
) -> None:
    build, expected = NATIVE_FRAGMENTS[cell]
    frame = build(native.sql(f"SELECT 1 AS id, {TINY} AS z"))
    frame.explain()
    assert shown(frame) == expected


NATIVE_LITERALS = {
    "cmp": ["True", "False", "False"],
    "in": ["True", "False", "False"],
    "between": ["True", "False", "True"],
    "case": ["a", "c", "c"],
    "cmp_typed": ["True", "False", "False"],
    "in_typed": ["True", "True", "False"],
}


@pytest.mark.parametrize("shape", sorted(NATIVE_LITERALS))
def test_the_four_literal_shapes_through_fexpr_keep_mains_answer_on_the_native_door(
    native: Any, shape: str
) -> None:
    text, _ = LITERAL_SHAPES[shape]
    frame = native.sql(LITERAL_ROWS).select("id", sf.expr(text).alias("p"))
    assert [str(row[1]) for row in frame.collect()] == NATIVE_LITERALS[shape]

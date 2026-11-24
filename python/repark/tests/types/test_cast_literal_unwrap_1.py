from __future__ import annotations

import itertools
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

SHIELD = "__repark_cast_shield__"
DEC = [
    "0.50",
    "1.00",
    "1.25",
    "1.50",
    "1.99",
    "-0.50",
    "-1.25",
    "-1.50",
    "2.50",
    "0.00",
    "99999999.99",
    "-99999999.99",
    None,
]
BIGINT = [
    "1",
    "2",
    "-1",
    "0",
    "2147483647",
    "2147483648",
    "4294967297",
    "-2147483649",
    "-9223372036854775808",
    None,
]
MODES = {
    "cow": (
        "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
        "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
    ),
    "mor": (
        "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
        "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
    ),
}
HEADLINE = {
    "int_eq": ("CAST(c AS INT) = 1", [2, 3, 4, 5]),
    "int_gt": ("CAST(c AS INT) > 0", [2, 3, 4, 5, 9, 11]),
    "dec0_eq": ("CAST(c AS DECIMAL(10,0)) = 1", [1, 2, 3]),
    "dec1_eq": ("CAST(c AS DECIMAL(10,1)) = 1.3", [3]),
    "int_in": ("CAST(c AS INT) IN (1, 5)", [2, 3, 4, 5]),
}
FRAMES: dict[str, Callable[[Any], Any]] = {
    "int_eq": lambda c: c.cast("int") == 1,
    "int_gt": lambda c: c.cast("int") > 0,
    "dec0_eq": lambda c: c.cast("decimal(10,0)") == 1,
    "dec1_eq": lambda c: c.cast("decimal(10,1)") == sf.lit("1.3").cast("decimal(10,1)"),
    "int_in": lambda c: c.cast("int").isin(1, 5),
}
SHAPES = {
    "nested_int": ("CAST(CAST(c AS INT) AS BIGINT) = 1", [2, 3, 4, 5]),
    "nested_dec": ("CAST(CAST(c AS DECIMAL(10,0)) AS DECIMAL(20,0)) = 1", [1, 2, 3]),
    "try_cast_int": ("TRY_CAST(c AS INT) = 1", [2, 3, 4, 5]),
    "try_cast_dec": ("TRY_CAST(c AS DECIMAL(10,0)) = 1", [1, 2, 3]),
    "colon_int": ("c::int = 1", [2, 3, 4, 5]),
    "colon_dec": ("c::decimal(10,0) = 1", [1, 2, 3]),
    "is_distinct": ("CAST(c AS INT) IS DISTINCT FROM 1", [1, 6, 7, 8, 9, 10, 11, 12, 13]),
    "is_not_distinct": ("CAST(c AS INT) IS NOT DISTINCT FROM 1", [2, 3, 4, 5]),
    "is_distinct_zero": ("CAST(c AS INT) IS DISTINCT FROM 0", [2, 3, 4, 5, 7, 8, 9, 11, 12, 13]),
    "null_safe_zero": ("CAST(c AS INT) <=> 0", [1, 6, 10]),
    "not_between": ("CAST(c AS INT) NOT BETWEEN 1 AND 2", [1, 6, 7, 8, 10, 11, 12]),
    "between": ("CAST(c AS INT) BETWEEN 1 AND 2", [2, 3, 4, 5, 9]),
    "not_in": ("CAST(c AS INT) NOT IN (1, 0)", [7, 8, 9, 11, 12]),
    "decimal_constant": ("CAST(c AS INT) = 1.0", [2, 3, 4, 5]),
    "double_constant": ("CAST(c AS DECIMAL(10,1)) = CAST(1.3 AS DOUBLE)", [3]),
    "double_in": ("CAST(c AS DECIMAL(10,1)) IN (CAST(1.3 AS DOUBLE), CAST(2.5 AS DOUBLE))", [3, 9]),
    "constant_left": ("1 < CAST(c AS INT)", [9, 11]),
    "neg_half_eq": ("CAST(c AS DECIMAL(10,0)) = -1", [6, 7]),
    "neg_half_lt": ("CAST(c AS DECIMAL(10,0)) < -1", [8, 12]),
    "control_arith": ("CAST(c AS INT) + 1 = 2", [2, 3, 4, 5]),
    "control_abs": ("abs(CAST(c AS INT)) = 1", [2, 3, 4, 5, 7, 8]),
}
CONTROLS = {
    "double_int": ("DOUBLE", ["0.5", "1.0", "1.25", "1.99", "-1.5", "2.5"], "INT", "1", [2, 3, 4]),
    "float_int": ("FLOAT", ["0.5", "1.0", "1.25", "1.99", "-1.5", "2.5"], "INT", "1", [2, 3, 4]),
    "string_int": ("STRING", ["1", "2", "10"], "INT", "1", [1]),
    "ts_date": (
        "TIMESTAMP",
        ["2024-01-01 00:00:00", "2024-01-01 23:59:59", "2024-01-02 00:00:00"],
        "DATE",
        "DATE'2024-01-01'",
        [1, 2],
    ),
}
DML = {
    "delete_eq": "DELETE FROM {t} WHERE CAST(c AS {T}) = {k}",
    "delete_gt": "DELETE FROM {t} WHERE CAST(c AS {T}) > {k}",
    "update_eq": "UPDATE {t} SET tag = 'u' WHERE CAST(c AS {T}) = {k}",
    "merge_on": (
        "MERGE INTO {t} t USING (SELECT 1 AS k) s ON CAST(t.c AS {T}) = {k} "
        "WHEN MATCHED THEN UPDATE SET tag = 'm'"
    ),
    "merge_and": (
        "MERGE INTO {t} t USING (SELECT * FROM VALUES {ids} AS x(sid)) s ON t.id = s.sid "
        "WHEN MATCHED AND CAST(t.c AS {T}) = {k} THEN UPDATE SET tag = 'm'"
    ),
}
DML_KINDS = {
    "dec_int": ("INT", "1", {"delete_gt": [9, 11]}, [2, 3, 4, 5]),
    "dec_dec0": ("DECIMAL(10,0)", "1", {"delete_gt": [4, 5, 9, 11]}, [1, 2, 3]),
    "dec_dec1": ("DECIMAL(10,1)", "1.3", {"delete_gt": [4, 5, 9, 11]}, [3]),
}
CAST_OVERFLOW = r"(?i)overflow|out of range|too large|can't cast value"
COUNTER = itertools.count()


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-cast-literal-unwrap-1")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def ansi_on(spark: Any) -> bool:
    return str(spark.conf.get("spark.sql.ansi.enabled")).lower() == "true"


def literal(value: str | None, sql_type: str) -> str:
    return "NULL" if value is None else f"CAST('{value}' AS {sql_type})"


def values(items: list[str | None], sql_type: str, column: str = "c") -> str:
    rows = ", ".join(f"({i}, {literal(v, sql_type)})" for i, v in enumerate(items, start=1))
    return f"SELECT * FROM VALUES {rows} AS x(id, `{column}`)"


def view(spark: Any, items: list[str | None], sql_type: str, column: str = "c") -> str:
    name = f"v{next(COUNTER)}"
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {values(items, sql_type, column)}").collect()
    return name


def ids(frame: Any) -> list[int]:
    return sorted(int(r[0]) for r in frame.collect())


def plan_of(spark: Any, sql: str) -> str:
    return " ".join(str(r[-1]) for r in spark.sql("EXPLAIN " + sql).collect())


@pytest.mark.parametrize("door", ["sql", "frame", "fexpr", "filter_str"])
@pytest.mark.parametrize("cell", sorted(HEADLINE))
def test_a_narrowing_cast_compares_as_its_cast_values(spark: Any, door: str, cell: str) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)")
    predicate, want = HEADLINE[cell]
    frame = spark.table(table)
    run = {
        "sql": lambda: spark.sql(f"SELECT id FROM {table} WHERE {predicate}"),
        "frame": lambda: frame.filter(FRAMES[cell](sf.col("c"))).select("id"),
        "fexpr": lambda: frame.filter(sf.expr(predicate)).select("id"),
        "filter_str": lambda: frame.filter(predicate).select("id"),
    }[door]
    assert ids(run()) == want


@pytest.mark.parametrize("cell", sorted(HEADLINE))
def test_the_native_door_compares_as_the_cast_values(cell: str) -> None:
    name = f"nv_cast_unwrap_{next(COUNTER)}"
    items = ", ".join(literal(v, "DECIMAL(10,2)") for v in DEC)
    rows = ", ".join(f"({i})" for i in range(1, len(DEC) + 1))
    repark.sql(
        f"CREATE VIEW {name} AS SELECT CAST(column1 AS INT) AS id, "
        f"array_element(make_array({items}), CAST(column1 AS INT)) AS c FROM (VALUES {rows})"
    ).collect()
    predicate, want = HEADLINE[cell]
    assert (
        sorted(int(r[0]) for r in repark.sql(f"SELECT id FROM {name} WHERE {predicate}").collect())
        == want
    )


@pytest.mark.parametrize("cell", sorted(SHAPES))
def test_the_critic_shapes_compare_as_the_cast_values(spark: Any, cell: str) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)")
    predicate, want = SHAPES[cell]
    assert ids(spark.sql(f"SELECT id FROM {table} WHERE {predicate}")) == want


def test_a_non_ascii_column_name_is_shielded(spark: Any) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)", "café")
    assert ids(spark.sql(f"SELECT id FROM {table} WHERE CAST(`café` AS INT) = 1")) == [2, 3, 4, 5]


def test_a_cast_defined_in_one_place_and_compared_in_another_is_shielded(spark: Any) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)")
    subquery = f"SELECT id FROM (SELECT id, CAST(c AS INT) AS k FROM {table}) WHERE k = 1"
    assert ids(spark.sql(subquery)) == [2, 3, 4, 5]
    outer = f"w{next(COUNTER)}"
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {outer} AS SELECT id, CAST(c AS INT) AS k FROM {table}")
    assert ids(spark.sql(f"SELECT id FROM {outer} WHERE k = 1")) == [2, 3, 4, 5]
    derived = spark.table(table).withColumn("k", sf.col("c").cast("int"))
    assert ids(derived.filter("k = 1").select("id")) == [2, 3, 4, 5]
    assert ids(derived.filter(sf.col("k") == 1).select("id")) == [2, 3, 4, 5]


def test_the_select_list_keeps_its_names(spark: Any) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)")
    frame = spark.sql(f"SELECT CAST(c AS INT) = 1, CAST(c AS INT) IN (1, 5) AS listed FROM {table}")
    assert not any(SHIELD in name for name in frame.columns)
    assert frame.columns[1] == "listed"
    rows = sorted(frame.collect(), key=lambda r: (r[0] is None, r[0]))
    assert [r[0] for r in rows].count(True) == 4


@pytest.mark.parametrize("cell", sorted(CONTROLS))
def test_float_string_and_timestamp_casts_are_never_shielded(spark: Any, cell: str) -> None:
    source, items, target, constant, want = CONTROLS[cell]
    table = view(spark, list(items), source)
    sql = f"SELECT id FROM {table} WHERE CAST(c AS {target}) = {constant}"
    assert ids(spark.sql(sql)) == want
    assert SHIELD not in plan_of(spark, sql)


def test_a_shielded_range_plans_no_per_row_cast_and_an_overflow_capable_one_stays(
    spark: Any,
) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)")
    wide = view(spark, BIGINT, "BIGINT")
    assert SHIELD not in plan_of(spark, f"SELECT id FROM {table} WHERE CAST(c AS INT) = 1")
    assert SHIELD not in plan_of(
        spark, f"SELECT id FROM {table} WHERE CAST(c AS INT) IN (1, 2, 3, 4, 5)"
    )
    assert "__repark_narrow_to_int__" in plan_of(
        spark, f"SELECT id FROM {wide} WHERE CAST(c AS INT) = 1"
    )
    assert "__repark_narrow_to_tinyint__" in plan_of(
        spark, f"SELECT id FROM {table} WHERE CAST(c AS TINYINT) = 1"
    )
    assert SHIELD in plan_of(spark, f"SELECT id FROM {wide} WHERE TRY_CAST(c AS INT) = 1")
    assert SHIELD in plan_of(spark, f"SELECT id FROM {table} WHERE TRY_CAST(c AS TINYINT) = 1")


def test_an_in_list_past_the_cap_stays_per_row_and_answers_as_spark(spark: Any) -> None:
    table = view(spark, DEC, "DECIMAL(10,2)")
    items = ", ".join(str(k) for k in range(2, 42))
    sql = f"SELECT id FROM {table} WHERE CAST(c AS INT) IN ({items})"
    assert SHIELD in plan_of(spark, sql)
    assert ids(spark.sql(sql)) == [9]


def make_table(spark: Any, items: list[str | None], sql_type: str, mode: str) -> str:
    name = f"ice.ns.t{next(COUNTER)}"
    spark.sql(
        f"CREATE TABLE {name} (id INT, c {sql_type}, tag STRING) USING iceberg "
        f"TBLPROPERTIES ({MODES[mode]})"
    ).collect()
    rows = ", ".join(f"({i}, {literal(v, sql_type)}, 'o')" for i, v in enumerate(items, start=1))
    spark.sql(f"INSERT INTO {name} VALUES {rows}").collect()
    return name


def changed(spark: Any, table: str, count: int) -> list[int]:
    kept = {int(r[0]): r[1] for r in spark.sql(f"SELECT id, tag FROM {table}").collect()}
    return [i for i in range(1, count + 1) if kept.get(i) != "o"]


def dml(kind: str, table: str, target: str, constant: str, count: int) -> str:
    sources = ", ".join(f"({i})" for i in range(1, count + 1))
    return DML[kind].format(t=table, T=target, k=constant, ids=sources)


@pytest.mark.parametrize("mode", sorted(MODES))
@pytest.mark.parametrize("kind", sorted(DML))
@pytest.mark.parametrize("family", sorted(DML_KINDS))
def test_dml_through_a_narrowing_cast_writes_as_spark(
    spark: Any, family: str, kind: str, mode: str
) -> None:
    target, constant, special, default = DML_KINDS[family]
    table = make_table(spark, DEC, "DECIMAL(10,2)", mode)
    spark.sql(dml(kind, table, target, constant, len(DEC))).collect()
    assert changed(spark, table, len(DEC)) == special.get(kind, default)


@pytest.mark.parametrize("mode", sorted(MODES))
@pytest.mark.parametrize("kind", sorted(DML))
def test_dml_through_an_overflowing_cast_raises_or_refuses(
    spark: Any, kind: str, mode: str
) -> None:
    table = make_table(spark, BIGINT, "BIGINT", mode)
    if ansi_on(spark):
        with pytest.raises(Exception, match=CAST_OVERFLOW):
            spark.sql(dml(kind, table, "INT", "1", len(BIGINT))).collect()
        assert changed(spark, table, len(BIGINT)) == []
    else:
        spark.sql(dml(kind, table, "INT", "1", len(BIGINT))).collect()
        want = [2, 5, 8] if kind == "delete_gt" else [1, 7]
        assert changed(spark, table, len(BIGINT)) == want


def test_an_overflow_capable_cast_refuses_per_row_with_ansi_off(spark: Any) -> None:
    wide = view(spark, BIGINT, "BIGINT")
    in_range = view(spark, BIGINT[:5], "BIGINT")
    if ansi_on(spark):
        for constant in ("1", "5"):
            with pytest.raises(Exception, match=CAST_OVERFLOW):
                spark.sql(f"SELECT id FROM {wide} WHERE CAST(c AS INT) = {constant}").collect()
    else:
        assert ids(spark.sql(f"SELECT id FROM {wide} WHERE CAST(c AS INT) = 1")) == [1, 7]
        assert ids(spark.sql(f"SELECT id FROM {wide} WHERE CAST(c AS INT) = 5")) == []
    assert ids(spark.sql(f"SELECT id FROM {in_range} WHERE CAST(c AS INT) = 1")) == [1]
    assert ids(spark.sql(f"SELECT id FROM {wide} WHERE TRY_CAST(c AS INT) = 1")) == [1]


def test_the_top_of_type_comparison_refuses_per_row(spark: Any) -> None:
    table = view(spark, ["1.00", "127.00", "127.99", "128.00", "-129.00", None], "DECIMAL(10,2)")
    for predicate in (
        "CAST(c AS TINYINT) > 127",
        "127 < CAST(c AS TINYINT)",
        "CAST(c AS TINYINT) < -128",
    ):
        if ansi_on(spark):
            with pytest.raises(Exception, match=CAST_OVERFLOW):
                spark.sql(f"SELECT id FROM {table} WHERE {predicate}").collect()
        else:
            assert ids(spark.sql(f"SELECT id FROM {table} WHERE {predicate}")) == []


def scan_metrics(spark: Any, sql: str) -> list[str]:
    text = " ".join(
        " ".join(str(r[-1]).split()) for r in spark.sql("EXPLAIN ANALYZE " + sql).collect()
    )
    return re.findall(
        r"files_ranges_pruned_statistics=[^,]*|row_groups_pruned_statistics=[^,]*|N=\d+", text
    )


@pytest.mark.parametrize(
    ("shielded", "hand"),
    [
        ("CAST(c AS INT) = 1", "c >= 1 AND c < 2"),
        ("CAST(c AS DECIMAL(10,1)) = 1.3", "c >= 1.25 AND c < 1.35"),
    ],
)
def test_a_shielded_filter_prunes_as_the_hand_written_range(
    spark: Any, tmp_path: Path, shielded: str, hand: str
) -> None:
    path = str(tmp_path / "pq")
    quarter = "CAST(CAST(id AS DECIMAL(10,0)) / 4 AS DECIMAL(10,2))"
    for part in range(4):
        lo, hi = part * 8, part * 8 + 8
        spark.sql(
            f"SELECT CAST(id AS INT) AS id, {quarter} AS c FROM range({lo}, {hi})"
        ).write.mode("append").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("pq")
    table = f"ice.ns.p{next(COUNTER)}"
    spark.sql(f"CREATE TABLE {table} (id INT, c DECIMAL(10,2)) USING iceberg").collect()
    for part in range(4):
        lo, hi = part * 8, part * 8 + 8
        spark.sql(
            f"INSERT INTO {table} SELECT id, c FROM pq WHERE id >= {lo} AND id < {hi}"
        ).collect()
    for source in ("pq", table):
        got = scan_metrics(spark, f"SELECT id FROM {source} WHERE {shielded}")
        assert got == scan_metrics(spark, f"SELECT id FROM {source} WHERE {hand}"), source
        assert got, source
        assert ids(spark.sql(f"SELECT id FROM {source} WHERE {shielded}")) == ids(
            spark.sql(f"SELECT id FROM {source} WHERE {hand}")
        )
    pruned = scan_metrics(spark, f"SELECT id FROM pq WHERE {shielded}")[0]
    total, matched = re.findall(r"\d+", pruned)[:2]
    assert int(matched) < int(total)

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

GROUPS = {
    ("DOUBLE", 10, 2): [
        ("1.255", "1.26"),
        ("1.005", "1.01"),
        ("9.995", "10.00"),
        ("-1.255", "-1.26"),
        ("1.2549999999999997", "1.25"),
        ("2.675", "2.68"),
    ],
    ("DOUBLE", 38, 18): [
        ("0.30000000000000004", "0.300000000000000040"),
        ("1.005", "1.005000000000000000"),
    ],
    ("DOUBLE", 38, 0): [("1e23", "99999999999999990000000")],
    ("FLOAT", 10, 2): [("1.255", "1.25"), ("2.675", "2.67")],
    ("FLOAT", 38, 18): [("1.255", "1.254999995231628400")],
}
COW = (
    "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
)
MOR = (
    "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
)
COUNTER = itertools.count()


def lit(text: str, source: str) -> str:
    return f"CAST('{text}' AS {source})"


class Cell:
    def __init__(self, session: Any, source: str, precision: int, scale: int) -> None:
        self.session = session
        self.source = source
        self.decimal = f"DECIMAL({precision},{scale})"
        self.values = GROUPS[(source, precision, scale)]
        self.src = f"ice.ns.src{next(COUNTER)}"
        rows = ", ".join(f"({i}, {lit(text, source)})" for i, (text, _) in enumerate(self.values))
        session.sql(f"CREATE TABLE {self.src} (id INT, v {source}) USING iceberg").collect()
        session.sql(f"INSERT INTO {self.src} VALUES {rows}").collect()

    def sql(self, statement: str) -> None:
        self.session.sql(statement).collect()

    def table(self, props: str = COW, seeded: bool = False, partitioned: bool = False) -> str:
        name = f"ice.ns.t{next(COUNTER)}"
        columns = f"id INT, v {self.decimal}" + (", p INT" if partitioned else "")
        tail = " PARTITIONED BY (p)" if partitioned else ""
        self.sql(f"CREATE TABLE {name} ({columns}) USING iceberg{tail} TBLPROPERTIES ({props})")
        if seeded:
            self.sql(f"INSERT INTO {name} SELECT id, CAST(0 AS {self.decimal}) FROM {self.src}")
        return name

    def frame(self) -> Any:
        return self.session.sql(f"SELECT id, v FROM {self.src}")

    def read(self, frame: Any) -> dict[int, str]:
        return {int(row[0]): str(row[1]) for row in frame.collect()}

    def stored(self, table: str) -> dict[int, str]:
        return self.read(self.session.sql(f"SELECT id, v FROM {table}"))


def sel_cast_col(cell: Cell) -> dict[int, str]:
    return cell.read(cell.session.sql(f"SELECT id, CAST(v AS {cell.decimal}) FROM {cell.src}"))


def sel_cast_lit(cell: Cell) -> dict[int, str]:
    out = {}
    for i, (text, _) in enumerate(cell.values):
        frame = cell.session.sql(f"SELECT {i}, CAST({lit(text, cell.source)} AS {cell.decimal})")
        out.update(cell.read(frame))
    return out


def sel_try_cast(cell: Cell) -> dict[int, str]:
    statement = f"SELECT id, TRY_CAST(v AS {cell.decimal}) FROM {cell.src}"
    return cell.read(cell.session.sql(statement))


def df_cast(cell: Cell) -> dict[int, str]:
    target = cell.decimal.lower()
    return cell.read(cell.frame().select("id", sf.col("v").cast(target).alias("v")))


def ins_values(cell: Cell) -> dict[int, str]:
    table = cell.table()
    rows = ", ".join(f"({i}, {lit(text, cell.source)})" for i, (text, _) in enumerate(cell.values))
    cell.sql(f"INSERT INTO {table} VALUES {rows}")
    return cell.stored(table)


def ins_select(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.sql(f"INSERT INTO {table} SELECT id, v FROM {cell.src}")
    return cell.stored(table)


def ins_union(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.sql(
        f"INSERT INTO {table} SELECT id, v FROM {cell.src} WHERE id % 2 = 0 "
        f"UNION ALL SELECT id, v FROM {cell.src} WHERE id % 2 = 1"
    )
    return cell.stored(table)


def ctas(cell: Cell) -> dict[int, str]:
    table = f"ice.ns.t{next(COUNTER)}"
    cell.sql(
        f"CREATE TABLE {table} USING iceberg AS "
        f"SELECT id, CAST(v AS {cell.decimal}) AS v FROM {cell.src}"
    )
    return cell.stored(table)


def update_where(props: str) -> Callable[[Cell], dict[int, str]]:
    def door(cell: Cell) -> dict[int, str]:
        table = cell.table(props, seeded=True)
        for i, (text, _) in enumerate(cell.values):
            cell.sql(f"UPDATE {table} SET v = {lit(text, cell.source)} WHERE id = {i}")
        return cell.stored(table)

    return door


def update_all(cell: Cell) -> dict[int, str]:
    table = cell.table(seeded=True)
    case = " ".join(
        f"WHEN {i} THEN {lit(text, cell.source)}" for i, (text, _) in enumerate(cell.values)
    )
    cell.sql(f"UPDATE {table} SET v = CASE id {case} END")
    return cell.stored(table)


def merge_update(props: str) -> Callable[[Cell], dict[int, str]]:
    def door(cell: Cell) -> dict[int, str]:
        table = cell.table(props, seeded=True)
        cell.sql(
            f"MERGE INTO {table} t USING {cell.src} s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v"
        )
        return cell.stored(table)

    return door


def merge_insert(props: str) -> Callable[[Cell], dict[int, str]]:
    def door(cell: Cell) -> dict[int, str]:
        table = cell.table(props)
        cell.sql(
            f"MERGE INTO {table} t USING {cell.src} s ON t.id = s.id "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        return cell.stored(table)

    return door


def df_writeto(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.frame().writeTo(table).append()
    return cell.stored(table)


def df_saveastable(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.frame().write.mode("append").saveAsTable(table)
    return cell.stored(table)


def df_insertinto(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.frame().write.insertInto(table)
    return cell.stored(table)


def df_overwrite(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.frame().writeTo(table).overwritePartitions()
    return cell.stored(table)


def ins_overwrite(cell: Cell) -> dict[int, str]:
    table = cell.table()
    cell.sql(f"INSERT OVERWRITE {table} SELECT id, v FROM {cell.src}")
    return cell.stored(table)


def ins_overwrite_part(cell: Cell) -> dict[int, str]:
    table = cell.table(partitioned=True)
    cell.sql(f"INSERT OVERWRITE {table} PARTITION (p = 1) SELECT id, v FROM {cell.src}")
    return cell.stored(table)


DOORS: dict[str, Callable[[Cell], dict[int, str]]] = {
    "sel_cast_col": sel_cast_col,
    "sel_cast_lit": sel_cast_lit,
    "sel_try_cast": sel_try_cast,
    "df_cast": df_cast,
    "ins_values": ins_values,
    "ins_select": ins_select,
    "ins_union": ins_union,
    "ctas": ctas,
    "update_cow": update_where(COW),
    "update_mor": update_where(MOR),
    "update_all": update_all,
    "merge_update_cow": merge_update(COW),
    "merge_update_mor": merge_update(MOR),
    "merge_insert_cow": merge_insert(COW),
    "merge_insert_mor": merge_insert(MOR),
    "df_writeto": df_writeto,
    "df_saveastable": df_saveastable,
    "df_insertinto": df_insertinto,
    "df_overwrite": df_overwrite,
    "ins_overwrite": ins_overwrite,
    "ins_overwrite_part": ins_overwrite_part,
}


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-double-decimal-store-round-1")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    yield session
    session.stop()
    _reset_active_session_for_tests()


@pytest.mark.parametrize("door", sorted(DOORS))
def test_a_float_into_decimal_rounds_the_doubles_text_half_up(spark: Any, door: str) -> None:
    misses = []
    for (source, precision, scale), values in GROUPS.items():
        cell = Cell(spark, source, precision, scale)
        got = DOORS[door](cell)
        for i, (text, want) in enumerate(values):
            answer = got.get(i)
            if answer is None or Decimal(answer) != Decimal(want):
                misses.append(
                    f"{source} {text} -> DECIMAL({precision},{scale}): {answer}, Spark {want}"
                )
    assert misses == []


def test_the_card_rows_store_sparks_values(spark: Any) -> None:
    spark.sql("CREATE TABLE ice.ns.card (id INT, v DECIMAL(10,2)) USING iceberg").collect()
    spark.sql("INSERT INTO ice.ns.card VALUES (1, 0.1), (2, CAST(1.255 AS DOUBLE))").collect()
    spark.sql(
        "INSERT INTO ice.ns.card SELECT 3, 0.1 UNION ALL SELECT 4, CAST(1.005 AS DOUBLE)"
    ).collect()
    rows = spark.sql("SELECT id, v FROM ice.ns.card ORDER BY id").collect()
    assert [(row[0], str(row[1])) for row in rows] == [
        (1, "0.10"),
        (2, "1.26"),
        (3, "0.10"),
        (4, "1.01"),
    ]


def test_the_engine_cast_rounds_the_text_as_cast_double_to_decimal_rounding_1(
    spark: Any,
) -> None:
    row = spark.sql(
        "SELECT CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2)), "
        "CAST(CAST('2.675' AS DOUBLE) AS DECIMAL(5,2)), CAST(1.005D AS DECIMAL(5,2))"
    ).collect()[0]
    assert [str(value) for value in row] == ["1.01", "2.68", "1.01"]


@pytest.mark.parametrize("text", ["NaN", "Infinity", "-Infinity"])
def test_a_non_finite_value_still_refuses_and_try_cast_still_answers_null(
    spark: Any, text: str
) -> None:
    spark.sql("CREATE TABLE ice.ns.nf (id INT, v DOUBLE) USING iceberg").collect()
    spark.sql(f"INSERT INTO ice.ns.nf VALUES (1, CAST('{text}' AS DOUBLE))").collect()
    with pytest.raises(Exception, match="Overflowing on"):
        spark.sql("SELECT CAST(v AS DECIMAL(10,2)) FROM ice.ns.nf").collect()
    row = spark.sql("SELECT TRY_CAST(v AS DECIMAL(10,2)) FROM ice.ns.nf").collect()[0]
    assert row[0] is None


def test_an_overflowing_store_still_refuses(spark: Any) -> None:
    spark.sql("CREATE TABLE ice.ns.big (id INT, v DOUBLE) USING iceberg").collect()
    spark.sql("INSERT INTO ice.ns.big VALUES (1, CAST('1e10' AS DOUBLE))").collect()
    spark.sql("CREATE TABLE ice.ns.over (id INT, v DECIMAL(10,2)) USING iceberg").collect()
    with pytest.raises(Exception, match="too large to store"):
        spark.sql("INSERT INTO ice.ns.over SELECT id, v FROM ice.ns.big").collect()
    assert spark.sql("SELECT count(*) FROM ice.ns.over").collect()[0][0] == 0

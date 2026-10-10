"""ICE-TSNS-NARROW-REFUSE-1: the write-door matrix shared by the recorder and the pins.

A cell writes four moments with non-zero digits below the microsecond through one door, as one
spelling of the value, from one nanosecond source type, into one target column type, in one
session zone. What the door stored is read from every Parquet file under the table directory, so
a branch, a staged snapshot and a replaced file are seen as well as the live table.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from repark import ReparkSession, functions

ZONES = ("UTC", "America/New_York", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Lord_Howe")
MOMENTS = (
    "2026-01-02 03:04:05.123456789",
    "2026-03-08 02:30:00.000000001",
    "1969-12-31 23:59:59.999999999",
    "2026-10-04 02:15:00.000000001",
)
EPOCH = datetime(1970, 1, 1)
IDS = tuple(range(1, len(MOMENTS) + 1))
SRC = "ice.ns.src"
SOURCES = {"ns": "timestamp_ns", "tzns": "timestamptz_ns"}
TARGETS = {
    "ts_ns": "timestamp_ns",
    "tz_ns": "timestamptz_ns",
    "ltz": "TIMESTAMP",
    "ntz": "TIMESTAMP_NTZ",
}
NANOSECOND_TARGETS = ("ts_ns", "tz_ns")
FAMILY = {
    "coalesce": "coalesce({c}, NULL)",
    "nvl": "nvl({c}, NULL)",
    "case": "CASE WHEN {i} > 0 THEN {c} ELSE NULL END",
    "if": "if({i} > 0, {c}, NULL)",
    "elt": "array({c}, NULL)[0]",
}
FAMILY_MORE = {
    "case_null_first": "CASE WHEN {i} < 0 THEN NULL ELSE {c} END",
    "coalesce_null_first": "coalesce(NULL, {c})",
    "ifnull": "ifnull({c}, NULL)",
    "nvl2": "nvl2({i}, {c}, NULL)",
    "element_at": "element_at(array(NULL, {c}), 2)",
    "recast_of_coalesce": "CAST(coalesce({c}, NULL) AS {n})",
    "nested_case": "CASE WHEN {i} > 0 THEN coalesce({c}, NULL) ELSE CAST(NULL AS {n}) END",
    "lambda": "transform(array({c}), x -> coalesce(x, NULL))[0]",
    "greatest": "greatest({c}, NULL)",
    "nullif": "nullif({c}, NULL)",
    "struct_field": "named_struct('f', coalesce({c}, NULL)).f",
    "case_three": (
        "CASE WHEN {i} = 1 THEN {c} WHEN {i} = 2 THEN TIMESTAMP '2026-01-02 03:04:05' ELSE NULL END"
    ),
}
WRITTEN = {
    "cast_ts": "CAST({c} AS TIMESTAMP)",
    "try_cast_ts": "TRY_CAST({c} AS TIMESTAMP)",
    "cast_ntz": "CAST({c} AS TIMESTAMP_NTZ)",
    "date_trunc": "date_trunc('second', {c})",
    "case_of_cast": "CASE WHEN {i} > 0 THEN CAST({c} AS TIMESTAMP) ELSE NULL END",
    "coalesce_of_cast": "coalesce(CAST({c} AS TIMESTAMP), NULL)",
    "if_of_trunc": "if({i} > 0, date_trunc('second', {c}), NULL)",
    "cast_date": "CAST({c} AS DATE)",
}
BESIDE_VALUE = {
    "typed_ts_null": "coalesce({c}, CAST(NULL AS TIMESTAMP))",
    "typed_ntz_null": "coalesce({c}, CAST(NULL AS TIMESTAMP_NTZ))",
    "beside_literal": "CASE WHEN {i} = 1 THEN {c} ELSE TIMESTAMP '2026-01-02 03:04:05' END",
    "beside_value": "coalesce({c}, CAST({c} AS TIMESTAMP))",
    "greatest_literal": "greatest({c}, TIMESTAMP '2026-01-02 03:04:05')",
    "array_literal": "array({c}, TIMESTAMP '2026-01-02 03:04:05')[0]",
}
WRITTEN_OVER = {
    "trunc_of_coalesce": "date_trunc('second', coalesce({c}, NULL))",
    "cast_ts_of_coalesce": "CAST(coalesce({c}, NULL) AS TIMESTAMP)",
    "cast_date_of_coalesce": "CAST(coalesce({c}, NULL) AS DATE)",
}
WRITTEN_TWIN = {
    "trunc_of_coalesce": "date_trunc",
    "cast_ts_of_coalesce": "cast_ts",
    "cast_date_of_coalesce": "cast_date",
}
EQUAL_OVER = {
    "trunc_of_coalesce": ("tzns",),
    "cast_ts_of_coalesce": ("ns", "tzns"),
    "cast_date_of_coalesce": ("ns",),
}
KEPT = {
    "plain": "{c}",
    "typed_null": "coalesce({c}, CAST(NULL AS {n}))",
    "case_typed_null": "CASE WHEN {i} > 0 THEN {c} ELSE CAST(NULL AS {n}) END",
    "case_no_else": "CASE WHEN {i} > 0 THEN {c} END",
    "condition_only": (
        "CASE WHEN coalesce({c}, NULL) IS NOT NULL THEN {c} ELSE CAST(NULL AS {n}) END"
    ),
    "struct_null_sibling": "named_struct('f', {c}, 'g', NULL).f",
    "lambda_plain": "transform(array({c}), x -> x)[0]",
}
SPELLINGS = {**FAMILY, **FAMILY_MORE, **BESIDE_VALUE, **WRITTEN_OVER, **WRITTEN, **KEPT}
GROUPS = {
    "family": {**FAMILY, **FAMILY_MORE},
    "beside value": BESIDE_VALUE,
    "written over": WRITTEN_OVER,
    "written": WRITTEN,
    "kept": KEPT,
}
MOR = (
    ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read'"
)
SQL_DOORS = (
    "insert_values",
    "insert_select",
    "insert_positional",
    "insert_by_name",
    "insert_overwrite",
    "insert_overwrite_dynamic",
    "insert_overwrite_static",
    "insert_replace_where",
    "merge_insert",
    "merge_insert_subquery",
    "merge_update",
    "merge_update_subquery",
    "merge_by_source",
    "update_where",
    "update_nowhere",
)
ROW_LEVEL_DOORS = (
    "merge_insert",
    "merge_update",
    "merge_update_subquery",
    "merge_by_source",
    "update_where",
    "update_nowhere",
)
FRAME_DOORS = (
    "df_append",
    "df_overwrite",
    "df_overwrite_partitions",
    "df_create_or_replace",
    "df_save_append",
    "df_save_overwrite",
    "df_insert_into",
    "df_insert_into_overwrite",
    "df_select_expr_append",
    "df_functions_append",
    "df_merge_into",
)
CARRIERS = (
    "branch_insert",
    "branch_merge",
    "branch_update",
    "wap_insert",
    "explain_insert",
    "explain_analyze_insert",
    "explain_analyze_update",
    "prepare_execute_insert",
    "ctas",
    "rtas",
    "cte_insert",
    "subquery_insert",
    "union_insert",
    "join_insert",
    "aggregate_insert",
    "window_insert",
    "distinct_insert",
    "scalar_subquery_insert",
    "frame_view_insert",
    "sql_view_insert",
    "catalog_view_insert",
    "frame_view_merge",
    "cached_frame_append",
)
HUNTED = (
    "merge_update_star",
    "merge_insert_star",
    "update_scalar_subquery",
    "insert_reordered",
    "sort_limit_insert",
    "union_null_insert",
    "df_union_null_append",
    "lambda_first_insert",
    "df_lambda_below_append",
)
UNION_NULL = ("union_null_insert", "df_union_null_append")
CREATES = ("df_create_or_replace", "df_save_overwrite", "ctas", "rtas")
DOORS = (
    *SQL_DOORS,
    *(f"{door}_mor" for door in ROW_LEVEL_DOORS),
    *FRAME_DOORS,
    *CARRIERS,
    *HUNTED,
)
CORE_DOORS = (
    "insert_select",
    "insert_by_name",
    "insert_overwrite",
    "insert_replace_where",
    "merge_insert_subquery",
    "merge_update_subquery",
    "merge_update",
    "update_where",
    "update_nowhere",
    "df_append",
    "df_overwrite_partitions",
    "df_insert_into_overwrite",
    "df_save_append",
)
PARTITIONED = ("insert_overwrite_dynamic", "insert_overwrite_static")
REFUSAL = "narrowed from nanoseconds to microseconds"
REFUSAL_HEAD = re.compile(r'Cannot safely cast `v` "TIMESTAMP" to "TIMESTAMP(TZ)?_NS"')
NANOSECOND_SCALE = 10**17
_NOISE = re.compile(
    r"__repark_[a-z_]+_[0-9a-f]{16,}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"|/[^\s'\"]*tmp[^\s'\"]*|\bx\d+\b"
)


_SESSION_NAME = re.compile(r"__repark_tt_\d+|\b[pv]_x\d+\b|<X>|Column: \d+")
COMPARED = 200


def table_number(source: str, spelling: str, door: str) -> int:
    """Return the number of the cell's table, the same in every run of a shard."""
    row = list(SOURCES).index(source) * len(SPELLINGS) + list(SPELLINGS).index(spelling)
    return row * len(DOORS) + DOORS.index(door) + 1


def comparable(cell: dict[str, Any]) -> dict[str, Any]:
    """Return ``cell`` without what differs between two runs of one statement."""
    kept = {name: item for name, item in cell.items() if name != "refused"}
    if "error" in kept:
        kept["error"] = _SESSION_NAME.sub("<N>", kept["error"])[:COMPARED]
    return kept


def nanos_of(text: str) -> int:
    """Return the int64 nanoseconds of a nine-digit wall ``text`` read as UTC."""
    whole, fraction = text.split(".")
    seconds = (datetime.fromisoformat(whole) - EPOCH) // timedelta(seconds=1)
    return seconds * 1_000_000_000 + int(fraction)


NANOS = tuple(nanos_of(text) for text in MOMENTS)


def literal(source: str, index: int) -> str:
    """Return the SQL literal of moment ``index`` typed as ``source``."""
    if source == "ns":
        return f"CAST('{MOMENTS[index]}' AS timestamp_ns)"
    return f"CAST('{MOMENTS[index]}+00:00' AS timestamptz_ns)"


def value(spelling: str, source: str, column: str, row: str) -> str:
    """Return ``spelling`` over ``column``, with ``row`` as the row's id."""
    return SPELLINGS[spelling].format(c=column, i=row, n=SOURCES[source])


def frame_of(spark: Any, table: pa.Table) -> Any:
    """Return a DataFrame over ``table`` that keeps every Arrow type."""
    return spark.createDataFrame(table.to_pandas(types_mapper=pd.ArrowDtype))


def open_session(zone: str, warehouse: Path) -> Any:
    """Open a v3-enabled session in ``zone`` with the source table written."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-narrow-refuse-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        f"CREATE TABLE {SRC} (id INT, ns timestamp_ns, tzns timestamptz_ns) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    arrays = {
        "id": pa.array(list(IDS), pa.int32()),
        "ns": pa.array(list(NANOS), pa.timestamp("ns")),
        "tzns": pa.array(list(NANOS), pa.timestamp("ns", tz="UTC")),
    }
    frame_of(spark, pa.table(arrays)).writeTo(SRC).append()
    return spark


def create_target(spark: Any, table: str, target: str, source: str, door: str) -> None:
    """Create ``table`` as ``(id INT, v <target>, k INT, c <source>)`` for ``door``."""
    layout = "PARTITIONED BY (k) " if door in PARTITIONED else ""
    mode = MOR if door.endswith("_mor") else ""
    spark.sql(
        f"CREATE TABLE {table} (id INT, v {TARGETS[target]}, k INT, c {SOURCES[source]}) "
        f"USING iceberg {layout}TBLPROPERTIES ('format-version' = '3', "
        f"'write.wap.enabled' = 'true'{mode})"
    )


def seed(spark: Any, table: str, source: str = "") -> None:
    """Write one row per moment with a NULL ``v``; ``c`` holds ``source`` when it is named."""
    if source:
        spark.sql(f"INSERT INTO {table} (id, k, c) SELECT id, 0, {source} FROM {SRC}")
    else:
        spark.sql(f"INSERT INTO {table} (id, k) SELECT id, 0 FROM {SRC}")


def stored(warehouse: Path, table: str) -> list[list[Any]]:
    """Return every non-null ``[id, v]`` pair of every Parquet file under ``table``."""
    directory = warehouse / "ns" / table.rsplit(".", 1)[-1]
    pairs: list[list[Any]] = []
    for path in sorted(directory.rglob("*.parquet")):
        file = pq.ParquetFile(path)
        if "id" not in file.schema_arrow.names or "v" not in file.schema_arrow.names:
            continue
        rows = file.read(columns=["id", "v"])
        column = rows.column("v")
        if pa.types.is_timestamp(column.type):
            ticks = column.cast(pa.int64()).to_pylist()
        else:
            ticks = [None if item is None else str(item) for item in column.to_pylist()]
        for row, tick in zip(rows.column("id").to_pylist(), ticks, strict=True):
            if tick is not None:
                pairs.append([row, tick])
    return sorted(pairs, key=repr)


def refusal_text(error: Exception) -> str:
    """Return the last line of ``error`` with run-specific names removed."""
    lines = [line for line in str(error).splitlines() if line.strip()]
    return _NOISE.sub("<X>", lines[-1] if lines else "")[:240]


class Cell:
    """One cell of the matrix: the statement pieces of a spelling, a source and a target."""

    def __init__(self, spark: Any, table: str, spelling: str, source: str) -> None:
        self.spark = spark
        self.table = table
        self.spelling = spelling
        self.source = source
        self.null = f"CAST(NULL AS {SOURCES[source]})"

    def over(self, column: str, row: str) -> str:
        """Return the spelling over ``column`` and ``row``."""
        return value(self.spelling, self.source, column, row)

    def select(self) -> str:
        """Return the query that yields ``id``, the value as ``v`` and ``k``."""
        return f"SELECT id, {self.over(self.source, 'id')} AS v, 0 AS k FROM {SRC}"

    def whole(self) -> str:
        """Return the query that yields all four target columns."""
        value_sql = self.over(self.source, "id")
        return f"SELECT id, {value_sql} AS v, 0 AS k, {self.null} AS c FROM {SRC}"

    def sql(self, statement: str) -> None:
        """Run one statement and drain it."""
        self.spark.sql(statement).toArrow()

    def frame(self) -> Any:
        """Return the DataFrame of :meth:`whole`."""
        return self.spark.sql(self.whole())


def run_sql_door(cell: Cell, door: str) -> None:
    """Run one SQL write door of ``cell``."""
    table, source = cell.table, cell.source
    using = f"MERGE INTO {table} t USING {SRC} s ON t.id = s.id"
    nested = f"MERGE INTO {table} t USING ({cell.select()}) s ON t.id = s.id"
    if door == "insert_values":
        rows = ", ".join(
            f"({index + 1}, {cell.over(literal(source, index), str(index + 1))}, 0)"
            for index in range(len(IDS))
        )
        cell.sql(f"INSERT INTO {table} (id, v, k) VALUES {rows}")
    elif door == "insert_select":
        cell.sql(f"INSERT INTO {table} (id, v, k) {cell.select()}")
    elif door == "insert_positional":
        cell.sql(f"INSERT INTO {table} {cell.whole()}")
    elif door == "insert_by_name":
        by_name = f"SELECT 0 AS k, {cell.over(source, 'id')} AS v, id FROM {SRC}"
        cell.sql(f"INSERT INTO {table} BY NAME {by_name}")
    elif door in ("insert_overwrite", "insert_overwrite_dynamic"):
        seed(cell.spark, table)
        cell.sql(f"INSERT OVERWRITE {table} {cell.whole()}")
    elif door == "insert_overwrite_static":
        seed(cell.spark, table)
        static = f"SELECT id, {cell.over(source, 'id')} AS v, {cell.null} AS c FROM {SRC}"
        cell.sql(f"INSERT OVERWRITE {table} PARTITION (k = 0) {static}")
    elif door == "insert_replace_where":
        seed(cell.spark, table)
        cell.sql(f"INSERT INTO {table} REPLACE WHERE id >= 0 {cell.whole()}")
    elif door == "merge_insert":
        inserted = cell.over(f"s.{source}", "s.id")
        cell.sql(f"{using} WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {inserted}, 0)")
    elif door == "merge_insert_subquery":
        cell.sql(f"{nested} WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s.v, 0)")
    elif door == "merge_update":
        seed(cell.spark, table)
        assigned = cell.over(f"s.{source}", "s.id")
        cell.sql(f"{using} WHEN MATCHED THEN UPDATE SET v = {assigned}")
    elif door == "merge_update_subquery":
        seed(cell.spark, table)
        cell.sql(f"{nested} WHEN MATCHED THEN UPDATE SET v = s.v")
    elif door == "merge_by_source":
        seed(cell.spark, table, source)
        cell.sql(
            f"MERGE INTO {table} t USING (SELECT -1 AS id) s ON t.id = s.id "
            f"WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = {cell.over('t.c', 't.id')}"
        )
    elif door == "update_where":
        seed(cell.spark, table, source)
        cell.sql(f"UPDATE {table} SET v = {cell.over('c', 'id')} WHERE id >= 0")
    else:
        seed(cell.spark, table, source)
        cell.sql(f"UPDATE {table} SET v = {cell.over('c', 'id')}")


def functions_frame(cell: Cell) -> Any:
    """Return the DataFrame built with the functions API, or None when it has no spelling."""
    column = functions.col(cell.source)
    spellings = {
        "coalesce": functions.coalesce(column, functions.lit(None)),
        "case": functions.when(functions.col("id") > 0, column).otherwise(functions.lit(None)),
        "case_no_else": functions.when(functions.col("id") > 0, column),
        "cast_ts": column.cast("timestamp"),
        "plain": column,
    }
    if cell.spelling not in spellings:
        return None
    return cell.spark.table(SRC).select(
        functions.col("id"),
        spellings[cell.spelling].alias("v"),
        functions.lit(0).alias("k"),
    )


def run_frame_door(cell: Cell, door: str) -> None:
    """Run one DataFrame write door of ``cell``."""
    table, spark = cell.table, cell.spark
    if door == "df_append":
        cell.frame().writeTo(table).append()
    elif door == "df_overwrite":
        seed(spark, table)
        cell.frame().writeTo(table).overwrite(functions.col("id") >= 0)
    elif door == "df_overwrite_partitions":
        seed(spark, table)
        cell.frame().writeTo(table).overwritePartitions()
    elif door == "df_create_or_replace":
        cell.frame().writeTo(table).using("iceberg").createOrReplace()
    elif door == "df_save_append":
        cell.frame().write.mode("append").saveAsTable(table)
    elif door == "df_save_overwrite":
        cell.frame().write.mode("overwrite").saveAsTable(table)
    elif door == "df_insert_into":
        cell.frame().write.insertInto(table)
    elif door == "df_insert_into_overwrite":
        seed(spark, table)
        cell.frame().write.insertInto(table, overwrite=True)
    elif door == "df_select_expr_append":
        value_sql = cell.over(cell.source, "id")
        frame = spark.table(SRC).selectExpr("id", f"{value_sql} AS v", "0 AS k")
        frame.writeTo(table).append()
    elif door == "df_functions_append":
        frame = functions_frame(cell)
        if frame is None:
            raise LookupError("no functions spelling")
        frame.writeTo(table).append()
    else:
        seed(spark, table)
        cell.frame().mergeInto(table, "id").whenMatched().updateAll().merge()


def run_carrier(cell: Cell, door: str) -> None:
    """Run one statement that carries or defers a write of ``cell``."""
    table, spark, source = cell.table, cell.spark, cell.source
    select = cell.select()
    insert = f"INSERT INTO {table} (id, v, k)"
    value_sql = cell.over(source, "id")
    view = "v_" + table.rsplit(".", 1)[-1]
    if door in ("branch_insert", "branch_merge", "branch_update"):
        seed(spark, table, source)
        cell.sql(f"ALTER TABLE {table} CREATE BRANCH b1")
        branch = f"{table}.branch_b1"
        if door == "branch_insert":
            cell.sql(f"INSERT INTO {branch} (id, v, k) {select}")
        elif door == "branch_merge":
            cell.sql(
                f"MERGE INTO {branch} t USING ({select}) s ON t.id = s.id "
                "WHEN MATCHED THEN UPDATE SET v = s.v"
            )
        else:
            cell.sql(f"UPDATE {branch} SET v = {cell.over('c', 'id')} WHERE id >= 0")
    elif door == "wap_insert":
        spark.conf.set("spark.wap.id", "w1")
        try:
            cell.sql(f"{insert} {select}")
        finally:
            spark.conf.unset("spark.wap.id")
    elif door == "explain_insert":
        cell.sql(f"EXPLAIN INSERT INTO {table} {cell.whole()}")
    elif door == "explain_analyze_insert":
        cell.sql(f"EXPLAIN ANALYZE INSERT INTO {table} {cell.whole()}")
    elif door == "explain_analyze_update":
        seed(spark, table, source)
        cell.sql(f"EXPLAIN ANALYZE UPDATE {table} SET v = {cell.over('c', 'id')} WHERE id >= 0")
    elif door == "prepare_execute_insert":
        cell.sql(f"PREPARE p_{view} AS INSERT INTO {table} {cell.whole()}")
        cell.sql(f"EXECUTE p_{view}")
    elif door in ("ctas", "rtas"):
        cell.sql(f"DROP TABLE {table}")
        head = "CREATE OR REPLACE TABLE" if door == "rtas" else "CREATE TABLE"
        cell.sql(f"{head} {table} USING iceberg TBLPROPERTIES ('format-version' = '3') AS {select}")
    elif door == "cte_insert":
        cell.sql(f"INSERT INTO {table} WITH q AS ({cell.whole()}) SELECT id, v, k, c FROM q")
    elif door == "subquery_insert":
        cell.sql(f"{insert} SELECT a.id, a.v, a.k FROM (SELECT b.* FROM ({select}) b) a")
    elif door == "union_insert":
        plain = f"SELECT id, {source} AS v, 0 AS k FROM {SRC} WHERE id = 1"
        cell.sql(f"{insert} {plain} UNION ALL {select} WHERE id > 1")
    elif door == "join_insert":
        cell.sql(f"{insert} SELECT l.id, r.v, 0 FROM {SRC} l JOIN ({select}) r ON l.id = r.id")
    elif door == "aggregate_insert":
        cell.sql(f"{insert} SELECT id, max({value_sql}), 0 FROM {SRC} GROUP BY id")
    elif door == "window_insert":
        windowed = f"first_value({value_sql}) OVER (PARTITION BY id ORDER BY id)"
        cell.sql(f"{insert} SELECT id, {windowed}, 0 FROM {SRC}")
    elif door == "distinct_insert":
        cell.sql(f"{insert} SELECT DISTINCT id, v, k FROM ({select} UNION ALL {select})")
    elif door == "scalar_subquery_insert":
        scalar = f"(SELECT max({cell.over('i.' + source, 'i.id')}) FROM {SRC} i WHERE i.id = o.id)"
        cell.sql(f"{insert} SELECT o.id, {scalar}, 0 FROM {SRC} o")
    elif door == "frame_view_insert":
        spark.sql(select).createOrReplaceTempView(view)
        cell.sql(f"{insert} SELECT id, v, k FROM {view}")
    elif door == "sql_view_insert":
        cell.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS {select}")
        cell.sql(f"{insert} SELECT id, v, k FROM {view}")
    elif door == "catalog_view_insert":
        cell.sql(f"CREATE VIEW ice.ns.{view} AS {select}")
        cell.sql(f"{insert} SELECT id, v, k FROM ice.ns.{view}")
    elif door == "frame_view_merge":
        seed(spark, table)
        spark.sql(select).createOrReplaceTempView(view)
        cell.sql(
            f"MERGE INTO {table} t USING {view} s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v"
        )
    else:
        cell.frame().cache().writeTo(table).append()


def run_hunted(cell: Cell, door: str) -> None:
    """Run one statement shape found while hunting for a door the refusal missed."""
    table, spark, source = cell.table, cell.spark, cell.source
    value_sql = cell.over(source, "id")
    if door == "merge_update_star":
        seed(spark, table)
        cell.sql(
            f"MERGE INTO {table} t USING ({cell.whole()}) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET *"
        )
    elif door == "merge_insert_star":
        cell.sql(
            f"MERGE INTO {table} t USING ({cell.whole()}) s ON t.id = s.id "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    elif door == "update_scalar_subquery":
        seed(spark, table, source)
        scalar = f"(SELECT max({cell.over('i.' + source, 'i.id')}) FROM {SRC} i)"
        cell.sql(f"UPDATE {table} SET v = {scalar} WHERE id >= 0")
    elif door == "insert_reordered":
        cell.sql(f"INSERT INTO {table} (k, v, id) SELECT 0, {value_sql}, id FROM {SRC}")
    elif door == "sort_limit_insert":
        cell.sql(f"INSERT INTO {table} (id, v, k) {cell.select()} ORDER BY id LIMIT 3")
    elif door == "lambda_first_insert":
        beside = "size(transform(array(id), x -> x))"
        cell.sql(f"INSERT INTO {table} (k, v, id) SELECT {beside}, {value_sql}, id FROM {SRC}")
    elif door == "df_lambda_below_append":
        below = spark.sql(f"SELECT id, ns, tzns, transform(array(id), x -> x) AS a FROM {SRC}")
        below.selectExpr("id", f"{value_sql} AS v", "0 AS k").writeTo(table).append()
    elif door == "union_null_insert":
        cell.sql(
            f"INSERT INTO {table} (id, v, k) {cell.select()} WHERE id > 1 "
            "UNION ALL SELECT 1, NULL, 0"
        )
    else:
        absent = spark.sql("SELECT 9 AS id, NULL AS v, 0 AS k")
        spark.sql(cell.select()).unionByName(absent).writeTo(table).append()


def measure(spark: Any, warehouse: Path, table: str, key: tuple[str, str, str, str]) -> dict:
    """Run the cell ``(target, source, spelling, door)`` and return what it stored."""
    target, source, spelling, door = key
    cell = Cell(spark, table, spelling, source)
    plain = door.removesuffix("_mor")
    result: dict[str, Any] = {}
    try:
        create_target(spark, table, target, source, door)
        if plain in SQL_DOORS:
            run_sql_door(cell, plain)
        elif door in FRAME_DOORS:
            run_frame_door(cell, door)
        elif door in HUNTED:
            run_hunted(cell, door)
        else:
            run_carrier(cell, door)
    except LookupError:
        return {"skip": True}
    except Exception as error:
        result["error"] = refusal_text(error)
        if REFUSAL in str(error):
            result["refused"] = True
    result["stored"] = stored(warehouse, table)
    return result

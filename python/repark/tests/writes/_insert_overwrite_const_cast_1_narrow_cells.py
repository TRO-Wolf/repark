from __future__ import annotations

import gc
import hashlib
import re
from typing import Any, NamedTuple

TYPES: dict[str, str] = {
    "DOUBLE": "DOUBLE",
    "FLOAT": "FLOAT",
    "INT": "INT",
    "BIGINT": "BIGINT",
    "TIMESTAMP": "TIMESTAMP",
    "NTZ": "TIMESTAMP_NTZ",
    "DATE": "DATE",
    "DECIMAL": "DECIMAL(10,2)",
}

SEEDS: dict[str, str] = {
    "DOUBLE": "CAST(1.5 AS DOUBLE)",
    "FLOAT": "CAST(1.5 AS FLOAT)",
    "INT": "CAST(7 AS INT)",
    "BIGINT": "CAST(7 AS BIGINT)",
    "TIMESTAMP": "TIMESTAMP'2024-01-02 03:04:05'",
    "NTZ": "TIMESTAMP_NTZ'2024-01-02 03:04:05'",
    "DATE": "DATE'2024-01-02'",
    "DECIMAL": "CAST(1.25 AS DECIMAL(10,2))",
}

PARTITIONED_DOORS: frozenset[str] = frozenset(
    {"sql_static_part", "sql_dynamic_part", "sql_dynamic_mode", "v2_overwrite_partitions"}
)

CARD = "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE 1 = 0"

IN_SCOPE_SHAPES: dict[str, str] = {
    "fromless_literal": "SELECT CAST('{V}' AS {T}) WHERE 1 = 0",
    "subquery": CARD,
    "nested_subquery": (
        "SELECT CAST(s AS {T}) FROM (SELECT s FROM (SELECT '{V}' AS s) a) b WHERE 1 = 0"
    ),
    "inner_cast_projected": (
        "SELECT d FROM (SELECT CAST(s AS {T}) AS d FROM (SELECT '{V}' AS s) a) b WHERE 1 = 0"
    ),
    "inner_filter": "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s WHERE 1 = 0) src",
    "cte": "WITH c AS (SELECT '{V}' AS s) SELECT CAST(s AS {T}) FROM c WHERE 1 = 0",
    "renamed_alias": (
        "SELECT CAST(r AS {T}) AS y FROM (SELECT s AS r FROM (SELECT '{V}' AS s) a) b WHERE 1 = 0"
    ),
    "where_false": "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE false",
    "two_filters": "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE 1 = 0 AND 2 = 2",
}

OUT_OF_SCOPE_SHAPES: dict[str, str] = {
    "values_column": "SELECT CAST(s AS {T}) FROM VALUES ('{V}') AS v(s) WHERE 1 = 0",
    "values_leaf_literal": "SELECT CAST('{V}' AS {T}) FROM VALUES (1) AS v(a) WHERE 1 = 0",
    "limit_zero": "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src LIMIT 0",
    "union": (
        "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s UNION ALL SELECT 'y' AS s) src WHERE 1 = 0"
    ),
    "join": (
        "SELECT CAST(a.s AS {T}) FROM (SELECT '{V}' AS s) a JOIN (SELECT 1 AS k) b ON true "
        "WHERE 1 = 0"
    ),
    "aggregate": "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src GROUP BY s HAVING 1 = 0",
    "case_dead": (
        "SELECT CASE WHEN false THEN CAST(s AS {T}) END FROM (SELECT '{V}' AS s) src WHERE 1 = 0"
    ),
    "try_cast": "SELECT TRY_CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE 1 = 0",
    "exists": (
        "SELECT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE 1 = 0 AND EXISTS (SELECT 1)"
    ),
    "distinct": "SELECT DISTINCT CAST(s AS {T}) FROM (SELECT '{V}' AS s) src WHERE 1 = 0",
    "temp_view": "SELECT CAST(s AS {T}) FROM tv_x WHERE 1 = 0",
    "real_table": "SELECT CAST(s AS {T}) FROM {SRC} WHERE 1 = 0",
}

ATTACKS: dict[str, tuple[str, str]] = {
    "left_join_union_filtered": (
        "DOUBLE",
        "SELECT CAST(r.s AS DOUBLE) FROM {L} l LEFT JOIN ((SELECT 1 AS id, 'x' AS s WHERE 1 = 0) "
        "UNION ALL (SELECT 2 AS id, '1.5' AS s)) r ON l.id = r.id",
    ),
    "left_join_offset": (
        "DOUBLE",
        "SELECT CAST(r.s AS DOUBLE) FROM {L} l LEFT JOIN "
        "(SELECT 1 AS id, 'x' AS s LIMIT 5 OFFSET 1) r ON l.id = r.id",
    ),
    "cte_left_join_empty": (
        "TIMESTAMP",
        "WITH j AS (SELECT r.s FROM (SELECT 1 AS id) l LEFT JOIN (SELECT 1 AS id, 'x' AS s) r "
        "ON l.id = r.id) SELECT CAST(s AS TIMESTAMP) FROM j WHERE 1 = 0",
    ),
    "seven_digit_year": ("DATE", "SELECT CAST(s AS DATE) FROM (SELECT '+1000000-01-01' AS s) src"),
    "seven_digit_year_empty": (
        "DATE",
        "SELECT CAST(s AS DATE) FROM (SELECT '+1000000-01-01' AS s) src WHERE 1 = 0",
    ),
    "systemv_zone_empty": (
        "TIMESTAMP",
        "SELECT CAST(s AS TIMESTAMP) FROM (SELECT '2024-01-01 00:00:00 SystemV/EST5' AS s) src "
        "WHERE 1 = 0",
    ),
    "del_trimmed_int": ("INT", "SELECT CAST(s AS INT) FROM (SELECT '\x7f42\x7f' AS s) src"),
    "systemv_zone": (
        "TIMESTAMP",
        "SELECT CAST(s AS TIMESTAMP) FROM (SELECT '2024-01-01 00:00:00 SystemV/EST5' AS s) src",
    ),
    "exists_select_list": (
        "INT",
        "SELECT 1 WHERE EXISTS (SELECT CAST(s AS TIMESTAMP) FROM (SELECT 'x' AS s) q)",
    ),
    "window_unprojected": (
        "INT",
        "SELECT a FROM (SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS n FROM "
        "(SELECT 1 AS a, CAST(s AS TIMESTAMP) AS unused FROM (SELECT 'x' AS s) q) u) w",
    ),
    "ntz_partial_date": (
        "NTZ",
        "SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT '2024-1-1' AS s) src WHERE 1 = 0",
    ),
    "ntz_year_only": (
        "NTZ",
        "SELECT CAST(s AS TIMESTAMP_NTZ) FROM (SELECT '2024' AS s) src WHERE 1 = 0",
    ),
    "hex_double": (
        "DOUBLE",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT '0x1.0p0' AS s) src WHERE 1 = 0",
    ),
    "nan_double": ("DOUBLE", "SELECT CAST(s AS DOUBLE) FROM (SELECT 'NaN' AS s) src WHERE 1 = 0"),
    "infinity_float": (
        "FLOAT",
        "SELECT CAST(s AS FLOAT) FROM (SELECT 'Infinity' AS s) src WHERE 1 = 0",
    ),
    "suffixed_nan_double": (
        "DOUBLE",
        "SELECT CAST(s AS DOUBLE) FROM (SELECT 'nand' AS s) src WHERE 1 = 0",
    ),
    "blank_double": ("DOUBLE", "SELECT CAST(s AS DOUBLE) FROM (SELECT ' ' AS s) src WHERE 1 = 0"),
    "padded_letters_ts": (
        "TIMESTAMP",
        "SELECT CAST(s AS TIMESTAMP) FROM (SELECT ' x ' AS s) src WHERE 1 = 0",
    ),
    "decimal_from_double": (
        "DECIMAL",
        "SELECT CAST(d AS DECIMAL(10,2)) FROM (SELECT 1e20 AS d) src WHERE 1 = 0",
    ),
    "date_letters": ("DATE", "SELECT CAST(s AS DATE) FROM (SELECT 'x' AS s) src WHERE 1 = 0"),
}

CARD_DOORS: tuple[str, ...] = (
    "sql_overwrite",
    "sql_overwrite_table",
    "sql_static_part",
    "sql_dynamic_part",
    "sql_dynamic_mode",
    "v2_overwrite_condition",
    "v2_overwrite_partitions",
    "v1_insert_into_overwrite",
    "v1_save_as_table_overwrite",
)

CACHE_DOORS: tuple[str, ...] = (
    "sql_overwrite",
    "sql_static_part",
    "v1_insert_into_overwrite",
    "v2_overwrite_condition",
    "v1_save_as_table_overwrite",
)

CACHE_SOURCES: dict[str, str] = {
    "matching": "SELECT 'x' AS s",
    "unpersisted": "SELECT 'x' AS s",
    "unrelated": "SELECT 7 AS n",
    "collected": "SELECT 'x' AS s",
}

SIBLING_MODES: tuple[str, ...] = (
    "cache_count",
    "cache_nocount",
    "persist",
    "cacheTable",
    "sql_cache",
    "unpersist",
    "uncacheTable",
    "uncache_sql",
    "clear_sibling",
    "clear_main",
    "frame_collected",
    "session_collected",
)

SIBLING_RELEASED: frozenset[str] = frozenset(
    {"unpersist", "uncacheTable", "uncache_sql", "clear_sibling"}
)

CONTROL_DOORS: tuple[str, ...] = (
    "ctl_insert_into",
    "ctl_sql_rtas",
    "ctl_v2_append",
    "ctl_v2_create_or_replace",
    "ctl_read",
)


class Cell(NamedTuple):
    ansi: str
    door: str
    type_key: str
    source: str


def build_cells() -> dict[str, Cell]:
    cells: dict[str, Cell] = {}
    for ansi in ("true", "false"):
        for type_key in ("DOUBLE", "TIMESTAMP"):
            source = CARD.format(T=TYPES[type_key], V="x")
            cells[f"card/{ansi}/{type_key}"] = Cell(ansi, "sql_overwrite", type_key, source)
        for door in CARD_DOORS + CONTROL_DOORS:
            for type_key in ("DOUBLE", "TIMESTAMP", "NTZ", "INT"):
                source = CARD.format(T=TYPES[type_key], V="x")
                cells[f"door/{ansi}/{door}/{type_key}"] = Cell(ansi, door, type_key, source)
    for name, template in IN_SCOPE_SHAPES.items():
        for type_key in ("DOUBLE", "FLOAT", "TIMESTAMP", "NTZ", "BIGINT"):
            source = template.format(T=TYPES[type_key], V="abc")
            cells[f"shape/{name}/{type_key}"] = Cell("true", "sql_overwrite", type_key, source)
    for name, template in OUT_OF_SCOPE_SHAPES.items():
        for type_key in ("DOUBLE", "TIMESTAMP", "INT"):
            source = template.format(T=TYPES[type_key], V="x", SRC="{SRC}")
            cells[f"outside/{name}/{type_key}"] = Cell("true", "sql_overwrite", type_key, source)
    for mode in CACHE_SOURCES:
        for door in CACHE_DOORS:
            for type_key in ("DOUBLE", "TIMESTAMP"):
                source = CARD.format(T=TYPES[type_key], V="x")
                cells[f"cache/{mode}/{door}/{type_key}"] = Cell("true", door, type_key, source)
    for mode in SIBLING_MODES:
        for door in CACHE_DOORS:
            for type_key in ("DOUBLE", "TIMESTAMP"):
                source = CARD.format(T=TYPES[type_key], V="x")
                cells[f"sibling/{mode}/{door}/{type_key}"] = Cell("true", door, type_key, source)
    for name, (type_key, source) in ATTACKS.items():
        for door in ("sql_overwrite", "sql_static_part", "v1_insert_into_overwrite"):
            cells[f"attack/{name}/{door}"] = Cell("true", door, type_key, source)
    return cells


WHOLE_TABLE_DOORS: frozenset[str] = frozenset(
    {
        "sql_overwrite",
        "sql_overwrite_table",
        "sql_dynamic_mode",
        "v2_overwrite_partitions",
        "v1_insert_into_overwrite",
    }
)

GUARDED_TYPES: frozenset[str] = frozenset({"DOUBLE", "FLOAT", "TIMESTAMP", "NTZ", "INT", "BIGINT"})


def guarded(key: str, cell: Cell) -> bool:
    kind = key.split("/", 1)[0]
    if kind == "cache":
        return key.split("/")[1] == "unpersisted"
    if kind == "sibling":
        return key.split("/")[1] in SIBLING_RELEASED
    if kind not in ("card", "door", "shape") or cell.ansi != "true":
        return False
    if cell.door not in CARD_DOORS or cell.type_key not in GUARDED_TYPES:
        return False
    return not (cell.door in WHOLE_TABLE_DOORS and cell.type_key in ("INT", "BIGINT"))


def error_class(message: str) -> str:
    found = re.search(r"\[([A-Z][A-Z0-9_.]*)\]", message)
    return found.group(1) if found else "UNCLASSIFIED"


def table_rows(spark: Any, table: str, partitioned: bool) -> list[list[str | None]] | None:
    columns = "CAST(x AS STRING), p" if partitioned else "CAST(x AS STRING)"
    try:
        rows = spark.sql(f"SELECT {columns} FROM {table}").collect()
    except Exception:
        return None
    return sorted(([None if v is None else str(v) for v in row] for row in rows), key=repr)


def setup(spark: Any, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}").collect()
    spark.sql(f"DROP TABLE IF EXISTS {namespace}.src_x").collect()
    spark.sql(f"CREATE TABLE {namespace}.src_x (s STRING) USING iceberg").collect()
    spark.sql(f"INSERT INTO {namespace}.src_x VALUES ('x')").collect()
    spark.sql(f"DROP TABLE IF EXISTS {namespace}.left_ids").collect()
    spark.sql(f"CREATE TABLE {namespace}.left_ids (id INT) USING iceberg").collect()
    spark.sql(f"INSERT INTO {namespace}.left_ids VALUES (2)").collect()
    spark.sql("CREATE OR REPLACE TEMP VIEW tv_x AS SELECT 'x' AS s")


def seed(spark: Any, table: str, cell: Cell) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
    column = TYPES[cell.type_key]
    value = SEEDS[cell.type_key]
    if cell.door in PARTITIONED_DOORS:
        spark.sql(
            f"CREATE TABLE {table} (x {column}, p INT) USING iceberg PARTITIONED BY (p)"
        ).collect()
        spark.sql(f"INSERT INTO {table} SELECT {value}, 1").collect()
        spark.sql(f"INSERT INTO {table} SELECT {value}, 2").collect()
    else:
        spark.sql(f"CREATE TABLE {table} (x {column}) USING iceberg").collect()
        spark.sql(f"INSERT INTO {table} SELECT {value}").collect()


def act(spark: Any, functions: Any, cell: Cell, table: str, source: str) -> None:
    door = cell.door
    sql_doors = {
        "sql_overwrite": f"INSERT OVERWRITE {table} {source}",
        "sql_overwrite_table": f"INSERT OVERWRITE TABLE {table} {source}",
        "sql_static_part": f"INSERT OVERWRITE {table} PARTITION (p = 1) {source}",
        "sql_dynamic_part": f"INSERT OVERWRITE {table} PARTITION (p) SELECT *, 1 FROM ({source}) q",
        "ctl_insert_into": f"INSERT INTO {table} {source}",
        "ctl_sql_rtas": f"CREATE OR REPLACE TABLE {table} USING iceberg AS {source}",
        "ctl_read": source,
    }
    if door in sql_doors:
        spark.sql(sql_doors[door]).collect()
    elif door == "sql_dynamic_mode":
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try:
            spark.sql(f"INSERT OVERWRITE {table} SELECT *, 1 FROM ({source}) q").collect()
        finally:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    elif door == "v2_overwrite_condition":
        spark.sql(source).toDF("x").writeTo(table).overwrite(functions.lit(True))
    elif door == "v2_overwrite_partitions":
        frame = spark.sql(f"SELECT *, 1 AS p FROM ({source}) q").toDF("x", "p")
        frame.writeTo(table).overwritePartitions()
    elif door == "v1_insert_into_overwrite":
        spark.sql(source).toDF("x").write.mode("overwrite").insertInto(table)
    elif door == "v1_save_as_table_overwrite":
        spark.sql(source).toDF("x").write.mode("overwrite").saveAsTable(table)
    elif door == "ctl_v2_append":
        spark.sql(source).toDF("x").writeTo(table).append()
    elif door == "ctl_v2_create_or_replace":
        spark.sql(source).toDF("x").writeTo(table).using("iceberg").createOrReplace()
    else:
        raise ValueError(door)


def cached_frame(spark: Any, key: str) -> Any:
    kind, mode = key.split("/")[:2]
    if kind != "cache":
        return None
    frame = spark.sql(CACHE_SOURCES[mode])
    frame.cache()
    frame.count()
    if mode == "unpersisted":
        frame.unpersist()
    return None if mode in ("unpersisted", "collected") else frame


def sibling_cache(spark: Any, key: str) -> tuple[Any, Any] | None:
    kind, mode = key.split("/")[:2]
    if kind != "sibling":
        return None
    sibling = spark.newSession()
    frame = sibling.sql("SELECT 'x' AS s")
    if mode in ("cacheTable", "uncacheTable", "sql_cache", "uncache_sql"):
        frame.createOrReplaceTempView("sibling_v")
        if mode in ("cacheTable", "uncacheTable"):
            sibling.catalog.cacheTable("sibling_v")
        else:
            sibling.sql("CACHE TABLE sibling_v").collect()
    elif mode == "persist":
        frame.persist()
        frame.count()
    else:
        frame.cache()
        if mode != "cache_nocount":
            frame.count()
    release_sibling(spark, sibling, frame, mode)
    if mode == "session_collected":
        return None
    return sibling, None if mode == "frame_collected" else frame


def release_sibling(spark: Any, sibling: Any, frame: Any, mode: str) -> None:
    if mode == "unpersist":
        frame.unpersist()
    elif mode == "uncacheTable":
        sibling.catalog.uncacheTable("sibling_v")
    elif mode == "uncache_sql":
        sibling.sql("UNCACHE TABLE sibling_v").collect()
    elif mode == "clear_sibling":
        sibling.catalog.clearCache()
    elif mode == "clear_main":
        spark.catalog.clearCache()


def run_cell(spark: Any, functions: Any, namespace: str, key: str, cell: Cell) -> dict[str, Any]:
    table = f"{namespace}.t_{hashlib.sha1(key.encode()).hexdigest()[:12]}"
    source = cell.source.format(SRC=f"{namespace}.src_x", L=f"{namespace}.left_ids")
    if cell.door != "ctl_read":
        seed(spark, table, cell)
    spark.conf.set("spark.sql.ansi.enabled", cell.ansi)
    frame = cached_frame(spark, key)
    held = sibling_cache(spark, key)
    if "collected" in key:
        gc.collect()
    try:
        act(spark, functions, cell, table, source)
        outcome: dict[str, Any] = {"ok": True, "cls": None}
    except Exception as error:
        outcome = {"ok": False, "cls": error_class(str(error))}
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        if frame is not None:
            frame.unpersist()
        if held is not None:
            held[0].catalog.clearCache()
        if "collected" in key:
            gc.collect()
            spark.catalog.clearCache()
    partitioned = cell.door in PARTITIONED_DOORS
    outcome["rows"] = None if cell.door == "ctl_read" else table_rows(spark, table, partitioned)
    return outcome

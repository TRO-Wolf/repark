"""Write doors for the ICE-TSNS-COERCION-1 twin matrix.

The door list is the R-017 list. Nothing here calls a store guard. Stored values
are read from every Parquet file under the table directory. ``toArrow`` drains
the statement and is the read path in the recorder.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from _ice_tsns_coercion_1_spellings import (
    CARRIERS,
    COLUMN_SIBLINGS,
    DOORS,
    FORMS,
    FRAME_DOORS,
    HUNTED,
    MOMENTS,
    MOR,
    PARTITIONED,
    SOURCES,
    SQL_DOORS,
    SRC,
    TARGETS,
    literal_arms,
    moment_literal,
    result_zoned,
    sibling_expr,
    union_body,
    value_of,
    wrap_member,
)

from repark import ReparkSession, functions

_TABLES = 0
_NOISE = re.compile(
    r"__repark_[a-z_]+_[0-9a-f]{16,}"
    r"|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"|/(?:[^\s'\"]+)"
)


def fresh_table() -> str:
    """Return a new table name inside ``ice.ns``."""
    global _TABLES
    _TABLES += 1
    return f"ice.ns.t{_TABLES}"


def error_text(error: Exception) -> str:
    """Return the last line of ``error`` with paths and generated names removed."""
    lines = [line for line in str(error).splitlines() if line.strip()]
    text = lines[-1] if lines else ""
    return _NOISE.sub("<X>", text)[:240]


def open_session(zone: str, warehouse: Path) -> Any:
    """Open one v3 session in ``zone`` and write the two source rows."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-coercion-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(
        f"CREATE TABLE {SRC} ("
        "id INT, ns timestamp_ns, tzns timestamptz_ns, ts TIMESTAMP, "
        "ntz TIMESTAMP_NTZ, far TIMESTAMP"
        ") USING iceberg TBLPROPERTIES ('format-version' = '3')"
    )
    for index, moment in enumerate(MOMENTS):
        us = "2026-01-02 03:04:05.123456" if index == 0 else "1969-12-31 23:59:59.999999"
        spark.sql(
            f"INSERT INTO {SRC} SELECT {index + 1}, "
            f"CAST('{moment}' AS timestamp_ns), "
            f"CAST('{moment}+00:00' AS timestamptz_ns), "
            f"TIMESTAMP '{us}', CAST(TIMESTAMP '{us}' AS TIMESTAMP_NTZ), "
            "TIMESTAMP '9999-12-31 00:00:00'"
        ).toArrow()
    return spark


def source_schema(spark: Any) -> dict[str, str]:
    """Return the Arrow type name of each source column on row 1."""
    table = spark.sql(f"SELECT ns, tzns, ts, ntz, far FROM {SRC} WHERE id = 1").toArrow()
    return {name: str(table.schema.field(name).type) for name in table.schema.names}


def create_target(spark: Any, table: str, target: str, source: str, door: str) -> None:
    """Create ``table`` as ``(id, v, k, c)`` for ``door``."""
    layout = "PARTITIONED BY (k) " if door in PARTITIONED else ""
    mode = MOR if door.endswith("_mor") else ""
    spark.sql(
        f"CREATE TABLE {table} (id INT, v {TARGETS[target]}, k INT, "
        f"c {SOURCES[source]}) USING iceberg {layout}"
        f"TBLPROPERTIES ('format-version' = '3', 'write.wap.enabled' = 'true'{mode})"
    )


def seed(spark: Any, table: str, source: str = "") -> None:
    """Write one row per moment with a null ``v``."""
    if source:
        spark.sql(f"INSERT INTO {table} (id, k, c) SELECT id, 0, {source} FROM {SRC}").toArrow()
    else:
        spark.sql(f"INSERT INTO {table} (id, k) SELECT id, 0 FROM {SRC}").toArrow()


def stored(warehouse: Path, table: str) -> tuple[list[list[Any]], str]:
    """Return non-null ``[id, v]`` pairs and the Parquet type of ``v``."""
    directory = warehouse / "ns" / table.rsplit(".", 1)[-1]
    pairs: list[list[Any]] = []
    types: list[str] = []
    if not directory.exists():
        return [], ""
    for path in sorted(directory.rglob("*.parquet")):
        file = pq.ParquetFile(path)
        if "id" not in file.schema_arrow.names or "v" not in file.schema_arrow.names:
            continue
        rows = file.read(columns=["id", "v"])
        column = rows.column("v")
        label = str(column.type)
        if label not in types:
            types.append(label)
        if pa.types.is_timestamp(column.type):
            ticks = column.cast(pa.int64()).to_pylist()
        else:
            ticks = [None if item is None else str(item) for item in column.to_pylist()]
        for row, tick in zip(rows.column("id").to_pylist(), ticks, strict=True):
            if tick is not None:
                pairs.append([row, tick])
    return sorted(pairs, key=repr), ",".join(types)


def cleanup(spark: Any, warehouse: Path, table: str) -> None:
    """Drop ``table`` and its views, then delete the table directory."""
    short = table.rsplit(".", 1)[-1]
    for statement in (
        f"DROP VIEW IF EXISTS v_{short}",
        f"DROP VIEW IF EXISTS ice.ns.v_{short}",
        f"DROP TABLE IF EXISTS {table}",
    ):
        try:
            spark.sql(statement).toArrow()
        except Exception:
            continue
    directory = warehouse / "ns" / short
    if directory.exists():
        shutil.rmtree(directory, ignore_errors=True)


class Cell:
    """One measured statement: a role, a form, a sibling and a source."""

    def __init__(
        self,
        spark: Any,
        table: str,
        source: str,
        form: str,
        sibling: str,
        role: str,
    ) -> None:
        self.spark = spark
        self.table = table
        self.source = source
        self.form = form
        self.sibling = sibling
        self.role = role

    def sql(self, statement: str) -> None:
        """Run ``statement`` and drain it."""
        self.spark.sql(statement).toArrow()

    def value_sql(
        self,
        column: str,
        ident: str,
        qualifier: str = "",
        outer_id: str = "",
    ) -> str:
        """Return the scalar SQL for this cell."""
        return value_of(
            self.role,
            self.form,
            self.source,
            self.sibling,
            column,
            ident,
            qualifier,
            outer_id,
        )

    def carries_union(self) -> bool:
        """Return whether this cell's spelling is a ``UNION ALL``."""
        return self.form == "union" and self.role != "plain"

    def _union(self) -> str:
        return union_body(self.source, self.sibling, self.role == "twin")

    def grouped(self) -> str:
        """Return one row per id. ``max`` keeps the source when the sibling is null."""
        return f"SELECT id, max(v) AS v, 0 AS k FROM ({self._union()}) u GROUP BY id"

    def select(self) -> str:
        """Return ``id, v, k`` for an insert of this cell."""
        if self.form == "union" and self.role != "plain":
            return f"SELECT id, v, k FROM ({self._union()}) u"
        value = self.value_sql(self.source, "id")
        return f"SELECT id, {value} AS v, 0 AS k FROM {SRC}"

    def whole(self) -> str:
        """Return ``id, v, k, c`` for a positional write."""
        null = f"CAST(NULL AS {SOURCES[self.source]})"
        if self.form == "union" and self.role != "plain":
            return f"SELECT id, v, k, {null} AS c FROM ({self._union()}) u"
        value = self.value_sql(self.source, "id")
        return f"SELECT id, {value} AS v, 0 AS k, {null} AS c FROM {SRC}"

    def frame(self) -> Any:
        """Return the DataFrame of :meth:`whole`."""
        return self.spark.sql(self.whole())


def run_insert_values(cell: Cell) -> None:
    """Insert one VALUES row per moment, or a union of those rows."""
    if cell.role != "plain" and cell.sibling in COLUMN_SIBLINGS:
        raise LookupError("values")
    if cell.form == "union" and cell.role != "plain":
        parts: list[str] = []
        for index in range(len(MOMENTS)):
            literal = moment_literal(cell.source, index)
            c_expr, s_expr = literal_arms(cell.source, cell.sibling, literal, cell.role == "twin")
            parts.append(f"SELECT {index + 1}, {c_expr}, 0")
            parts.append(f"SELECT {index + 1}, {s_expr}, 0")
        cell.sql(f"INSERT INTO {cell.table} (id, v, k) " + " UNION ALL ".join(parts))
        return
    rows = ", ".join(
        f"({index + 1}, {cell.value_sql(moment_literal(cell.source, index), str(index + 1))}, 0)"
        for index in range(len(MOMENTS))
    )
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) VALUES {rows}")


def run_insert_select(cell: Cell) -> None:
    """Insert the cell's select."""
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) {cell.select()}")


def run_insert_positional(cell: Cell) -> None:
    """Insert the four-column projection positionally."""
    cell.sql(f"INSERT INTO {cell.table} {cell.whole()}")


def run_insert_by_name(cell: Cell) -> None:
    """Insert by column name."""
    if cell.carries_union():
        cell.sql(f"INSERT INTO {cell.table} BY NAME {cell.grouped()}")
        return
    value = cell.value_sql(cell.source, "id")
    by_name = f"SELECT 0 AS k, {value} AS v, id FROM {SRC}"
    cell.sql(f"INSERT INTO {cell.table} BY NAME {by_name}")


def run_insert_overwrite(cell: Cell) -> None:
    """Overwrite every partition with the cell."""
    seed(cell.spark, cell.table)
    cell.sql(f"INSERT OVERWRITE {cell.table} {cell.whole()}")


def run_insert_overwrite_dynamic(cell: Cell) -> None:
    """Overwrite dynamic partitions with the cell."""
    seed(cell.spark, cell.table)
    cell.sql(f"INSERT OVERWRITE {cell.table} {cell.whole()}")


def run_insert_overwrite_static(cell: Cell) -> None:
    """Overwrite the static ``k = 0`` partition."""
    seed(cell.spark, cell.table)
    null = f"CAST(NULL AS {SOURCES[cell.source]})"
    if cell.form == "union" and cell.role != "plain":
        static = f"SELECT id, v, {null} AS c FROM ({cell._union()}) u"
    else:
        value = cell.value_sql(cell.source, "id")
        static = f"SELECT id, {value} AS v, {null} AS c FROM {SRC}"
    cell.sql(f"INSERT OVERWRITE {cell.table} PARTITION (k = 0) {static}")


def run_insert_replace_where(cell: Cell) -> None:
    """Replace every row by the cell."""
    seed(cell.spark, cell.table)
    cell.sql(f"INSERT INTO {cell.table} REPLACE WHERE id >= 0 {cell.whole()}")


def _merge_grouped(cell: Cell, table: str, matched: bool) -> None:
    """Update or insert from one row per id of the union."""
    action = (
        "WHEN MATCHED THEN UPDATE SET v = s.v"
        if matched
        else "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s.v, 0)"
    )
    cell.sql(f"MERGE INTO {table} t USING ({cell.grouped()}) s ON t.id = s.id {action}")


def run_merge_insert(cell: Cell) -> None:
    """MERGE insert from the source table."""
    if cell.carries_union():
        _merge_grouped(cell, cell.table, False)
        return
    inserted = cell.value_sql(f"s.{cell.source}", "s.id", qualifier="s.")
    cell.sql(
        f"MERGE INTO {cell.table} t USING {SRC} s ON t.id = s.id "
        f"WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {inserted}, 0)"
    )


def run_merge_insert_subquery(cell: Cell) -> None:
    """MERGE insert from the cell's select."""
    if cell.carries_union():
        _merge_grouped(cell, cell.table, False)
        return
    cell.sql(
        f"MERGE INTO {cell.table} t USING ({cell.select()}) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s.v, 0)"
    )


def run_merge_update(cell: Cell) -> None:
    """MERGE update from the source table."""
    seed(cell.spark, cell.table)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    assigned = cell.value_sql(f"s.{cell.source}", "s.id", qualifier="s.")
    cell.sql(
        f"MERGE INTO {cell.table} t USING {SRC} s ON t.id = s.id "
        f"WHEN MATCHED THEN UPDATE SET v = {assigned}"
    )


def run_merge_update_subquery(cell: Cell) -> None:
    """MERGE update from the cell's select."""
    seed(cell.spark, cell.table)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    cell.sql(
        f"MERGE INTO {cell.table} t USING ({cell.select()}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )


def run_merge_by_source(cell: Cell) -> None:
    """Update rows the source does not match."""
    seed(cell.spark, cell.table, cell.source)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    assigned = cell.value_sql("t.c", "t.id", qualifier="t.", outer_id="t.id")
    cell.sql(
        f"MERGE INTO {cell.table} t USING (SELECT -1 AS id) s ON t.id = s.id "
        f"WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = {assigned}"
    )


def run_update_where(cell: Cell) -> None:
    """Update every row that has ``id >= 0``."""
    seed(cell.spark, cell.table, cell.source)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    assigned = cell.value_sql("c", "id", outer_id="id")
    cell.sql(f"UPDATE {cell.table} SET v = {assigned} WHERE id >= 0")


def run_update_nowhere(cell: Cell) -> None:
    """Update every row, with no WHERE clause."""
    seed(cell.spark, cell.table, cell.source)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    assigned = cell.value_sql("c", "id", outer_id="id")
    cell.sql(f"UPDATE {cell.table} SET v = {assigned}")


def _fn_sibling(cell: Cell, twin: bool) -> Any:
    """Return the sibling as a Column."""
    text = sibling_expr(cell.sibling, "", "")
    if twin:
        text = wrap_member(text, "other", result_zoned(cell.source, cell.sibling))
        return functions.expr(text)
    if cell.sibling == "untyped_null":
        return functions.lit(None)
    if cell.sibling in ("us_column_ts", "us_column_ntz", "column_far"):
        name = {"us_column_ts": "ts", "us_column_ntz": "ntz", "column_far": "far"}[cell.sibling]
        return functions.col(name)
    return functions.expr(text)


def _fn_source(cell: Cell, twin: bool) -> Any:
    """Return the source column, wrapped when ``twin`` needs the kernel."""
    if not twin:
        return functions.col(cell.source)
    kind = "ns" if cell.source == "ns" else "tzns"
    zoned = result_zoned(cell.source, cell.sibling)
    wrapped = wrap_member(cell.source, kind, zoned)
    if wrapped == cell.source:
        return functions.col(cell.source)
    return functions.expr(wrapped)


def functions_frame(cell: Cell) -> Any:
    """Return the functions-API frame, or raise when the form has none."""
    if cell.role == "plain":
        value = functions.col(cell.source)
    elif cell.form == "union":
        if cell.role == "twin":
            return cell.spark.sql(cell.select())
        left = cell.spark.table(SRC).select(
            functions.col("id"),
            functions.col(cell.source).alias("v"),
            functions.lit(0).alias("k"),
        )
        right = cell.spark.table(SRC).select(
            functions.col("id"),
            _fn_sibling(cell, False).alias("v"),
            functions.lit(0).alias("k"),
        )
        return left.unionByName(right)
    elif cell.role == "twin":
        value = functions.expr(cell.value_sql(cell.source, "id"))
    else:
        left = _fn_source(cell, False)
        right = _fn_sibling(cell, False)
        if cell.form == "coalesce":
            value = functions.coalesce(left, right)
        elif cell.form == "nvl":
            value = functions.nvl(left, right)
        elif cell.form == "ifnull":
            value = functions.ifnull(left, right)
        elif cell.form == "array":
            value = functions.element_at(functions.array(left, right), 1)
        elif cell.form == "case":
            value = functions.when(functions.col("id") > 0, left).otherwise(right)
        elif cell.form == "if":
            value = functions.expr(cell.value_sql(cell.source, "id"))
        else:
            raise LookupError(cell.form)
    return cell.spark.table(SRC).select(
        functions.col("id"),
        value.alias("v"),
        functions.lit(0).alias("k"),
    )


def run_df_append(cell: Cell) -> None:
    """Append the SQL frame."""
    cell.frame().writeTo(cell.table).append()


def run_df_overwrite(cell: Cell) -> None:
    """Overwrite rows with ``id >= 0``."""
    seed(cell.spark, cell.table)
    cell.frame().writeTo(cell.table).overwrite(functions.col("id") >= 0)


def run_df_overwrite_partitions(cell: Cell) -> None:
    """Overwrite partitions touched by the frame."""
    seed(cell.spark, cell.table)
    cell.frame().writeTo(cell.table).overwritePartitions()


def run_df_create_or_replace(cell: Cell) -> None:
    """Replace the table with the frame."""
    cell.frame().writeTo(cell.table).using("iceberg").createOrReplace()


def run_df_save_append(cell: Cell) -> None:
    """Append through ``saveAsTable``."""
    cell.frame().write.mode("append").saveAsTable(cell.table)


def run_df_save_overwrite(cell: Cell) -> None:
    """Overwrite through ``saveAsTable``."""
    cell.frame().write.mode("overwrite").saveAsTable(cell.table)


def run_df_insert_into(cell: Cell) -> None:
    """Append through ``insertInto``."""
    cell.frame().write.insertInto(cell.table)


def run_df_insert_into_overwrite(cell: Cell) -> None:
    """Overwrite through ``insertInto``."""
    seed(cell.spark, cell.table)
    cell.frame().write.insertInto(cell.table, overwrite=True)


def run_df_select_expr_append(cell: Cell) -> None:
    """Append a ``selectExpr`` projection."""
    if cell.carries_union():
        cell.spark.sql(cell.select()).writeTo(cell.table).append()
        return
    value = cell.value_sql(cell.source, "id")
    frame = cell.spark.table(SRC).selectExpr("id", f"{value} AS v", "0 AS k")
    frame.writeTo(cell.table).append()


def run_df_functions_append(cell: Cell) -> None:
    """Append the functions-API frame."""
    functions_frame(cell).writeTo(cell.table).append()


def run_df_merge_into(cell: Cell) -> None:
    """Merge the SQL frame on ``id``."""
    seed(cell.spark, cell.table)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    cell.frame().mergeInto(cell.table, "id").whenMatched().updateAll().merge()


def run_branch_insert(cell: Cell) -> None:
    """Insert the select into a new branch."""
    seed(cell.spark, cell.table, cell.source)
    cell.sql(f"ALTER TABLE {cell.table} CREATE BRANCH b1")
    cell.sql(f"INSERT INTO {cell.table}.branch_b1 (id, v, k) {cell.select()}")


def run_branch_merge(cell: Cell) -> None:
    """Merge the select into a new branch."""
    seed(cell.spark, cell.table, cell.source)
    cell.sql(f"ALTER TABLE {cell.table} CREATE BRANCH b1")
    if cell.carries_union():
        _merge_grouped(cell, f"{cell.table}.branch_b1", True)
        return
    cell.sql(
        f"MERGE INTO {cell.table}.branch_b1 t USING ({cell.select()}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )


def run_branch_update(cell: Cell) -> None:
    """Update a new branch from the seeded source column."""
    seed(cell.spark, cell.table, cell.source)
    cell.sql(f"ALTER TABLE {cell.table} CREATE BRANCH b1")
    if cell.carries_union():
        _merge_grouped(cell, f"{cell.table}.branch_b1", True)
        return
    assigned = cell.value_sql("c", "id", outer_id="id")
    cell.sql(f"UPDATE {cell.table}.branch_b1 SET v = {assigned} WHERE id >= 0")


def run_wap_insert(cell: Cell) -> None:
    """Insert under a WAP id."""
    cell.spark.conf.set("spark.wap.id", "w1")
    try:
        cell.sql(f"INSERT INTO {cell.table} (id, v, k) {cell.select()}")
    finally:
        cell.spark.conf.unset("spark.wap.id")


def run_explain_insert(cell: Cell) -> None:
    """Explain an insert of the cell."""
    cell.sql(f"EXPLAIN INSERT INTO {cell.table} {cell.whole()}")


def run_explain_analyze_insert(cell: Cell) -> None:
    """Explain-analyze an insert of the cell."""
    cell.sql(f"EXPLAIN ANALYZE INSERT INTO {cell.table} {cell.whole()}")


def run_explain_analyze_update(cell: Cell) -> None:
    """Explain-analyze an update of the cell."""
    seed(cell.spark, cell.table, cell.source)
    if cell.carries_union():
        cell.sql(
            f"EXPLAIN ANALYZE MERGE INTO {cell.table} t USING ({cell.grouped()}) s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"
        )
        return
    assigned = cell.value_sql("c", "id", outer_id="id")
    cell.sql(f"EXPLAIN ANALYZE UPDATE {cell.table} SET v = {assigned} WHERE id >= 0")


def run_prepare_execute_insert(cell: Cell) -> None:
    """Prepare an insert and execute it."""
    short = cell.table.rsplit(".", 1)[-1]
    cell.sql(f"PREPARE p_{short} AS INSERT INTO {cell.table} {cell.whole()}")
    cell.sql(f"EXECUTE p_{short}")


def run_ctas(cell: Cell) -> None:
    """Create the table from the select."""
    cell.sql(f"DROP TABLE {cell.table}")
    cell.sql(
        f"CREATE TABLE {cell.table} USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3') AS {cell.select()}"
    )


def run_rtas(cell: Cell) -> None:
    """Replace the table from the select."""
    cell.sql(f"DROP TABLE {cell.table}")
    cell.sql(
        f"CREATE OR REPLACE TABLE {cell.table} USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3') AS {cell.select()}"
    )


def run_cte_insert(cell: Cell) -> None:
    """Insert through a CTE."""
    cell.sql(f"INSERT INTO {cell.table} WITH q AS ({cell.whole()}) SELECT id, v, k, c FROM q")


def run_subquery_insert(cell: Cell) -> None:
    """Insert through a nested subquery."""
    cell.sql(
        f"INSERT INTO {cell.table} (id, v, k) "
        f"SELECT a.id, a.v, a.k FROM (SELECT b.* FROM ({cell.select()}) b) a"
    )


def run_union_insert(cell: Cell) -> None:
    """Insert one plain row unioned with the rest of the select."""
    plain = f"SELECT id, {cell.source} AS v, 0 AS k FROM {SRC} WHERE id = 1"
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) {plain} UNION ALL {cell.select()} WHERE id > 1")


def run_join_insert(cell: Cell) -> None:
    """Insert the select joined back to the source."""
    cell.sql(
        f"INSERT INTO {cell.table} (id, v, k) "
        f"SELECT l.id, r.v, 0 FROM {SRC} l JOIN ({cell.select()}) r ON l.id = r.id"
    )


def run_aggregate_insert(cell: Cell) -> None:
    """Insert ``max`` of the spelling, grouped by id."""
    if cell.form == "union" and cell.role != "plain":
        cell.sql(
            f"INSERT INTO {cell.table} (id, v, k) "
            f"SELECT id, max(v), 0 FROM ({cell._union()}) u GROUP BY id"
        )
        return
    value = cell.value_sql(cell.source, "id")
    cell.sql(
        f"INSERT INTO {cell.table} (id, v, k) SELECT id, max({value}), 0 FROM {SRC} GROUP BY id"
    )


def run_window_insert(cell: Cell) -> None:
    """Insert ``first_value`` of the spelling."""
    if cell.carries_union():
        cell.sql(
            f"INSERT INTO {cell.table} (id, v, k) SELECT id, "
            "first_value(v) OVER (PARTITION BY id ORDER BY id), 0 "
            f"FROM (SELECT id, v FROM ({cell._union()}) u WHERE v IS NOT NULL) q"
        )
        return
    value = cell.value_sql(cell.source, "id")
    windowed = f"first_value({value}) OVER (PARTITION BY id ORDER BY id)"
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) SELECT id, {windowed}, 0 FROM {SRC}")


def run_distinct_insert(cell: Cell) -> None:
    """Insert the select unioned with itself, then distinct."""
    cell.sql(
        f"INSERT INTO {cell.table} (id, v, k) "
        f"SELECT DISTINCT id, v, k FROM ({cell.select()} UNION ALL {cell.select()})"
    )


def run_scalar_subquery_insert(cell: Cell) -> None:
    """Insert a correlated max of the spelling."""
    if cell.carries_union():
        cell.sql(f"INSERT INTO {cell.table} (id, v, k) {cell.grouped()}")
        return
    inner = cell.value_sql(f"i.{cell.source}", "i.id", qualifier="i.")
    scalar = f"(SELECT max({inner}) FROM {SRC} i WHERE i.id = o.id)"
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) SELECT o.id, {scalar}, 0 FROM {SRC} o")


def run_frame_view_insert(cell: Cell) -> None:
    """Insert from a DataFrame temp view."""
    short = "v_" + cell.table.rsplit(".", 1)[-1]
    cell.spark.sql(cell.select()).createOrReplaceTempView(short)
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) SELECT id, v, k FROM {short}")


def run_sql_view_insert(cell: Cell) -> None:
    """Insert from a SQL temporary view."""
    short = "v_" + cell.table.rsplit(".", 1)[-1]
    cell.sql(f"CREATE OR REPLACE TEMPORARY VIEW {short} AS {cell.select()}")
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) SELECT id, v, k FROM {short}")


def run_catalog_view_insert(cell: Cell) -> None:
    """Insert from a catalog view."""
    short = "v_" + cell.table.rsplit(".", 1)[-1]
    cell.sql(f"CREATE VIEW ice.ns.{short} AS {cell.select()}")
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) SELECT id, v, k FROM ice.ns.{short}")


def run_frame_view_merge(cell: Cell) -> None:
    """Merge a DataFrame temp view."""
    seed(cell.spark, cell.table)
    if cell.carries_union():
        _merge_grouped(cell, cell.table, True)
        return
    short = "v_" + cell.table.rsplit(".", 1)[-1]
    cell.spark.sql(cell.select()).createOrReplaceTempView(short)
    cell.sql(
        f"MERGE INTO {cell.table} t USING {short} s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )


def run_cached_frame_append(cell: Cell) -> None:
    """Cache the frame, then append it."""
    cell.frame().cache().writeTo(cell.table).append()


def run_merge_update_star(cell: Cell) -> None:
    """MERGE update every matched column."""
    seed(cell.spark, cell.table)
    source = f"({cell.whole()})"
    if cell.carries_union():
        source = f"(SELECT id, v, k, c FROM ({cell.whole()}) q WHERE v IS NOT NULL)"
    cell.sql(
        f"MERGE INTO {cell.table} t USING {source} s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
    )


def run_merge_insert_star(cell: Cell) -> None:
    """MERGE insert every column."""
    source = f"({cell.whole()})"
    if cell.carries_union():
        source = f"(SELECT id, v, k, c FROM ({cell.whole()}) q WHERE v IS NOT NULL)"
    cell.sql(
        f"MERGE INTO {cell.table} t USING {source} s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
    )


def run_update_scalar_subquery(cell: Cell) -> None:
    """Update from an uncorrelated max."""
    seed(cell.spark, cell.table, cell.source)
    if cell.carries_union():
        body = cell._union()
        cell.sql(f"UPDATE {cell.table} SET v = (SELECT max(v) FROM ({body}) u) WHERE id >= 0")
        return
    inner = cell.value_sql(f"i.{cell.source}", "i.id", qualifier="i.")
    scalar = f"(SELECT max({inner}) FROM {SRC} i)"
    cell.sql(f"UPDATE {cell.table} SET v = {scalar} WHERE id >= 0")


def run_insert_reordered(cell: Cell) -> None:
    """Insert with the target columns named out of order."""
    if cell.carries_union():
        cell.sql(f"INSERT INTO {cell.table} (k, v, id) SELECT k, v, id FROM ({cell.grouped()}) q")
        return
    value = cell.value_sql(cell.source, "id")
    cell.sql(f"INSERT INTO {cell.table} (k, v, id) SELECT 0, {value}, id FROM {SRC}")


def run_sort_limit_insert(cell: Cell) -> None:
    """Insert the select ordered and limited."""
    cell.sql(f"INSERT INTO {cell.table} (id, v, k) {cell.select()} ORDER BY id LIMIT 3")


def run_union_null_insert(cell: Cell) -> None:
    """Insert the select above id 1, unioned with a null row."""
    cell.sql(
        f"INSERT INTO {cell.table} (id, v, k) {cell.select()} WHERE id > 1 "
        "UNION ALL SELECT 1, NULL, 0"
    )


def run_df_union_null_append(cell: Cell) -> None:
    """Append the select unioned with a null row."""
    absent = cell.spark.sql("SELECT 9 AS id, NULL AS v, 0 AS k")
    cell.spark.sql(cell.select()).unionByName(absent).writeTo(cell.table).append()


def run_lambda_first_insert(cell: Cell) -> None:
    """Insert the spelling beside a lambda that mentions only ``id``."""
    beside = "size(transform(array(id), x -> x))"
    if cell.carries_union():
        cell.sql(
            f"INSERT INTO {cell.table} (k, v, id) SELECT {beside}, v, id FROM ({cell.grouped()}) q"
        )
        return
    value = cell.value_sql(cell.source, "id")
    cell.sql(f"INSERT INTO {cell.table} (k, v, id) SELECT {beside}, {value}, id FROM {SRC}")


def run_df_lambda_below_append(cell: Cell) -> None:
    """Append the spelling from a frame whose lambda is a sibling column."""
    if cell.carries_union():
        cell.spark.sql(
            "SELECT g.id, g.v, 0 AS k FROM "
            f"({cell.grouped()}) g JOIN "
            f"(SELECT id, size(transform(array(id), x -> x)) AS n FROM {SRC}) lam "
            "ON g.id = lam.id"
        ).writeTo(cell.table).append()
        return
    below = cell.spark.sql(
        f"SELECT id, ns, tzns, ts, ntz, far, transform(array(id), x -> x) AS a FROM {SRC}"
    )
    value = cell.value_sql(cell.source, "id")
    below.selectExpr("id", f"{value} AS v", "0 AS k").writeTo(cell.table).append()


RUNNERS = {
    "insert_values": run_insert_values,
    "insert_select": run_insert_select,
    "insert_positional": run_insert_positional,
    "insert_by_name": run_insert_by_name,
    "insert_overwrite": run_insert_overwrite,
    "insert_overwrite_dynamic": run_insert_overwrite_dynamic,
    "insert_overwrite_static": run_insert_overwrite_static,
    "insert_replace_where": run_insert_replace_where,
    "merge_insert": run_merge_insert,
    "merge_insert_subquery": run_merge_insert_subquery,
    "merge_update": run_merge_update,
    "merge_update_subquery": run_merge_update_subquery,
    "merge_by_source": run_merge_by_source,
    "update_where": run_update_where,
    "update_nowhere": run_update_nowhere,
    "df_append": run_df_append,
    "df_overwrite": run_df_overwrite,
    "df_overwrite_partitions": run_df_overwrite_partitions,
    "df_create_or_replace": run_df_create_or_replace,
    "df_save_append": run_df_save_append,
    "df_save_overwrite": run_df_save_overwrite,
    "df_insert_into": run_df_insert_into,
    "df_insert_into_overwrite": run_df_insert_into_overwrite,
    "df_select_expr_append": run_df_select_expr_append,
    "df_functions_append": run_df_functions_append,
    "df_merge_into": run_df_merge_into,
    "branch_insert": run_branch_insert,
    "branch_merge": run_branch_merge,
    "branch_update": run_branch_update,
    "wap_insert": run_wap_insert,
    "explain_insert": run_explain_insert,
    "explain_analyze_insert": run_explain_analyze_insert,
    "explain_analyze_update": run_explain_analyze_update,
    "prepare_execute_insert": run_prepare_execute_insert,
    "ctas": run_ctas,
    "rtas": run_rtas,
    "cte_insert": run_cte_insert,
    "subquery_insert": run_subquery_insert,
    "union_insert": run_union_insert,
    "join_insert": run_join_insert,
    "aggregate_insert": run_aggregate_insert,
    "window_insert": run_window_insert,
    "distinct_insert": run_distinct_insert,
    "scalar_subquery_insert": run_scalar_subquery_insert,
    "frame_view_insert": run_frame_view_insert,
    "sql_view_insert": run_sql_view_insert,
    "catalog_view_insert": run_catalog_view_insert,
    "frame_view_merge": run_frame_view_merge,
    "cached_frame_append": run_cached_frame_append,
    "merge_update_star": run_merge_update_star,
    "merge_insert_star": run_merge_insert_star,
    "update_scalar_subquery": run_update_scalar_subquery,
    "insert_reordered": run_insert_reordered,
    "sort_limit_insert": run_sort_limit_insert,
    "union_null_insert": run_union_null_insert,
    "df_union_null_append": run_df_union_null_append,
    "lambda_first_insert": run_lambda_first_insert,
    "df_lambda_below_append": run_df_lambda_below_append,
}


def run_door(cell: Cell, door: str) -> None:
    """Run ``door`` for ``cell``."""
    plain = door.removesuffix("_mor")
    if plain in SQL_DOORS or plain in FRAME_DOORS or plain in CARRIERS or plain in HUNTED:
        RUNNERS[plain](cell)
        return
    raise KeyError(door)


def measure(
    spark: Any,
    warehouse: Path,
    target: str,
    source: str,
    form: str,
    sibling: str,
    role: str,
    door: str,
) -> dict[str, Any]:
    """Run one cell and return what it stored."""
    table = fresh_table()
    cell = Cell(spark, table, source, form, sibling, role)
    result: dict[str, Any] = {}
    try:
        create_target(spark, table, target, source, door)
        try:
            run_door(cell, door)
        except LookupError:
            result["skip"] = True
        except Exception as error:
            result["error"] = error_text(error)
            result["error_type"] = type(error).__name__
        if result.get("skip"):
            result["stored"] = []
        else:
            pairs, parquet_type = stored(warehouse, table)
            result["stored"] = pairs
            if parquet_type:
                result["parquet_type"] = parquet_type
    except Exception as error:
        result["error"] = error_text(error)
        result["error_type"] = type(error).__name__
        result["stored"] = []
    finally:
        cleanup(spark, warehouse, table)
    return result


def door_names() -> tuple[str, ...]:
    """Return every door the matrix runs."""
    missing = [
        door.removesuffix("_mor") for door in DOORS if door.removesuffix("_mor") not in RUNNERS
    ]
    if missing:
        raise RuntimeError(",".join(missing))
    return DOORS


def form_names() -> tuple[str, ...]:
    """Return the spelling forms."""
    return FORMS

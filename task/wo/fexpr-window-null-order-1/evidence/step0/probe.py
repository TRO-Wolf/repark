import json
import os
import struct
import sys
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any, TextIO

ROWS = (
    "(1, 10, 'b', CAST(1.5 AS DOUBLE), 1), "
    "(2, CAST(NULL AS INT), 'e', CAST(2.5 AS DOUBLE), 1), "
    "(3, 30, CAST(NULL AS STRING), CAST(3.5 AS DOUBLE), 2), "
    "(4, 20, 'a', CAST(NULL AS DOUBLE), 2), "
    "(5, 40, 'd', CAST(0.5 AS DOUBLE), 1)"
)
COLUMNS = "t(id, ki, ks, kd, g)"
KEYS = ("ki", "ks", "kd")
DIRECTIONS = (
    ("default", ""),
    ("asc", " ASC"),
    ("desc", " DESC"),
    ("asc_nf", " ASC NULLS FIRST"),
    ("asc_nl", " ASC NULLS LAST"),
    ("desc_nf", " DESC NULLS FIRST"),
    ("desc_nl", " DESC NULLS LAST"),
)
WINDOW_FUNCTIONS = (
    ("row_number", "row_number() OVER ({p}ORDER BY {o})"),
    ("rank", "rank() OVER ({p}ORDER BY {o})"),
    ("dense_rank", "dense_rank() OVER ({p}ORDER BY {o})"),
    ("first_value", "first_value(id) OVER ({p}ORDER BY {o})"),
    (
        "last_value",
        "last_value(id) OVER ({p}ORDER BY {o} "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    ),
    ("lag", "lag(id) OVER ({p}ORDER BY {o})"),
    ("lead", "lead(id) OVER ({p}ORDER BY {o})"),
    ("running_sum", "sum(id) OVER ({p}ORDER BY {o})"),
    (
        "collect_list",
        "collect_list(id) OVER ({p}ORDER BY {o} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    ),
)
MULTI_ORDERS = (
    ("g_ki_desc", "g, ki DESC"),
    ("g_desc_ki", "g DESC, ki"),
    ("g_ks_desc_nf", "g, ks DESC NULLS FIRST"),
    ("kd_desc_g", "kd DESC, g"),
    ("ks_g_desc", "ks, g DESC"),
)
AGGREGATES = (
    ("percentile_cont_asc", "percentile_cont(0.25) WITHIN GROUP (ORDER BY kd)"),
    ("percentile_cont_desc", "percentile_cont(0.25) WITHIN GROUP (ORDER BY kd DESC)"),
    ("percentile_disc_asc", "percentile_disc(0.25) WITHIN GROUP (ORDER BY ki)"),
    ("percentile_disc_desc", "percentile_disc(0.25) WITHIN GROUP (ORDER BY ki DESC)"),
    ("mode_within", "mode() WITHIN GROUP (ORDER BY ki)"),
    ("listagg_ki", "listagg(ks, ',') WITHIN GROUP (ORDER BY ki)"),
    ("listagg_ki_desc", "listagg(ks, ',') WITHIN GROUP (ORDER BY ki DESC)"),
    ("listagg_ki_desc_nf", "listagg(ks, ',') WITHIN GROUP (ORDER BY ki DESC NULLS FIRST)"),
    ("listagg_kd", "listagg(ks, ',') WITHIN GROUP (ORDER BY kd)"),
    ("listagg_kd_asc_nl", "listagg(ks, ',') WITHIN GROUP (ORDER BY kd ASC NULLS LAST)"),
    ("string_agg_ki_desc", "string_agg(ks, ',') WITHIN GROUP (ORDER BY ki DESC)"),
    ("array_agg_order_ki", "array_agg(id ORDER BY ki)"),
    ("array_agg_order_ki_desc", "array_agg(id ORDER BY ki DESC)"),
    ("string_agg_order_ks", "string_agg(ks, ',' ORDER BY ks)"),
    ("first_value_agg_order", "first_value(id ORDER BY ki DESC)"),
)
SCALARS = (
    ("array_sort_plain", "array_sort(array(3, CAST(NULL AS INT), 1))"),
    ("sort_array_asc", "sort_array(array(3, CAST(NULL AS INT), 1))"),
    ("sort_array_desc", "sort_array(array(3, CAST(NULL AS INT), 1), false)"),
    (
        "array_sort_comparator",
        "array_sort(array(3, CAST(NULL AS INT), 1), (l, r) -> CASE WHEN l IS NULL THEN -1 "
        "WHEN r IS NULL THEN 1 WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END)",
    ),
    (
        "subquery_order_asc",
        "(SELECT a FROM VALUES (CAST(NULL AS INT)), (1) AS v(a) ORDER BY a LIMIT 1)",
    ),
    (
        "subquery_order_desc",
        "(SELECT a FROM VALUES (CAST(NULL AS INT)), (1) AS v(a) ORDER BY a DESC LIMIT 1)",
    ),
)


def plain(value: Any) -> Any:
    if isinstance(value, float):
        if value != value or value == 0.0:
            return "f64:" + struct.pack(">d", value).hex()
        return value
    if isinstance(value, Decimal):
        return "dec:" + str(value)
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "asDict"):
        return {key: plain(item) for key, item in value.asDict().items()}
    return value


def rows_of(frame: Any) -> list[list[Any]]:
    return [[plain(cell) for cell in row] for row in frame.collect()]


class Probe:
    def __init__(self, engine: str, session: Any, functions: Any, window: Any, native: Any) -> None:
        self.engine = engine
        self.session = session
        self.F = functions
        self.Window = window
        self.native = native
        session.sql(f"SELECT * FROM VALUES {ROWS} AS {COLUMNS}").createOrReplaceTempView("w")
        self.w = session.table("w")

    def window_text(self, template: str, order: str, partition: bool = False) -> str:
        return template.format(p="PARTITION BY g " if partition else "", o=order)

    def window_cells(self) -> Iterator[tuple[str, str]]:
        for name, template in WINDOW_FUNCTIONS:
            for key in KEYS:
                for direction, suffix in DIRECTIONS:
                    yield f"W|{name}|{key}|{direction}", self.window_text(template, key + suffix)
            for label, order in MULTI_ORDERS:
                yield f"W|{name}|multi|{label}", self.window_text(template, order)
            for key in KEYS:
                yield f"W|{name}|{key}|part_desc", self.window_text(template, key + " DESC", True)
                yield f"W|{name}|{key}|part_default", self.window_text(template, key, True)

    def run(self, out: TextIO) -> None:
        for cell, text in self.window_cells():
            self.emit(out, cell, "fexpr", text, lambda t=text: self.fexpr_select(t))
            self.emit(out, cell, "fexpr_withcolumn", text, lambda t=text: self.fexpr_withcolumn(t))
            self.emit(out, cell, "selectexpr", text, lambda t=text: self.select_expr(t))
            self.emit(out, cell, "sparksql", text, lambda t=text: self.spark_sql(t))
            if self.native is not None:
                self.emit(out, cell, "native", text, lambda t=text: self.native_sql(t))
        for key in KEYS:
            for direction, suffix in DIRECTIONS:
                text = f"row_number() OVER (ORDER BY {key}{suffix})"
                cell = f"D|dedup|{key}|{direction}"
                self.emit(out, cell, "fexpr", text, lambda t=text: self.dedup(t))
                part = f"row_number() OVER (PARTITION BY g ORDER BY {key}{suffix})"
                cell = f"D|dedup_part|{key}|{direction}"
                self.emit(out, cell, "fexpr", part, lambda t=part: self.dedup(t))
            for direction, suffix in DIRECTIONS:
                text = f"{key}{suffix}"
                cell = f"S|orderby|{key}|{direction}"
                self.emit(out, cell, "orderby_fexpr", text, lambda t=text: self.order_by_fexpr(t))
                self.emit(out, cell, "sort_string", text, lambda t=text: self.sort_text(t))
            for method in (
                "asc",
                "desc",
                "asc_nulls_first",
                "asc_nulls_last",
                "desc_nulls_first",
                "desc_nulls_last",
            ):
                cell = f"T|typed|{key}|{method}"
                self.emit(
                    out, cell, "sort_col", method, lambda k=key, m=method: self.sort_col(k, m)
                )
                self.emit(
                    out,
                    cell,
                    "orderby_fexpr_method",
                    method,
                    lambda k=key, m=method: self.sort_fexpr(k, m),
                )
                self.emit(
                    out,
                    cell,
                    "typed_window",
                    method,
                    lambda k=key, m=method: self.typed_window(k, m),
                )
        for name, text in AGGREGATES:
            cell = f"A|{name}"
            self.emit(out, cell, "fexpr_agg", text, lambda t=text: self.fexpr_agg(t))
            self.emit(out, cell, "fexpr_groupby", text, lambda t=text: self.fexpr_groupby(t))
            self.emit(out, cell, "selectexpr", text, lambda t=text: self.select_expr_agg(t))
            self.emit(out, cell, "sparksql", text, lambda t=text: self.spark_sql_agg(t))
            if self.native is not None:
                self.emit(out, cell, "native", text, lambda t=text: self.native_agg(t))
        for name, text in SCALARS:
            cell = f"X|{name}"
            self.emit(
                out,
                cell,
                "fexpr",
                text,
                lambda t=text: rows_of(self.w.limit(1).select(self.F.expr(t))),
            )
            self.emit(
                out, cell, "sparksql", text, lambda t=text: rows_of(self.session.sql(f"SELECT {t}"))
            )

    def emit(self, out: TextIO, cell: str, door: str, text: str, action: Callable[[], Any]) -> None:
        record = {"cell": cell, "door": door, "text": text}
        try:
            record["rows"] = action()
            record["outcome"] = "rows"
        except Exception as error:
            record["outcome"] = "error"
            record["error"] = f"{type(error).__name__}: {error}".replace("\n", " ")[:400]
        out.write(json.dumps(record, sort_keys=True) + "\n")

    def fexpr_select(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.select("id", self.F.expr(text).alias("r")).orderBy("id"))

    def fexpr_withcolumn(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.withColumn("r", self.F.expr(text)).select("id", "r").orderBy("id"))

    def select_expr(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.selectExpr("id", f"{text} AS r").orderBy("id"))

    def spark_sql(self, text: str) -> list[list[Any]]:
        return rows_of(self.session.sql(f"SELECT id, {text} AS r FROM w ORDER BY id"))

    def native_sql(self, text: str) -> list[list[Any]]:
        query = (
            f"WITH w AS (SELECT * FROM (VALUES {ROWS}) AS {COLUMNS}) "
            f"SELECT id, {text} AS r FROM w ORDER BY id"
        )
        return rows_of(self.native(query))

    def dedup(self, text: str) -> list[list[Any]]:
        frame = self.w.withColumn("r", self.F.expr(text)).filter("r = 1").select("id")
        return sorted(rows_of(frame))

    def order_by_fexpr(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.orderBy(self.F.expr(text)).select("id"))

    def sort_text(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.sort(self.F.expr(text)).select("id"))

    def sort_col(self, key: str, method: str) -> list[list[Any]]:
        return rows_of(self.w.sort(getattr(self.F.col(key), method)()).select("id"))

    def sort_fexpr(self, key: str, method: str) -> list[list[Any]]:
        return rows_of(self.w.orderBy(getattr(self.F.expr(key), method)()).select("id"))

    def typed_window(self, key: str, method: str) -> list[list[Any]]:
        spec = self.Window.orderBy(getattr(self.F.col(key), method)())
        return rows_of(self.w.select("id", self.F.row_number().over(spec).alias("r")).orderBy("id"))

    def fexpr_agg(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.agg(self.F.expr(text).alias("r")))

    def fexpr_groupby(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.groupBy("g").agg(self.F.expr(text).alias("r")).orderBy("g"))

    def select_expr_agg(self, text: str) -> list[list[Any]]:
        return rows_of(self.w.selectExpr(f"{text} AS r"))

    def spark_sql_agg(self, text: str) -> list[list[Any]]:
        return rows_of(self.session.sql(f"SELECT g, {text} AS r FROM w GROUP BY g ORDER BY g"))

    def native_agg(self, text: str) -> list[list[Any]]:
        query = (
            f"WITH w AS (SELECT * FROM (VALUES {ROWS}) AS {COLUMNS}) "
            f"SELECT g, {text} AS r FROM w GROUP BY g ORDER BY g"
        )
        return rows_of(self.native(query))


def spark_probe() -> tuple[dict[str, str], Probe]:
    from pyspark.sql import SparkSession, functions
    from pyspark.sql.window import Window

    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    header = {"engine": "spark", "version": session.version}
    return header, Probe("spark", session, functions, Window, None)


def repark_probe() -> tuple[dict[str, str], Probe]:
    import repark
    from repark import ReparkSession, Window, functions

    session = ReparkSession.builder.getOrCreate()
    header = {
        "engine": "repark",
        "version": repark.__version__,
        "native_module": Path(repark._native.__file__).name,
    }
    return header, Probe("repark", session, functions, Window, repark.sql)


def main() -> None:
    engine, target = sys.argv[1], sys.argv[2]
    header, probe = spark_probe() if engine == "spark" else repark_probe()
    header["tree"] = os.environ.get("PROBE_TREE", "")
    header["module_sha256"] = os.environ.get("PROBE_MODULE_SHA256", "")
    with Path(target).open("w", encoding="utf-8") as out:
        out.write(json.dumps({"header": header}, sort_keys=True) + "\n")
        probe.run(out)


if __name__ == "__main__":
    main()

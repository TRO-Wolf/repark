import json
from collections.abc import Callable

import pyarrow as pa

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.types import ArrayType, IntegerType, StructField, StructType

spark = ReparkSession.builder.appName("array-null-1-repark").getOrCreate()

NONNULL_SCHEMA = StructType([StructField("a", ArrayType(IntegerType(), False))])


def describe_field(field: pa.Field) -> str:
    type_obj = field.type
    if pa.types.is_list(type_obj) or pa.types.is_large_list(type_obj):
        return f"{type_obj} value_field_nullable={type_obj.value_field.nullable}"
    return str(type_obj)


def cell(
    name: str,
    facade_fn: Callable[[], object],
    sql_text: str | None = None,
    view_rows: list | None = None,
    view_schema: object = None,
) -> None:
    out = {"cell": name}
    try:
        table = facade_fn().to_arrow()
        field = table.schema.field(table.num_columns - 1)
        out["facade"] = {
            "answer": table.column(field.name).to_pylist(),
            "result_type": describe_field(field),
        }
    except Exception as error:
        out["facade"] = {"error": type(error).__name__ + ": " + str(error).splitlines()[0][:300]}
    if sql_text is not None:
        try:
            if view_schema is not None:
                spark.createDataFrame(view_rows, view_schema).createOrReplaceTempView("v")
            table = spark.sql(sql_text).to_arrow()
            field = table.schema.field(table.num_columns - 1)
            out["door"] = {
                "answer": table.column(field.name).to_pylist(),
                "result_type": describe_field(field),
            }
        except Exception as error:
            out["door"] = {"error": type(error).__name__ + ": " + str(error).splitlines()[0][:300]}
    print("RESULT" + json.dumps(out))


def base(ddl: str, rows: list) -> object:
    return spark.createDataFrame(rows, ddl)


for label, fn, sql_name in (
    ("append", F.array_append, "array_append"),
    ("prepend", F.array_prepend, "array_prepend"),
):
    cell(
        f"{label}:null-array",
        lambda fn=fn: base("a array<int>, e int", [(None, 4)]).select(fn("a", F.col("e"))),
        sql_text=f"SELECT {sql_name}(a, e) AS r FROM v",
        view_rows=[(None, 4)],
        view_schema="a array<int>, e int",
    )
    cell(
        f"{label}:null-element",
        lambda fn=fn: base("a array<int>, e int", [([1, 2], None)]).select(fn("a", F.col("e"))),
        sql_text=f"SELECT {sql_name}(a, e) AS r FROM v",
        view_rows=[([1, 2], None)],
        view_schema="a array<int>, e int",
    )
    cell(
        f"{label}:empty-array",
        lambda fn=fn: base("a array<int>, e int", [([], 4)]).select(fn("a", F.col("e"))),
        sql_text=f"SELECT {sql_name}(a, e) AS r FROM v",
        view_rows=[([], 4)],
        view_schema="a array<int>, e int",
    )
    cell(
        f"{label}:nested",
        lambda fn=fn: base("a array<array<int>>, e array<int>", [([[1], [2, 3]], [9])]).select(
            fn("a", F.col("e"))
        ),
        sql_text=f"SELECT {sql_name}(a, e) AS r FROM v",
        view_rows=[([[1], [2, 3]], [9])],
        view_schema="a array<array<int>>, e array<int>",
    )
    cell(
        f"{label}:int-into-bigint",
        lambda fn=fn: base("a array<bigint>", [([1, 2],)]).select(fn("a", F.lit(4))),
        sql_text=f"SELECT {sql_name}(a, 4) AS r FROM v",
        view_rows=[([1, 2],)],
        view_schema="a array<bigint>",
    )
    cell(
        f"{label}:int-into-double",
        lambda fn=fn: base("a array<double>", [([1.0],)]).select(fn("a", F.lit(4))),
        sql_text=f"SELECT {sql_name}(a, 4) AS r FROM v",
        view_rows=[([1.0],)],
        view_schema="a array<double>",
    )
    cell(
        f"{label}:string-into-int",
        lambda fn=fn: base("a array<int>", [([1, 2],)]).select(fn("a", F.lit("x"))),
        sql_text=f"SELECT {sql_name}(a, 'x') AS r FROM v",
        view_rows=[([1, 2],)],
        view_schema="a array<int>",
    )
    cell(
        f"{label}:null-into-nonnullable-element",
        lambda fn=fn: spark.createDataFrame([([1, 2],)], NONNULL_SCHEMA).select(
            fn("a", F.lit(None).cast("int"))
        ),
        sql_text=f"SELECT {sql_name}(a, CAST(NULL AS INT)) AS r FROM v",
        view_rows=[([1, 2],)],
        view_schema=NONNULL_SCHEMA,
    )
    cell(
        f"{label}:literal-array",
        lambda fn=fn: spark.range(1).select(fn(F.array(F.lit(1), F.lit(2)), F.lit(3))),
        sql_text=f"SELECT {sql_name}(array(1, 2), 3) AS r",
    )

spark.stop()

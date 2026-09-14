import json
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812 — PySpark idiom
from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

spark = SparkSession.builder.appName("array-null-1-oracle").master("local[1]").getOrCreate()


def cell(name: str, fn: Callable[[], DataFrame]) -> None:
    try:
        frame = fn()
        rows = frame.collect()
        schema = frame.schema
        field = schema.fields[-1] if schema.fields else None
        result_type = field.dataType.simpleString() if field is not None else "?"
        contains_null = (
            field.dataType.containsNull
            if field is not None and isinstance(field.dataType, ArrayType)
            else None
        )
        payload = [row[-1] for row in rows]
        print(
            "RESULT"
            + json.dumps(
                {
                    "cell": name,
                    "answer": payload,
                    "result_type": result_type,
                    "contains_null": contains_null,
                }
            )
        )
    except Exception as error:
        print(
            "RESULT"
            + json.dumps(
                {
                    "cell": name,
                    "error": type(error).__name__ + ": " + str(error).splitlines()[0][:300],
                }
            )
        )


def base(ddl: str, rows: list) -> DataFrame:
    return spark.createDataFrame(rows, ddl)


for label, fn in (("append", F.array_append), ("prepend", F.array_prepend)):
    cell(
        f"{label}:null-array",
        lambda fn=fn: base("a array<int>, e int", [(None, 4)]).select(fn("a", F.col("e"))),
    )
    cell(
        f"{label}:null-element",
        lambda fn=fn: base("a array<int>, e int", [([1, 2], None)]).select(fn("a", F.col("e"))),
    )
    cell(
        f"{label}:empty-array",
        lambda fn=fn: base("a array<int>, e int", [([], 4)]).select(fn("a", F.col("e"))),
    )
    cell(
        f"{label}:nested",
        lambda fn=fn: base("a array<array<int>>, e array<int>", [([[1], [2, 3]], [9])]).select(
            fn("a", F.col("e"))
        ),
    )
    cell(
        f"{label}:int-into-bigint",
        lambda fn=fn: base("a array<bigint>", [([1, 2],)]).select(fn("a", F.lit(4))),
    )
    cell(
        f"{label}:int-into-double",
        lambda fn=fn: base("a array<double>", [([1.0],)]).select(fn("a", F.lit(4))),
    )
    cell(
        f"{label}:string-into-int",
        lambda fn=fn: base("a array<int>", [([1, 2],)]).select(fn("a", F.lit("x"))),
    )
    cell(
        f"{label}:null-into-nonnullable-element",
        lambda fn=fn: spark.createDataFrame(
            [([1, 2],)],
            StructType([StructField("a", ArrayType(IntegerType(), containsNull=False))]),
        ).select(fn("a", F.lit(None).cast("int"))),
    )
    cell(
        f"{label}:literal-array",
        lambda fn=fn: spark.range(1).select(fn(F.array(F.lit(1), F.lit(2)), F.lit(3))),
    )

spark.stop()

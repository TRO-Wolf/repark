"""Read a frame's schema and project SQL expressions over its columns.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.schema", "DataFrame.selectExpr", "DataFrame.select_expr"]


def main() -> None:
    """Run the measured schema metadata and the selectExpr projections."""
    repark = ReparkSession.builder.appName("ex-df-schema-select").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        simple = frame.schema.simpleString()
        if simple != "struct<g:string,k:bigint,v:double>":
            raise SystemExit(f"DataFrame.schema {simple!r} != 'struct<g:string,k:bigint,v:double>'")
        json_value = frame.schema.jsonValue()
        json_expected = {
            "type": "struct",
            "fields": [
                {"name": "g", "type": "string", "nullable": True, "metadata": {}},
                {"name": "k", "type": "long", "nullable": True, "metadata": {}},
                {"name": "v", "type": "double", "nullable": True, "metadata": {}},
            ],
        }
        if json_value != json_expected:
            raise SystemExit(f"DataFrame.schema {json_value!r} != {json_expected!r}")

        projected_rows = frame.selectExpr("k", "v * 2 AS dv").collect()
        projected_expected = [(1, 20.0), (2, 40.0), (2, 60.0), (3, 80.0), (1, 100.0), (2, None)]
        if projected_rows != projected_expected:
            raise SystemExit(
                f"DataFrame.selectExpr rows {projected_rows!r} != {projected_expected!r}"
            )
        snake_rows = frame.select_expr("k", "v * 2 AS dv").collect()
        if snake_rows != projected_expected:
            raise SystemExit(f"DataFrame.select_expr rows {snake_rows!r} != {projected_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()

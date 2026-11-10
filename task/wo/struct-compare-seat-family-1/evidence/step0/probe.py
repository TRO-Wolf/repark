import json
import re
import sys
from pathlib import Path
from typing import Any

LEG = sys.argv[1]
OUT = sys.argv[2]

CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")

A = "named_struct('a', 1, 'b', 2)"
B = "named_struct('b', 1, 'a', 2)"
C = "named_struct('b', 2, 'a', 1)"


def view(name: Any, columns: Any, rows: Any) -> Any:
    selects = []
    for row in rows:
        selects.append(
            "SELECT "
            + ", ".join(f"{value} AS {column}" for column, value in zip(columns, row, strict=False))
        )
    return f"CREATE OR REPLACE TEMPORARY VIEW {name} AS " + " UNION ALL ".join(selects)


SETUP = [
    view(
        "lk",
        ["lid", "s"],
        [
            (1, "named_struct('a', 1, 'b', 2)"),
            (2, "named_struct('a', 2, 'b', 1)"),
            (3, "CAST(NULL AS STRUCT<a: INT, b: INT>)"),
        ],
    ),
    view(
        "rk",
        ["rid", "s"],
        [
            (10, "named_struct('b', 1, 'a', 2)"),
            (30, "named_struct('b', 7, 'a', 7)"),
            (40, "CAST(NULL AS STRUCT<b: INT, a: INT>)"),
        ],
    ),
    view("rw", ["rid", "s"], [(10, "named_struct('b', CAST(1 AS BIGINT), 'a', 2)")]),
    view(
        "rx",
        ["rid", "s"],
        [(10, "named_struct('x', 1, 'y', 2)"), (30, "named_struct('x', 7, 'y', 7)")],
    ),
    view(
        "rs",
        ["rid", "s"],
        [(10, "named_struct('a', 1, 'b', 2)"), (30, "named_struct('a', 7, 'b', 7)")],
    ),
    view(
        "l2",
        ["lid", "s", "u"],
        [
            (1, "named_struct('a', 1, 'b', 2)", "named_struct('c', 5, 'd', 6)"),
            (2, "named_struct('a', 2, 'b', 1)", "named_struct('c', 6, 'd', 5)"),
        ],
    ),
    view(
        "r2",
        ["rid", "s", "u"],
        [(10, "named_struct('b', 1, 'a', 2)", "named_struct('d', 5, 'c', 6)")],
    ),
    view(
        "lm",
        ["lid", "k", "s"],
        [(1, 7, "named_struct('a', 1, 'b', 2)"), (2, 7, "named_struct('a', 2, 'b', 1)")],
    ),
    view("rm", ["rid", "k", "s"], [(10, 7, "named_struct('b', 1, 'a', 2)")]),
    view(
        "la",
        ["lid", "s"],
        [(1, "array(named_struct('a', 1, 'b', 2))"), (2, "array(named_struct('a', 2, 'b', 1))")],
    ),
    view("ra", ["rid", "s"], [(10, "array(named_struct('b', 1, 'a', 2))")]),
    view("li", ["lid", "k"], [(1, 1), (2, 2)]),
    view("ri", ["rid", "k"], [(10, 1), (30, 3)]),
]

SQL = {}
for how, word in [
    ("inner", "JOIN"),
    ("left", "LEFT JOIN"),
    ("right", "RIGHT JOIN"),
    ("full", "FULL JOIN"),
    ("semi", "LEFT SEMI JOIN"),
    ("anti", "LEFT ANTI JOIN"),
]:
    SQL[f"J-using-{how}-star"] = f"SELECT * FROM lk {word} rk USING (s)"
    SQL[f"J-on-{how}-star"] = f"SELECT * FROM lk {word} rk ON lk.s = rk.s"
for how, word in [
    ("inner", "JOIN"),
    ("left", "LEFT JOIN"),
    ("right", "RIGHT JOIN"),
    ("full", "FULL JOIN"),
]:
    SQL[f"J-natural-{how}-star"] = f"SELECT * FROM lk NATURAL {word} rk"
    SQL[f"J-using-{how}-key"] = f"SELECT s, lid, rid FROM lk {word} rk USING (s)"
SQL.update(
    {
        "J-using-inner-both": "SELECT lk.s AS ls, rk.s AS rs FROM lk JOIN rk USING (s)",
        "J-using-full-both": "SELECT lk.s AS ls, rk.s AS rs, lid, rid FROM lk FULL JOIN rk "
        "USING (s)",
        "J-using-two": "SELECT * FROM l2 JOIN r2 USING (s, u)",
        "J-natural-two": "SELECT * FROM l2 NATURAL JOIN r2",
        "J-using-mixed": "SELECT * FROM lm JOIN rm USING (k, s)",
        "J-natural-mixed": "SELECT * FROM lm NATURAL JOIN rm",
        "J-using-widen": "SELECT * FROM lk JOIN rw USING (s)",
        "J-on-widen": "SELECT * FROM lk JOIN rw ON lk.s = rw.s",
        "J-using-othernames": "SELECT * FROM lk JOIN rx USING (s)",
        "J-using-othernames-full": "SELECT * FROM lk FULL JOIN rx USING (s)",
        "J-using-samenames-full": "SELECT * FROM lk FULL JOIN rs USING (s)",
        "J-using-samenames": "SELECT * FROM lk JOIN rs USING (s)",
        "J-using-list": "SELECT * FROM la JOIN ra USING (s)",
        "J-on-list": "SELECT lid, rid FROM la JOIN ra ON la.s = ra.s",
        "J-using-int-full": "SELECT * FROM li FULL JOIN ri USING (k)",
        "J-using-sub": "SELECT lid, rid FROM (SELECT * FROM lk) x JOIN (SELECT * FROM rk) y "
        "USING (s)",
        "J-using-where": "SELECT lid, rid FROM lk JOIN rk USING (s) WHERE s = "
        "named_struct('a', 1, 'b', 2)",
        "J-using-group": "SELECT s, count(*) AS n FROM lk JOIN rk USING (s) GROUP BY s",
        "J-using-three": "SELECT * FROM lk JOIN rk USING (s) JOIN rs USING (s)",
        "J-lateral-using": "SELECT lid, rid FROM lk JOIN LATERAL (SELECT * FROM rk) y USING (s)",
        "J-setop-intersect": "SELECT s FROM lk INTERSECT SELECT s FROM rk",
        "J-setop-except": "SELECT s FROM lk EXCEPT SELECT s FROM rk",
        "F-nullif-AB": f"SELECT nullif({A}, {B}) AS x",
        "F-nullif-AC": f"SELECT nullif({A}, {C}) AS x",
        "F-nullif-col": "SELECT lid, rid FROM lk CROSS JOIN rk WHERE nullif(lk.s, rk.s) IS "
        "NULL AND lk.s IS NOT NULL",
        "F-greatest": f"SELECT greatest({A}, {B}) AS x",
        "F-least": f"SELECT least({A}, {B}) AS x",
        "F-greatest-col": "SELECT lid, rid, greatest(lk.s, rk.s) AS g FROM lk CROSS JOIN rk "
        "WHERE lid = 1 AND rid = 10",
        "F-case-simple-AB": f"SELECT CASE {A} WHEN {B} THEN 'hit' ELSE 'miss' END AS x",
        "F-case-simple-AC": f"SELECT CASE {A} WHEN {C} THEN 'hit' ELSE 'miss' END AS x",
        "F-case-simple-col": "SELECT lid, rid FROM lk CROSS JOIN rk WHERE CASE lk.s WHEN rk.s "
        "THEN true ELSE false END",
        "F-between-AB": f"SELECT {A} BETWEEN {B} AND {B} AS x",
        "F-between-AC": f"SELECT {A} BETWEEN {C} AND {C} AS x",
        "F-between-col": "SELECT lid, rid FROM lk CROSS JOIN rk WHERE lk.s BETWEEN rk.s AND rk.s",
        "F-equal-null": f"SELECT equal_null({A}, {B}) AS x, equal_null({A}, {C}) AS y",
        "F-decode": f"SELECT decode({A}, {B}, 'hit', 'miss') AS x, decode({A}, {C}, 'hit', "
        f"'miss') AS y",
        "F-array-contains-AB": f"SELECT array_contains(array({A}), {B}) AS x",
        "F-array-contains-AC": f"SELECT array_contains(array({A}), {C}) AS x",
        "F-array-contains-col": "SELECT lid, rid FROM la CROSS JOIN rk WHERE "
        "array_contains(la.s, rk.s)",
        "F-array-position": f"SELECT array_position(array({A}), {B}) AS x, "
        f"array_position(array({A}), {C}) AS y",
        "F-array-remove": f"SELECT array_remove(array({A}), {B}) AS x",
        "F-arrays-overlap": f"SELECT arrays_overlap(array({A}), array({B})) AS x, "
        f"arrays_overlap(array({A}), array({C})) AS y",
        "F-arrays-overlap-col": "SELECT lid, rid FROM la CROSS JOIN ra WHERE "
        "arrays_overlap(la.s, ra.s)",
        "F-array-union": f"SELECT array_union(array({A}), array({B})) AS x",
        "F-array-intersect": f"SELECT array_intersect(array({A}), array({B})) AS x",
        "F-array-except": f"SELECT array_except(array({A}), array({B})) AS x",
        "F-array-intersect-col": "SELECT lid, rid, array_intersect(la.s, ra.s) AS x FROM la "
        "CROSS JOIN ra",
        "F-map-contains-key": f"SELECT map_contains_key(map({A}, 1), {B}) AS x",
        "F-map-subscript": f"SELECT map({A}, 1)[{B}] AS x",
        "F-array-append": f"SELECT array_append(array({A}), {B}) AS x",
        "F-array-prepend": f"SELECT array_prepend(array({A}), {B}) AS x",
        "F-array-insert": f"SELECT array_insert(array({A}), 1, {B}) AS x",
        "F-concat-arrays": f"SELECT concat(array({A}), array({B})) AS x",
        "F-map-concat": f"SELECT map_concat(map({A}, 1), map({B}, 2)) AS x",
        "U-coalesce": f"SELECT coalesce({A}, {B}) AS x",
        "U-coalesce-col": "SELECT lid, rid, coalesce(lk.s, rk.s) AS x FROM lk CROSS JOIN rk "
        "WHERE lid = 3 AND rid = 10",
        "U-if": f"SELECT if(true, {A}, {B}) AS x",
        "U-nvl": f"SELECT nvl({A}, {B}) AS x",
        "U-nvl2": f"SELECT nvl2(1, {A}, {B}) AS x",
        "U-case-branches": f"SELECT CASE WHEN true THEN {A} ELSE {B} END AS x",
        "U-array-ctor": f"SELECT array({A}, {B}) AS x",
        "U-map-ctor": f"SELECT map({A}, 1, {B}, 2) AS x",
        "U-values": f"SELECT * FROM VALUES ({A}), ({B}) AS t(x)",
        "P-pivot": f"SELECT * FROM (SELECT s, lid FROM lk) PIVOT (sum(lid) FOR s IN ({B} AS hit))",
        "P-any-subquery": "SELECT lid FROM lk WHERE s = ANY (SELECT s FROM rk)",
        "P-agg-filter": f"SELECT count(*) FILTER (WHERE s = {B}) AS n FROM lk",
        "P-window-when": f"SELECT lid, sum(CASE WHEN s = {B} THEN 1 ELSE 0 END) OVER () AS n "
        f"FROM lk",
        "P-max-by": f"SELECT max_by(lid, s = {B}) AS x FROM lk WHERE s IS NOT NULL",
        "P-orderby-cmp": f"SELECT lid FROM lk WHERE s IS NOT NULL ORDER BY s = {B} DESC, lid",
        "P-groupby-cmp": f"SELECT s = {B} AS k, count(*) AS n FROM lk WHERE s IS NOT NULL "
        f"GROUP BY s = {B}",
        "P-array-sort-lambda": f"SELECT filter(array({A}), x -> x = {B}) AS x",
        "P-distinct-union": "SELECT count(*) AS n FROM (SELECT s FROM lk WHERE lid = 1 UNION "
        "SELECT s FROM rk WHERE rid = 10)",
        "I-using-where": "SELECT lid, rid FROM li JOIN ri USING (k) WHERE k = 1",
        "I-using-group": "SELECT k, count(*) AS n FROM li JOIN ri USING (k) GROUP BY k",
        "I-using-right-star": "SELECT * FROM li RIGHT JOIN ri USING (k)",
        "I-natural-full-star": "SELECT * FROM li NATURAL FULL JOIN ri",
        "I-using-full-key": "SELECT k FROM li FULL JOIN ri USING (k)",
        "I-using-inner-star": "SELECT * FROM li JOIN ri USING (k)",
        "T-values-typing": "SELECT * FROM VALUES (1, named_struct('a', 1, 'b', 2)) AS t(lid, s)",
        "V-view-using-star": "SELECT * FROM v_using",
        "V-view-using-key": "SELECT s FROM v_using",
    }
)

VIEWS = [
    "CREATE OR REPLACE TEMPORARY VIEW v_using AS SELECT * FROM lk JOIN rk USING (s)",
]


def plain(value: Any, data_type: Any) -> Any:
    if value is None:
        return None
    kind = type(data_type).__name__
    if kind == "StructType":
        items = list(value.values()) if isinstance(value, dict) else list(value)
        return [
            plain(item, field.dataType)
            for item, field in zip(items, data_type.fields, strict=False)
        ]
    if kind == "ArrayType":
        return [plain(item, data_type.elementType) for item in value]
    if kind == "MapType":
        return sorted(
            (
                [plain(k, data_type.keyType), plain(v, data_type.valueType)]
                for k, v in value.items()
            ),
            key=repr,
        )
    return value


def record(frame: Any) -> Any:
    schema = frame.schema
    rows = sorted((plain(row, schema) for row in frame.collect()), key=repr)
    return {"status": "ok", "rows": rows, "schema": schema.simpleString()}


def failure(error: Any) -> Any:
    text = str(error)
    found = CLASS.search(text)
    return {
        "status": "error",
        "class": found.group(1) if found else type(error).__name__,
        "text": " ".join(text.split())[:220],
    }


def session() -> Any:
    if LEG == "spark":
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("cmpmemo")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def frame_cells(spark: Any) -> Any:
    row_left, r = spark.table("lk"), spark.table("rk")
    li, ri = spark.table("li"), spark.table("ri")
    lm, rm = spark.table("lm"), spark.table("rm")
    l2, r2 = spark.table("l2"), spark.table("r2")
    cells = {}
    for how in ["inner", "left", "right", "full", "left_semi", "left_anti"]:
        cells[f"D-str-{how}"] = lambda how=how: row_left.join(r, "s", how)
    cells["D-list-inner"] = lambda: row_left.join(r, ["s"])
    cells["D-list-mixed"] = lambda: lm.join(rm, ["k", "s"])
    cells["D-list-two"] = lambda: l2.join(r2, ["s", "u"])
    cells["D-cond-inner"] = lambda: row_left.join(r, row_left.s == r.s)
    cells["D-cond-full"] = lambda: row_left.join(r, row_left.s == r.s, "full")
    cells["D-str-inner-select-key"] = lambda: row_left.join(r, "s").select("s")
    cells["D-str-inner-filter"] = lambda: (
        row_left.join(r, "s").filter("lid > 0").select("lid", "rid")
    )
    cells["D-str-int-full"] = lambda: li.join(ri, "k", "full")
    cells["D-str-int-right"] = lambda: li.join(ri, "k", "right")
    cells["D-sql-using-then-select"] = lambda: spark.sql(
        "SELECT * FROM lk JOIN rk USING (s)"
    ).select("s", "lid")
    cells["D-sql-using-then-filter"] = lambda: spark.sql(
        "SELECT * FROM lk JOIN rk USING (s)"
    ).filter("lid > 0")
    cells["D-sql-using-cache"] = lambda: (
        spark.sql("SELECT * FROM lk JOIN rk USING (s)").cache().select("s", "lid")
    )
    cells["D-str-then-join-again"] = lambda: row_left.join(r, "s").join(
        spark.table("rs").withColumnRenamed("rid", "rid2"), "s"
    )
    return cells


def main() -> Any:
    spark = session()
    for statement in SETUP:
        spark.sql(statement)
    results = {}
    for statement in VIEWS:
        try:
            spark.sql(statement)
        except Exception as error:
            results["V-create:" + statement[:50]] = failure(error)
    for key, statement in SQL.items():
        try:
            results[key] = record(spark.sql(statement))
        except Exception as error:
            results[key] = failure(error)
    for key, build in frame_cells(spark).items():
        try:
            results[key] = record(build())
        except Exception as error:
            results[key] = failure(error)
    with Path(OUT).open("w") as handle:
        for key, value in results.items():
            handle.write(json.dumps({"key": key, **value}, default=str) + "\n")
    spark.stop()


main()

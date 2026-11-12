from __future__ import annotations

from typing import Any

TYPES = {
    "double": {"k": "d", "k2": "e", "sql": "DOUBLE"},
    "float": {"k": "f", "k2": "fe", "sql": "FLOAT"},
    "decimal": {"k": "m", "k2": "me", "sql": "DECIMAL(10,2)"},
}
COLUMNS_T = (
    "id BIGINT, g INT, d DOUBLE, e DOUBLE, f FLOAT, fe FLOAT, "
    "m DECIMAL(10,2), me DECIMAL(10,2), flag STRING"
)
COLUMNS_U = (
    "uid BIGINT, g INT, d DOUBLE, e DOUBLE, f FLOAT, fe FLOAT, "
    "m DECIMAL(10,2), me DECIMAL(10,2), flag STRING"
)
NAMES_T = ("id", "g", "d", "e", "f", "fe", "m", "me", "flag")
NAMES_U = ("uid", "g", "d", "e", "f", "fe", "m", "me", "flag")


def lit_d(value: str | None) -> str:
    return "CAST(NULL AS DOUBLE)" if value is None else f"CAST('{value}' AS DOUBLE)"


def lit_f(value: str | None) -> str:
    return "CAST(NULL AS FLOAT)" if value is None else f"CAST('{value}' AS FLOAT)"


def lit_m(value: str | None) -> str:
    if value is None:
        return "CAST(NULL AS DECIMAL(10,2))"
    if value == "NaN":
        value = "9.99"
    return f"CAST('{value}' AS DECIMAL(10,2))"


def row(ident: int, group: int, d: str | None, e: str | None, flag: str) -> str:
    return (
        f"(CAST({ident} AS BIGINT), {group}, {lit_d(d)}, {lit_d(e)}, {lit_f(d)}, {lit_f(e)}, "
        f"{lit_m(d)}, {lit_m(e)}, '{flag}')"
    )


DATA = {
    "t": (
        row(1, 1, "0.0", "-0.0", "a1"),
        row(2, 0, "-0.0", "0.0", "a2"),
        row(3, 1, "0.0", "0.0", "a3"),
        row(4, 0, "-0.0", "-0.0", "a4"),
        row(5, 1, "NaN", "NaN", "a5"),
        row(6, 0, "1.5", "1.5", "a6"),
        row(7, 1, None, "0.0", "a7"),
    ),
    "up": (row(10, 0, "0.0", "0.0", "u0"), row(12, 1, "NaN", "NaN", "un")),
    "un": (row(11, 0, "-0.0", "-0.0", "u1"), row(13, 1, "NaN", "NaN", "un")),
    "ub": (row(10, 0, "0.0", "0.0", "u0"), row(11, 0, "-0.0", "-0.0", "u1")),
}
NN_ROWS = (
    "(CAST(1 AS BIGINT), CAST('NaN' AS DOUBLE))",
    "(CAST(2 AS BIGINT), sqrt(CAST(-1.0 AS DOUBLE)))",
    "(CAST(4 AS BIGINT), CAST('1.0' AS DOUBLE))",
    "(CAST(5 AS BIGINT), CAST('0.0' AS DOUBLE))",
    "(CAST(6 AS BIGINT), CAST('-0.0' AS DOUBLE))",
    "(CAST(7 AS BIGINT), -CAST('NaN' AS DOUBLE))",
)

JOIN_KINDS = (
    ("inner", "JOIN", "inner", "t.id, u.uid", "'id', 'uid'"),
    ("left", "LEFT OUTER JOIN", "left", "t.id, u.uid", "'id', 'uid'"),
    ("right", "RIGHT OUTER JOIN", "right", "t.id, u.uid", "'id', 'uid'"),
    ("full", "FULL OUTER JOIN", "full", "t.id, u.uid", "'id', 'uid'"),
    ("semi", "LEFT SEMI JOIN", "left_semi", "t.id", "'id'"),
    ("anti", "LEFT ANTI JOIN", "left_anti", "t.id", "'id'"),
)


def cell(
    site: str,
    group: str,
    sql: str,
    *,
    native: str | None = "same",
    fexpr: str | None = None,
    typed: str | None = None,
    ordered: bool = False,
    builds: tuple[str | None, ...] = (None,),
    types: tuple[str, ...] = ("double", "float", "decimal"),
    pre: dict[str, tuple[str, ...]] | None = None,
    post: dict[str, tuple[str, ...]] | None = None,
    seat: str = "",
) -> dict[str, Any]:
    return {
        "site": site,
        "group": group,
        "sql": sql,
        "native": sql if native == "same" else native,
        "fexpr": fexpr,
        "typed": typed,
        "ordered": ordered,
        "builds": builds,
        "types": types,
        "pre": pre or {},
        "post": post or {},
        "seat": seat,
    }


COUNT_C = "F.count(F.lit(1)).alias('c')"
BOTH_BUILDS = ("up", "un")


def read_specs() -> list[dict[str, Any]]:
    specs = [
        cell(
            "GB-1",
            "group-by",
            "SELECT {k}, count(*) AS c FROM {t} GROUP BY {k}",
            fexpr="t.groupBy(F.expr('{k}')).agg(" + COUNT_C + ")",
            typed="t.groupBy('{k}').agg(" + COUNT_C + ")",
            seat="Aggregate.group_expr, column key",
        ),
        cell(
            "GB-2",
            "group-by",
            "SELECT {k} * 1 AS x, count(*) AS c FROM {t} GROUP BY {k} * 1",
            fexpr="t.groupBy(F.expr('{k} * 1').alias('x')).agg(" + COUNT_C + ")",
            typed="t.groupBy((F.col('{k}') * 1).alias('x')).agg(" + COUNT_C + ")",
            seat="Aggregate.group_expr, computed key",
        ),
        cell(
            "GB-3",
            "group-by",
            "SELECT {k}, g, count(*) AS c FROM {t} GROUP BY {k}, g",
            fexpr="t.groupBy(F.expr('{k}'), F.expr('g')).agg(" + COUNT_C + ")",
            typed="t.groupBy('{k}', 'g').agg(" + COUNT_C + ")",
            seat="Aggregate.group_expr, key list",
        ),
        cell(
            "GB-4",
            "group-by",
            "SELECT {k}, count(*) AS c FROM {t} GROUP BY ROLLUP({k})",
            typed="t.rollup('{k}').agg(" + COUNT_C + ")",
            seat="GroupingSet ROLLUP member",
        ),
        cell(
            "GB-5",
            "group-by",
            "SELECT {k}, g, count(*) AS c FROM {t} GROUP BY CUBE({k}, g)",
            typed="t.cube('{k}', 'g').agg(" + COUNT_C + ")",
            seat="GroupingSet CUBE member",
        ),
        cell(
            "GB-6",
            "group-by",
            "SELECT {k}, g, count(*) AS c FROM {t} GROUP BY GROUPING SETS (({k}), (g))",
            seat="GroupingSet GROUPING SETS member",
        ),
        cell(
            "GB-7",
            "group-by",
            "SELECT {k}, count(*) AS c FROM {t} GROUP BY {k} HAVING count(*) > 1",
            seat="Aggregate.group_expr under HAVING",
        ),
        cell(
            "GB-8",
            "group-by",
            "SELECT g, {k} FROM {t} GROUP BY g, {k} ORDER BY g, {k}",
            ordered=False,
            seat="group output value (normalised key output)",
        ),
        cell(
            "GB-9",
            "group-by",
            "SELECT count(*) AS c FROM (SELECT {k} FROM {t} GROUP BY {k}) AS x",
            seat="group count through a subquery",
        ),
        cell(
            "GB-10",
            "group-by",
            "SELECT {k}, count(*) AS c FROM (SELECT {k} FROM {t} DISTRIBUTE BY {k}) AS x GROUP BY {k}",
            native="SELECT {k}, count(*) AS c FROM (SELECT {k} FROM {t}) AS x GROUP BY {k}",
            typed="t.repartition(4, '{k}').groupBy('{k}').agg(" + COUNT_C + ")",
            seat="Repartition Hash then Aggregate",
        ),
        cell(
            "GB-11",
            "group-by",
            "SELECT g, count(*) AS c FROM {t} GROUP BY g",
            typed="t.groupBy('g').pivot('{k}').agg(F.count(F.lit(1)))",
            seat="pivot value discovery (typed only; SQL is the integer control)",
        ),
        cell(
            "DI-1",
            "distinct",
            "SELECT DISTINCT {k} FROM {t}",
            fexpr="t.select(F.expr('{k}')).distinct()",
            typed="t.select('{k}').distinct()",
            seat="Distinct::All",
        ),
        cell(
            "DI-2",
            "distinct",
            "SELECT DISTINCT {k}, g FROM {t}",
            typed="t.select('{k}', 'g').distinct()",
            seat="Distinct::All, two columns",
        ),
        cell(
            "DI-3",
            "distinct",
            "SELECT count(DISTINCT {k}) AS c FROM {t}",
            fexpr="t.agg(F.expr('count(DISTINCT {k})').alias('c'))",
            typed="t.agg(F.countDistinct('{k}').alias('c'))",
            seat="distinct aggregate, single (single_distinct_to_groupby)",
        ),
        cell(
            "DI-4",
            "distinct",
            "SELECT count(DISTINCT {k}) AS a, count(DISTINCT g) AS b FROM {t}",
            typed="t.agg(F.countDistinct('{k}').alias('a'), F.countDistinct('g').alias('b'))",
            seat="distinct aggregate, multi-distinct path",
        ),
        cell(
            "DI-5",
            "distinct",
            "SELECT g, count(DISTINCT {k}) AS c FROM {t} GROUP BY g",
            typed="t.groupBy('g').agg(F.countDistinct('{k}').alias('c'))",
            seat="distinct aggregate under a group key",
        ),
        cell(
            "DI-6",
            "distinct",
            "SELECT {k} FROM {t} GROUP BY {k}",
            native=None,
            typed="t.dropDuplicates(['{k}']).select('{k}')",
            seat="dropDuplicates (typed only)",
        ),
        cell(
            "DI-7",
            "distinct",
            "SELECT g, sum(DISTINCT {k}) AS s FROM {t} WHERE id <> 5 GROUP BY g",
            seat="distinct aggregate argument, sum",
        ),
        cell(
            "WI-1",
            "window",
            "SELECT id, count(*) OVER (PARTITION BY {k}) AS c FROM {t}",
            fexpr="t.select('id', F.expr('count(*) OVER (PARTITION BY {k})').alias('c'))",
            typed="t.select('id', F.count(F.lit(1)).over(W.partitionBy('{k}')).alias('c'))",
            seat="Window.partition_by",
        ),
        cell(
            "WI-2",
            "window",
            "SELECT id, rank() OVER (ORDER BY {k}) AS r FROM {t}",
            fexpr="t.select('id', F.expr('rank() OVER (ORDER BY {k})').alias('r'))",
            typed="t.select('id', F.rank().over(W.orderBy('{k}')).alias('r'))",
            seat="Window.order_by, rank ties",
        ),
        cell(
            "WI-3",
            "window",
            "SELECT id, dense_rank() OVER (ORDER BY {k}) AS r FROM {t}",
            typed="t.select('id', F.dense_rank().over(W.orderBy('{k}')).alias('r'))",
            seat="Window.order_by, dense_rank ties",
        ),
        cell(
            "WI-4",
            "window",
            "SELECT id, count(*) OVER (ORDER BY {k} RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS c FROM {t}",
            typed=(
                "t.select('id', F.count(F.lit(1)).over(W.orderBy('{k}').rangeBetween(0, 0))"
                ".alias('c'))"
            ),
            seat="Window.order_by, RANGE peers",
        ),
        cell(
            "WI-5",
            "window",
            "SELECT id, row_number() OVER (ORDER BY {k}, id) AS r FROM {t}",
            typed="t.select('id', F.row_number().over(W.orderBy('{k}', 'id')).alias('r'))",
            seat="Window.order_by with a tie-breaker",
        ),
        cell(
            "WI-6",
            "window",
            "SELECT id, row_number() OVER (PARTITION BY {k} ORDER BY id) AS r FROM {t}",
            typed="t.select('id', F.row_number().over(W.partitionBy('{k}').orderBy('id')).alias('r'))",
            seat="Window.partition_by with ordered frame",
        ),
        cell(
            "OB-1",
            "order-by",
            "SELECT id, {k} FROM {t} ORDER BY {k}, id",
            fexpr="t.select('id', '{k}').orderBy(F.expr('{k}'), F.expr('id'))",
            typed="t.select('id', '{k}').orderBy('{k}', 'id')",
            ordered=True,
            seat="Sort with a tie-breaker",
        ),
        cell(
            "OB-2",
            "order-by",
            "SELECT id FROM {t} ORDER BY {k}, id LIMIT 2",
            typed="t.orderBy('{k}', 'id').select('id').limit(2)",
            ordered=True,
            seat="TopK (Sort + Limit)",
        ),
        cell(
            "OB-3",
            "order-by",
            "SELECT id FROM {t} ORDER BY {k} DESC NULLS LAST, id",
            typed="t.orderBy(F.col('{k}').desc_nulls_last(), 'id').select('id')",
            ordered=True,
            seat="Sort descending",
        ),
        cell(
            "OB-4",
            "order-by",
            "SELECT {k} FROM {t} WHERE id <= 4 ORDER BY {k}",
            ordered=True,
            seat="Sort, zero ties only (declared ORDER-DIFFER)",
        ),
        cell(
            "MM-1",
            "min-max",
            "SELECT min({k}) AS lo, max({k}) AS hi FROM {t} WHERE id <= 4",
            typed="t.filter(F.col('id') <= 4).agg(F.min('{k}').alias('lo'), F.max('{k}').alias('hi'))",
            seat="min/max of zeros (declared SIGN-ONLY)",
        ),
        cell(
            "CM-1",
            "compare",
            "SELECT id FROM {t} WHERE {k} = {k2}",
            fexpr="t.filter(F.expr('{k} = {k2}')).select('id')",
            typed="t.filter(F.col('{k}') == F.col('{k2}')).select('id')",
            seat="non-literal BinaryExpr =",
        ),
        cell(
            "CM-2",
            "compare",
            "SELECT id FROM {t} WHERE {k} <> {k2}",
            fexpr="t.filter(F.expr('{k} <> {k2}')).select('id')",
            typed="t.filter(F.col('{k}') != F.col('{k2}')).select('id')",
            seat="non-literal BinaryExpr <>",
        ),
        cell(
            "CM-3",
            "compare",
            "SELECT id FROM {t} WHERE {k} < {k2}",
            fexpr="t.filter(F.expr('{k} < {k2}')).select('id')",
            typed="t.filter(F.col('{k}') < F.col('{k2}')).select('id')",
            seat="non-literal BinaryExpr <",
        ),
        cell(
            "CM-4",
            "compare",
            "SELECT id FROM {t} WHERE {k} <= {k2}",
            typed="t.filter(F.col('{k}') <= F.col('{k2}')).select('id')",
            seat="non-literal BinaryExpr <=",
        ),
        cell(
            "CM-5",
            "compare",
            "SELECT id FROM {t} WHERE {k} <=> {k2}",
            native="SELECT id FROM {t} WHERE {k} IS NOT DISTINCT FROM {k2}",
            typed="t.filter(F.col('{k}').eqNullSafe(F.col('{k2}'))).select('id')",
            seat="non-literal <=>",
        ),
        cell(
            "CM-6",
            "compare",
            "SELECT id FROM {t} WHERE {k} BETWEEN {k2} AND {k2}",
            typed="t.filter(F.col('{k}').between(F.col('{k2}'), F.col('{k2}'))).select('id')",
            seat="Between, non-literal bounds",
        ),
        cell(
            "CM-7",
            "compare",
            "SELECT id FROM {t} WHERE {k} IN ({k2}, 1.5)",
            typed="t.filter(F.col('{k}').isin(F.col('{k2}'), 1.5)).select('id')",
            seat="InList, non-literal item",
        ),
        cell(
            "CM-8",
            "compare",
            "SELECT id, CASE {k} WHEN {k2} THEN 'z' ELSE 'n' END AS c FROM {t}",
            fexpr="t.select('id', F.expr(\"CASE {k} WHEN {k2} THEN 'z' ELSE 'n' END\").alias('c'))",
            seat="simple CASE, non-literal WHEN (D8)",
        ),
        cell(
            "CM-9",
            "compare",
            "SELECT id, CASE {k} WHEN 0.0 THEN 'z' ELSE 'n' END AS c FROM {t}",
            fexpr="t.select('id', F.expr(\"CASE {k} WHEN 0.0 THEN 'z' ELSE 'n' END\").alias('c'))",
            seat="simple CASE, zero literal WHEN (part A control)",
        ),
        cell(
            "CM-10",
            "compare",
            "SELECT id, ({k} = {k2}) AS x FROM {t}",
            typed="t.select('id', (F.col('{k}') == F.col('{k2}')).alias('x'))",
            seat="non-literal = in a projection",
        ),
        cell(
            "CM-11",
            "compare",
            "SELECT id FROM {t} WHERE {k} IS DISTINCT FROM {k2}",
            typed="t.filter(~F.col('{k}').eqNullSafe(F.col('{k2}'))).select('id')",
            seat="non-literal IS DISTINCT FROM",
        ),
    ]
    for name, sql_kind, how, sql_cols, frame_cols in JOIN_KINDS:
        specs.append(
            cell(
                f"JN-{name}",
                "join",
                f"SELECT {sql_cols} FROM {{t}} AS t {sql_kind} {{u}} AS u ON t.{{k}} = u.{{k}}",
                fexpr=(
                    f"t.alias('a').join(u.alias('b'), F.expr('a.{{k}} = b.{{k}}'), '{how}')"
                    f".select({frame_cols})"
                ),
                typed=f"t.join(u, t['{{k}}'] == u['{{k}}'], '{how}').select({frame_cols})",
                builds=BOTH_BUILDS,
                seat=f"Join::on, {name}",
            )
        )
    specs += [
        cell(
            "JN-using",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u USING ({k})",
            typed="t.join(u, '{k}').select('id', 'uid')",
            builds=BOTH_BUILDS,
            seat="Join::on, USING",
        ),
        cell(
            "JN-keylist",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} = u.{k} AND t.g = u.g",
            typed="t.join(u, (t['{k}'] == u['{k}']) & (t['g'] == u['g'])).select('id', 'uid')",
            builds=BOTH_BUILDS,
            seat="Join::on, key list (float + int)",
        ),
        cell(
            "JN-twofloat",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} = u.{k} AND t.{k2} = u.{k2}",
            builds=BOTH_BUILDS,
            seat="Join::on, two float keys",
        ),
        cell(
            "JN-nullsafe",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} <=> u.{k}",
            native="SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} IS NOT DISTINCT FROM u.{k}",
            typed="t.join(u, t['{k}'].eqNullSafe(u['{k}'])).select('id', 'uid')",
            builds=BOTH_BUILDS,
            seat="Join::on, null-safe",
        ),
        cell(
            "JN-comma",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t, {u} AS u WHERE t.{k} = u.{k}",
            builds=BOTH_BUILDS,
            seat="Join::on via eliminate_cross_join",
        ),
        cell(
            "JN-sortmerge",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} = u.{k}",
            builds=BOTH_BUILDS,
            pre={
                "spark": (
                    "SET spark.sql.join.preferSortMergeJoin = true",
                    "SET spark.sql.autoBroadcastJoinThreshold = -1",
                ),
                "repark": ("SET datafusion.optimizer.prefer_hash_join = false",),
            },
            post={
                "spark": (
                    "SET spark.sql.join.preferSortMergeJoin = true",
                    "SET spark.sql.autoBroadcastJoinThreshold = 10485760",
                ),
                "repark": ("SET datafusion.optimizer.prefer_hash_join = true",),
            },
            seat="Join::on, sort-merge physical path",
        ),
        cell(
            "JN-nonequi",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} < u.{k}",
            builds=BOTH_BUILDS,
            seat="Join::filter, non-literal <",
        ),
        cell(
            "JN-filter",
            "join",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.g = u.g AND t.{k} = u.{k2}",
            builds=BOTH_BUILDS,
            seat="Join::on second key across columns",
        ),
        cell(
            "SQ-in",
            "subquery",
            "SELECT id FROM {t} WHERE {k} IN (SELECT {k} FROM {u})",
            builds=BOTH_BUILDS,
            seat="InSubquery -> LeftSemi",
        ),
        cell(
            "SQ-notin",
            "subquery",
            "SELECT id FROM {t} WHERE {k} NOT IN (SELECT {k} FROM {u})",
            builds=BOTH_BUILDS,
            seat="NOT IN subquery -> null-aware LeftAnti",
        ),
        cell(
            "SQ-notin-ieee",
            "subquery",
            "SELECT id FROM {t} WHERE {k} NOT IN (SELECT {k} FROM {u})",
            builds=BOTH_BUILDS,
            pre={"spark": ("SET spark.sql.optimizeNullAwareAntiJoin = false",)},
            post={"spark": ("SET spark.sql.optimizeNullAwareAntiJoin = true",)},
            seat="NOT IN subquery, Spark truth with the null-aware anti join off (Q-NAAJ)",
        ),
        cell(
            "SQ-exists",
            "subquery",
            "SELECT id FROM {t} AS t WHERE EXISTS (SELECT 1 FROM {u} AS u WHERE u.{k} = t.{k})",
            builds=BOTH_BUILDS,
            seat="EXISTS -> LeftSemi",
        ),
        cell(
            "SQ-notexists",
            "subquery",
            "SELECT id FROM {t} AS t WHERE NOT EXISTS (SELECT 1 FROM {u} AS u WHERE u.{k} = t.{k})",
            builds=BOTH_BUILDS,
            seat="NOT EXISTS -> LeftAnti",
        ),
        cell(
            "SQ-scalar",
            "subquery",
            "SELECT id FROM {t} WHERE {k} = (SELECT min({k}) FROM {u})",
            builds=BOTH_BUILDS,
            seat="scalar subquery -> join / filter",
        ),
        cell(
            "SQ-any",
            "subquery",
            "SELECT id FROM {t} WHERE {k} = ANY (SELECT {k} FROM {u})",
            builds=BOTH_BUILDS,
            seat="= ANY (SetComparison)",
        ),
        cell(
            "SO-union",
            "set-op",
            "SELECT {k} FROM {t} UNION SELECT {k} FROM {u}",
            typed="t.select('{k}').union(u.select('{k}')).distinct()",
            builds=BOTH_BUILDS,
            seat="UNION distinct",
        ),
        cell(
            "SO-unionall",
            "set-op",
            "SELECT {k} FROM {t} UNION ALL SELECT {k} FROM {u}",
            typed="t.select('{k}').union(u.select('{k}'))",
            builds=BOTH_BUILDS,
            seat="UNION ALL (control: no dedup)",
        ),
        cell(
            "SO-intersect",
            "set-op",
            "SELECT {k} FROM {t} INTERSECT SELECT {k} FROM {u}",
            typed="t.select('{k}').intersect(u.select('{k}'))",
            builds=BOTH_BUILDS,
            seat="INTERSECT (LeftSemi + dedup)",
        ),
        cell(
            "SO-intersectall",
            "set-op",
            "SELECT {k} FROM {t} INTERSECT ALL SELECT {k} FROM {u}",
            typed="t.select('{k}').intersectAll(u.select('{k}'))",
            builds=BOTH_BUILDS,
            seat="INTERSECT ALL",
        ),
        cell(
            "SO-except",
            "set-op",
            "SELECT {k} FROM {t} EXCEPT SELECT {k} FROM {u}",
            typed="t.select('{k}').subtract(u.select('{k}'))",
            builds=BOTH_BUILDS,
            seat="EXCEPT (LeftAnti + dedup)",
        ),
        cell(
            "SO-exceptall",
            "set-op",
            "SELECT {k} FROM {t} EXCEPT ALL SELECT {k} FROM {u}",
            typed="t.select('{k}').exceptAll(u.select('{k}'))",
            builds=BOTH_BUILDS,
            seat="EXCEPT ALL",
        ),
        cell(
            "SO-union-in",
            "set-op",
            "SELECT {k} FROM {u} UNION SELECT {k} FROM {u}",
            builds=("ub",),
            seat="UNION distinct of a both-sign input",
        ),
        cell(
            "SO-recursive",
            "set-op",
            "WITH RECURSIVE r(n) AS (SELECT CAST('-0.0' AS DOUBLE) UNION SELECT n * 0 FROM r WHERE n < 1) SELECT n FROM r",
            types=("double",),
            seat="RecursiveQuery is_distinct",
        ),
        cell(
            "PR-1",
            "prepared",
            "SELECT {k}, count(*) AS c FROM {t} WHERE {k} >= -1.0 GROUP BY {k}",
            native="EXECUTE szbp(-1.0)",
            types=("double", "float"),
            pre={"repark": ("PREPARE szbp({ksql}) AS SELECT {k}, count(*) AS c FROM {t} WHERE {k} >= $1 GROUP BY {k}",)},
            post={"repark": ("DEALLOCATE szbp",)},
            seat="native PREPARE / EXECUTE, GROUP BY (seat-2 idempotence across two optimizer runs)",
        ),
        cell(
            "PR-2",
            "prepared",
            "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} = u.{k} WHERE t.{k} >= -1.0",
            native="EXECUTE szbq(-1.0)",
            types=("double", "float"),
            builds=BOTH_BUILDS,
            pre={"repark": ("PREPARE szbq({ksql}) AS SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.{k} = u.{k} WHERE t.{k} >= $1",)},
            post={"repark": ("DEALLOCATE szbq",)},
            seat="native PREPARE / EXECUTE, join key",
        ),
        cell(
            "CM-12",
            "compare",
            "SELECT g, count(*) FILTER (WHERE {k} = {k2}) AS c FROM {t} GROUP BY g",
            seat="aggregate FILTER clause, non-literal =",
        ),
        cell(
            "JN-natural",
            "join",
            "SELECT id, uid FROM (SELECT id, {k} FROM {t}) AS a NATURAL JOIN (SELECT uid, {k} FROM {u}) AS b",
            builds=BOTH_BUILDS,
            seat="Join::on, NATURAL",
        ),
        cell(
            "TV-1",
            "temp-view",
            "SELECT {k}, count(*) AS c FROM vtk GROUP BY {k}",
            pre={"both": ("CREATE OR REPLACE TEMPORARY VIEW vtk AS SELECT * FROM {t}",)},
            post={"both": ("DROP VIEW IF EXISTS vtk",)},
            seat="GROUP BY over a temporary view",
        ),
    ]
    return specs


NN_SPECS = (
    ("NN-1", "SELECT d, count(*) AS c FROM {nn} GROUP BY d", False),
    ("NN-2", "SELECT DISTINCT d FROM {nn}", False),
    ("NN-3", "SELECT a.id, b.id FROM {nn} AS a JOIN {nn} AS b ON a.d = b.d", False),
    ("NN-4", "SELECT id, d FROM {nn} ORDER BY d, id", True),
    ("NN-5", "SELECT id, rank() OVER (ORDER BY d) AS r FROM {nn}", False),
    ("NN-6", "SELECT count(DISTINCT d) AS c FROM {nn}", False),
)

DML_SPECS = (
    (
        "WR-merge-on",
        "MERGE INTO {t} AS t USING {u} AS u ON t.{k} = u.{k} WHEN MATCHED THEN UPDATE SET flag = 'z'",
        BOTH_BUILDS,
        "merge",
    ),
    (
        "WR-merge-card",
        "MERGE INTO {t} AS t USING {u} AS u ON t.{k} = u.{k} WHEN MATCHED THEN UPDATE SET flag = 'z'",
        ("ub",),
        "merge",
    ),
    (
        "WR-merge-notmatched",
        "MERGE INTO {t} AS t USING {u} AS u ON t.{k} = u.{k} AND t.g = u.g "
        "WHEN NOT MATCHED THEN INSERT (id, g, d, e, f, fe, m, me, flag) "
        "VALUES (u.uid, u.g, u.d, u.e, u.f, u.fe, u.m, u.me, 'ins')",
        BOTH_BUILDS,
        "merge",
    ),
    (
        "WR-delete-in",
        "DELETE FROM {t} WHERE {k} IN (SELECT {k} FROM {u})",
        BOTH_BUILDS,
        "delete",
    ),
    (
        "WR-delete-exists",
        "DELETE FROM {t} AS t WHERE EXISTS (SELECT 1 FROM {u} AS u WHERE u.{k} = t.{k})",
        BOTH_BUILDS,
        "delete",
    ),
    (
        "WR-update-in",
        "UPDATE {t} SET flag = 'z' WHERE {k} IN (SELECT {k} FROM {u})",
        BOTH_BUILDS,
        "update",
    ),
    (
        "WR-insert-notin-ieee",
        "INSERT INTO {t} SELECT uid + 100, g, d, e, f, fe, m, me, 'ins' FROM {u} "
        "WHERE {k} NOT IN (SELECT {k} FROM {t} WHERE id IN (1, 3))",
        BOTH_BUILDS,
        "insert",
    ),
    (
        "WR-ctas-group",
        "CREATE TABLE {x} AS SELECT {k}, count(*) AS c FROM {t} GROUP BY {k}",
        (None,),
        "ctas",
    ),
    (
        "WR-ctas-distinct",
        "CREATE TABLE {x} AS SELECT DISTINCT {k} FROM {t}",
        (None,),
        "ctas",
    ),
    (
        "WR-insert-notin",
        "INSERT INTO {t} SELECT uid + 100, g, d, e, f, fe, m, me, 'ins' FROM {u} "
        "WHERE {k} NOT IN (SELECT {k} FROM {t} WHERE id IN (1, 3))",
        BOTH_BUILDS,
        "insert",
    ),
)
DML_KINDS = ("v2-cow", "v2-mor", "v3")

FLOAT_SHAPES = (
    "K1-group-by",
    "K2-distinct",
    "K3-count-distinct",
    "K4-inner-join",
    "K5-left-semi",
    "K6-left-anti",
    "K7-full-outer",
    "K8-window-partition",
    "K9-intersect",
    "K10-except",
    "K11-union-distinct",
    "K12-in-subquery",
    "W1-merge-on",
    "W2-delete-in",
)
FLOAT_WRITE_SHAPES = frozenset({"W1-merge-on", "W2-delete-in"})
FLOAT_SQL = {
    "K1-group-by": "SELECT d, count(*) AS n FROM {t} GROUP BY d",
    "K2-distinct": "SELECT DISTINCT d FROM {t}",
    "K3-count-distinct": "SELECT count(DISTINCT d) AS n FROM {t}",
    "K4-inner-join": "SELECT t.id, u.id FROM {t} AS t JOIN {u} AS u ON t.d = u.d",
    "K5-left-semi": "SELECT t.id FROM {t} AS t LEFT SEMI JOIN {u} AS u ON t.d = u.d",
    "K6-left-anti": "SELECT t.id FROM {t} AS t LEFT ANTI JOIN {u} AS u ON t.d = u.d",
    "K7-full-outer": "SELECT t.id, u.id FROM {t} AS t FULL OUTER JOIN {u} AS u ON t.d = u.d",
    "K8-window-partition": "SELECT id, rank() OVER (PARTITION BY d ORDER BY id) AS r FROM {t}",
    "K9-intersect": "SELECT d FROM {t} INTERSECT SELECT d FROM {u}",
    "K10-except": "SELECT d FROM {t} EXCEPT SELECT d FROM {u}",
    "K11-union-distinct": "SELECT d FROM {t} UNION SELECT d FROM {u}",
    "K12-in-subquery": "SELECT id FROM {t} WHERE d IN (SELECT d FROM {u})",
    "W1-merge-on": "MERGE INTO {t} AS t USING {u} AS s ON t.d = s.d WHEN MATCHED THEN UPDATE SET flag = 'z'",
    "W2-delete-in": "DELETE FROM {t} WHERE d IN (SELECT d FROM {u})",
}
NEGATIVE_ZERO = "CAST('-0.0' AS DOUBLE)"
FLOAT_SEED_T = (
    "(1, CAST('0.0' AS DOUBLE), NULL)",
    f"(2, {NEGATIVE_ZERO}, NULL)",
    "(3, CAST('1.5' AS DOUBLE), NULL)",
    "(4, CAST('0.0' AS DOUBLE), NULL)",
)
FLOAT_SEED_U = (f"(10, {NEGATIVE_ZERO}, NULL)",)
FLOAT_SEED_BARE_T = ("(1, 0.0, NULL)", "(2, -0.0, NULL)", "(3, 1.5, NULL)", "(4, 0.0, NULL)")
FLOAT_SEED_BARE_U = ("(10, -0.0, NULL)",)
FLOAT_SEED_VARIANTS = {
    "matrix": (FLOAT_SEED_T, FLOAT_SEED_U),
    "bare": (FLOAT_SEED_BARE_T, FLOAT_SEED_BARE_U),
    "signed": (
        (
            "(1, CAST('0.0' AS {ty}), NULL)",
            "(2, CAST('-0.0' AS {ty}), NULL)",
            "(3, CAST('1.5' AS {ty}), NULL)",
            "(4, CAST('0.0' AS {ty}), NULL)",
        ),
        ("(10, CAST('-0.0' AS {ty}), NULL)",),
    ),
}

PART_SPECS = (
    ("PT-1", "SELECT id, {k} FROM {t}", "stored value read back through an identity partition"),
    ("PT-2", "SELECT {k}, count(*) AS c FROM {t} GROUP BY {k}", "GROUP BY on the identity partition column"),
    ("PT-3", "SELECT count(*) AS n FROM {t}.partitions", "partition count (write-side partition key)"),
    ("PT-4", "SELECT count(DISTINCT {k}) AS n FROM {t}", "count(DISTINCT) on the partition column"),
)

from __future__ import annotations

V2 = " USING iceberg TBLPROPERTIES ('format-version'='2')"

WIDE_COLUMNS = (
    "id INT, v INT, i INT, l BIGINT, d DOUBLE, f FLOAT, c DECIMAL(12,2), c5 DECIMAL(5,2), "
    "c7 DECIMAL(7,2), dt DATE, ts TIMESTAMP, s STRING, b BOOLEAN, m MAP<STRING, INT>, "
    "st STRUCT<x: INT>, sl STRUCT<x: BIGINT>, ss STRUCT<x: STRING>, sy STRUCT<y: INT>, "
    "ai ARRAY<STRUCT<x: INT>>, mi MAP<STRING, STRUCT<x: INT>>"
)


def _wide_row(n: int, flag: str) -> str:
    return (
        f"({n}, {n}, {n}, CAST({n} AS BIGINT), CAST({n} AS DOUBLE), CAST({n} AS FLOAT), "
        f"CAST({n} AS DECIMAL(12,2)), CAST({n} AS DECIMAL(5,2)), CAST({n} AS DECIMAL(7,2)), "
        f"DATE'2020-01-0{n}', TIMESTAMP'2020-01-0{n} 00:00:00', '{n}', {flag}, map('x', {n}), "
        f"named_struct('x', {n}), named_struct('x', CAST({n} AS BIGINT)), "
        f"named_struct('x', '{n}'), named_struct('y', {n}), array(named_struct('x', {n})), "
        f"map('k', named_struct('x', {n})))"
    )


FIXTURES = {
    "W": (
        "CREATE TABLE {t} (" + WIDE_COLUMNS + ")" + V2,
        "INSERT INTO {t} VALUES " + _wide_row(1, "true") + ", " + _wide_row(2, "false"),
        "SELECT id, v FROM {t}",
    ),
    "VI": (
        "CREATE TABLE {t} (id INT, st STRUCT<x: INT>)" + V2,
        "INSERT INTO {t} VALUES (1, named_struct('x', 1)), (2, named_struct('x', 2))",
        "SELECT id, st FROM {t}",
    ),
    "VL": (
        "CREATE TABLE {t} (id INT, st STRUCT<x: BIGINT>)" + V2,
        "INSERT INTO {t} VALUES (1, named_struct('x', CAST(1 AS BIGINT))), "
        "(2, named_struct('x', CAST(2 AS BIGINT)))",
        "SELECT id, st FROM {t}",
    ),
}

OPS = {"eq": "=", "nse": "<=>", "ne": "!=", "lt": "<", "in": "IN"}
DF_OPS = {
    "eq": "({l} == {r})",
    "nse": "{l}.eqNullSafe({r})",
    "ne": "({l} != {r})",
    "lt": "({l} < {r})",
    "in": "{l}.isin({r})",
}

LEFT_ARMS = (
    ("bare", "{a}i", "i"),
    ("qualified", "w.i", "i"),
    ("nested", "{a}st.x", "x"),
    ("paren", "({a}i)", "i"),
    ("subscript", "{a}m['x']", "x"),
    ("dot", "named_struct('x', {a}i).x", "x"),
    ("literal", "1", "col1"),
    ("expression", "{a}i + 0", "col1"),
    ("alias", "{a}i AS x", "x"),
)
RIGHT_TYPES = (
    ("int_int", "v"),
    ("int_bigint", "l"),
    ("int_double", "d"),
    ("int_dec12", "c"),
    ("int_string", "s"),
    ("int_boolean", "b"),
)
OTHER_PAIRS = (
    ("float_double", "f", "d"),
    ("date_timestamp", "dt", "ts"),
    ("dec5_dec7", "c5", "c7"),
)

COMBOS = (
    (
        "k1_pos",
        "int_int|diff",
        "struct({a}i)",
        "struct({b}v)",
        "F.struct(F.col('i'))",
        "F.struct(F.col('v'))",
    ),
    (
        "k2_clash",
        "int_bigint|diff",
        "struct({a}i)",
        "struct({b}l)",
        "F.struct(F.col('i'))",
        "F.struct(F.col('l'))",
    ),
    (
        "k3_widen",
        "int_bigint|same",
        "struct({a}i AS x)",
        "struct({b}l AS x)",
        "F.struct(F.col('i').alias('x'))",
        "F.struct(F.col('l').alias('x'))",
    ),
    (
        "k4_string",
        "int_string|same",
        "named_struct('x', {a}i)",
        "named_struct('x', {b}s)",
        "F.struct(F.col('i').alias('x'))",
        "F.struct(F.col('s').alias('x'))",
    ),
    (
        "k5_literal",
        "int_int|diff",
        "struct({a}i)",
        "struct(1)",
        "F.struct(F.col('i'))",
        "F.struct(F.lit(1))",
    ),
)

COMPARE_SEATS = {
    "select": ("W", "SELECT {cmp} AS r FROM {W} WHERE id = 1", "", ""),
    "where": ("W", "SELECT id FROM {W} WHERE {cmp}", "", ""),
    "cte": ("W", "WITH c AS (SELECT id FROM {W} WHERE {cmp}) SELECT id FROM c", "", ""),
    "scalar_subquery": ("W", "SELECT (SELECT {cmp} FROM {W} WHERE id = 1) AS r", "", ""),
    "view": ("W", "CREATE OR REPLACE TEMPORARY VIEW sdv AS SELECT id FROM {W} WHERE {cmp}", "", ""),
    "delete": ("T", "DELETE FROM {T} WHERE {cmp}", "", ""),
    "update_where": ("T", "UPDATE {T} SET v = 77 WHERE {cmp}", "", ""),
    "merge_on": (
        "T",
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON {cmp} AND t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = 77",
        "t.",
        "s.",
    ),
    "merge_matched_and": (
        "T",
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON t.id = s.id "
        "WHEN MATCHED AND {cmp} THEN UPDATE SET v = 77",
        "t.",
        "s.",
    ),
    "merge_not_matched_and": (
        "T",
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON t.id = s.id + 100 "
        "WHEN NOT MATCHED AND {cmp} THEN INSERT *",
        "s.",
        "s.",
    ),
}

VALUES = (
    ("int_named", "struct({a}i)", "struct(id)", "F.struct(F.col('i'))"),
    ("bigint_named", "struct({a}l)", "struct(CAST(id AS BIGINT))", "F.struct(F.col('l'))"),
    ("int_alias_x", "struct({a}i AS x)", "struct(id AS x)", "F.struct(F.col('i').alias('x'))"),
    (
        "bigint_alias_x",
        "struct({a}l AS x)",
        "struct(CAST(id AS BIGINT) AS x)",
        "F.struct(F.col('l').alias('x'))",
    ),
    ("string_named", "struct({a}s)", "struct(CAST(id AS STRING))", "F.struct(F.col('s'))"),
    (
        "string_x",
        "named_struct('x', {a}s)",
        "named_struct('x', CAST(id AS STRING))",
        "F.struct(F.col('s').alias('x'))",
    ),
    ("arity", "struct({a}i, {a}i)", "struct(id, id)", "F.struct(F.col('i'), F.col('i'))"),
    ("literal", "struct(1)", "struct(1)", "F.struct(F.lit(1))"),
    (
        "int_y",
        "named_struct('y', {a}i)",
        "named_struct('y', id)",
        "F.struct(F.col('i').alias('y'))",
    ),
    ("double_named", "struct({a}d)", "struct(CAST(id AS DOUBLE))", "F.struct(F.col('d'))"),
    ("boolean_named", "struct({a}b)", "struct(id = 1)", "F.struct(F.col('b'))"),
    (
        "null_y",
        "CAST(NULL AS STRUCT<y: INT>)",
        "CAST(NULL AS STRUCT<y: INT>)",
        "F.lit(None).cast('struct<y: int>')",
    ),
    ("column_x", "{a}st", "named_struct('x', id)", "F.col('st')"),
    ("column_y", "{a}sy", "named_struct('y', id)", "F.col('sy')"),
)

VALUE_SEATS = {
    "insert_select": "INSERT INTO {T} SELECT 9, {val} FROM {W} WHERE id = 1",
    "insert_union_all": (
        "INSERT INTO {T} SELECT 8, named_struct('x', 1) UNION ALL "
        "SELECT 9, {val} FROM {W} WHERE id = 1"
    ),
    "update_set": "UPDATE {T} SET st = {upd} WHERE id = 1",
    "merge_update_set": (
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON t.id = s.id AND s.id = 1 "
        "WHEN MATCHED THEN UPDATE SET st = {val}"
    ),
    "merge_insert_value": (
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON t.id = s.id + 100 "
        "WHEN NOT MATCHED AND s.id = 1 THEN INSERT (id, st) VALUES (9, {val})"
    ),
    "df_insert_into": (
        "insert_into('{T}', w.where(F.col('id') == 1)"
        ".select(F.lit(9).alias('id'), {df}.alias('st')))"
    ),
    "df_append": (
        "append('{T}', w.where(F.col('id') == 1).select(F.lit(9).alias('id'), {df}.alias('st')))"
    ),
}

NESTED = (
    ("struct_in_struct", "int_int|diff", "struct(struct({a}i))", "struct(struct({b}v))"),
    ("struct_in_struct", "int_bigint|diff", "struct(struct({a}i))", "struct(struct({b}l))"),
    (
        "struct_in_struct",
        "int_bigint|same",
        "struct(struct({a}i AS x) AS n)",
        "struct(struct({b}l AS x) AS n)",
    ),
    (
        "struct_in_struct",
        "int_string|same",
        "struct(struct({a}i AS x) AS n)",
        "struct(struct({b}s AS x) AS n)",
    ),
    ("struct_in_array", "int_int|literal", "array(struct({a}i))", "array(struct(1))"),
    ("struct_in_array", "int_int|diff", "array(struct({a}i))", "array(struct({b}v))"),
    ("struct_in_array", "int_bigint|diff", "array(struct({a}i))", "array(struct({b}l))"),
    ("struct_in_array", "int_bigint|same", "array(struct({a}i AS x))", "array(struct({b}l AS x))"),
    ("struct_in_array", "int_string|diff", "array(struct({a}i))", "array(struct({b}s))"),
    (
        "struct_in_array",
        "int_int|named",
        "array(named_struct('x', {a}i))",
        "array(named_struct('x', 1))",
    ),
    ("struct_in_array", "column_literal|diff", "{a}ai", "array(struct(1))"),
    (
        "struct_in_array",
        "column_bigint|same",
        "{a}ai",
        "array(named_struct('x', CAST(1 AS BIGINT)))",
    ),
    ("array_of_array", "int_int|literal", "array(array(struct({a}i)))", "array(array(struct(1)))"),
    (
        "array_of_array",
        "int_bigint|diff",
        "array(array(struct({a}i)))",
        "array(array(struct({b}l)))",
    ),
    ("struct_in_map", "int_int|literal", "map('k', struct({a}i))", "map('k', struct(1))"),
    ("struct_in_map", "column_literal|diff", "{a}mi", "map('k', struct(1))"),
    ("literal_vs_column", "int_int|diff", "{a}st", "struct(1)"),
    ("literal_vs_column", "int_bigint|diff", "{a}st", "struct(CAST(1 AS BIGINT))"),
    ("literal_vs_column", "int_bigint|same", "{a}st", "named_struct('x', CAST(1 AS BIGINT))"),
    ("literal_vs_column", "bigint_int|diff", "{a}sl", "struct(1)"),
    ("two_columns", "int_int|diff", "{a}st", "{b}sy"),
    ("two_columns", "int_bigint|same", "{a}st", "{b}sl"),
    ("two_columns", "int_string|same", "{a}st", "{b}ss"),
    ("two_columns", "bigint_int|diff", "{a}sl", "{b}sy"),
    ("typed_null", "int_int|diff", "{a}st", "CAST(NULL AS STRUCT<y: INT>)"),
    ("typed_null", "int_bigint|diff", "{a}st", "CAST(NULL AS STRUCT<y: BIGINT>)"),
    ("typed_null", "int_bigint|same", "struct({a}i)", "CAST(NULL AS STRUCT<i: BIGINT>)"),
    ("named_struct", "int_int|diff", "named_struct('a', {a}i)", "named_struct('b', {b}v)"),
    ("named_struct", "int_bigint|diff", "named_struct('a', {a}i)", "named_struct('b', {b}l)"),
    ("named_struct", "int_bigint|same", "named_struct('a', {a}i)", "named_struct('a', {b}l)"),
)
NESTED_SEATS = ("select", "where", "delete", "update_where", "merge_on")


NULL_PREDICATES = (
    ("null_same", "{a}st = CAST(NULL AS STRUCT<x: INT>)"),
    ("null_other", "{a}st = CAST(NULL AS STRUCT<y: INT>)"),
    ("not_null_same", "NOT ({a}st = CAST(NULL AS STRUCT<x: INT>))"),
    ("not_null_other", "NOT ({a}st = CAST(NULL AS STRUCT<y: INT>))"),
    ("named_null", "struct({a}i) = CAST(NULL AS STRUCT<i: INT>)"),
    ("not_named_null", "NOT (struct({a}i) = CAST(NULL AS STRUCT<z: INT>))"),
    ("null_or_true", "{a}st = CAST(NULL AS STRUCT<x: INT>) OR TRUE"),
    ("null_is_null", "({a}st = CAST(NULL AS STRUCT<x: INT>)) IS NULL"),
    ("null_safe_column", "{a}st <=> CAST(NULL AS STRUCT<x: INT>)"),
    ("null_safe_null", "CAST(NULL AS STRUCT<x: INT>) <=> CAST(NULL AS STRUCT<x: INT>)"),
    ("int_null", "{a}v = CAST(NULL AS INT)"),
    ("folded_false", "{a}id = 1 AND 1 = 0"),
)
NULL_SEATS = ("where", "delete", "update_where", "merge_on", "merge_matched_and")


def _cmp(op: str, left: str, right: str) -> str:
    if op == "in":
        return f"{left} IN ({right})"
    return f"{left} {OPS[op]} {right}"


def _cell(key: str, seat: str, shape: str, types: str, stmt: str) -> dict[str, str]:
    return {"key": key, "seat": seat, "shape": shape, "types": types, "stmt": stmt}


def _shape_cells() -> list[dict[str, str]]:
    out = []
    for arm, left, name in LEFT_ARMS:
        for types, column in RIGHT_TYPES:
            pairs = {
                "diff": (f"struct({left})", f"struct({{a}}{column})"),
                "same": (f"struct({left})", f"struct({{a}}{column} AS {name})"),
                "dup": (
                    f"struct({left}, {left})",
                    f"struct({{a}}{column} AS {name}, {{a}}{column} AS {name})",
                ),
            }
            out.extend(_shape_seats(arm, types, pairs))
    for types, left, right in OTHER_PAIRS:
        pairs = {
            "diff": (f"struct({{a}}{left})", f"struct({{a}}{right})"),
            "same": (f"struct({{a}}{left} AS x)", f"struct({{a}}{right} AS x)"),
            "dup": (
                f"struct({{a}}{left} AS x, {{a}}{left} AS x)",
                f"struct({{a}}{right} AS x, {{a}}{right} AS x)",
            ),
        }
        out.extend(_shape_seats("bare", types, pairs))
    return out


def _shape_seats(arm: str, types: str, pairs: dict[str, tuple[str, str]]) -> list[dict[str, str]]:
    out = []
    for names, (left, right) in pairs.items():
        cmp = _cmp("eq", left, right).replace("{a}", "")
        tag = f"{types}|{names}"
        out.append(
            _cell(
                f"a_select_{arm}_{types}_{names}",
                "select",
                arm,
                tag,
                f"SELECT {cmp} AS r FROM {{W}} w WHERE id = 1",
            )
        )
        if names != "dup":
            out.append(
                _cell(
                    f"a_delete_{arm}_{types}_{names}",
                    "delete",
                    arm,
                    tag,
                    f"DELETE FROM {{T}} w WHERE {cmp}",
                )
            )
    return out


def _combo_cells() -> list[dict[str, str]]:
    out = []
    for combo, types, left, right, df_left, df_right in COMBOS:
        for seat, (_, template, qa, qb) in COMPARE_SEATS.items():
            for op in OPS:
                cmp = _cmp(op, left.replace("{a}", qa), right.replace("{b}", qb))
                stmt = template.replace("{cmp}", cmp)
                out.append(_cell(f"b_{seat}_{combo}_{op}", seat, combo, types, stmt))
        for op, pattern in DF_OPS.items():
            cmp = pattern.format(l=df_left, r=df_right)
            out.append(
                _cell(
                    f"b_df_select_{combo}_{op}",
                    "df_select",
                    combo,
                    types,
                    f"w.where(F.col('id') == 1).select({cmp}.alias('r'))",
                )
            )
            out.append(
                _cell(
                    f"b_df_filter_{combo}_{op}",
                    "df_filter",
                    combo,
                    types,
                    f"w.filter({cmp}).select('id')",
                )
            )
        plain_left = left.replace("{a}", "")
        plain_right = right.replace("{b}", "")
        out.append(
            _cell(
                f"b_in_subquery_{combo}",
                "in_subquery",
                combo,
                types,
                f"SELECT id FROM {{W}} WHERE {plain_left} IN (SELECT {plain_right} FROM {{W}})",
            )
        )
        out.append(
            _cell(
                f"b_union_all_{combo}",
                "union_all",
                combo,
                types,
                f"SELECT {plain_left} AS r FROM {{W}} WHERE id = 1 UNION ALL "
                f"SELECT {plain_right} FROM {{W}} WHERE id = 2",
            )
        )
    return out


def _value_cells() -> list[dict[str, str]]:
    out = []
    for value, sql, upd, df in VALUES:
        for target in ("VI", "VL"):
            for seat, template in VALUE_SEATS.items():
                qualifier = "s." if seat.startswith("merge") else ""
                stmt = (
                    template.replace("{val}", sql.replace("{a}", qualifier))
                    .replace("{upd}", upd)
                    .replace("{df}", df)
                    .replace("{T}", "{" + target + "}")
                )
                out.append(_cell(f"c_{seat}_{target.lower()}_{value}", seat, "value", value, stmt))
    return out


def _nested_cells() -> list[dict[str, str]]:
    out = []
    for index, (shape, types, left, right) in enumerate(NESTED):
        for seat in NESTED_SEATS:
            _, template, qa, qb = COMPARE_SEATS[seat]
            cmp = _cmp("eq", left.replace("{a}", qa), right.replace("{b}", qb))
            stmt = template.replace("{cmp}", cmp)
            out.append(_cell(f"d_{seat}_{shape}_{index}", seat, shape, types, stmt))
        out.append(
            _cell(
                f"d_union_all_{shape}_{index}",
                "union_all",
                shape,
                types,
                f"SELECT {left.replace('{a}', '')} AS r FROM {{W}} WHERE id = 1 UNION ALL "
                f"SELECT {right.replace('{b}', '')} FROM {{W}} WHERE id = 2",
            )
        )
    return out


def _null_cells() -> list[dict[str, str]]:
    out = []
    for name, predicate in NULL_PREDICATES:
        for seat in NULL_SEATS:
            _, template, qualifier, _ = COMPARE_SEATS[seat]
            cmp = "(" + predicate.replace("{a}", qualifier) + ")"
            stmt = template.replace("{cmp}", cmp)
            out.append(_cell(f"e_{seat}_{name}", seat, "null_literal", name, stmt))
    return out


def cells() -> list[dict[str, str]]:
    """Return the struct-domain grid: seats x shapes x type pairs, without answers."""
    out = _shape_cells() + _combo_cells() + _value_cells() + _nested_cells() + _null_cells()
    keys = [cell["key"] for cell in out]
    if len(keys) != len(set(keys)):
        raise ValueError("struct domain keys must be unique")
    return out

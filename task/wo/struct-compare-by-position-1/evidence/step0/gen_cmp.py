from __future__ import annotations

import json
import sys
from pathlib import Path

OPS = (
    ("eq", "{l} = {r}", "({l} == {r})"),
    ("nse", "{l} <=> {r}", "{l}.eqNullSafe({r})"),
    ("ne", "{l} != {r}", "({l} != {r})"),
    ("lt", "{l} < {r}", "({l} < {r})"),
    ("le", "{l} <= {r}", "({l} <= {r})"),
    ("gt", "{l} > {r}", "({l} > {r})"),
    ("ge", "{l} >= {r}", "({l} >= {r})"),
    ("in", "{l} IN ({r})", "{l}.isin({r})"),
    ("not_in", "{l} NOT IN ({r})", "(~{l}.isin({r}))"),
    ("not_nse", "NOT ({l} <=> {r})", "(~{l}.eqNullSafe({r}))"),
)
OP_SQL = {name: sql for name, sql, _ in OPS}
OP_DF = {name: df for name, _, df in OPS}
OPS_TYPED = ("eq", "nse", "lt", "in")

SQL_SEATS = (
    "select",
    "where",
    "cte",
    "scalar_subquery",
    "view",
    "df_temp_view",
    "join_on",
    "delete",
    "update_where",
    "merge_on",
    "merge_matched_and",
    "merge_not_matched_and",
)
TYPED_SEATS = ("select", "where", "join_on")
STRING_SEATS = ("filter_str", "where_str", "expr_select", "selectexpr_select")

SQL_TEMPLATES = {
    "select": "SELECT {c} AS r FROM {W} WHERE id = 1",
    "where": "SELECT id FROM {W} WHERE {c}",
    "cte": "WITH c AS (SELECT id FROM {W} WHERE {c}) SELECT id FROM c",
    "scalar_subquery": "SELECT (SELECT {c} FROM {W} WHERE id = 1) AS r",
    "view": "CREATE OR REPLACE TEMPORARY VIEW sdv AS SELECT id FROM {W} WHERE {c}",
    "df_temp_view": "SELECT id FROM {W} WHERE {c}",
    "join_on": "SELECT x.id AS xid, y.id AS yid FROM {W} x JOIN {W} y ON {c}",
    "delete": "DELETE FROM {T} WHERE {c}",
    "update_where": "UPDATE {T} SET v = 77 WHERE {c}",
    "merge_on": (
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON {c} AND t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = 77"
    ),
    "merge_matched_and": (
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON t.id = s.id "
        "WHEN MATCHED AND {c} THEN UPDATE SET v = 77"
    ),
    "merge_not_matched_and": (
        "MERGE INTO {T} t USING (SELECT * FROM {W}) s ON t.id = s.id + 100 "
        "WHEN NOT MATCHED AND {c} THEN INSERT *"
    ),
}
TYPED_TEMPLATES = {
    "select": "w.where(F.col('id') == 1).select({c}.alias('r'))",
    "where": "w.where({c}).select('id')",
    "join_on": (
        "w.alias('x').join(w.alias('y'), {c}).select("
        "F.col('x.id').alias('xid'), F.col('y.id').alias('yid'))"
    ),
}
STRING_TEMPLATES = {
    "filter_str": "w.filter({s}).select('id')",
    "where_str": "w.where({s}).select('id')",
    "expr_select": "w.where(F.col('id') == 1).select(F.expr({s}).alias('r'))",
    "selectexpr_select": "w.where(F.col('id') == 1).selectExpr({s})",
}

SHAPE_BASE = (
    ("ctrl_same_lit", "int_int|same_order", "{L}p", "named_struct('a', 3, 'b', 4)", "control"),
    ("ctrl_same_col", "int_int|same_order", "{L}p", "{R}p", "control"),
    ("reord_col", "int_int|reordered", "{L}p", "{R}q", ""),
    ("reord_lit", "int_int|reordered", "{L}p", "named_struct('b', 4, 'a', 3)", ""),
    (
        "reord_both_lit",
        "int_int|reordered",
        "named_struct('a', 3, 'b', 4)",
        "named_struct('b', 4, 'a', 3)",
        "",
    ),
    ("diff_names", "int_int|different", "{L}p", "named_struct('c', 3, 'd', 4)", ""),
    ("count_mismatch", "int_int|count", "{L}p", "named_struct('b', 4)", ""),
    (
        "dup_both",
        "int_int|duplicate",
        "named_struct('a', 3, 'a', 4)",
        "named_struct('a', 4, 'a', 3)",
        "",
    ),
    ("dup_one", "int_int|duplicate", "{L}p", "named_struct('a', 3, 'a', 4)", ""),
    (
        "nested_inner",
        "struct_in_struct|reordered",
        "{L}n",
        "named_struct('a', 3, 'b', named_struct('d', 2, 'c', 1))",
        "",
    ),
    ("nested_outer", "struct_in_struct|reordered", "{L}n", "{R}nq", ""),
    ("array_col", "array_of_struct|reordered", "{L}ap", "{R}aq", ""),
    ("array_lit", "array_of_struct|reordered", "{L}ap", "array(named_struct('b', 4, 'a', 3))", ""),
    ("map_value", "map_value_struct|reordered", "{L}mp['k']", "named_struct('b', 4, 'a', 3)", ""),
    (
        "typed_null_reord",
        "typed_null|reordered",
        "{L}p",
        "CAST(NULL AS STRUCT<b: INT, a: INT>)",
        "",
    ),
    (
        "typed_null_same",
        "typed_null|same_order",
        "{L}p",
        "CAST(NULL AS STRUCT<a: INT, b: INT>)",
        "control",
    ),
)

TYPE_RIGHTS = (
    ("int_bigint", "CAST(4 AS BIGINT)", "CAST(3 AS BIGINT)"),
    ("int_double", "4.0D", "3.0D"),
    ("int_dec12", "CAST(4 AS DECIMAL(12,2))", "CAST(3 AS DECIMAL(12,2))"),
    ("int_string", "'4'", "'3'"),
    ("int_boolean", "true", "false"),
)


def _type_shapes() -> list[tuple[str, str, str, str, str]]:
    out = []
    for tag, four, three in TYPE_RIGHTS:
        out.append(
            (
                f"reord_lit_{tag}",
                f"{tag}|reordered",
                "{L}p",
                f"named_struct('b', {four}, 'a', {three})",
                "",
            )
        )
        out.append(
            (
                f"diff_lit_{tag}",
                f"{tag}|different",
                "{L}p",
                f"named_struct('c', {four}, 'd', {three})",
                "",
            )
        )
        out.append(
            (
                f"same_lit_{tag}",
                f"{tag}|same_order",
                "{L}p",
                f"named_struct('a', {three}, 'b', {four})",
                "control",
            )
        )
    return out


SHAPE_TYPED = tuple(_type_shapes())

CASE_SHAPE = ("reord_case", "int_int|reordered_case", "{L}p", "named_struct('B', 4, 'A', 3)", "")

ANSI_SHAPES = ("reord_col", "reord_both_lit", "reord_lit")
ANSI_OPS = ("eq", "nse", "in")
ANSI_SEATS = ("select", "where", "delete")

PROBES = (
    (
        "probe_prepare",
        "PREPARE s1 FROM SELECT id FROM {W} WHERE p = named_struct('b', 4, 'a', 3)",
        "sql",
        "probe",
    ),
    (
        "probe_group_by",
        "SELECT s, count(*) AS c FROM (SELECT p AS s FROM {W} WHERE id = 1 UNION ALL "
        "SELECT named_struct('b', 4, 'a', 3) AS s FROM {W} WHERE id = 1) GROUP BY s",
        "sql",
        "probe",
    ),
    (
        "probe_distinct",
        "SELECT DISTINCT s FROM (SELECT p AS s FROM {W} WHERE id = 1 UNION ALL "
        "SELECT named_struct('b', 4, 'a', 3) AS s FROM {W} WHERE id = 1)",
        "sql",
        "probe",
    ),
    ("probe_collect_list", "SELECT collect_list(p) AS r FROM {W}", "sql", "probe"),
    (
        "probe_window_partition",
        "SELECT id, row_number() OVER (PARTITION BY p ORDER BY id) AS rn FROM {W}",
        "sql",
        "probe",
    ),
    (
        "probe_map_struct_key",
        "SELECT element_at(map(named_struct('a', 3, 'b', 4), 'x'), named_struct('b', 4, 'a', "
        "3)) AS r "
        "FROM {W} WHERE id = 1",
        "sql",
        "probe",
    ),
    (
        "probe_map_nested_key",
        "SELECT map('k', map(named_struct('a', 3, 'b', 4), 1))['k'][named_struct('b', 4, 'a', "
        "3)] AS r "
        "FROM {W} WHERE id = 1",
        "sql",
        "probe",
    ),
    (
        "probe_delete_metadata",
        "DELETE FROM {T} WHERE p = named_struct('b', 4, 'a', 3)",
        "sql",
        "probe",
    ),
    (
        "probe_d6_inner_reorder",
        "INSERT INTO {T} BY NAME SELECT 9 AS id, named_struct('a', 3, 'b', named_struct('d', 2, "
        "'c', 1)) AS n",
        "sql",
        "probe",
    ),
    (
        "probe_d6_outer_reorder",
        "INSERT INTO {T} BY NAME SELECT 9 AS id, named_struct('b', 4, 'a', 3) AS p",
        "sql",
        "probe",
    ),
    (
        "probe_d6_nq_inner",
        "INSERT INTO {T} BY NAME SELECT 9 AS id, named_struct('b', named_struct('c', 1, 'd', "
        "2), 'a', 3) AS nq",
        "sql",
        "probe",
    ),
    (
        "control_union_all_reord",
        "SELECT p AS r FROM {W} WHERE id = 1 UNION ALL SELECT q FROM {W} WHERE id = 2",
        "sql",
        "control",
    ),
    (
        "control_write_insert_positional",
        "INSERT INTO {T} SELECT 9, 0, named_struct('b', 4, 'a', 3), named_struct('a', 3, 'b', 4), "
        "named_struct('a', 3, 'b', named_struct('c', 1, 'd', 2)), "
        "named_struct('b', named_struct('d', 2, 'c', 1), 'a', 3), array(named_struct('a', 3, "
        "'b', 4)), "
        "array(named_struct('b', 4, 'a', 3)), map('k', named_struct('a', 3, 'b', 4)) FROM {W} "
        "WHERE id = 1",
        "sql",
        "control",
    ),
    (
        "control_union_by_name",
        "w.select('id', 'p').unionByName(w.select('id', F.col('q').alias('p')))",
        "df_typed",
        "control",
    ),
)


def _quals(seat: str) -> tuple[str, str]:
    if seat == "join_on":
        return "x.", "y."
    if seat in ("merge_on", "merge_matched_and"):
        return "t.", "s."
    if seat == "merge_not_matched_and":
        return "s.", "s."
    return "", ""


def _operands(shape_left: str, shape_right: str, seat: str) -> tuple[str, str]:
    lq, rq = _quals(seat)
    return shape_left.replace("{L}", lq), shape_right.replace("{R}", rq)


def _control_tag(shape: str, op: str, seat: str, door: str) -> str:
    named = {
        ("reord_both_lit", "eq", "select", "sql"): "c10_eq_select",
        ("reord_both_lit", "lt", "select", "sql"): "c13_lt_select",
        ("reord_both_lit", "in", "select", "sql"): "c14_in_select",
        ("reord_lit", "in_sub", "where", "sql"): "c16_in_subquery_where",
        ("reord_col", "eq", "join_on", "sql"): "c22_join_on",
        ("reord_lit", "in", "select", "df_typed"): "c30_isin_typed",
        ("reord_lit", "eq", "expr_select", "df_string"): "c31_expr_select",
        ("reord_lit", "eq", "where", "sql"): "c36_where",
        ("reord_lit", "eq", "update_where", "sql"): "c37_update_where",
        ("reord_lit", "eq", "delete", "sql"): "c38_delete",
        ("reord_lit", "eq", "merge_on", "sql"): "c39_merge_on",
    }
    return named.get((shape, op, seat, door), "")


def _add(
    out: list[dict],
    family: str,
    shape: str,
    types: str,
    op: str,
    door: str,
    seat: str,
    case: str,
    ansi: str,
    stmt: str,
    control: str,
) -> None:
    tag = control or ("" if ansi == "off" else _control_tag(shape, op, seat, door))
    out.append(
        {
            "family": family,
            "shape": shape,
            "types": types,
            "op": op,
            "door": door,
            "seat": seat,
            "case": case,
            "ansi": ansi,
            "control": tag,
            "stmt": stmt,
        }
    )


def _sql_stmt(seat: str, cmp: str) -> str:
    return SQL_TEMPLATES[seat].replace("{c}", cmp)


def _typed_stmt(seat: str, op: str, left: str, right: str) -> str:
    pattern = OP_DF[op].format(l=f"F.expr({json.dumps(left)})", r=f"F.expr({json.dumps(right)})")
    return TYPED_TEMPLATES[seat].replace("{c}", pattern)


def _string_stmt(seat: str, cmp: str) -> str:
    text = cmp + " AS r" if seat == "selectexpr_select" else cmp
    return STRING_TEMPLATES[seat].replace("{s}", json.dumps(text))


def _emit_shape(
    out: list[dict],
    family: str,
    shape: str,
    types: str,
    left: str,
    right: str,
    control: str,
    ops: tuple[str, ...],
    case: str = "insensitive",
    ansi: str = "on",
) -> None:
    for op in ops:
        for seat in SQL_SEATS:
            if ansi == "off" and seat not in ANSI_SEATS:
                continue
            lsql, rsql = _operands(left, right, seat)
            cmp = OP_SQL[op].format(l=lsql, r=rsql)
            _add(
                out,
                family,
                shape,
                types,
                op,
                "sql",
                seat,
                case,
                ansi,
                _sql_stmt(seat, cmp),
                control,
            )
        if ansi == "off":
            continue
        for seat in TYPED_SEATS:
            lsql, rsql = _operands(left, right, seat)
            _add(
                out,
                family,
                shape,
                types,
                op,
                "df_typed",
                seat,
                case,
                ansi,
                _typed_stmt(seat, op, lsql, rsql),
                control,
            )
        for seat in STRING_SEATS:
            lsql, rsql = _operands(left, right, "")
            cmp = OP_SQL[op].format(l=lsql, r=rsql)
            _add(
                out,
                family,
                shape,
                types,
                op,
                "df_string",
                seat,
                case,
                ansi,
                _string_stmt(seat, cmp),
                control,
            )


def generate() -> list[dict]:
    out: list[dict] = []
    for shape, types, left, right, control in SHAPE_BASE:
        _emit_shape(
            out, "base", shape, types, left, right, control, tuple(name for name, _, _ in OPS)
        )
        lsql, rsql = _operands(left, right, "")
        _add(
            out,
            "base",
            shape,
            types,
            "in_sub",
            "sql",
            "where",
            "insensitive",
            "on",
            f"SELECT id FROM {{W}} WHERE {lsql} IN (SELECT {rsql} FROM {{W}})",
            control,
        )
    for shape, types, left, right, control in SHAPE_TYPED:
        _emit_shape(out, "typed", shape, types, left, right, control, OPS_TYPED)
    shape, types, left, right, control = CASE_SHAPE
    for case in ("insensitive", "sensitive"):
        _emit_shape(out, "case", shape, types, left, right, control, OPS_TYPED, case=case)
    lookup = {name: (left, right) for name, _, left, right, _ in SHAPE_BASE}
    for shape in ANSI_SHAPES:
        left, right = lookup[shape]
        _emit_shape(out, "ansi", shape, "int_int|reordered", left, right, "", ANSI_OPS, ansi="off")
    for name, sql, door, seat in PROBES:
        _add(out, "probe", name, "", "", door, seat, "insensitive", "on", sql, "")
    return out


def main() -> None:
    target = Path(sys.argv[1])
    cells = generate()
    lines = []
    for index, cell in enumerate(cells, start=1):
        cell = {"key": f"{index:05d}", **cell}
        lines.append(json.dumps(cell, sort_keys=True))
    target.write_text("\n".join(lines) + "\n")
    print(len(cells))


if __name__ == "__main__":
    main()

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ELEM_SQL = {"I64": "Int64", "UTF8": "Utf8", "STRUCT": 'Struct("x": Int64, "y": Utf8)'}
ELEM_DDL = {"I64": "bigint", "UTF8": "string", "STRUCT": "struct<x:bigint,y:string>"}
REE_I64 = 'RunEndEncoded("run_ends": non-null Int32, "values": Int64)'
REE_UTF8 = 'RunEndEncoded("run_ends": non-null Int32, "values": Utf8)'
MAP_LV = 'Map("entries": non-null Struct("key": non-null Utf8, "value": ListView(Int64)), unsorted)'

BASE_KINDS = {
    "LV_I64": {"container": "ListView", "elem": "I64"},
    "LLV_I64": {"container": "LargeListView", "elem": "I64"},
    "LV_UTF8": {"container": "ListView", "elem": "UTF8"},
    "LLV_UTF8": {"container": "LargeListView", "elem": "UTF8"},
    "LV_STRUCT": {"container": "ListView", "elem": "STRUCT"},
    "LLV_STRUCT": {"container": "LargeListView", "elem": "STRUCT"},
    "L_I64": {"container": "List", "elem": "I64"},
    "LL_I64": {"container": "LargeList", "elem": "I64"},
    "FSL2_I64": {"container": "FixedSizeList", "elem": "I64"},
    "L_UTF8": {"container": "List", "elem": "UTF8"},
    "L_STRUCT": {"container": "List", "elem": "STRUCT"},
    "NEST_LV": {"nest": "ListView", "elem": "I64"},
    "NEST_L": {"nest": "List", "elem": "I64"},
}
VARIANT_KINDS = {
    "REE_I64": {"variant": "ree_i64", "ddl": "bigint"},
    "REE_UTF8": {"variant": "ree_utf8", "ddl": "string"},
    "DICT_LV": {"variant": "dict_lv", "ddl": "array<bigint>"},
    "DICT_REE": {"variant": "dict_ree", "ddl": "bigint"},
    "UNION_I64": {"variant": "union_i64", "ddl": "bigint"},
    "L_LV": {"variant": "l_lv", "ddl": "array<array<bigint>>"},
    "LL_LV": {"variant": "ll_lv", "ddl": "array<array<bigint>>"},
    "FSL_LV": {"variant": "fsl_lv", "ddl": "array<array<bigint>>"},
    "MAP_LV": {"variant": "map_lv", "ddl": "map<string,array<bigint>>"},
    "STRUCT_REE": {"variant": "struct_ree", "ddl": "struct<a:bigint>"},
    "L_REE": {"variant": "l_ree", "ddl": "array<bigint>"},
}
KINDS = {**BASE_KINDS, **VARIANT_KINDS}
VIEW_KINDS = ["LV_I64", "LLV_I64", "LV_UTF8", "LLV_UTF8", "LV_STRUCT", "LLV_STRUCT", "NEST_LV"]
PLAN_KINDS = [*VIEW_KINDS, "L_I64", "NEST_L"]

LOGICAL = {"A": [1, 2], "B": [2, 1], "C": [], "D": [1, None], "E": None, "F": [7, 7], "G": [2]}
FSL2_LOGICAL = {
    "A": [1, 2],
    "B": [2, 1],
    "C": [3, 3],
    "D": [1, None],
    "E": None,
    "F": [7, 7],
    "G": [2, 2],
}
SCALAR = {"A": 1, "B": 2, "C": 3, "D": 4, "E": None, "F": 7, "G": 5}
SCALAR_VARIANTS = {"ree_i64", "ree_utf8", "dict_ree", "union_i64", "struct_ree"}

BASE_LEFT = [
    (1, "A", 1, 1),
    (2, "B", 2, 2),
    (3, "A", 3, 1),
    (4, "C", 4, 2),
    (5, "E", 5, 1),
    (6, "D", 6, 2),
    (7, "F", 7, 1),
    (8, "A", 0, 1),
]
BASE_RIGHT = [
    (10, "A", 1, "p"),
    (11, "A", 2, "q"),
    (12, "B", 2, "r"),
    (13, "C", 3, "s"),
    (14, "E", 1, "t"),
    (15, "D", 2, "u"),
    (16, "G", 1, "v"),
]
SIZE = (8, 7)

FAMILY_KINDS = ["plain", "large", "fsl2", "view"]
DF_HOW = {"inner": "inner", "left": "left", "semi": "left_semi"}
PLAN_STAGES = [
    "initial_logical_plan",
    "logical_plan after type_coercion",
    "analyzed_logical_plan",
    "logical_plan after extract_equijoin_predicate",
    "logical_plan after eliminate_cross_join",
    "logical_plan after optimize_projections",
    "logical_plan",
    "physical_plan",
]


def first_line(error: BaseException, limit: int = 600) -> str:
    text = str(error)
    return text.splitlines()[0][:limit] if text else ""


def last_line(error: BaseException, limit: int = 400) -> str:
    text = str(error).strip()
    return text.splitlines()[-1][:limit] if text else ""


def elements(spec: dict[str, Any], letter: str) -> Any:
    if spec.get("variant") in {*SCALAR_VARIANTS, "l_ree"}:
        return SCALAR[letter]
    table = FSL2_LOGICAL if spec.get("container") == "FixedSizeList" else LOGICAL
    return table[letter]


def sql_type(container: str, elem: str) -> str:
    if container == "FixedSizeList":
        return f"FixedSizeList(2, {ELEM_SQL[elem]})"
    return f"{container}({ELEM_SQL[elem]})"


def spark_elem(elem: str, v: Any) -> Any:
    if elem == "I64":
        return v
    if elem == "UTF8":
        return None if v is None else chr(96 + v)
    return (None, None) if v is None else (v, chr(96 + v))


def spark_key(spec: dict[str, Any], letter: str) -> Any:
    vals = elements(spec, letter)
    variant = spec.get("variant")
    if variant is not None:
        return spark_variant_key(variant, vals)
    if "nest" in spec:
        inner = None if vals is None else [spark_elem(spec["elem"], v) for v in vals]
        return (inner,)
    if vals is None:
        return None
    return [spark_elem(spec["elem"], v) for v in vals]


def spark_variant_key(variant: str, vals: Any) -> Any:
    if variant in ("ree_i64", "dict_ree", "union_i64"):
        return vals
    if variant == "ree_utf8":
        return None if vals is None else chr(96 + vals)
    if variant == "struct_ree":
        return (vals,)
    if variant == "dict_lv":
        return vals
    if variant == "l_ree":
        return None if vals is None else [vals]
    if vals is None:
        return None
    if variant in ("l_lv", "ll_lv"):
        return [vals]
    if variant == "fsl_lv":
        return [vals, vals]
    if variant == "map_lv":
        return {"m": vals}
    raise ValueError(variant)


def spark_ddl(spec: dict[str, Any]) -> str:
    if "ddl" in spec:
        return spec["ddl"]
    if "nest" in spec:
        return "struct<a:array<bigint>>"
    return f"array<{ELEM_DDL[spec['elem']]}>"


def sql_elem(elem: str, v: Any) -> str:
    if elem == "I64":
        return f"CAST({v} AS BIGINT)" if v is not None else "CAST(NULL AS BIGINT)"
    if elem == "UTF8":
        return f"'{chr(96 + v)}'" if v is not None else "CAST(NULL AS VARCHAR)"
    if v is None:
        return "named_struct('x', CAST(NULL AS BIGINT), 'y', CAST(NULL AS VARCHAR))"
    return f"named_struct('x', CAST({v} AS BIGINT), 'y', '{chr(96 + v)}')"


def sql_list(container: str, elem: str, vals: Any) -> str:
    t = sql_type(container, elem)
    if vals is None:
        return f"arrow_cast(NULL, '{t}')"
    if not vals:
        return f"arrow_cast(make_array(), '{t}')"
    items = ", ".join(sql_elem(elem, v) for v in vals)
    return f"arrow_cast(array({items}), '{t}')"


def sql_scalar(variant: str, v: Any) -> str:
    if variant == "ree_i64":
        return f"arrow_cast({sql_elem('I64', v)}, '{REE_I64}')"
    if variant == "ree_utf8":
        return f"arrow_cast({sql_elem('UTF8', v)}, '{REE_UTF8}')"
    if variant == "dict_ree":
        return f"arrow_cast({sql_elem('I64', v)}, 'Dictionary(Int32, {REE_I64})')"
    if variant == "union_i64":
        return f"arrow_cast({sql_elem('I64', v)}, 'Union(Sparse, 0: (\"a\": Int64))')"
    if variant == "struct_ree":
        return f"named_struct('a', {sql_scalar('ree_i64', v)})"
    raise ValueError(variant)


def sql_variant_key(variant: str, vals: Any) -> str:
    if variant in SCALAR_VARIANTS:
        return sql_scalar(variant, vals)
    if variant == "dict_lv":
        inner = sql_list("List", "I64", vals)
        return f"arrow_cast({inner}, 'Dictionary(Int32, ListView(Int64))')"
    if variant == "l_ree":
        if vals is None:
            return f"arrow_cast(NULL, 'List({REE_I64})')"
        return f"arrow_cast(array({sql_elem('I64', vals)}), 'List({REE_I64})')"
    targets = {
        "l_lv": "List(ListView(Int64))",
        "ll_lv": "LargeList(ListView(Int64))",
        "fsl_lv": "FixedSizeList(2, ListView(Int64))",
        "map_lv": MAP_LV,
    }
    target = targets[variant]
    if vals is None:
        return f"arrow_cast(NULL, '{target}')"
    inner = sql_list("List", "I64", vals)
    if variant == "fsl_lv":
        return f"arrow_cast(array({inner}, {inner}), '{target}')"
    if variant == "map_lv":
        return f"arrow_cast(map(['m'], [{inner}]), '{target}')"
    return f"arrow_cast(array({inner}), '{target}')"


def sql_key(spec: dict[str, Any], letter: str) -> str:
    vals = elements(spec, letter)
    if "variant" in spec:
        return sql_variant_key(spec["variant"], vals)
    if "nest" in spec:
        return f"named_struct('a', {sql_list(spec['nest'], 'I64', vals)})"
    return sql_list(spec["container"], spec["elem"], vals)


def left_sql(spec: dict[str, Any]) -> str:
    return " UNION ALL ".join(
        f"SELECT CAST({lid} AS BIGINT) AS lid, {sql_key(spec, letter)} AS k, "
        f"CAST({idv} AS BIGINT) AS id, CAST({j} AS INT) AS j"
        for lid, letter, idv, j in BASE_LEFT
    )


def right_sql(spec: dict[str, Any]) -> str:
    return " UNION ALL ".join(
        f"SELECT CAST({rid} AS BIGINT) AS rid, {sql_key(spec, letter)} AS k, "
        f"CAST({j} AS INT) AS j, '{rv}' AS rv"
        for rid, letter, j, rv in BASE_RIGHT
    )


def build_views(leg: str, s: Any, kind: str) -> None:
    spec = KINDS[kind]
    if leg == "spark":
        ddl_k = spark_ddl(spec)
        lrows = [(lid, spark_key(spec, x), idv, j) for lid, x, idv, j in BASE_LEFT]
        rrows = [(rid, spark_key(spec, x), j, rv) for rid, x, j, rv in BASE_RIGHT]
        lschema = f"lid bigint, k {ddl_k}, id bigint, j int"
        rschema = f"rid bigint, k {ddl_k}, j int, rv string"
        s.createDataFrame(lrows, lschema).createOrReplaceTempView("lv_l")
        s.createDataFrame(rrows, rschema).createOrReplaceTempView("lv_r")
        return
    s.sql(f"CREATE OR REPLACE TEMPORARY VIEW lv_l AS {left_sql(spec)}").collect()
    s.sql(f"CREATE OR REPLACE TEMPORARY VIEW lv_r AS {right_sql(spec)}").collect()


def reached_type(leg: str, frame: Any) -> str:
    try:
        if leg == "main":
            return str(frame.toArrow().schema.field("k").type)
        return frame.schema["k"].dataType.simpleString()
    except Exception as error:
        return f"ERR {type(error).__name__}: {first_line(error, 160)}"


def norm(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "asDict"):
        value = value.asDict()
    if isinstance(value, dict):
        return [norm(value[key]) for key in sorted(value)]
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    return value


def record(collect: Any) -> dict[str, Any]:
    try:
        rows = collect()
    except Exception as error:
        return {
            "status": "error",
            "error_class": type(error).__name__,
            "error_msg": first_line(error),
            "error_cause": last_line(error),
        }
    lines = sorted(json.dumps(norm(row), sort_keys=True, default=str) for row in rows)
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]
    return {"status": "ok", "n": len(lines), "sha": digest, "sample": lines[:3]}


def frame_for(s: Any, fn: Any, form: str, how: str) -> Any:
    left = s.table("lv_l")
    right = s.table("lv_r")
    cols = ["lid"] if how == "left_semi" else ["lid", "rid"]
    if form == "df_str":
        return left.join(right, "k", how).select(*cols)
    if form == "df_list":
        return left.join(right, ["k"], how).select(*cols)
    if form == "df_two":
        return left.join(right, ["k", "j"], how).select("lid", "rid")
    if form == "df_expr":
        cond = fn.col("l.k") == fn.col("r.k")
        return left.alias("l").join(right.alias("r"), cond, how).select("lid", "rid")
    if form == "cross_where":
        cond = fn.col("l.k") == fn.col("r.k")
        return left.alias("l").crossJoin(right.alias("r")).where(cond).select("lid", "rid")
    if form == "sql_comma":
        return s.sql("SELECT lid, rid FROM lv_l l, lv_r r WHERE l.k = r.k")
    if form == "agg":
        joined = left.join(right, "k", how)
        return joined.agg(fn.count(fn.lit(1)).alias("c"), fn.sum("lid").alias("s"))
    if form == "lateral_str":
        return left.lateralJoin(right, "k", how).select("lid", "rid")
    if form == "lateral_list":
        return left.lateralJoin(right, ["k"], how).select("lid", "rid")
    if form == "sql_on":
        word = "JOIN" if how == "inner" else "LEFT JOIN"
        return s.sql(f"SELECT lid, rid FROM lv_l l {word} lv_r r ON l.k = r.k")
    if form == "sql_using":
        word = "JOIN" if how == "inner" else "LEFT JOIN"
        return s.sql(f"SELECT lid, rid FROM lv_l {word} lv_r USING (k)")
    raise ValueError(form)


def write_cell(s: Any, kind: str, label: str) -> dict[str, Any]:
    table = f"ice.db.i1_wt_{label}_{kind.lower()}"
    try:
        s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        s.sql(f"DROP TABLE IF EXISTS {table}").collect()
        s.sql(
            f"CREATE TABLE {table} (lid BIGINT, rid BIGINT) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        ).collect()
        joined = s.table("lv_l").join(s.table("lv_r"), "k", "inner").select("lid", "rid")
        joined.writeTo(table).append()
    except Exception as error:
        return {
            "status": "error",
            "error_class": type(error).__name__,
            "error_msg": first_line(error),
            "error_cause": last_line(error),
            "stage": "write",
        }
    return record(lambda: s.sql(f"SELECT lid, rid FROM {table}").collect())


def explain_verbose(s: Any, query: str) -> list[dict[str, str]]:
    rows = s.sql(f"EXPLAIN VERBOSE {query}").collect()
    return [{"plan_type": str(row[0]), "plan": str(row[1])} for row in rows]


def join_lines(plan: str) -> list[str]:
    keys = ("Join", "CrossJoinExec", "HashJoinExec", "NestedLoopJoinExec", "Filter")
    return [line.strip()[:240] for line in plan.splitlines() if any(k in line for k in keys)]


def summarize_plan(stages: list[dict[str, str]]) -> dict[str, Any]:
    summary: dict[str, Any] = {"stages": [], "texts": {}}
    first_cross = None
    previous: list[str] = []
    for stage in stages:
        name = stage["plan_type"]
        text = stage["plan"]
        lines = previous if text.strip() == "SAME TEXT AS ABOVE" else join_lines(text)
        previous = lines
        summary["stages"].append({"stage": name, "join_lines": lines})
        is_cross = any(line.startswith("Cross Join") for line in lines)
        if first_cross is None and is_cross and "logical_plan" in name:
            first_cross = name
        if name in PLAN_STAGES and text.strip() != "SAME TEXT AS ABOVE":
            summary["texts"].setdefault(name, "\n".join(t[:220] for t in text.splitlines()[:40]))
    summary["first_cross_stage"] = first_cross
    names = [stage["stage"] for stage in summary["stages"]]
    if "logical_plan after eliminate_cross_join" in names:
        index = names.index("logical_plan after eliminate_cross_join")
        summary["ecj_before_stage"] = names[index - 1]
        summary["ecj_before"] = [
            x for x in summary["stages"][index - 1]["join_lines"] if "Join" in x
        ]
        summary["ecj_after"] = [x for x in summary["stages"][index]["join_lines"] if "Join" in x]
    physical = [stage for stage in summary["stages"] if stage["stage"] == "physical_plan"]
    if physical:
        summary["physical_join"] = [x for x in physical[0]["join_lines"] if "Join" in x]
    return summary


def plan_cell(s: Any, fn: Any, kind: str, form: str, label: str) -> dict[str, Any]:
    rec: dict[str, Any] = {"cell_id": f"plan|{kind}|{form}", "kind": kind, "form": form}
    try:
        build_views("main", s, kind)
        if form == "wt":
            frame = frame_for(s, fn, "df_str", "inner")
            frame.createOrReplaceTempView("jv")
            table = f"ice.db.i1_plan_{label.replace('-', '_')}_{kind.lower()}"
            s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
            s.sql(f"DROP TABLE IF EXISTS {table}").collect()
            s.sql(f"CREATE TABLE {table} (lid BIGINT, rid BIGINT) USING iceberg").collect()
            stages = explain_verbose(s, f"INSERT INTO {table} SELECT lid, rid FROM jv")
        else:
            frame_for(s, fn, form, "inner").createOrReplaceTempView("jv")
            stages = explain_verbose(s, "SELECT * FROM jv")
        rec["plan"] = summarize_plan(stages)
    except Exception as error:
        rec["plan_error"] = f"{type(error).__name__}: {first_line(error)}"
    return rec


def cell(leg: str, s: Any, fn: Any, section: str, kind: str, form: str, how: str) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "cell_id": f"{section}|{kind}|{form}|{how}",
        "kind": kind,
        "form": form,
        "jt": how,
    }
    try:
        frame = frame_for(s, fn, form, how)
    except Exception as error:
        rec["outcome"] = {
            "status": "error",
            "error_class": type(error).__name__,
            "error_msg": first_line(error),
            "error_cause": last_line(error),
            "stage": "build",
        }
        return rec
    rec["outcome"] = record(frame.collect)
    return rec


def build_error_cells(section: str, kind: str, plan: list[tuple[str, str]], error: Any) -> list:
    out = []
    for form, how in plan:
        out.append(
            {
                "cell_id": f"{section}|{kind}|{form}|{how}",
                "kind": kind,
                "form": form,
                "jt": how,
                "outcome": {
                    "status": "error",
                    "error_class": "BuildError",
                    "error_msg": f"{type(error).__name__}: {first_line(error)}",
                    "error_cause": last_line(error),
                },
            }
        )
    return out


def run_kind_section(
    leg: str, s: Any, fn: Any, section: str, kinds: list[str], plan: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    out = []
    for kind in kinds:
        try:
            build_views(leg, s, kind)
            reached = reached_type(leg, s.table("lv_l"))
        except Exception as error:
            out.extend(build_error_cells(section, kind, plan, error))
            continue
        for form, how in plan:
            if form == "wt":
                rec = {"cell_id": f"{section}|{kind}|wt|inner", "kind": kind, "form": "wt"}
                rec["jt"] = "inner"
                rec["outcome"] = write_cell(s, kind, section)
            else:
                rec = cell(leg, s, fn, section, kind, form, how)
            rec["reached"] = reached
            out.append(rec)
        print(f"{leg} {section} {kind} done", flush=True)
    return out


def section_variant(leg: str, s: Any, fn: Any) -> list[dict[str, Any]]:
    plan = [
        ("df_str", "inner"),
        ("df_list", "inner"),
        ("wt", "inner"),
        ("sql_on", "inner"),
        ("df_str", "left"),
    ]
    return run_kind_section(leg, s, fn, "variant", list(VARIANT_KINDS), plan)


def section_failing(leg: str, s: Any, fn: Any) -> list[dict[str, Any]]:
    plan = [
        ("df_str", "inner"),
        ("df_list", "inner"),
        ("df_two", "inner"),
        ("agg", "inner"),
        ("wt", "inner"),
        ("df_str", "left"),
        ("df_str", "left_semi"),
        ("sql_on", "inner"),
        ("sql_using", "inner"),
        ("cross_where", "inner"),
        ("sql_comma", "inner"),
    ]
    return run_kind_section(leg, s, fn, "failing", [*VIEW_KINDS, "L_I64", "NEST_L"], plan)


def section_lateral(leg: str, s: Any, fn: Any) -> list[dict[str, Any]]:
    out = run_kind_section(leg, s, fn, "lateral", list(BASE_KINDS), [("lateral_str", "inner")])
    extra = [("lateral_list", "inner"), ("lateral_str", "left")]
    out += run_kind_section(leg, s, fn, "lateral", ["LV_I64", "L_I64"], extra)
    return out


def native_query(spec: dict[str, Any], form: str) -> str:
    ctes = f"WITH lv_l AS ({left_sql(spec)}), lv_r AS ({right_sql(spec)}) "
    ctes = re.sub(r"(?<!\w)array\(", "make_array(", ctes)
    if form == "native_using":
        return ctes + "SELECT lid, rid FROM lv_l JOIN lv_r USING (k)"
    return ctes + "SELECT lid, rid FROM lv_l l JOIN lv_r r ON l.k = r.k"


def section_native(leg: str, s: Any, fn: Any, native_sql: Any) -> list[dict[str, Any]]:
    out = []
    for kind in BASE_KINDS:
        for form in ("native_using", "native_on"):
            rec = {"cell_id": f"native|{kind}|{form}|inner", "kind": kind, "form": form}
            rec["jt"] = "inner"
            if leg == "spark":
                build_views(leg, s, kind)
                sql_form = "sql_using" if form == "native_using" else "sql_on"
                rec["outcome"] = record(frame_for(s, fn, sql_form, "inner").collect)
            else:
                query = native_query(KINDS[kind], form)
                rec["outcome"] = record(lambda q=query: native_sql(q).collect())
            out.append(rec)
        print(f"{leg} native {kind} done", flush=True)
    return out


def pandas_frame(kind: str, side: str) -> Any:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc

    types = {
        "LV_I64": pa.list_view(pa.int64()),
        "LLV_I64": pa.large_list_view(pa.int64()),
        "REE_I64": pa.run_end_encoded(pa.int32(), pa.int64()),
        "UNION_I64": pa.sparse_union([pa.field("a", pa.int64())]),
        "DICT_LV": pa.dictionary(pa.int32(), pa.list_view(pa.int64())),
    }
    arrow_type = types[kind]
    rows = BASE_LEFT if side == "l" else BASE_RIGHT
    letters = [row[1] for row in rows]
    if kind in ("REE_I64", "UNION_I64"):
        values = [SCALAR[x] for x in letters]
    else:
        values = [LOGICAL[x] for x in letters]
    if kind == "REE_I64":
        array = pc.run_end_encode(pa.array(values, pa.int64()))
    elif kind == "UNION_I64":
        child = pa.array(values, pa.int64())
        type_ids = pa.array([0] * len(values), pa.int8())
        array = pa.UnionArray.from_sparse(type_ids, [child], ["a"])
    elif kind == "DICT_LV":
        array = pa.array(values, pa.list_view(pa.int64())).dictionary_encode()
    else:
        array = pa.array(values, arrow_type)
    key = pd.Series(pd.arrays.ArrowExtensionArray(array))
    if side == "l":
        return pd.DataFrame(
            {
                "lid": [row[0] for row in rows],
                "k": key,
                "id": [row[2] for row in rows],
                "j": [row[3] for row in rows],
            }
        )
    return pd.DataFrame(
        {
            "rid": [row[0] for row in rows],
            "k": key,
            "j": [row[2] for row in rows],
            "rv": [row[3] for row in rows],
        }
    )


def arrow_table(kind: str, side: str) -> Any:
    import pyarrow as pa

    return pa.Table.from_pandas(pandas_frame(kind, side), preserve_index=False)


def route_views(s: Any, route: str, kind: str) -> None:
    if route == "pandas":
        s.createDataFrame(pandas_frame(kind, "l")).createOrReplaceTempView("lv_l")
        s.createDataFrame(pandas_frame(kind, "r")).createOrReplaceTempView("lv_r")
        return
    if route == "polars":
        import polars as pl

        left = pl.from_arrow(arrow_table(kind, "l"))
        right = pl.from_arrow(arrow_table(kind, "r"))
        s.createDataFrame(left).createOrReplaceTempView("lv_l")
        s.createDataFrame(right).createOrReplaceTempView("lv_r")
        return
    if route == "arrow":
        s.createDataFrame(arrow_table(kind, "l")).createOrReplaceTempView("lv_l")
        s.createDataFrame(arrow_table(kind, "r")).createOrReplaceTempView("lv_r")
        return
    raise ValueError(route)


ROUTES = [
    ("pandas", "LV_I64"),
    ("pandas", "LLV_I64"),
    ("pandas", "REE_I64"),
    ("pandas", "UNION_I64"),
    ("pandas", "DICT_LV"),
    ("polars", "LV_I64"),
    ("polars", "LLV_I64"),
    ("arrow", "LV_I64"),
]
ROUTE_TWINS = {
    "LV_I64": "LV_I64",
    "LLV_I64": "LV_I64",
    "REE_I64": "REE_I64",
    "UNION_I64": "REE_I64",
    "DICT_LV": "LV_I64",
}


def section_routes(leg: str, s: Any, fn: Any) -> list[dict[str, Any]]:
    out = []
    for route, kind in ROUTES:
        rec: dict[str, Any] = {
            "cell_id": f"route|{route}:{kind}|df_str|inner",
            "kind": f"{route}:{kind}",
            "form": "df_str",
            "jt": "inner",
        }
        try:
            if leg == "spark":
                build_views(leg, s, ROUTE_TWINS[kind])
            else:
                route_views(s, route, kind)
            rec["reached"] = reached_type(leg, s.table("lv_l"))
            rec["schema_report"] = s.table("lv_l").schema["k"].dataType.simpleString()
        except Exception as error:
            rec["outcome"] = {
                "status": "error",
                "error_class": "BuildError",
                "error_msg": f"{type(error).__name__}: {first_line(error)}",
            }
            out.append(rec)
            continue
        rec["outcome"] = record(frame_for(s, fn, "df_str", "inner").collect)
        out.append(rec)
        print(f"{leg} route {route} {kind} done", flush=True)
    return out


def family_struct(order: str, a: int, b: int) -> str:
    values = {"a": a, "b": b}
    parts = [f"'{name}', CAST({values[name]} AS BIGINT)" for name in order]
    return "named_struct(" + ", ".join(parts) + ")"


def family_wrap(kind: str, element: str, order: str, leg: str) -> str:
    fields = ", ".join(f'"{name}": Int64' for name in order)
    struct = f"Struct({fields})"
    if kind == "plain" or leg == "spark":
        if kind == "fsl2":
            return f"array({element}, {element})"
        return f"array({element})"
    if kind == "large":
        return f"arrow_cast(array({element}), 'LargeList({struct})')"
    if kind == "fsl2":
        return f"arrow_cast(array({element}, {element}), 'FixedSizeList(2, {struct})')"
    return f"arrow_cast(array({element}), 'ListView({struct})')"


def family_view(s: Any, name: str, idcol: str, rows: list[tuple[int, str]]) -> None:
    body = " UNION ALL ".join(f"SELECT {i} AS {idcol}, {v} AS s" for i, v in rows)
    s.sql(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {body}").collect()


def family_views(leg: str, s: Any) -> None:
    for kind in FAMILY_KINDS:
        left = [
            (1, family_wrap(kind, family_struct("ab", 1, 2), "ab", leg)),
            (2, family_wrap(kind, family_struct("ab", 2, 1), "ab", leg)),
        ]
        family_view(s, f"kL_{kind}", "lid", left)
        for order, names in (("rev", "ba"), ("same", "ab")):
            right = [
                (10, family_wrap(kind, family_struct(names, 2, 1), names, leg)),
                (30, family_wrap(kind, family_struct(names, 7, 7), names, leg)),
            ]
            family_view(s, f"kR_{kind}_{order}", "rid", right)


def mixed_pairs() -> list[tuple[str, str]]:
    return [
        (lk, rk) for lk in FAMILY_KINDS for rk in FAMILY_KINDS if (lk == "view") != (rk == "view")
    ]


def mixed_frame(fn: Any, left: Any, right: Any, form: str, how: str) -> Any:
    if form == "join_str":
        return left.join(right, "s", DF_HOW[how]).select("lid", "rid").collect()
    cond = fn.col("l.s") == fn.col("r.s")
    return left.alias("l").join(right.alias("r"), cond, how).select("lid", "rid").collect()


def section_mixed(leg: str, s: Any, fn: Any) -> list[dict[str, Any]]:
    family_views(leg, s)
    out = []
    for lk, rk in mixed_pairs():
        for order in ("rev", "same"):
            left = s.table(f"kL_{lk}")
            right = s.table(f"kR_{rk}_{order}")
            forms = [("join_str", "inner"), ("join_str", "left"), ("df_expr", "inner")]
            for form, how in forms:
                kind = f"{lk}x{rk}_{order}"
                rec = {"cell_id": f"mixed|{kind}|{form}|{how}", "kind": kind, "form": form}
                rec["jt"] = how
                rec["outcome"] = record(
                    lambda a=left, b=right, f=form, h=how: mixed_frame(fn, a, b, f, h)
                )
                out.append(rec)
    print(f"{leg} mixed done, {len(out)} cells", flush=True)
    return out


def section_plan(leg: str, s: Any, fn: Any, label: str) -> list[dict[str, Any]]:
    if leg != "main":
        return []
    out = []
    for kind in PLAN_KINDS:
        for form in ("df_str", "df_list", "df_two", "agg", "wt"):
            out.append(plan_cell(s, fn, kind, form, label))
        print(f"{leg} plan {kind} done", flush=True)
    return out


def section_using(leg: str, s: Any, fn: Any, native_sql: Any) -> list[dict[str, Any]]:
    out = []
    queries = {
        "using_pair": "SELECT lid, rid FROM lv_l JOIN lv_r USING (k)",
        "using_star": "SELECT * FROM lv_l JOIN lv_r USING (k)",
        "using_alias": "SELECT lid, rid FROM lv_l l JOIN lv_r r USING (k)",
        "using_left": "SELECT lid, rid FROM lv_l LEFT JOIN lv_r USING (k)",
        "natural": "SELECT lid, rid FROM lv_l NATURAL JOIN (SELECT rid, k FROM lv_r) r",
    }
    for kind in ("LV_I64", "L_I64"):
        build_views(leg, s, kind)
        for name, query in queries.items():
            rec = {"cell_id": f"using|{kind}|{name}|inner", "kind": kind, "form": name}
            rec["jt"] = "left" if name == "using_left" else "inner"
            rec["outcome"] = record(lambda q=query: s.sql(q).collect())
            out.append(rec)
        if leg == "main":
            rec = {"cell_id": f"using|{kind}|explain_using|inner", "kind": kind}
            rec["form"] = "explain_using"
            try:
                rec["plan"] = summarize_plan(explain_verbose(s, queries["using_pair"]))
            except Exception as error:
                rec["plan_error"] = f"{type(error).__name__}: {first_line(error)}"
            out.append(rec)
            native = native_query(KINDS[kind], "native_using")
            rec = {"cell_id": f"using|{kind}|native_explain|inner", "kind": kind}
            rec["form"] = "native_explain"
            try:
                rows = native_sql(f"EXPLAIN VERBOSE {native}").collect()
                stages = [{"plan_type": str(r[0]), "plan": str(r[1])} for r in rows]
                rec["plan"] = summarize_plan(stages)
            except Exception as error:
                rec["plan_error"] = f"{type(error).__name__}: {first_line(error)}"
            out.append(rec)
    return out


def make_spark(out: Path) -> tuple[Any, Any, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as fn

    s = (
        SparkSession.builder.master("local[4]")
        .appName("lv-item1-spark")
        .config("spark.jars", os.environ["ICEBERG_JAR"])
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{out / 'warehouse-spark'}")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s, fn, None


def make_main(out: Path, label: str) -> tuple[Any, Any, Any]:
    import repark
    from repark import ReparkSession
    from repark.spark import functions as fn

    s = (
        ReparkSession.builder.appName("lv-item1-main")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{out / ('warehouse-' + label)}")
        .getOrCreate()
    )
    return s, fn, getattr(repark, "sql", None)


def meta(leg: str, s: Any, label: str) -> dict[str, Any]:
    import pyarrow

    info: dict[str, Any] = {"leg": leg, "label": label, "python": sys.version.split()[0]}
    info["pyarrow"] = pyarrow.__version__
    info["experiment"] = os.environ.get("REPARK_LV_EXP", "")
    try:
        if leg == "spark":
            import pyspark

            info["pyspark"] = pyspark.__version__
            info["spark_version"] = s.version
        else:
            import repark._native as native

            info["debug_assertions"] = native.__debug_assertions__
    except Exception as error:
        info["meta_error"] = f"{type(error).__name__}: {first_line(error, 160)}"
    return info


SECTIONS = ["plan", "failing", "variant", "lateral", "native", "routes", "mixed", "using"]


def run_leg(leg: str, label: str, sections: list[str], out: Path) -> None:
    s, fn, native_sql = make_spark(out) if leg == "spark" else make_main(out, label)
    raw = out / "raw" / label
    raw.mkdir(parents=True, exist_ok=True)
    info = meta(leg, s, label)
    for section in sections:
        if section == "plan":
            records = section_plan(leg, s, fn, label)
        elif section == "native":
            records = section_native(leg, s, fn, native_sql)
        elif section == "using":
            records = section_using(leg, s, fn, native_sql)
        else:
            records = globals()[f"section_{section}"](leg, s, fn)
        with (raw / f"{section}.jsonl").open("w") as handle:
            handle.write(json.dumps({"meta": info}) + "\n")
            for rec in records:
                rec["leg"] = label
                handle.write(json.dumps(rec) + "\n")
    s.stop()
    print(f"{label} finished", flush=True)


def load(directory: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    records: dict[str, Any] = {}
    info: dict[str, Any] = {}
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if "meta" in row:
                info = row["meta"]
                continue
            records[row["cell_id"]] = row
    return records, info


def label_of(rec: dict[str, Any], ref: dict[str, Any] | None) -> str:
    outcome = rec.get("outcome")
    if outcome is None:
        return "PLAN"
    if outcome["status"] != "ok" and (
        outcome["error_class"] == "BuildError" or str(rec.get("reached", "")).startswith("ERR")
    ):
        return "STRUCK-NO-ROUTE"
    if ref is None or "outcome" not in ref:
        return "NO-REFERENCE"
    oracle = ref["outcome"]
    if outcome["status"] != "ok" and oracle["status"] != "ok":
        return "BOTH-REFUSED"
    if outcome["status"] != "ok":
        return "REFUSED-WHERE-SPARK-ANSWERED"
    if oracle["status"] != "ok":
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if outcome["sha"] == oracle["sha"] and outcome["n"] == oracle["n"]:
        return "EQUAL"
    if rec["form"] == "wt":
        return "WRONG-WRITE"
    if outcome["n"] == SIZE[0] * SIZE[1] and rec["cell_id"].split("|")[0] != "mixed":
        return "CROSS-PRODUCT"
    return "WRONG-ANSWER"


def short(outcome: dict[str, Any] | None) -> str:
    if outcome is None:
        return "-"
    if outcome["status"] == "ok":
        return f"ok n={outcome['n']} sha={outcome['sha']}"
    message = outcome.get("error_msg", "").replace("|", "/")[:110]
    return f"{outcome['error_class']}: {message}"


def render_counts(lines: list[str], title: str, rows: list[tuple[dict[str, Any], str]]) -> None:
    counts = Counter()
    sections = set()
    labels = sorted({label for _, label in rows})
    for rec, label in rows:
        section = rec["cell_id"].split("|")[0]
        counts[(section, label)] += 1
        sections.add(section)
    lines.append(f"## {title}")
    lines.append("")
    lines.append("| section | " + " | ".join(labels) + " | total |")
    lines.append("|" + "---|" * (len(labels) + 2))
    for section in sorted(sections):
        cells = [str(counts[(section, label)]) for label in labels]
        total = sum(counts[(section, label)] for label in labels)
        lines.append(f"| {section} | " + " | ".join(cells) + f" | {total} |")
    lines.append("")


def render(out: Path, labels: list[str]) -> None:
    spark, spark_meta = load(out / "raw" / "spark")
    lines = ["# LISTVIEW-INNER-JOIN-KEY-1 item-1 table", ""]
    lines.append(f"Spark meta: `{json.dumps(spark_meta)}`.")
    lines.append("")
    for label in labels:
        main, main_meta = load(out / "raw" / label)
        if not main:
            continue
        lines.append(f"# Leg `{label}`")
        lines.append("")
        lines.append(f"Meta: `{json.dumps(main_meta)}`.")
        lines.append("")
        rows = [(rec, label_of(rec, spark.get(cid))) for cid, rec in main.items()]
        rows = [(rec, lab) for rec, lab in rows if lab != "PLAN"]
        render_counts(lines, f"Counts for `{label}`: label by section", rows)
        lines.append(f"## Cells for `{label}`")
        lines.append("")
        lines.append("| cell | reached | main outcome | spark outcome | label |")
        lines.append("|---|---|---|---|---|")
        for rec, lab in rows:
            ref = spark.get(rec["cell_id"])
            ref_outcome = ref.get("outcome") if ref else None
            reached = rec.get("reached", "")
            lines.append(
                f"| {rec['cell_id']} | {reached} | {short(rec['outcome'])} | "
                f"{short(ref_outcome)} | {lab} |"
            )
        lines.append("")
    lines.append("# Spark oracle cells")
    lines.append("")
    lines.append("| cell | reached | spark outcome |")
    lines.append("|---|---|---|")
    for cid, rec in spark.items():
        lines.append(f"| {cid} | {rec.get('reached', '')} | {short(rec.get('outcome'))} |")
    (out / "table.md").write_text("\n".join(lines) + "\n")
    print("rendered table.md", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=["spark", "main"])
    parser.add_argument("--label", default="")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--sections", default=",".join(SECTIONS))
    parser.add_argument("--render", default="")
    args = parser.parse_args()
    if args.render:
        render(args.out, args.render.split(","))
        return
    label = args.label or args.leg
    run_leg(args.leg, label, args.sections.split(","), args.out)


if __name__ == "__main__":
    main()

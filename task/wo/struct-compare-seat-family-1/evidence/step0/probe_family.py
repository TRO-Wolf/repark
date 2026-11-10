import argparse
import json
import os
import re
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parent
RAW = OUT / "raw"
WORK = OUT / "work"
PROBE_DIR = Path(os.environ.get("STRUCT_FAMILY_MEMO_PROBES", str(OUT / "memo-probes")))
JAR = os.environ.get("STRUCT_FAMILY_IVY_JAR", "<ivy-jar>")
LEGS = ["spark", "head", "base"]
CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")
PY_ERRORS = {
    "AttributeError",
    "NameError",
    "TypeError",
    "KeyError",
    "IndexError",
    "ImportError",
    "ModuleNotFoundError",
}
DERIVE_MARK = "Cannot create expression literal"
A = "named_struct('a', 1, 'b', 2)"
B = "named_struct('b', 1, 'a', 2)"
C = "named_struct('b', 2, 'a', 1)"
KINDS = ["plain", "large", "fsl2", "view"]
SHAPES = [
    ("rev", "lk", "rrev"),
    ("same", "lk", "rsame"),
    ("name", "lk", "rname"),
    ("wide_rev", "lk", "rwide_rev"),
    ("wide_same", "lk", "rwide_same"),
    ("arity", "lk", "rarity"),
    ("unrel", "lk", "runrel"),
    ("dup", "ldup", "rdup"),
    ("nest", "lnest", "rnest"),
    ("map", "lmap", "rmap"),
    ("arr", "la", "rarr"),
    ("null", "lk", "rnull"),
    ("case", "lk", "rcase"),
]
JOIN_WORDS = [
    ("inner", "JOIN"),
    ("left", "LEFT JOIN"),
    ("right", "RIGHT JOIN"),
    ("full", "FULL JOIN"),
    ("semi", "LEFT SEMI JOIN"),
    ("anti", "LEFT ANTI JOIN"),
]
DF_HOW = {
    "inner": "inner",
    "left": "left",
    "right": "right",
    "full": "full",
    "semi": "left_semi",
    "anti": "left_anti",
}
SEMI = ("semi", "anti")
FORMS = {
    "between": ("{x} BETWEEN {y} AND {y}", lambda sf, x, y: x.between(y, y)),
    "not_between": ("{x} NOT BETWEEN {y} AND {y}", None),
    "case1": ("CASE {x} WHEN {y} THEN true ELSE false END", None),
    "case2": ("CASE {x} WHEN {y} THEN true WHEN " + A + " THEN false ELSE NULL END", None),
    "nullif_lr": ("nullif({x}, {y})", lambda sf, x, y: sf.nullif(x, y)),
    "nullif_rl": ("nullif({y}, {x})", lambda sf, x, y: sf.nullif(y, x)),
}
SEATS = {
    "between": "BETWEEN",
    "not_between": "NOT BETWEEN",
    "case1": "simple CASE",
    "case2": "simple CASE",
    "nullif_lr": "nullif",
    "nullif_rl": "nullif",
}
SQL_DETAILS = {
    "between": [("select", None), ("where", None), ("case_when", None)],
    "not_between": [("select", None), ("where", None)],
    "case1": [("select", None), ("where", None)],
    "case2": [("select", None), ("where", None)],
    "nullif_lr": [("select", None), ("where", "isnull"), ("where", "notnull")],
    "nullif_rl": [("select", None), ("where", "isnull"), ("where", "notnull")],
}
NULLIF_DF = [
    ("df_typed", "typed_select", None),
    ("df_typed", "typed_filter", "isnull"),
    ("df_typed", "typed_filter", "notnull"),
    ("df_str", "str_filter", "isnull"),
    ("df_str", "str_filter", "notnull"),
    ("df_str", "str_select_expr", None),
    ("df_str", "str_expr", None),
]
BOOL_DF = [
    ("df_str", "str_filter", None),
    ("df_str", "str_select_expr", None),
    ("df_str", "str_expr", None),
]
DF_DETAILS = {
    "between": [
        ("df_typed", "typed_select", None),
        ("df_typed", "typed_filter", None),
        ("df_str", "str_filter", None),
        ("df_str", "str_select_expr", None),
        ("df_str", "str_expr", None),
    ],
    "not_between": [],
    "case1": BOOL_DF,
    "case2": BOOL_DF,
    "nullif_lr": NULLIF_DF,
    "nullif_rl": NULLIF_DF,
}
FORMATS = {
    "v2cow": "TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write')",
    "v2mor": "TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')",
    "v3cow": "TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write')",
}


def struct_of(order: Any, a: Any, b: Any, wide: Any = "") -> Any:
    values = {"a": a, "b": b}
    parts = []
    for name in order:
        value = str(values[name])
        if name in wide:
            value = f"CAST({value} AS BIGINT)"
        parts.append(f"'{name}', {value}")
    return "named_struct(" + ", ".join(parts) + ")"


def null_of(kind: Any) -> Any:
    return f"CAST(NULL AS {kind})"


def nested(inner: Any) -> Any:
    return f"named_struct('p', {inner})"


def mapped(inner: Any) -> Any:
    return f"map('k', {inner})"


def arrayed(inner: Any) -> Any:
    return f"array({inner})"


FIXTURES = [
    (
        "lk",
        ["lid", "s"],
        [
            ["1", struct_of("ab", 1, 2)],
            ["2", struct_of("ab", 2, 1)],
            ["3", null_of("STRUCT<a: INT, b: INT>")],
        ],
    ),
    (
        "ldup",
        ["lid", "s"],
        [
            ["1", "named_struct('a', 1, 'a', 2)"],
            ["2", "named_struct('a', 2, 'a', 1)"],
            ["3", null_of("STRUCT<a: INT, a: INT>")],
        ],
    ),
    (
        "lnest",
        ["lid", "s"],
        [
            ["1", nested(struct_of("ab", 1, 2))],
            ["2", nested(struct_of("ab", 2, 1))],
            ["3", null_of("STRUCT<p: STRUCT<a: INT, b: INT>>")],
        ],
    ),
    (
        "lmap",
        ["lid", "s"],
        [
            ["1", mapped(struct_of("ab", 1, 2))],
            ["2", mapped(struct_of("ab", 2, 1))],
            ["3", null_of("MAP<STRING, STRUCT<a: INT, b: INT>>")],
        ],
    ),
    (
        "la",
        ["lid", "s"],
        [
            ["1", arrayed(struct_of("ab", 1, 2))],
            ["2", arrayed(struct_of("ab", 2, 1))],
            ["3", null_of("ARRAY<STRUCT<a: INT, b: INT>>")],
        ],
    ),
    (
        "rrev",
        ["rid", "s"],
        [
            ["10", struct_of("ba", 2, 1)],
            ["30", struct_of("ba", 7, 7)],
            ["40", null_of("STRUCT<b: INT, a: INT>")],
        ],
    ),
    (
        "rsame",
        ["rid", "s"],
        [
            ["10", struct_of("ab", 1, 2)],
            ["30", struct_of("ab", 7, 7)],
            ["40", null_of("STRUCT<a: INT, b: INT>")],
        ],
    ),
    (
        "rname",
        ["rid", "s"],
        [
            ["10", struct_of("ba", 1, 2)],
            ["30", struct_of("ba", 7, 7)],
            ["40", null_of("STRUCT<b: INT, a: INT>")],
        ],
    ),
    (
        "rwide_rev",
        ["rid", "s"],
        [
            ["10", struct_of("ba", 2, 1, "b")],
            ["30", struct_of("ba", 7, 7, "b")],
            ["40", null_of("STRUCT<b: BIGINT, a: INT>")],
        ],
    ),
    (
        "rwide_same",
        ["rid", "s"],
        [
            ["10", struct_of("ab", 1, 2, "ab")],
            ["30", struct_of("ab", 7, 7, "ab")],
            ["40", null_of("STRUCT<a: BIGINT, b: BIGINT>")],
        ],
    ),
    (
        "rarity",
        ["rid", "s"],
        [
            ["10", "named_struct('a', 1, 'b', 2, 'c', 3)"],
            ["30", "named_struct('a', 7, 'b', 7, 'c', 7)"],
            ["40", null_of("STRUCT<a: INT, b: INT, c: INT>")],
        ],
    ),
    (
        "runrel",
        ["rid", "s"],
        [
            ["10", "named_struct('x', 1, 'y', 2)"],
            ["30", "named_struct('x', 7, 'y', 7)"],
            ["40", null_of("STRUCT<x: INT, y: INT>")],
        ],
    ),
    (
        "rdup",
        ["rid", "s"],
        [
            ["10", "named_struct('a', 1, 'a', 2)"],
            ["30", "named_struct('a', 7, 'a', 7)"],
            ["40", null_of("STRUCT<a: INT, a: INT>")],
        ],
    ),
    (
        "rnest",
        ["rid", "s"],
        [
            ["10", nested(struct_of("ba", 2, 1))],
            ["30", nested(struct_of("ba", 7, 7))],
            ["40", null_of("STRUCT<p: STRUCT<b: INT, a: INT>>")],
        ],
    ),
    (
        "rmap",
        ["rid", "s"],
        [
            ["10", mapped(struct_of("ba", 2, 1))],
            ["30", mapped(struct_of("ba", 7, 7))],
            ["40", null_of("MAP<STRING, STRUCT<b: INT, a: INT>>")],
        ],
    ),
    (
        "rarr",
        ["rid", "s"],
        [
            ["10", arrayed(struct_of("ba", 2, 1))],
            ["30", arrayed(struct_of("ba", 7, 7))],
            ["40", null_of("ARRAY<STRUCT<b: INT, a: INT>>")],
        ],
    ),
    (
        "rnull",
        ["rid", "s"],
        [["10", null_of("STRUCT<a: INT, b: INT>")], ["40", null_of("STRUCT<a: INT, b: INT>")]],
    ),
    (
        "rcase",
        ["rid", "s"],
        [
            ["10", "named_struct('B', 1, 'A', 2)"],
            ["30", "named_struct('B', 7, 'A', 7)"],
            ["40", null_of("STRUCT<B: INT, A: INT>")],
        ],
    ),
    ("ik", ["lid", "k"], [["1", "1"], ["2", "2"], ["3", null_of("INT")]]),
    ("irk", ["rid", "k"], [["10", "1"], ["30", "3"], ["40", null_of("INT")]]),
    (
        "dml_src",
        ["id", "s", "r", "q", "v"],
        [
            ["1", struct_of("ab", 1, 2), struct_of("ba", 2, 1), struct_of("ab", 1, 2), "0"],
            ["2", struct_of("ab", 2, 1), struct_of("ba", 2, 1), struct_of("ab", 2, 1), "0"],
            [
                "3",
                null_of("STRUCT<a: INT, b: INT>"),
                struct_of("ba", 5, 5),
                null_of("STRUCT<a: INT, b: INT>"),
                "0",
            ],
            ["4", struct_of("ab", 7, 7), struct_of("ba", 7, 7), struct_of("ab", 7, 7), "0"],
        ],
    ),
]


def view_sql(name: Any, columns: Any, rows: Any) -> Any:
    selects = [
        "SELECT "
        + ", ".join(f"{value} AS {column}" for column, value in zip(columns, row, strict=False))
        for row in rows
    ]
    return f"CREATE OR REPLACE TEMPORARY VIEW {name} AS " + " UNION ALL ".join(selects)


def kind_wrap(kind: Any, element: Any, order: Any, leg: Any) -> Any:
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


def view_statements(leg: Any) -> Any:
    statements = [(name, view_sql(name, columns, rows)) for name, columns, rows in FIXTURES]
    for kind in KINDS:
        left = [
            ["1", kind_wrap(kind, struct_of("ab", 1, 2, "ab"), "ab", leg)],
            ["2", kind_wrap(kind, struct_of("ab", 2, 1, "ab"), "ab", leg)],
        ]
        statements.append((f"kL_{kind}", view_sql(f"kL_{kind}", ["lid", "s"], left)))
        for order, names in (("rev", "ba"), ("same", "ab")):
            right = [
                ["10", kind_wrap(kind, struct_of(names, 2, 1, names), names, leg)],
                ["30", kind_wrap(kind, struct_of(names, 7, 7, names), names, leg)],
            ]
            name = f"kR_{kind}_{order}"
            statements.append((name, view_sql(name, ["rid", "s"], right)))
    return statements


def make(**fields: Any) -> Any:
    cell = {
        "group": None,
        "seat": None,
        "door": None,
        "detail": None,
        "shape": None,
        "case": "insensitive",
        "jt": None,
        "proj": None,
        "twin": None,
        "variant": None,
        "op": None,
        "prepare": False,
    }
    cell.update(fields)
    return cell


def wrapped(text: Any, wrap: Any) -> Any:
    if wrap == "isnull":
        return f"({text}) IS NULL"
    if wrap == "notnull":
        return f"({text}) IS NOT NULL"
    return text


def join_cells(shape: Any, lview: Any, rview: Any, key: Any, case: Any) -> Any:
    out = []
    for jt, word in JOIN_WORDS:
        pairs = ["lid"] if jt in SEMI else ["lid", "rid"]
        for proj, cols in [("pair", ", ".join(pairs)), ("star", "*"), ("key", key)]:
            out.append(
                make(
                    group="J-USING",
                    seat="key-list join",
                    door="sql",
                    detail="using",
                    shape=shape,
                    case=case,
                    jt=jt,
                    proj=proj,
                    twin=f"using|{jt}|{proj}" if proj != "pair" else None,
                    op={
                        "kind": "sql",
                        "text": f"SELECT {cols} FROM {lview} {word} {rview} USING ({key})",
                    },
                )
            )
    for jt, word in JOIN_WORDS[:4]:
        for proj, cols in [("pair", "lid, rid"), ("star", "*")]:
            out.append(
                make(
                    group="J-NATURAL",
                    seat="key-list join",
                    door="sql",
                    detail="natural",
                    shape=shape,
                    case=case,
                    jt=jt,
                    proj=proj,
                    twin=f"natural|{jt}|{proj}" if proj == "star" else None,
                    op={
                        "kind": "sql",
                        "text": f"SELECT {cols} FROM {lview} NATURAL {word} {rview}",
                    },
                )
            )
    for form, detail in [("str", "join_str"), ("list", "join_list")]:
        for jt, _ in JOIN_WORDS:
            pairs = ["lid"] if jt in SEMI else ["lid", "rid"]
            for proj in ("pair", "star", "key"):
                out.append(
                    make(
                        group="J-DF",
                        seat="key-list join",
                        door="df_typed",
                        detail=detail,
                        shape=shape,
                        case=case,
                        jt=jt,
                        proj=proj,
                        twin=f"df_{form}|{jt}|{proj}" if proj != "pair" else None,
                        op={
                            "kind": "df_join",
                            "L": lview,
                            "R": rview,
                            "key": key,
                            "how": DF_HOW[jt],
                            "form": form,
                            "proj": proj,
                            "pairs": pairs,
                        },
                    )
                )
    for jt, word in JOIN_WORDS:
        pairs = ["l.lid"] if jt in SEMI else ["l.lid", "r.rid"]
        for proj, cols in [("pair", ", ".join(pairs)), ("star", "*")]:
            out.append(
                make(
                    group="C-ON",
                    seat="ON control",
                    door="sql",
                    detail="on",
                    shape=shape,
                    case=case,
                    jt=jt,
                    proj=proj,
                    op={
                        "kind": "sql",
                        "text": f"SELECT {cols} FROM {lview} l {word} {rview} r ON l.{key} = "
                        f"r.{key}",
                    },
                )
            )
        out.append(
            make(
                group="C-ON",
                seat="ON control",
                door="df_typed",
                detail="join_on",
                shape=shape,
                case=case,
                jt=jt,
                proj="pair",
                op={
                    "kind": "df_join",
                    "L": lview,
                    "R": rview,
                    "key": key,
                    "how": DF_HOW[jt],
                    "form": "on",
                    "proj": "pair",
                    "pairs": pairs,
                },
            )
        )
    out.append(
        make(
            group="J-LATERAL",
            seat="lateral join",
            door="df_typed",
            detail="lateral_key",
            shape=shape,
            case=case,
            jt="inner",
            proj="key",
            op={"kind": "lateral", "L": lview, "R": rview, "key": key},
        )
    )
    return out


def sql_cmp(form: Any, rhs: Any, detail: Any, wrap: Any, lview: Any, rview: Any) -> Any:
    text = FORMS[form][0].format(x="l.s", y=rhs)
    cond = wrapped(text, wrap)
    source = f"{lview} l" + (f" CROSS JOIN {rview} r" if rview else "")
    ids = "l.lid, r.rid" if rview else "l.lid"
    if detail == "select":
        return f"SELECT {ids}, {cond} AS x FROM {source}"
    if detail == "where":
        return f"SELECT {ids} FROM {source} WHERE {cond}"
    return f"SELECT {ids}, CASE WHEN {text} THEN 'y' ELSE 'n' END AS x FROM {source}"


def cmp_cells(shape: Any, lview: Any, rview: Any, case: Any, rhs: Any) -> Any:
    out = []
    for form in FORMS:
        group = "F-" + form.upper()
        for detail, wrap in SQL_DETAILS[form]:
            label = detail + (f"-{wrap}" if wrap else "")
            out.append(
                make(
                    group=group,
                    seat=SEATS[form],
                    door="sql",
                    detail=label,
                    shape=shape,
                    case=case,
                    op={"kind": "sql", "text": sql_cmp(form, rhs, detail, wrap, lview, rview)},
                )
            )
        for door, detail, wrap in DF_DETAILS[form]:
            label = detail + (f"-{wrap}" if wrap else "")
            out.append(
                make(
                    group=group,
                    seat=SEATS[form],
                    door=door,
                    detail=label,
                    shape=shape,
                    case=case,
                    op={
                        "kind": "cmp",
                        "form": form,
                        "door": door,
                        "detail": detail,
                        "wrap": wrap,
                        "L": lview,
                        "R": rview,
                        "rhs": rhs,
                    },
                )
            )
    return out


def setop_cells(shape: Any, lview: Any, rview: Any, case: Any) -> Any:
    out = []
    for word in ("INTERSECT", "EXCEPT", "UNION ALL"):
        out.append(
            make(
                group="S-SETOP",
                seat="set-op control",
                door="sql",
                detail=word.lower().replace(" ", "_"),
                shape=shape,
                case=case,
                op={"kind": "sql", "text": f"SELECT s FROM {lview} {word} SELECT s FROM {rview}"},
            )
        )
    return out


def kind_cells() -> Any:
    out = []
    for lk in KINDS:
        for rk in KINDS:
            for order in ("rev", "same"):
                for jt, word in [("inner", "JOIN"), ("left", "LEFT JOIN")]:
                    lview = f"kL_{lk}"
                    rview = f"kR_{rk}_{order}"
                    base = {"lk": lk, "rk": rk, "order": order, "jt": jt}
                    out.append(
                        make(
                            group="K-KIND",
                            seat="list kinds",
                            door="sql",
                            detail="using",
                            shape="kind",
                            jt=jt,
                            proj="pair",
                            variant=dict(base, api="sql"),
                            op={
                                "kind": "sql",
                                "text": f"SELECT lid, rid FROM {lview} {word} {rview} USING (s)",
                            },
                        )
                    )
                    out.append(
                        make(
                            group="K-KIND",
                            seat="list kinds",
                            door="df_typed",
                            detail="join_str",
                            shape="kind",
                            jt=jt,
                            proj="pair",
                            variant=dict(base, api="df"),
                            op={
                                "kind": "df_join",
                                "L": lview,
                                "R": rview,
                                "key": "s",
                                "how": DF_HOW[jt],
                                "form": "str",
                                "proj": "pair",
                                "pairs": ["lid", "rid"],
                            },
                        )
                    )
    return out


def cases_for(shape: Any) -> Any:
    return ["sensitive", "insensitive"] if shape == "case" else ["insensitive"]


def gen_query_cells() -> Any:
    cells = []
    for shape, lview, rview in SHAPES:
        for case in cases_for(shape):
            cells += join_cells(shape, lview, rview, "s", case)
            cells += cmp_cells(shape, lview, rview, case, "r.s")
            cells += setop_cells(shape, lview, rview, case)
    cells += join_cells("INT", "ik", "irk", "k", "insensitive")
    for name, rhs in [("B", B), ("A", A), ("C", C)]:
        cells += cmp_cells(f"lit-{name}", "lk", None, "insensitive", rhs)
    cells += kind_cells()
    prepared = []
    for cell in cells:
        if cell["shape"] != "rev" or cell["door"] != "sql" or cell["case"] != "insensitive":
            continue
        chosen = (
            (cell["group"] == "J-USING" and cell["proj"] == "pair")
            or (cell["group"] == "J-NATURAL" and cell["jt"] == "inner" and cell["proj"] == "pair")
            or (cell["group"] == "C-ON" and cell["jt"] == "inner" and cell["proj"] == "pair")
            or cell["group"].startswith("F-")
        )
        if chosen:
            prepared.append(dict(cell, group="P-PREPARE", seat="PREPARE/EXECUTE", prepare=True))
    cells += prepared
    for index, cell in enumerate(cells, 1):
        cell["id"] = f"Q{index:04d}"
    return cells


def join_source(mode: Any, word: Any, rview: Any) -> Any:
    if mode == "using":
        return f"SELECT lid, s, rid FROM lk {word} {rview} USING (s)"
    return f"SELECT lk.lid, lk.s, {rview}.rid FROM lk {word} {rview} ON lk.s = {rview}.s"


def gen_dml_cells() -> Any:
    cells = []
    preds = []
    for bound in ("r", "q"):
        preds += [
            ("between", f"s BETWEEN {bound} AND {bound}", bound),
            ("nullif", f"nullif(s, {bound}) IS NULL", bound),
            ("case", f"CASE s WHEN {bound} THEN true ELSE false END", bound),
            ("eq", f"s = {bound}", bound),
        ]
    for name, lit in (("B", B), ("A", A)):
        preds += [
            ("between", f"s BETWEEN {lit} AND {lit}", name),
            ("nullif", f"nullif(s, {lit}) IS NULL", name),
            ("case", f"CASE s WHEN {lit} THEN true ELSE false END", name),
            ("eq", f"s = {lit}", name),
        ]
    shape_of = {"r": "rev", "q": "same", "B": "lit-rev", "A": "lit-same"}
    for fmt in FORMATS:
        for form, pred, bound in preds:
            for stmt in ("delete", "update", "merge"):
                table = f"hd.vp.p{len(cells) + 1}"
                if stmt == "delete":
                    text = f"DELETE FROM {table} WHERE {pred}"
                elif stmt == "update":
                    text = f"UPDATE {table} SET v = 77 WHERE {pred}"
                else:
                    text = (
                        f"MERGE INTO {table} USING (SELECT id FROM dml_src) src ON {table}.id "
                        f"= src.id "
                        f"WHEN MATCHED AND {pred} THEN UPDATE SET v = 77"
                    )
                cells.append(
                    {
                        "group": "D-PRED",
                        "seat": "DML predicate",
                        "door": "dml",
                        "detail": stmt,
                        "fmt": fmt,
                        "pred_form": form,
                        "pred": pred,
                        "bound": bound,
                        "shape": shape_of[bound],
                        "control": bound in ("q", "A"),
                        "kind": "pred",
                        "table": table,
                        "setup": [
                            f"CREATE TABLE {table} (id INT, s STRUCT<a: INT, b: INT>, r "
                            f"STRUCT<b: INT, a: INT>, q STRUCT<a: INT, b: INT>, v INT) "
                            f"USING iceberg {FORMATS[fmt]}",
                            f"INSERT INTO {table} SELECT id, s, r, q, v FROM dml_src",
                        ],
                        "stmt": text,
                        "select": f"SELECT id FROM {table} WHERE {pred}",
                    }
                )
    for fmt in FORMATS:
        for shape, rview in (("rev", "rrev"), ("same", "rsame")):
            for mode, word in (
                ("using", "JOIN"),
                ("using", "LEFT JOIN"),
                ("on", "JOIN"),
                ("on", "LEFT JOIN"),
            ):
                table = f"hd.vp.i{len(cells) + 1}"
                cells.append(
                    {
                        "group": "D-INSERT",
                        "seat": "DML insert-select",
                        "door": "dml",
                        "detail": f"insert-{mode}-{word.lower().replace(' ', '_')}",
                        "fmt": fmt,
                        "shape": shape,
                        "control": shape == "same",
                        "kind": "insert",
                        "table": table,
                        "setup": [
                            f"CREATE TABLE {table} (lid INT, s STRUCT<a: INT, b: INT>, rid "
                            f"INT) USING iceberg {FORMATS[fmt]}"
                        ],
                        "stmt": f"INSERT INTO {table} {join_source(mode, word, rview)}",
                        "select": None,
                    }
                )
    for fmt in FORMATS:
        for shape, rview in (("rev", "rrev"), ("same", "rsame")):
            for mode, word in (("using", "JOIN"), ("using", "LEFT JOIN"), ("on", "JOIN")):
                table = f"hd.vp.g{len(cells) + 1}"
                cells.append(
                    {
                        "group": "D-MERGE",
                        "seat": "DML merge-from-join",
                        "door": "dml",
                        "detail": f"merge-{mode}-{word.lower().replace(' ', '_')}",
                        "fmt": fmt,
                        "shape": shape,
                        "control": shape == "same",
                        "kind": "merge_join",
                        "table": table,
                        "setup": [
                            f"CREATE TABLE {table} (lid INT, s STRUCT<a: INT, b: INT>, rid "
                            f"INT) USING iceberg {FORMATS[fmt]}",
                            f"INSERT INTO {table} VALUES (1, named_struct('a', 9, 'b', 9), 0), "
                            f"(9, named_struct('a', 9, 'b', 9), 0)",
                        ],
                        "stmt": (
                            f"MERGE INTO {table} tgt USING ({join_source(mode, word, rview)}) "
                            f"src ON tgt.lid = src.lid "
                            f"WHEN MATCHED THEN UPDATE SET rid = src.rid WHEN NOT MATCHED THEN "
                            f"INSERT *"
                        ),
                        "select": None,
                    }
                )
    for index, cell in enumerate(cells, 1):
        cell["id"] = f"M{index:04d}"
    return cells


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
    return {
        "status": "ok",
        "rows": rows,
        "schema": schema.simpleString(),
        "columns": list(frame.columns),
    }


def failure(error: Any) -> Any:
    text = str(error)
    found = CLASS.search(text)
    return {
        "status": "error",
        "class": found.group(1) if found else type(error).__name__,
        "text": " ".join(text.split())[:220],
        "pyerr": type(error).__name__ in PY_ERRORS,
    }


def attempt(thunk: Any) -> Any:
    try:
        return record(thunk())
    except BaseException as error:
        return failure(error)


def try_call(thunk: Any) -> Any:
    try:
        thunk()
        return {"status": "ok"}
    except BaseException as error:
        return failure(error)


def df_join_frame(spark: Any, sf: Any, op: Any) -> Any:
    on = op["form"] == "on"
    left = spark.table(op["L"]).alias("l") if on else spark.table(op["L"])
    right = spark.table(op["R"]).alias("r") if on else spark.table(op["R"])
    key = op["key"]
    if on:
        frame = left.join(right, sf.col("l." + key) == sf.col("r." + key), op["how"])
    elif op["form"] == "list":
        frame = left.join(right, [key], op["how"])
    else:
        frame = left.join(right, key, op["how"])
    if op["proj"] == "star":
        return frame.select("*")
    if op["proj"] == "key":
        return frame.select(key)
    if on:
        return frame.select(*[sf.col(name) for name in op["pairs"]])
    return frame.select(*op["pairs"])


def cmp_frame(spark: Any, sf: Any, op: Any) -> Any:
    base = spark.table(op["L"]).alias("l")
    ids = ["l.lid"]
    if op["R"]:
        base = base.crossJoin(spark.table(op["R"]).alias("r"))
        ids.append("r.rid")
    text = FORMS[op["form"]][0].format(x="l.s", y=op["rhs"])
    wrap = op["wrap"]
    if op["door"] == "df_typed":
        rhs = sf.col(op["rhs"]) if op["R"] else sf.expr(op["rhs"])
        cond = FORMS[op["form"]][1](sf, sf.col("l.s"), rhs)
        if wrap == "isnull":
            cond = cond.isNull()
        elif wrap == "notnull":
            cond = cond.isNotNull()
        if op["detail"] == "typed_select":
            return base.select(*[sf.col(name) for name in ids], cond.alias("x"))
        return base.filter(cond).select(*[sf.col(name) for name in ids])
    expr = wrapped(text, wrap)
    if op["detail"] == "str_filter":
        return base.filter(expr).select(*ids)
    if op["detail"] == "str_select_expr":
        return base.selectExpr(*ids, f"{expr} AS x")
    return base.select(*[sf.col(name) for name in ids], sf.expr(expr).alias("x"))


def lateral_frame(spark: Any, sf: Any, op: Any) -> Any:
    left = spark.table(op["L"]).alias("l")
    right = spark.table(op["R"]).alias("r")
    return left.lateralJoin(right, on=[op["key"]])


def run_query(spark: Any, sf: Any, cell: Any, leg: Any) -> Any:
    op = cell["op"]
    if op["kind"] == "sql":
        if cell["prepare"]:
            return run_prepare(spark, cell, leg)
        return attempt(lambda: spark.sql(op["text"]))
    if op["kind"] == "df_join":
        return attempt(lambda: df_join_frame(spark, sf, op))
    if op["kind"] == "cmp":
        return attempt(lambda: cmp_frame(spark, sf, op))
    return attempt(lambda: lateral_frame(spark, sf, op))


def run_prepare(spark: Any, cell: Any, leg: Any) -> Any:
    name = "p_" + cell["id"].lower()
    text = cell["op"]["text"]
    prep = try_call(lambda: spark.sql(f"PREPARE {name} AS {text}"))
    if leg == "spark":
        result = attempt(lambda: spark.sql(text))
    elif prep["status"] != "ok":
        result = dict(prep)
    else:
        result = attempt(lambda: spark.sql(f"EXECUTE {name}"))
    result["prepare"] = prep
    return result


def run_dml(spark: Any, cell: Any) -> Any:
    table = cell["table"]
    out = {"id": cell["id"]}
    for statement in cell["setup"]:
        result = try_call(lambda text=statement: spark.sql(text).collect())
        if result["status"] != "ok":
            out["setup"] = result
            return out
    out["pre"] = attempt(lambda: spark.table(table))
    out["snap_before"] = attempt(lambda: spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots"))
    if cell.get("select"):
        out["selected"] = attempt(lambda: spark.sql(cell["select"]))
    out["dml"] = try_call(lambda: spark.sql(cell["stmt"]).collect())
    if out["dml"]["status"] == "ok":
        out["post"] = attempt(lambda: spark.table(table))
        out["snap_after"] = attempt(
            lambda: spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots")
        )
    return out


def session(leg: Any, warehouse: Any) -> Any:
    if leg == "spark":
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[2]")
            .config("spark.jars", JAR)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.hd", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
        )
    else:
        from repark import ReparkSession

        builder = ReparkSession.builder.appName("family-" + leg).config(
            "spark.sql.session.timeZone", "UTC"
        )
    builder = builder.config("spark.sql.catalog.hd.type", "hadoop").config(
        "spark.sql.catalog.hd.warehouse", warehouse
    )
    return builder.getOrCreate()


def frame_functions(leg: Any) -> Any:
    if leg == "spark":
        from pyspark.sql import functions as sf
    else:
        from repark.spark import functions as sf
    return sf


def identity(leg: Any) -> Any:
    if leg == "spark":
        return {"leg": leg}
    import repark
    import repark._native

    return {
        "leg": leg,
        "repark_file": repark.__file__,
        "debug_assertions": getattr(repark._native, "__debug_assertions__", None),
        "python": sys.executable,
        "flags_I": sys.flags.isolated,
    }


def run_leg(leg: Any, only: Any, with_dml: Any) -> Any:
    wh = WORK / leg / "wh"
    shutil.rmtree(wh, ignore_errors=True)
    wh.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    spark = session(leg, str(wh))
    (RAW / f"{leg}.identity.json").write_text(json.dumps(identity(leg), indent=1))
    functions = frame_functions(leg)
    setup_errors = []
    for name, statement in view_statements(leg):
        try:
            spark.sql(statement).collect()
        except Exception as error:
            setup_errors.append({"view": name, "error": " ".join(str(error).split())[:300]})
    (RAW / f"{leg}.setup.json").write_text(json.dumps(setup_errors, indent=1))
    with (RAW / f"{leg}.jsonl").open("w") as handle:
        for cell in gen_query_cells():
            if only and only not in cell["group"] and only != cell["id"]:
                continue
            spark.conf.set(
                "spark.sql.caseSensitive", "true" if cell["case"] == "sensitive" else "false"
            )
            result = run_query(spark, functions, cell, leg)
            result["id"] = cell["id"]
            handle.write(json.dumps(result, default=str) + "\n")
            handle.flush()
        spark.conf.set("spark.sql.caseSensitive", "false")
        if with_dml:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.vp").collect()
            for cell in gen_dml_cells():
                if only and only not in cell["group"] and only != cell["id"]:
                    continue
                result = run_dml(spark, cell)
                handle.write(json.dumps(result, default=str) + "\n")
                handle.flush()
    spark.stop()


def ok(rec: Any) -> Any:
    return bool(rec) and rec.get("status") == "ok"


def load(leg: Any) -> Any:
    path = RAW / f"{leg}.jsonl"
    if not path.exists():
        return {}
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return {rec["id"]: rec for rec in records}


def qlabel(s: Any, h: Any) -> Any:
    if s is None or h is None:
        return "MISSING"
    if ok(s) and ok(h):
        return "EQUAL" if s["rows"] == h["rows"] else "WRONG-ANSWER"
    if not ok(s) and not ok(h):
        return "BOTH-REFUSED"
    return "REFUSED-WHERE-SPARK-ANSWERED" if ok(s) else "ACCEPTED-WHERE-SPARK-REFUSED"


def qpayload(rec: Any) -> Any:
    if rec is None:
        return None
    if ok(rec):
        return ["ok", rec["rows"], rec["schema"]]
    return ["err", rec.get("class")]


def short(rec: Any) -> Any:
    if rec is None:
        return "MISSING"
    if ok(rec):
        return (json.dumps(rec["rows"])[:70] + " :: " + rec["schema"])[:140]
    return "ERR " + str(rec.get("class")) + (" (pyerr)" if rec.get("pyerr") else "")


def dml_ok(rec: Any) -> Any:
    return bool(rec) and rec.get("dml", {}).get("status") == "ok" and ok(rec.get("post"))


def snap_delta(rec: Any) -> Any:
    try:
        return rec["snap_after"]["rows"][0][0] - rec["snap_before"]["rows"][0][0]
    except (KeyError, IndexError, TypeError):
        return None


def derived_post(rec: Any, cell: Any) -> Any:
    ids = {row[0] for row in rec["selected"]["rows"]}
    pre = rec["pre"]["rows"]
    if cell["detail"] == "delete":
        return [row for row in pre if row[0] not in ids]
    return [[*row[:4], 77] if row[0] in ids else row for row in pre]


def dml_oracle(s: Any, cell: Any) -> Any:
    if s is None:
        return None, None
    if dml_ok(s):
        return s["post"]["rows"], "spark"
    text = s.get("dml", {}).get("text", "")
    if (
        cell["kind"] == "pred"
        and DERIVE_MARK in text
        and ok(s.get("selected"))
        and ok(s.get("pre"))
    ):
        return derived_post(s, cell), "derived"
    return None, None


def dml_label(s: Any, h: Any, cell: Any) -> Any:
    oracle, kind = dml_oracle(s, cell)
    if oracle is None:
        return "ORACLE-MISSING"
    prefix = "DERIVED-" if kind == "derived" else ""
    if h is None:
        return "MISSING"
    if not dml_ok(h):
        return prefix + "REFUSED-WHERE-SPARK-ANSWERED"
    if h["post"]["rows"] != oracle:
        return prefix + "WRONG-WRITE"
    sd, hd = snap_delta(s), snap_delta(h)
    if kind == "spark" and sd is not None and hd is not None and sd != hd:
        return "SNAPSHOT-DIFF"
    return prefix + "EQUAL"


def dml_payload(rec: Any) -> Any:
    if rec is None:
        return None
    return [
        rec.get("dml", {}).get("status"),
        rec.get("post", {}).get("rows") if rec.get("post") else None,
    ]


def describe(cell: Any) -> Any:
    op = cell.get("op") or {}
    kind = op.get("kind")
    if kind == "sql":
        return op["text"]
    if kind == "df_join":
        return (
            f"DF {op['form']} join {op['L']} x {op['R']} "
            f"key={op['key']} how={op['how']} proj={op['proj']}"
        )
    if kind == "cmp":
        text = FORMS[op["form"]][0].format(x="l.s", y=op["rhs"])
        return (
            f"DF {op['door']} {op['detail']} {op['wrap'] or ''}: "
            f"{text} from {op['L']} x {op['R'] or '-'}"
        )
    if kind == "lateral":
        return f"DF lateralJoin {op['L']} x {op['R']} on=[{op['key']}]"
    return cell.get("stmt", "")


def cell_fable_check() -> Any:
    lines = []
    total = 0
    for leg in LEGS:
        ours = {}
        path = RAW / f"fable-{leg}.jsonl"
        if path.exists():
            for line in path.read_text().splitlines():
                rec = json.loads(line)
                ours[rec["key"]] = rec
        theirs = {}
        for line in (PROBE_DIR / f"{leg}.jsonl").read_text().splitlines():
            rec = json.loads(line)
            theirs[rec["key"]] = rec
        for key in theirs:
            total += 1
            if key not in ours or qpayload(ours[key]) != qpayload(theirs[key]):
                lines.append(
                    f"- {leg}: `{key}` memo={short(theirs[key])} rerun={short(ours.get(key))}"
                )
    return total, lines


def dml_fable_check() -> Any:
    lines = []
    for leg in LEGS:
        path = RAW / f"fable-dml-{leg}.txt"
        ours = {}
        if path.exists():
            for line in path.read_text().splitlines():
                if line.startswith("RESULT "):
                    parts = line.split(" ", 2)
                    ours[parts[1]] = json.loads(parts[2])
        theirs = {}
        for line in (PROBE_DIR / "dml-results.txt").read_text().splitlines():
            if line.startswith("RESULT "):
                parts = line.split(" ", 2)
                theirs[parts[1]] = json.loads(parts[2])
        if leg not in ours or leg not in theirs:
            lines.append(f"- {leg}: missing run or memo result")
            continue
        for key in theirs[leg]:
            if ours[leg].get(key) != theirs[leg][key]:
                lines.append(
                    f"- dml {leg} `{key}` memo={theirs[leg][key]} rerun={ours[leg].get(key)}"
                )
    return lines


def render() -> Any:
    legs = {leg: load(leg) for leg in LEGS}
    cells = gen_query_cells()
    dml_cells = gen_dml_cells()
    int_map = {c["twin"]: c for c in cells if c["shape"] == "INT" and c["twin"]}
    plain_map = {}
    for c in cells:
        v = c.get("variant")
        if v and v["lk"] == "plain" and v["rk"] == "plain":
            plain_map[(v["api"], v["jt"], v["order"])] = c
    rows = []
    for c in cells:
        s, h, b = (legs[leg].get(c["id"]) for leg in LEGS)
        lab = qlabel(s, h)
        bh = "BASE=HEAD" if qpayload(b) == qpayload(h) else "BASE!=HEAD"
        extra = ""
        if c["twin"] and c["shape"] != "INT" and c["proj"] != "pair" and c["twin"] in int_map:
            t = int_map[c["twin"]]
            tl = qlabel(legs["spark"].get(t["id"]), legs["head"].get(t["id"]))
            extra = "INT-TWIN-SAME" if tl == lab else "INT-TWIN-DIFFERS"
        if c.get("variant"):
            v = c["variant"]
            if v["lk"] == "plain" and v["rk"] == "plain":
                extra = "PLAIN-TWIN"
            else:
                t = plain_map.get((v["api"], v["jt"], v["order"]))
                if t is not None:
                    same = qpayload(h) == qpayload(legs["head"].get(t["id"]))
                    extra = "INVARIANT" if same else "VARIANT-DIFFERS"
        schema = "-"
        if ok(s) and ok(h):
            schema = "same" if s["schema"] == h["schema"] else "differs"
        rows.append(
            {
                "cell": c,
                "id": c["id"],
                "lab": lab,
                "bh": bh,
                "extra": extra,
                "schema": schema,
                "s": s,
                "h": h,
                "b": b,
            }
        )
    dml_rows = []
    for c in dml_cells:
        s, h, b = (legs[leg].get(c["id"]) for leg in LEGS)
        lab = dml_label(s, h, c)
        _oracle, kind = dml_oracle(s, c)
        bh = "BASE=HEAD" if dml_payload(b) == dml_payload(h) else "BASE!=HEAD"
        dml_rows.append(
            {
                "cell": c,
                "id": c["id"],
                "lab": lab,
                "bh": bh,
                "kind": kind or "-",
                "s": s,
                "h": h,
                "b": b,
            }
        )
    lines = []
    lines.append("# Family step-0 table (STRUCT-COMPARE-SEAT-FAMILY-1)")
    lines.append("")
    lines.append(
        f"Query cells: {len(rows)}. DML cells: {len(dml_rows)} (per leg). Labels are vs Spark "
        f"(`spark` leg) unless marked base."
    )
    lines.append("")
    seat_head = Counter((r["cell"]["seat"], r["lab"]) for r in rows)
    seat_base = Counter((r["cell"]["seat"], qlabel(r["s"], r["b"])) for r in rows)
    lines.append("## Counts: label vs Spark, per seat (head and base legs)")
    lines.append("")
    lines.append("| seat | leg | label | count |")
    lines.append("|---|---|---|---|")
    for (seat, lab), count in sorted(seat_head.items()):
        lines.append(f"| {seat} | head | {lab} | {count} |")
    for (seat, lab), count in sorted(seat_base.items()):
        lines.append(f"| {seat} | base | {lab} | {count} |")
    lines.append("")
    lines.append("## Counts: head label by seat, door, join")
    lines.append("")
    lines.append("| label | seat | door | join | count |")
    lines.append("|---|---|---|---|---|")
    grid = Counter(
        (r["lab"], r["cell"]["seat"], r["cell"]["door"], r["cell"]["jt"] or "-") for r in rows
    )
    for key, count in sorted(grid.items()):
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {count} |")
    lines.append("")
    lines.append("## DML counts")
    lines.append("")
    lines.append("| label (head vs oracle) | group | fmt | count |")
    lines.append("|---|---|---|---|")
    for key, count in sorted(
        Counter((r["lab"], r["cell"]["group"], r["cell"]["fmt"]) for r in dml_rows).items()
    ):
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {count} |")
    lines.append("")
    wrong = defaultdict(list)
    for r in rows:
        if r["lab"] == "WRONG-ANSWER":
            c = r["cell"]
            wrong[
                (
                    c["group"],
                    c["seat"],
                    c["door"],
                    c["detail"],
                    c["jt"] or "-",
                    c["shape"],
                    c["proj"] or "-",
                    c["case"],
                )
            ].append(r)
    for r in dml_rows:
        if r["lab"] in ("WRONG-WRITE", "DERIVED-WRONG-WRITE"):
            c = r["cell"]
            wrong[
                (c["group"], c["seat"], c["fmt"], c["detail"], "-", c["shape"], "-", r["lab"])
            ].append(r)
    lines.append("## Wrong on head (WRONG-ANSWER and WRONG-WRITE groups)")
    lines.append("")
    lines.append(
        "| group | seat | door/fmt | detail | join | shape | proj | case/label | cells | "
        "example statement |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for key, items in sorted(wrong.items()):
        example = describe(items[0]["cell"]) if "cell" in items[0] else ""
        lines.append(
            f"| {' | '.join(str(k) for k in key)} | {len(items)} | {example.replace('|', '/')} |"
        )
    lines.append("")
    lines.append("## Accepted where Spark refused")
    lines.append("")
    accepted = [r for r in rows if r["lab"] == "ACCEPTED-WHERE-SPARK-REFUSED"]
    lines.append(f"Count: {len(accepted)}")
    for r in accepted:
        lines.append(
            f"- {r['id']} {r['cell']['group']} {r['cell']['detail']} "
            f"{r['cell']['shape']} {r['cell']['case']} :: "
            f"{describe(r['cell'])[:200].replace('|', '/')} :: spark {short(r['s'])}"
        )
    lines.append("")
    lines.append("## Variant-differs (list kinds, head against plain-List twin)")
    lines.append("")
    variant = [r for r in rows if r["extra"] == "VARIANT-DIFFERS"]
    lines.append(
        f"Count: {len(variant)}; INVARIANT: {sum(1 for r in rows if r['extra'] == 'INVARIANT')}"
    )
    for r in variant:
        v = r["cell"]["variant"]
        lines.append(
            f"- {r['id']} lk={v['lk']} rk={v['rk']} order={v['order']} "
            f"jt={v['jt']} api={v['api']}: head {short(r['h'])}"
        )
    lines.append("")
    lines.append("## Base differs from head (ON control shown separately)")
    lines.append("")
    diff = [r for r in rows if r["bh"] == "BASE!=HEAD"]
    lines.append(
        f"Count outside C-ON: {sum(1 for r in diff if r['cell']['group'] != 'C-ON')}; "
        f"C-ON: {sum(1 for r in diff if r['cell']['group'] == 'C-ON')}"
    )
    for r in diff:
        if r["cell"]["group"] == "C-ON":
            continue
        lines.append(
            f"- {r['id']} {r['cell']['group']} {r['cell']['detail']} {r['cell']['shape']} :: "
            f"head {short(r['h'])} :: base {short(r['b'])}"
        )
    lines.append("")
    lines.append("## DML rows: wrong writes, derived-oracle cells, same-order controls")
    lines.append("")
    for r in dml_rows:
        c = r["cell"]
        flag = []
        if r["lab"] in ("WRONG-WRITE", "DERIVED-WRONG-WRITE"):
            flag.append("wrong-write")
        if r["kind"] == "derived":
            flag.append("derived-oracle")
        if c["control"]:
            flag.append("same-order-control")
        if not flag:
            continue
        lines.append(
            f"- {r['id']} {c['fmt']} {c['group']} {c['detail']} shape={c['shape']} "
            f"pred={c.get('pred', '-')} :: {r['lab']} :: base {r['bh']} :: [{', '.join(flag)}]"
        )
    lines.append("")
    total, fable_lines = cell_fable_check()
    lines.append(
        f"## Fable section 1 reproduction: {total} memo cells compared, {len(fable_lines)} differ"
    )
    lines.extend(fable_lines)
    lines.extend(dml_fable_check())
    lines.append("")
    lines.append("## Full query table")
    lines.append("")
    lines.append(
        "| id | group | seat | door | detail | join | shape | proj | case | statement | label "
        "vs Spark | base vs head | twin/variant | schema | spark | head | base |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        c = r["cell"]
        statement = describe(c).replace("|", "/")[:220]
        lines.append(
            f"| {r['id']} | {c['group']} | {c['seat']} | {c['door']} | {c['detail']} "
            f"| {c['jt'] or '-'} | {c['shape']} | {c['proj'] or '-'} | {c['case']} "
            f"| {statement} | {r['lab']} | {r['bh']} | {r['extra'] or '-'} | {r['schema']} "
            f"| {short(r['s']).replace('|', '/')} | {short(r['h']).replace('|', '/')} "
            f"| {short(r['b']).replace('|', '/')} |"
        )
    lines.append("")
    lines.append("## Full DML table")
    lines.append("")
    lines.append(
        "| id | fmt | group | detail | shape | predicate or source | oracle | label (head vs "
        "oracle) | base vs head | spark dml | head dml |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in dml_rows:
        c = r["cell"]
        lines.append(
            f"| {r['id']} | {c['fmt']} | {c['group']} | {c['detail']} | {c['shape']} "
            f"| {(c.get('pred') or c['stmt'])[:120].replace('|', '/')} | {r['kind']} "
            f"| {r['lab']} | {r['bh']} "
            f"| {(r['s'] or {}).get('dml', {}).get('status', 'MISSING') if r['s'] else 'MISSING'} "
            f"| {(r['h'] or {}).get('dml', {}).get('status', 'MISSING') if r['h'] else 'MISSING'} |"
        )
    (OUT / "table.md").write_text("\n".join(lines) + "\n")
    return len(rows), len(dml_rows)


def plan() -> Any:
    query = gen_query_cells()
    dml = gen_dml_cells()
    print("query cells", len(query))
    for key, count in sorted(Counter(c["group"] for c in query).items()):
        print(f"  {key} {count}")
    print("dml cells", len(dml))
    for key, count in sorted(Counter(c["group"] for c in dml).items()):
        print(f"  {key} {count}")
    print("total cells per leg", len(query) + len(dml))


def main() -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=LEGS)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--only", default="")
    parser.add_argument("--no-dml", action="store_true")
    args = parser.parse_args()
    if args.plan:
        plan()
        return
    if args.render:
        query_count, dml_count = render()
        print("rendered", query_count, dml_count)
        return
    run_leg(args.leg, args.only, not args.no_dml)


if __name__ == "__main__":
    main()

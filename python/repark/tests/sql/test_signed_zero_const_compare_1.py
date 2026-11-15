from __future__ import annotations

import math
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession, functions
from repark._native import PyReparkSession

pytestmark = pytest.mark.xdist_group("test_signed_zero_const_compare_1")

CATALOG = "ice"
NAMESPACE = "szc"
TYPES = ("DOUBLE", "FLOAT")
OTHER = {"DOUBLE": "FLOAT", "FLOAT": "DOUBLE"}
SUFFIX = {"DOUBLE": "D", "FLOAT": "F"}
ANSI = ("true", "false")

OPS = {
    "eq": "{l} = {r}",
    "ne": "{l} <> {r}",
    "nseq": "{l} <=> {r}",
    "ndist": "{l} IS NOT DISTINCT FROM {r}",
    "dist": "{l} IS DISTINCT FROM {r}",
    "lt": "{l} < {r}",
    "le": "{l} <= {r}",
    "gt": "{l} > {r}",
    "ge": "{l} >= {r}",
    "in1": "{l} IN ({r})",
    "in2": "{l} IN ({r}, CAST(1 AS {t}))",
    "notin1": "{l} NOT IN ({r})",
    "notin2": "{l} NOT IN ({r}, CAST(1 AS {t}))",
    "between": "{l} BETWEEN {r} AND {r}",
    "nbetween": "{l} NOT BETWEEN {r} AND {r}",
    "case_simple": "CASE {l} WHEN {r} THEN 1 ELSE 0 END",
    "case_searched": "CASE WHEN {l} = {r} THEN 1 ELSE 0 END",
    "if": "IF({l} = {r}, 1, 0)",
}
TRUTH = {
    "eq": True,
    "ne": False,
    "nseq": True,
    "ndist": True,
    "dist": False,
    "lt": False,
    "le": True,
    "gt": False,
    "ge": True,
    "in1": True,
    "in2": True,
    "notin1": False,
    "notin2": False,
    "between": True,
    "nbetween": False,
    "case_simple": 1,
    "case_searched": 1,
    "if": 1,
}
FRAME_OPS = tuple(op for op in OPS if op not in ("case_simple", "if", "dist", "ndist"))
NATIVE_OPS = tuple(op for op in OPS if op != "if")


def neg_forms(t: str) -> dict[str, str]:
    return {
        "dlit": f"-0.0{SUFFIX[t]}",
        "neg": f"-CAST(0 AS {t})",
        "str": f"CAST('-0.0' AS {t})",
        "arith": f"(CAST(0 AS {t}) * -1)",
        "nested": f"CAST(CAST('-0.0' AS {OTHER[t]}) AS {t})",
    }


def pos_forms(t: str) -> dict[str, str]:
    return {
        "dlit": f"0.0{SUFFIX[t]}",
        "cast": f"CAST(0 AS {t})",
        "str": f"CAST('0.0' AS {t})",
        "arith": f"(-CAST(0 AS {t}) * -1)",
        "dec": f"CAST(CAST(0 AS DECIMAL(10,2)) AS {t})",
        "nested": f"CAST(CAST('0.0' AS {OTHER[t]}) AS {t})",
    }


def form(t: str, key: str) -> str:
    sign, name = key.split(".")
    return (neg_forms(t) if sign == "neg" else pos_forms(t))[name]


def pairs() -> list[tuple[str, str, str]]:
    found = []
    for t in TYPES:
        for n in neg_forms(t):
            for p in pos_forms(t):
                found.append((t, f"neg.{n}", f"pos.{p}"))
                found.append((t, f"pos.{p}", f"neg.{n}"))
        found.extend((t, f"neg.{n}", f"neg.{n}") for n in neg_forms(t))
        found.extend((t, f"pos.{p}", f"pos.{p}") for p in pos_forms(t))
    return found


PAIRS = pairs()


def frame_form(t: str, key: str) -> Any:
    sign, name = key.split(".")
    ft, fu = t.lower(), OTHER[t].lower()
    zero = functions.lit(0).cast(ft)
    if name == "dlit":
        value = functions.lit(-0.0 if sign == "neg" else 0.0)
        return value if t == "DOUBLE" else value.cast(ft)
    table = {
        ("neg", "neg"): -zero,
        ("neg", "str"): functions.lit("-0.0").cast(ft),
        ("neg", "arith"): zero * -1,
        ("neg", "nested"): functions.lit("-0.0").cast(fu).cast(ft),
        ("pos", "cast"): zero,
        ("pos", "str"): functions.lit("0.0").cast(ft),
        ("pos", "arith"): (-zero) * -1,
        ("pos", "dec"): functions.lit(0).cast("decimal(10,2)").cast(ft),
        ("pos", "nested"): functions.lit("0.0").cast(fu).cast(ft),
    }
    return table[(sign, name)]


def frame_op(op: str, left: Any, right: Any, t: str) -> Any:
    one = functions.lit(1).cast(t.lower())
    table = {
        "eq": lambda: left == right,
        "ne": lambda: left != right,
        "nseq": lambda: left.eqNullSafe(right),
        "lt": lambda: left < right,
        "le": lambda: left <= right,
        "gt": lambda: left > right,
        "ge": lambda: left >= right,
        "in1": lambda: left.isin(right),
        "in2": lambda: left.isin(right, one),
        "notin1": lambda: ~left.isin(right),
        "notin2": lambda: ~left.isin(right, one),
        "between": lambda: left.between(right, right),
        "nbetween": lambda: ~left.between(right, right),
        "case_searched": lambda: functions.when(left == right, 1).otherwise(0),
    }
    return table[op]()


def rows(frame: Any) -> list[list[Any]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    return [list(row.values()) for row in table.to_pylist()]


def facade_session(ansi: str, root: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("test_signed_zero_const_compare_1")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, str(root))
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}").collect()
    return session


@pytest.fixture(params=ANSI)
def facade(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[ReparkSession]:
    session = facade_session(request.param, tmp_path / "facade")
    yield session
    session.stop()


@pytest.fixture
def native(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PyReparkSession:
    root = tmp_path / "native"
    root.mkdir()
    session = PyReparkSession.native(config={})
    session.register_memory_catalog(CATALOG, str(root))
    session.create_namespace(CATALOG, NAMESPACE, str(root / NAMESPACE))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", session)
    return session


def wrong_cells(answers: list[Any], cells: list[tuple[str, str, str]], op: str) -> list[str]:
    return [
        f"{op} {t} {left} {right}: {answer!r}"
        for (t, left, right), answer in zip(cells, answers, strict=True)
        if answer != TRUTH[op]
    ]


@pytest.mark.parametrize("op", tuple(OPS))
def test_every_constant_zero_pair_answers_like_spark_on_spark_sql(
    facade: ReparkSession, op: str
) -> None:
    columns = ", ".join(
        OPS[op].format(l=form(t, left), r=form(t, right), t=t) + f" AS c{index}"
        for index, (t, left, right) in enumerate(PAIRS)
    )
    answers = rows(facade.sql(f"SELECT {columns}"))[0]
    assert wrong_cells(answers, PAIRS, op) == []


@pytest.mark.parametrize("op", tuple(OPS))
def test_every_constant_zero_pair_answers_like_spark_in_f_expr(
    facade: ReparkSession, op: str
) -> None:
    frame = facade.sql("SELECT 1 AS k").select(
        *(
            functions.expr(OPS[op].format(l=form(t, left), r=form(t, right), t=t)).alias(f"c{i}")
            for i, (t, left, right) in enumerate(PAIRS)
        )
    )
    assert wrong_cells(rows(frame)[0], PAIRS, op) == []


@pytest.mark.parametrize("op", FRAME_OPS)
def test_every_constant_zero_pair_answers_like_spark_on_the_dataframe_api(
    facade: ReparkSession, op: str
) -> None:
    frame = facade.sql("SELECT 1 AS k").select(
        *(
            frame_op(op, frame_form(t, left), frame_form(t, right), t).alias(f"c{i}")
            for i, (t, left, right) in enumerate(PAIRS)
        )
    )
    assert wrong_cells(rows(frame)[0], PAIRS, op) == []


@pytest.mark.parametrize("op", NATIVE_OPS)
def test_every_constant_zero_pair_answers_like_spark_on_the_native_door(
    native: PyReparkSession, op: str
) -> None:
    cells = [cell for cell in PAIRS if "dlit" not in cell[1] + cell[2]]
    columns = ", ".join(
        OPS[op].format(l=form(t, left), r=form(t, right), t=t) + f" AS c{index}"
        for index, (t, left, right) in enumerate(cells)
    )
    answers = rows(repark.sql(f"SELECT {columns}"))[0]
    assert wrong_cells(answers, cells, op) == []


CONTEXT_PAIRS = (
    ("DOUBLE", "neg.str", "pos.str"),
    ("DOUBLE", "neg.neg", "pos.dec"),
    ("DOUBLE", "neg.nested", "pos.str"),
    ("DOUBLE", "neg.arith", "pos.arith"),
    ("FLOAT", "neg.str", "pos.str"),
    ("FLOAT", "neg.nested", "pos.dec"),
)
CONTEXT_OPS = ("eq", "ne", "nseq", "lt", "in2", "between")
CONTEXTS = {
    "where": ("SELECT count(*) FROM (VALUES (1), (2)) AS t(x) WHERE {p}", (2, 0)),
    "having": (
        "SELECT count(*) FROM (SELECT x FROM (VALUES (1), (2)) AS t(x) GROUP BY x HAVING {p}) AS h",
        (2, 0),
    ),
    "join": (
        "SELECT count(*) FROM (VALUES (1), (2)) AS a(x) "
        "JOIN (VALUES (1), (2)) AS b(y) ON a.x = b.y AND {p}",
        (2, 0),
    ),
    "left_join": (
        "SELECT count(b.y) FROM (VALUES (1), (2)) AS a(x) "
        "LEFT JOIN (VALUES (1), (2)) AS b(y) ON a.x = b.y AND {p}",
        (2, 0),
    ),
    "where_or": ("SELECT count(*) FROM (VALUES (1), (2)) AS t(x) WHERE x = 1 OR {p}", (2, 1)),
    "scalar_subquery": (
        "SELECT count(*) FROM (VALUES (1), (2)) AS t(x) WHERE (SELECT {p})",
        (2, 0),
    ),
}


def context_cells() -> list[tuple[str, str, int]]:
    found = []
    for t, left, right in CONTEXT_PAIRS:
        for op in CONTEXT_OPS:
            pred = OPS[op].format(l=form(t, left), r=form(t, right), t=t)
            for name, (template, (when_true, when_false)) in CONTEXTS.items():
                found.append(
                    (name, template.format(p=pred), when_true if TRUTH[op] else when_false)
                )
    return found


CONTEXT_CELLS = context_cells()


def test_every_context_cell_answers_like_spark_on_spark_sql(facade: ReparkSession) -> None:
    wrong = [
        f"{name}: {text}"
        for name, text, expected in CONTEXT_CELLS
        if rows(facade.sql(text)) != [[expected]]
    ]
    assert wrong == []


def test_every_context_cell_answers_like_spark_on_the_native_door(
    native: PyReparkSession,
) -> None:
    wrong = [
        f"{name}: {text}"
        for name, text, expected in CONTEXT_CELLS
        if rows(repark.sql(text)) != [[expected]]
    ]
    assert wrong == []


def test_a_filter_string_and_f_expr_predicate_answer_like_spark(facade: ReparkSession) -> None:
    base = facade.sql("SELECT x FROM (VALUES (1), (2)) AS t(x)")
    wrong = []
    for t, left, right in CONTEXT_PAIRS:
        for op in CONTEXT_OPS:
            pred = OPS[op].format(l=form(t, left), r=form(t, right), t=t)
            expected = 2 if TRUTH[op] else 0
            if base.filter(pred).count() != expected:
                wrong.append(f"filter(str) {pred}")
            if base.filter(functions.expr(pred)).count() != expected:
                wrong.append(f"F.expr {pred}")
    assert wrong == []


def test_a_column_against_constant_zeros_does_not_move(facade: ReparkSession) -> None:
    table = (
        "(VALUES (CAST('-0.0' AS DOUBLE)), (CAST('0.0' AS DOUBLE)), (CAST('1.5' AS DOUBLE)), "
        "(CAST(NULL AS DOUBLE))) AS m(d)"
    )
    bounds = "CAST('-0.0' AS DOUBLE) AND CAST('0.0' AS DOUBLE)"
    for text in (
        f"SELECT count(*) FROM {table} WHERE d BETWEEN {bounds}",
        f"SELECT count(*) FROM {table} WHERE d = CAST('-0.0' AS DOUBLE)",
        f"SELECT count(*) FROM {table} WHERE (CAST(0 AS DOUBLE) * -1) = d",
    ):
        assert rows(facade.sql(text)) == [[2]], text
        assert rows(repark.sql(text)) == [[2]], text


def test_a_temp_view_and_a_prepared_body_answer_like_spark(facade: ReparkSession) -> None:
    pred = "CAST('0.0' AS DOUBLE) = CAST('-0.0' AS DOUBLE)"
    facade.sql(f"CREATE OR REPLACE TEMP VIEW szc_view AS SELECT {pred} AS r").collect()
    assert rows(facade.sql("SELECT r FROM szc_view")) == [[True]]
    assert rows(facade.sql("SELECT count(*) FROM szc_view WHERE r")) == [[1]]
    facade.sql(f"PREPARE szc_prepared AS SELECT {pred} AS r").collect()
    assert rows(facade.sql("EXECUTE szc_prepared")) == [[True]]


DML_PAIRS = (
    ("DOUBLE", "neg.str", "pos.str"),
    ("DOUBLE", "neg.nested", "pos.dec"),
    ("DOUBLE", "neg.arith", "pos.arith"),
    ("DOUBLE", "neg.arith", "pos.cast"),
    ("FLOAT", "neg.str", "pos.str"),
)
DML_OPS = ("eq", "ne", "lt")
SEED = [[1, "a"], [2, "b"], [3, "c"]]
DML = {
    "delete_all": ("DELETE FROM {tbl} WHERE {p}", [], SEED),
    "delete_id": ("DELETE FROM {tbl} WHERE id <= 2 AND {p}", [[3, "c"]], SEED),
    "update": (
        "UPDATE {tbl} SET v = 'u' WHERE id <= 2 AND {p}",
        [[1, "u"], [2, "u"], [3, "c"]],
        SEED,
    ),
    "merge": (
        "MERGE INTO {tbl} AS t USING (SELECT CAST(1 AS BIGINT) AS id, 'm' AS v) AS s "
        "ON t.id = s.id AND {p} WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        [[1, "m"], [2, "b"], [3, "c"]],
        [[1, "a"], [1, "m"], [2, "b"], [3, "c"]],
    ),
    "merge_clause": (
        "MERGE INTO {tbl} AS t USING (SELECT CAST(1 AS BIGINT) AS id, 'm' AS v) AS s "
        "ON t.id = s.id WHEN MATCHED AND {p} THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        [[1, "m"], [2, "b"], [3, "c"]],
        SEED,
    ),
}
NATIVE_DML = ("delete_all", "delete_id", "update")


def dml_cells(statements: tuple[str, ...]) -> list[tuple[str, str, list[list[Any]]]]:
    found = []
    for t, left, right in DML_PAIRS:
        for op in DML_OPS:
            pred = OPS[op].format(l=form(t, left), r=form(t, right), t=t)
            for stmt in statements:
                template, when_true, when_false = DML[stmt]
                found.append((template, pred, when_true if TRUTH[op] else when_false))
    return found


@pytest.mark.parametrize("mode", ("copy-on-write", "merge-on-read"))
def test_every_constant_dml_predicate_writes_like_spark(tmp_path: Path, mode: str) -> None:
    session = facade_session("true", tmp_path / "dml")
    wrong = []
    for index, (template, pred, expected) in enumerate(dml_cells(tuple(DML))):
        tbl = f"{CATALOG}.{NAMESPACE}.d{index}"
        session.sql(
            f"CREATE TABLE {tbl} (id BIGINT, v STRING) USING iceberg TBLPROPERTIES "
            f"('format-version' = '2', 'write.delete.mode' = '{mode}', "
            f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}')"
        ).collect()
        session.sql(f"INSERT INTO {tbl} VALUES (1, 'a'), (2, 'b'), (3, 'c')").collect()
        session.sql(template.format(tbl=tbl, p=pred)).collect()
        state = rows(session.sql(f"SELECT id, v FROM {tbl} ORDER BY id, v"))
        if state != expected:
            wrong.append(f"{template.format(tbl=tbl, p=pred)} -> {state}")
    session.stop()
    assert wrong == []


def test_every_constant_dml_predicate_writes_like_spark_on_the_native_door(
    native: PyReparkSession,
) -> None:
    wrong = []
    for index, (template, pred, expected) in enumerate(dml_cells(NATIVE_DML)):
        tbl = f"{CATALOG}.{NAMESPACE}.n{index}"
        rows(repark.sql(f"CREATE TABLE {tbl} (id BIGINT, v STRING)"))
        rows(repark.sql(f"INSERT INTO {tbl} VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
        rows(repark.sql(template.format(tbl=tbl, p=pred)))
        state = rows(repark.sql(f"SELECT id, v FROM {tbl} ORDER BY id, v"))
        if state != expected:
            wrong.append(f"{template.format(tbl=tbl, p=pred)} -> {state}")
    assert wrong == []


def test_a_nan_constant_comparison_does_not_move(facade: ReparkSession) -> None:
    answer = rows(facade.sql("SELECT (CAST('NaN' AS DOUBLE) * -1) = CAST('NaN' AS DOUBLE)"))
    assert answer == [[True]]
    assert math.isnan(rows(facade.sql("SELECT CAST('NaN' AS DOUBLE) * -1"))[0][0])

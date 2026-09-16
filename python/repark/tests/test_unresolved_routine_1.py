"""UNRESOLVED-ROUTINE-1 pins: every unknown routine refuses with Spark's shape on both doors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import SparkSession
from repark.errors import AnalysisException, ParseException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812

ORACLE = "unresolved_routine_1_spark_oracle.json"

SQL_CELLS: tuple[str, ...] = (
    "UR-SQL-00",
    "UR-SQL-01",
    "UR-SQL-02",
    "UR-SQL-03",
    "UR-SQL-04",
    "UR-SQL-06",
    "UR-SQL-07",
    "UR-SQL-08",
    "UR-SQL-09",
    "UR-SQL-10",
    "UR-SQL-11",
    "UR-SQL-12",
    "UR-SQL-13",
    "UR-SQL-15",
)

BLANKET_NAMES: tuple[str, ...] = (
    "zz_nosuch_fn_alpha",
    "zz_nosuch_fn_beta",
    "zz_nosuch_fn_gamma",
    "zz_nosuch_fn_delta",
    "zz_nosuch_fn_epsilon",
    "zz_nosuch_fn_zeta",
    "zz_nosuch_fn_eta",
    "zz_nosuch_fn_theta",
    "zz_nosuch_fn_iota",
    "zz_nosuch_fn_kappa",
    "zz_nosuch_fn_lambda",
    "zz_nosuch_fn_mu",
    "zz_nosuch_fn_nu",
    "zz_nosuch_fn_xi",
    "zz_nosuch_fn_omicron",
    "zz_nosuch_fn_pi",
    "zz_nosuch_fn_rho",
    "zz_nosuch_fn_sigma",
    "zz_nosuch_fn_tau",
    "zz_nosuch_fn_upsilon",
    "ZzNoSuchFnMixedCase",
    "zz_nosuch_fn_second",
)


@pytest.fixture
def spark() -> Any:
    session = SparkSession.builder.master("local[1]").appName("unresolved-routine-1").getOrCreate()
    yield session
    session.stop()


def _cells() -> dict[str, dict[str, Any]]:
    payload = json.loads(Path(__file__).with_name(ORACLE).read_text())
    assert payload["spark_version"] == "4.1.2"
    return {cell["id"]: cell for cell in payload["cells"]}


def _expected(cell_id: str) -> str:
    message = _cells()[cell_id]["message"]
    assert isinstance(message, str) and message != ""
    return message


@pytest.mark.parametrize("cell_id", SQL_CELLS)
def test_sql_door_unknown_routine_matches_spark_message(spark: Any, cell_id: str) -> None:
    """SQL-door unknown routines carry the full Spark message.

    pins: unresolved-routine-1/C-001, C-002
    """
    cell = _cells()[cell_id]
    with pytest.raises(AnalysisException) as caught:
        spark.sql(cell["expr"]).collect()
    assert str(caught.value) == cell["message"]


def test_sql_door_system_builtin_qualifier_needs_single_part_namespace(spark: Any) -> None:
    """System qualified names refuse with the namespace class. pins: unresolved-routine-1/C-004"""
    cell = _cells()["UR-SQL-05"]
    with pytest.raises(AnalysisException) as caught:
        spark.sql(cell["expr"]).collect()
    assert str(caught.value) == cell["message"]


def test_sql_door_unknown_table_valued_function_names_spark_class(spark: Any) -> None:
    """Unknown table functions resolve to the TVF class. pins: unresolved-routine-1/C-004"""
    cell = _cells()["UR-SQL-17"]
    with pytest.raises(AnalysisException) as caught:
        spark.sql(cell["expr"]).collect()
    assert str(caught.value) == cell["message"]


def test_sql_door_lateral_view_generator_stays_backlogged(spark: Any) -> None:
    """Lateral view parsing belongs to the grammar unit. pins: unresolved-routine-1/C-004"""
    cell = _cells()["UR-SQL-16"]
    with pytest.raises(UnsupportedOperationException, match="LATERAL VIEWS"):
        spark.sql(cell["expr"]).collect()


def test_sql_door_typeof_still_resolves(spark: Any) -> None:
    """Known names keep resolving after the blanket mapping. pins: unresolved-routine-1/C-001"""
    assert _cells()["UR-SQL-14"]["outcome"] == "ok"
    rows = spark.sql("SELECT typeof(1)").collect()
    assert rows[0][0] == "int"


def test_py_door_call_function_stays_green(spark: Any) -> None:
    """call_function already answers the Spark shape. pins: unresolved-routine-1/C-003"""
    cell = _cells()["UR-PY-00"]
    with pytest.raises(AnalysisException) as caught:
        frame = eval(cell["expr"], {"spark": spark, "F": F})
        frame.collect()
    assert str(caught.value) == cell["message"]


@pytest.mark.parametrize("cell_id", ("UR-PY-01", "UR-PY-03"))
def test_py_door_fragment_positions_match_spark(spark: Any, cell_id: str) -> None:
    """Expression and filter fragments position at the fragment.

    pins: unresolved-routine-1/C-002, C-003
    """
    cell = _cells()[cell_id]
    with pytest.raises(AnalysisException) as caught:
        frame = eval(cell["expr"], {"spark": spark, "F": F})
        frame.collect()
    assert str(caught.value) == cell["message"]


def test_py_door_select_expr_answers_spark_class(spark: Any) -> None:
    """selectExpr answers the Spark class; fragment position is run 18b.

    pins: unresolved-routine-1/C-003
    """
    with pytest.raises(AnalysisException, match=r"\[UNRESOLVED_ROUTINE\]") as caught:
        spark.range(1).selectExpr("nosuchfn(id)").collect()
    assert "Cannot resolve routine `nosuchfn` on search path" in str(caught.value)
    assert "SQLSTATE: 42883" in str(caught.value)


@pytest.mark.parametrize("name", BLANKET_NAMES)
def test_sql_door_blanket_mapping_holds_for_any_unknown_name(spark: Any, name: str) -> None:
    """The mapping is blanket: any unknown name refuses alike. pins: unresolved-routine-1/C-005"""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"SELECT {name}(1)").collect()
    text = str(caught.value)
    assert text.startswith(f"[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}`")
    assert "SQLSTATE: 42883; line 1 pos 7" in text


def test_expected_fixture_cells_are_present() -> None:
    """The oracle copy carries every pinned cell id."""
    cells = _cells()
    for cell_id in (*SQL_CELLS, "UR-SQL-05", "UR-SQL-14", "UR-SQL-16", "UR-SQL-17"):
        assert cells[cell_id]["door"] == "sql"
    for cell_id in ("UR-PY-00", "UR-PY-01", "UR-PY-02", "UR-PY-03"):
        assert cells[cell_id]["door"] == "py"
    assert _expected("UR-SQL-00").endswith("; line 1 pos 7")


UR3_SQL_CELLS: tuple[str, ...] = (
    "UR3-SQL-00",
    "UR3-SQL-01",
    "UR3-SQL-02",
    "UR3-SQL-03",
    "UR3-SQL-04",
    "UR3-SQL-05",
    "UR3-SQL-06",
    "UR3-SQL-07",
    "UR3-SQL-08",
    "UR3-SQL-09",
    "UR3-SQL-10",
    "UR3-SQL-11",
    "UR3-SQL-12",
)


@pytest.mark.parametrize("cell_id", UR3_SQL_CELLS)
def test_sql_door_decoy_cells_match_spark_message(spark: Any, cell_id: str) -> None:
    """Decoy literals, comments, quotes and nesting refuse Spark-shaped.

    pins: unresolved-routine-1/C-001, C-002
    """
    cell = _cells()[cell_id]
    with pytest.raises(AnalysisException) as caught:
        spark.sql(cell["expr"]).collect()
    assert str(caught.value) == cell["message"]


def test_sql_door_schema_of_csv_is_divergence_tripwire(spark: Any) -> None:
    """Spark HAS schema_of_csv; repark refuses until 18a lands the name.

    pins: unresolved-routine-1/C-006
    """
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SELECT schema_of_csv('')").collect()
    assert "UNRESOLVED_ROUTINE" in str(caught.value)


def test_sql_door_found_function_arity_is_not_rewritten(spark: Any) -> None:
    """A found name with wrong arity keeps its own error class.

    pins: unresolved-routine-1/C-001
    """
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SELECT abs(1, 2)").collect()
    assert "UNRESOLVED_ROUTINE" not in str(caught.value)


def test_sql_door_double_quoted_name_stays_parse_error(spark: Any) -> None:
    """Double-quoted routine names refuse at parse on both doors.

    pins: unresolved-routine-1/C-001
    """
    with pytest.raises(ParseException):
        spark.sql('SELECT "nosuchfn"(1)').collect()


def test_py_door_leading_space_fragment_positions(spark: Any) -> None:
    """F.expr positions inside the given fragment text.

    pins: unresolved-routine-1/C-002, C-003
    """
    cell = _cells()["UR3-PY-02"]
    with pytest.raises(AnalysisException) as caught:
        frame = eval(cell["expr"], {"spark": spark, "F": F})
        frame.collect()
    assert str(caught.value) == cell["message"]


@pytest.mark.parametrize("cell_id", ("UR3-PY-00", "UR3-PY-03"))
def test_py_door_filter_fragments_answer_spark_class(spark: Any, cell_id: str) -> None:
    """Filter fragments answer the class; positions need 18b.

    pins: unresolved-routine-1/C-003
    """
    cell = _cells()[cell_id]
    with pytest.raises(AnalysisException, match=r"\[UNRESOLVED_ROUTINE\]") as caught:
        frame = eval(cell["expr"], {"spark": spark, "F": F})
        frame.collect()
    assert "Cannot resolve routine `nosuchfn` on search path" in str(caught.value)
    assert "SQLSTATE: 42883" in str(caught.value)


def test_py_door_select_expr_fragment_answers_spark_class(spark: Any) -> None:
    """Multi-fragment selectExpr answers the class; position is 18b.

    pins: unresolved-routine-1/C-003
    """
    cell = _cells()["UR3-PY-01"]
    with pytest.raises(AnalysisException, match=r"\[UNRESOLVED_ROUTINE\]") as caught:
        frame = eval(cell["expr"], {"spark": spark, "F": F})
        frame.collect()
    assert "Cannot resolve routine `nosuchfn` on search path" in str(caught.value)
    assert "SQLSTATE: 42883" in str(caught.value)


def test_remediation_fixture_cells_are_present() -> None:
    """The oracle copy carries every round-2 cell id."""
    cells = _cells()
    for cell_id in (*UR3_SQL_CELLS, "UR3-SQL-13", "UR3-SQL-14", "UR3-SQL-15"):
        assert cells[cell_id]["door"] == "sql"
    for cell_id in ("UR3-PY-00", "UR3-PY-01", "UR3-PY-02", "UR3-PY-03"):
        assert cells[cell_id]["door"] == "py"

"""WO CASESENS-2 DIFF-PROBE fold: the R2 qualifier and R4 ambiguity fixes.

The probe over 1890 statements (2026-09-28, base ``adc26586`` vs PR #881)
found the alias qualifier stored folded, so under ``caseSensitive=true`` a
wrong-case qualifier hit (R2) while the exact spelling missed, and the
overlay twin select refused with the facade text instead of Spark's
``AMBIGUOUS_REFERENCE`` (R4). These pins replay the four must-change cells
plus the guards the fixes flip: the exact spelling answers under true, the
aliased self-join names qualified candidates, and the final-sigma shape
stays refusal-pinned as residue R-CS2-13 (Spark leaves ``ς`` unresolved
while the Java fold matches it, so the class differs).

Spark texts are verbatim live PySpark 4.1.2 (UTC): the ``np`` cells from
``target/diff-probe/out/spark/np-spark.json`` (DIFF-PROBE, banner 4.1.2),
the catalog-join candidates, the sigma refusal and the ``t_lazy_ID``
timing from this round's micro-probe (banner 4.1.2, UTC, same JAR).
Refusals compare per R12 (head plus candidate set, position suffix
stripped); ambiguity texts compare byte-exact past the engine prefix.

The pins live here rather than in ``test_casesens_2.py`` because that file
is at its 1000-line ceiling and ceilings only ratchet down.

pins: casesens-2/C-002, C-006
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from repark import ReparkSession
from repark.spark import functions

_SPARK_FOLDED_QUALIFIER_REFUSAL: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
    "parameter with name `t`.`id` cannot be resolved. Did you mean one of the "
    "following? [`T`.`id`, `T`.`Data`]. SQLSTATE: 42703;",
    "getCondition": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    "getSqlState": "42703",
}

_SPARK_N_FJ_SEL: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `ID` is ambiguous, could be: [`ID`, `ID`]. "
    "SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_F_JOIN_EQ: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `ID` is ambiguous, could be: "
    "[`sc`.`ns`.`t`.`ID`, `sc`.`ns`.`u`.`ID`]. SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_SJ2_SEL_ID: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `ID` is ambiguous, could be: "
    "[`l`.`ID`, `r`.`ID`]. SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_UNI_SIGMA: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
    "parameter with name `ς` cannot be resolved. Did you mean one of the "
    "following? [`é`, `Σ`, `σ`, `İd`, `Éte`]. SQLSTATE: 42703;",  # noqa: RUF001
    "getCondition": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    "getSqlState": "42703",
}

_REPARK_PREFIX: str = "Error during planning: "
_SPARK_POSITION_SUFFIX = "; line "
_SUGGESTION_MARK: str = "Did you mean one of the following? ["


def _open(warehouse: Path) -> ReparkSession:
    """Open a session with hadoop catalog sc."""
    return (
        ReparkSession.builder.appName("casesens-2-diffprobe")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _setup_tables(session: ReparkSession) -> None:
    """Create the probe tables sc.ns.t, sc.ns.u and sc.ns.nt."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.t (id INT, Data STRING, s STRUCT<a: INT>) USING iceberg"
    ).collect()
    session.sql(
        "INSERT INTO sc.ns.t VALUES (1, 'a', named_struct('a', 5)), (2, 'b', named_struct('a', 6))"
    ).collect()
    session.sql("CREATE TABLE sc.ns.u (id INT, Val STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.u VALUES (1, 'x'), (3, 'y')").collect()
    session.sql("CREATE TABLE sc.ns.nt (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.nt VALUES (1, 'a'), (2, 'b')").collect()


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame into rows sorted by repr."""
    return sorted([list(row) for row in frame.collect()], key=repr)


def _dtypes(frame: Any) -> list[list[str]]:
    """Read a frame's dtypes as name/type pairs."""
    return [[name, dtype] for name, dtype in frame.dtypes]


def _condition(error: BaseException) -> Any:
    """Read the attached Spark condition, if the error carries one."""
    method = getattr(error, "getCondition", None)
    if not callable(method):
        return None
    return method()


def _sql_state(error: BaseException) -> Any:
    """Read the attached SQLSTATE, if the error carries one."""
    method = getattr(error, "getSqlState", None)
    if not callable(method):
        return None
    return method()


def _plain_message(message: str) -> str:
    """Strip the engine framings R13 registers, keeping the first line."""
    first = message.splitlines()[0]
    first = first.removeprefix(_REPARK_PREFIX)
    head, _, _ = first.partition(_SPARK_POSITION_SUFFIX)
    return head.rstrip().removesuffix(";").rstrip()


def _bare(entry: str) -> str:
    """Strip one relation qualifier from a candidate entry."""
    _, found, qualified = entry.partition("`.`")
    return f"`{qualified}" if found else entry


def _candidates(message: str) -> set[str]:
    """Read the suggestion-list candidate set from a refusal message."""
    _, _, tail = message.partition(_SUGGESTION_MARK)
    head, _, _ = tail.partition("]")
    return {_bare(entry.strip()) for entry in head.split(",") if entry.strip()}


def _assert_unresolved(error: BaseException, spark: dict[str, Any]) -> None:
    """Replay one unresolved refusal against Spark's recorded answer per R12."""
    assert type(error).__name__ == spark["error"]
    assert _condition(error) == spark["getCondition"]
    assert _sql_state(error) == spark["getSqlState"]
    mine = _plain_message(str(error))
    want = _plain_message(spark["msg"])
    assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0]
    assert _candidates(mine) == _candidates(want)


def _assert_ambiguous(error: BaseException, spark: dict[str, Any]) -> None:
    """Replay one ambiguous refusal byte-exact past the engine prefix."""
    assert type(error).__name__ == spark["error"]
    assert _condition(error) == spark["getCondition"]
    assert _sql_state(error) == spark["getSqlState"]
    assert _plain_message(str(error)) == _plain_message(spark["msg"])


def test_r2_folded_alias_qualifier_refuses_nt_fold(tmp_path: Path) -> None:
    """A folded qualifier on an uppercase alias refuses naming t.id (R2)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "true")
        try:
            session.table("sc.ns.nt").alias("T").select("t.id").collect()
        except Exception as error:
            _assert_unresolved(error, _SPARK_FOLDED_QUALIFIER_REFUSAL)
        else:
            raise AssertionError("n_aT_fold answered instead of refusing")
    finally:
        session.stop()


def test_r2_folded_alias_qualifier_refuses_p6diag_fold(tmp_path: Path) -> None:
    """The p6-diag twin of n_aT_fold refuses the same way (R2).

    DIFF-PROBE records one repro for both cells; only the nt candidate
    set is recorded, so this pin takes the head from Spark's text and
    pins our three-column suggestion list against drift.
    """
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "true")
        try:
            session.table("sc.ns.t").alias("T").select("t.id").collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert _sql_state(error) == "42703"
            mine = _plain_message(str(error))
            want = _plain_message(_SPARK_FOLDED_QUALIFIER_REFUSAL["msg"])
            assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0]
            assert _candidates(mine) == {"`id`", "`Data`", "`s`"}
        else:
            raise AssertionError("aliasT_qualfold_true answered instead of refusing")
    finally:
        session.stop()


def test_r2_exact_alias_qualifier_answers_under_true(tmp_path: Path) -> None:
    """The exact spelling on an uppercase alias answers under true (R2)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "true")
        for name in ("sc.ns.nt", "sc.ns.t"):
            selected = session.table(name).alias("T").select("T.id")
            assert _dtypes(selected) == [["id", "int"]]
            assert _rows(selected) == [[1], [2]]
    finally:
        session.stop()


def test_r4_twin_select_refuses_ambiguous_n_fj_sel(tmp_path: Path) -> None:
    """A folded twin select refuses AMBIGUOUS_REFERENCE with state (R4)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        left = session.createDataFrame([(1, "a"), (2, "b"), (3, None)], ["id", "Data"])
        right = session.createDataFrame([(1, "x"), (3, "y")], ["id", "Val"])
        try:
            left.join(right, left["ID"] == right["id"]).select("ID", "Val").collect()
        except Exception as error:
            _assert_ambiguous(error, _SPARK_N_FJ_SEL)
        else:
            raise AssertionError("n_fj_sel answered instead of refusing")
    finally:
        session.stop()


def test_r4_twin_select_refuses_ambiguous_f_join_eq(tmp_path: Path) -> None:
    """The catalog-twin join names fully qualified candidates (R4)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "false")
        table = session.table("sc.ns.t")
        other = session.table("sc.ns.u")
        try:
            table.join(other, table["ID"] == other["id"]).select("ID", "VAL").collect()
        except Exception as error:
            _assert_ambiguous(error, _SPARK_F_JOIN_EQ)
        else:
            raise AssertionError("f_join_eq answered instead of refusing")
    finally:
        session.stop()


def test_r4_aliased_selfjoin_names_qualified_candidates(tmp_path: Path) -> None:
    """A folded select on an aliased self-join names l and r candidates."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = session.createDataFrame(
            [(1, "a", None, 1.5), (2, "b", 3, None), (2, "b", 3, None)],
            ["id", "Name", "val", "Score"],
        )
        joined = frame.alias("l").join(
            frame.alias("r"), functions.col("l.id") == functions.col("r.id")
        )
        try:
            joined.select("ID").collect()
        except Exception as error:
            _assert_ambiguous(error, _SPARK_SJ2_SEL_ID)
        else:
            raise AssertionError("self-join twin select answered instead of refusing")
    finally:
        session.stop()


def test_r4_final_sigma_refusal_stays_pinned(tmp_path: Path) -> None:
    """The final-sigma twin refusal stays pinned as residue R-CS2-13.

    Spark 4.1.2 leaves ``ς`` unresolved while the Java fold matches both
    sigmas, so the class differs; the routing still refuses ambiguous.
    """
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = session.createDataFrame(
            [(1, 2, 3, 4, 5, 6, 7)],
            ["Ünï", "Éte", "straße", "İd", "σ", "Σ", "é"],  # noqa: RUF001
        )
        try:
            frame.select("ς").collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "AMBIGUOUS_REFERENCE"
            assert _sql_state(error) == "42704"
            assert _plain_message(str(error)) == (
                "[AMBIGUOUS_REFERENCE] Reference `ς` is ambiguous, could be: [`ς`, `ς`]. "
                "SQLSTATE: 42704"
            )
        else:
            raise AssertionError("final sigma answered instead of refusing")
        assert _SPARK_UNI_SIGMA["getCondition"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        assert _SPARK_UNI_SIGMA["getSqlState"] == "42703"
    finally:
        session.stop()

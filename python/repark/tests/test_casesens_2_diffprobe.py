"""WO CASESENS-2 DIFF-PROBE fold: the R2 qualifier and R4 ambiguity fixes.

The probe over 1890 statements (2026-09-28, base ``adc26586`` vs PR #881)
found the alias qualifier stored folded, so under ``caseSensitive=true`` a
wrong-case qualifier hit (R2) while the exact spelling missed, and the
overlay twin select refused with the facade text instead of Spark's
``AMBIGUOUS_REFERENCE`` (R4).

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


def test_r4_final_sigma_refuses_unresolved_like_spark(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = session.createDataFrame(
            [(1, 2, 3, 4, 5, 6, 7)],
            ["\u00dcn\u00ef", "\u00c9te", "stra\u00dfe", "\u0130d", "\u03c3", "\u03a3", "\u00e9"],
        )
        try:
            frame.select("\u03c2").collect()
        except Exception as error:
            assert type(error).__name__ == _SPARK_UNI_SIGMA["error"]
            assert _condition(error) == _SPARK_UNI_SIGMA["getCondition"]
            assert _sql_state(error) == _SPARK_UNI_SIGMA["getSqlState"]
            mine = _plain_message(str(error))
            want = _plain_message(_SPARK_UNI_SIGMA["msg"])
            assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0]
            assert _candidates(want) <= _candidates(mine)
        else:
            raise AssertionError("final sigma answered instead of refusing")
    finally:
        session.stop()


_SPARK_DOTLESS_REFUSAL: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
    "parameter with name `\u0131d` cannot be resolved. Did you mean one of the "
    "following? [`id`, `v`]. SQLSTATE: 42703;",
    "getCondition": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    "getSqlState": "42703",
}

_SPARK_WRONG_CASE_ALIAS_FILTER: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
    "parameter with name `tb`.`id` cannot be resolved. Did you mean one of the "
    "following? [`Tb`.`id`, `Tb`.`Val`, `Tb`.`Data`]. SQLSTATE: 42703; line 1 pos 0;",
    "getCondition": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    "getSqlState": "42703",
}

_SPARK_TRUE_JOIN_BARE_ID: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`id`, `id`]. "
    "SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_ALIAS_JOIN_FILL: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `data` is ambiguous, could be: "
    "[`L`.`data`, `R`.`data`]. SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_SELF_USING: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `Data` is ambiguous, could be: "
    "[`Data`, `Data`]. SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_ALIAS_USING: dict[str, Any] = {
    "error": "AnalysisException",
    "msg": "[AMBIGUOUS_REFERENCE] Reference `Data` is ambiguous, could be: "
    "[`x`.`Data`, `y`.`Data`]. SQLSTATE: 42704",
    "getCondition": "AMBIGUOUS_REFERENCE",
    "getSqlState": "42704",
}

_SPARK_SORT_AMBIGUITY_HEAD: str = (
    "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter "
    "with name `id` cannot be resolved. "
)


def _refusal(run: Any) -> BaseException:
    try:
        run()
    except Exception as error:
        return error
    raise AssertionError("answered instead of refusing")


def _base_frame(session: ReparkSession) -> Any:
    return session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "Data", "Val"])


def test_rc2_1_alias_qualified_filter_strings_bind_by_rule(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = _base_frame(session)
        other = session.createDataFrame([(1, "x"), (2, "y")], ["id", "W"])
        for filtered in (
            frame.alias("T").filter("T.id > 1"),
            frame.alias("T").where("t.id > 1"),
            frame.alias("Tb").where("tb.id > 1"),
            frame.alias("T").pl.filter("T.id > 1").spark,
        ):
            assert filtered.columns == ["id", "Data", "Val"]
            assert _rows(filtered) == [[2, "b", 20]]
        joined = frame.alias("T").join(
            other.alias("E"), functions.col("T.id") == functions.col("E.id")
        )
        assert _rows(joined.filter("T.Val > 10")) == [[2, "b", 20, 2, "y"]]
        session.conf.set("spark.sql.caseSensitive", "true")
        exact = _base_frame(session)
        assert _rows(exact.alias("T").where("T.id > 1")) == [[2, "b", 20]]
        wrong = _refusal(lambda: exact.alias("Tb").where("tb.id > 1").collect())
        assert _condition(wrong) == _SPARK_WRONG_CASE_ALIAS_FILTER["getCondition"]
        assert _sql_state(wrong) == _SPARK_WRONG_CASE_ALIAS_FILTER["getSqlState"]
        mine = _plain_message(str(wrong)).split(_SUGGESTION_MARK)[0]
        assert (
            mine == _plain_message(_SPARK_WRONG_CASE_ALIAS_FILTER["msg"]).split(_SUGGESTION_MARK)[0]
        )
    finally:
        session.stop()


def test_rc2_2_true_condition_joins_over_shared_names_answer(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        left = session.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
        right = session.createDataFrame([(1, "x"), (3, "y")], ["id", "w"])
        joined = left.join(right, left.id == right.id)
        assert joined.columns == ["id", "v", "id", "w"]
        assert _rows(joined) == [[1, "a", 1, "x"]]
        picked = left.join(right, left["id"] == right["id"]).select(left["id"], "v", "w")
        assert _rows(picked) == [[1, "a", "x"]]
        assert left.join(right, left["id"] == right["id"], "left").count() == 2
        assert left.join(right, left.id == right.id - 1).count() == 1
        _assert_ambiguous(_refusal(lambda: joined.select("id").collect()), _SPARK_TRUE_JOIN_BARE_ID)
    finally:
        session.stop()


def test_rc2_3_lookup_lowers_and_the_resolver_folds(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        plain = session.createDataFrame([(1, 2)], ["id", "v"])
        _assert_unresolved(
            _refusal(lambda: plain.select("\u0131d").collect()), _SPARK_DOTLESS_REFUSAL
        )
        column = _refusal(lambda: plain.filter(functions.col("\u0131d").isNotNull()).collect())
        assert _condition(column) == _SPARK_DOTLESS_REFUSAL["getCondition"]
        assert _sql_state(column) == _SPARK_DOTLESS_REFUSAL["getSqlState"]
        assert (
            _plain_message(str(column)).split(_SUGGESTION_MARK)[0]
            == (_plain_message(_SPARK_DOTLESS_REFUSAL["msg"]).split(_SUGGESTION_MARK)[0])
        )
        assert plain.withColumnRenamed("\u0131d", "z").columns == ["z", "v"]
        sigmas = session.createDataFrame([(1, 2)], ["\u03c3", "\u03c2"])
        assert _rows(sigmas.select("\u03c3")) == [[1]]
        appended = plain.withColumns({"\u0131": functions.lit(1), "I": functions.lit(2)})
        assert appended.columns == ["id", "v", "\u0131", "I"]
        assert _rows(appended) == [[1, 2, 1, 2]]
        for keys, twin in (
            (("\u00dcN\u00cf", "\u00dcn\u00ef"), "\u00fcn\u00ef"),
            (("\u0391\u03a3", "\u03b1\u03c2"), "\u03b1\u03c2"),
        ):
            clash = _refusal(
                lambda keys=keys: plain.withColumns(
                    {keys[0]: functions.lit(1), keys[1]: functions.lit(2)}
                ).collect()
            )
            assert _condition(clash) == "COLUMN_ALREADY_EXISTS"
            assert _plain_message(str(clash)) == (
                f"[COLUMN_ALREADY_EXISTS] The column `{twin}` already exists. Choose "
                "another name or rename the existing column. SQLSTATE: 42711"
            )
        upper = session.createDataFrame([(1, 2)], ["\u03a3", "v"])
        assert _rows(upper.select("\u03c3")) == [[1]]
        umlaut = session.createDataFrame([(1, 2)], ["\u00dcn\u00ef", "v"])
        assert umlaut.select("\u00fcn\u00ef").columns == ["\u00fcn\u00ef"]
        eszett = session.createDataFrame([(1, 2)], ["stra\u00dfe", "v"])
        assert type(_refusal(lambda: eszett.select("STRASSE").collect())).__name__ == (
            "AnalysisException"
        )
        assert eszett.withColumn("STRASSE", functions.lit(0)).columns == [
            "stra\u00dfe",
            "v",
            "STRASSE",
        ]
    finally:
        session.stop()


def test_rc2_4_one_attribute_projected_twice_orders(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = _base_frame(session)
        doubled = frame.select("id", "id", "Data")
        ordered = doubled.orderBy(functions.col("id"))
        assert ordered.columns == ["id", "id", "Data"]
        assert [list(row) for row in ordered.collect()] == [[1, 1, "a"], [2, 2, "b"]]
        descending = doubled.orderBy(functions.col("id").desc())
        assert [list(row) for row in descending.collect()] == [[2, 2, "b"], [1, 1, "a"]]
        twins = frame.select(frame["id"], frame["id"].alias("ID"))
        _assert_ambiguous(
            _refusal(lambda: twins.select(functions.col("id")).collect()),
            _SPARK_TRUE_JOIN_BARE_ID,
        )
    finally:
        session.stop()


def test_rc2_5_alias_join_children_drop_rename_and_fill(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        left = _base_frame(session)
        right = session.createDataFrame([(1, "x", 7), (3, "y", 8)], ["ID", "Data", "W"])
        joined = left.alias("L").join(
            right.alias("R"), functions.col("L.id") == functions.col("R.ID")
        )
        dropped = joined.drop(functions.col("L.id"))
        assert dropped.columns == ["Data", "Val", "ID", "Data", "W"]
        assert _rows(dropped) == [["a", 10, 1, "x", 7]]
        renamed = joined.withColumnRenamed("data", "z")
        assert renamed.columns == ["id", "z", "Val", "ID", "z", "W"]
        bare = joined.drop("id")
        assert bare.columns == ["Data", "Val", "Data", "W"]
        assert _rows(bare) == [["a", 10, "x", 7]]
        _assert_ambiguous(
            _refusal(lambda: joined.fillna("q", subset=["data"]).collect()),
            _SPARK_ALIAS_JOIN_FILL,
        )
        order = _refusal(lambda: joined.orderBy(functions.col("id")).collect())
        assert _condition(order) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        assert _sql_state(order) == "42703"
        assert _plain_message(str(order)).split(_SUGGESTION_MARK)[0] == (_SPARK_SORT_AMBIGUITY_HEAD)
    finally:
        session.stop()


def test_rc2_6_true_using_self_join_twins_refuse_ambiguous(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        frame = _base_frame(session)
        _assert_ambiguous(
            _refusal(lambda: frame.join(frame, "id").select("Data").collect()),
            _SPARK_SELF_USING,
        )
        aliased = _refusal(
            lambda: frame.alias("x").join(frame.alias("y"), "id").select("Data").collect()
        )
        assert _condition(aliased) == _SPARK_ALIAS_USING["getCondition"]
        assert _sql_state(aliased) == _SPARK_ALIAS_USING["getSqlState"]
        head = "[AMBIGUOUS_REFERENCE] Reference `Data` is ambiguous, could be: ["
        assert _plain_message(str(aliased)).startswith(head)
        assert _SPARK_ALIAS_USING["msg"].startswith(head)
    finally:
        session.stop()

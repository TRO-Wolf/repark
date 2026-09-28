"""WO CASESENS-1 slice 1: nested scopes keep the written spelling; MERGE folds its source.

The oracle is ``casesens_1_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0,
measured 2026-09-27): each step carries its door, statement, the
``caseSensitive`` value in force, and Spark's answer. Every success step
compares column names, types and rows exactly after the probe's normalization
(values normalized as the probe normalized them, rows sorted by repr). DML and
DDL success steps assert success only: RePark's empty-result framing (``[[]]``
where Spark answers ``[]``, a count row for INSERT) predates this unit and is
out of its fence. Refusal steps compare the class, condition, SQLSTATE, the
message head up to ``Did you mean one of the following? [`` and the candidate
set (R12), after stripping Spark's ``; line L pos P`` suffix and RePark's
``Error during planning: `` prefix (R13). S1b lands R-CS1-1 and C-003:
``p1/r5_cte_outer`` joins the nested legs and the catalog-view legs replay in
``test_s1_catalog_view_keeps_its_spelling``. S2 replays the
``caseSensitive=true`` SQL-door legs: wrong-case references refuse naming the
written spelling in every scope, exact spellings answer, relation and CTE names
match exactly, and wrong-case DML refuses leaving the table unchanged.
Candidate comparison strips relation qualification (out of scope
per the WO); three legs compare against RePark's recorded rendering where it
differs from Spark's for a registered reason (``p1/cs_order_ID`` and
``p3/cs_order_alias`` list the whole scope, ``p3/cs_rel_alias_upper`` names
`` `T.id` ``), and the ``TABLE_OR_VIEW_NOT_FOUND`` / ``FIELD_NOT_FOUND`` /
function-naming legs assert the refusal or rows with RePark's recorded text
(ledger R-CS1-2 … R-CS1-7). S3 replays the ``caseSensitive=true``
DataFrame-door legs where written names reach Rust: ``filter``, ``describe``,
``join(on=)``, ``unionByName``, ``drop``, exact-hit ``select``, the window
leg and ``selectExpr`` refuse or answer per the oracle, and the four
``false`` legs answer named with the written spelling; ``describe`` with
explicit columns resolves one name per call. The six pre-bound legs (select
string / ``F.col`` / lowercase-data, ``orderBy``, ``groupBy``, string
``filter``) stay unpinned (ledger R-CS1-10, descoped to CASESENS-2: the
facade pre-binds them in ``dataframe/core.py`` before Rust sees the written
name). The ``unionByName`` pin asserts the class and the byte-exact message
and records the condition gap (ledger R-CS1-9: Spark reports
``_LEGACY_ERROR_TEMP_1201``, RePark carries none). S3 also pins
``p3/cs_temp_view_upper`` (ledger R-CS1-8 closed: the rule-aware probe
refuses; the exact and ``false`` legs answer). S4 replays the case-twin
legs: twin outputs answer with both spellings, a reference into the twins
refuses ``AMBIGUOUS_REFERENCE`` byte-exact, and ``CREATE TABLE``, CTAS,
``CREATE VIEW`` and ``CREATE TEMPORARY VIEW`` with twin columns refuse
``COLUMN_ALREADY_EXISTS`` (the ``tw_view`` leg strips Spark's recorded
trailing ``;``); the positional insert answers and ``writeTo`` routes through
CTAS into the same refusal. S5 replays the Iceberg DDL legs: partition field
names and transform sources bind exactly under both settings, the write order
follows the session rule, and identifier fields bind exactly; each ``_meta``
step asserts the recorded spec, sort and identifier state from the table's
metadata file. Partition-name and sort/source refusals assert RePark's
``PySparkException`` text after its ``DataInvalid => `` prefix against Spark's
``msg`` or ``java`` line; identifier refusals match Spark's
``IllegalArgumentException`` text with no prefix.

pins: casesens-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012,
    C-013, C-014, C-015, C-016, C-017, C-018
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions

ORACLE_PATH: Path = Path(__file__).with_name("casesens_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))["steps"]

_NESTED_KEYS: tuple[str, ...] = (
    "p1/r5_subq_inner_ID",
    "p1/r5_cte_ID",
    "p1/r5_cte_outer",
    "p1/r5_cte_mixed",
    "p1/r5_union_subq",
    "p1/r5_subq_ID",
    "p1/r5_subq_both",
    "p1/r5_subq_alias_qual",
    "p1/r5_in_subq",
    "p1/r5_temp_view_star",
    "p1/r5_temp_view_lower",
    "p3/n_nested2",
    "p3/n_join_derived",
    "p3/n_col_alias_list",
    "p3/n_df_sql_subq",
    "p3/n_group_having",
    "p3/n_cte_star_outer_upper",
    "p3/n_exists",
)

_SPARK_POSITION_SUFFIX: re.Pattern[str] = re.compile(r"; line \d+ pos \d+")
_REPARK_PREFIX: str = "Error during planning: "
_SUGGESTION_MARK: str = "Did you mean one of the following? ["


def _open(warehouse: Path) -> ReparkSession:
    """Open a session with hadoop catalog sc."""
    return (
        ReparkSession.builder.appName("casesens-1-s1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _setup_tables(session: ReparkSession) -> None:
    """Create the probe's sc tables and the tv temp view."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.t (id INT, Data STRING, s STRUCT<a: INT>) USING iceberg"
    ).collect()
    session.sql(
        "INSERT INTO sc.ns.t VALUES (1, 'a', named_struct('a', 5)), (2, 'b', named_struct('a', 6))"
    ).collect()
    session.sql("CREATE TABLE sc.ns.u (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.u VALUES (1, 'x'), (5, 'y')").collect()
    session.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT ID, DATA FROM sc.ns.t").collect()


def _norm(value: Any) -> Any:
    """Normalize one collected value the way the recording probe did."""
    if hasattr(value, "asDict"):
        return {key: _norm(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _norm(item) for key, item in value.items()}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame into probe-normalized rows sorted by repr."""
    return sorted([[_norm(value) for value in row] for row in frame.collect()], key=repr)


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
    return _SPARK_POSITION_SUFFIX.sub("", first)


def _bare(entry: str) -> str:
    """Strip one relation qualifier from a candidate entry."""
    _, found, qualified = entry.partition("`.`")
    return f"`{qualified}" if found else entry


def _candidates(message: str) -> set[str]:
    """Read the suggestion-list candidate set from a refusal message."""
    _, _, tail = message.partition(_SUGGESTION_MARK)
    head, _, _ = tail.partition("]")
    return {_bare(entry.strip()) for entry in head.split(",") if entry.strip()}


def _s2_expect_msg(key: str, spark_msg: str) -> str:
    """Render RePark's recorded message where it differs from Spark's (S2)."""
    if key == "p1/cs_order_ID":
        return spark_msg.replace("[`id`]", "[`id`, `Data`, `s`]")
    if key == "p3/cs_order_alias":
        return spark_msg.replace("[`X`]", "[`X`, `id`, `Data`]")
    if key == "p3/cs_rel_alias_upper":
        return spark_msg.replace("`T`.`id`", "`T.id`")
    return spark_msg


def _assert_error(
    key: str, error: BaseException, spark: dict[str, Any], expect_msg: str | None = None
) -> None:
    """Replay one oracle refusal against Spark's recorded answer per R12."""
    assert type(error).__name__ == spark["error"], key
    assert _condition(error) == spark.get("getCondition"), key
    assert _sql_state(error) == spark.get("getSqlState"), key
    mine = _plain_message(str(error))
    want = _plain_message(expect_msg if expect_msg is not None else spark["msg"])
    assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0], key
    if _SUGGESTION_MARK in want:
        assert _candidates(mine) == _candidates(want), key


def _sql_of(step: dict[str, Any]) -> str:
    """Read the runnable SQL of an oracle step, unwrapping dataframe lambdas."""
    statement = step["statement"]
    prefix = 'lambda: S.sql("'
    if statement.startswith(prefix) and statement.endswith('")'):
        return statement[len(prefix) : -len('")')]
    return statement


def _assert_step(session: ReparkSession, key: str, expect_msg: str | None = None) -> None:
    """Run one oracle step under its recorded flag and compare with Spark."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    if "error" in spark:
        try:
            session.sql(_sql_of(step)).collect()
        except Exception as error:
            _assert_error(key, error, spark, expect_msg)
        else:
            raise AssertionError(f"{key} answered instead of refusing")
        return
    frame = session.sql(_sql_of(step))
    if spark["cols"]:
        assert _dtypes(frame) == spark["cols"], key
        assert _rows(frame) == spark["rows"], key
    else:
        frame.collect()


_S2_TRUE_KEYS: tuple[str, ...] = (
    "p1/cs_sel_ID",
    "p1/cs_join_ID",
    "p1/cs_join_on_ID",
    "p1/cs_subq_ID",
    "p1/cs_cte_ID",
    "p1/cs_where_ID",
    "p1/cs_order_ID",
    "p1/cs_group_ID",
    "p1/cs_sel_Data",
    "p1/cs_struct_A",
    "p1/cs_insert_cols",
    "p1/cs_update_where",
    "p1/cs_delete_where",
    "p1/cs_merge_on",
    "p1/cs_tw_ID_id",
    "p1/cs_tw_lit_ref",
    "p1/cs_table_T",
    "p1/cs_ns_NS",
    "p3/cs_rel_alias_upper",
    "p3/cs_order_alias",
    "p3/cs_backtick_ID",
    "p3/cs_backtick_Data",
    "p3/cs_temp_view_exact",
    "p3/cs_cte_name",
    "p3/cs_star",
    "p3/cs_func_upper",
    "p4/ins_exact_true",
    "p4/mt_upper_true",
)

_S2_FALSE_KEYS: tuple[str, ...] = (
    "p3/rel_alias_upper",
    "p3/cte_name_upper",
    "p3/temp_view_upper",
    "p4/mt_upper_false",
)

_S2_DML_KEYS: frozenset[str] = frozenset(
    {"p1/cs_insert_cols", "p1/cs_update_where", "p1/cs_delete_where", "p1/cs_merge_on"}
)

_S2_PLAIN_REFUSAL_KEYS: frozenset[str] = frozenset(
    {"p1/cs_table_T", "p1/cs_ns_NS", "p3/cs_cte_name"}
)


def _setup_tables_plain(session: ReparkSession) -> None:
    """Create the probe's struct-less sc tables and the tv temp view."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.t (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')").collect()
    session.sql("CREATE TABLE sc.ns.u (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.u VALUES (1, 'x'), (5, 'y')").collect()
    session.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT id, Data FROM sc.ns.t").collect()


def _assert_plain_refusal(session: ReparkSession, key: str) -> None:
    """Run one oracle step that refuses without a stamped condition (S2)."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    try:
        session.sql(_sql_of(step)).collect()
    except Exception as error:
        assert "not found" in str(error), key
    else:
        raise AssertionError(f"{key} answered instead of refusing")


def _assert_func_upper(session: ReparkSession, key: str) -> None:
    """Run the UPPER leg asserting Spark's rows with RePark's name (S2)."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    frame = session.sql(_sql_of(step))
    assert _rows(frame) == step["spark"]["rows"], key
    assert frame.dtypes[0][0] == "upper(sc.ns.t.Data)", key


def _assert_struct_refusal(session: ReparkSession, key: str) -> None:
    """Run the struct-field leg asserting the refusal (S2)."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    try:
        session.sql(_sql_of(step)).collect()
    except Exception as error:
        assert "not found in struct" in str(error), key
    else:
        raise AssertionError(f"{key} answered instead of refusing")


@pytest.mark.parametrize("key", list(_NESTED_KEYS))
def test_s1_nested_scopes_keep_the_written_spelling(key: str, tmp_path: Path) -> None:
    """Each S1 nested-scope SELECT replays Spark's names and rows."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        _assert_step(session, key)
    finally:
        session.stop()


def test_s1_catalog_view_keeps_its_spelling(tmp_path: Path) -> None:
    """Each C-003 catalog-view step replays Spark's names and rows (S1b)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.register_memory_catalog("vc", tmp_path / "vc")
        for key in (
            "p1/vc_ns",
            "p1/vc_t",
            "p1/vc_t_ins",
            "p1/r5_view_create",
            "p1/r5_view_star",
            "p1/r5_view_lower",
            "p1/r5_view_describe",
            "p1/r5_view2_create",
            "p1/r5_view2_upper",
        ):
            _assert_step(session, key)
    finally:
        session.stop()


def test_s1_merge_derived_source(tmp_path: Path) -> None:
    """MERGE over a derived source spelled in another case replays Spark."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.sql("CREATE TABLE sc.ns.m (id INT, Data STRING) USING iceberg").collect()
        session.sql("INSERT INTO sc.ns.m VALUES (1, 'p')").collect()
        _assert_step(session, "p1/r30_merge_derived")
        _assert_step(session, "p1/r30_after")
        _assert_step(session, "p1/r30_merge_upper_set")
        _assert_step(session, "p1/r30_after2")
    finally:
        session.stop()


@pytest.mark.parametrize("key", list(_S2_TRUE_KEYS))
def test_s2_sql_door_is_exact_under_case_sensitive(key: str, tmp_path: Path) -> None:
    """Each S2 true-mode step replays Spark's answer or recorded refusal."""
    session = _open(tmp_path)
    try:
        if key.startswith("p1/"):
            _setup_tables(session)
        else:
            _setup_tables_plain(session)
        if key in _S2_PLAIN_REFUSAL_KEYS:
            _assert_plain_refusal(session, key)
        elif key == "p1/cs_struct_A":
            _assert_struct_refusal(session, key)
        elif key == "p3/cs_func_upper":
            _assert_func_upper(session, key)
        elif key in _S2_DML_KEYS:
            before = _rows(session.sql("SELECT * FROM sc.ns.u"))
            _assert_step(session, key)
            assert _rows(session.sql("SELECT * FROM sc.ns.u")) == before, key
        else:
            step = _ORACLE[key]
            expect = _s2_expect_msg(key, step["spark"]["msg"]) if "error" in step["spark"] else None
            _assert_step(session, key, expect)
    finally:
        session.stop()


def test_s2_default_session_unchanged(tmp_path: Path) -> None:
    """Each S2 false-mode step replays Spark's answer exactly (S2)."""
    session = _open(tmp_path)
    try:
        _setup_tables_plain(session)
        for key in _S2_FALSE_KEYS:
            _assert_step(session, key)
        frame = session.sql("SELECT ID FROM sc.ns.t")
        assert [name for name, _ in frame.dtypes] == ["ID"]
        assert _rows(frame) == [[1], [2]]
    finally:
        session.stop()


_S3_TRUE_KEYS: tuple[str, ...] = (
    "p1/cs_df_filter_ID",
    "p1/cs_df_describe_ID",
    "p1/cs_df_selectExpr_ID",
    "p1/cs_df_select_Data",
    "p1/cs_df_drop_ID",
    "p1/cs_df_join_on_ID",
    "p4/df_window_ID_true",
)

_S3_FALSE_NAME_KEYS: tuple[str, ...] = (
    "p4/df_col_ID_false",
    "p4/df_select_str_ID_false",
    "p4/df_expr_ID_false",
    "p4/df_window_ID_false",
)


def _run_df(session: ReparkSession, statement: str) -> Any:
    """Run a recorded dataframe-door lambda against the session (S3)."""
    namespace: dict[str, Any] = {"S": session, "F": functions, "ENGINE": "repark"}
    return eval(statement, namespace)()


def _assert_df_step(session: ReparkSession, key: str) -> None:
    """Run one dataframe-door oracle step under its recorded flag (S3)."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    if "error" in spark:
        try:
            _run_df(session, step["statement"]).collect()
        except Exception as error:
            _assert_error(key, error, spark)
        else:
            raise AssertionError(f"{key} answered instead of refusing")
        return
    frame = _run_df(session, step["statement"])
    if spark["cols"]:
        assert _dtypes(frame) == spark["cols"], key
        assert _rows(frame) == spark["rows"], key
    else:
        frame.collect()


def _assert_union_refusal(session: ReparkSession, key: str) -> None:
    """Run the unionByName leg asserting class and exact message (S3).

    Spark reports condition ``_LEGACY_ERROR_TEMP_1201`` for this legacy text;
    RePark carries no condition (ledger R-CS1-9), so the pin asserts the class
    and the byte-exact message and records both conditions.
    """
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    try:
        _run_df(session, step["statement"]).collect()
    except Exception as error:
        assert type(error).__name__ == spark["error"], key
        assert spark.get("getCondition") == "_LEGACY_ERROR_TEMP_1201", key
        assert _condition(error) is None, key
        assert _plain_message(str(error)) == _plain_message(spark["msg"]), key
    else:
        raise AssertionError(f"{key} answered instead of refusing")


@pytest.mark.parametrize("key", list(_S3_TRUE_KEYS))
def test_s3_dataframe_door_is_exact_under_case_sensitive(key: str, tmp_path: Path) -> None:
    """Each S3 true-mode dataframe leg replays Spark's answer or refusal."""
    session = _open(tmp_path)
    try:
        if key.startswith("p1/"):
            _setup_tables(session)
        else:
            _setup_tables_plain(session)
        _assert_df_step(session, key)
    finally:
        session.stop()


def test_s3_dataframe_union_refuses_the_missing_name(tmp_path: Path) -> None:
    """unionByName refuses Spark's legacy text with no condition attached."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        _assert_union_refusal(session, "p1/cs_df_unionByName")
    finally:
        session.stop()


@pytest.mark.parametrize("key", list(_S3_FALSE_NAME_KEYS))
def test_s3_default_door_binds_and_names_as_written(key: str, tmp_path: Path) -> None:
    """Each S3 false-mode name leg answers named with the written spelling."""
    session = _open(tmp_path)
    try:
        _setup_tables_plain(session)
        _assert_df_step(session, key)
    finally:
        session.stop()


def test_s3_describe_resolves_one_name_per_call(tmp_path: Path) -> None:
    """Explicit describe columns resolve and label with the written spelling."""
    struct_session = _open(tmp_path / "struct")
    try:
        _setup_tables(struct_session)
        _assert_df_step(struct_session, "p1/r7_describe_ID")
    finally:
        struct_session.stop()
    plain_session = _open(tmp_path / "plain")
    try:
        _setup_tables_plain(plain_session)
        _assert_df_step(plain_session, "p3/n_df_describe_two")
    finally:
        plain_session.stop()


def test_s3_temp_view_name_is_exact_under_case_sensitive(tmp_path: Path) -> None:
    """Temp-view reads match exactly under true and fold under false (S3)."""
    session = _open(tmp_path)
    try:
        _setup_tables_plain(session)
        _assert_plain_refusal(session, "p3/cs_temp_view_upper")
        _assert_step(session, "p3/cs_temp_view_exact")
        _assert_step(session, "p3/temp_view_upper")
    finally:
        session.stop()


_S4_KEYS: tuple[str, ...] = (
    "p1/tw_ID_id",
    "p1/tw_Id_ID",
    "p1/tw_star_ID",
    "p1/tw_lit",
    "p1/tw_lit_ref",
    "p1/tw_ctas",
    "p1/tw_create",
    "p3/tw_subq_star",
    "p3/tw_view",
    "p3/tw_temp_view",
)


def _s4_expect_msg(key: str, spark_msg: str) -> str:
    """Render RePark's recorded message where it differs from Spark's (S4)."""
    if key == "p3/tw_view":
        return spark_msg.removesuffix(";")
    return spark_msg


def test_s4_case_twins(tmp_path: Path) -> None:
    """Case-twin outputs answer and case-twin creations refuse 42711 (S4)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.register_memory_catalog("vc", tmp_path / "vc")
        session.sql("CREATE NAMESPACE IF NOT EXISTS vc.ns").collect()
        for key in _S4_KEYS:
            step = _ORACLE[key]
            expect = _s4_expect_msg(key, step["spark"]["msg"]) if "error" in step["spark"] else None
            _assert_step(session, key, expect)
        _assert_plain_refusal(session, "p3/tw_temp_view_read")
        _assert_step(session, "p3/tw_insert_into")
        _assert_step(session, "p3/tw_insert_into_run")
        _assert_step(session, "p3/tw_insert_after")
        _assert_df_step(session, "p3/tw_df_write_create")
    finally:
        session.stop()


def _setup_ddl_tables(session: ReparkSession) -> None:
    """Create the probe's p2 tables plus the p3 identifier table (S5)."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    schema = "(id INT NOT NULL, cat STRING, s STRUCT<a: INT>)"
    session.sql(f"CREATE TABLE sc.ns.p1 {schema} USING iceberg PARTITIONED BY (cat)").collect()
    session.sql(f"CREATE TABLE sc.ns.p2 {schema} USING iceberg PARTITIONED BY (cat)").collect()
    session.sql(
        f"CREATE TABLE sc.ns.p3 {schema} USING iceberg PARTITIONED BY (bucket(4, id))"
    ).collect()
    session.sql(f"CREATE TABLE sc.ns.p4 {schema} USING iceberg").collect()
    session.sql(f"CREATE TABLE sc.ns.o1 {schema} USING iceberg").collect()
    session.sql(f"CREATE TABLE sc.ns.o2 {schema} USING iceberg").collect()
    session.sql("CREATE TABLE sc.ns.i (id INT NOT NULL, cat STRING) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.i SET IDENTIFIER FIELDS id").collect()


def _assert_ddl_refusal(session: ReparkSession, key: str) -> None:
    """Run one DDL refusal leg asserting the text after the class prefix (S5)."""
    step = _ORACLE[key]
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    try:
        session.sql(_sql_of(step)).collect()
    except Exception as error:
        assert type(error).__name__ == "PySparkException", key
        assert _condition(error) is None, key
        assert _sql_state(error) is None, key
        want = spark["java"] if "java" in spark else spark["msg"]
        want = _plain_message(want).removeprefix(":").strip()
        assert _plain_message(str(error)).removeprefix("DataInvalid => ") == want, key
    else:
        raise AssertionError(f"{key} answered instead of refusing")


def _assert_meta(warehouse: Path, table: str, meta_key: str) -> None:
    """Replay one _meta step from the table's latest metadata file (S5)."""
    base = warehouse / "sc" / "ns" / table / "metadata"
    files = sorted(base.glob("*.metadata.json"), key=lambda item: item.stat().st_mtime_ns)
    meta = json.loads(files[-1].read_text(encoding="utf-8"))
    spec = next(
        entry for entry in meta["partition-specs"] if entry["spec-id"] == meta["default-spec-id"]
    )
    orders = [
        entry
        for entry in meta.get("sort-orders", [])
        if entry["order-id"] == meta.get("default-sort-order-id")
    ]
    schema = next(
        entry for entry in meta["schemas"] if entry["schema-id"] == meta["current-schema-id"]
    )
    got = {
        "spec": [
            [field["name"], field["transform"], field["source-id"]] for field in spec["fields"]
        ],
        "sort": [
            [field["transform"], field["source-id"], field["direction"]]
            for field in (orders[0]["fields"] if orders else [])
        ],
        "identifier": schema.get("identifier-field-ids", []),
    }
    assert got == _ORACLE[meta_key]["spark"], meta_key


def test_s5_iceberg_ddl_binds_exactly(tmp_path: Path) -> None:
    """Partition, sort and identifier names bind as Iceberg binds them (S5)."""
    session = _open(tmp_path)
    try:
        _setup_ddl_tables(session)
        _assert_ddl_refusal(session, "p2/pt_drop_CAT_present")
        _assert_meta(tmp_path, "p1", "p2/pt_drop_CAT_present_meta")
        _assert_step(session, "p2/pt_drop_cat_present")
        _assert_meta(tmp_path, "p1", "p2/pt_drop_cat_present_meta")
        _assert_ddl_refusal(session, "p2/pt_replace_CAT_by_name")
        _assert_meta(tmp_path, "p2", "p2/pt_replace_CAT_by_name_meta")
        _assert_ddl_refusal(session, "p2/pt_drop_bucket_ID")
        _assert_meta(tmp_path, "p3", "p2/pt_drop_bucket_ID_meta")
        _assert_ddl_refusal(session, "p2/pt_drop_field_upper")
        _assert_meta(tmp_path, "p3", "p2/pt_drop_field_upper_meta")
        _assert_ddl_refusal(session, "p2/pt_replace_bucket_ID")
        _assert_meta(tmp_path, "p3", "p2/pt_replace_bucket_ID_meta")
        _assert_step(session, "p2/pt_add_named_upper")
        _assert_ddl_refusal(session, "p2/pt_drop_named_lower")
        _assert_meta(tmp_path, "p4", "p2/pt_drop_named_lower_meta")
        _assert_step(session, "p2/so_false_CAT")
        _assert_meta(tmp_path, "o1", "p2/so_false_CAT_meta")
        _assert_step(session, "p2/so_false_nested_A")
        _assert_meta(tmp_path, "o1", "p2/so_false_nested_A_meta")
        _assert_step(session, "p2/so_false_local_ID")
        _assert_step(session, "p2/so_false_dist_ID")
        _assert_step(session, "p2/id_false_ID")
        _assert_meta(tmp_path, "o1", "p2/id_false_ID_meta")
        _assert_ddl_refusal(session, "p2/so_true_CAT")
        _assert_step(session, "p2/so_true_cat")
        _assert_ddl_refusal(session, "p2/so_true_local_ID")
        _assert_ddl_refusal(session, "p2/so_true_nested_A")
        _assert_ddl_refusal(session, "p2/so_true_bucket_ID")
        _assert_step(session, "p2/id_true_ID")
        _assert_step(session, "p2/id_true_id")
        _assert_meta(tmp_path, "o2", "p2/id_true_id_meta")
        _assert_step(session, "p2/pt_true_drop_bucket_ID")
        _assert_ddl_refusal(session, "p2/pt_true_drop_bucket_ID2")
        _assert_meta(tmp_path, "o2", "p2/pt_true_drop_bucket_ID2_meta")
        _assert_step(session, "p3/id_drop_ID")
        _assert_ddl_refusal(session, "p3/so_false_missing")
    finally:
        session.stop()


def test_cast_of_a_column_keeps_the_written_child_name(tmp_path: Path) -> None:
    """Cast and try_cast of F.col keep the child name, never the engine path (VC-3)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        frame = session.createDataFrame([(1, "a", 2.5)], ["id", "Data", "Amount"])
        assert frame.select(functions.col("Amount").cast("int")).columns == ["Amount"]
        assert frame.select(functions.col("Data").cast("string")).columns == ["Data"]
        assert frame.select(functions.col("id").cast("string")).columns == ["id"]
        assert frame.select(functions.col("ID").cast("string")).columns == ["ID"]
        assert frame.select(functions.col("Amount").try_cast("int")).columns == ["Amount"]
        assert frame.select(functions.col("Data").try_cast("string")).columns == ["Data"]
        cast = frame.select(functions.col("Amount").cast("int"))
        assert _rows(cast) == [[2]]
        table = session.table("sc.ns.t")
        assert table.select(functions.col("Data").cast("string")).columns == ["Data"]
        assert table.select(functions.col("ID").cast("string")).columns == ["ID"]
        assert table.select(functions.col("ID").try_cast("string")).columns == ["ID"]
        assert table.select(functions.col("id").cast("string")).columns == ["id"]
        assert table.select(functions.col("Data").try_cast("string")).columns == ["Data"]
        rows = table.select(functions.col("ID").cast("string"))
        assert _rows(rows) == [["1"], ["2"]]
    finally:
        session.stop()


def test_describe_resolves_display_names_under_case_insensitive(tmp_path: Path) -> None:
    """Describe with explicit columns binds transpose display names (VC-4)."""
    session = _open(tmp_path)
    try:
        frame = session.createDataFrame([("k1", 1), ("k2", 2)], ["key", "v"])
        described = frame.transpose().describe("k1")
        assert described.columns == ["summary", "k1"]
        rows = {row[0]: row[1] for row in described.collect()}
        assert rows["count"] == "1"
        assert rows["max"] == "1"
        assert rows["mean"] == "1.0"
        assert rows["min"] == "1"
    finally:
        session.stop()


def test_describe_refuses_duplicate_display_names_as_ambiguous(tmp_path: Path) -> None:
    """Describe over a duplicate-display frame refuses ambiguous like Spark (VC-4)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        frame = session.table("sc.ns.t").select("id", "id")
        with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
            frame.describe("id")
    finally:
        session.stop()


def test_nested_cast_of_a_column_keeps_the_written_child_name(tmp_path: Path) -> None:
    """Nested casts of F.col keep the child name, never the engine path (RC-3)."""
    session = _open(tmp_path)
    try:
        frame = session.createDataFrame([(1, "a", 2.5)], ["id", "Data", "Amount"])
        assert frame.select(functions.col("Amount").cast("int").cast("string")).columns == [
            "Amount"
        ]
        assert frame.select(functions.col("Amount").try_cast("int").cast("string")).columns == [
            "Amount"
        ]
        assert frame.select(
            functions.col("Data").cast("string").cast("string").cast("string")
        ).columns == ["Data"]
    finally:
        session.stop()


def test_describe_resolves_display_names_under_case_sensitive(tmp_path: Path) -> None:
    """Describe with explicit columns binds transpose display names under true (RC-4)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        frame = session.createDataFrame([("k1", 1), ("k2", 2)], ["key", "v"])
        described = frame.transpose().describe("k1")
        assert described.columns == ["summary", "k1"]
        rows = {row[0]: row[1] for row in described.collect()}
        assert rows["count"] == "1"
        assert rows["max"] == "1"
    finally:
        session.stop()

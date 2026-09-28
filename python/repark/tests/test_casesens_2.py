"""WO CASESENS-2 slice 1: bare DataFrame names resolve in Rust.

Under ``caseSensitive=true`` the six R-CS1-10 legs (select string / ``F.col`` /
lowercase-data, ``orderBy``, ``groupBy``, string ``filter``) plus ``df["ID"]``
refuse ``UNRESOLVED_COLUMN.WITH_SUGGESTION`` naming the written spelling, and
the p6 miss cells refuse the same way. Under ``false`` qualified strings bind:
``q.select("t.id")`` / ``"t.ID"`` and the aliased-table shape answer named with
the written last segment, and the exact qualified shape answers under ``true``
while the self-join shape refuses naming ``l.ID``. The ``false`` door is
otherwise byte-identical: the r7 guard legs, the S3 ``df_*_false`` legs,
today's facade miss text, the R-19 lazy timing and the quoter battery.

The p6 oracle is ``casesens_2_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0,
measured 2026-09-28); p1/p4 legs read ``casesens_1_spark_oracle.json``.
Refusals compare per R12/R13 (head plus candidate set, position suffix
stripped). ``p1/r7_selfjoin`` and ``p6/qs_sel_t_id_true`` stay unpinned,
re-homed per the 2026-09-28 ruling (R-CS2-1/R-CS2-2): the join condition
refuses first, and a false-built frame reused under ``true`` reads its
captured rule.

Slice 2 (same file): ``withColumn(s)`` and ``withColumnRenamed`` follow the
rule. Under ``true`` a folded key appends and a folded rename no-ops; under
``false`` renames fan out to twins and folded ``withColumns`` keys refuse
``COLUMN_ALREADY_EXISTS``. The overlay pin guards display-spelling replace.

Slice 3 (same file): ``fillna`` / ``dropna`` subsets and ``dropDuplicates``
resolve in Rust. Under ``true`` a folded or missing subset name refuses
(``UNRESOLVED_COLUMN`` for ``na``, Spark's legacy subset text for
``dropDuplicates``); under ``false`` subsets match ignoring case (over the
null table for ``na``) and the ``dropDuplicates`` miss raises the legacy
text. Legacy legs assert type plus message only (R8).

pins: casesens-2/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repark import ReparkSession
from repark.spark import functions

ORACLE2_PATH: Path = Path(__file__).with_name("casesens_2_spark_oracle.json")
_ORACLE2: dict[str, Any] = json.loads(ORACLE2_PATH.read_text(encoding="utf-8"))["steps"]
ORACLE1_PATH: Path = Path(__file__).with_name("casesens_1_spark_oracle.json")
_ORACLE1: dict[str, Any] = json.loads(ORACLE1_PATH.read_text(encoding="utf-8"))["steps"]

_TRUE_BARE_KEYS: tuple[str, ...] = (
    "p1/cs_df_select_ID",
    "p1/cs_df_select_col_ID",
    "p1/cs_df_select_data",
    "p1/cs_df_orderBy_ID",
    "p1/cs_df_groupBy_ID",
    "p1/cs_df_filter_str_ID",
    "p1/cs_df_getitem_ID",
)

_TRUE_MISS_KEYS: tuple[str, ...] = (
    "p6/sel_nope_true",
    "p6/getitem_nope_true",
    "p6/order_nope_true",
    "p6/group_nope_true",
    "p6/filter_str_nope_true",
)

_FALSE_GUARD_KEYS: tuple[str, ...] = (
    "p1/r7_orderBy_ID",
    "p1/r7_groupBy_DATA",
    "p1/r7_sort_col_ID",
)

_S3_FALSE_KEYS: tuple[str, ...] = (
    "p4/df_col_ID_false",
    "p4/df_select_str_ID_false",
    "p4/df_expr_ID_false",
    "p4/df_window_ID_false",
)

_S2_WITHCOLUMN_KEYS: tuple[str, ...] = (
    "p1/cs_df_withColumn_ID",
    "p1/r7_withColumn_ID",
    "p6/wcs_true",
    "p6/wcs_true_exact",
    "p6/wc_twin_true",
    "p6/r20_wc_twin",
)

_S2_RENAMED_KEYS: tuple[str, ...] = (
    "p1/cs_df_renamed_ID",
    "p1/r7_withColumnRenamed_ID",
    "p6/wcrn_plural_true",
    "p6/wcr_twin_true",
    "p6/r20_wcr_twin",
)

_SPARK_POSITION_SUFFIX = "; line "
_REPARK_PREFIX: str = "Error during planning: "
_SUGGESTION_MARK: str = "Did you mean one of the following? ["


def _open(warehouse: Path) -> ReparkSession:
    """Open a session with hadoop catalog sc."""
    return (
        ReparkSession.builder.appName("casesens-2-s1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _setup_tables(session: ReparkSession) -> None:
    """Create the probe's struct table sc.ns.t."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql(
        "CREATE TABLE sc.ns.t (id INT, Data STRING, s STRUCT<a: INT>) USING iceberg"
    ).collect()
    session.sql(
        "INSERT INTO sc.ns.t VALUES (1, 'a', named_struct('a', 5)), (2, 'b', named_struct('a', 6))"
    ).collect()


def _setup_tables_plain(session: ReparkSession) -> None:
    """Create the probe's struct-less table sc.ns.t."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.t (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')").collect()


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


def _assert_error(key: str, error: BaseException, spark: dict[str, Any]) -> None:
    """Replay one oracle refusal against Spark's recorded answer per R12."""
    assert type(error).__name__ == spark["error"], key
    assert _condition(error) == spark.get("getCondition"), key
    assert _sql_state(error) == spark.get("getSqlState"), key
    mine = _plain_message(str(error))
    want = _plain_message(spark["msg"])
    assert mine.split(_SUGGESTION_MARK)[0] == want.split(_SUGGESTION_MARK)[0], key
    if _SUGGESTION_MARK in want:
        assert _candidates(mine) == _candidates(want), key


def _oracle(key: str) -> dict[str, Any]:
    """Read one oracle step from the probe set its prefix names."""
    if key.startswith("p6/"):
        return _ORACLE2[key]
    return _ORACLE1[key]


def _twins(session: ReparkSession) -> Any:
    """Build the probe's twin frame ``(id, ID)`` holding ``(1, 2)`` (S2)."""
    return session.createDataFrame([(1, 2)], ["id", "ID"])


def _run_df(session: ReparkSession, statement: str, frame: Any = None) -> Any:
    """Run a recorded dataframe-door lambda against the session (S1)."""
    namespace: dict[str, Any] = {
        "S": session,
        "F": functions,
        "ENGINE": "repark",
        "Q": frame,
        "twins": lambda: _twins(session),
    }
    return eval(statement, namespace)()


def _assert_df_step(session: ReparkSession, key: str, frame: Any = None) -> None:
    """Run one dataframe-door oracle step under its recorded flag (S1)."""
    step = _oracle(key)
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    if "error" in spark:
        try:
            _run_df(session, step["statement"], frame).collect()
        except Exception as error:
            _assert_error(key, error, spark)
        else:
            raise AssertionError(f"{key} answered instead of refusing")
        return
    ran = _run_df(session, step["statement"], frame)
    if spark["cols"]:
        assert _dtypes(ran) == spark["cols"], key
        assert _rows(ran) == spark["rows"], key
    else:
        ran.collect()


def test_s1_true_door_refuses_bare_names(tmp_path: Path) -> None:
    """Each R-CS1-10 leg plus getitem refuses naming the written spelling."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _TRUE_BARE_KEYS:
            _assert_df_step(session, key)
    finally:
        session.stop()


def test_s1_true_misses_refuse(tmp_path: Path) -> None:
    """Each p6 true-mode miss refuses Spark's recorded text."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _TRUE_MISS_KEYS:
            _assert_df_step(session, key)
    finally:
        session.stop()


def test_s1_qualified_strings_bind(tmp_path: Path) -> None:
    """Qualified strings bind under false and follow the rule under true."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "false")
        _assert_df_step(session, "p6/qs_sel_t_id")
        _assert_df_step(session, "p6/qs_sel_t_ID")
        _assert_df_step(session, "p6/qs_alias_sel")
        shared = session.sql("SELECT ID, data FROM sc.ns.t t")
        _assert_df_step(session, "p6/qs_sel_t_ID_exact_true", shared)
        _assert_df_step(session, "p6/selfjoin_true")
    finally:
        session.stop()


def test_s1_false_door_byte_identical(tmp_path: Path) -> None:
    """Guards, miss text, lazy timing and the quoter match the S1 base."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _FALSE_GUARD_KEYS:
            _assert_df_step(session, key)
        session.conf.set("spark.sql.caseSensitive", "false")
        table = session.table("sc.ns.t")
        try:
            table.select("nope")
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert str(error) == (
                "A column with name `nope` cannot be resolved; "
                "available columns: ['id', 'Data', 's']"
            )
        else:
            raise AssertionError("select(nope) answered instead of refusing")
        twins = session.createDataFrame([(1, 2)], ["id", "ID"])
        assert twins["id"].spark_display_part() == "id"
        try:
            twins.select("id").collect()
        except Exception as error:
            assert "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous" in str(error)
            assert _sql_state(error) == "42704"
        else:
            raise AssertionError("twin select answered instead of refusing")
        try:
            twins.filter("id > 0").collect()
        except Exception as error:
            assert str(error) == (
                "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`id`, `ID`]."
            )
        else:
            raise AssertionError("twin filter answered instead of refusing")
    finally:
        session.stop()
    plain_session = _open(tmp_path / "plain")
    try:
        _setup_tables_plain(plain_session)
        for key in _S3_FALSE_KEYS:
            _assert_df_step(plain_session, key)
    finally:
        plain_session.stop()


def test_s2_withcolumn_follows_the_rule(tmp_path: Path) -> None:
    """Folded keys append under true and replace (every twin) under false."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _S2_WITHCOLUMN_KEYS:
            _assert_df_step(session, key)
    finally:
        session.stop()


def test_s2_renamed_follows_the_rule(tmp_path: Path) -> None:
    """Folded renames no-op under true and fan out to twins under false."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _S2_RENAMED_KEYS:
            _assert_df_step(session, key)
    finally:
        session.stop()


def test_s2_folded_keys_refuse(tmp_path: Path) -> None:
    """Folded withColumns keys refuse COLUMN_ALREADY_EXISTS under false."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        _assert_df_step(session, "p6/r26_wcs_dup")
    finally:
        session.stop()


def test_s2_overlay_replace_unchanged(tmp_path: Path) -> None:
    """withColumn on an overlay frame replaces by display spelling (R7)."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "false")
        table = session.table("sc.ns.t")
        overlay = table.select(functions.col("id").alias("ID"), functions.col("Data").alias("ID"))
        assert overlay.columns == ["ID", "ID"]
        replaced = overlay.withColumn("id", functions.lit(9))
        assert _dtypes(replaced) == [["id", "int"], ["id", "int"]]
        assert _rows(replaced) == [[9, 9], [9, 9]]
    finally:
        session.stop()


def _setup_tables_null(session: ReparkSession) -> None:
    """Create the probe's null table sc.ns.tn (S3)."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.tn (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.tn VALUES (1, NULL), (NULL, 'b')").collect()


def _setup_tables_p4(session: ReparkSession) -> None:
    """Create the probe-4 struct-less table sc.ns.t (S3)."""
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.t (id INT, Data STRING) USING iceberg").collect()
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a'), (2, 'b')").collect()


def _assert_legacy_step(session: ReparkSession, key: str) -> None:
    """Run one oracle step whose refusal pins type plus message only (R8, S3)."""
    step = _oracle(key)
    session.conf.set("spark.sql.caseSensitive", "true" if step["case_sensitive"] else "false")
    spark = step["spark"]
    try:
        _run_df(session, step["statement"]).collect()
    except Exception as error:
        assert type(error).__name__ == spark["error"], key
        assert _plain_message(str(error)) == _plain_message(spark["msg"]), key
    else:
        raise AssertionError(f"{key} answered instead of refusing")


def test_s3_fillna_follows_the_rule(tmp_path: Path) -> None:
    """fillna subsets fold under false and refuse under true."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        _setup_tables_null(session)
        _assert_df_step(session, "p6/fill_null_false")
        _assert_df_step(session, "p6/fill_null_true")
        _assert_df_step(session, "p6/fill_subset_nope_true")
    finally:
        session.stop()
    plain_session = _open(tmp_path / "plain")
    try:
        _setup_tables_p4(plain_session)
        _assert_df_step(plain_session, "p4/df_fillna_true")
    finally:
        plain_session.stop()


def test_s3_dropna_follows_the_rule(tmp_path: Path) -> None:
    """dropna subsets fold under false, on the plain and overlay paths."""
    session = _open(tmp_path)
    try:
        _setup_tables_null(session)
        _assert_df_step(session, "p6/dropna_subset_false")
        _assert_df_step(session, "p6/dropna_subset_true")
        session.conf.set("spark.sql.caseSensitive", "false")
        overlay = session.table("sc.ns.tn").select(
            functions.col("id").alias("ID"), functions.col("Data").alias("ID")
        )
        assert overlay.columns == ["ID", "ID"]
        dropped = overlay.dropna(subset=["id"])
        assert _dtypes(dropped) == [["ID", "int"], ["ID", "string"]]
        assert _rows(dropped) == []
        dup = session.table("sc.ns.tn").select(
            functions.col("Data").alias("X"), functions.col("Data").alias("X")
        )
        filled = dup.fillna("z", subset=["x"])
        assert _dtypes(filled) == [["X", "string"], ["X", "string"]]
        assert _rows(filled) == [["b", "b"], ["z", "z"]]
    finally:
        session.stop()


def test_s3_drop_duplicates_follows_the_rule(tmp_path: Path) -> None:
    """dropDuplicates subsets fold under false; misses raise the legacy text."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        _assert_df_step(session, "p1/r7_dropDuplicates_ID")
        _assert_df_step(session, "p6/dd_exact_true")
        _assert_legacy_step(session, "p6/dd_nope_true")
        _assert_legacy_step(session, "p6/dd_nope_false")
    finally:
        session.stop()
    plain_session = _open(tmp_path / "plain")
    try:
        _setup_tables_p4(plain_session)
        _assert_legacy_step(plain_session, "p4/df_dropDuplicates_true")
    finally:
        plain_session.stop()

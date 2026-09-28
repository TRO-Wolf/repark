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

Slice 4 (same file): DataFrame-alias qualified names bind in join
conditions and on the join child through R4 (qualifier via the side
schema, name via the session rule). ``p1/r7_selfjoin`` and
``p6/r18_alias_join`` answer Spark's names and rows,
``p6/selfjoin_true`` keeps refusing Spark's text, and a wrong-case alias
qualifier under ``true`` refuses the class and SQLSTATE (its text is
unrecorded). The p10 cells pin the non-join overlay shapes: the two
``true`` qualified misses match Spark head plus candidates, the
``false`` shapes that Spark refuses keep the R4 facade text, and the
three shapes where Spark answers stay refuse-pinned as residue R-CS2-7
(RePark's select/withColumn children lose the alias qualifier).

Slice 5 (same file, verifier fold 2026-09-28): non-ASCII names fold like
Java ``equalsIgnoreCase`` under ``false`` on the DataFrame door (select,
getitem, orderBy, groupBy, dropna, dropDuplicates, fillna, withColumn,
withColumnRenamed, withColumns) while ``STRASSE`` still misses ``straße``;
under ``true`` the folded legs refuse. A qualified hit on two join sides
refuses ``AMBIGUOUS_REFERENCE`` with Spark's text on every shape (select,
groupBy, getitem, ``F.col``), and the plural rename fans out like the
singular form. The asymmetric self-join binds each side. The SQL-door
unicode gap, the dict-fillna gap and the exact-duplicate rename gap stay
refuse-pinned as residues R-CS2-8 and R-CS2-9 with Spark's answers
recorded. The p11 oracle is live PySpark 4.1.2, measured 2026-09-28
(``cs_probe11.py`` / ``cs_probe11b.py``).

pins: casesens-2/C-001, C-002, C-003, C-004, C-005, C-006, C-008
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
    if key.startswith(("p6/", "p10/")):
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


_S4_SELFJOIN_KEYS: tuple[str, ...] = (
    "p1/r7_selfjoin",
    "p6/r18_alias_join",
    "p6/selfjoin_true",
)

_P10_TRUE_MATCH_KEYS: tuple[str, ...] = (
    "p10/alias_dupe_sel_fold_true",
    "p10/alias_dupe_sel_first_true",
)

_P10_FACADE_KEYS: tuple[tuple[str, str], ...] = (
    (
        "p10/alias_dupe_sel",
        "A column with name `l.id` cannot be resolved; available columns: ['id', 'id']",
    ),
    (
        "p10/alias_dupe_sel_fold",
        "A column with name `l.ID` cannot be resolved; available columns: ['id', 'id']",
    ),
    (
        "p10/alias_dupe_sel_wrongqual",
        "A column with name `x.id` cannot be resolved; available columns: ['id', 'id']",
    ),
    (
        "p10/alias_wc_sel",
        "A column with name `l.ID` cannot be resolved; available columns: ['ID', 'Data', 's']",
    ),
    (
        "p10/alias_wc_sel_lower",
        "A column with name `l.id` cannot be resolved; available columns: ['ID', 'Data', 's']",
    ),
    (
        "p10/alias_dupe_sel_first",
        "A column with name `l.x` cannot be resolved; available columns: ['x', 'x']",
    ),
)


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


def test_s4_selfjoin_answers_as_spark(tmp_path: Path) -> None:
    """Alias-qualified join halves answer Spark's names, rows and refusal."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _S4_SELFJOIN_KEYS:
            _assert_df_step(session, key)
    finally:
        session.stop()


def test_s4_wrongcase_alias_qualifier_refuses_under_true(tmp_path: Path) -> None:
    """A wrong-case alias qualifier refuses the class and SQLSTATE under true."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        session.conf.set("spark.sql.caseSensitive", "true")
        table = session.table("sc.ns.t")
        try:
            table.alias("l").join(
                table.alias("r"),
                functions.col("L.id") == functions.col("r.id"),
            ).select("l.id").collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert _sql_state(error) == "42703"
        else:
            raise AssertionError("wrong-case alias qualifier answered instead of refusing")
    finally:
        session.stop()


def test_s4_probe10_true_qualified_misses_match(tmp_path: Path) -> None:
    """True overlay qualified misses name the qualifier as Spark does."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key in _P10_TRUE_MATCH_KEYS:
            _assert_df_step(session, key)
    finally:
        session.stop()


def test_s4_probe10_false_shapes_refuse(tmp_path: Path) -> None:
    """False overlay qualified shapes keep the facade text, gaps recorded."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        for key, facade_text in _P10_FACADE_KEYS:
            step = _oracle(key)
            session.conf.set("spark.sql.caseSensitive", "false")
            try:
                _run_df(session, step["statement"]).collect()
            except Exception as error:
                assert type(error).__name__ == "AnalysisException", key
                assert str(error) == facade_text, key
            else:
                raise AssertionError(f"{key} answered instead of refusing")
            spark = step["spark"]
            if "error" in spark:
                assert spark["error"] == "AnalysisException", key
                assert spark["getCondition"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION", key
                assert spark["getSqlState"] == "42703", key
            else:
                assert spark["cols"], key
    finally:
        session.stop()


def _uni(session: ReparkSession) -> Any:
    """Build the probe's unicode frame ``[id, Ünï, Éte]`` (S5)."""
    return session.createDataFrame(
        [(1, "a", "x"), (2, None, "y"), (3, "a", "z")], ["id", "Ünï", "Éte"]
    )


def _aliased_join(session: ReparkSession) -> Any:
    """Build ``L.join(R, L.id == R.id - 1).alias("j")`` (S5)."""
    left = session.createDataFrame([(1, "L1"), (2, "L2"), (3, "L3")], ["id", "s"])
    right = session.createDataFrame([(2, "R2"), (3, "R3"), (4, "R4")], ["id", "s"])
    return left.join(right, left["id"] == right["id"] - 1).alias("j")


def test_s5_unicode_names_fold(tmp_path: Path) -> None:
    """Non-ASCII names fold like Java equalsIgnoreCase under false (V2-1)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        selected = _uni(session).select("ünï")
        assert _dtypes(selected) == [["ünï", "string"]]
        assert _rows(selected) == [["a"], ["a"], [None]]
        pointed = _uni(session)
        gotten = pointed.select(pointed["ÜNÏ"])
        assert _dtypes(gotten) == [["ÜNÏ", "string"]]
        assert _rows(gotten) == [["a"], ["a"], [None]]
        assert type(_uni(session)["ÜNÏ"]).__name__ == "Column"
        ordered = _uni(session).orderBy("éte")
        assert ordered.columns == ["id", "Ünï", "Éte"]
        assert [row[2] for row in ordered.collect()] == ["x", "y", "z"]
        grouped = _uni(session).groupBy("ÉTE").count()
        assert _dtypes(grouped) == [["ÉTE", "string"], ["count", "bigint"]]
        assert _rows(grouped) == [["x", 1], ["y", 1], ["z", 1]]
        dropped = _uni(session).dropna(subset=["ünï"])
        assert _rows(dropped) == [[1, "a", "x"], [3, "a", "z"]]
        deduped = _uni(session).dropDuplicates(["ünï"])
        assert _rows(deduped) == [[1, "a", "x"], [2, None, "y"]]
        replaced = _uni(session).withColumn("ÜNÏ", functions.lit(0))
        assert _dtypes(replaced) == [["id", "bigint"], ["ÜNÏ", "int"], ["Éte", "string"]]
        assert _rows(replaced) == [[1, 0, "x"], [2, 0, "y"], [3, 0, "z"]]
        renamed = _uni(session).withColumnRenamed("ünï", "z")
        assert renamed.columns == ["id", "z", "Éte"]
        filled = _uni(session).fillna("z", subset=["ünï"])
        assert _rows(filled) == [[1, "a", "x"], [2, "z", "y"], [3, "a", "z"]]
        try:
            _uni(session).withColumns({"Ünï": functions.lit(1), "ünï": functions.lit(2)}).collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "COLUMN_ALREADY_EXISTS"
            assert _sql_state(error) == "42711"
            assert _plain_message(str(error)) == (
                "[COLUMN_ALREADY_EXISTS] The column `ünï` already exists. Choose another "
                "name or rename the existing column. SQLSTATE: 42711"
            )
        else:
            raise AssertionError("folded unicode keys answered instead of refusing")
        strasse = session.createDataFrame([(1, "v")], ["id", "straße"])
        try:
            strasse.select("STRASSE")
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert str(error) == (
                "A column with name `STRASSE` cannot be resolved; "
                "available columns: ['id', 'straße']"
            )
        else:
            raise AssertionError("STRASSE answered instead of refusing")
        assert strasse.select("straße").columns == ["straße"]
        _uni(session).createOrReplaceTempView("uni")
        for key, run in (
            ("sql", lambda: session.sql("SELECT ünï FROM uni").collect()),
            ("filter", lambda: _uni(session).filter("ünï is null").collect()),
            ("selectExpr", lambda: _uni(session).selectExpr("ünï").collect()),
        ):
            try:
                run()
            except Exception as error:
                assert type(error).__name__ == "ParseException", key
            else:
                raise AssertionError(f"{key} answered instead of refusing")
    finally:
        session.stop()


def _assert_true_refusal(key: str, run: Any, written: str, candidates: set[str]) -> None:
    """Assert one true-door refusal names the written spelling (S5)."""
    try:
        run()
    except Exception as error:
        assert type(error).__name__ == "AnalysisException", key
        assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION", key
        assert _sql_state(error) == "42703", key
        mine = _plain_message(str(error))
        assert mine.split(_SUGGESTION_MARK)[0] == (
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
            f"parameter with name `{written}` cannot be resolved. "
        ), key
        assert _candidates(mine) == candidates, key
    else:
        raise AssertionError(f"{key} answered instead of refusing")


def test_s5_unicode_names_refuse_under_true(tmp_path: Path) -> None:
    """Folded non-ASCII names refuse under true (V2-1)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        frame = _uni(session)
        uni_candidates = {"`Ünï`", "`id`", "`Éte`"}
        _assert_true_refusal("select", lambda: frame.select("ünï").collect(), "ünï", uni_candidates)
        _assert_true_refusal(
            "getitem",
            lambda: frame.select(frame["ÜNÏ"]).collect(),
            "ÜNÏ",
            uni_candidates,
        )
        _assert_true_refusal(
            "orderBy", lambda: frame.orderBy("éte").collect(), "éte", uni_candidates
        )
        _assert_true_refusal(
            "dropna",
            lambda: frame.dropna(subset=["ünï"]).collect(),
            "ünï",
            uni_candidates,
        )
        strasse = session.createDataFrame([(1, "v")], ["id", "straße"])
        _assert_true_refusal(
            "strasse",
            lambda: strasse.select("STRASSE").collect(),
            "STRASSE",
            {"`id`", "`straße`"},
        )
        try:
            _uni(session)["ÜNÏ"]
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert _sql_state(error) == "42703"
        else:
            raise AssertionError("true getitem answered instead of refusing")
        try:
            _uni(session).dropDuplicates(["ünï"]).collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _plain_message(str(error)) == (
                'Cannot resolve column name "ünï" among (id, Ünï, Éte).'
            )
        else:
            raise AssertionError("true dropDuplicates answered instead of refusing")
        appended = _uni(session).withColumn("ÜNÏ", functions.lit(0))
        assert appended.columns == ["id", "Ünï", "Éte", "ÜNÏ"]
        kept = _uni(session).withColumnRenamed("ünï", "z")
        assert kept.columns == ["id", "Ünï", "Éte"]
        _uni(session).createOrReplaceTempView("uni")
        try:
            session.sql("SELECT ünï FROM uni").collect()
        except Exception as error:
            assert type(error).__name__ == "ParseException"
        else:
            raise AssertionError("true bare-unicode SQL answered instead of refusing")
    finally:
        session.stop()


def test_s5_aliased_join_refuses_ambiguous(tmp_path: Path) -> None:
    """A qualified hit on two join sides refuses AMBIGUOUS_REFERENCE (V2-2)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        heads = (
            ("select", lambda frame: frame.select("j.s"), "`j`.`s`", ("`j`.`s`", "`j`.`s`")),
            (
                "group",
                lambda frame: frame.groupBy("j.s").count(),
                "`j`.`s`",
                ("`j`.`s`", "`j`.`s`"),
            ),
            (
                "getitem",
                lambda frame: frame.select(frame["j.s"]),
                "`j`.`s`",
                ("`j`.`s`", "`j`.`s`"),
            ),
            (
                "fcol",
                lambda frame: frame.select(functions.col("j.s")),
                "`j`.`s`",
                ("`j`.`s`", "`j`.`s`"),
            ),
            ("id", lambda frame: frame.select("j.id"), "`j`.`id`", ("`j`.`id`", "`j`.`id`")),
            ("fold", lambda frame: frame.select("J.S"), "`J`.`S`", ("`j`.`S`", "`j`.`S`")),
        )
        for key, build, reference, options in heads:
            try:
                build(_aliased_join(session)).collect()
            except Exception as error:
                assert type(error).__name__ == "AnalysisException", key
                assert _condition(error) == "AMBIGUOUS_REFERENCE", key
                assert _sql_state(error) == "42704", key
                assert _plain_message(str(error)) == (
                    f"[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: "
                    f"[{', '.join(options)}]. SQLSTATE: 42704"
                ), key
            else:
                raise AssertionError(f"{key} answered instead of refusing")
        try:
            _aliased_join(session)["j.s"]
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "AMBIGUOUS_REFERENCE"
            assert _sql_state(error) == "42704"
        else:
            raise AssertionError("bare getitem answered instead of refusing")
    finally:
        session.stop()


def test_s5_true_attribute_join_refuses_at_construction(tmp_path: Path) -> None:
    """An attribute-condition join refuses while building under true (V2-2 note).

    Spark 4.1.2 builds the join and refuses the later ``j.s`` select; RePark
    refuses at construction. Pinned loose (class plus SQLSTATE) to guard the
    join-condition matcher the fold touches.
    """
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "true")
        left = session.createDataFrame([(1, "L1")], ["id", "s"])
        right = session.createDataFrame([(2, "R2")], ["id", "s"])
        try:
            left.join(right, left["id"] == right["id"] - 1)
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "AMBIGUOUS_REFERENCE"
            assert _sql_state(error) == "42704"
        else:
            raise AssertionError("true attribute join answered instead of refusing")
    finally:
        session.stop()


def test_s5_asymmetric_selfjoin_binds_sides(tmp_path: Path) -> None:
    """An asymmetric alias self-join answers each side's rows (NSP-4)."""
    session = _open(tmp_path)
    try:
        _setup_tables_p4(session)
        session.conf.set("spark.sql.caseSensitive", "false")
        table = session.table("sc.ns.t")
        joined = (
            table.alias("l")
            .join(table.alias("r"), functions.col("l.id") == functions.col("r.id") + 1)
            .select("l.id", "r.id")
        )
        assert _dtypes(joined) == [["id", "int"], ["id", "int"]]
        assert _rows(joined) == [[2, 1]]
    finally:
        session.stop()


def test_s5_plural_renamed_fans_out(tmp_path: Path) -> None:
    """withColumnsRenamed answers duplicate names like the singular form (V2-3)."""
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        twins = session.createDataFrame([(1, 2)], ["id", "ID"])
        renamed = twins.withColumnsRenamed({"id": "a"})
        assert renamed.columns == ["a", "a"]
        assert _rows(renamed) == [[1, 2]]
        named = session.createDataFrame([(1, 2, 3)], ["ID", "id", "v"]).withColumnsRenamed(
            {"ID": "Name"}
        )
        assert named.columns == ["Name", "Name", "v"]
        assert _rows(named) == [[1, 2, 3]]
    finally:
        session.stop()


def test_s5_unicode_sql_door_gap_stays_pinned(tmp_path: Path) -> None:
    """Backticked unicode SQL refs refuse where Spark answers (R-CS2-8).

    Spark 4.1.2 answers ``SELECT `ünï``` / ``SELECT `ÜNÏ``` with the folded
    column and the backticked filter with row ``[2, null, "y"]``; RePark's
    SQL fold is still ASCII-only, so these refuse. ``SELECT `ÉTE``` answers
    on both doors (only ASCII case differs). ``STRASSE`` refuses on both.
    """
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        _uni(session).createOrReplaceTempView("uni")
        ete = session.sql("SELECT `ÉTE` FROM uni")
        assert _dtypes(ete) == [["ÉTE", "string"]]
        assert _rows(ete) == [["x"], ["y"], ["z"]]
        for key, statement in (
            ("fold", "SELECT `ünï` FROM uni"),
            ("upper", "SELECT `ÜNÏ` FROM uni"),
        ):
            try:
                session.sql(statement).collect()
            except Exception as error:
                assert type(error).__name__ == "AnalysisException", key
                assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION", key
                assert _sql_state(error) == "42703", key
            else:
                raise AssertionError(f"{key} answered instead of refusing")
        try:
            session.sql("SELECT `STRASSE` FROM (SELECT 1 AS id, 'v' AS `straße`)").collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert _sql_state(error) == "42703"
            assert _candidates(_plain_message(str(error))) == {"`id`", "`straße`"}
        else:
            raise AssertionError("STRASSE answered instead of refusing")
        try:
            _uni(session).selectExpr("`ünï`").collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert _sql_state(error) == "42703"
        else:
            raise AssertionError("selectExpr answered instead of refusing")
        try:
            _uni(session).filter("`ünï` is null").collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
        else:
            raise AssertionError("backticked filter answered instead of refusing")
    finally:
        session.stop()


def test_s5_preexisting_gaps_stay_pinned(tmp_path: Path) -> None:
    """The dict-fillna and exact-duplicate rename gaps stay pinned (R-CS2-9).

    Spark 4.1.2 answers ``fillna({"ID": 5})`` with ``[[1, null], [5, "b"]]``
    and renames both exact-duplicate displays to ``[z, z]``; RePark refuses
    the dict subset and no-ops the rename. Both predate this stack.
    """
    session = _open(tmp_path)
    try:
        session.conf.set("spark.sql.caseSensitive", "false")
        frame = session.createDataFrame([(1, None), (None, "b")], ["id", "Data"])
        try:
            frame.fillna({"ID": 5}).collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert str(error) == (
                "A column with name `ID` cannot be resolved for fillna; "
                "available columns: ['Data', 'id']"
            )
        else:
            raise AssertionError("dict fillna answered instead of refusing")
        _setup_tables_p4(session)
        table = session.table("sc.ns.t")
        doubled = table.select(
            functions.col("id").alias("id"), functions.col("id").alias("id")
        ).withColumnRenamed("id", "z")
        assert doubled.columns == ["id", "id"]
    finally:
        session.stop()


def test_s4_probe10_true_gap_stays_pinned(tmp_path: Path) -> None:
    """True multi-hit qualified select refuses qualified where Spark answers."""
    session = _open(tmp_path)
    try:
        _setup_tables(session)
        step = _oracle("p10/alias_dupe_sel_true")
        session.conf.set("spark.sql.caseSensitive", "true")
        try:
            _run_df(session, step["statement"]).collect()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert _sql_state(error) == "42703"
            mine = _plain_message(str(error))
            assert mine.split(_SUGGESTION_MARK)[0] == (
                "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
                "parameter with name `l`.`id` cannot be resolved. "
            ), "p10/alias_dupe_sel_true"
            assert _candidates(mine) == {"`id`"}, "p10/alias_dupe_sel_true"
        else:
            raise AssertionError("p10/alias_dupe_sel_true answered instead of refusing")
        assert step["spark"]["cols"], "p10/alias_dupe_sel_true"
    finally:
        session.stop()

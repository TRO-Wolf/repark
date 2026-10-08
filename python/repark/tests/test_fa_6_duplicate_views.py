from __future__ import annotations

from pathlib import Path

import _sm2_shared as sm2
import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as spark_functions
from repark.spark.dataframe import DataFrame

SELF_COLUMNS = ["id", "s", "v", "id", "s", "v"]
SELF_ROWS = [(1, "a", 10, 1, "a", 10), (2, "b", 20, 2, "b", 20)]


def _using_join(session: ReparkSession) -> DataFrame:
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    return left.alias("l").join(left.alias("r"), "id", "inner")


def _registered(tmp_path: Path, app: str) -> ReparkSession:
    session = sm2._open(tmp_path, app)
    sm2._self_join(session).createOrReplaceTempView("v1")
    sm2._mixed_join(session).createOrReplaceTempView("vm")
    _using_join(session).createOrReplaceTempView("vu")
    session.createDataFrame([(1, "w")], ["id", "w"]).createOrReplaceTempView("plain")
    return session


def _rows(frame: DataFrame) -> list[tuple[object, ...]]:
    return sorted(tuple(row) for row in frame.collect())


def _ambiguous(reference: str, *candidates: str) -> str:
    return (
        f"[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, could be: "
        f"[{', '.join(candidates)}]. SQLSTATE: 42704"
    )


def _first_line(error: Exception) -> str:
    return str(error).splitlines()[0].removeprefix("Error during planning: ")


def test_both_local_doors_register_a_duplicate_name_frame(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa6-doors")
    frame = sm2._self_join(session)
    frame.createOrReplaceTempView("v1")
    frame.createTempView("v2")
    for name in ("v1", "v2"):
        assert session.catalog.tableExists(name) is True
        assert session.sql(f"SELECT * FROM {name}").columns == SELF_COLUMNS
    assert frame.columns == SELF_COLUMNS
    session.stop()


def test_star_answers_display_names_values_and_types(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-star")
    frame = session.sql("SELECT * FROM v1")
    assert frame.columns == SELF_COLUMNS
    assert _rows(frame) == SELF_ROWS
    assert frame.schema.simpleString() == (
        "struct<id:bigint,s:string,v:bigint,id:bigint,s:string,v:bigint>"
    )
    table = frame.toArrow()
    assert table.column_names == SELF_COLUMNS
    assert [field.type for field in table.schema] == [
        pa.int64(),
        pa.string(),
        pa.int64(),
        pa.int64(),
        pa.string(),
        pa.int64(),
    ]
    assert list(frame.toPandas().columns) == SELF_COLUMNS
    session.stop()


@pytest.mark.parametrize(
    ("sql", "columns", "rows"),
    [
        ("SELECT * FROM v1 ORDER BY 4 DESC", SELF_COLUMNS, SELF_ROWS),
        ("SELECT x.* FROM v1 x", SELF_COLUMNS, SELF_ROWS),
        ("SELECT * FROM (SELECT * FROM v1) q", SELF_COLUMNS, SELF_ROWS),
        ("WITH c AS (SELECT * FROM v1) SELECT * FROM c", SELF_COLUMNS, SELF_ROWS),
        ("SELECT DISTINCT * FROM v1", SELF_COLUMNS, SELF_ROWS),
        ("SELECT * FROM v1 UNION ALL SELECT * FROM v1", SELF_COLUMNS, SELF_ROWS + SELF_ROWS),
        ("SELECT * FROM v1 LIMIT 5", SELF_COLUMNS, SELF_ROWS),
        (
            "SELECT *, 1 AS one FROM v1",
            [*SELF_COLUMNS, "one"],
            [(*row, 1) for row in SELF_ROWS],
        ),
        ("SELECT * FROM vm", ["id", "s", "v", "id", "t"], [(1, "a", 10, 1, "x")]),
        (
            "SELECT * FROM vu",
            ["id", "s", "v", "s", "v"],
            [(1, "a", 10, "a", 10), (2, "b", 20, "b", 20)],
        ),
        ("SELECT count(*) AS c FROM v1", ["c"], [(2,)]),
        ("SELECT t FROM vm", ["t"], [("x",)]),
        ("SELECT t, v, s FROM vm", ["t", "v", "s"], [("x", 10, "a")]),
        ("SELECT id FROM vu", ["id"], [(1,), (2,)]),
        ("SELECT a, d FROM v1 AS x(a, b, c, d, e, f)", ["a", "d"], [(1, 1), (2, 2)]),
        ("SELECT plain.id, w FROM v1 CROSS JOIN plain", ["id", "w"], [(1, "w"), (1, "w")]),
        ("SELECT (SELECT count(*) FROM v1) AS c", ["c"], [(2,)]),
    ],
)
def test_sql_over_the_view_answers_as_spark(
    tmp_path: Path, sql: str, columns: list[str], rows: list[tuple[object, ...]]
) -> None:
    session = _registered(tmp_path, "fa6-sql")
    frame = session.sql(sql)
    assert frame.columns == columns
    assert _rows(frame) == sorted(rows)
    session.stop()


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT id FROM v1", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        ("SELECT v1.id FROM v1", _ambiguous("`v1`.`id`", "`v1`.`id`", "`v1`.`id`")),
        ("SELECT s FROM v1", _ambiguous("`s`", "`v1`.`s`", "`v1`.`s`")),
        ("SELECT 1 AS one FROM v1 WHERE id = 1", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        ("SELECT id, count(*) FROM v1 GROUP BY id", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        ("SELECT max(v) FROM v1", _ambiguous("`v`", "`v1`.`v`", "`v1`.`v`")),
        ("SELECT id + 1 FROM v1", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        ("SELECT * FROM v1 WHERE v > 10", _ambiguous("`v`", "`v1`.`v`", "`v1`.`v`")),
        ("SELECT x.id FROM v1 x", _ambiguous("`x`.`id`", "`x`.`id`", "`x`.`id`")),
        ("SELECT id FROM (SELECT * FROM v1) q", _ambiguous("`id`", "`q`.`id`", "`q`.`id`")),
        (
            "WITH c AS (SELECT * FROM v1) SELECT id FROM c",
            _ambiguous("`id`", "`c`.`id`", "`c`.`id`"),
        ),
        ("SELECT id FROM vm", _ambiguous("`id`", "`vm`.`id`", "`vm`.`id`")),
        ("SELECT s FROM vu", _ambiguous("`s`", "`vu`.`s`", "`vu`.`s`")),
        (
            "SELECT * FROM v1 JOIN vm ON v1.v = vm.v",
            _ambiguous("`v1`.`v`", "`v1`.`v`", "`v1`.`v`"),
        ),
        (
            "SELECT v1.id FROM v1 CROSS JOIN vm",
            _ambiguous("`v1`.`id`", "`v1`.`id`", "`v1`.`id`"),
        ),
        (
            "SELECT id FROM v1 CROSS JOIN plain",
            _ambiguous("`id`", "`plain`.`id`", "`v1`.`id`", "`v1`.`id`"),
        ),
    ],
)
def test_a_reference_to_a_repeated_name_is_ambiguous(
    tmp_path: Path, sql: str, expected: str
) -> None:
    session = _registered(tmp_path, "fa6-ambiguous")
    refused = sm2._refusal_of(lambda: session.sql(sql).collect())
    assert isinstance(refused, AnalysisException)
    assert _first_line(refused) == expected
    assert "__repark_" not in str(refused)
    session.stop()


def test_a_missing_name_suggests_display_names_only(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-missing")
    refused = sm2._refusal_of(lambda: session.sql("SELECT nope FROM vm").collect())
    assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(refused)
    assert "[`id`, `s`, `v`, `id`, `t`]" in str(refused)
    assert "__repark_" not in str(refused)
    session.stop()


def test_case_sensitive_session_answers_the_same_cells(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa6-sensitive")
    session.conf.set("spark.sql.caseSensitive", "true")
    sm2._self_join(session).createOrReplaceTempView("vs")
    frame = session.sql("SELECT * FROM vs")
    assert frame.columns == SELF_COLUMNS
    assert _rows(frame) == SELF_ROWS
    refused = sm2._refusal_of(lambda: session.sql("SELECT id FROM vs").collect())
    assert _first_line(refused) == _ambiguous("`id`", "`vs`.`id`", "`vs`.`id`")
    session.stop()


def test_describe_and_list_columns_answer_display_names_in_schema_order(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-describe")
    for statement in ("DESCRIBE v1", "DESCRIBE TABLE v1"):
        described = session.sql(statement).collect()
        assert [(row["col_name"], row["data_type"]) for row in described] == [
            ("id", "bigint"),
            ("s", "string"),
            ("v", "bigint"),
            ("id", "bigint"),
            ("s", "string"),
            ("v", "bigint"),
        ]
    assert [column.name for column in session.catalog.listColumns("v1")] == SELF_COLUMNS
    assert [column.name for column in session.catalog.listColumns("vm")] == [
        "id",
        "s",
        "v",
        "id",
        "t",
    ]
    session.stop()


def test_table_frame_carries_display_names_through_dataframe_operations(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-table")
    frame = session.table("v1")
    assert frame.columns == SELF_COLUMNS
    assert frame.dtypes == [
        ("id", "bigint"),
        ("s", "string"),
        ("v", "bigint"),
        ("id", "bigint"),
        ("s", "string"),
        ("v", "bigint"),
    ]
    assert _rows(frame) == SELF_ROWS
    assert frame.count() == 2
    assert frame.limit(1).columns == SELF_COLUMNS
    assert frame.distinct().columns == SELF_COLUMNS
    assert frame.withColumn("z", spark_functions.lit(1)).columns == [*SELF_COLUMNS, "z"]
    dropped = frame.drop("id")
    assert dropped.columns == ["s", "v", "s", "v"]
    assert _rows(dropped) == [("a", 10, "a", 10), ("b", 20, "b", 20)]
    renamed = frame.toDF("a", "b", "c", "d", "e", "f").select("a", "d")
    assert _rows(renamed) == [(1, 1), (2, 2)]
    mixed = session.table("vm")
    assert _rows(mixed.select("t", "v")) == [("x", 10)]
    assert _rows(mixed.filter("t = 'x'")) == [(1, "a", 10, 1, "x")]
    assert _rows(mixed.orderBy("v")) == [(1, "a", 10, 1, "x")]
    session.stop()


def test_table_frame_refuses_a_repeated_name_on_the_dataframe_door(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-table-ambiguous")
    frame = session.table("v1")
    for action in (lambda: frame.select("id").collect(), lambda: frame["id"]):
        refused = sm2._refusal_of(action)
        assert isinstance(refused, AnalysisException)
        assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
        assert "__repark_" not in str(refused)
    session.stop()


def test_durable_writes_of_the_view_frame_follow_the_file_doors(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-writes")
    frame = session.table("v1")
    refused = sm2._refusal_of(lambda: frame.write.parquet(str(tmp_path / "pq")))
    assert _first_line(refused) == sm2._expected_dup_message("id")
    assert not (tmp_path / "pq").exists()
    target = tmp_path / "csv"
    frame.write.csv(str(target), header=True)
    for part in sorted(target.rglob("*.csv")):
        assert part.read_text(encoding="utf-8").splitlines()[0] == "id,s,v,id,s,v"
    sm2._assert_no_twin_bytes(target)
    session.stop()


def test_a_view_frame_registers_again_under_another_name(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-again")
    session.table("v1").createOrReplaceTempView("v9")
    session.sql("SELECT * FROM v1").createOrReplaceTempView("v8")
    for name in ("v9", "v8"):
        assert session.table(name).columns == SELF_COLUMNS
        assert _rows(session.sql(f"SELECT * FROM {name}")) == SELF_ROWS
    session.stop()


def test_create_table_as_select_star_refuses_and_creates_nothing(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-ctas")
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    refused = sm2._refusal_of(lambda: session.sql("CREATE TABLE sc.ns.dup AS SELECT * FROM v1"))
    assert isinstance(refused, AnalysisException)
    assert _first_line(refused) == sm2._expected_dup_message("id")
    assert session.catalog.tableExists("sc.ns.dup") is False
    session.sql("CREATE TABLE sc.ns.clean AS SELECT t, v, s FROM vm")
    clean = session.table("sc.ns.clean")
    assert clean.columns == ["t", "v", "s"]
    assert _rows(clean) == [("x", 10, "a")]
    session.stop()


def test_create_temp_view_as_select_star_refuses_unless_renamed(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa6-view-over")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    left.alias("l").join(
        left.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id")
    ).createOrReplaceTempView("v1")
    refused = sm2._refusal_of(
        lambda: session.sql("CREATE OR REPLACE TEMP VIEW v_over AS SELECT * FROM v1")
    )
    assert isinstance(refused, AnalysisException)
    assert _first_line(refused) == sm2._expected_dup_message("id")
    assert session.catalog.tableExists("v_over") is False
    session.sql("CREATE OR REPLACE TEMP VIEW va (a, b, c, d, e, f) AS SELECT * FROM v1")
    assert _rows(session.sql("SELECT a, d FROM va")) == [(1, 1), (2, 2)]
    session.stop()


def test_a_cached_view_answers_the_same(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-cache")
    session.sql("CACHE TABLE v1")
    assert session.sql("SELECT * FROM v1").columns == SELF_COLUMNS
    assert _rows(session.sql("SELECT * FROM v1")) == SELF_ROWS
    refused = sm2._refusal_of(lambda: session.sql("SELECT id FROM v1").collect())
    assert _first_line(refused) == _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")
    session.sql("UNCACHE TABLE v1")
    session.catalog.cacheTable("vm")
    assert session.sql("SELECT * FROM vm").columns == ["id", "s", "v", "id", "t"]
    cached = sm2._self_join(session).cache()
    cached.count()
    cached.createOrReplaceTempView("vc")
    assert session.sql("SELECT * FROM vc").columns == SELF_COLUMNS
    assert _rows(session.sql("SELECT * FROM vc")) == SELF_ROWS
    session.stop()


def test_replace_and_drop_leave_no_display_state_behind(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-replace")
    session.createDataFrame([(1, "a", 10)], ["id", "s", "v"]).createOrReplaceTempView("v1")
    assert session.table("v1").columns == ["id", "s", "v"]
    assert _rows(session.sql("SELECT id FROM v1")) == [(1,)]
    assert session.catalog.dropTempView("vm") is True
    assert session.catalog.tableExists("vm") is False
    names = [table.name for table in session.catalog.listTables()]
    assert not any("__repark_" in name for name in names)
    session.stop()


@pytest.mark.parametrize("door", ["createGlobalTempView", "createOrReplaceGlobalTempView"])
def test_global_doors_stay_unsupported_for_every_frame(tmp_path: Path, door: str) -> None:
    session = sm2._open(tmp_path, f"fa6-global-{door}")
    for frame in (sm2._self_join(session), session.createDataFrame([(1,)], ["id"])):
        refused = sm2._refusal_of(lambda frame=frame: getattr(frame, door)("g"))
        assert isinstance(refused, UnsupportedOperationException)
    session.stop()


def test_a_frame_without_repeated_names_registers_as_before(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "fa6-control")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    left.createOrReplaceTempView("pl")
    frame = session.sql("SELECT * FROM pl")
    assert frame.columns == ["id", "s", "v"]
    assert frame._display_names is None
    assert _rows(session.sql("SELECT id FROM pl")) == [(1,), (2,)]
    session.stop()


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM v1 ORDER BY id", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        (
            "SELECT count(*) AS c FROM v1 HAVING max(id) > 0",
            _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`"),
        ),
        ("SELECT * EXCEPT (id) FROM v1", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        ("SELECT ID FROM v1", _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`")),
        (
            "SELECT plain.id AS id FROM v1 CROSS JOIN plain ORDER BY id",
            _ambiguous("`id`", "`plain`.`id`", "`v1`.`id`", "`v1`.`id`"),
        ),
    ],
)
def test_loud_refusals_whose_text_differs_from_spark_divergence(
    tmp_path: Path, sql: str, expected: str
) -> None:
    session = _registered(tmp_path, "fa6-divergence")
    refused = sm2._refusal_of(lambda: session.sql(sql).collect())
    assert isinstance(refused, AnalysisException)
    assert _first_line(refused) == expected
    assert "__repark_" not in str(refused)
    session.stop()


def test_star_except_of_two_repeated_names_refuses_one_of_them_divergence(
    tmp_path: Path,
) -> None:
    session = _registered(tmp_path, "fa6-divergence-except")
    refused = sm2._refusal_of(lambda: session.sql("SELECT * EXCEPT (id, s) FROM v1").collect())
    assert isinstance(refused, AnalysisException)
    assert _first_line(refused) in {
        _ambiguous("`id`", "`v1`.`id`", "`v1`.`id`"),
        _ambiguous("`s`", "`v1`.`s`", "`v1`.`s`"),
    }
    session.stop()


def test_dataframe_door_ambiguity_lists_bare_candidates_divergence(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-divergence-frame")
    refused = sm2._refusal_of(lambda: session.table("v1").select("id").collect())
    assert _first_line(refused) == (
        "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`id`, `id`]."
    )
    session.stop()


def test_more_sql_shapes_over_the_view_answer_as_spark(tmp_path: Path) -> None:
    session = _registered(tmp_path, "fa6-more")
    two = session.sql("SELECT * FROM v1 CROSS JOIN vm LIMIT 1")
    assert two.columns == [*SELF_COLUMNS, "id", "s", "v", "id", "t"]
    assert _rows(session.sql("SELECT count(*) AS c, 1 AS one FROM v1 GROUP BY 2")) == [(2, 1)]
    windowed = session.sql("SELECT *, row_number() OVER (ORDER BY 1) AS rn FROM vm")
    assert windowed.columns == ["id", "s", "v", "id", "t", "rn"]
    assert _rows(windowed) == [(1, "a", 10, 1, "x", 1)]
    assert _rows(session.sql("SELECT w FROM plain WHERE id IN (SELECT v FROM vm)")) == []
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    session.sql(
        "CREATE TABLE sc.ns.tgt (a BIGINT, b STRING, c BIGINT, d BIGINT, e STRING, f BIGINT)"
    )
    session.sql("INSERT INTO sc.ns.tgt SELECT * FROM v1")
    assert _rows(session.sql("SELECT * FROM sc.ns.tgt")) == SELF_ROWS
    refused = sm2._refusal_of(lambda: session.sql("CREATE VIEW sc.ns.pv AS SELECT * FROM v1"))
    assert _first_line(refused) == sm2._expected_dup_message("id")
    refused = sm2._refusal_of(lambda: session.table("v1").write.saveAsTable("sc.ns.sat"))
    assert _first_line(refused) == sm2._expected_dup_message("id")
    assert session.catalog.tableExists("sc.ns.sat") is False
    assert session.read.table("v1").columns == SELF_COLUMNS
    assert session.table("v1").union(session.table("v1")).columns == SELF_COLUMNS
    session.stop()

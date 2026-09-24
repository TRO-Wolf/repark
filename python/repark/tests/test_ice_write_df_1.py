"""U7 PR1 — the DataFrame writer surface: ``save(name)``, ``bucketBy`` and ``output-spec-id``.

Every expected value is Spark 4.1.2 + Iceberg 1.11.0's answer, read from the committed
``ice_write_df_1_spark_oracle.json``: the four scoreboard cells ``W-DF-SAVE-NAME``,
``W-DF-SAVE-OVERWRITE-NAME``, ``W-DF-V1-BUCKETBY-ERR`` and ``W-DF-OPT-OUTPUT-SPEC-ID``
under ``recorded``, and the step-1 probe shapes under ``measured``. The catalog is named
``sc`` with namespace ``u7`` so the refusal texts carry the recorded relation names.

pins: u7-write-df/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010,
C-011, C-012, C-013
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, NumberFormatException

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_write_df_1_spark_oracle.json").read_text(encoding="utf-8")
)
_RECORDED: dict[str, Any] = _ORACLE["recorded"]
_MEASURED: dict[str, Any] = _ORACLE["measured"]
_T = "sc.u7.t"
_CTAS = "CREATE TABLE {t} USING iceberg "
_AS = " AS SELECT * FROM v"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """A session with the ``sc`` memory catalog and the ``u7`` namespace."""
    session = ReparkSession.builder.appName("pytest-ice-write-df-1").getOrCreate()
    session.register_memory_catalog("sc", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE sc.u7")
    return session


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )


def _seed(spark: ReparkSession, table: str = _T, part: str = "") -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part}")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")


def _seed_empty(spark: ReparkSession, part: str) -> None:
    spark.sql(f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg {part}")


def _rows(spark: ReparkSession, table: str = _T) -> list[list[Any]]:
    return [list(row) for row in spark.sql(f"SELECT * FROM {table} ORDER BY id").collect()]


def _operations(spark: ReparkSession, table: str = _T) -> list[str]:
    rows = spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at").collect()
    return [str(row[0]) for row in rows]


def _summaries(spark: ReparkSession, table: str = _T) -> list[dict[str, str]]:
    keys = ("added-records", "deleted-records", "total-records", "added-data-files")
    rows = spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at").collect()
    return [{k: v for k, v in dict(row[0]).items() if k in keys} for row in rows]


def _specs(spark: ReparkSession, table: str = _T) -> list[list[int]]:
    query = f"SELECT spec_id, count(*) FROM {table}.files GROUP BY spec_id ORDER BY spec_id"
    return [list(row) for row in spark.sql(query).collect()]


def _partitioning(spark: ReparkSession, table: str = _T) -> list[str]:
    rows = spark.sql(f"DESCRIBE TABLE {table}").collect()
    return [str(row[1]) for row in rows if str(row[0]).startswith("Part ")]


def _partition_info(spark: ReparkSession, table: str = _T) -> list[str]:
    names = [str(row[0]) for row in spark.sql(f"DESCRIBE TABLE {table}").collect()]
    if "# Partition Information" not in names:
        return []
    section = names[names.index("# Partition Information") + 2 :]
    ends = [index for index, name in enumerate(section) if name == "" or name.startswith("#")]
    return section[: ends[0]] if ends else section


def _assert_state(spark: ReparkSession, cell: str, table: str = _T) -> None:
    expected = _MEASURED[cell]
    assert _rows(spark, table) == expected["rows"]
    assert _operations(spark, table) == expected["operations"]
    assert _specs(spark, table) == expected["specs"]
    assert _partitioning(spark, table) == expected["partitioning"]
    if "partition_info" in expected:
        assert _partition_info(spark, table) == expected["partition_info"]


def _assert_error(raised: BaseException, cell: str, root: Path | None = None) -> None:
    expected = _MEASURED[cell]["error"]
    message = expected["message"]
    if root is not None:
        message = message.replace("<root>", str(root))
    assert type(raised).__name__ == expected["type"]
    assert str(raised) == message
    if expected["condition"] is not None:
        assert raised.getCondition() == expected["condition"]  # type: ignore[attr-defined]


def _assert_recorded(spark: ReparkSession, cell: str) -> None:
    recorded = _RECORDED[cell]
    assert _rows(spark) == recorded["data"]
    operations = [dict(summary)["operation"] for summary in recorded["md.snapshots"]]
    assert _operations(spark) == operations
    spec = [[name, transform, source] for name, transform, source in recorded["md.spec"]]
    assert _spec_fields(spark) == spec


def _spec_fields(spark: ReparkSession) -> list[list[str]]:
    fields: list[list[str]] = []
    for transform in _partitioning(spark):
        if transform.startswith("bucket("):
            count, source = transform[len("bucket(") : -1].split(", ")
            fields.append([f"{source}_bucket", f"bucket[{count}]", source])
        else:
            fields.append([transform, "identity", transform])
    return fields


def test_save_name_appends_like_the_recorded_cell(spark: ReparkSession) -> None:
    """``save('sc.u7.t')`` in append mode is a table append (cell W-DF-SAVE-NAME).

    pins: u7-write-df/C-001
    """
    _seed(spark)
    _frame(spark).write.format("iceberg").mode("append").save(_T)
    _assert_recorded(spark, "W-DF-SAVE-NAME")
    assert _summaries(spark)[1]["total-records"] == "5"


def test_save_name_overwrites_like_the_recorded_cell(spark: ReparkSession) -> None:
    """``save('sc.u7.t')`` in overwrite mode replaces every row (W-DF-SAVE-OVERWRITE-NAME).

    pins: u7-write-df/C-002
    """
    _seed(spark)
    _frame(spark).write.format("iceberg").mode("overwrite").save(_T)
    _assert_recorded(spark, "W-DF-SAVE-OVERWRITE-NAME")
    summary = _summaries(spark)[1]
    assert (summary["deleted-records"], summary["total-records"]) == ("3", "2")


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_save_name_on_a_missing_table_refuses_in_write_modes(
    spark: ReparkSession, mode: str
) -> None:
    """Append and overwrite need the table; Spark raises TABLE_OR_VIEW_NOT_FOUND.

    pins: u7-write-df/C-003
    """
    with pytest.raises(AnalysisException) as raised:
        _frame(spark).write.format("iceberg").mode(mode).save(f"sc.u7.missing_{mode[0]}")
    _assert_error(raised.value, f"save_name_missing_{mode}")
    assert not spark.catalog.tableExists(f"sc.u7.missing_{mode[0]}")


@pytest.mark.parametrize(
    ("cell", "write"),
    [
        ("save_name_missing_default", lambda w: w.save("sc.u7.missing_d")),
        ("save_name_missing_ignore", lambda w: w.mode("ignore").save("sc.u7.missing_i")),
        ("save_name_missing_partby", lambda w: w.partitionBy("cat").save("sc.u7.missing_p")),
    ],
)
def test_save_name_creates_a_missing_table_in_create_modes(
    spark: ReparkSession, cell: str, write: Callable[[Any], None]
) -> None:
    """The error and ignore modes create the table (Spark's CTAS arm), partitionBy included.

    pins: u7-write-df/C-003
    """
    write(_frame(spark).write.format("iceberg"))
    table = "sc.u7.missing_" + cell.rsplit("_", 1)[1][0]
    expected = _MEASURED[cell]
    assert _rows(spark, table) == expected["rows"]
    assert _operations(spark, table) == expected["operations"]
    assert _specs(spark, table) == expected["specs"]


@pytest.mark.parametrize(
    "cell", ["save_name_existing_default_mode", "save_name_existing_errorifexists"]
)
def test_save_name_on_an_existing_table_refuses_in_error_mode(
    spark: ReparkSession, cell: str
) -> None:
    """The error modes refuse with Spark's TABLE_OR_VIEW_ALREADY_EXISTS text; ignore is a no-op.

    pins: u7-write-df/C-004
    """
    _seed(spark)
    writer = _frame(spark).write.format("iceberg")
    if cell.endswith("errorifexists"):
        writer = writer.mode("errorifexists")
    with pytest.raises(AnalysisException) as raised:
        writer.save(_T)
    _assert_error(raised.value, cell)
    _frame(spark).write.format("iceberg").mode("ignore").save(_T)
    _assert_state(spark, "save_name_existing_ignore")


def test_save_reads_the_name_from_the_path_option(spark: ReparkSession) -> None:
    """``option('path', name).save()`` routes the same way as ``save(name)``.

    pins: u7-write-df/C-004
    """
    _seed(spark)
    _frame(spark).write.format("iceberg").mode("append").option("path", _T).save()
    _assert_state(spark, "save_name_via_option_path")


@pytest.mark.parametrize(
    ("cell", "mode"),
    [("save_name_partitionBy", "append"), ("save_name_overwrite_partby_existing", "overwrite")],
)
def test_save_name_refuses_a_partitioning_that_differs_from_the_table(
    spark: ReparkSession, cell: str, mode: str
) -> None:
    """A partitionBy that differs from the table's spec refuses; the table is untouched.

    pins: u7-write-df/C-005
    """
    _seed(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        _frame(spark).write.format("iceberg").mode(mode).partitionBy("cat").save(_T)
    _assert_error(raised.value, cell)
    _assert_state(spark, cell)


def test_save_name_with_a_matching_partitioning_appends(spark: ReparkSession) -> None:
    """partitionBy equal to the table's spec passes the check and appends.

    pins: u7-write-df/C-005
    """
    _seed_empty(spark, "PARTITIONED BY (cat)")
    _frame(spark).write.format("iceberg").mode("append").partitionBy("cat").save(_T)
    expected = _MEASURED["save_name_append_partby_match"]
    assert _rows(spark) == expected["rows"]
    assert _operations(spark) == expected["operations"]
    assert _specs(spark) == expected["specs"]


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_save_to_a_real_path_refuses_with_spark_path_relation(
    spark: ReparkSession, tmp_path: Path, mode: str
) -> None:
    """A path target is a path identifier; with no table there Spark says TABLE_OR_VIEW_NOT_FOUND.

    pins: u7-write-df/C-006
    """
    leaf = "pathtbl" if mode == "append" else "pathtbl2"
    target = tmp_path / leaf
    with pytest.raises(AnalysisException) as raised:
        _frame(spark).write.format("iceberg").mode(mode).save(str(target))
    expected = _MEASURED[f"save_path_{mode}"]["error"]["message"].replace("`<wh>`", f"`{tmp_path}`")
    assert str(raised.value) == expected
    assert raised.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert not target.exists()


def test_save_to_a_real_path_in_create_mode_is_a_declared_refusal(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """RePark has no path-based Iceberg tables; the create modes keep the declared refusal.

    pins: u7-write-df/C-006
    """
    with pytest.raises(AnalysisException) as raised:
        _frame(spark).write.format("iceberg").save(str(tmp_path / "pathtbl3"))
    assert str(raised.value) == (
        "DataFrameWriter.save(path) requires format('parquet'|'csv'|'json'|'text'); "
        "use saveAsTable for Iceberg tables"
    )


def test_bucket_by_creates_a_bucket_partitioned_table(spark: ReparkSession) -> None:
    """``bucketBy(4, 'id').saveAsTable`` creates ``id_bucket bucket[4] id`` (W-DF-V1-BUCKETBY-ERR).

    pins: u7-write-df/C-007
    """
    _frame(spark).write.format("iceberg").bucketBy(4, "id").saveAsTable(_T)
    _assert_recorded(spark, "W-DF-V1-BUCKETBY-ERR")
    expected = {"added-records": "2", "total-records": "2", "added-data-files": "1"}
    assert _summaries(spark) == [expected]


@pytest.mark.parametrize(
    ("cell", "write"),
    [
        ("bucketBy_new_append", lambda w: w.bucketBy(4, "id").mode("append")),
        ("bucketBy_new_overwrite", lambda w: w.bucketBy(4, "id").mode("overwrite")),
        ("bucketBy_new_upper_col", lambda w: w.bucketBy(4, "ID")),
        ("bucketBy_new_partitionBy", lambda w: w.partitionBy("cat").bucketBy(4, "id")),
    ],
)
def test_bucket_by_new_table_shapes(
    spark: ReparkSession, cell: str, write: Callable[[Any], Any]
) -> None:
    """Every mode creates the table; overwrite is Spark's RTAS; the column resolves case-free.

    pins: u7-write-df/C-007
    """
    write(_frame(spark).write.format("iceberg")).saveAsTable(_T)
    _assert_state(spark, cell)


@pytest.mark.parametrize(
    ("cell", "write"),
    [
        ("bucketBy_new_sortBy", lambda w: w.bucketBy(4, "id").sortBy("data")),
        ("bucketBy_new_two_cols", lambda w: w.bucketBy(4, "id", "data")),
    ],
)
def test_bucket_by_shapes_iceberg_cannot_convert_refuse(
    spark: ReparkSession, cell: str, write: Callable[[Any], Any]
) -> None:
    """sortBy and multi-column buckets refuse with Spark's text; no table is created.

    pins: u7-write-df/C-008
    """
    with pytest.raises(IllegalArgumentException) as raised:
        write(_frame(spark).write.format("iceberg")).saveAsTable(_T)
    _assert_error(raised.value, cell)
    assert not spark.catalog.tableExists(_T)


def test_bucket_by_existing_unbucketed_table(spark: ReparkSession) -> None:
    """On an existing table: append refuses the mismatch, error refuses, overwrite replaces.

    pins: u7-write-df/C-009
    """
    _seed(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        _frame(spark).write.format("iceberg").bucketBy(4, "id").mode("append").saveAsTable(_T)
    _assert_error(raised.value, "bucketBy_existing_append")
    with pytest.raises(AnalysisException) as exists:
        _frame(spark).write.format("iceberg").bucketBy(4, "id").saveAsTable(_T)
    assert exists.value.getCondition() == "TABLE_OR_VIEW_ALREADY_EXISTS"
    _frame(spark).write.format("iceberg").bucketBy(4, "id").mode("overwrite").saveAsTable(_T)
    _assert_state(spark, "bucketBy_existing_overwrite")


def test_bucket_by_existing_bucketed_table(spark: ReparkSession) -> None:
    """A matching layout appends; count, case and extra transforms refuse with Spark's text.

    pins: u7-write-df/C-009
    """
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (bucket(4, id))"
    )
    spark.sql(f"INSERT INTO {_T} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    writer = _frame(spark).write.format("iceberg")
    writer.bucketBy(4, "id").mode("append").saveAsTable(_T)
    _assert_state(spark, "bucket_existing_match_append")
    for cell, count, column in [
        ("bucket_existing_other_n_append", 8, "id"),
        ("bucket_existing_upper_append", 4, "ID"),
    ]:
        with pytest.raises(IllegalArgumentException) as raised:
            _frame(spark).write.format("iceberg").bucketBy(count, column).mode(
                "append"
            ).saveAsTable(_T)
        _assert_error(raised.value, cell)
    _assert_state(spark, "bucket_existing_upper_append")


def test_bucket_by_existing_partitioned_table(spark: ReparkSession) -> None:
    """partitionBy plus bucketBy compares the whole transform list, in Spark's order.

    pins: u7-write-df/C-009
    """
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (cat, bucket(4, id))"
    )
    frame = _frame(spark)
    frame.write.format("iceberg").partitionBy("cat").bucketBy(4, "id").mode("append").saveAsTable(
        _T
    )
    _assert_state(spark, "bucket_existing_part_match_append")
    refusals: list[tuple[str, Callable[[Any], Any]]] = [
        ("bucket_existing_part_only_bucket_append", lambda w: w.bucketBy(4, "id")),
        ("bucket_existing_two_append", lambda w: w.bucketBy(4, "id", "data")),
        ("bucket_existing_sorted_append", lambda w: w.bucketBy(4, "id").sortBy("data")),
        ("bucket_existing_missing_append", lambda w: w.bucketBy(4, "nope")),
    ]
    for cell, write in refusals:
        with pytest.raises(IllegalArgumentException) as raised:
            write(frame.write.format("iceberg")).mode("append").saveAsTable(_T)
        _assert_error(raised.value, cell)
    frame.write.format("iceberg").bucketBy(4, "id").mode("ignore").saveAsTable(_T)
    _assert_state(spark, "bucket_existing_ignore")


def test_bucket_by_existing_table_overwrite_with_partition_by(spark: ReparkSession) -> None:
    """Overwrite replaces the table with ``[cat, id_bucket]`` (Spark's RTAS, spec id 1).

    pins: u7-write-df/C-009
    """
    _seed(spark)
    _frame(spark).write.format("iceberg").partitionBy("cat").bucketBy(4, "id").mode(
        "overwrite"
    ).saveAsTable(_T)
    _assert_state(spark, "bucket_existing_overwrite_part")


def test_output_spec_id_writes_under_the_old_spec(spark: ReparkSession) -> None:
    """``output-spec-id=0`` after an added field lands under spec 0 (W-DF-OPT-OUTPUT-SPEC-ID).

    pins: u7-write-df/C-010
    """
    _seed(spark)
    spark.sql(f"ALTER TABLE {_T} ADD PARTITION FIELD cat")
    _frame(spark).write.format("iceberg").option("output-spec-id", "0").mode("append").saveAsTable(
        _T
    )
    recorded = _RECORDED["W-DF-OPT-OUTPUT-SPEC-ID"]
    assert _specs(spark) == recorded["specs"]
    assert _rows(spark) == recorded["data"]


@pytest.mark.parametrize(
    ("cell", "write"),
    [
        (
            "spec_id_old_insertInto",
            lambda df: df.write.option("output-spec-id", "0").insertInto(_T),
        ),
        (
            "spec_id_old_writeTo_append",
            lambda df: df.writeTo(_T).option("output-spec-id", "0").append(),
        ),
        (
            "spec_id_old_save_name",
            lambda df: (
                df.write.format("iceberg").option("output-spec-id", "0").mode("append").save(_T)
            ),
        ),
        (
            "spec_id_old_overwrite",
            lambda df: (
                df.write.format("iceberg")
                .option("output-spec-id", "0")
                .mode("overwrite")
                .saveAsTable(_T)
            ),
        ),
        (
            "spec_id_old_overwritePartitions",
            lambda df: df.writeTo(_T).option("output-spec-id", "0").overwritePartitions(),
        ),
    ],
)
def test_output_spec_id_reaches_every_writer(
    spark: ReparkSession, cell: str, write: Callable[[Any], None]
) -> None:
    """insertInto, writeTo append, save(name), overwrite and overwritePartitions honour it.

    pins: u7-write-df/C-010
    """
    _seed(spark)
    spark.sql(f"ALTER TABLE {_T} ADD PARTITION FIELD cat")
    write(_frame(spark))
    expected = _MEASURED[cell]
    assert _rows(spark) == expected["rows"]
    assert _operations(spark) == expected["operations"]
    assert _specs(spark) == expected["specs"]


def test_output_spec_id_writes_partitioned_files_under_an_old_partitioned_spec(
    spark: ReparkSession,
) -> None:
    """Spec 0 partitioned by cat after the field is dropped: two files per write under spec 0.

    pins: u7-write-df/C-010
    """
    _seed(spark, part="PARTITIONED BY (cat)")
    spark.sql(f"ALTER TABLE {_T} DROP PARTITION FIELD cat")
    _frame(spark).write.format("iceberg").option("output-spec-id", "0").mode("append").saveAsTable(
        _T
    )
    assert _specs(spark) == _MEASURED["spec_id_old_partitioned"]["specs"]
    spark.sql(f"INSERT INTO {_T} VALUES (9, 'z', 'q')")
    assert _specs(spark) == _MEASURED["spec_id_sql_insert"]["specs"]


@pytest.mark.parametrize("cell", ["spec_id_unknown", "spec_id_negative", "spec_id_nonnumeric"])
def test_output_spec_id_refusals(spark: ReparkSession, cell: str) -> None:
    """An unknown or negative id and a non-integer refuse with Spark's class and text.

    pins: u7-write-df/C-011
    """
    _seed(spark)
    spark.sql(f"ALTER TABLE {_T} ADD PARTITION FIELD cat")
    raw = {"spec_id_unknown": "7", "spec_id_negative": "-1", "spec_id_nonnumeric": "x"}[cell]
    with pytest.raises(IllegalArgumentException) as raised:
        _frame(spark).write.format("iceberg").option("output-spec-id", raw).mode(
            "append"
        ).saveAsTable(_T)
    _assert_error(raised.value, cell)
    assert _operations(spark) == ["append"]


def test_output_spec_id_parses_like_java_integer(spark: ReparkSession) -> None:
    """A padded id is a ``NumberFormatException``; a signed one parses, as Java's parseInt.

    pins: u7-write-df/C-011
    """
    _seed(spark)
    writer = _frame(spark).write.format("iceberg").mode("append")
    with pytest.raises(NumberFormatException) as raised:
        writer.option("output-spec-id", " 1").saveAsTable(_T)
    _assert_error(raised.value, "spec_id_space")
    assert issubclass(NumberFormatException, IllegalArgumentException)
    writer.option("output-spec-id", "+0").saveAsTable(_T)
    _assert_state(spark, "spec_id_plus")


def test_output_spec_id_current_and_absent_land_under_the_current_spec(
    spark: ReparkSession,
) -> None:
    """The current id and no option both write under spec 1 (near miss).

    pins: u7-write-df/C-011, C-012
    """
    _seed(spark)
    spark.sql(f"ALTER TABLE {_T} ADD PARTITION FIELD cat")
    _frame(spark).write.format("iceberg").option("output-spec-id", "1").mode("append").saveAsTable(
        _T
    )
    assert _specs(spark) == _MEASURED["spec_id_current"]["specs"]
    _frame(spark).write.format("iceberg").mode("append").saveAsTable(_T)
    assert _specs(spark) == [[0, 1], [1, 4]]


def test_near_misses_keep_their_paths(spark: ReparkSession, tmp_path: Path) -> None:
    """Parquet/CSV path saves still write files; saveAsTable without bucketBy still appends.

    pins: u7-write-df/C-012
    """
    frame = _frame(spark)
    frame.write.format("parquet").mode("overwrite").save(str(tmp_path / "p"))
    frame.write.format("csv").mode("overwrite").save(str(tmp_path / "c"))
    written = sorted(tuple(row) for row in spark.read.parquet(str(tmp_path / "p")).collect())
    assert written == sorted(tuple(row) for row in frame.collect())
    assert any((tmp_path / "c").glob("*.csv"))
    _seed(spark)
    frame.write.format("iceberg").mode("append").saveAsTable(_T)
    assert _operations(spark) == ["append", "append"]
    assert _partitioning(spark) == []


@pytest.mark.parametrize(
    ("cell", "sql"),
    [
        ("sql_clustered_one", _CTAS + "CLUSTERED BY (id) INTO 4 BUCKETS" + _AS),
        (
            "sql_part_clustered",
            "CREATE TABLE {t} USING iceberg PARTITIONED BY (cat) CLUSTERED BY (id) INTO 4 BUCKETS "
            "AS SELECT * FROM v",
        ),
        ("sql_clustered_upper", _CTAS + "CLUSTERED BY (ID) INTO 4 BUCKETS" + _AS),
        ("ctas_bucket_upper", _CTAS + "PARTITIONED BY (bucket(4, ID))" + _AS),
        ("ctas_part_upper", _CTAS + "PARTITIONED BY (CAT)" + _AS),
        (
            "rtas_new_clustered",
            "CREATE OR REPLACE TABLE {t} USING iceberg CLUSTERED BY (id) INTO 4 BUCKETS "
            "AS SELECT * FROM v",
        ),
    ],
)
def test_sql_door_bucket_clauses(spark: ReparkSession, cell: str, sql: str) -> None:
    """The SQL twins of bucketBy: CLUSTERED BY joins PARTITIONED BY; columns resolve case-free.

    pins: u7-write-df/C-013
    """
    _frame(spark).createOrReplaceTempView("v")
    spark.sql(sql.format(t=_T))
    _assert_state(spark, cell)


@pytest.mark.parametrize(
    ("cell", "sql"),
    [
        ("sql_clustered_two", _CTAS + "CLUSTERED BY (id, data) INTO 4 BUCKETS" + _AS),
        (
            "sql_clustered_sorted",
            "CREATE TABLE {t} USING iceberg CLUSTERED BY (id) SORTED BY (data) INTO 4 BUCKETS "
            "AS SELECT * FROM v",
        ),
        (
            "create_clustered_two",
            "CREATE TABLE {t} (a INT, b INT) USING iceberg CLUSTERED BY (a, b) INTO 4 BUCKETS",
        ),
        (
            "create_clustered_sorted",
            "CREATE TABLE {t} (id BIGINT, data STRING) USING iceberg CLUSTERED BY (id) "
            "SORTED BY (data) INTO 4 BUCKETS",
        ),
    ],
)
def test_sql_door_bucket_refusals(spark: ReparkSession, cell: str, sql: str) -> None:
    """Multi-column and sorted CLUSTERED BY refuse with Spark's IllegalArgumentException text.

    pins: u7-write-df/C-013
    """
    _frame(spark).createOrReplaceTempView("v")
    with pytest.raises(IllegalArgumentException) as raised:
        spark.sql(sql.format(t=_T))
    _assert_error(raised.value, cell)
    assert not spark.catalog.tableExists(_T)


def test_sql_door_create_resolves_partition_columns_case_free(spark: ReparkSession) -> None:
    """``PARTITIONED BY (CAT)`` / ``bucket(4, ID)`` on a column-def CREATE resolve like Spark.

    pins: u7-write-df/C-013
    """
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg "
        "PARTITIONED BY (bucket(4, ID))"
    )
    assert _partitioning(spark) == _MEASURED["create_bucket_upper"]["partitioning"]
    spark.sql(f"DROP TABLE {_T}")
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg PARTITIONED BY (CAT)"
    )
    _assert_state(spark, "create_part_upper")
    spark.sql(f"INSERT INTO {_T} VALUES (1, 'a', 'x'), (2, 'b', 'y')")
    assert _specs(spark) == [[0, 2]]

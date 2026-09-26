"""E-CASE-SELECT facade pins: SELECT output columns keep the query spelling."""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.functions import col, lit, when


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-u11-edge-case-select").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql("CREATE TABLE sc.ns.t (id INT, Data STRING) USING iceberg")
    session.sql("INSERT INTO sc.ns.t VALUES (1, 'a')")
    session.sql("CREATE TEMP VIEW sv AS SELECT 1 AS id, STRUCT(5 AS a, 'z' AS b) AS s")
    return session


def _names(frame) -> list[str]:
    """Return the output column names of a frame."""
    return [field.name for field in frame.schema.fields]


def test_select_keeps_written_spelling(spark: ReparkSession) -> None:
    """SELECT ID, data names the outputs ID/data with the matching row.

    pins: u11-edge-1/C-001
    """
    frame = spark.sql("SELECT ID, data FROM sc.ns.t WHERE DATA = 'a'")
    assert _names(frame) == ["ID", "data"]
    assert [tuple(row) for row in frame.collect()] == [(1, "a")]


def test_select_mixed_case_single_column(spark: ReparkSession) -> None:
    """SELECT Data names the output Data.

    pins: u11-edge-1/C-002
    """
    assert _names(spark.sql("SELECT Data FROM sc.ns.t")) == ["Data"]


def test_select_alias_wins(spark: ReparkSession) -> None:
    """SELECT id AS X names the output X.

    pins: u11-edge-1/C-003
    """
    assert _names(spark.sql("SELECT id AS X FROM sc.ns.t")) == ["X"]


def test_select_star_keeps_stored_names(spark: ReparkSession) -> None:
    """SELECT * names the outputs with the stored column names.

    pins: u11-edge-1/C-004
    """
    assert _names(spark.sql("SELECT * FROM sc.ns.t")) == ["id", "Data"]


def test_select_qualified_and_struct_field(spark: ReparkSession) -> None:
    """Qualified t.ID names the output ID; struct field s.A names it A.

    pins: u11-edge-1/C-005
    """
    assert _names(spark.sql("SELECT t.ID FROM sc.ns.t t")) == ["ID"]
    struct_frame = spark.sql("SELECT s.A FROM sv")
    assert _names(struct_frame) == ["A"]
    assert [tuple(row) for row in struct_frame.collect()] == [(5,)]


def test_select_group_by_and_union(spark: ReparkSession) -> None:
    """GROUP BY ID keeps the ID spelling and the alias; UNION takes the left branch spelling.

    pins: u11-edge-1/C-006
    """
    grouped = spark.sql("SELECT ID, count(*) AS c FROM sc.ns.t GROUP BY ID")
    assert _names(grouped) == ["ID", "c"]
    assert [tuple(row) for row in grouped.collect()] == [(1, 1)]
    union = spark.sql("SELECT ID FROM sc.ns.t UNION SELECT id FROM sc.ns.t")
    assert _names(union) == ["ID"]
    assert [tuple(row) for row in union.collect()] == [(1,)]


def test_case_sensitive_select_refuses(spark: ReparkSession) -> None:
    """SELECT ID under caseSensitive=true refuses with UNRESOLVED_COLUMN.

    pins: u11-edge-1/C-007
    """
    sensitive = ReparkSession.builder.appName("pytest-u11-edge-sensitive").getOrCreate()
    sensitive.sql("CREATE TEMP VIEW v AS SELECT 1 AS id")
    sensitive.sql("SET spark.sql.caseSensitive=true")
    try:
        with pytest.raises(Exception) as error:
            sensitive.sql("SELECT ID FROM v").collect()
        assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(error.value)
        assert "with name `ID` cannot be resolved" in str(error.value)
        assert "Did you mean one of the following? [`id`]" in str(error.value)
        assert [field.name for field in sensitive.sql("SELECT id FROM v").schema.fields] == ["id"]
    finally:
        sensitive.stop()


def test_dataframe_select_keeps_spelling(spark: ReparkSession) -> None:
    """df.select('ID') names the output ID with the matching row.

    pins: u11-edge-1/C-008
    """
    frame = spark.table("sc.ns.t").select("ID")
    assert _names(frame) == ["ID"]
    assert [tuple(row) for row in frame.collect()] == [(1,)]


def test_dataframe_door_binds_spelled_sql_columns(spark: ReparkSession) -> None:
    """F.col binds a spelled SQL output case-insensitively, like live Spark.

    pins: u11-edge-1/C-015
    """
    spark.sql("INSERT INTO sc.ns.t VALUES (2, 'b')")
    frame = spark.sql("SELECT ID, data FROM sc.ns.t")
    for name in ("id", "ID"):
        kept = frame.filter(col(name) > 1)
        assert _names(kept) == ["ID", "data"]
        assert [tuple(row) for row in kept.collect()] == [(2, "b")]
    shifted = frame.select(col("id") + 1)
    assert _names(shifted) == ["(id + 1)"]
    assert sorted(tuple(row) for row in shifted.collect()) == [(2,), (3,)]
    aliased = spark.sql("SELECT id AS Id FROM sc.ns.t").filter(col("Id") > 1)
    assert _names(aliased) == ["Id"]
    assert [tuple(row) for row in aliased.collect()] == [(2,)]
    created = spark.createDataFrame([(1,), (2,)], ["Id"]).filter(col("id") > 1)
    assert _names(created) == ["Id"]
    assert [tuple(row) for row in created.collect()] == [(2,)]


def _spelled(spark: ReparkSession, sql: str):
    """Seed the second row and return the spelled SQL frame."""
    spark.sql("INSERT INTO sc.ns.t VALUES (2, 'b')")
    return spark.sql(sql)


def _shape(frame) -> tuple[list[str], list[tuple]]:
    """Return a frame's names and its sorted rows."""
    return _names(frame), sorted(tuple(row) for row in frame.collect())


def _typed(frame) -> tuple[list[tuple[str, str]], list[tuple]]:
    """Return a frame's names with their simple types and its sorted rows."""
    fields = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
    return fields, sorted(tuple(row) for row in frame.collect())


def test_drop_binds_spelled_names(spark: ReparkSession) -> None:
    """drop('ID'), drop('id') and drop(F.col('ID')) remove the spelled ID; drop of both empties.

    pins: u11-edge-1/C-017
    """
    frame = _spelled(spark, "SELECT ID, data FROM sc.ns.t")
    for target in ("ID", "id", col("ID")):
        assert _shape(frame.drop(target)) == (["data"], [("a",), ("b",)])
    assert _shape(frame.drop("ID", "data")) == ([], [(), ()])
    assert _shape(frame.drop("data")) == (["ID"], [(1,), (2,)])


def test_qualified_references_bind_spelled_names(spark: ReparkSession) -> None:
    """t.ID and t.id resolve through the relation alias and name the output as written.

    pins: u11-edge-1/C-018
    """
    frame = _spelled(spark, "SELECT ID, data FROM sc.ns.t t")
    assert _shape(frame.select(col("t.ID"))) == (["ID"], [(1,), (2,)])
    assert _shape(frame.select(col("t.id"))) == (["id"], [(1,), (2,)])
    assert _shape(frame.filter(col("t.id") > 1)) == (["ID", "data"], [(2, "b")])
    stored = spark.sql("SELECT id FROM sc.ns.t t").select(col("t.id"))
    assert _shape(stored) == (["id"], [(1,), (2,)])
    aliased = spark.table("sc.ns.t").alias("t").select(col("t.ID"))
    assert _shape(aliased) == (["ID"], [(1,), (2,)])


def test_join_on_a_spelled_key(spark: ReparkSession) -> None:
    """join(…, 'ID') binds the spelled key on both sides and keeps one key column.

    pins: u11-edge-1/C-019
    """
    frame = _spelled(spark, "SELECT ID, data FROM sc.ns.t")
    joined = frame.join(spark.sql("SELECT 1 AS ID, 'q' AS w"), "ID")
    assert _shape(joined) == (["ID", "data", "w"], [(1, "a", "q")])


def test_union_by_name_matches_names_case_insensitively(spark: ReparkSession) -> None:
    """unionByName pairs ID/id and data/Data and keeps the left spelling.

    pins: u11-edge-1/C-020
    """
    left = _spelled(spark, "SELECT ID, data FROM sc.ns.t")
    unioned = left.unionByName(spark.sql("SELECT id, Data FROM sc.ns.t"))
    assert _shape(unioned) == (["ID", "data"], [(1, "a"), (1, "a"), (2, "b"), (2, "b")])


def test_qualified_drop_binds_through_its_relation(spark: ReparkSession) -> None:
    """A qualified Column drop binds through its relation; an unmatched drop is a no-op.

    pins: u11-edge-1/C-022
    """
    frame = _spelled(spark, "SELECT ID, data FROM sc.ns.t t")
    whole = (["ID", "data"], [(1, "a"), (2, "b")])
    assert _shape(frame.drop(col("t.ID"))) == (["data"], [("a",), ("b",)])
    assert _shape(frame.drop(col("t.id"))) == (["data"], [("a",), ("b",)])
    for target in ("t.ID", "u.id", col("u.id")):
        assert _shape(frame.drop(target)) == whole
    joined = spark.sql("SELECT a.id, b.ID FROM sc.ns.t a JOIN sc.ns.t b ON a.id = b.id")
    assert _shape(joined.drop(col("b.id"))) == (["id"], [(1,), (2,)])
    assert _shape(joined.drop(col("A.ID"))) == (["ID"], [(1,), (2,)])


def _ambiguous(reference: str, options: str) -> str:
    """Return Spark's AMBIGUOUS_REFERENCE sentence."""
    return (
        f"[AMBIGUOUS_REFERENCE] Reference `{reference}` is ambiguous, could be: [{options}]. "
        "SQLSTATE: 42704"
    )


def test_bare_reference_matching_two_fields_is_ambiguous(spark: ReparkSession) -> None:
    """A bare reference matching two fields ignoring case refuses, exact spelling included.

    pins: u11-edge-1/C-023
    """
    joined = _spelled(spark, "SELECT a.id, b.ID FROM sc.ns.t a JOIN sc.ns.t b ON a.id = b.id")
    left = spark.sql("SELECT ID, data FROM sc.ns.t")
    right = spark.sql("SELECT 1 AS id, 'q' AS w")
    refusals = [
        (lambda: joined.select("id"), _ambiguous("id", "`a`.`id`, `b`.`id`")),
        (lambda: joined.select(col("ID")), _ambiguous("ID", "`a`.`ID`, `b`.`ID`")),
        (
            lambda: left.join(right, left["ID"] == right["id"]).select("id"),
            _ambiguous("id", "`id`, `sc`.`ns`.`t`.`id`"),
        ),
        (
            lambda: left.join(right, col("ID") == col("id")),
            _ambiguous("ID", "`ID`, `sc`.`ns`.`t`.`ID`"),
        ),
        (
            lambda: right.join(left, col("id") == col("ID")),
            _ambiguous("id", "`id`, `sc`.`ns`.`t`.`id`"),
        ),
    ]
    for build, sentence in refusals:
        with pytest.raises(AnalysisException) as excinfo:
            build().collect()
        assert sentence in str(excinfo.value)
    with pytest.raises(AnalysisException, match=r"^Error during planning: \[AMBIGUOUS_REFERENCE\]"):
        joined.filter(col("Id") > 1).collect()
    assert _shape(joined.select(col("a.Id"))) == (["Id"], [(1,), (2,)])


def _twins(spark: ReparkSession):
    """Return the joined twin frame and a createDataFrame twin frame."""
    joined = _spelled(spark, "SELECT a.id, b.ID FROM sc.ns.t a JOIN sc.ns.t b ON a.id = b.id")
    return joined, spark.createDataFrame([(1, 2)], ["id", "ID"])


def test_whole_frame_projections_bind_by_attribute(spark: ReparkSession) -> None:
    """The facade's own re-projections of a twin frame answer like Spark; written refs refuse.

    pins: u11-edge-1/C-024
    """
    joined, created = _twins(spark)
    both = (["id", "ID"], [(1, 2)])
    assert _shape(created.withColumn("z", lit(1))) == (["id", "ID", "z"], [(1, 2, 1)])
    assert _shape(created.withColumns({"z": lit(1)})) == (["id", "ID", "z"], [(1, 2, 1)])
    assert _shape(created.toDF("p", "q")) == (["p", "q"], [(1, 2)])
    for frame in (
        created.select("*"),
        created.selectExpr("*"),
        created.fillna(0),
        created.dropDuplicates(["id"]),
        created.dropDuplicates(["ID"]),
    ):
        assert _shape(frame) == both
    keyed = spark.createDataFrame([(1, 2), (1, 3)], ["id", "ID"]).dropDuplicates(["id"])
    assert _shape(keyed) == (["id", "ID"], [(1, 2), (1, 3)])
    pairs = [(1, 1), (2, 2)]
    assert _shape(joined.select("*")) == (["id", "ID"], pairs)
    assert _shape(joined.dropDuplicates(["id"])) == (["id", "ID"], pairs)
    assert _shape(joined.withColumn("z", lit(1))) == (["id", "ID", "z"], [(1, 1, 1), (2, 2, 1)])
    assert _shape(joined.toDF("p", "q")) == (["p", "q"], pairs)
    for build in (
        lambda: created.select("id"),
        lambda: created.select(col("id")),
        lambda: created.withColumn("z", col("id")),
        lambda: created.dropna(),
    ):
        with pytest.raises(AnalysisException) as excinfo:
            build().collect()
        assert _ambiguous("id", "`id`, `id`") in str(excinfo.value)


def test_origin_columns_bind_their_own_side(spark: ReparkSession) -> None:
    """Origin Columns select, filter and drop their own side's field after a join.

    pins: u11-edge-1/C-025
    """
    left = spark.sql("SELECT ID, data FROM sc.ns.t")
    right = spark.sql("SELECT 1 AS id, 'q' AS w")
    joined = left.join(right, left["ID"] == right["id"])
    assert _shape(joined.select(right["id"])) == (["id"], [(1,)])
    assert _shape(joined.select(left["ID"])) == (["ID"], [(1,)])
    three = joined.select(left["data"], right["w"], right["id"])
    assert _shape(three) == (["data", "w", "id"], [("a", "q", 1)])
    whole = (["ID", "data", "id", "w"], [(1, "a", 1, "q")])
    assert _shape(joined.filter(right["id"] > 0)) == whole
    assert _shape(joined.drop(right["id"])) == (["ID", "data", "w"], [(1, "a", "q")])
    assert _shape(joined.drop(left["ID"])) == (["data", "id", "w"], [("a", 1, "q")])


def test_column_drop_matching_two_fields_is_ambiguous(spark: ReparkSession) -> None:
    """drop(F.col('id')) on twins refuses like select; the string form drops every twin.

    pins: u11-edge-1/C-026
    """
    joined, created = _twins(spark)
    for frame, options in ((created, "`id`, `id`"), (joined, "`a`.`id`, `b`.`id`")):
        with pytest.raises(AnalysisException) as excinfo:
            frame.drop(col("id")).collect()
        assert _ambiguous("id", options) in str(excinfo.value)
    assert _shape(created.drop("id")) == ([], [()])
    assert _shape(joined.drop("id")) == ([], [(), ()])


def test_compound_origin_columns_keep_their_attribute_binding(spark: ReparkSession) -> None:
    """Arithmetic, cast, alias, when and withColumn over origin Columns bind their side.

    pins: u11-edge-1/C-027
    """
    left = spark.sql("SELECT ID, data FROM sc.ns.t")
    right = spark.sql("SELECT 1 AS id, 'q' AS w")
    joined = left.join(right, left["ID"] == right["id"])
    assert _typed(joined.select(right["id"] + 1)) == ([("(id + 1)", "int")], [(2,)])
    assert _typed(joined.select(right["id"].cast("string"))) == ([("id", "string")], [("1",)])
    cast = joined.select((right["id"] + 1).cast("string").alias("n"))
    assert _typed(cast) == ([("n", "string")], [("2",)])
    picked = joined.select(when(left["ID"] > 0, right["w"]).alias("r"))
    assert _typed(picked) == ([("r", "string")], [("q",)])
    assert _shape(joined.withColumn("id", right["id"] + 1)) == (
        ["id", "data", "id", "w"],
        [(2, "a", 2, "q")],
    )
    with pytest.raises(AnalysisException) as excinfo:
        joined.select(right["w"], col("id") + 1).collect()
    assert "[AMBIGUOUS_REFERENCE]" in str(excinfo.value)
    assert "_repark_" not in str(excinfo.value)


def _projections(spark: ReparkSession):
    """Return the plain and the twin-expression projections of the joined frame."""
    left = spark.sql("SELECT ID, data FROM sc.ns.t")
    right = spark.sql("SELECT 1 AS id, 'q' AS w")
    joined = left.join(right, left["ID"] == right["id"])
    return (
        joined.select(left["ID"].alias("id"), left["data"].alias("Data")),
        joined.select((right["id"] + 1).alias("id"), left["data"].alias("Data")),
    )


def _table(spark: ReparkSession) -> list[tuple]:
    """Return the sorted rows of sc.ns.t."""
    return sorted(tuple(row) for row in spark.sql("SELECT * FROM sc.ns.t").collect())


def test_projection_of_a_twin_join_writes_back_into_its_table(spark: ReparkSession) -> None:
    """Appending, inserting or overwriting a projection of a twin join into t works like Spark.

    pins: u11-edge-1/C-028
    """
    spark.sql("INSERT INTO sc.ns.t VALUES (2, 'b')")
    writes = (
        lambda frame: frame.writeTo("sc.ns.t").append(),
        lambda frame: frame.write.mode("append").saveAsTable("sc.ns.t"),
        lambda frame: (
            frame.createOrReplaceTempView("jv"),
            spark.sql("INSERT INTO sc.ns.t SELECT id, Data FROM jv"),
        ),
    )
    for write in writes:
        write(_projections(spark)[0])
        assert _table(spark) == [(1, "a"), (1, "a"), (2, "b")]
        write(_projections(spark)[1])
        assert _table(spark) == [(1, "a"), (1, "a"), (2, "a"), (2, "a"), (2, "b")]
        spark.sql("DELETE FROM sc.ns.t WHERE Data = 'a'")
        spark.sql("INSERT INTO sc.ns.t VALUES (1, 'a')")
    plain, _ = _projections(spark)
    plain.writeTo("sc.ns.t").overwrite(col("id") >= 1)
    assert _table(spark) == [(1, "a")]
    spark.sql("INSERT INTO sc.ns.t VALUES (2, 'b')")
    _, twin = _projections(spark)
    twin.writeTo("sc.ns.t").overwrite(col("id") >= 1)
    assert _table(spark) == [(2, "a")]


def test_star_column_and_suggestions_show_presented_fields_only(spark: ReparkSession) -> None:
    """col('*') over a twin-join projection expands like Spark; no scratch name is suggested.

    pins: u11-edge-1/C-029
    """
    left = spark.sql("SELECT ID, data FROM sc.ns.t")
    right = spark.sql("SELECT 1 AS id, 'q' AS w")
    joined = left.join(right, left["ID"] == right["id"])
    assert _typed(joined.select(right["id"] + 1, col("*"))) == (
        [("(id + 1)", "int"), ("ID", "int"), ("data", "string"), ("id", "int"), ("w", "string")],
        [(2, 1, "a", 1, "q")],
    )
    with pytest.raises(AnalysisException) as excinfo:
        joined.select(right["id"] + 1, col("nope")).collect()
    assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(excinfo.value)
    assert all(f"`{name}`" in str(excinfo.value) for name in ("ID", "data", "id", "w"))
    assert "repark" not in str(excinfo.value)


def test_attribute_copies_never_collide_with_a_user_field(spark: ReparkSession) -> None:
    """A user field spelled like an attribute copy keeps its value beside the copy.

    pins: u11-edge-1/C-030
    """
    left = spark.sql("SELECT ID, data, 7 AS __repark_attr_6964 FROM sc.ns.t")
    right = spark.sql("SELECT 1 AS id, 'q' AS w")
    joined = left.join(right, left["ID"] == right["id"])
    assert _typed(joined.select(right["id"] + 1, left["data"])) == (
        [("(id + 1)", "int"), ("data", "string")],
        [(2, "a")],
    )
    kept = joined.select(right["id"] + 1, left["data"], left["__repark_attr_6964"])
    assert _typed(kept) == (
        [("(id + 1)", "int"), ("data", "string"), ("__repark_attr_6964", "int")],
        [(2, "a", 7)],
    )

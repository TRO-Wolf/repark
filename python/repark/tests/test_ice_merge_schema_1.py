"""IPI-19 + IPI-56 + IPI-37 — schema evolution on write.

Oracle: live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 measured by run 26e into
``/tmp/oc-worker/qe/probe/p2.json`` (facts M-1..M-9 of the unit plan) and the twelve inventory
cells ``W-DF-OPT-MERGE-SCHEMA-*``, ``W-DF-V2-MERGE-SCHEMA``, ``W-MERGE-SCHEMA-EVOLUTION-*``,
``W-DFMERGE-*`` and ``SC-MERGE-SCHEMA-SQL``.

Every expected value below is Spark's recorded answer, not a RePark derivation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException
from repark.spark.functions import col, expr, lit

NS = "mem.ns"
MERGE_SCHEMA_CONF = "spark.sql.iceberg.merge-schema"
ACCEPT_ANY = "write.spark.accept-any-schema"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ice-merge-schema-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _create(spark: ReparkSession, name: str, *, accept_any: bool = False, part: str = "") -> str:
    table = f"{NS}.{name}"
    props = "'format-version' = '2'"
    if accept_any:
        props += f", '{ACCEPT_ANY}' = 'true'"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part} "
        f"TBLPROPERTIES ({props})"
    )
    return table


def _seed_named(spark: ReparkSession, table: str) -> None:
    spark.sql(
        f"INSERT INTO {table} SELECT 1 AS id, 'a' AS data, 'x' AS cat "
        "UNION ALL SELECT 2, 'b', 'y' UNION ALL SELECT 3, 'c', 'x'"
    )


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )


def _schema(spark: ReparkSession, table: str) -> list[tuple[str, str, bool]]:
    arrow = spark.sql(f"SELECT * FROM {table} LIMIT 0").to_arrow().schema
    return [(field.name, str(field.type), field.nullable) for field in arrow]


def _rows(spark: ReparkSession, table: str) -> list[list[Any]]:
    arrow = spark.sql(f"SELECT * FROM {table} ORDER BY id").to_arrow()
    names = arrow.schema.names
    return [[row[name] for name in names] for row in arrow.to_pylist()]


def _snapshots(spark: ReparkSession, table: str) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").to_arrow().to_pylist()[0]["n"]


BASE_SCHEMA = [("id", "int64", True), ("data", "string", True), ("cat", "string", True)]
EVOLVED_LONG = [*BASE_SCHEMA, ("extra", "int64", True)]
EVOLVED_INT = [*BASE_SCHEMA, ("extra", "int32", True)]
MERGE_SCHEMA_ROWS = [
    [1, "a", "x", None],
    [2, "b", "y", None],
    [3, "c", "x", None],
    [7, "g", "x", 14],
    [8, "h", "w", 16],
]


@pytest.mark.parametrize("key", ["mergeSchema", "merge-schema"])
def test_df_merge_schema_option_adds_the_column_last(spark: ReparkSession, key: str) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-001"""
    table = _create(spark, f"df_{key.replace('-', '_')}", accept_any=True)
    _seed_named(spark, table)
    frame = _frame(spark)
    frame.withColumn("extra", frame.id * 2).write.format("iceberg").option(key, "true").mode(
        "append"
    ).saveAsTable(table)
    assert _schema(spark, table) == EVOLVED_LONG
    assert _rows(spark, table) == MERGE_SCHEMA_ROWS
    assert _snapshots(spark, table) == 2


def test_df_write_to_merge_schema_adds_the_column_last(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-001"""
    table = _create(spark, "df_write_to", accept_any=True)
    _seed_named(spark, table)
    frame = _frame(spark)
    frame.withColumn("extra", frame.id * 2).writeTo(table).option("mergeSchema", "true").append()
    assert _schema(spark, table) == EVOLVED_LONG
    assert _rows(spark, table) == MERGE_SCHEMA_ROWS
    assert _snapshots(spark, table) == 2


def test_df_merge_schema_commits_exactly_one_snapshot(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-001"""
    table = _create(spark, "df_one_snap", accept_any=True)
    _seed_named(spark, table)
    before = _snapshots(spark, table)
    frame = _frame(spark)
    frame.withColumn("extra", frame.id * 2).write.format("iceberg").option(
        "mergeSchema", "true"
    ).mode("append").saveAsTable(table)
    assert _schema(spark, table) == EVOLVED_LONG
    assert _snapshots(spark, table) - before == 1


def test_merge_schema_without_accept_any_schema_raises(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-002"""
    table = _create(spark, "df_no_prop")
    _seed_named(spark, table)
    frame = _frame(spark)
    with pytest.raises(AnalysisException) as caught:
        frame.withColumn("extra", frame.id * 2).write.format("iceberg").option(
            "mergeSchema", "true"
        ).mode("append").saveAsTable(table)
    message = str(caught.value)
    assert "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]" in message
    assert "Table columns: `id`, `data`, `cat`." in message
    assert "Data columns: `id`, `data`, `cat`, `extra`." in message
    assert "SQLSTATE: 21S01" in message
    assert _schema(spark, table) == BASE_SCHEMA
    assert _rows(spark, table) == [[1, "a", "x"], [2, "b", "y"], [3, "c", "x"]]
    assert _snapshots(spark, table) == 1


def test_merge_schema_missing_column_writes_null(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-003"""
    table = _create(spark, "df_missing", accept_any=True)
    _seed_named(spark, table)
    frame = spark.createDataFrame([(7, "g", 14)], "id BIGINT, data STRING, extra BIGINT")
    frame.write.format("iceberg").option("mergeSchema", "true").mode("append").saveAsTable(table)
    assert _schema(spark, table) == EVOLVED_LONG
    assert _rows(spark, table) == [
        [1, "a", "x", None],
        [2, "b", "y", None],
        [3, "c", "x", None],
        [7, "g", None, 14],
    ]


def test_merge_schema_does_not_narrow_a_wider_column(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-003"""
    table = _create(spark, "df_widen", accept_any=True)
    _seed_named(spark, table)
    frame = spark.createDataFrame(
        [(7, "g", "x", 14)], "id INT, data STRING, cat STRING, extra BIGINT"
    )
    frame.write.format("iceberg").option("mergeSchema", "true").mode("append").saveAsTable(table)
    assert _schema(spark, table) == EVOLVED_LONG
    assert _rows(spark, table) == [
        [1, "a", "x", None],
        [2, "b", "y", None],
        [3, "c", "x", None],
        [7, "g", "x", 14],
    ]


def test_merge_schema_false_with_conf_true_still_raises(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-004"""
    table = _create(spark, "df_opt_false", accept_any=True)
    _seed_named(spark, table)
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        frame = _frame(spark)
        with pytest.raises(IllegalArgumentException) as caught:
            frame.withColumn("extra", frame.id * 2).write.format("iceberg").option(
                "mergeSchema", "false"
            ).mode("append").saveAsTable(table)
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)
    assert "Field extra not found in source schema" in str(caught.value)
    assert _schema(spark, table) == BASE_SCHEMA
    assert _snapshots(spark, table) == 1


def test_df_merge_schema_from_the_session_conf(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-004"""
    table = _create(spark, "df_conf_true", accept_any=True)
    _seed_named(spark, table)
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        frame = _frame(spark)
        frame.withColumn("extra", frame.id * 2).write.format("iceberg").mode("append").saveAsTable(
            table
        )
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)
    assert _schema(spark, table) == EVOLVED_LONG
    assert _rows(spark, table) == MERGE_SCHEMA_ROWS


def test_extra_column_with_the_property_but_no_flag_refuses_like_java(
    spark: ReparkSession,
) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-002"""
    table = _create(spark, "df_plain_extra", accept_any=True)
    _seed_named(spark, table)
    frame = _frame(spark)
    with pytest.raises(IllegalArgumentException, match="Field extra not found in source schema"):
        frame.withColumn("extra", frame.id * 2).write.format("iceberg").mode("append").saveAsTable(
            table
        )
    assert _schema(spark, table) == BASE_SCHEMA
    assert _snapshots(spark, table) == 1


def test_extra_column_without_the_property_is_the_arity_error(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-002"""
    table = _create(spark, "df_plain_extra_no_prop")
    _seed_named(spark, table)
    frame = _frame(spark)
    with pytest.raises(AnalysisException) as caught:
        frame.withColumn("extra", frame.id * 2).write.format("iceberg").mode("append").saveAsTable(
            table
        )
    message = str(caught.value)
    assert "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]" in message
    assert "Table columns: `id`, `data`, `cat`." in message
    assert "Data columns: `id`, `data`, `cat`, `extra`." in message
    assert "SQLSTATE: 21S01" in message
    assert _schema(spark, table) == BASE_SCHEMA
    assert _snapshots(spark, table) == 1


def test_write_to_extra_column_without_the_property_is_the_arity_error(
    spark: ReparkSession,
) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-002"""
    table = _create(spark, "df_v2_extra_no_prop")
    _seed_named(spark, table)
    frame = _frame(spark)
    with pytest.raises(AnalysisException) as caught:
        frame.withColumn("extra", frame.id * 2).writeTo(table).option(
            "mergeSchema", "true"
        ).append()
    assert "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]" in str(caught.value)
    assert _schema(spark, table) == BASE_SCHEMA


MERGE_STMT = (
    "MERGE WITH SCHEMA EVOLUTION INTO {t} t USING {v} s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
)
BIGINT_SOURCE = (
    "SELECT CAST(2 AS BIGINT) AS id, 'B' AS data, 'y' AS cat, 7 AS extra "
    "UNION ALL SELECT CAST(4 AS BIGINT), 'D', 'z', 8"
)
NO_NEW_SOURCE = (
    "SELECT CAST(2 AS BIGINT) AS id, 'B' AS data, 'y' AS cat "
    "UNION ALL SELECT CAST(4 AS BIGINT), 'D', 'z'"
)


def _merge_fixture(
    spark: ReparkSession, name: str, source: str, *, accept_any: bool = False
) -> str:
    table = _create(spark, name, accept_any=accept_any)
    _seed_named(spark, table)
    view = f"v_{name}"
    spark.sql(source).createOrReplaceTempView(view)
    return table


def test_merge_with_schema_evolution_bigint(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-005"""
    table = _merge_fixture(spark, "mse_bigint", BIGINT_SOURCE)
    spark.sql(MERGE_STMT.format(t=table, v="v_mse_bigint"))
    assert _schema(spark, table) == EVOLVED_INT
    assert _rows(spark, table) == [
        [1, "a", "x", None],
        [2, "B", "y", 7],
        [3, "c", "x", None],
        [4, "D", "z", 8],
    ]
    assert _snapshots(spark, table) == 2


def test_schema_evolution_commits_one_snapshot(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-005"""
    table = _merge_fixture(spark, "mse_one_snap", BIGINT_SOURCE)
    before = _snapshots(spark, table)
    spark.sql(MERGE_STMT.format(t=table, v="v_mse_one_snap"))
    assert _schema(spark, table) == EVOLVED_INT
    assert _snapshots(spark, table) - before == 1


def test_merge_with_schema_evolution_no_new_column(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-005"""
    table = _merge_fixture(spark, "mse_nonew", NO_NEW_SOURCE)
    spark.sql(MERGE_STMT.format(t=table, v="v_mse_nonew"))
    assert _schema(spark, table) == BASE_SCHEMA
    assert _rows(spark, table) == [
        [1, "a", "x"],
        [2, "B", "y"],
        [3, "c", "x"],
        [4, "D", "z"],
    ]
    assert _snapshots(spark, table) == 2


def test_merge_with_schema_evolution_needs_no_property(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-006"""
    table = _merge_fixture(spark, "mse_plain", BIGINT_SOURCE)
    frame = spark.createDataFrame([(9, "i", "z")], "id BIGINT, data STRING, cat STRING")
    with pytest.raises(AnalysisException) as without_property:
        frame.withColumn("extra", frame.id).write.format("iceberg").option(
            "mergeSchema", "true"
        ).mode("append").saveAsTable(table)
    assert "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]" in str(without_property.value)
    spark.sql(MERGE_STMT.format(t=table, v="v_mse_plain"))
    assert _schema(spark, table) == EVOLVED_INT


def test_merge_with_schema_evolution_is_case_insensitive_in_the_clause(
    spark: ReparkSession,
) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-005"""
    table = _merge_fixture(spark, "mse_case", BIGINT_SOURCE)
    spark.sql(
        "merge   with\n schema\tevolution into {t} t USING {v} s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(
            t=table, v="v_mse_case"
        )
    )
    assert _schema(spark, table) == EVOLVED_INT


def test_plain_merge_with_extra_source_column_still_works(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-007"""
    table = _merge_fixture(spark, "plain_merge", BIGINT_SOURCE)
    spark.sql(
        f"MERGE INTO {table} t USING v_plain_merge s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.data = s.data"
    )
    assert _schema(spark, table) == BASE_SCHEMA
    assert _rows(spark, table) == [[1, "a", "x"], [2, "B", "y"], [3, "c", "x"]]


def test_plain_merge_named_schema_evolution_column_is_not_swallowed(
    spark: ReparkSession,
) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-007"""
    table = _create(spark, "evolution_col")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'evolution', 'x')")
    spark.sql("SELECT CAST(1 AS BIGINT) AS id, 'z' AS data, 'q' AS cat").createOrReplaceTempView(
        "v_evolution_col"
    )
    spark.sql(
        f"MERGE INTO {table} t USING v_evolution_col s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET *"
    )
    assert _rows(spark, table) == [[1, "z", "q"]]


def _dfmerge_fixture(spark: ReparkSession, name: str) -> tuple[str, Any]:
    table = _create(spark, name)
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    source = spark.createDataFrame(
        [(2, "B", "y"), (4, "D", "z")], "id BIGINT, data STRING, cat STRING"
    ).alias("s")
    return table, source


def test_df_merge_into_upsert_with_the_spark_qualifier(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-008"""
    table, source = _dfmerge_fixture(spark, "dfm_upsert")
    (
        source.mergeInto(table, expr("dfm_upsert.id = s.id"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    assert _rows(spark, table) == [[1, "a", "x"], [2, "B", "y"], [3, "c", "x"], [4, "D", "z"]]


def test_df_merge_into_delete_with_the_spark_qualifier(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-008"""
    table, source = _dfmerge_fixture(spark, "dfm_delete")
    source.mergeInto(table, expr("dfm_delete.id = s.id")).whenMatched().delete().merge()
    assert _rows(spark, table) == [[1, "a", "x"], [3, "c", "x"]]


def test_df_merge_into_update_cols_with_the_spark_qualifier(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-008"""
    table, source = _dfmerge_fixture(spark, "dfm_update_cols")
    source.mergeInto(table, expr("dfm_update_cols.id = s.id")).whenMatched().update(
        {"data": expr("s.data")}
    ).merge()
    assert _rows(spark, table) == [[1, "a", "x"], [2, "B", "y"], [3, "c", "x"]]


def test_df_merge_into_not_matched_by_source_with_the_spark_qualifier(
    spark: ReparkSession,
) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-008"""
    table, source = _dfmerge_fixture(spark, "dfm_nmbs")
    source.mergeInto(table, expr("dfm_nmbs.id = s.id")).whenNotMatchedBySource().delete().merge()
    assert _rows(spark, table) == [[2, "b", "y"]]


def test_df_merge_into_conditional_with_the_spark_qualifier(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-008"""
    table, source = _dfmerge_fixture(spark, "dfm_conditional")
    (
        source.mergeInto(table, expr("dfm_conditional.id = s.id"))
        .whenMatched(expr("s.id = 2"))
        .updateAll()
        .whenNotMatched(expr("s.id > 3"))
        .insert({"id": expr("s.id"), "data": lit("n"), "cat": expr("s.cat")})
        .merge()
    )
    assert _rows(spark, table) == [[1, "a", "x"], [2, "B", "y"], [3, "c", "x"], [4, "n", "z"]]


def test_df_merge_into_with_schema_evolution(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-009"""
    table, source = _dfmerge_fixture(spark, "dfm_evolution")
    (
        source.withColumn("extra", lit(1))
        .mergeInto(table, expr("dfm_evolution.id = s.id"))
        .withSchemaEvolution()
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    assert _schema(spark, table) == EVOLVED_INT
    assert _rows(spark, table) == [
        [1, "a", "x", None],
        [2, "B", "y", 1],
        [3, "c", "x", None],
        [4, "D", "z", 1],
    ]
    assert _snapshots(spark, table) == 2


def test_df_merge_into_keeps_the_target_source_qualifiers(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-010"""
    table, source = _dfmerge_fixture(spark, "dfm_legacy")
    (
        source.mergeInto(table, col("target.id") == col("source.id"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    assert _rows(spark, table) == [[1, "a", "x"], [2, "B", "y"], [3, "c", "x"], [4, "D", "z"]]


def test_df_merge_into_bare_key_sugar_still_upserts(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-010"""
    table, source = _dfmerge_fixture(spark, "dfm_sugar")
    source.mergeInto(table, "id").whenMatched().updateAll().whenNotMatched().insertAll().merge()
    assert _rows(spark, table) == [[1, "a", "x"], [2, "B", "y"], [3, "c", "x"], [4, "D", "z"]]


BY_NAME = "INSERT INTO {t} BY NAME SELECT 1 AS id, 'a' AS data, 'x' AS cat, 5 AS extra"


def test_sql_insert_by_name_merge_schema_matrix(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-011"""
    plain_false = _create(spark, "bn_plain_false")
    with pytest.raises(AnalysisException) as arity_off:
        spark.sql(BY_NAME.format(t=plain_false))
    assert "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]" in str(arity_off.value)
    assert "SQLSTATE: 21S01" in str(arity_off.value)

    plain_true = _create(spark, "bn_plain_true")
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        with pytest.raises(AnalysisException) as arity_on:
            spark.sql(BY_NAME.format(t=plain_true))
        assert "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]" in str(arity_on.value)
        assert "SQLSTATE: 21S01" in str(arity_on.value)
        assert _schema(spark, plain_true) == BASE_SCHEMA

        opted_false = _create(spark, "bn_opted_false", accept_any=True)
        spark.conf.set(MERGE_SCHEMA_CONF, "false")
        with pytest.raises(IllegalArgumentException) as not_found:
            spark.sql(BY_NAME.format(t=opted_false))
        assert "Field extra not found in source schema" in str(not_found.value)
        assert "EXTRA_COLUMNS" not in str(not_found.value)
        assert _schema(spark, opted_false) == BASE_SCHEMA

        opted_true = _create(spark, "bn_opted_true", accept_any=True)
        spark.conf.set(MERGE_SCHEMA_CONF, "true")
        spark.sql(BY_NAME.format(t=opted_true))
        assert _schema(spark, opted_true) == EVOLVED_INT
        assert _rows(spark, opted_true) == [[1, "a", "x", 5]]
        assert _snapshots(spark, opted_true) == 1
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)


def test_sql_insert_by_name_extra_column_keeps_the_extra_columns_class(
    spark: ReparkSession,
) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-011"""
    table = _create(spark, "bn_misnamed")
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"INSERT INTO {table} BY NAME SELECT 1 AS id, 'a' AS data, 'q' AS nope")
    assert "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS]" in str(caught.value)
    assert "SQLSTATE: KD000" in str(caught.value)


def test_sql_set_carries_the_merge_schema_conf(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-012"""
    table = _create(spark, "bn_sql_set", accept_any=True)
    spark.sql(f"SET {MERGE_SCHEMA_CONF} = true")
    try:
        assert spark.conf.get(MERGE_SCHEMA_CONF) == "true"
        spark.sql(BY_NAME.format(t=table))
        assert _schema(spark, table) == EVOLVED_INT
        spark.conf.unset(MERGE_SCHEMA_CONF)
        assert spark.conf.get(MERGE_SCHEMA_CONF, "false") != "true"
        with pytest.raises(
            IllegalArgumentException, match="Field extra2 not found in source schema"
        ):
            spark.sql(
                f"INSERT INTO {table} BY NAME "
                "SELECT 1 AS id, 'a' AS data, 'x' AS cat, 5 AS extra, 6 AS extra2"
            )
        assert _schema(spark, table) == EVOLVED_INT
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)


def test_positional_insert_values_never_evolves(spark: ReparkSession) -> None:
    """pins: ipi-19-56-37-schema-evolution-write/C-013"""
    table = _create(spark, "pos_values", accept_any=True)
    spark.conf.set(MERGE_SCHEMA_CONF, "true")
    try:
        with pytest.raises(AnalysisException) as caught:
            spark.sql(f"INSERT INTO {table} VALUES (1, 'a', 'x', 5)")
    finally:
        spark.conf.unset(MERGE_SCHEMA_CONF)
    assert "Field col1 not found in source schema" not in str(caught.value)
    assert _schema(spark, table) == BASE_SCHEMA

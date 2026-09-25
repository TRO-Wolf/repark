"""U7 PR2 slice 2 — ``DataFrameWriterV2.overwrite(condition)`` is Spark's overwrite by filter.

Every expected value is Spark 4.1.2 + Iceberg 1.11.0's answer, read through the helpers of
``test_ice_write_df_2.py`` from the committed ``ice_write_df_1_spark_oracle.json``: the
scoreboard cells ``W-DF-V2-OVERWRITE-COND-PART`` and ``W-DF-V2-OVERWRITE-COND-ROWS`` under
``recorded``, and the ``oc_*`` and ``vf_*`` shapes under ``measured``. A residue pin states
RePark's answer as a rule over Spark's recorded one.

pins: u7-write-df-2/C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from test_ice_write_df_2 import (
    _MEASURED,
    _PARTITIONED,
    _RECORDED,
    _T,
    Action,
    _branch_rows,
    _create_branch,
    _frame,
    _recorded_summaries,
    _refs_like_the_record,
    _run,
    _seed,
    _sorted_rows,
    _summaries_like,
    spark,
    warehouse,
)

from repark import ReparkSession
from repark.spark import functions

__all__ = ["spark", "warehouse"]


def _cond(column: str, value: object) -> Any:
    return functions.col(column) == value


def _first_snapshot(spark: ReparkSession) -> str:
    rows = spark.sql(f"SELECT snapshot_id FROM {_T}.snapshots ORDER BY committed_at").collect()
    return str(rows[0][0])


def _validated_overwrite(
    spark: ReparkSession,
    frame: Any,
    level: str | None,
    from_first: bool | None,
    change: str = f"DELETE FROM {_T} WHERE id = 1",
) -> None:
    first = _first_snapshot(spark)
    spark.sql(change)
    rows = spark.sql(f"SELECT snapshot_id FROM {_T}.snapshots ORDER BY committed_at").collect()
    writer = frame.writeTo(_T)
    if from_first is not None:
        start = first if from_first else str(rows[-1][0])
        writer = writer.option("validate-from-snapshot-id", start)
    if level is not None:
        writer = writer.option("isolation-level", level)
    writer.overwrite(_cond("id", 1))


def _overwrite_twice_by_condition(frame: Any) -> None:
    frame.writeTo(_T).overwrite(_cond("cat", "x"))
    frame.writeTo(_T).overwrite(_cond("cat", "x"))


def _snapshot_property_overwrite(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    frame.writeTo(_T).option("snapshot-property.k", "v").overwrite(_cond("cat", "x"))
    rows = spark.sql(f"SELECT summary FROM {_T}.snapshots ORDER BY committed_at").collect()
    out["custom_summary"] = [dict(row[0]).get("k") for row in rows]


def _overwrite_on_branch(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    frame.writeTo(f"{_T}.branch_b1").overwrite(_cond("cat", "x"))
    _branch_rows(spark, out)


def _validated_overwrite_on_branch(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    first = _first_snapshot(spark)
    spark.sql(f"DELETE FROM {_T}.branch_b1 WHERE id = 1")
    writer = frame.writeTo(f"{_T}.branch_b1").option("validate-from-snapshot-id", first)
    writer.option("isolation-level", "serializable").overwrite(_cond("id", 1))
    _branch_rows(spark, out)


def _nested_frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(7, (9, "g")), (8, (1, "h"))], "id BIGINT, s STRUCT<a: INT, b: STRING>"
    )


def _after_a_null_partition_row(spark: ReparkSession, frame: Any, condition: Any) -> None:
    spark.sql(f"INSERT INTO {_T} SELECT 4 AS id, 'd' AS data, CAST(NULL AS STRING) AS cat")
    frame.writeTo(_T).overwrite(condition)


def _extra_and_missing(frame: Any, extra: Any, order: tuple[str, ...] = ()) -> None:
    wide = frame.drop("data").withColumn("extra", extra)
    if order:
        wide = wide.select(*order)
    wide.writeTo(_T).overwrite(_cond("id", 1))


def _struct_frame(spark: ReparkSession, ddl: str, value: tuple[Any, ...]) -> Any:
    return spark.createDataFrame([(7, value)], f"id BIGINT, s STRUCT<{ddl}>")


def _case_sensitive(action: Action) -> Action:
    def act(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
        spark.conf.set("spark.sql.caseSensitive", "true")
        try:
            action(spark, frame, out)
        finally:
            spark.conf.set("spark.sql.caseSensitive", "false")

    return act


_APPEND_ID_1 = f"INSERT INTO {_T} VALUES (1, 'z', 'q')"
_ACCEPT_ANY_SCHEMA = ", 'write.spark.accept-any-schema'='true'"

_OVERWRITE_CONDITION: dict[str, tuple[str, str, str, int, Action]] = {
    "oc_partition": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(_cond("cat", "x")),
    ),
    "oc_rows": ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", 1))),
    "oc_matches_nothing": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", 99)),
    ),
    "oc_or": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", 1) | _cond("id", 2)),
    ),
    "oc_and_partition": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(_cond("cat", "x") & (functions.col("id") > 0)),
    ),
    "oc_expr_condition": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.expr("id = 1")),
    ),
    "oc_true": ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(functions.lit(True))),
    "oc_false": ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(functions.lit(False))),
    "oc_empty_frame": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.limit(0).writeTo(_T).overwrite(_cond("id", 1)),
    ),
    "oc_empty_frame_unsnapshotted": (
        "empty",
        "",
        "",
        2,
        lambda s, f, o: f.limit(0).writeTo(_T).overwrite(_cond("id", 1)),
    ),
    "oc_isin_partition": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.col("cat").isin("x", "y")),
    ),
    "oc_isnull_partition": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.col("cat").isNull()),
    ),
    "oc_reordered_frame": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: f.select("cat", "id", "data").writeTo(_T).overwrite(_cond("cat", "x")),
    ),
    "oc_bucket_aligned": (
        "named",
        "PARTITIONED BY (bucket(2, id))",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.col("id") < 5),
    ),
    "oc_between": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.col("id").between(1, 2)),
    ),
    "oc_not_equal": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.col("id") != 2),
    ),
    "oc_string_literal_on_long": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", "1")),
    ),
    "oc_missing_frame_column": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.drop("data").writeTo(_T).overwrite(_cond("id", 1)),
    ),
    "oc_bool_condition": ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(True)),
    "oc_repeated": ("named", _PARTITIONED, "", 2, lambda s, f, o: _overwrite_twice_by_condition(f)),
    "oc_snapshot_property": ("named", _PARTITIONED, "", 2, _snapshot_property_overwrite),
    "oc_isin_rows": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(functions.col("id").isin(1, 2)),
    ),
    "oc_data_string_rows": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).overwrite(_cond("data", "a")),
    ),
    "oc_isnull_with_null_row": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: _after_a_null_partition_row(s, f, functions.col("cat").isNull()),
    ),
    "oc_not_partition_with_null_row": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: _after_a_null_partition_row(s, f, ~_cond("cat", "x")),
    ),
    "oc_not_in_with_null_row": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: _after_a_null_partition_row(s, f, ~functions.col("cat").isin("x")),
    ),
    "oc_frame_upper_case": (
        "named",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: (
            f.select(functions.col("id").alias("ID"), functions.col("data").alias("DATA"), "cat")
            .writeTo(_T)
            .overwrite(_cond("cat", "x"))
        ),
    ),
    "oc_cond_on_dropped_frame_column": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: f.drop("data").writeTo(_T).overwrite(_cond("data", "a")),
    ),
    "oc_branch_target": ("named", _PARTITIONED, "", 2, _overwrite_on_branch),
}

_VALIDATE_FROM: dict[str, tuple[str, str, str, int, Action]] = {
    "vf_from_s0_no_level": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: _validated_overwrite(s, f, None, True),
    ),
    "vf_serializable_from_latest": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: _validated_overwrite(s, f, "serializable", False),
    ),
    "vf_snapshot_append_conflict": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: _validated_overwrite(s, f, "snapshot", True, _APPEND_ID_1),
    ),
    "vf_serializable_other_rows": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: _validated_overwrite(
            s, f, "serializable", True, f"DELETE FROM {_T} WHERE id = 2"
        ),
    ),
    "vf_bad_snapshot_id": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: (
            f.writeTo(_T)
            .option("validate-from-snapshot-id", "abc")
            .option("isolation-level", "serializable")
            .overwrite(_cond("id", 1))
        ),
    ),
    "vf_bad_id_no_level": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: (
            f.writeTo(_T).option("validate-from-snapshot-id", "abc").overwrite(_cond("id", 1))
        ),
    ),
    "vf_unknown_id_no_level": (
        "named",
        "",
        "",
        2,
        lambda s, f, o: (
            f.writeTo(_T).option("validate-from-snapshot-id", "12345").overwrite(_cond("id", 1))
        ),
    ),
    "vf_empty_table_validate": (
        "empty",
        "",
        "",
        2,
        lambda s, f, o: (
            f.writeTo(_T)
            .option("validate-from-snapshot-id", "12345")
            .option("isolation-level", "serializable")
            .overwrite(_cond("id", 1))
        ),
    ),
}


def _planning(spark_error: dict[str, Any]) -> dict[str, Any]:
    return spark_error | {"message": "Error during planning: " + spark_error["message"]}


def _data_invalid(spark_error: dict[str, Any]) -> dict[str, Any]:
    message = spark_error["message"].replace('ref(name="id") == 1', "id = 1")
    return {
        "type": "PySparkException",
        "condition": None,
        "sqlstate": None,
        "message": "DataInvalid => " + message.replace("[<file>]", "<file>"),
    }


def _suggestions_in_table_order(spark_error: dict[str, Any]) -> dict[str, Any]:
    reordered = spark_error["message"].replace("[`id`, `cat`, `data`]", "[`id`, `data`, `cat`]")
    return _planning(spark_error | {"message": reordered})


def _analysis_class(spark_error: dict[str, Any]) -> dict[str, Any]:
    return _planning(spark_error | {"type": "AnalysisException"})


def _facade_spelling(spark_error: dict[str, Any]) -> dict[str, Any]:
    return spark_error | {
        "message": spark_error["message"].replace("UPPER(cat) = 'X'", "upper(`cat`) = 'X'")
    }


def _untranslatable(spelling: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def rule(spark_error: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "IllegalArgumentException",
            "condition": None,
            "sqlstate": None,
            "message": f"Cannot convert Spark predicate to Iceberg expression: {spelling}",
        }

    return rule


def _writer_not_found(spark_error: dict[str, Any]) -> dict[str, Any]:
    return spark_error | {
        "message": "[TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'sc.u7.t' because it does "
        "not exist. Use create() or createOrReplace() first. SQLSTATE: 42P01"
    }


_CONDITION_RESIDUES: dict[
    str, tuple[tuple[str, str, str, int, Action], Callable[[dict[str, Any]], dict[str, Any]]]
] = {
    "oc_non_partition_column": (
        ("named", _PARTITIONED, "", 2, lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", 1))),
        _data_invalid,
    ),
    "oc_partial_file": (
        ("values", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", 1))),
        _data_invalid,
    ),
    "oc_string_condition": (
        ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite("id = 1")),
        _planning,
    ),
    "oc_unknown_column": (
        ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(_cond("zz", 1))),
        _suggestions_in_table_order,
    ),
    "oc_upper_column": (
        ("named", _PARTITIONED, "", 2, lambda s, f, o: f.writeTo(_T).overwrite(_cond("CAT", "x"))),
        _analysis_class,
    ),
    "oc_function": (
        (
            "named",
            _PARTITIONED,
            "",
            2,
            lambda s, f, o: f.writeTo(_T).overwrite(functions.upper(functions.col("cat")) == "X"),
        ),
        _facade_spelling,
    ),
    "oc_extra_column": (
        (
            "named",
            _PARTITIONED,
            "",
            2,
            lambda s, f, o: (
                f.withColumn("extra", f.id * 2).writeTo(_T).overwrite(_cond("cat", "x"))
            ),
        ),
        _planning,
    ),
    "oc_nested_missing_subfield": (
        (
            "nested",
            "",
            "",
            2,
            lambda s, f, o: _struct_frame(s, "a: INT", (9,)).writeTo(_T).overwrite(_cond("id", 1)),
        ),
        _planning,
    ),
    "oc_nested_extra_subfield": (
        (
            "nested",
            "",
            "",
            2,
            lambda s, f, o: (
                _struct_frame(s, "a: INT, b: STRING, c: INT", (9, "g", 5))
                .writeTo(_T)
                .overwrite(_cond("id", 1))
            ),
        ),
        _planning,
    ),
    "oc_nested_missing_and_extra": (
        (
            "nested",
            "",
            "",
            2,
            lambda s, f, o: (
                _struct_frame(s, "a: INT, c: INT", (9, 5)).writeTo(_T).overwrite(_cond("id", 1))
            ),
        ),
        _planning,
    ),
    "oc_nested_missing_reordered": (
        (
            "nested",
            "",
            "",
            2,
            lambda s, f, o: (
                _struct_frame(s, "a: INT", (9,))
                .select("s", "id")
                .writeTo(_T)
                .overwrite(_cond("id", 1))
            ),
        ),
        _planning,
    ),
    "oc_nested_upper_subfield_case_sensitive": (
        (
            "nested",
            "",
            "",
            2,
            _case_sensitive(
                lambda s, f, o: (
                    _struct_frame(s, "A: INT, b: STRING", (9, "g"))
                    .writeTo(_T)
                    .overwrite(_cond("id", 1))
                )
            ),
        ),
        _planning,
    ),
    "vf_serializable_from_s0": (
        ("named", "", "", 2, lambda s, f, o: _validated_overwrite(s, f, "serializable", True)),
        _data_invalid,
    ),
    "vf_snapshot_from_s0": (
        ("named", "", "", 2, lambda s, f, o: _validated_overwrite(s, f, "snapshot", True)),
        _data_invalid,
    ),
    "vf_serializable_append_conflict": (
        (
            "named",
            "",
            "",
            2,
            lambda s, f, o: _validated_overwrite(s, f, "serializable", True, _APPEND_ID_1),
        ),
        _data_invalid,
    ),
    "vf_bad_level": (
        (
            "named",
            "",
            "",
            2,
            lambda s, f, o: (
                f.writeTo(_T).option("isolation-level", "bogus").overwrite(_cond("id", 1))
            ),
        ),
        _analysis_class,
    ),
    "vf_bad_id_level_none": (
        (
            "named",
            "",
            "",
            2,
            lambda s, f, o: (
                f.writeTo(_T)
                .option("validate-from-snapshot-id", "abc")
                .option("isolation-level", "none")
                .overwrite(_cond("id", 1))
            ),
        ),
        _analysis_class,
    ),
    "vf_on_branch_target": (("named", "", "", 2, _validated_overwrite_on_branch), _data_invalid),
    "oc_extra_and_missing_bigint": (
        ("named", "", "", 2, lambda s, f, o: _extra_and_missing(f, f.id * 2)),
        _planning,
    ),
    "oc_extra_and_missing_string": (
        ("named", "", "", 2, lambda s, f, o: _extra_and_missing(f, functions.lit("e"))),
        _planning,
    ),
    "oc_extra_and_missing_reordered": (
        (
            "named",
            "",
            "",
            2,
            lambda s, f, o: _extra_and_missing(f, functions.lit("e"), ("extra", "cat", "id")),
        ),
        _planning,
    ),
    "oc_lit_null": (
        ("named", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(functions.lit(None))),
        _untranslatable("null"),
    ),
    "oc_nested_field": (
        (
            "nested",
            "",
            "",
            2,
            lambda s, f, o: _nested_frame(s).writeTo(_T).overwrite(_cond("s.a", 1)),
        ),
        _untranslatable("`s`.`a` = 1"),
    ),
}


def test_overwrite_condition_replaces_like_the_recorded_cells(spark: ReparkSession) -> None:
    """``writeTo.overwrite(cat = 'x')`` and ``overwrite(id = 1)`` answer the recorded cells.

    Cells W-DF-V2-OVERWRITE-COND-PART (identity partition ``cat``) and
    W-DF-V2-OVERWRITE-COND-ROWS (unpartitioned, one data file per seed row).

    pins: u7-write-df-2/C-006
    """
    for cell, part, condition in (
        ("W-DF-V2-OVERWRITE-COND-PART", _PARTITIONED, _cond("cat", "x")),
        ("W-DF-V2-OVERWRITE-COND-ROWS", "", _cond("id", 1)),
    ):
        spark.sql(f"DROP TABLE IF EXISTS {_T}")
        _seed(spark, "named", part, "", 2)
        _frame(spark).writeTo(_T).overwrite(condition)
        recorded = _RECORDED[cell]
        rows = spark.sql(f"SELECT * FROM {_T} ORDER BY id").collect()
        assert [list(row) for row in rows] == recorded["data"], cell
        expected = _recorded_summaries(cell)
        assert _summaries_like(spark, expected) == expected, cell
        assert _refs_like_the_record(spark) == recorded["md.refs"], cell


@pytest.mark.parametrize("name", list(_OVERWRITE_CONDITION))
def test_overwrite_condition_shapes_match_spark(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """Files whose rows all match are replaced and the frame appended in one snapshot.

    A ``false`` condition appends, an empty frame deletes, a column the frame lacks is NULL,
    the frame writes by name (reordered, narrower, upper-cased, or missing the column the
    condition names), a non-Column condition refuses ``NOT_COLUMN_OR_STR``, ``IN`` and a
    string equality on rows, ``IS NULL``, ``NOT`` and ``NOT IN`` over a NULL partition key,
    and a ``t.branch_b1`` target commits to the branch.

    pins: u7-write-df-2/C-007, C-013, C-015
    """
    assert _run(spark, warehouse, _OVERWRITE_CONDITION[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", [key for key in _CONDITION_RESIDUES if key.startswith("oc_")])
def test_overwrite_condition_residues(spark: ReparkSession, warehouse: Path, name: str) -> None:
    """Residues R-2 to R-5, R-13 and R-14: RePark's class or text differs by a stated rule.

    The table is left as Spark leaves it. A frame with an extra column and a missing one
    refuses ``EXTRA_COLUMNS`` with Spark's condition, SQLSTATE and text (R-4 prefix), whether
    the extra is a bigint, a string or the frame is reordered; a frame struct missing a
    sub-field refuses ``CANNOT_FIND_DATA`` naming `s`.`b` (also reordered, beside an extra
    sub-field, or spelled ``A`` under ``caseSensitive=true``) and one with an extra sub-field
    refuses ``EXTRA_STRUCT_FIELDS``, in Spark's text; a ``lit(None)`` or a struct-field
    condition refuses where Spark refuses, in the engine's untranslatable-predicate class.

    pins: u7-write-df-2/C-008, C-013, C-015, C-016
    """
    shape, rule = _CONDITION_RESIDUES[name]
    expected = _sorted_rows(_MEASURED[name])
    expected["error"] = rule(expected["error"])
    assert _run(spark, warehouse, shape) == expected


@pytest.mark.parametrize("name", list(_VALIDATE_FROM))
def test_validate_from_snapshot_shapes_match_spark(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """``validate-from-snapshot-id`` counts only beside an ``isolation-level`` option.

    ``snapshot`` isolation ignores a matching append, a change to other rows never conflicts,
    a start that is not a Java long refuses ``NumberFormatException`` with or without a level,
    an unknown start without a level is ignored, and a table with no snapshot validates.

    pins: u7-write-df-2/C-009, C-015
    """
    assert _run(spark, warehouse, _VALIDATE_FROM[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", [key for key in _CONDITION_RESIDUES if key.startswith("vf_")])
def test_validate_from_snapshot_residues(spark: ReparkSession, warehouse: Path, name: str) -> None:
    """A conflict after the requested snapshot refuses as on Spark, in RePark's class (R-2).

    On a branch target too; ``isolation-level=none`` refuses ``Invalid isolation level: none``
    in RePark's class (R-4).

    pins: u7-write-df-2/C-009, C-014
    """
    shape, rule = _CONDITION_RESIDUES[name]
    expected = _sorted_rows(_MEASURED[name])
    expected["error"] = rule(expected["error"])
    assert _run(spark, warehouse, shape) == expected


def test_an_unknown_validation_start_refuses_with_java_text(
    spark: ReparkSession, warehouse: Path
) -> None:
    """An id outside the history names the oldest ancestor, as Java's validation does (R-2).

    pins: u7-write-df-2/C-009
    """
    shape = (
        "named",
        "",
        "",
        2,
        lambda s, f, o: (
            f.writeTo(_T)
            .option("validate-from-snapshot-id", "12345")
            .option("isolation-level", "serializable")
            .overwrite(_cond("id", 1))
        ),
    )
    observed = _run(spark, warehouse, shape)
    expected = _sorted_rows(_MEASURED["vf_unknown_snapshot_id"])
    spark_message = expected["error"]["message"]
    oldest = _first_snapshot(spark)
    assert re.fullmatch(
        r"Cannot determine history between starting snapshot 12345 and the last known "
        r"ancestor -?\d+",
        spark_message,
    )
    expected["error"] = _data_invalid(expected["error"]) | {
        "message": "DataInvalid => " + re.sub(r"-?\d+$", oldest, spark_message)
    }
    assert observed == expected


def test_overwrite_condition_divergences_where_one_engine_answers(
    spark: ReparkSession, warehouse: Path
) -> None:
    """Residues R-3, R-6, R-7 and R-8, each beside Spark's recorded answer.

    pins: u7-write-df-2/C-010
    """
    merged = _run(
        spark,
        warehouse,
        (
            "named",
            "",
            _ACCEPT_ANY_SCHEMA,
            2,
            lambda s, f, o: (
                f.withColumn("extra", f.id * 2)
                .writeTo(_T)
                .option("mergeSchema", "true")
                .overwrite(_cond("id", 1))
            ),
        ),
    )
    assert _MEASURED["oc_merge_schema"]["schema"][-1] == ["extra", "bigint"]
    assert merged["error"]["condition"] == "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS"
    assert merged["rows"] == [[1, "a", "x"], [2, "b", "y"], [3, "c", "x"]]
    spark.sql(f"DROP TABLE {_T}")
    whole_history = _run(
        spark,
        warehouse,
        ("named", "", "", 2, lambda s, f, o: _validated_overwrite(s, f, "serializable", None)),
    )
    assert _MEASURED["vf_serializable_no_from"]["error"]["type"] == "ValidationException"
    assert whole_history == _sorted_rows(_MEASURED["vf_serializable_from_latest"])
    spark.sql(f"DROP TABLE {_T}")
    frame_bound = _run(
        spark,
        warehouse,
        ("named", _PARTITIONED, "", 2, lambda s, f, o: f.writeTo(_T).overwrite(f["cat"] == "x")),
    )
    assert _MEASURED["oc_frame_join_column"]["error"]["condition"] == (
        "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION"
    )
    assert frame_bound == _sorted_rows(_MEASURED["oc_partition"])
    spark.sql(f"DROP TABLE {_T}")
    missing = _run(
        spark,
        warehouse,
        ("none", "", "", 2, lambda s, f, o: f.writeTo(_T).overwrite(_cond("id", 1))),
    )
    assert missing == {"error": _writer_not_found(_MEASURED["oc_missing_table"]["error"])}


def test_overwrite_condition_divergences_on_the_row_filter(
    spark: ReparkSession, warehouse: Path
) -> None:
    """Residues R-11 and R-12, each beside Spark's recorded answer.

    A fractional literal on a long column (``between(0.5, 1.5)``, ``id <= 1.5``) commits the
    ``id = 1`` replacement where Spark refuses the predicate; ``startswith`` on the partition
    column refuses where Spark commits, and the table stays as seeded.

    pins: u7-write-df-2/C-015
    """
    for name, condition in (
        ("oc_between_fractional", functions.col("id").between(0.5, 1.5)),
        ("oc_le_fractional", functions.col("id") <= 1.5),
    ):
        spark.sql(f"DROP TABLE IF EXISTS {_T}")
        observed = _run(
            spark,
            warehouse,
            ("named", "", "", 2, lambda s, f, o, c=condition: f.writeTo(_T).overwrite(c)),
        )
        assert _MEASURED[name]["error"]["type"] == "IllegalArgumentException", name
        assert _MEASURED[name]["error"]["message"].startswith(
            "Cannot convert Spark predicate to Iceberg expression: CAST(id AS double)"
        ), name
        assert observed == _sorted_rows(_MEASURED["oc_rows"]), name
    spark.sql(f"DROP TABLE {_T}")
    observed = _run(
        spark,
        warehouse,
        (
            "named",
            _PARTITIONED,
            "",
            2,
            lambda s, f, o: f.writeTo(_T).overwrite(functions.col("cat").startswith("x")),
        ),
    )
    assert _MEASURED["oc_startswith_partition"]["ok"] is True
    assert _MEASURED["oc_startswith_partition"]["rows"] == _MEASURED["oc_partition"]["rows"]
    expected = _sorted_rows(_MEASURED["oc_upper_column"])
    expected["error"] = _untranslatable("starts_with(`cat`, 'x')")(expected["error"])
    assert observed == expected

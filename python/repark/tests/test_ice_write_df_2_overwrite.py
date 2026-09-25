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
    the frame writes by name, and a non-Column condition refuses ``NOT_COLUMN_OR_STR``.

    pins: u7-write-df-2/C-007
    """
    assert _run(spark, warehouse, _OVERWRITE_CONDITION[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", [key for key in _CONDITION_RESIDUES if key.startswith("oc_")])
def test_overwrite_condition_residues(spark: ReparkSession, warehouse: Path, name: str) -> None:
    """Residues R-2 to R-5: RePark's class or text differs from Spark's by a stated rule.

    The table is left as Spark leaves it.

    pins: u7-write-df-2/C-008
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
    and a start that is not a Java long refuses ``NumberFormatException``.

    pins: u7-write-df-2/C-009
    """
    assert _run(spark, warehouse, _VALIDATE_FROM[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", [key for key in _CONDITION_RESIDUES if key.startswith("vf_")])
def test_validate_from_snapshot_residues(spark: ReparkSession, warehouse: Path, name: str) -> None:
    """A conflict after the requested snapshot refuses as on Spark, in RePark's class (R-2).

    pins: u7-write-df-2/C-009
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

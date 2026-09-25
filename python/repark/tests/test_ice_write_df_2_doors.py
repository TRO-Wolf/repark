"""U7 PR2 slice-2 round 3 — `isolation-level` door by door, and nested binding residues.

Every expected value is Spark 4.1.2 + Iceberg 1.11.0's answer from the ``measured`` shapes of
the committed ``ice_write_df_1_spark_oracle.json`` (``iso_*``, ``append_nested_*``,
``op_nested_*``, ``oc_nested_upper_subfield``, ``oc_string_into_bigint``,
``oc_null_into_not_null``), read through the helpers of ``test_ice_write_df_2.py``. A residue
pin states RePark's answer beside Spark's recorded one.

pins: u7-write-df-2/C-013, C-014, C-016
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from test_ice_write_df_2 import (
    _MEASURED,
    _PARTITIONED,
    _T,
    Action,
    _run,
    _sorted_rows,
    spark,
    warehouse,
)
from test_ice_write_df_2_overwrite import _analysis_class, _cond, _struct_frame

from repark import ReparkSession
from repark.spark import functions

__all__ = ["spark", "warehouse"]


def _isolation(level: str, door: str) -> Action:
    def act(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
        v1 = frame.write.format("iceberg").option("isolation-level", level)
        v2 = frame.writeTo(_T).option("isolation-level", level)
        if door == "append":
            v2.append()
        elif door == "v1_append":
            v1.mode("append").saveAsTable(_T)
        elif door == "insert_into_append":
            frame.write.option("isolation-level", level).insertInto(_T)
        elif door == "overwrite_partitions":
            v2.overwritePartitions()
        elif door == "sat_overwrite":
            v1.mode("overwrite").saveAsTable(_T)
        elif door == "insert_into_overwrite":
            frame.write.option("isolation-level", level).insertInto(_T, overwrite=True)
        elif door == "sat_ctas":
            v1.saveAsTable(_T)
        else:
            writer = frame.writeTo(_T).using("iceberg").option("isolation-level", level)
            getattr(writer, door)()

    return act


_ISOLATION_IGNORED: dict[str, tuple[str, str, str, int, Action]] = {
    "iso_append_none": ("named", "", "", 2, _isolation("none", "append")),
    "iso_append_bogus": ("named", "", "", 2, _isolation("bogus", "append")),
    "iso_v1_append_none": ("named", "", "", 2, _isolation("none", "v1_append")),
    "iso_v1_append_bogus": ("named", "", "", 2, _isolation("bogus", "v1_append")),
    "iso_insert_into_append_bogus": ("named", "", "", 2, _isolation("bogus", "insert_into_append")),
    "iso_sat_ctas_bogus": ("none", "", "", 2, _isolation("bogus", "sat_ctas")),
    "iso_v2_create_none": ("none", "", "", 2, _isolation("none", "create")),
}

_ISOLATION_REFUSED: dict[str, tuple[str, str, str, int, Action]] = {
    "iso_overwrite_partitions_none": (
        "named",
        _PARTITIONED,
        "",
        2,
        _isolation("none", "overwrite_partitions"),
    ),
    "iso_sat_overwrite_none": ("named", "", "", 2, _isolation("none", "sat_overwrite")),
    "iso_insert_into_overwrite_none": (
        "named",
        "",
        "",
        2,
        _isolation("none", "insert_into_overwrite"),
    ),
    "iso_v2_replace_bogus": ("named", "", "", 2, _isolation("bogus", "replace")),
    "iso_v2_create_or_replace_none": ("none", "", "", 2, _isolation("none", "createOrReplace")),
}


@pytest.mark.parametrize("name", list(_ISOLATION_IGNORED))
def test_isolation_level_is_ignored_where_spark_ignores_it(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """An append or a plain create commits whatever ``isolation-level`` says, ``none`` too.

    Spark parses the option only on the overwrite and replace doors: ``writeTo.append``,
    ``saveAsTable`` append and create, ``insertInto`` append and ``writeTo.create`` commit with
    ``none`` or ``bogus``.

    pins: u7-write-df-2/C-014, C-016
    """
    assert _run(spark, warehouse, _ISOLATION_IGNORED[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", list(_ISOLATION_REFUSED))
def test_isolation_level_refuses_on_the_overwrite_and_replace_doors(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """``overwritePartitions``, ``saveAsTable`` and ``insertInto`` overwrite, ``replace`` and
    ``createOrReplace`` refuse ``Invalid isolation level: <value>`` with Spark's text in
    RePark's class (R-4), and write nothing; ``createOrReplace`` on no table creates none.

    pins: u7-write-df-2/C-014, C-016
    """
    observed = _run(spark, warehouse, _ISOLATION_REFUSED[name])
    measured = _MEASURED[name]
    if "state_error" in measured:
        assert "TABLE_OR_VIEW_NOT_FOUND" in measured["state_error"]
        assert observed == {"error": _analysis_class(measured["error"])}
        return
    expected = _sorted_rows(measured)
    expected["error"] = _analysis_class(expected["error"])
    assert observed == expected


def _nested(action: Action) -> tuple[str, str, str, int, Action]:
    return ("nested", "", "", 2, action)


def test_nested_binding_residues_beside_spark(spark: ReparkSession, warehouse: Path) -> None:
    """Residues R-15 and R-16, each beside Spark's recorded answer.

    R-15: the append doors do not run the by-name sub-field resolver, so a struct missing
    ``b`` or carrying an extra ``c`` commits where Spark refuses, and
    ``overwritePartitions`` refuses the missing ``b`` in the engine's cast text. R-16: a
    case-only sub-field spelling (``A`` for ``a``) under the default case-insensitive setting
    writes ``a`` NULL through ``overwrite(condition)`` where Spark writes 9.

    pins: u7-write-df-2/C-013, C-016
    """
    missing = _run(
        spark,
        warehouse,
        _nested(lambda s, f, o: _struct_frame(s, "a: INT", (9,)).writeTo(_T).append()),
    )
    assert _MEASURED["append_nested_missing_subfield"]["error"]["condition"] == (
        "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA"
    )
    assert missing["rows"] == [[1, [1, "x"]], [2, [2, "y"]], [7, [9, None]]]
    spark.sql(f"DROP TABLE {_T}")
    extra = _run(
        spark,
        warehouse,
        _nested(
            lambda s, f, o: (
                _struct_frame(s, "a: INT, b: STRING, c: INT", (9, "g", 5)).writeTo(_T).append()
            )
        ),
    )
    assert _MEASURED["append_nested_extra_subfield"]["error"]["condition"] == (
        "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS"
    )
    assert extra["rows"] == [[1, [1, "x"]], [2, [2, "y"]], [7, [9, "g"]]]
    spark.sql(f"DROP TABLE {_T}")
    partitions = _run(
        spark,
        warehouse,
        _nested(lambda s, f, o: _struct_frame(s, "a: INT", (9,)).writeTo(_T).overwritePartitions()),
    )
    spark_partitions = _MEASURED["op_nested_missing_subfield"]
    assert spark_partitions["error"]["condition"] == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA"
    assert partitions["rows"] == spark_partitions["rows"]
    assert partitions["error"]["type"] == "PySparkException"
    assert partitions["error"]["message"].startswith(
        "datafusion engine error: Execution error: INSERT OVERWRITE cast of list column `s` to `s`"
    )
    assert partitions["error"]["message"].endswith(
        "Incorrect number of arrays for StructArray fields, expected 2 got 1"
    )
    spark.sql(f"DROP TABLE {_T}")
    folded = _run(
        spark,
        warehouse,
        _nested(
            lambda s, f, o: (
                _struct_frame(s, "A: INT, b: STRING", (9, "g"))
                .writeTo(_T)
                .overwrite(functions.col("id") >= 1)
            )
        ),
    )
    spark_folded = _MEASURED["oc_nested_upper_subfield"]
    assert spark_folded["rows"] == [[7, [9, "g"]]]
    assert folded["operations"] == spark_folded["operations"]
    assert folded["rows"] == [[7, [None, "g"]]]


def test_store_assignment_residues_beside_spark(spark: ReparkSession, warehouse: Path) -> None:
    """Residues R-17 and R-18: both engines refuse and leave the table, in different texts.

    R-17: a STRING ``id`` into BIGINT refuses in RePark's store-assignment text where Spark
    raises ``INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST``. R-18: a NULL into the NOT NULL
    ``id`` refuses in the Arrow schema text where Spark raises ``NOT_NULL_ASSERT_VIOLATION``.

    pins: u7-write-df-2/C-016
    """
    cast = _run(
        spark,
        warehouse,
        (
            "named",
            "",
            "",
            2,
            lambda s, f, o: (
                f.select(functions.col("id").cast("string").alias("id"), "data", "cat")
                .writeTo(_T)
                .overwrite(_cond("id", 1))
            ),
        ),
    )
    spark_cast = _MEASURED["oc_string_into_bigint"]
    assert spark_cast["error"]["message"] == (
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the "
        'table `sc`.`u7`.`t`: Cannot safely cast `id` "STRING" to "BIGINT". SQLSTATE: KD000'
    )
    assert cast["rows"] == spark_cast["rows"]
    assert cast["operations"] == spark_cast["operations"]
    assert cast["error"] == {
        "type": "AnalysisException",
        "condition": None,
        "sqlstate": None,
        "message": "repark_insert_store_assignment\ncaused by\nError during planning: INSERT INTO "
        "cannot store-assign column `id`: source type Utf8 is not ANSI-store-assignable to "
        "target type Int64 (Spark INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST; add an explicit "
        "CAST only if the reinterpretation is intended semantics)",
    }
    spark.sql(f"DROP TABLE {_T}")
    null = _run(
        spark,
        warehouse,
        (
            "notnull",
            "",
            "",
            2,
            lambda s, f, o: (
                f.select("cat", "data", functions.lit(None).cast("bigint").alias("id"))
                .writeTo(_T)
                .overwrite(_cond("cat", "x"))
            ),
        ),
    )
    spark_null = _MEASURED["oc_null_into_not_null"]
    assert spark_null["error"]["condition"] == "NOT_NULL_ASSERT_VIOLATION"
    assert null["rows"] == spark_null["rows"]
    assert null["operations"] == spark_null["operations"]
    assert null["error"] == {
        "type": "PySparkException",
        "condition": None,
        "sqlstate": None,
        "message": "Unexpected => Arrow Schema Error, source: Invalid argument error: Column 'id' "
        "is declared as non-nullable but contains null values",
    }

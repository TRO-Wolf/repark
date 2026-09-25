"""U7 PR2 — DataFrame writer semantics: saveAsTable overwrite replaces, the branch option.

Every expected value is Spark 4.1.2 + Iceberg 1.11.0's answer, read from the committed
``ice_write_df_1_spark_oracle.json``: the scoreboard cells ``W-DF-SAVEASTABLE-OVERWRITE`` and
``W-DF-V2-OPTION-BRANCH`` under ``recorded``, and the PR2 shapes under ``measured``. Seed
``values`` is one ``INSERT … VALUES`` (one data file); seed ``named`` is an
``INSERT … UNION ALL`` (three data files on both engines); seed ``none`` creates no table.
Snapshot references are indexes in commit order.

pins: u7-write-df-2/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_write_df_1_spark_oracle.json").read_text(encoding="utf-8")
)
_RECORDED: dict[str, Any] = _ORACLE["recorded"]
_MEASURED: dict[str, Any] = _ORACLE["measured"]
_T = "sc.u7.t"
_SUMMARY_KEYS = (
    "added-records",
    "deleted-records",
    "total-records",
    "added-data-files",
    "deleted-data-files",
)
_PROPERTY_KEYS = (
    "k1",
    "custom.k",
    "write.format.default",
    "write.distribution-mode",
    "write.spark.accept-any-schema",
)
_PARTITIONED = "PARTITIONED BY (cat)"

Action = Callable[[ReparkSession, Any, dict[str, Any]], object]


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    """The memory catalog's warehouse directory."""
    return tmp_path / "wh"


@pytest.fixture
def spark(warehouse: Path) -> ReparkSession:
    """A session with the ``sc`` memory catalog, the ``u7`` namespace and v3 creates on."""
    session = (
        ReparkSession.builder.appName("pytest-ice-write-df-2")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE sc.u7")
    return session


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame(
        [(7, "g", "x"), (8, "h", "w")], "id BIGINT, data STRING, cat STRING"
    )


def _seed(spark: ReparkSession, seed: str, part: str, props: str, version: int) -> None:
    if seed == "none":
        return
    spark.sql(
        f"CREATE TABLE {_T} (id BIGINT, data STRING, cat STRING) USING iceberg {part} "
        f"TBLPROPERTIES ('format-version'='{version}'{props})"
    )
    if seed == "values":
        spark.sql(f"INSERT INTO {_T} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    elif seed == "named":
        spark.sql(
            f"INSERT INTO {_T} SELECT 1 AS id, 'a' AS data, 'x' AS cat "
            "UNION ALL SELECT 2, 'b', 'y' UNION ALL SELECT 3, 'c', 'x'"
        )


def _metadata(warehouse: Path) -> dict[str, Any]:
    files = sorted(
        (warehouse / "u7" / "t" / "metadata").glob("*.metadata.json"),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )
    return json.loads(files[-1].read_text(encoding="utf-8"))


def _state(spark: ReparkSession, warehouse: Path, uuid_before: str | None) -> dict[str, Any]:
    state: dict[str, Any] = {
        "rows": [list(row) for row in spark.sql(f"SELECT * FROM {_T} ORDER BY id").collect()]
    }
    snapshots = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {_T}.snapshots ORDER BY committed_at"
    ).collect()
    index = {row[0]: position for position, row in enumerate(snapshots)}
    state["operations"] = [row[1] for row in snapshots]
    state["summaries"] = [
        {key: value for key, value in dict(row[2]).items() if key in _SUMMARY_KEYS}
        for row in snapshots
    ]
    specs = f"SELECT spec_id, count(*) FROM {_T}.files GROUP BY spec_id ORDER BY spec_id"
    state["specs"] = [list(row) for row in spark.sql(specs).collect()]
    described = spark.sql(f"DESCRIBE TABLE {_T}").collect()
    state["partitioning"] = [str(row[1]) for row in described if str(row[0]).startswith("Part ")]
    state["schema"] = [
        [field.name, field.dataType.simpleString()] for field in spark.table(_T).schema.fields
    ]
    refs = spark.sql(f"SELECT name, type, snapshot_id FROM {_T}.refs ORDER BY name").collect()
    state["refs"] = [[row[0], row[1], index.get(row[2])] for row in refs]
    history = spark.sql(
        f"SELECT snapshot_id, parent_id, is_current_ancestor FROM {_T}.history "
        "ORDER BY made_current_at"
    ).collect()
    state["history"] = [[index.get(row[0]), index.get(row[1]), bool(row[2])] for row in history]
    metadata = _metadata(warehouse)
    properties = metadata.get("properties", {})
    state["properties"] = {key: properties[key] for key in _PROPERTY_KEYS if key in properties}
    state["format_version"] = metadata.get("format-version")
    orders = [
        order
        for order in metadata.get("sort-orders", [])
        if order.get("order-id") == metadata.get("default-sort-order-id")
    ]
    state["sort_fields"] = len(orders[0]["fields"]) if orders else None
    if uuid_before is not None:
        state["uuid_same"] = metadata["table-uuid"] == uuid_before
    return state


def _branch_rows(spark: ReparkSession, out: dict[str, Any], name: str = "b1") -> None:
    query = f"SELECT * FROM {_T}.branch_{name} ORDER BY id"
    out["branch_rows"] = [list(row) for row in spark.sql(query).collect()]


def _iceberg(frame: Any) -> Any:
    return frame.write.format("iceberg")


def _run(
    spark: ReparkSession,
    warehouse: Path,
    shape: tuple[str, str, str, int, Action],
) -> dict[str, Any]:
    seed, part, props, version, action = shape
    _seed(spark, seed, part, props, version)
    uuid_before = None if seed == "none" else _metadata(warehouse)["table-uuid"]
    out: dict[str, Any] = {}
    action(spark, _frame(spark), out)
    out["ok"] = True
    out.update(_state(spark, warehouse, uuid_before))
    return out


def _dynamic_session_overwrite(spark: ReparkSession, frame: Any) -> None:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        _iceberg(frame).mode("overwrite").saveAsTable(_T)
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")


def _overwrite_twice(frame: Any) -> None:
    _iceberg(frame).mode("overwrite").saveAsTable(_T)
    _iceberg(frame).mode("overwrite").saveAsTable(_T)


def _create_branch(spark: ReparkSession) -> None:
    spark.sql(f"ALTER TABLE {_T} CREATE BRANCH b1")


def _overwrite_with_branch(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    _iceberg(frame).mode("overwrite").saveAsTable(_T)
    _branch_rows(spark, out)


def _v2_branch_append(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    frame.writeTo(_T).option("branch", "b1").append()
    _branch_rows(spark, out)


def _v1_branch_append(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    _iceberg(frame).option("branch", "b1").mode("append").saveAsTable(_T)
    _branch_rows(spark, out)


def _v2_branch_overwrite_partitions(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    frame.writeTo(_T).option("branch", "b1").overwritePartitions()
    _branch_rows(spark, out)


def _v2_tag_append(spark: ReparkSession, frame: Any) -> None:
    spark.sql(f"ALTER TABLE {_T} CREATE TAG t1")
    frame.writeTo(_T).option("tag", "t1").append()


def _v2_options_branch_upper(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    frame.writeTo(_T).options(BRANCH="b1").append()
    _branch_rows(spark, out)


def _replace_overwrite(frame: Any) -> None:
    _iceberg(frame).mode("overwrite").saveAsTable(_T)


def _sorted_overwrite(spark: ReparkSession, frame: Any) -> None:
    spark.sql(f"ALTER TABLE {_T} WRITE ORDERED BY id")
    _replace_overwrite(frame)


def _self_source_overwrite(spark: ReparkSession) -> None:
    _iceberg(spark.table(_T).filter("id > 1")).mode("overwrite").saveAsTable(_T)


_SAVE_AS_TABLE: dict[str, tuple[str, str, str, int, Action]] = {
    "sat_overwrite": ("values", "", "", 2, lambda s, f, o: _replace_overwrite(f)),
    "sat_overwrite_partitioned": (
        "values",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: _replace_overwrite(f),
    ),
    "sat_overwrite_partby": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _iceberg(f).partitionBy("cat").mode("overwrite").saveAsTable(_T),
    ),
    "sat_overwrite_props": ("values", "", ", 'k1'='v1'", 2, lambda s, f, o: _replace_overwrite(f)),
    "sat_overwrite_option": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _iceberg(f).option("custom.k", "v").mode("overwrite").saveAsTable(_T),
    ),
    "sat_overwrite_schema": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _replace_overwrite(f.selectExpr("id", "CAST(id AS INT) AS extra")),
    ),
    "sat_overwrite_missing": ("none", "", "", 2, lambda s, f, o: _replace_overwrite(f)),
    "sat_overwrite_branch_ref": ("values", "", "", 2, _overwrite_with_branch),
    "sat_overwrite_sorted": ("values", "", "", 2, lambda s, f, o: _sorted_overwrite(s, f)),
    "sat_overwrite_empty": ("values", "", "", 2, lambda s, f, o: _replace_overwrite(f.limit(0))),
    "sat_overwrite_dynamic_session": (
        "values",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: _dynamic_session_overwrite(s, f),
    ),
    "sat_overwrite_twice": ("values", "", "", 2, lambda s, f, o: _overwrite_twice(f)),
    "sat_overwrite_v3": ("values", "", "", 3, lambda s, f, o: _replace_overwrite(f)),
    "sat_overwrite_self_source": ("values", "", "", 2, lambda s, f, o: _self_source_overwrite(s)),
}

_NO_FORMAT: dict[str, tuple[str, str, str, int, Action]] = {
    "sat_overwrite_no_format": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: f.write.mode("overwrite").saveAsTable(_T),
    ),
    "sat_new_no_format": ("none", "", "", 2, lambda s, f, o: f.write.saveAsTable(_T)),
}

_BRANCH_OPTION: dict[str, tuple[str, str, str, int, Action]] = {
    "v2_option_branch_append": ("values", "", "", 2, _v2_branch_append),
    "v1_option_branch_append": ("values", "", "", 2, _v1_branch_append),
    "v2_option_branch_missing": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).option("branch", "b9").append(),
    ),
    "v2_option_branch_overwrite_partitions": (
        "values",
        _PARTITIONED,
        "",
        2,
        _v2_branch_overwrite_partitions,
    ),
    "v2_option_tag_append": ("values", "", "", 2, lambda s, f, o: _v2_tag_append(s, f)),
    "v2_options_branch_upper": ("values", "", "", 2, _v2_options_branch_upper),
    "v2_option_branch_create": (
        "none",
        "",
        "",
        2,
        lambda s, f, o: f.writeTo(_T).option("branch", "b1").using("iceberg").create(),
    ),
}


def _recorded_summaries(cell: str) -> list[dict[str, str]]:
    recorded = _RECORDED[cell]
    return [
        dict(summary) | dict(layout)
        for summary, layout in zip(
            recorded["md.snapshots"], recorded["layout.snapshots"], strict=True
        )
    ]


def _summaries_like(spark: ReparkSession, expected: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = spark.sql(f"SELECT operation, summary FROM {_T}.snapshots ORDER BY committed_at")
    return [
        {
            key: value
            for key, value in ({"operation": row[0]} | dict(row[1])).items()
            if key in wanted
        }
        for row, wanted in zip(rows.collect(), expected, strict=True)
    ]


def _refs_like_the_record(spark: ReparkSession) -> list[list[Any]]:
    snapshots = spark.sql(f"SELECT snapshot_id FROM {_T}.snapshots ORDER BY committed_at")
    index = {row[0]: f"S{position}" for position, row in enumerate(snapshots.collect())}
    refs = spark.sql(f"SELECT name, type, snapshot_id FROM {_T}.refs ORDER BY name").collect()
    return [[row[0], str(row[1]).lower(), index[row[2]], None, None, None] for row in refs]


def test_save_as_table_overwrite_replaces_like_the_recorded_cell(
    spark: ReparkSession, warehouse: Path
) -> None:
    """``mode('overwrite').saveAsTable`` is Spark's RTAS (cell W-DF-SAVEASTABLE-OVERWRITE).

    pins: u7-write-df-2/C-001
    """
    _seed(spark, "values", "", "", 2)
    _iceberg(_frame(spark)).mode("overwrite").saveAsTable(_T)
    recorded = _RECORDED["W-DF-SAVEASTABLE-OVERWRITE"]
    assert [list(row) for row in spark.sql(f"SELECT * FROM {_T} ORDER BY id").collect()] == (
        recorded["data"]
    )
    expected = _recorded_summaries("W-DF-SAVEASTABLE-OVERWRITE")
    assert _summaries_like(spark, expected) == expected
    assert _refs_like_the_record(spark) == recorded["md.refs"]


@pytest.mark.parametrize("name", list(_SAVE_AS_TABLE))
def test_save_as_table_overwrite_shapes_match_spark(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """A saveAsTable overwrite replaces schema, spec and sort order, keeps uuid and properties.

    The history cuts the lineage: the replace snapshot has no parent.

    pins: u7-write-df-2/C-002
    """
    assert _run(spark, warehouse, _SAVE_AS_TABLE[name]) == _MEASURED[name]


@pytest.mark.parametrize("name", list(_NO_FORMAT))
def test_save_as_table_without_a_format_writes_no_format_property_divergence(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """Residue R-1: Spark stores ``write.format.default=parquet``; RePark stores nothing.

    Every other observation equals Spark's.

    pins: u7-write-df-2/C-003
    """
    observed = _run(spark, warehouse, _NO_FORMAT[name])
    expected = dict(_MEASURED[name])
    assert expected.pop("properties") == {"write.format.default": "parquet"}
    assert observed.pop("properties") == {}
    assert observed == expected


def test_writer_v2_branch_option_writes_main_like_the_recorded_cell(spark: ReparkSession) -> None:
    """``writeTo.option('branch', 'b1').append()`` lands on main; b1 stays at S0.

    Cell W-DF-V2-OPTION-BRANCH.

    pins: u7-write-df-2/C-004
    """
    _seed(spark, "values", "", "", 2)
    _create_branch(spark)
    _frame(spark).writeTo(_T).option("branch", "b1").append()
    recorded = _RECORDED["W-DF-V2-OPTION-BRANCH"]
    assert [list(row) for row in spark.sql(f"SELECT * FROM {_T} ORDER BY id").collect()] == (
        recorded["data"]
    )
    branch = spark.sql(f"SELECT * FROM {_T}.branch_b1 ORDER BY id").collect()
    assert [list(row) for row in branch] == recorded["branch"]
    expected = _recorded_summaries("W-DF-V2-OPTION-BRANCH")
    assert _summaries_like(spark, expected) == expected
    assert _refs_like_the_record(spark) == recorded["md.refs"]


@pytest.mark.parametrize("name", list(_BRANCH_OPTION))
def test_branch_and_tag_options_are_ignored_like_spark(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """``branch``/``tag`` in any case, on any writer, even a missing ref, write the main branch.

    pins: u7-write-df-2/C-005
    """
    assert _run(spark, warehouse, _BRANCH_OPTION[name]) == _MEASURED[name]

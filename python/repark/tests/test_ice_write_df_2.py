"""U7 PR2 — DataFrame writer semantics: saveAsTable overwrite, the branch option, overwrite().

Every expected value is Spark 4.1.2 + Iceberg 1.11.0's answer, read from the committed
``ice_write_df_1_spark_oracle.json``: the scoreboard cells ``W-DF-SAVEASTABLE-OVERWRITE``,
``W-DF-V2-OPTION-BRANCH``, ``W-DF-V2-OVERWRITE-COND-PART`` and ``W-DF-V2-OVERWRITE-COND-ROWS``
under ``recorded``, and the PR2 shapes under ``measured``. Seed ``values`` is one
``INSERT … VALUES`` (one data file); seed ``named`` is an ``INSERT … UNION ALL`` (three data
files on both engines); seed ``none`` creates no table; seed ``empty`` creates the table with
no snapshot. Snapshot references are indexes in commit order. A residue pin states RePark's
answer as a rule over Spark's recorded one. The ``overwrite(condition)`` pins (C-006..C-010) live
in ``test_ice_write_df_2_overwrite.py`` on this module's helpers.

pins: u7-write-df-2/C-001, C-002, C-003, C-004, C-005, C-011, C-012
"""

from __future__ import annotations

import itertools
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions

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
    if seed == "nested":
        spark.sql(
            f"CREATE TABLE {_T} (id BIGINT, s STRUCT<a: INT, b: STRING>) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            f"INSERT INTO {_T} VALUES (1, named_struct('a', 1, 'b', 'x')), "
            "(2, named_struct('a', 2, 'b', 'y'))"
        )
        return
    if seed == "notnull":
        spark.sql(
            f"CREATE TABLE {_T} (id BIGINT NOT NULL, data STRING, cat STRING) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            f"INSERT INTO {_T} SELECT 1 AS id, 'a' AS data, 'x' AS cat "
            "UNION ALL SELECT 2, 'b', 'y' UNION ALL SELECT 3, 'c', 'x'"
        )
        return
    if seed == "identifier":
        spark.sql(
            f"CREATE TABLE {_T} (id BIGINT NOT NULL, data STRING, cat STRING) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(f"ALTER TABLE {_T} SET IDENTIFIER FIELDS id")
        spark.sql(f"INSERT INTO {_T} VALUES (1, 'a', 'x')")
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


def _plain(value: Any) -> Any:
    if isinstance(value, dict):
        return [_plain(item) for item in value.values()]
    return value


def _rows_of(spark: ReparkSession, query: str) -> list[list[Any]]:
    return [[_plain(value) for value in row] for row in spark.sql(query).collect()]


def _type_ids(field_type: Any) -> Any:
    if isinstance(field_type, str):
        return field_type
    kind = field_type["type"]
    if kind == "struct":
        return [
            [field["id"], field["name"], field["required"], _type_ids(field["type"])]
            for field in field_type["fields"]
        ]
    if kind == "list":
        return {"element-id": field_type["element-id"], "element": _type_ids(field_type["element"])}
    if kind == "map":
        return {
            "key-id": field_type["key-id"],
            "value-id": field_type["value-id"],
            "key": _type_ids(field_type["key"]),
            "value": _type_ids(field_type["value"]),
        }
    return kind


def _ids(metadata: dict[str, Any]) -> dict[str, Any]:
    current = next(
        schema
        for schema in metadata["schemas"]
        if schema["schema-id"] == metadata["current-schema-id"]
    )
    spec = next(
        spec
        for spec in metadata["partition-specs"]
        if spec["spec-id"] == metadata["default-spec-id"]
    )
    return {
        "field_ids": _type_ids({"type": "struct", "fields": current["fields"]}),
        "identifier_field_ids": current.get("identifier-field-ids", []),
        "last_column_id": metadata["last-column-id"],
        "current_schema_id": metadata["current-schema-id"],
        "schema_ids": sorted(schema["schema-id"] for schema in metadata["schemas"]),
        "spec_fields": [
            [field["source-id"], field["field-id"], field["name"], field["transform"]]
            for field in spec["fields"]
        ],
        "spec_ids": sorted(spec["spec-id"] for spec in metadata["partition-specs"]),
    }


def _state(spark: ReparkSession, warehouse: Path, uuid_before: str | None) -> dict[str, Any]:
    state: dict[str, Any] = {"rows": _rows_of(spark, f"SELECT * FROM {_T} ORDER BY id")}
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
    state.update(_ids(metadata))
    return state


def _branch_rows(spark: ReparkSession, out: dict[str, Any], name: str = "b1") -> None:
    out["branch_rows"] = _rows_of(spark, f"SELECT * FROM {_T}.branch_{name} ORDER BY id")


def _iceberg(frame: Any) -> Any:
    return frame.write.format("iceberg")


def _error(raised: BaseException, warehouse: Path) -> dict[str, Any]:
    message = str(raised).replace(str(warehouse), "<wh>")
    message = re.sub(r"<wh>/u7/t/data/\S+\.parquet", "<file>", message)
    return {
        "type": type(raised).__name__,
        "condition": getattr(raised, "getCondition", lambda: None)(),
        "sqlstate": getattr(raised, "getSqlState", lambda: None)(),
        "message": message,
    }


def _run(
    spark: ReparkSession,
    warehouse: Path,
    shape: tuple[str, str, str, int, Action],
) -> dict[str, Any]:
    seed, part, props, version, action = shape
    _seed(spark, seed, part, props, version)
    uuid_before = None if seed == "none" else _metadata(warehouse)["table-uuid"]
    out: dict[str, Any] = {}
    try:
        action(spark, _frame(spark), out)
        out["ok"] = True
    except Exception as raised:
        out["error"] = _error(raised, warehouse)
    if seed != "none" or out.get("ok"):
        out.update(_state(spark, warehouse, uuid_before))
    return _sorted_rows(out)


def _sorted_rows(observed: dict[str, Any]) -> dict[str, Any]:
    ordered = dict(observed)
    for key in ("rows", "branch_rows"):
        if isinstance(ordered.get(key), list):
            ordered[key] = _sorted_within_equal_ids(ordered[key])
    return ordered


def _sorted_within_equal_ids(rows: list[list[Any]]) -> list[list[Any]]:
    ordered: list[list[Any]] = []
    for _, group in itertools.groupby(rows, key=lambda row: repr(row[0])):
        ordered.extend(sorted(group, key=repr))
    return ordered


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


def _v1_branch_overwrite(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    _iceberg(frame).option("branch", "b1").mode("overwrite").saveAsTable(_T)
    _branch_rows(spark, out)


def _v1_branch_insert_overwrite(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    frame.write.option("branch", "b1").mode("overwrite").insertInto(_T)
    _branch_rows(spark, out)


def _v2_branch_condition(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    frame.writeTo(_T).option("branch", "b1").overwrite(functions.col("cat") == "x")
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
    "sat_overwrite_dynamic_option": (
        "values",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: (
            _iceberg(f).option("overwrite-mode", "dynamic").mode("overwrite").saveAsTable(_T)
        ),
    ),
    "sat_overwrite_partby_on_partitioned": (
        "values",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: _iceberg(f).partitionBy("data").mode("overwrite").saveAsTable(_T),
    ),
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
    "v1_option_branch_overwrite": ("values", "", "", 2, _v1_branch_overwrite),
    "v1_option_branch_insert_into_overwrite": ("values", "", "", 2, _v1_branch_insert_overwrite),
    "v2_option_branch_overwrite_condition": ("named", _PARTITIONED, "", 2, _v2_branch_condition),
}


def _replace_with_branch(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _create_branch(spark)
    _replace_overwrite(frame)
    _branch_rows(spark, out)


def _nested_reorder(spark: ReparkSession, frame: Any, out: dict[str, Any]) -> None:
    _ = frame
    _create_branch(spark)
    reordered = spark.createDataFrame([(7, ("g", 70))], "id BIGINT, s STRUCT<b: STRING, a: INT>")
    _replace_overwrite(reordered)
    out["branch_rows"] = _rows_of(spark, f"SELECT id, s.a, s.b FROM {_T}.branch_b1 ORDER BY id")
    out["flat_rows"] = _rows_of(spark, f"SELECT id, s.a, s.b FROM {_T} ORDER BY id")


def _readd_dropped(frame: Any) -> None:
    _replace_overwrite(frame.withColumn("extra", functions.lit(1)))
    _replace_overwrite(frame)
    _replace_overwrite(frame.withColumn("extra", functions.lit(2)))


_FIELD_IDS: dict[str, tuple[str, str, str, int, Action]] = {
    "ids_reorder_branch": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _replace_with_branch(s, f.select("cat", "data", "id"), o),
    ),
    "ids_swap_branch": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _replace_with_branch(
            s, f.selectExpr("cat AS data", "data AS cat", "id"), o
        ),
    ),
    "ids_rename_branch": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _replace_with_branch(s, f.withColumnRenamed("data", "payload"), o),
    ),
    "ids_add_branch": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _replace_with_branch(s, f.withColumn("extra", functions.lit(1)), o),
    ),
    "ids_drop_branch": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _replace_with_branch(s, f.drop("cat"), o),
    ),
    "ids_nested_reorder_branch": ("nested", "", "", 2, _nested_reorder),
    "ids_identifier": ("identifier", "", "", 2, lambda s, f, o: _replace_overwrite(f)),
    "ids_partby_reordered": (
        "values",
        _PARTITIONED,
        "",
        2,
        lambda s, f, o: (
            _iceberg(f.select("cat", "id", "data"))
            .partitionBy("cat")
            .mode("overwrite")
            .saveAsTable(_T)
        ),
    ),
    "ids_readd_dropped": ("values", "", "", 2, lambda s, f, o: _readd_dropped(f)),
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
    assert _run(spark, warehouse, _SAVE_AS_TABLE[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", list(_NO_FORMAT))
def test_save_as_table_without_a_format_writes_no_format_property_divergence(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """Residue R-1: Spark stores ``write.format.default=parquet``; RePark stores nothing.

    Every other observation equals Spark's.

    pins: u7-write-df-2/C-003
    """
    observed = _run(spark, warehouse, _NO_FORMAT[name])
    expected = _sorted_rows(_MEASURED[name])
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
    assert _run(spark, warehouse, _BRANCH_OPTION[name]) == _sorted_rows(_MEASURED[name])


@pytest.mark.parametrize("name", list(_FIELD_IDS))
def test_save_as_table_overwrite_keeps_field_ids_by_name_like_spark(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """The replace keeps each column's field id by name, as Java's ``assignFreshIds`` does.

    A new name takes a fresh id above ``last-column-id``, nested fields keep theirs by dotted
    name, and a branch that still points at a pre-replace snapshot reads its own rows.

    pins: u7-write-df-2/C-011
    """
    assert _run(spark, warehouse, _FIELD_IDS[name]) == _sorted_rows(_MEASURED[name])


def test_a_type_change_on_a_kept_name_reads_the_old_branch_as_null_divergence(
    spark: ReparkSession, warehouse: Path
) -> None:
    """Residue R-9: after ``id`` turns INT, the old branch reads NULL ids on RePark.

    Spark 4.1.2 fails that read (``ClassCastException`` IntVector to BigIntVector). Every other
    observation, the kept field ids included, equals Spark's.

    pins: u7-write-df-2/C-012
    """
    observed = _run(
        spark,
        warehouse,
        (
            "values",
            "",
            "",
            2,
            lambda s, f, o: _replace_with_branch(
                s, f.selectExpr("CAST(id AS INT) AS id", "data", "cat"), o
            ),
        ),
    )
    expected = _sorted_rows(_MEASURED["ids_type_change_branch"])
    assert expected.pop("branch_rows")["error"]["message"].startswith(
        "java.lang.ClassCastException"
    )
    assert observed.pop("branch_rows") == [[None, "a", "x"], [None, "b", "y"], [None, "c", "x"]]
    assert observed == expected


def _door_with_branch(
    spark: ReparkSession, out: dict[str, Any], replace: Callable[[], object]
) -> None:
    _create_branch(spark)
    replace()
    _branch_rows(spark, out)


_REPLACE_DOORS: dict[str, tuple[str, str, str, int, Action]] = {
    "sql_column_def_replace": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _door_with_branch(
            s,
            o,
            lambda: s.sql(
                f"CREATE OR REPLACE TABLE {_T} (cat STRING, data STRING, id BIGINT) USING iceberg"
            ),
        ),
    ),
    "sql_replace_table": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _door_with_branch(
            s,
            o,
            lambda: s.sql(
                f"REPLACE TABLE {_T} (cat STRING, payload STRING, id BIGINT) USING iceberg"
            ),
        ),
    ),
    "sql_rtas_reordered": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _door_with_branch(
            s,
            o,
            lambda: s.sql(
                f"CREATE OR REPLACE TABLE {_T} USING iceberg AS SELECT 'z' AS cat, 'q' AS data, "
                "CAST(9 AS BIGINT) AS id"
            ),
        ),
    ),
    "v2_create_or_replace_reordered": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _door_with_branch(
            s,
            o,
            lambda: f.select("cat", "data", "id").writeTo(_T).using("iceberg").createOrReplace(),
        ),
    ),
    "v2_replace_renamed": (
        "values",
        "",
        "",
        2,
        lambda s, f, o: _door_with_branch(
            s,
            o,
            lambda: (
                f.selectExpr("cat", "data AS payload", "id").writeTo(_T).using("iceberg").replace()
            ),
        ),
    ),
}


@pytest.mark.parametrize("name", list(_REPLACE_DOORS))
def test_every_replace_door_keeps_field_ids_by_name_like_spark(
    spark: ReparkSession, warehouse: Path, name: str
) -> None:
    """Column-def ``CREATE OR REPLACE`` and ``REPLACE TABLE``, SQL RTAS, ``createOrReplace()``
    and ``replace()`` keep each column's id by name; the old branch reads Spark's rows.

    pins: u7-write-df-2/C-011
    """
    assert _run(spark, warehouse, _REPLACE_DOORS[name]) == _sorted_rows(_MEASURED[name])

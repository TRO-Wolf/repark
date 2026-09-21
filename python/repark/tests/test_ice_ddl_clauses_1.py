"""IPI-26 + IPI-27 round 1 — the two silent rewrite bugs in the Spark door.

Three inventory cells die in the pre-parse rewrite layer of ``crates/repark-spark``,
not in missing features:

* ``D-ADD-COL-STRUCT`` — ``split_top_level_comma_segments`` tracks only paren depth,
  so the comma inside ``STRUCT<p: INT, q: STRING>`` splits the column list.
* ``D-X-ADD-COL-MAP-KEY-STRUCT`` — needs the splitter fix and the dialect widening:
  an ``ALTER TABLE`` carrying an angle-bracket ``MAP<`` selects ``SparkSqlDialect``
  exactly as ``CREATE TABLE`` already does.
* ``D-X-CLUSTERED-BY`` — ``CLUSTERED BY (col) INTO n BUCKETS`` is token-rewritten
  into ``PARTITIONED BY (bucket(n, col))`` with the explicit field name
  ``{col}_bucket``; Spark records ``[["id_bucket", "bucket[4]", "id"]]``.

Oracle: the run-25/26 inventory harness cells recorded against live PySpark 4.1.2 +
``iceberg-spark-runtime-4.1_2.13:1.11.0`` (``/tmp/oc-worker/nc-inventory/matrix.json``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

CATALOG = "sc"
NAMESPACE = "ns"


@pytest.fixture
def spark(tmp_path: Path) -> Any:
    """Yield a facade session with an in-memory Iceberg catalog named ``sc``."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-ice-ddl-clauses-1").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path / "wh")
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _create(session: Any, name: str) -> str:
    """Create one empty seed-shaped Iceberg table and return its three-part name."""
    table = f"{CATALOG}.{NAMESPACE}.{name}"
    session.sql(f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg")
    return table


def _metadata(warehouse: Path, table: str) -> dict[str, Any]:
    """Parse the newest table metadata JSON under the warehouse."""
    metas = sorted(warehouse.rglob(f"{table}/metadata/*.metadata.json"))
    assert metas, f"no metadata found for {table}"
    return json.loads(metas[-1].read_text(encoding="utf-8"))


def _type_string(node: Any) -> str:
    """Render one metadata type node in the inventory harness spelling."""
    if isinstance(node, str):
        return node
    kind = node.get("type")
    if kind == "struct":
        inner = ",".join(
            f"{field['name']}:{_type_string(field['type'])}" for field in node["fields"]
        )
        return f"struct<{inner}>"
    if kind == "list":
        return f"list<{_type_string(node['element'])}>"
    if kind == "map":
        return f"map<{_type_string(node['key'])},{_type_string(node['value'])}>"
    return str(node)


def _walk_schema_fields(
    fields: list[dict[str, Any]], prefix: str, by_id: dict[int, str]
) -> list[list[Any]]:
    """Flatten schema fields to ``[name, type, required, doc]`` rows, nested dotted."""
    rows: list[list[Any]] = []
    for field in fields:
        by_id[field["id"]] = prefix + field["name"]
        rows.append(
            [
                prefix + field["name"],
                _type_string(field["type"]),
                field.get("required"),
                field.get("doc"),
            ]
        )
        node = field["type"]
        if isinstance(node, dict) and node.get("type") == "struct":
            rows.extend(_walk_schema_fields(node["fields"], prefix + field["name"] + ".", by_id))
    return rows


def _schema_rows(meta: dict[str, Any]) -> tuple[list[list[Any]], dict[int, str]]:
    """Schema rows plus the field-id to dotted-name map of the current schema."""
    by_id: dict[int, str] = {}
    current = meta["current-schema-id"]
    schema = next(item for item in meta["schemas"] if item["schema-id"] == current)
    return _walk_schema_fields(schema["fields"], "", by_id), by_id


def _spec(meta: dict[str, Any], by_id: dict[int, str]) -> list[list[Any]]:
    """Default partition spec as ``[name, transform, source]`` rows."""
    specs = [s for s in meta["partition-specs"] if s["spec-id"] == meta["default-spec-id"]]
    fields = specs[0]["fields"] if specs else []
    return [[f["name"], f["transform"], by_id.get(f["source-id"], f["source-id"])] for f in fields]


def _arrow_fields(session: Any, table: str) -> dict[str, pa.Field]:
    """Arrow fields of ``SELECT *`` keyed by top-level column name."""
    arrow = session.sql(f"SELECT * FROM {table}").to_arrow()
    return {field.name: field for field in arrow.schema}


def test_add_columns_plural_with_nested_types(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-ADD-COL-STRUCT``: commas inside ``<>`` must not split the list."""
    table = _create(spark, "t_nested_add")
    short = table.split(".")[-1]
    spark.sql(
        f"ALTER TABLE {table} ADD COLUMNS "
        "(s2 STRUCT<p: INT, q: STRING>, m2 MAP<STRING, INT>, a2 ARRAY<STRING>)"
    )

    rows, _ = _schema_rows(_metadata(tmp_path / "wh", short))
    assert ["s2", "struct<p:int,q:string>", False, None] in rows
    assert ["s2.p", "int", False, None] in rows
    assert ["s2.q", "string", False, None] in rows
    assert ["m2", "map<string,int>", False, None] in rows
    assert ["a2", "list<string>", False, None] in rows

    fields = _arrow_fields(spark, table)
    assert pa.types.is_struct(fields["s2"].type)
    assert [(item.name, str(item.type)) for item in fields["s2"].type] == [
        ("p", "int32"),
        ("q", "string"),
    ]
    assert pa.types.is_map(fields["m2"].type)
    assert str(fields["m2"].type.key_type) == "string"
    assert str(fields["m2"].type.item_type) == "int32"
    assert pa.types.is_list(fields["a2"].type)
    assert str(fields["a2"].type.value_type) == "string"


def test_add_column_map_key_struct_and_deep_nesting(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-X-ADD-COL-MAP-KEY-STRUCT`` plus ``>>``-closing plural defs."""
    table = _create(spark, "t_map_key_struct")
    short = table.split(".")[-1]
    spark.sql(f"ALTER TABLE {table} ADD COLUMN m1 MAP<STRUCT<a: INT>, STRING>")
    spark.sql(
        f"ALTER TABLE {table} ADD COLUMNS "
        "(deep ARRAY<STRUCT<x: INT, y: STRING>>, m3 MAP<STRING, ARRAY<INT>>)"
    )

    rows, _ = _schema_rows(_metadata(tmp_path / "wh", short))
    assert ["m1", "map<struct<a:int>,string>", False, None] in rows
    assert ["deep", "list<struct<x:int,y:string>>", False, None] in rows
    assert ["m3", "map<string,list<int>>", False, None] in rows

    fields = _arrow_fields(spark, table)
    assert pa.types.is_map(fields["m1"].type)
    assert pa.types.is_list(fields["deep"].type)
    assert pa.types.is_struct(fields["deep"].type.value_type)


def test_dialect_widening_changes_no_other_alter(spark: Any) -> None:
    """Every non-MAP ALTER keeps its pre-widening behaviour."""
    table = _create(spark, "t_alter_corpus")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('k'='v')")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN c1 INT")
    spark.sql(f"ALTER TABLE {table} ADD COLUMNS (c2 INT, c3 STRING)")
    spark.sql(f"ALTER TABLE {table} DROP COLUMN c3")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN c1 TYPE BIGINT")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN data FIRST")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN c1 AFTER id")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(8, id) AS id_b8")
    spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD id_b8 WITH bucket(16, id) AS id_b16")
    spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD id_b16")

    arrow = spark.sql(f"SELECT * FROM {table}").to_arrow()
    assert [field.name for field in arrow.schema] == ["data", "id", "c1", "c2"]
    assert str(arrow.schema.field("c1").type) == "int64"


def test_clustered_by_is_not_silently_dropped(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-X-CLUSTERED-BY``: the spec must equal Spark's, not vanish."""
    table = f"{CATALOG}.{NAMESPACE}.t_clustered"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, data STRING) "
        "USING iceberg CLUSTERED BY (id) INTO 4 BUCKETS"
    )

    meta = _metadata(tmp_path / "wh", "t_clustered")
    _, by_id = _schema_rows(meta)
    assert _spec(meta, by_id) == [["id_bucket", "bucket[4]", "id"]]

    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    arrow = spark.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    assert arrow.column("id").to_pylist() == [1, 2]


def _properties(meta: dict[str, Any]) -> dict[str, Any]:
    """Table properties of one metadata document."""
    return dict(meta.get("properties", {}))


def test_create_column_comment(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-CREATE-COL-COMMENT``: the doc is the schema row's fourth field."""
    table = f"{CATALOG}.{NAMESPACE}.t_col_comment"
    spark.sql(f"CREATE TABLE {table} (id BIGINT COMMENT 'the id', data STRING) USING iceberg")

    rows, _ = _schema_rows(_metadata(tmp_path / "wh", "t_col_comment"))
    assert ["id", "long", False, "the id"] in rows
    assert ["data", "string", False, None] in rows
    assert str(_arrow_fields(spark, table)["id"].type) == "int64"


def test_create_table_comment(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-CREATE-COMMENT``: the table comment is the ``comment`` property."""
    table = f"{CATALOG}.{NAMESPACE}.t_create_comment"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT COMMENT 'the id', data STRING) "
        "USING iceberg COMMENT 'tbl doc'"
    )

    meta = _metadata(tmp_path / "wh", "t_create_comment")
    assert _properties(meta)["comment"] == "tbl doc"
    rows, _ = _schema_rows(meta)
    assert ["id", "long", False, "the id"] in rows


def test_ctas_comment(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-CTAS-COMMENT``: CTAS carries the table comment and the rows."""
    table = f"{CATALOG}.{NAMESPACE}.t_ctas_comment"
    spark.sql(f"CREATE TABLE {table} USING iceberg COMMENT 'hello' AS SELECT 1 AS i")

    assert _properties(_metadata(tmp_path / "wh", "t_ctas_comment"))["comment"] == "hello"
    arrow = spark.sql(f"SELECT i FROM {table} ORDER BY i").to_arrow()
    assert arrow.column("i").to_pylist() == [1]


def test_comment_on_table(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-COMMENT-ON``: the statement sets the ``comment`` property."""
    table = _create(spark, "t_comment_on")
    spark.sql(f"COMMENT ON TABLE {table} IS 'doc here'")

    assert _properties(_metadata(tmp_path / "wh", "t_comment_on"))["comment"] == "doc here"


def test_comment_on_table_is_null_removes_the_property(spark: Any, tmp_path: Path) -> None:
    """``COMMENT ON TABLE t IS NULL`` removes the ``comment`` property."""
    table = _create(spark, "t_comment_null")
    spark.sql(f"COMMENT ON TABLE {table} IS 'doc here'")
    spark.sql(f"COMMENT ON TABLE {table} IS NULL")

    assert "comment" not in _properties(_metadata(tmp_path / "wh", "t_comment_null"))


def test_change_column_hive_type_and_comment(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-X-CHANGE-COLUMN-TYPE``: the Hive form sets type and doc, keeps rows."""
    table = _create(spark, "t_hive_change")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a')")
    spark.sql(f"ALTER TABLE {table} CHANGE COLUMN id id BIGINT COMMENT 'hive-style'")

    rows, _ = _schema_rows(_metadata(tmp_path / "wh", "t_hive_change"))
    assert ["id", "long", False, "hive-style"] in rows
    assert ["data", "string", False, None] in rows
    arrow = spark.sql(f"SELECT id, data FROM {table} ORDER BY id").to_arrow()
    assert arrow.to_pylist() == [{"id": 1, "data": "a"}]


def test_create_location_files_land_under_path(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-CREATE-LOCATION``: data files land under the given path."""
    location = tmp_path / "custom_loc" / "t_create_loc"
    table = f"{CATALOG}.{NAMESPACE}.t_create_loc"
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{location}'")
    spark.sql(f"INSERT INTO {table} VALUES (1)")

    assert _metadata(tmp_path / "custom_loc", "t_create_loc")["location"] == str(location)
    assert sorted(location.rglob("*.parquet"))
    arrow = spark.sql(f"SELECT id FROM {table} ORDER BY id").to_arrow()
    assert arrow.column("id").to_pylist() == [1]


def test_ctas_location_files_land_under_path(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-CTAS-LOCATION``: CTAS data files land under the given path."""
    location = tmp_path / "ctasloc" / "t_ctas_loc"
    table = f"{CATALOG}.{NAMESPACE}.t_ctas_loc"
    spark.sql(f"CREATE TABLE {table} USING iceberg LOCATION '{location}' AS SELECT 1 AS i")

    assert _metadata(tmp_path / "ctasloc", "t_ctas_loc")["location"] == str(location)
    assert sorted(location.rglob("*.parquet"))
    arrow = spark.sql(f"SELECT i FROM {table} ORDER BY i").to_arrow()
    assert arrow.column("i").to_pylist() == [1]


def test_ctas_location_and_comment_after_tblproperties(spark: Any, tmp_path: Path) -> None:
    """dbt emits ``tblproperties`` before ``location`` and ``comment``; that order serves."""
    location = tmp_path / "dbtorder" / "t_dbt_order"
    table = f"{CATALOG}.{NAMESPACE}.t_dbt_order"
    spark.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ('k' = 'v') "
        f"LOCATION '{location}' COMMENT 'dbt order' AS SELECT 1 AS i"
    )

    meta = _metadata(tmp_path / "dbtorder", "t_dbt_order")
    assert meta["location"] == str(location)
    assert _properties(meta)["comment"] == "dbt order"
    assert _properties(meta)["k"] == "v"


def test_replace_with_location_refuses(spark: Any) -> None:
    """``OR REPLACE`` with ``LOCATION`` refuses: the replace keeps its location."""
    from repark.errors import PySparkException

    table = _create(spark, "t_replace_loc")
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"CREATE OR REPLACE TABLE {table} (id BIGINT) USING iceberg LOCATION '/tmp/x'")
    assert "OR REPLACE" in str(caught.value)


def test_date_alias_names_the_field_ts_day(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-CREATE-PART-DATE-ALIAS``: ``date``/``date_hour`` alias day/hour."""
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_date_alias (ts TIMESTAMP, data STRING) "
        "USING iceberg PARTITIONED BY (date(ts))"
    )
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_date_hour_alias (ts TIMESTAMP, data STRING) "
        "USING iceberg PARTITIONED BY (date_hour(ts))"
    )

    meta = _metadata(tmp_path / "wh", "t_date_alias")
    _, by_id = _schema_rows(meta)
    assert _spec(meta, by_id) == [["ts_day", "day", "ts"]]

    meta = _metadata(tmp_path / "wh", "t_date_hour_alias")
    _, by_id = _schema_rows(meta)
    assert _spec(meta, by_id) == [["ts_hour", "hour", "ts"]]


def test_bucket_both_argument_orders_give_the_same_spec(spark: Any, tmp_path: Path) -> None:
    """Near-miss pin: ``bucket(id, 4)`` and ``bucket(4, id)`` record the same spec."""
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_bucket_w_first (id BIGINT) "
        "USING iceberg PARTITIONED BY (bucket(4, id))"
    )
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_bucket_c_first (id BIGINT) "
        "USING iceberg PARTITIONED BY (bucket(id, 4))"
    )

    for name in ("t_bucket_w_first", "t_bucket_c_first"):
        meta = _metadata(tmp_path / "wh", name)
        _, by_id = _schema_rows(meta)
        assert _spec(meta, by_id) == [["id_bucket", "bucket[4]", "id"]]


def test_truncate_both_argument_orders_give_the_same_spec(spark: Any, tmp_path: Path) -> None:
    """Near-miss pin: ``truncate(data, 4)`` and ``truncate(4, data)`` record the same spec."""
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_trunc_w_first (data STRING) "
        "USING iceberg PARTITIONED BY (truncate(4, data))"
    )
    spark.sql(
        f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_trunc_c_first (data STRING) "
        "USING iceberg PARTITIONED BY (truncate(data, 4))"
    )

    for name in ("t_trunc_w_first", "t_trunc_c_first"):
        meta = _metadata(tmp_path / "wh", name)
        _, by_id = _schema_rows(meta)
        assert _spec(meta, by_id) == [["data_trunc", "truncate[4]", "data"]]


def test_bucket_two_integer_arguments_raises(spark: Any) -> None:
    """``bucket(1, 2)`` is ambiguous — both arguments read as integers — and must refuse."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException) as caught:
        spark.sql(
            f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_bucket_ambiguous (id BIGINT) "
            "USING iceberg PARTITIONED BY (bucket(1, 2))"
        )
    assert "bucket" in str(caught.value)


def test_partition_transform_near_misses_still_refuse(spark: Any) -> None:
    """Neither-integer bucket args and an unknown transform keep refusing."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException) as caught:
        spark.sql(
            f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_bucket_str (id BIGINT) "
            "USING iceberg PARTITIONED BY (bucket('a', 'b'))"
        )
    assert "bucket" in str(caught.value)

    with pytest.raises(PySparkException) as caught:
        spark.sql(
            f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_bucket_unknown (id BIGINT) "
            "USING iceberg PARTITIONED BY (unknown_xform(id))"
        )
    assert "not a supported partition transform" in str(caught.value)


def test_bucket_quoted_string_is_never_the_width(spark: Any) -> None:
    """``bucket('x', 16)``/``bucket(\"x\", 16)`` refuse; the table has column ``x``.

    The wrong implementation under this pin strips the quotes, sniffs ``16`` as the
    width, and records ``x_bucket``/``bucket[16]`` instead of refusing.
    """
    from repark.errors import PySparkException

    for literal in ("'x'", '"x"'):
        with pytest.raises(PySparkException) as caught:
            spark.sql(
                f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_bucket_quoted_width (x BIGINT) "
                f"USING iceberg PARTITIONED BY (bucket({literal}, 16))"
            )
        assert "numBuckets must be an integer" in str(caught.value)


def test_truncate_quoted_string_is_never_the_width(spark: Any) -> None:
    """``truncate(ts, 'day')`` refuses: a quoted string is never a width."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException) as caught:
        spark.sql(
            f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_trunc_quoted_width (ts TIMESTAMP) "
            "USING iceberg PARTITIONED BY (truncate(ts, 'day'))"
        )
    assert "width must be an integer" in str(caught.value)


def test_date_transform_quoted_column_refuses(spark: Any) -> None:
    """``date('ts')`` refuses: a quoted string is never a partition column."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException) as caught:
        spark.sql(
            f"CREATE TABLE {CATALOG}.{NAMESPACE}.t_date_quoted_col (ts TIMESTAMP) "
            "USING iceberg PARTITIONED BY (date('ts'))"
        )
    assert "cannot resolve CTAS partition column" in str(caught.value)


def test_replace_partition_field_transform_lhs_days_with_hours(spark: Any, tmp_path: Path) -> None:
    """Cell ``D-REPLACE-PART-FIELD``: ``days(ts) WITH hours(ts)`` resolves via the spec.

    The ACTUAL inventory sequence is ``CREATE … PARTITIONED BY (days(ts))`` then
    ``REPLACE … days(ts) WITH hours(ts)``. Live Spark records spec
    ``[["ts_hour","hour","ts"]]`` with spec-count 2; RePark records the same: the
    CREATE carries one spec (default-spec-id 0) and the REPLACE adds exactly one.
    """
    table = f"{CATALOG}.{NAMESPACE}.t_replace_lhs"
    spark.sql(
        f"CREATE TABLE {table} (ts TIMESTAMP, data STRING) "
        "USING iceberg PARTITIONED BY (days(ts))"
    )
    spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD days(ts) WITH hours(ts)")

    meta = _metadata(tmp_path / "wh", "t_replace_lhs")
    _, by_id = _schema_rows(meta)
    assert _spec(meta, by_id) == [["ts_hour", "hour", "ts"]]
    assert len(meta["partition-specs"]) == 2
    assert meta["default-spec-id"] == 1


def test_replace_partition_field_transform_lhs_matching_no_field_refuses(spark: Any) -> None:
    """A transform LHS naming no current partition field refuses loud."""
    from repark.errors import PySparkException

    table = _create(spark, "t_replace_lhs_nomatch")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(4, id) AS id_b4")
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD truncate(4, id) WITH bucket(8, id)")
    assert "matches no partition field" in str(caught.value)


def test_replace_partition_field_days_lhs_wrong_source_refuses(spark: Any) -> None:
    """``days(data)`` vs a spec whose only days field is ``days(ts)`` refuses loud.

    The transform alone is present in the spec; only the (source column, transform)
    pair matches, so a resolver ignoring the source column is killed by this pin.
    """
    from repark.errors import PySparkException

    table = f"{CATALOG}.{NAMESPACE}.t_replace_lhs_wrong_src"
    spark.sql(
        f"CREATE TABLE {table} (ts TIMESTAMP, data STRING) "
        "USING iceberg PARTITIONED BY (days(ts))"
    )
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD days(data) WITH hours(data)")
    assert "matches no partition field" in str(caught.value)


def test_replace_partition_field_truncate_lhs_wrong_source_refuses(spark: Any) -> None:
    """The truncate LHS shares the (source column, transform) resolution and refuses."""
    from repark.errors import PySparkException

    table = f"{CATALOG}.{NAMESPACE}.t_replace_lhs_trunc_src"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, data STRING) USING iceberg")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD truncate(4, id)")
    with pytest.raises(PySparkException) as caught:
        spark.sql(
            f"ALTER TABLE {table} REPLACE PARTITION FIELD truncate(4, data) "
            "WITH truncate(8, data)"
        )
    assert "matches no partition field" in str(caught.value)


def test_describe_shows_column_comment(spark: Any) -> None:
    """Cell ``D-DESCRIBE`` re-check: the comment column carries the doc."""
    table = f"{CATALOG}.{NAMESPACE}.t_describe_doc"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT COMMENT 'c', data STRING) "
        "USING iceberg PARTITIONED BY (bucket(4, id))"
    )

    rows = spark.sql(f"DESCRIBE TABLE {table}").to_arrow().to_pylist()
    by_name = {row["col_name"]: row for row in rows}
    assert by_name["id"] == {"col_name": "id", "data_type": "bigint", "comment": "c"}
    assert by_name["Part 0"] == {
        "col_name": "Part 0",
        "data_type": "bucket(4, id)",
        "comment": "",
    }

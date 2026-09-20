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
    spark.sql(
        f"ALTER TABLE {table} REPLACE PARTITION FIELD id_b8 WITH bucket(16, id) AS id_b16"
    )
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

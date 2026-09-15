"""CATALOG-SURFACE-1: the 13 Catalog names against the run-15b facade oracle."""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    ParseException,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark import catalog as catalog_module
from repark.spark.catalog import Catalog, Table
from repark.spark.storage import StorageLevel
from repark.spark.types import IntegerType, StringType, StructField, StructType

_FIXTURE = json.loads(
    (Path(__file__).parent / "facade_catalog_oracle.json").read_text(encoding="utf-8")
)


def _result(name: str) -> Any:
    return _FIXTURE["cells"][name]["result"]


def _error(name: str) -> Any:
    return _FIXTURE["cells"][name]["error"]


def _decode(node: Any) -> Any:
    if isinstance(node, dict):
        if "items" in node:
            items = node["items"]
            if isinstance(items, dict):
                return {key: _decode(value) for key, value in items.items()}
            return [_decode(item) for item in items]
        if "value" in node:
            return _decode(node["value"])
        return {key: _decode(value) for key, value in node.items()}
    return node


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-catalog-surface-1").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql("CREATE NAMESPACE glue_catalog.ns1")
    session.sql("CREATE TABLE glue_catalog.ns1.t1 (a INT, p STRING)")
    session.sql("INSERT INTO glue_catalog.ns1.t1 VALUES (1, 'x'), (2, 'y')")
    session.sql("CREATE TABLE glue_catalog.ns1.pt1 (a INT, p STRING) PARTITIONED BY (p)")
    session.createDataFrame(
        [("x", 1, 2), ("y", 3, 4)], "key string, a int, b int"
    ).createOrReplaceTempView("tv1")
    session.catalog.setCurrentCatalog("glue_catalog")
    session.catalog.setCurrentDatabase("ns1")
    return session


def _columns(cell_name: str) -> list[tuple[Any, ...]]:
    return [tuple(item) for item in _decode(_result(cell_name))]


def test_get_table_permanent(spark: ReparkSession) -> None:
    """C-001: getTable/get_table on an Iceberg table (oracle getTable, getTable_fields)."""
    row = spark.catalog.getTable("t1")
    oracle = _decode(_result("getTable"))
    assert isinstance(row, Table)
    assert tuple(row) == ("t1", "glue_catalog", ["ns1"], None, "MANAGED", False)
    assert tuple(row)[3:] == tuple(oracle[3:])
    assert list(Table._fields) == _decode(_result("getTable_fields"))
    assert spark.catalog.get_table("t1") == row


def test_get_table_qualified(spark: ReparkSession) -> None:
    """C-001: a three-part name answers the same row (oracle getTable_qualified)."""
    row = spark.catalog.getTable("glue_catalog.ns1.t1")
    assert tuple(row) == ("t1", "glue_catalog", ["ns1"], None, "MANAGED", False)


def test_get_table_two_part(spark: ReparkSession) -> None:
    """C-001: a two-part name resolves under the current catalog."""
    row = spark.catalog.getTable("ns1.t1")
    assert tuple(row) == ("t1", "glue_catalog", ["ns1"], None, "MANAGED", False)


def test_get_table_temp_view(spark: ReparkSession) -> None:
    """C-001: a temp view answers the TEMPORARY row (oracle getTable_temp_view)."""
    row = spark.catalog.getTable("tv1")
    assert tuple(row) == tuple(_decode(_result("getTable_temp_view")))
    assert row == Table(
        name="tv1",
        catalog=None,
        namespace=[],
        description=None,
        tableType="TEMPORARY",
        isTemporary=True,
    )


def test_get_table_missing(spark: ReparkSession) -> None:
    """C-001: a missing name raises TABLE_OR_VIEW_NOT_FOUND (oracle getTable_missing)."""
    oracle = _error("getTable_missing")
    assert oracle["raises"] == "AnalysisException"
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.catalog.getTable("nope_tbl")


def test_list_columns_permanent(spark: ReparkSession) -> None:
    """C-002: one Column per field in order (oracle listColumns, column_fields)."""
    columns = spark.catalog.listColumns("t1")
    assert [tuple(column) for column in columns] == _columns("listColumns")
    assert all(isinstance(column, catalog_module.Column) for column in columns)
    assert list(catalog_module.Column._fields) == _decode(_result("column_fields"))
    assert spark.catalog.list_columns("t1") == columns


def test_list_columns_partitioned(spark: ReparkSession) -> None:
    """C-002: identity-partition source columns are flagged (oracle listColumns_partitioned)."""
    columns = spark.catalog.listColumns("pt1")
    assert [tuple(column) for column in columns] == _columns("listColumns_partitioned")


def test_list_columns_db_name_warns(spark: ReparkSession) -> None:
    """C-002: dbName warns FutureWarning and qualifies (oracle listColumns_dbName)."""
    with pytest.warns(FutureWarning, match="`dbName` has been deprecated since Spark 3.4"):
        columns = spark.catalog.listColumns("t1", "ns1")
    assert [tuple(column) for column in columns] == _columns("listColumns_dbName")


def test_list_columns_view(spark: ReparkSession) -> None:
    """C-002: temp views list their columns (oracle listColumns_view)."""
    columns = spark.catalog.listColumns("tv1")
    assert [tuple(column) for column in columns] == _columns("listColumns_view")


def test_list_columns_missing(spark: ReparkSession) -> None:
    """C-002: a missing name raises TABLE_OR_VIEW_NOT_FOUND (oracle listColumns_missing)."""
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.catalog.listColumns("nope_tbl")


def test_list_functions_shape(spark: ReparkSession) -> None:
    """C-003: Function rows sorted by name (oracle listFunctions_len_type / CAT-FUNCS-1)."""
    oracle = _decode(_result("listFunctions_len_type"))
    functions = spark.catalog.listFunctions()
    assert type(functions[0]).__name__ == oracle["type"]
    assert list(catalog_module.Function._fields) == oracle["fields"]
    names = [function.name for function in functions]
    assert names == sorted(names)
    assert len(names) == len(set(names))
    builtins = [function for function in functions if function.name == "abs"]
    assert builtins == [
        catalog_module.Function(
            name="abs",
            catalog=None,
            namespace=None,
            description="",
            className="repark.builtin",
            isTemporary=True,
        )
    ]
    assert spark.catalog.list_functions() == functions


def test_list_functions_pattern(spark: ReparkSession) -> None:
    """C-003: the `*`/`|` glob filters names (oracle listFunctions_pattern)."""
    functions = spark.catalog.listFunctions(pattern="to_*")
    assert functions
    assert all(function.name.startswith("to_") for function in functions)
    assert all(isinstance(function, catalog_module.Function) for function in functions)
    assert "to_date" in {function.name for function in functions}


def test_list_functions_db_name(spark: ReparkSession) -> None:
    """C-003: a dbName argument is accepted (oracle listFunctions_db / CAT-FUNCS-1)."""
    assert len(spark.catalog.listFunctions("ns1")) == len(spark.catalog.listFunctions())


def test_list_functions_udf(spark: ReparkSession) -> None:
    """C-003: registered session UDFs list as temp functions (oracle listFunctions_udf)."""
    spark.udf.register("my_udf1", lambda value: value, "int")
    rows = [function for function in spark.catalog.listFunctions() if function.name == "my_udf1"]
    assert rows == [
        catalog_module.Function(
            name="my_udf1",
            catalog=None,
            namespace=None,
            description="N/A.",
            className="repark.python_udf",
            isTemporary=True,
        )
    ]


def test_get_function_builtin(spark: ReparkSession) -> None:
    """C-003: a built-in resolves to the repark registry row (oracle getFunction_builtin)."""
    oracle = _decode(_result("getFunction_builtin"))
    function = spark.catalog.getFunction("abs")
    assert isinstance(function, catalog_module.Function)
    assert function.name == oracle[0]
    assert function.catalog is None and function.namespace is None
    assert function.className == "repark.builtin"
    assert function.isTemporary == oracle[5]
    assert spark.catalog.get_function("abs") == function


def test_get_function_udf(spark: ReparkSession) -> None:
    """C-003: a session UDF resolves to the temp-function row (oracle getFunction_udf)."""
    spark.udf.register("my_udf1", lambda value: value, "int")
    oracle = _decode(_result("getFunction_udf"))
    function = spark.catalog.getFunction("my_udf1")
    assert function.name == oracle[0]
    assert function.description == oracle[3]
    assert function.className == "repark.python_udf"
    assert function.isTemporary == oracle[5]


def test_get_function_missing(spark: ReparkSession) -> None:
    """C-003: a missing routine raises UNRESOLVED_ROUTINE (oracle getFunction_missing)."""
    oracle = _error("getFunction_missing")
    assert oracle["condition"] == "UNRESOLVED_ROUTINE"
    with pytest.raises(AnalysisException, match="UNRESOLVED_ROUTINE") as caught:
        spark.catalog.getFunction("nope_fn")
    message = str(caught.value)
    assert "Cannot resolve routine `nope_fn` on search path" in message
    assert "[`system`.`builtin`, `system`.`session`, `glue_catalog`.`ns1`]" in message


def test_cache_table_then_is_cached_and_uncache(spark: ReparkSession) -> None:
    """C-004: cacheTable/isCached/uncacheTable on a temp view (oracle cacheTable arm)."""
    catalog = spark.catalog
    assert catalog.isCached("tv1") is False
    assert catalog.cacheTable("tv1") is None
    assert catalog.isCached("tv1") is True
    assert catalog.uncacheTable("tv1") is None
    assert catalog.isCached("tv1") is False
    assert catalog.uncacheTable("tv1") is None
    assert catalog.is_cached("tv1") is False


def test_cache_table_storage_level(spark: ReparkSession) -> None:
    """C-004: cacheTable with a StorageLevel caches (oracle cacheTable_storage_level)."""
    catalog = spark.catalog
    assert catalog.isCached("t1") is False
    catalog.cacheTable("t1", StorageLevel.DISK_ONLY)
    assert catalog.isCached("t1") is True


def test_cache_table_missing_names_raise(spark: ReparkSession) -> None:
    """C-004: missing names raise TABLE_OR_VIEW_NOT_FOUND (oracle *_missing cells)."""
    catalog = spark.catalog
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        catalog.cacheTable("nope_tbl")
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        catalog.isCached("nope_tbl")
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        catalog.uncacheTable("nope_tbl")


def test_cached_table_reads_the_cache_view(spark: ReparkSession) -> None:
    """C-004: a second spark.table read scans the cache, not the live table."""
    catalog = spark.catalog
    assert spark.table("t1").count() == 2
    catalog.cacheTable("t1")
    spark.sql("INSERT INTO glue_catalog.ns1.t1 VALUES (3, 'z')")
    assert spark.table("t1").count() == 2
    catalog.uncacheTable("t1")
    assert spark.table("t1").count() == 3


def test_frame_cache_visible_to_is_cached(spark: ReparkSession) -> None:
    """C-004: spark.table(name).cache() registers for isCached (df_cache_visible_isCached)."""
    catalog = spark.catalog
    assert catalog.isCached("t1") is False
    assert (spark.table("t1").cache(), catalog.isCached("t1"))[1] is True
    frame = spark.table("t1")
    frame.cache()
    assert catalog.isCached("t1") is True
    catalog.uncacheTable("t1")
    assert catalog.isCached("t1") is False
    assert frame.is_cached is False


def test_clear_cache_drops_catalog_tables(spark: ReparkSession) -> None:
    """C-004: clearCache releases catalog-owned cache views too."""
    catalog = spark.catalog
    catalog.cacheTable("t1")
    assert catalog.isCached("t1") is True
    catalog.clearCache()
    assert catalog.isCached("t1") is False


def test_sql_cache_uncache_refresh_todays_refusals(spark: ReparkSession) -> None:
    """C-004/C-006: SQL CACHE/UNCACHE/REFRESH doors refuse today (run 15c owns them)."""
    with pytest.raises(UnsupportedOperationException):
        spark.sql("cache table tv1")
    with pytest.raises(UnsupportedOperationException):
        spark.sql("uncache table tv1")
    with pytest.raises(ParseException):
        spark.sql("refresh table t1")


def test_create_table_iceberg_schema(spark: ReparkSession) -> None:
    """C-005: schema + iceberg source creates an empty table (oracle createTable_parquet)."""
    schema = StructType([StructField("a", IntegerType()), StructField("p", StringType())])
    frame = spark.catalog.createTable("ct1", schema=schema, source="iceberg")
    oracle = _decode(_result("createTable_parquet"))
    assert frame.columns == oracle["columns"]
    assert frame.schema.simpleString() == oracle["schema"]
    assert _decode(frame.collect()) == oracle["rows"]
    assert spark.table("ct1").collect() == []


def test_create_table_return_type_and_default_source(spark: ReparkSession) -> None:
    """C-005: source=None defaults to Iceberg; the return is a DataFrame."""
    frame = spark.catalog.createTable("ct2", schema=StructType([StructField("a", IntegerType())]))
    assert type(frame).__name__ == _decode(_result("createTable_return_type"))
    assert spark.catalog.create_table(
        "ct2b", schema=StructType([StructField("a", IntegerType())])
    ).columns == ["a"]


def test_create_table_description(spark: ReparkSession) -> None:
    """C-005: description lands as the table comment (oracle createTable_description)."""
    spark.catalog.createTable(
        "ct4",
        schema=StructType([StructField("a", IntegerType())]),
        source="iceberg",
        description="hello",
    )
    row = spark.catalog.getTable("ct4")
    assert row.description == _decode(_result("createTable_description"))[3]


def test_create_table_options_become_properties(spark: ReparkSession) -> None:
    """C-005: **options land as table properties (oracle createTable_options arm)."""
    spark.catalog.createTable(
        "ct6",
        schema=StructType([StructField("a", IntegerType())]),
        source="iceberg",
        custom_opt="v",
    )
    rows = spark.sql("DESCRIBE TABLE EXTENDED glue_catalog.ns1.ct6").collect()
    props = [tuple(row) for row in rows if row[0] == "Table Properties"]
    assert props and "custom_opt=v" in props[0][1]


def test_create_table_exists(spark: ReparkSession) -> None:
    """C-005: an existing name raises TABLE_OR_VIEW_ALREADY_EXISTS (createTable_exists)."""
    schema = StructType([StructField("a", IntegerType())])
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_ALREADY_EXISTS"):
        spark.catalog.createTable("t1", schema=schema, source="parquet")


def test_create_table_no_schema_no_path(spark: ReparkSession) -> None:
    """C-005: no schema and no path refuses UNABLE_TO_INFER_SCHEMA."""
    with pytest.raises(AnalysisException, match="UNABLE_TO_INFER_SCHEMA"):
        spark.catalog.createTable("ct5", source="iceberg")


def test_create_table_non_iceberg_source_refuses(spark: ReparkSession) -> None:
    """C-005: a non-Iceberg source refuses like saveAsTable (EX-IO-6)."""
    schema = StructType([StructField("a", IntegerType())])
    with pytest.raises(PySparkValueError, match="iceberg"):
        spark.catalog.createTable("ct1", schema=schema, source="parquet")
    with pytest.raises(PySparkValueError, match="iceberg"):
        spark.catalog.createTable("ct6", schema=schema, source="csv", header="true")


def test_create_table_path_refuses(spark: ReparkSession) -> None:
    """C-005: an external path refuses like saveAsTable outside Iceberg (EX-IO-6)."""
    with pytest.raises(PySparkValueError, match="iceberg"):
        spark.catalog.createTable("ct3", path="/tmp/ct3path")


def test_create_external_table_warns_and_delegates(spark: ReparkSession) -> None:
    """C-005: createExternalTable warns FutureWarning then delegates (oracle cell)."""
    with pytest.warns(FutureWarning, match="createExternalTable is deprecated since Spark 2.2"):
        frame = spark.catalog.createExternalTable(
            "cet1",
            schema=StructType([StructField("a", IntegerType())]),
            source="iceberg",
        )
    assert frame.columns == ["a"]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        spark.catalog.create_external_table(
            "cet2",
            schema=StructType([StructField("a", IntegerType())]),
        )
    assert any(item.category is FutureWarning for item in caught)


def test_drop_global_temp_view_missing(spark: ReparkSession) -> None:
    """C-006: dropGlobalTempView answers False (EX-DF-2; oracle *_missing)."""
    assert spark.catalog.dropGlobalTempView("nope_gv") is False
    assert spark.catalog.drop_global_temp_view("nope_gv") is False


def test_recover_partitions(spark: ReparkSession) -> None:
    """C-006: Iceberg tables answer None, partitioned or not (CAT-RECOVER-1)."""
    assert spark.catalog.recoverPartitions("pt1") is None
    assert spark.catalog.recover_partitions("pt1") is None
    assert spark.catalog.recoverPartitions("t1") is None


def test_recover_partitions_view(spark: ReparkSession) -> None:
    """C-006: a view raises EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE (oracle cell)."""
    oracle = _error("recoverPartitions_view")
    assert oracle["condition"] == "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE"
    with pytest.raises(AnalysisException, match="EXPECT_TABLE_NOT_VIEW") as caught:
        spark.catalog.recoverPartitions("tv1")
    assert "'recoverPartitions()' expects a table but `tv1` is a view." in str(caught.value)


def test_recover_partitions_missing(spark: ReparkSession) -> None:
    """C-006: a missing name raises TABLE_OR_VIEW_NOT_FOUND."""
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.catalog.recoverPartitions("nope_tbl")


def test_refresh_table_roundtrip(spark: ReparkSession, tmp_path: Path) -> None:
    """C-006: refreshTable republishes out-of-band creates (oracle refreshTable*)."""
    warehouse = str(tmp_path)
    spark._testing_oob_create_table("glue_catalog", "ns1", "oob_t", warehouse)
    with pytest.raises(AnalysisException):
        spark.table("oob_t").collect()
    assert spark.catalog.refreshTable("oob_t") is None
    assert spark.table("oob_t").collect() == []
    assert spark.catalog.refresh_table("t1") is None


def test_refresh_table_view_and_missing(spark: ReparkSession) -> None:
    """C-006: a view is a no-op; a missing name raises (oracle refreshTable_*)."""
    assert spark.catalog.refreshTable("tv1") is None
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.catalog.refreshTable("nope_tbl")


def test_refresh_by_path(spark: ReparkSession) -> None:
    """C-006: refreshByPath answers None for any path (oracle refreshByPath*)."""
    assert spark.catalog.refreshByPath("/tmp/whatever") is None
    assert spark.catalog.refreshByPath("/nonexistent/zz") is None
    assert spark.catalog.refresh_by_path("/nonexistent/zz") is None


def test_catalog_surface_records_type(spark: ReparkSession) -> None:
    """C-007: the facade exposes the Spark-shaped record types."""
    assert Catalog is not None
    assert list(Table._fields) == _decode(_result("getTable_fields"))
    assert list(catalog_module.Column._fields) == _decode(_result("column_fields"))
    assert (
        list(catalog_module.Function._fields)
        == _decode(_result("listFunctions_len_type"))["fields"]
    )

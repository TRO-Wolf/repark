"""Every statement shape dbt emits, measured against the RePark SQL door.

pins: dbt-1-adapter/C-001
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

import pytest
from _sql_harden_cutover_run import _FCT_SQL, _seed_gold_sql, make_names

CATALOG = "ice"
NAMESPACE = "gold"
STEM = "silver"
PROPERTIES = "'format-version' = '2'"


class Shape(NamedTuple):
    """One dbt-emitted statement shape and the verdict the SQL door gives it."""

    shape_id: str
    macro: str
    statement: str
    refusal: str | None


def _names() -> dict[str, str]:
    """Fully qualified names of the silver fixture the gold models read."""
    return make_names(CATALOG, STEM, True, namespace=NAMESPACE)


def _fact_select() -> str:
    """The S6 gold fact model body, single-homed in the cutover program."""
    names = _names()
    return _FCT_SQL.format(
        survey=names["survey"],
        visit=names["visit"],
        provider=names["provider"],
        appointment=names["appointment"],
        dates=names["dates"],
    )


def _test_wrapper(body: str) -> str:
    """dbt's ``default__get_test_sql`` wrapper around one generic-test body."""
    return (
        "select\n"
        "      count(*) as failures,\n"
        "      count(*) != 0 as should_warn,\n"
        "      count(*) != 0 as should_error\n"
        "    from (\n"
        f"      {body}\n"
        "    ) dbt_internal_test"
    )


def _served() -> tuple[Shape, ...]:
    """Statement shapes the SQL door serves, in the order dbt emits them."""
    fact = f"{CATALOG}.{NAMESPACE}.gold_fct"
    return (
        Shape(
            "S-CREATE-NS",
            "repark__create_schema",
            f"create namespace if not exists {CATALOG}.{NAMESPACE}",
            None,
        ),
        Shape("S-SHOW-NS", "repark__list_schemas", f"show namespaces in {CATALOG}", None),
        Shape(
            "S-CTAS",
            "spark__create_table_as",
            f"create or replace table {fact}\n"
            "      using iceberg\n"
            f"      tblproperties ({PROPERTIES})\n"
            "      as\n"
            f"      {_fact_select()}",
            None,
        ),
        Shape(
            "S-CTAS-REPLACE",
            "spark__create_table_as",
            f"create or replace table {fact}\n"
            "      using iceberg\n"
            f"      tblproperties ({PROPERTIES})\n"
            "      as\n"
            f"      {_fact_select()}",
            None,
        ),
        Shape(
            "S-TEST-UNIQUE",
            "default__test_unique",
            _test_wrapper(
                "select\n    survey_id as unique_field,\n    count(*) as n_records\n"
                f"from {fact}\nwhere survey_id is not null\n"
                "group by survey_id\nhaving count(*) > 1\n"
            ),
            None,
        ),
        Shape(
            "S-TEST-NOT-NULL",
            "default__test_not_null",
            _test_wrapper(f"select clinic_id\nfrom {fact}\nwhere clinic_id is null\n"),
            None,
        ),
        Shape(
            "S-RENAME",
            "spark__rename_relation",
            f"alter table {fact} rename to {CATALOG}.{NAMESPACE}.gold_fct_renamed",
            None,
        ),
        Shape(
            "S-DROP",
            "spark__drop_relation",
            f"drop table if exists {CATALOG}.{NAMESPACE}.gold_fct_renamed",
            None,
        ),
        Shape(
            "S-DROP-ABSENT",
            "spark__drop_relation",
            f"drop table if exists {CATALOG}.{NAMESPACE}.never_created",
            None,
        ),
    )


def _refused() -> tuple[Shape, ...]:
    """Statement shapes the SQL door refuses, with the message it refuses with."""
    fact = f"{CATALOG}.{NAMESPACE}.{STEM}_survey"
    return (
        Shape(
            "R-SHOW-DATABASES",
            "spark__list_schemas",
            "show databases",
            "SHOW NAMESPACES requires an explicit catalog",
        ),
        Shape(
            "R-CREATE-SCHEMA-ONE-PART",
            "spark__create_schema",
            f"create schema if not exists {NAMESPACE}",
            "expected a two-part `catalog.namespace` name",
        ),
        Shape(
            "R-SHOW-TABLE-EXTENDED",
            "spark__list_relations_without_caching",
            f"show table extended in {NAMESPACE} like '*'",
            "SHOW [VARIABLE] is not supported unless information_schema is enabled",
        ),
        Shape(
            "R-SHOW-TABLES",
            "list_relations_show_tables_without_caching",
            f"show tables in {NAMESPACE} like '*'",
            "SHOW TABLES IN not supported",
        ),
        Shape(
            "R-DESCRIBE-TWO-PART",
            "describe_table_extended_without_caching",
            f"describe extended {NAMESPACE}.{STEM}_survey",
            f"table 'datafusion.{NAMESPACE}.{STEM}_survey' not found",
        ),
        Shape(
            "R-SHOW-TBLPROPERTIES",
            "fetch_tbl_properties",
            f"show tblproperties {fact}",
            "SHOW [VARIABLE] is not supported unless information_schema is enabled",
        ),
        Shape(
            "R-CREATE-VIEW",
            "spark__create_view_as",
            f"create or replace view {CATALOG}.{NAMESPACE}.v_fct as select * from {fact}",
            "register_table does not support tables with data",
        ),
        Shape(
            "R-CREATE-TEMPORARY-VIEW",
            "spark__create_temporary_view",
            f"create or replace temporary view dbt_tmp as select * from {fact}",
            "Temporary views not supported",
        ),
        Shape(
            "R-RENAME-TWO-PART",
            "spark__rename_relation",
            f"alter table {NAMESPACE}.{STEM}_survey rename to {NAMESPACE}.renamed",
            "ALTER TABLE expects a three-part `catalog.namespace.table` name",
        ),
        Shape(
            "R-COLUMN-COMMENT",
            "spark__alter_column_comment",
            f"alter table {fact} alter column survey_id comment 'x'",
            "ALTER COLUMN … COMMENT is not supported yet via SQL",
        ),
    )


@pytest.fixture(scope="module")
def seeded_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    """A memory-catalog session holding the S6 silver fixture the gold models join."""
    from repark import ReparkSession

    warehouse: Path = tmp_path_factory.mktemp("dbt1-statement-surface")
    session = ReparkSession.builder.appName("dbt-1-statement-surface").getOrCreate()
    session.register_memory_catalog(CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE} LOCATION '{warehouse / NAMESPACE}'")
    for statement in _seed_gold_sql(_names(), PROPERTIES):
        session.sql(statement)
    try:
        yield session
    finally:
        session.stop()


@pytest.mark.parametrize("shape", _served(), ids=lambda shape: shape.shape_id)
def test_served_shapes_run(seeded_session: Any, shape: Shape) -> None:
    """Each served shape executes and returns an Arrow result."""
    assert seeded_session.sql(shape.statement).to_arrow() is not None


@pytest.mark.parametrize("shape", _refused(), ids=lambda shape: shape.shape_id)
def test_refused_shapes_fail_loud(seeded_session: Any, shape: Shape) -> None:
    """Each refused shape raises, and the message is the one the ledger records."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException) as caught:
        seeded_session.sql(shape.statement).to_arrow()
    assert shape.refusal is not None
    assert shape.refusal in str(caught.value)


def test_describe_extended_answers_arrow_type_spellings(seeded_session: Any) -> None:
    """Three-part DESCRIBE runs, but its columns are not Spark's describe-extended shape."""
    described = seeded_session.sql(
        f"describe extended {CATALOG}.{NAMESPACE}.{STEM}_survey"
    ).to_arrow()
    assert described.column_names == ["column_name", "data_type", "is_nullable"]
    rows = described.to_pylist()
    assert rows[0]["column_name"] == "survey_id"
    assert rows[0]["data_type"] == "Utf8"


def test_facade_schema_answers_spark_type_spellings(seeded_session: Any) -> None:
    """The facade schema is the column source the adapter uses instead of DESCRIBE."""
    fields = seeded_session.table(f"{CATALOG}.{NAMESPACE}.{STEM}_survey").schema.fields
    measured = [(field.name, field.dataType.simpleString()) for field in fields]
    assert measured[0] == ("survey_id", "string")
    assert measured[2] == ("gene_prissy_score", "int")

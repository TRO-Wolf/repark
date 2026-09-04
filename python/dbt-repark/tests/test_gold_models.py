"""The two gold models and their ten test blocks, built by dbt on a memory catalog.

pins: dbt-1-adapter/C-002, dbt-1-adapter/C-003, dbt-1-adapter/C-004
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from _sql_harden_cutover_run import _AGG_SQL, _FCT_SQL, _seed_gold_sql, make_names

CATALOG = "ice"
NAMESPACE = "gold"
STEM = "silver"
PROPERTIES = "'format-version' = '2'"
PROFILE = "repark_gold"

SILVER_TABLES = ("survey", "visit", "provider", "appointment", "dates")

TBLPROPERTIES = {
    "format-version": "2",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "write.target-file-size-bytes": "268435456",
}

FCT_ROWS = [
    {"survey_id": "s1", "clinic_id": 10, "wait_time_minutes": 15},
    {"survey_id": "s2", "clinic_id": 20, "wait_time_minutes": 40},
]

AGG_ROWS = [
    {
        "clinic_id": 10,
        "day_of_week": "Thursday",
        "avg_gene_prissy_score": 8.0,
        "avg_experience_score": 9.0,
        "avg_wait_time_minutes": 15.0,
        "num_surveys": 1,
        "num_patient_visits": 1,
    },
    {
        "clinic_id": 20,
        "day_of_week": "Friday",
        "avg_gene_prissy_score": 6.0,
        "avg_experience_score": 7.0,
        "avg_wait_time_minutes": 40.0,
        "num_surveys": 1,
        "num_patient_visits": 1,
    },
]


def _source(table: str) -> str:
    """The dbt ``source()`` call for one silver table."""
    return "{{ source('silver', '" + f"{STEM}_{table}" + "') }}"


def _fact_model() -> str:
    """The S6 gold fact model, with its five silver tables resolved through dbt sources."""
    return _FCT_SQL.format(**{name: _source(name) for name in SILVER_TABLES})


def _aggregate_model() -> str:
    """The S6 gold aggregate model, reading the fact model through ``ref()``."""
    return _AGG_SQL.format(fct="{{ ref('gold_fct') }}", dates=_source("dates"))


def _schema_yaml() -> dict[str, Any]:
    """The five sources and the ten generic test blocks the gold stage runs."""
    return {
        "version": 2,
        "sources": [
            {
                "name": "silver",
                "database": CATALOG,
                "schema": NAMESPACE,
                "tables": [{"name": f"{STEM}_{name}"} for name in SILVER_TABLES],
            }
        ],
        "models": [
            {
                "name": "gold_fct",
                "columns": [
                    {"name": "survey_id", "tests": ["unique", "not_null"]},
                    {"name": "clinic_id", "tests": ["not_null"]},
                    {"name": "calendar_date", "tests": ["not_null"]},
                    {
                        "name": "patient_visit_id",
                        "tests": [
                            {"accepted_values": {"values": ["v1", "v2"], "quote": True}},
                        ],
                    },
                ],
            },
            {
                "name": "gold_agg",
                "columns": [
                    {
                        "name": "clinic_id",
                        "tests": [
                            "unique",
                            "not_null",
                            {"relationships": {"to": "ref('gold_fct')", "field": "clinic_id"}},
                        ],
                    },
                    {
                        "name": "day_of_week",
                        "tests": [
                            "not_null",
                            {
                                "accepted_values": {
                                    "values": ["Thursday", "Friday"],
                                    "quote": True,
                                }
                            },
                        ],
                    },
                ],
            },
        ],
    }


def _write_project(root: Path, warehouse: Path) -> None:
    """Write a dbt project and profile that build the two gold models on a memory catalog."""
    models = root / "models"
    models.mkdir(parents=True, exist_ok=True)
    (models / "gold_fct.sql").write_text(_fact_model(), encoding="utf-8")
    (models / "gold_agg.sql").write_text(_aggregate_model(), encoding="utf-8")
    (models / "schema.yml").write_text(yaml.safe_dump(_schema_yaml()), encoding="utf-8")
    (root / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": PROFILE,
                "version": "1.0",
                "config-version": 2,
                "profile": PROFILE,
                "model-paths": ["models"],
                "models": {
                    PROFILE: {
                        "+materialized": "table",
                        "+file_format": "iceberg",
                        "+tblproperties": TBLPROPERTIES,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (root / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                PROFILE: {
                    "target": "local",
                    "outputs": {
                        "local": {
                            "type": "repark",
                            "catalog": CATALOG,
                            "schema": NAMESPACE,
                            "warehouse": str(warehouse),
                            "threads": 4,
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )


def _seed_silver(warehouse: Path) -> None:
    """Create the S6 silver fixture in the session dbt will attach to.

    A memory catalog lives in the session that registered it, so the fixture must not stop
    the session it seeds: dbt's ``getOrCreate`` attaches to this one and finds the tables.
    """
    from repark import ReparkSession

    session = ReparkSession.builder.appName("dbt-1-seed").getOrCreate()
    session.register_memory_catalog(CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE} LOCATION '{warehouse / NAMESPACE}'")
    names = make_names(CATALOG, STEM, True, namespace=NAMESPACE)
    for statement in _seed_gold_sql(names, PROPERTIES):
        session.sql(statement)


def _invoke(command: list[str], root: Path) -> Any:
    """Run one dbt command in the project directory and return its result object."""
    from dbt.cli.main import dbtRunner

    return dbtRunner().invoke([*command, "--project-dir", str(root), "--profiles-dir", str(root)])


def _read(warehouse: Path, sql: str) -> list[dict[str, Any]]:
    """Read rows back from the built tables. The adapter's session stays live for the test."""
    from repark import ReparkSession

    session = ReparkSession.builder.appName("dbt-1-read").getOrCreate()
    if not any(entry.name == CATALOG for entry in session.catalog.list_catalogs()):
        session.register_memory_catalog(CATALOG, warehouse)
    return session.sql(sql).to_arrow().to_pylist()


@pytest.fixture
def project(tmp_path: Path) -> Iterator[tuple[Path, Path]]:
    """A seeded warehouse and a written dbt project, with the session released after."""
    from dbt.adapters.repark import release_session

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    root = tmp_path / "project"
    root.mkdir()
    _seed_silver(warehouse)
    _write_project(root, warehouse)
    try:
        yield root, warehouse
    finally:
        release_session()
        _stop_any_session()


def _stop_any_session() -> None:
    """Stop a session a test built outside the adapter, so the next test starts clean."""
    from repark import ReparkSession

    lingering = ReparkSession.getActiveSession()
    if lingering is not None:
        lingering.stop()


def test_dbt_run_builds_both_gold_models(project: tuple[Path, Path]) -> None:
    """``dbt run`` builds gold_fct and gold_agg, and they answer the S6 measured rows."""
    root, warehouse = project
    built = _invoke(["run"], root)
    assert built.success, _failures(built)
    assert sorted(node.node.name for node in built.result) == ["gold_agg", "gold_fct"]

    fact = _read(
        warehouse,
        f"select survey_id, clinic_id, wait_time_minutes "
        f"from {CATALOG}.{NAMESPACE}.gold_fct order by survey_id",
    )
    assert fact == FCT_ROWS

    aggregate = _read(
        warehouse,
        "select clinic_id, day_of_week, avg_gene_prissy_score, avg_experience_score, "
        "avg_wait_time_minutes, num_surveys, num_patient_visits "
        f"from {CATALOG}.{NAMESPACE}.gold_agg order by clinic_id",
    )
    assert aggregate == AGG_ROWS


def test_dbt_test_runs_ten_blocks_green(project: tuple[Path, Path]) -> None:
    """``dbt test`` runs the ten generic test blocks and every one passes."""
    root, _ = project
    assert _invoke(["run"], root).success
    tested = _invoke(["test"], root)
    assert tested.success, _failures(tested)
    assert len(tested.result) == 10
    assert {node.status for node in tested.result} == {"pass"}


def test_dbt_run_is_idempotent(project: tuple[Path, Path]) -> None:
    """A second run replaces both tables in place and keeps the same rows."""
    root, warehouse = project
    assert _invoke(["run"], root).success
    assert _invoke(["run"], root).success
    fact = _read(
        warehouse,
        f"select survey_id, clinic_id, wait_time_minutes "
        f"from {CATALOG}.{NAMESPACE}.gold_fct order by survey_id",
    )
    assert fact == FCT_ROWS


def test_docs_generate_reads_relations_and_columns(project: tuple[Path, Path]) -> None:
    """``dbt docs generate`` exercises the two overridden catalog paths.

    ``list_relations_without_caching`` and ``get_columns_in_relation`` are the surfaces that
    replace ``SHOW TABLES IN`` and ``DESCRIBE EXTENDED``. The catalog artifact is where their
    output is observable, and it must carry Spark type spellings, not Arrow ones.
    """
    root, _ = project
    assert _invoke(["run"], root).success
    assert _invoke(["docs", "generate"], root).success

    catalog = json.loads((root / "target" / "catalog.json").read_text(encoding="utf-8"))
    node = catalog["nodes"][f"model.{PROFILE}.gold_fct"]
    assert node["metadata"]["database"] == CATALOG
    assert node["metadata"]["schema"] == NAMESPACE
    columns = {name: entry["type"] for name, entry in node["columns"].items()}
    assert columns["survey_id"] == "string"
    assert columns["clinic_id"] == "int"
    assert columns["calendar_date"] == "date"
    assert columns["appointment_datetime"] == "timestamp"

    sources = catalog["sources"]
    assert f"source.{PROFILE}.silver.{STEM}_survey" in sources


def test_built_tables_carry_the_configured_iceberg_properties(
    project: tuple[Path, Path],
) -> None:
    """The models are Iceberg tables and the project's TBLPROPERTIES reach the metadata.

    A RePark catalog makes every table Iceberg, so ``using iceberg`` alone proves nothing.
    The write properties the cutover pipeline configures are what must arrive.
    """
    root, warehouse = project
    assert _invoke(["run"], root).success

    for table in ("gold_fct", "gold_agg"):
        snapshots = _read(
            warehouse, f"select operation from {CATALOG}.{NAMESPACE}.{table}.snapshots"
        )
        assert snapshots == [{"operation": "append"}]
        pointers = sorted(warehouse.glob(f"*/{table}/metadata/*.metadata.json"))
        assert pointers, f"{table} has no Iceberg metadata"
        document = json.loads(pointers[-1].read_text(encoding="utf-8"))
        assert document["format-version"] == 2
        properties = document.get("properties", {})
        assert properties["write.merge.mode"] == "merge-on-read"
        assert properties["write.target-file-size-bytes"] == "268435456"


def test_full_refresh_rebuilds_both_models(project: tuple[Path, Path]) -> None:
    """``--full-refresh`` is accepted and rebuilds the same rows."""
    root, warehouse = project
    assert _invoke(["run"], root).success
    assert _invoke(["run", "--full-refresh"], root).success
    fact = _read(
        warehouse,
        f"select survey_id, clinic_id, wait_time_minutes "
        f"from {CATALOG}.{NAMESPACE}.gold_fct order by survey_id",
    )
    assert fact == FCT_ROWS


def test_relation_documentation_refuses(project: tuple[Path, Path]) -> None:
    """``persist_docs.relation`` refuses: RePark has no CREATE TABLE ... COMMENT on a CTAS.

    Without the override the emitted CTAS reaches the parser, which reports the failure at
    ``using`` rather than at the comment clause (registry DBT-RELCOMMENT-1). The compile-time
    refusal is what keeps that misleading diagnostic away from the user.
    """
    root, _ = project
    _prepend_config(root, "gold_fct", "config(persist_docs={'relation': true})")
    schema_path = root / "models" / "schema.yml"
    document = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    document["models"][0]["description"] = "gold fact, one row per survey"
    schema_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    built = _invoke(["run", "--select", "gold_fct"], root)
    assert not built.success
    assert "DBT-RELCOMMENT-1" in _failures(built)


def test_partition_by_builds(project: tuple[Path, Path]) -> None:
    """``partition_by`` is the one placement clause the SQL door serves on an Iceberg CTAS."""
    root, warehouse = project
    _prepend_config(root, "gold_fct", "config(partition_by='clinic_id')")
    built = _invoke(["run", "--select", "gold_fct"], root)
    assert built.success, _failures(built)
    fact = _read(
        warehouse,
        f"select survey_id, clinic_id, wait_time_minutes "
        f"from {CATALOG}.{NAMESPACE}.gold_fct order by survey_id",
    )
    assert fact == FCT_ROWS


@pytest.mark.parametrize(
    ("setting", "row"),
    [
        ("config(location_root='/tmp/elsewhere')", "DBT-CTASCLAUSE-1"),
        ("config(options={'compression': 'zstd'})", "DBT-CTASCLAUSE-1"),
        ("config(clustered_by='clinic_id', buckets=4)", "DBT-CTASCLAUSE-1"),
    ],
)
def test_unsupported_ctas_clauses_refuse(
    project: tuple[Path, Path], setting: str, row: str
) -> None:
    """Each unsupported placement clause refuses at compile time, naming its registry row.

    Without these overrides the clause reaches the parser, which reports the failure at
    ``using`` rather than at the clause that failed.
    """
    root, _ = project
    _prepend_config(root, "gold_fct", setting)
    built = _invoke(["run", "--select", "gold_fct"], root)
    assert not built.success
    assert row in _failures(built)


def _prepend_config(root: Path, model: str, setting: str) -> None:
    """Put one ``{{ config(...) }}`` call at the top of a model file."""
    path = root / "models" / f"{model}.sql"
    path.write_text("{{ " + setting + " }}\n" + path.read_text(encoding="utf-8"), encoding="utf-8")


def test_shared_session_serves_concurrent_statements(project: tuple[Path, Path]) -> None:
    """The one shared session answers concurrent statements, which is what `threads` rests on."""
    from concurrent.futures import ThreadPoolExecutor

    root, warehouse = project
    assert _invoke(["run"], root).success
    with ThreadPoolExecutor(max_workers=8) as pool:
        counts = list(
            pool.map(
                lambda _: _read(
                    warehouse, f"select count(*) as c from {CATALOG}.{NAMESPACE}.gold_fct"
                ),
                range(16),
            )
        )
    assert counts == [[{"c": 2}]] * 16


def test_view_materialization_refuses(project: tuple[Path, Path]) -> None:
    """A view model fails with the message that names the registry row."""
    root, _ = project
    _prepend_config(root, "gold_fct", "config(materialized='view')")
    built = _invoke(["run", "--select", "gold_fct"], root)
    assert not built.success
    assert "DBT-VIEW-1" in _failures(built)


def test_incremental_materialization_refuses(project: tuple[Path, Path]) -> None:
    """An incremental model fails with the message that names the registry row."""
    root, _ = project
    _prepend_config(root, "gold_fct", "config(materialized='incremental')")
    built = _invoke(["run", "--select", "gold_fct"], root)
    assert not built.success
    assert "DBT-TEMPVIEW-1" in _failures(built)


def test_column_documentation_refuses(project: tuple[Path, Path]) -> None:
    """persist_docs.columns fails with the message that names the registry row."""
    root, _ = project
    _prepend_config(root, "gold_fct", "config(persist_docs={'columns': true})")
    built = _invoke(["run", "--select", "gold_fct"], root)
    assert not built.success
    assert "DBT-COLCOMMENT-1" in _failures(built)


def test_a_second_profile_refuses_rather_than_reusing_the_session(tmp_path: Path) -> None:
    """One process, one session: a profile with another catalog is refused, never reused.

    ``getOrCreate`` answers the live session whatever the builder says, so silently attaching a
    second profile would run its models against the first profile's catalog.
    """
    from dbt.adapters.repark import ReparkCredentials, acquire_session, release_session
    from dbt_common.exceptions import DbtRuntimeError

    first = ReparkCredentials(database=CATALOG, schema=NAMESPACE, warehouse=str(tmp_path / "one"))
    (tmp_path / "one").mkdir()
    second = ReparkCredentials(database="other", schema=NAMESPACE, warehouse=str(tmp_path / "two"))
    (tmp_path / "two").mkdir()
    try:
        acquire_session(first)
        with pytest.raises(DbtRuntimeError) as caught:
            acquire_session(second)
        assert "already live for a different profile" in str(caught.value)
    finally:
        release_session()


def test_relations_are_three_part(project: tuple[Path, Path]) -> None:
    """Every relation dbt renders carries the catalog part the SQL door requires."""
    from dbt.adapters.repark import ReparkRelation

    relation = ReparkRelation.create(database=CATALOG, schema=NAMESPACE, identifier="gold_fct")
    assert relation.render() == f"{CATALOG}.{NAMESPACE}.gold_fct"


def _failures(result: Any) -> str:
    """Every failure message in a dbt result, as one searchable string."""
    if result.exception is not None:
        return str(result.exception)
    rows = result.result if result.result is not None else []
    return json.dumps([str(getattr(node, "message", node)) for node in rows])

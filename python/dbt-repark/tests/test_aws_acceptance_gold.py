"""The gold stage on real Glue, gated on ``REPARK_AWS_ACCEPTANCE`` like the silver module."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    ACCEPTANCE_TABLE_PREFIX,
    GLUE_WAREHOUSE,
    SILVER_CATALOG,
    acceptance_namespace_location,
    assert_glue_scratch_namespace_location,
    assert_real_buckets_configured,
    glue_catalog_config,
)
from _sql_harden_cutover_run import _seed_gold_sql, make_names
from test_gold_models import (
    AGG_ROWS,
    FCT_ROWS,
    PROPERTIES,
    _invoke,
    _write_project,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason=(
        "real-AWS acceptance harness: set REPARK_AWS_ACCEPTANCE=1 to run the gold stage "
        "against Glue. Cutover step C6."
    ),
)


def _glue_session() -> Any:
    """A RePark session over the real Glue catalog, in the publish job's configuration."""
    from repark import ReparkSession

    builder = ReparkSession.builder.appName("dbt-1-gold-acceptance")
    for key, value in glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _ensure_scratch_namespace(session: Any) -> None:
    """Create the scratch namespace if it is missing. Production is never touched."""
    try:
        session.create_namespace(
            SILVER_CATALOG,
            ACCEPTANCE_NAMESPACE,
            location=acceptance_namespace_location(GLUE_WAREHOUSE),
        )
    except RuntimeError as error:
        if "exist" not in str(error).lower():
            raise
    assert_glue_scratch_namespace_location(session, GLUE_WAREHOUSE)


def _snapshot_count(session: Any, table: str) -> int:
    return session.sql(f"select snapshot_id from {table}.snapshots").to_arrow().num_rows


@pytest.fixture
def glue_project(tmp_path: Path) -> Iterator[tuple[Path, str]]:
    """A dbt project on a per-run stem pointed at the Glue scratch namespace, S6-seeded."""
    from dbt.adapters.repark import release_session

    assert_real_buckets_configured()
    session = _glue_session()
    _ensure_scratch_namespace(session)
    stem = f"{ACCEPTANCE_TABLE_PREFIX}dbt1_{uuid.uuid4().hex[:8]}"
    names = make_names(SILVER_CATALOG, stem, True, namespace=ACCEPTANCE_NAMESPACE)
    for statement in _seed_gold_sql(names, PROPERTIES):
        session.sql(statement)

    root = tmp_path / "project"
    root.mkdir()
    _write_project(root, tmp_path / "unused")
    _repoint_profile(root, stem)
    try:
        yield root, stem
    finally:
        release_session()


def _repoint_profile(root: Path, stem: str) -> None:
    """Swap the memory-catalog profile for the Glue one, and the sources for the run's stem."""
    import yaml

    profiles = yaml.safe_load((root / "profiles.yml").read_text(encoding="utf-8"))
    output = next(iter(profiles.values()))["outputs"]["local"]
    output.pop("warehouse")
    output["catalog"] = SILVER_CATALOG
    output["schema"] = ACCEPTANCE_NAMESPACE
    output["catalog_properties"] = glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE)
    (root / "profiles.yml").write_text(yaml.safe_dump(profiles), encoding="utf-8")

    schema_path = root / "models" / "schema.yml"
    document = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    for model in document["models"]:
        model["config"] = {"alias": f"{stem}_{model['name']}"}
    source = document["sources"][0]
    source["database"] = SILVER_CATALOG
    source["schema"] = ACCEPTANCE_NAMESPACE
    for table in source["tables"]:
        table["name"] = table["name"].replace("silver_", f"{stem}_", 1)
    schema_path.write_text(yaml.safe_dump(document), encoding="utf-8")

    for model in ("gold_fct.sql", "gold_agg.sql"):
        path = root / "models" / model
        path.write_text(
            path.read_text(encoding="utf-8").replace("'silver_", f"'{stem}_"),
            encoding="utf-8",
        )


def test_gold_stage_on_glue(glue_project: tuple[Path, str]) -> None:
    """Two ``dbt run`` passes then ``dbt test`` on Glue: in-place replace, S6 rows, ten blocks."""
    root, stem = glue_project
    built = _invoke(["run"], root)
    assert built.success, json.dumps([str(node.message) for node in built.result])

    session = _glue_session()
    fact_table = f"{SILVER_CATALOG}.{ACCEPTANCE_NAMESPACE}.{stem}_gold_fct"
    aggregate_table = f"{SILVER_CATALOG}.{ACCEPTANCE_NAMESPACE}.{stem}_gold_agg"
    fact = session.sql(
        f"select survey_id, clinic_id, wait_time_minutes from {fact_table} order by survey_id"
    ).to_arrow()
    assert fact.to_pylist() == FCT_ROWS
    aggregate = session.sql(
        "select clinic_id, day_of_week, avg_gene_prissy_score, avg_experience_score, "
        "avg_wait_time_minutes, num_surveys, num_patient_visits "
        f"from {aggregate_table} order by clinic_id"
    ).to_arrow()
    assert aggregate.to_pylist() == AGG_ROWS
    fct_snapshots_after_first = _snapshot_count(session, fact_table)
    agg_snapshots_after_first = _snapshot_count(session, aggregate_table)

    rebuilt = _invoke(["run"], root)
    assert rebuilt.success, json.dumps([str(node.message) for node in rebuilt.result])
    fact = session.sql(
        f"select survey_id, clinic_id, wait_time_minutes from {fact_table} order by survey_id"
    ).to_arrow()
    assert fact.to_pylist() == FCT_ROWS
    aggregate = session.sql(
        "select clinic_id, day_of_week, avg_gene_prissy_score, avg_experience_score, "
        "avg_wait_time_minutes, num_surveys, num_patient_visits "
        f"from {aggregate_table} order by clinic_id"
    ).to_arrow()
    assert aggregate.to_pylist() == AGG_ROWS
    assert _snapshot_count(session, fact_table) == fct_snapshots_after_first + 1
    assert _snapshot_count(session, aggregate_table) == agg_snapshots_after_first + 1

    tested = _invoke(["test"], root)
    assert tested.success, json.dumps([str(node.message) for node in tested.result])
    assert len(tested.result) == 10

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    GLUE_WAREHOUSE,
    S3TABLES_CATALOG,
    SILVER_CATALOG,
    assert_real_buckets_configured,
    glue_catalog_config,
    mask_account_ids,
    run_table_prefix,
    s3tables_catalog_config,
)
from _acceptance_cross_release import (
    INDEX_WHEEL,
    KINDS,
    LOCAL_BUILD,
    PREVIOUS_PYTHON_ENV,
    PREVIOUS_RELEASE,
    Cell,
    Side,
    cross_cell,
    format_cells,
    mixed_cell,
)
from _acceptance_v3 import (
    S3T_V3_REFUSED_AT_CREATE,
    classify_v3_create_outcome,
    format_v3_refusal_record,
)

import repark

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason="real-AWS integrity leg X: set REPARK_AWS_ACCEPTANCE=1 to run (the AWS workflow only)",
)

CATALOGS = ("glue", "s3tables")
FORMATS = (2, 3)
DIRECTIONS = ("previous_writes", "head_writes")


@pytest.fixture(scope="module")
def sides() -> tuple[Side, Side]:
    python = os.environ.get(PREVIOUS_PYTHON_ENV)
    if not python:
        pytest.fail(
            f"{PREVIOUS_PYTHON_ENV} is unset with REPARK_AWS_ACCEPTANCE=1: leg X needs the "
            f"hash-checked repark=={PREVIOUS_RELEASE} interpreter and never skips without it"
        )
    return Side("previous", python, PREVIOUS_RELEASE, INDEX_WHEEL), Side(
        "head", sys.executable, repark.__version__, LOCAL_BUILD
    )


def catalog_spec(catalog: str) -> dict[str, Any]:
    assert_real_buckets_configured()
    if catalog == "glue":
        return {
            "catalog": SILVER_CATALOG,
            "catalog_config": glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE),
        }
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip("TABLE_BUCKET_ARN is not set; the S3 Tables cells need the table bucket")
    return {
        "catalog": S3TABLES_CATALOG,
        "catalog_config": s3tables_catalog_config(S3TABLES_CATALOG, arn),
    }


def ordered(sides: tuple[Side, Side], direction: str) -> tuple[Side, Side]:
    previous, head = sides
    return (previous, head) if direction == "previous_writes" else (head, previous)


def assert_equal_or_v3_refused(
    cell: Cell | None, failure: AssertionError | None, catalog: str, format_version: int
) -> None:
    if failure is not None:
        refused = classify_v3_create_outcome(RuntimeError(str(failure))) == S3T_V3_REFUSED_AT_CREATE
        if catalog == "s3tables" and format_version == 3 and refused:
            print(format_v3_refusal_record(RuntimeError(str(failure))))
            return
        raise AssertionError(mask_account_ids(str(failure))) from None
    assert cell is not None
    print(mask_account_ids(format_cells([cell])))
    assert not cell.differences, mask_account_ids(f"H-X1 {cell.label}: {cell.differences}")


@pytest.mark.parametrize("direction", DIRECTIONS)
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("format_version", FORMATS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_each_release_reads_the_other_releases_table(
    catalog: str,
    format_version: int,
    kind: str,
    direction: str,
    sides: tuple[Side, Side],
    tmp_path: Path,
) -> None:
    writer, reader = ordered(sides, direction)
    spec = {
        **catalog_spec(catalog),
        "namespace": ACCEPTANCE_NAMESPACE,
        "table": f"{run_table_prefix()}xrel_{kind}_v{format_version}_{writer.role}",
        "kind": kind,
        "format": format_version,
    }
    label = f"X-{kind} {catalog} v{format_version} {direction}"
    cell = failure = None
    try:
        cell = cross_cell(writer=writer, reader=reader, spec=spec, workdir=tmp_path, label=label)
    except AssertionError as error:
        failure = error
    assert_equal_or_v3_refused(cell, failure, catalog, format_version)


@pytest.mark.parametrize("direction", DIRECTIONS)
@pytest.mark.parametrize("format_version", FORMATS)
@pytest.mark.parametrize("catalog", CATALOGS)
def test_the_other_release_appends_and_the_writer_reads_it_back(
    catalog: str, format_version: int, direction: str, sides: tuple[Side, Side], tmp_path: Path
) -> None:
    writer, reader = ordered(sides, direction)
    spec = {
        **catalog_spec(catalog),
        "namespace": ACCEPTANCE_NAMESPACE,
        "table": f"{run_table_prefix()}xrel_mixed_v{format_version}_{writer.role}",
        "kind": "plain",
        "format": format_version,
    }
    label = f"X-mixed {catalog} v{format_version} {direction}"
    cell = failure = None
    try:
        cell = mixed_cell(writer=writer, reader=reader, spec=spec, workdir=tmp_path, label=label)
    except AssertionError as error:
        failure = error
    assert_equal_or_v3_refused(cell, failure, catalog, format_version)

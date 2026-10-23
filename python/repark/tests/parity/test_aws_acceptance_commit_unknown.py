from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pyarrow.fs as pafs
import pytest
from _acceptance import (
    ACCEPTANCE_NAMESPACE,
    GLUE_WAREHOUSE,
    S3TABLES_CATALOG,
    SILVER_CATALOG,
    assert_real_buckets_configured,
    glue_catalog_config,
    is_commit_conflict_error,
    mask_account_ids,
    s3tables_catalog_config,
)
from _acceptance_commit_gate import GLUE, S3TABLES, CommitGate, Script, regional_upstream
from _acceptance_commit_resolver import (
    UnresolvedError,
    attempted_snapshot,
    missing_properties,
    stable_snapshot_ids,
)
from _acceptance_sigv4 import Credentials
from _acceptance_v3 import (
    S3T_V3_REFUSED_AT_CREATE,
    V3_ALLOW_CREATE_KEY,
    classify_v3_create_outcome,
    current_metadata_location,
    format_v3_refusal_record,
)

from repark import ReparkSession
from repark.errors import CommitStateUnknownException

pytestmark = pytest.mark.skipif(
    os.environ.get("REPARK_AWS_ACCEPTANCE") != "1",
    reason="real-AWS integrity leg U: set REPARK_AWS_ACCEPTANCE=1 to run (the AWS workflow only)",
)

OPERATION_ID = "engine.operation-id"
SESSION_KEY = "spark.sql.iceberg.snapshot-property.k"
STATUS_CHECK = {
    "commit.status-check.num-retries": "1",
    "commit.status-check.min-wait-ms": "100",
    "commit.status-check.max-wait-ms": "200",
    "commit.status-check.total-timeout-ms": "5000",
}
SOURCE = "(SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS v(id, name)) AS s"
PRE_ROWS = [(1, "a"), (2, "b"), (3, "c")]
INSERT_ROWS = [*PRE_ROWS, (11, "a"), (12, "b"), (13, "c")]
MERGE_ROWS = [(1, "a"), (2, "m"), (3, "c")]
CATALOGS = (GLUE, S3TABLES)
FORMATS = (2, 3)
DOORS = ("plain", "session_key", "merge")


@dataclass(frozen=True)
class CellSpec:
    cell: str
    script: Script | None
    raises: bool
    landed: bool
    commit_requests: int | None


CELLS = (
    CellSpec("U-1", Script.LANDED_READS_OPEN, raises=False, landed=True, commit_requests=1),
    CellSpec("U-2", Script.LANDED_READS_CUT, raises=True, landed=True, commit_requests=1),
    CellSpec("U-3", Script.ABSENT_READS_CUT, raises=True, landed=False, commit_requests=1),
    CellSpec("U-4", Script.ABSENT_READS_OPEN, raises=True, landed=False, commit_requests=1),
    CellSpec("U-5", Script.LANDED_RESENDS_FORWARDED, raises=False, landed=True, commit_requests=1),
    CellSpec("U-5r", Script.LANDED_THROTTLED_RESEND, raises=False, landed=True, commit_requests=2),
    CellSpec("U-C0", Script.FORWARD, raises=False, landed=True, commit_requests=1),
    CellSpec("U-C1", None, raises=False, landed=True, commit_requests=None),
)


@dataclass
class Target:
    service: str
    catalog: str
    config: dict[str, str]
    region: str
    table_bucket_arn: str | None

    def endpoint_key(self) -> str:
        key = "uri" if self.service == GLUE else "endpoint_url"
        return f"spark.sql.catalog.{self.catalog}.{key}"


def region() -> str:
    value = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not value:
        pytest.fail("AWS_REGION is required when REPARK_AWS_ACCEPTANCE=1 (the gate's allow list)")
    return value


def credentials() -> Credentials:
    key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not key or not secret:
        pytest.fail("the gate signs again with the job's environment credentials; none are set")
    return Credentials(key, secret, os.environ.get("AWS_SESSION_TOKEN"))


def target_for(service: str) -> Target:
    assert_real_buckets_configured()
    if service == GLUE:
        return Target(
            GLUE,
            SILVER_CATALOG,
            glue_catalog_config(SILVER_CATALOG, GLUE_WAREHOUSE),
            region(),
            None,
        )
    arn = os.environ.get("TABLE_BUCKET_ARN")
    if not arn:
        pytest.skip("TABLE_BUCKET_ARN is not set; the S3 Tables cells need the table bucket")
    return Target(
        S3TABLES, S3TABLES_CATALOG, s3tables_catalog_config(S3TABLES_CATALOG, arn), region(), arn
    )


@contextmanager
def session(target: Target, extra: dict[str, str]) -> Iterator[ReparkSession]:
    builder = (
        ReparkSession.builder.appName("integrity-leg-u")
        .config("spark.sql.session.timeZone", "UTC")
        .config(V3_ALLOW_CREATE_KEY, "true")
    )
    for key, value in {**target.config, **extra}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def table_of(target: Target, door: str, spec: CellSpec, format_version: int) -> str:
    cell = spec.cell.lower().replace("-", "")
    return f"{target.catalog}.{ACCEPTANCE_NAMESPACE}.testing_cu_{door}_{cell}_v{format_version}"


def statement(door: str, table: str) -> str:
    if door == "merge":
        return (
            f"MERGE INTO {table} AS t USING {SOURCE} ON t.id = s.id "
            "WHEN MATCHED AND s.id = 2 THEN UPDATE SET name = 'm' "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    return f"INSERT INTO {table} SELECT id + 10, name FROM {SOURCE}"


def rows(spark: ReparkSession, table: str) -> list[tuple[int, str]]:
    arrow = spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow()
    return list(zip(arrow.column(0).to_pylist(), arrow.column(1).to_pylist(), strict=True))


def snapshot_ids(spark: ReparkSession, table: str) -> set[int]:
    arrow = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").to_arrow()
    return {int(value) for value in arrow.column(0).to_pylist()}


def read_json(target: Target, location: str) -> dict[str, Any]:
    filesystem, path = pafs.FileSystem.from_uri(location)
    if isinstance(filesystem, pafs.S3FileSystem):
        filesystem = pafs.S3FileSystem(region=target.region)
    with filesystem.open_input_stream(path) as stream:
        return json.loads(stream.read())


def exists(target: Target, location: str) -> bool:
    filesystem, path = pafs.FileSystem.from_uri(location)
    if isinstance(filesystem, pafs.S3FileSystem):
        filesystem = pafs.S3FileSystem(region=target.region)
    return filesystem.get_file_info(path).type != pafs.FileType.NotFound


def seed(spark: ReparkSession, target: Target, table: str, format_version: int) -> bool:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target.catalog}.{ACCEPTANCE_NAMESPACE}")
    properties = ", ".join(f"'{key}' = '{value}'" for key, value in STATUS_CHECK.items())
    try:
        spark.sql(
            f"CREATE OR REPLACE TABLE {table} (id INT, name STRING) USING iceberg "
            f"TBLPROPERTIES ('format-version' = '{format_version}', {properties})"
        ).collect()
    except Exception as error:
        refused = classify_v3_create_outcome(error) == S3T_V3_REFUSED_AT_CREATE
        if target.service == S3TABLES and format_version == 3 and refused:
            print(format_v3_refusal_record(error))
            return False
        raise AssertionError(mask_account_ids(f"{table}: create failed: {error!r}")) from None
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')").collect()
    metadata = read_json(target, current_metadata_location(spark, table))
    stored = metadata.get("properties", {})
    missing = missing_properties(stored, STATUS_CHECK)
    assert not missing, f"{table}: the drill table lacks its status-check properties {missing}"
    return True


def stable_snapshots(spark: ReparkSession, table: str) -> set[int]:
    try:
        return stable_snapshot_ids(lambda: snapshot_ids(spark, table), label=table)
    except UnresolvedError as unresolved:
        pytest.fail(mask_account_ids(str(unresolved)))


def count_operation(spark: ReparkSession, table: str, operation_id: str) -> int:
    arrow = spark.sql(
        f"SELECT count(*) FROM {table}.snapshots WHERE summary['{OPERATION_ID}'] = '{operation_id}'"
    ).to_arrow()
    return int(arrow.column(0)[0].as_py())


def run_statement(
    target: Target, gate: CommitGate | None, door: str, table: str
) -> BaseException | None:
    extra = {} if gate is None else {target.endpoint_key(): gate.url}
    with session(target, extra) as writer:
        if door == "session_key":
            writer.conf.set(SESSION_KEY, "v")
        try:
            writer.sql(statement(door, table)).collect()
        except Exception as error:
            return error
    return None


def assert_answer(label: str, door: str, spec: CellSpec, error: BaseException | None) -> None:
    if not spec.raises:
        assert error is None, f"{label}: expected success, got {mask_account_ids(repr(error))}"
        return
    assert isinstance(error, CommitStateUnknownException), (
        f"{label}: expected CommitStateUnknownException, got {mask_account_ids(repr(error))}"
    )
    if door == "plain":
        assert error.operation_id is None, f"{label}: the plain door carries no id"
    else:
        assert isinstance(error.operation_id, str), f"{label}: the error carries the attempt's id"


def drill(service: str, door: str, spec: CellSpec, format_version: int) -> None:
    target = target_for(service)
    table = table_of(target, door, spec, format_version)
    label = f"{spec.cell} {service} {door} v{format_version}"
    with session(target, {}) as resolver:
        if not seed(resolver, target, table, format_version):
            return
        before = snapshot_ids(resolver, table)
        assert rows(resolver, table) == PRE_ROWS
    if spec.script is None:
        error = run_statement(target, None, door, table)
        commits = []
    else:
        upstream = regional_upstream(service, target.region)
        with CommitGate(
            upstream=upstream,
            credentials=credentials(),
            namespace=ACCEPTANCE_NAMESPACE,
            table_bucket_arn=target.table_bucket_arn,
        ) as gate:
            gate.arm(spec.script)
            error = run_statement(target, gate, door, table)
            gate.stop_cutting()
            commits = gate.commits()
            refusals = gate.refusals()
            seen = [(entry.method, entry.path, entry.operation) for entry in gate.exchanges()]
        assert not refusals, mask_account_ids(
            f"{label}: the gate refused {refusals}; exchanges {seen}"
        )
        assert len(commits) == spec.commit_requests, (
            f"{label}: commit requests at the gate {[c.answer for c in commits]}"
        )
    assert_answer(label, door, spec, error)
    with session(target, {}) as resolver:
        after = stable_snapshots(resolver, table)
        new = after - before
        if commits:
            location = commits[0].metadata_location
            assert location is not None, f"{label}: the commit request named no metadata file"
            attempt = attempted_snapshot(read_json(target, location), before, label)
            attempt_id = int(attempt["snapshot-id"])
            operation_id = attempt.get("summary", {}).get(OPERATION_ID)
            assert operation_id, f"{label}: the attempted snapshot carries no {OPERATION_ID}"
            if isinstance(error, CommitStateUnknownException) and door != "plain":
                assert error.operation_id == operation_id, f"{label}: the error names the attempt"
            if spec.cell == "U-3":
                held = exists(target, location)
                listed = exists(target, attempt["manifest-list"])
                print(f"{label} orphans metadata={int(held)} lists={int(listed)}")
                assert held, f"{label}: the held metadata file is on storage"
                assert listed, f"{label}: the attempt's manifest list is on storage"
        else:
            assert len(new) == 1, f"{label}: the ungated statement added {len(new)} snapshots"
            attempt_id = next(iter(new))
            operation_id = None
        landed = attempt_id in after
        assert landed is spec.landed, (
            f"{label}: resolved {'committed' if landed else 'not committed'}"
        )
        if operation_id is not None:
            count = count_operation(resolver, table, operation_id)
            assert count <= 1, (
                f"H-U5 {label}: {count} snapshots carry one operation id, a duplicate commit; "
                "the live leg stops and the owner hears first"
            )
            assert count == int(spec.landed), f"{label}: snapshots carrying the id"
        expected = (INSERT_ROWS if door != "merge" else MERGE_ROWS) if spec.landed else PRE_ROWS
        assert rows(resolver, table) == expected, f"{label}: rows once or not at all"


def drill_with_one_retry(service: str, door: str, spec: CellSpec, format_version: int) -> None:
    try:
        drill(service, door, spec, format_version)
    except AssertionError as failure:
        if not is_commit_conflict_error(failure):
            raise
        print(f"{spec.cell} {service} {door} v{format_version}: a maintenance commit, retried once")
        drill(service, door, spec, format_version)


@pytest.mark.parametrize("format_version", FORMATS)
@pytest.mark.parametrize("door", DOORS)
@pytest.mark.parametrize("spec", CELLS, ids=lambda spec: spec.cell)
@pytest.mark.parametrize("service", CATALOGS)
def test_unknown_commit_outcome_resolves_by_id(
    service: str, spec: CellSpec, door: str, format_version: int
) -> None:
    drill_with_one_retry(service, door, spec, format_version)

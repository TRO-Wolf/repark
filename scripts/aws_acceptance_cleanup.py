"""Remove the scratch tables of finished aws-acceptance runs; dry run unless --execute."""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple
from urllib.parse import urlparse

SCRATCH_NAMESPACE = "testing_repark_acceptance"
RUN_PREFIX = re.compile(r"testing_r[0-9]+a[0-9]+_(?:acc|u|x)_")
RUN_GROUP = re.compile(r"testing_r[0-9]+a[0-9]+_(?:(?:acc|u|x)_)?")
STEM_OF_PREFIX = re.compile(r"testing_(r[0-9]+a[0-9]+_(?:acc|u|x)_)")
PLACEHOLDER_WAREHOUSE = "s3://example-warehouse/"
GLUE_DELETE_BATCH = 100
S3_DELETE_BATCH = 1000
ClientFactory = Callable[[str], Any]
DESCRIPTION = (
    "Delete the tables and S3 path-write objects of one run-and-leg prefix"
    " (testing_r<digits>a<digits>_<acc|u|x>_), or the tables of every run prefix whose newest"
    " table is older than N hours, from the Glue scratch database and the S3 Tables scratch"
    " namespace."
    " Dry run unless --execute."
)


class ScratchTable(NamedTuple):
    """One catalog table: its name, creation time and storage location (Glue only)."""

    name: str
    created_at: datetime
    location: str | None


def validate_run_prefix(prefix: str) -> str:
    """Return ``prefix`` when it is ``testing_r<digits>a<digits>_<acc|u|x>_``, else raise."""
    if RUN_PREFIX.fullmatch(prefix) is None:
        raise ValueError(
            f"run prefix must match testing_r<digits>a<digits>_<acc|u|x>_, got {prefix!r}"
        )
    return prefix


def run_prefix_of(name: str) -> str | None:
    """The run-and-leg prefix (or bare run prefix) a name carries; None for any other name."""
    matched = RUN_GROUP.match(name)
    return None if matched is None else matched.group(0)


def assert_deletable(name: str) -> None:
    """Raise ValueError unless ``name`` has the run shape; the bench is excluded by shape."""
    if run_prefix_of(name) is None:
        raise ValueError(f"refusing {name!r}: not a testing_r<digits>a<digits>_ scratch table")


def select_by_prefix(tables: Iterable[ScratchTable], prefix: str) -> list[ScratchTable]:
    """The tables whose run prefix is exactly ``prefix``, sorted by name."""
    validate_run_prefix(prefix)
    return sorted(table for table in tables if run_prefix_of(table.name) == prefix)


def select_older_than(
    tables: Iterable[ScratchTable], hours: float, now: datetime
) -> list[ScratchTable]:
    """Tables of every run prefix whose newest table is older than ``hours``, sorted by name."""
    newest: dict[str, datetime] = {}
    members: dict[str, list[ScratchTable]] = defaultdict(list)
    for table in tables:
        prefix = run_prefix_of(table.name)
        if prefix is None:
            continue
        members[prefix].append(table)
        newest[prefix] = max(newest.get(prefix, table.created_at), table.created_at)
    cutoff = now - timedelta(hours=hours)
    return sorted(
        table for prefix, group in members.items() if newest[prefix] < cutoff for table in group
    )


def chunked[Item](items: Sequence[Item], size: int) -> Iterator[Sequence[Item]]:
    """Consecutive slices of ``items`` holding at most ``size`` entries."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def scratch_prefix(warehouse: str) -> str:
    """The warehouse scratch prefix ``<warehouse>/<namespace>/`` with one trailing slash."""
    return f"{warehouse.rstrip('/')}/{SCRATCH_NAMESPACE}/"


def location_under_scratch(location: str | None, warehouse: str) -> bool:
    """True when ``location`` is strictly below the warehouse scratch prefix."""
    if not location:
        return False
    prefix = scratch_prefix(warehouse)
    normalized = location.rstrip("/") + "/"
    if ".." in normalized.split("/") or not normalized.startswith(prefix):
        return False
    return normalized != prefix


def list_glue_tables(glue: Any, database: str) -> list[ScratchTable]:
    """Every table of the Glue database as a ScratchTable."""
    tables = []
    for page in glue.get_paginator("get_tables").paginate(DatabaseName=database):
        for entry in page.get("TableList", []):
            location = (entry.get("StorageDescriptor") or {}).get("Location")
            tables.append(ScratchTable(entry["Name"], entry["CreateTime"], location))
    return tables


def list_s3tables(client: Any, table_bucket_arn: str, namespace: str) -> list[ScratchTable]:
    """Every table of the S3 Tables namespace as a ScratchTable."""
    tables = []
    paginator = client.get_paginator("list_tables")
    for page in paginator.paginate(tableBucketARN=table_bucket_arn, namespace=namespace):
        for entry in page.get("tables", []):
            tables.append(ScratchTable(entry["name"], entry["createdAt"], None))
    return tables


def delete_glue_tables(glue: Any, database: str, tables: Sequence[ScratchTable]) -> list[str]:
    """Batch-delete the tables from Glue; return the names of every table not deleted."""
    for table in tables:
        assert_deletable(table.name)
    names = [table.name for table in tables]
    failed: list[str] = []
    for batch in chunked(names, GLUE_DELETE_BATCH):
        try:
            response = glue.batch_delete_table(DatabaseName=database, TablesToDelete=list(batch))
        except Exception as error:
            print(f"glue: a batch of {len(batch)} failed: {type(error).__name__}", file=sys.stderr)
            failed.extend(batch)
            continue
        failed.extend(error["TableName"] for error in response.get("Errors", []))
    return failed


def delete_s3tables(
    client: Any, table_bucket_arn: str, namespace: str, tables: Sequence[ScratchTable]
) -> list[str]:
    """Delete the tables from the S3 Tables namespace one by one; return the names that failed."""
    for table in tables:
        assert_deletable(table.name)
    failed = []
    for table in tables:
        try:
            client.delete_table(
                tableBucketARN=table_bucket_arn, namespace=namespace, name=table.name
            )
        except Exception as error:
            print(f"s3tables: {table.name} failed: {type(error).__name__}", file=sys.stderr)
            failed.append(table.name)
    return failed


def scratch_object_keys(s3: Any, location: str, warehouse: str) -> list[tuple[str, str]]:
    """The (bucket, key) pairs under ``location``; empty unless it sits under the scratch prefix."""
    if not location_under_scratch(location, warehouse):
        return []
    parsed = urlparse(location)
    if parsed.scheme != "s3":
        return []
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=parsed.netloc, Prefix=prefix):
        keys.extend((parsed.netloc, entry["Key"]) for entry in page.get("Contents", []))
    return keys


def delete_scratch_objects(s3: Any, objects: Sequence[tuple[str, str]]) -> int:
    """Delete the (bucket, key) pairs in batches; return how many objects were not deleted."""
    by_bucket: dict[str, list[str]] = defaultdict(list)
    for bucket, key in objects:
        by_bucket[bucket].append(key)
    refused = 0
    for bucket, keys in by_bucket.items():
        for batch in chunked(keys, S3_DELETE_BATCH):
            try:
                response = s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
                )
            except Exception as error:
                print(
                    f"s3: a batch of {len(batch)} failed: {type(error).__name__}", file=sys.stderr
                )
                refused += len(batch)
                continue
            refused += len(response.get("Errors", []))
    return refused


def build_parser() -> argparse.ArgumentParser:
    """The command line: a run prefix or an age, and the execute switch."""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--run-prefix", help="the run-and-leg prefix testing_r<digits>a<digits>_<leg>_"
    )
    mode.add_argument("--older-than-hours", type=float, help="remove every run prefix older")
    parser.add_argument("--execute", action="store_true", help="delete; without it, only report")
    return parser


def select_tables(
    tables: Sequence[ScratchTable], prefix: str | None, hours: float | None, now: datetime
) -> list[ScratchTable]:
    """Apply the one selection mode the command line chose."""
    if prefix is not None:
        return select_by_prefix(tables, prefix)
    if hours is None or hours <= 0:
        raise ValueError("--older-than-hours must be positive")
    return select_older_than(tables, hours, now)


def location_is_direct_child(location: str | None, warehouse: str) -> bool:
    """True when ``location`` is exactly ``<warehouse scratch prefix>/<one segment>``."""
    if not location:
        return False
    parent = location.rstrip("/").rsplit("/", 1)[0] + "/"
    return parent == scratch_prefix(warehouse)


def location_leaf(table: ScratchTable) -> str | None:
    """The last path segment of the table's location, or None without a location."""
    if not table.location:
        return None
    return table.location.rstrip("/").rsplit("/", 1)[-1]


def clean_glue(
    glue: Any, s3: Any, warehouse: str, chosen: Sequence[ScratchTable], execute: bool
) -> int:
    """Report or perform the Glue deletes, then the objects of each deleted table; failures.

    A twin entry points at the directory of another selected table: its entry goes, its objects
    do not. A table whose location is neither its own directory nor a selected sibling's, or lies
    outside the scratch prefix, is kept with its objects and counted as a failure.
    """
    names = {t.name for t in chosen}
    sound, twins, anomalies = [], [], []
    for table in chosen:
        leaf = location_leaf(table)
        if not location_is_direct_child(table.location, warehouse):
            anomalies.append(table)
        elif leaf == table.name:
            sound.append(table)
        elif leaf in names:
            twins.append(table)
        else:
            anomalies.append(table)
    for table in anomalies:
        print(f"glue: kept {table.name}: unexpected location {table.location!r}", file=sys.stderr)
    objects = {t.name: scratch_object_keys(s3, t.location or "", warehouse) for t in sound}
    selected = sum(len(keys) for keys in objects.values())
    deletable = sound + twins
    print(f"glue: {len(deletable)} tables, {selected} objects selected", flush=True)
    if not execute:
        return len(anomalies)
    failed = delete_glue_tables(glue, SCRATCH_NAMESPACE, deletable)
    removable = [key for name, keys in objects.items() if name not in failed for key in keys]
    refused = delete_scratch_objects(s3, removable)
    print(
        f"glue: deleted {len(deletable) - len(failed)} tables, {len(removable)} objects", flush=True
    )
    return len(anomalies) + len(failed) + refused


def path_object_stem(prefix: str) -> str:
    """The path-write object stem ``u12s3path_r<digits>a<digits>_<leg>_`` of a prefix."""
    matched = STEM_OF_PREFIX.fullmatch(validate_run_prefix(prefix))
    if matched is None:
        raise ValueError(f"no object stem for {prefix!r}")
    return f"u12s3path_{matched.group(1)}"


def path_object_keys(s3: Any, warehouse: str, prefix: str) -> list[tuple[str, str]]:
    """The (bucket, key) pairs of the leg's path-write objects below the scratch prefix."""
    stem = path_object_stem(prefix)
    root = scratch_prefix(warehouse)
    parsed = urlparse(root)
    if parsed.scheme != "s3":
        return []
    key_prefix = f"{parsed.path.lstrip('/')}{stem}"
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=parsed.netloc, Prefix=key_prefix
    ):
        keys.extend(
            (parsed.netloc, entry["Key"])
            for entry in page.get("Contents", [])
            if entry["Key"].startswith(key_prefix)
        )
    return keys


def clean_path_objects(s3: Any, warehouse: str, prefix: str, execute: bool) -> int:
    """Report or perform the delete of the leg's S3 path-write objects; return failures."""
    objects = path_object_keys(s3, warehouse, prefix)
    print(f"s3 path objects: {len(objects)} selected", flush=True)
    if not execute:
        return 0
    refused = delete_scratch_objects(s3, objects)
    print(f"s3 path objects: deleted {len(objects) - refused}", flush=True)
    return refused


def clean_s3tables(
    client: Any, table_bucket_arn: str, chosen: Sequence[ScratchTable], execute: bool
) -> int:
    """Report or perform the S3 Tables deletes; return how many failed."""
    print(f"s3tables: {len(chosen)} tables selected", flush=True)
    if not execute:
        return 0
    failed = delete_s3tables(client, table_bucket_arn, SCRATCH_NAMESPACE, chosen)
    print(f"s3tables: deleted {len(chosen) - len(failed)} tables", flush=True)
    return len(failed)


def boto3_client(service: str) -> Any:
    """Build a boto3 client for ``service``; boto3 is imported here and nowhere earlier."""
    import boto3

    return boto3.client(service)


def main(argv: Sequence[str] | None = None, clients: ClientFactory | None = None) -> int:
    """Run the cleanup against the environment's AWS account; return the exit status."""
    arguments = build_parser().parse_args(argv)
    if arguments.run_prefix is not None:
        try:
            validate_run_prefix(arguments.run_prefix)
        except ValueError as error:
            print(str(error), file=sys.stderr)
            return 2
    warehouse = os.environ.get("REPARK_ACCEPT_WAREHOUSE", "")
    placeholder = PLACEHOLDER_WAREHOUSE.rstrip("/")
    if not warehouse.startswith("s3://") or warehouse.rstrip("/").startswith(placeholder):
        print("REPARK_ACCEPT_WAREHOUSE must be a real s3:// location", file=sys.stderr)
        return 2
    factory = boto3_client if clients is None else clients
    now = datetime.now(UTC)
    glue = factory("glue")
    s3 = factory("s3")
    glue_tables = list_glue_tables(glue, SCRATCH_NAMESPACE)
    chosen = select_tables(glue_tables, arguments.run_prefix, arguments.older_than_hours, now)
    failures = clean_glue(glue, s3, warehouse, chosen, arguments.execute)
    if arguments.run_prefix is not None:
        failures += clean_path_objects(s3, warehouse, arguments.run_prefix, arguments.execute)
    table_bucket_arn = os.environ.get("TABLE_BUCKET_ARN", "")
    if table_bucket_arn:
        client = factory("s3tables")
        listed = list_s3tables(client, table_bucket_arn, SCRATCH_NAMESPACE)
        selected = select_tables(listed, arguments.run_prefix, arguments.older_than_hours, now)
        failures += clean_s3tables(client, table_bucket_arn, selected, arguments.execute)
    if failures:
        print(f"{failures} deletes failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

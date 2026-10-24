"""Stub clients and fixtures shared by the cleanup test files; nothing here touches AWS."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "aws_acceptance_cleanup.py"
_NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
_WAREHOUSE = "s3://example-warehouse/"
_SCRATCH = "s3://example-warehouse/testing_repark_acceptance"


def _load() -> ModuleType:
    """Import the script by path so no installed package is needed."""
    spec = importlib.util.spec_from_file_location("aws_acceptance_cleanup", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


cleanup = _load()


def _table(name: str, age_hours: float = 1.0, location: str | None = None) -> Any:
    """A ScratchTable created ``age_hours`` before the fixed clock."""
    return cleanup.ScratchTable(name, _NOW - timedelta(hours=age_hours), location)


class _Pages:
    """A paginator stub that yields the pages it was given."""

    def __init__(
        self, pages: list[dict[str, Any]], calls: list[tuple[str, dict[str, Any]]]
    ) -> None:
        """Keep the pages and the shared call log."""
        self.pages = pages
        self.calls = calls

    def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Record the call and return the pages."""
        self.calls.append(("paginate", kwargs))
        return self.pages


class _Client:
    """A boto3-shaped client stub that records every call."""

    def __init__(self, pages: list[dict[str, Any]] | None = None) -> None:
        """Start with the given list pages and an empty call log."""
        self.pages = pages or []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[dict[str, Any]] = []

    def get_paginator(self, name: str) -> _Pages:
        """Return the page stub for one list operation."""
        self.calls.append(("get_paginator", {"name": name}))
        return _Pages(self.pages, self.calls)

    def batch_delete_table(self, **kwargs: Any) -> dict[str, Any]:
        """Record a Glue batch delete."""
        self.calls.append(("batch_delete_table", kwargs))
        return {"Errors": self.errors}

    def delete_table(self, **kwargs: Any) -> dict[str, Any]:
        """Record an S3 Tables delete."""
        self.calls.append(("delete_table", kwargs))
        return {}

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        """Record an S3 batch delete."""
        self.calls.append(("delete_objects", kwargs))
        return {"Errors": self.errors}


def _deleted(client: _Client, operation: str) -> list[dict[str, Any]]:
    """The recorded keyword arguments of one operation."""
    return [kwargs for name, kwargs in client.calls if name == operation]


_WH = "s3://real-bucket/wh/"
_WH_ROOT = "wh/testing_repark_acceptance"
_OLD = datetime(2020, 1, 1, tzinfo=UTC)


class _KeyPages:
    """A list_objects_v2 paginator stub that filters stored keys by the requested prefix."""

    def __init__(self, keys: list[str], calls: list[tuple[str, dict[str, Any]]]) -> None:
        """Keep the stored keys and the shared call log."""
        self.keys = keys
        self.calls = calls

    def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Record the call and return one page of the matching keys."""
        self.calls.append(("paginate", kwargs))
        hits = [{"Key": key} for key in self.keys if key.startswith(kwargs["Prefix"])]
        return [{"Contents": hits}]


class _World(_Client):
    """A client stub whose paginators answer by operation name."""

    def __init__(
        self, pages: dict[str, list[dict[str, Any]]], keys: list[str] | None = None
    ) -> None:
        """Keep the pages per operation and the stored object keys."""
        super().__init__()
        self.world = pages
        self.keys = keys or []

    def get_paginator(self, name: str) -> Any:
        """Return the stub paginator of one list operation."""
        self.calls.append(("get_paginator", {"name": name}))
        if name == "list_objects_v2":
            return _KeyPages(self.keys, self.calls)
        return _Pages(self.world[name], self.calls)


class _Factory:
    """A client factory that records which services were built."""

    def __init__(self, clients: dict[str, _World]) -> None:
        """Keep the stub per service."""
        self.clients = clients
        self.built: list[str] = []

    def __call__(self, service: str) -> _World:
        """Return the stub of ``service`` and note the build."""
        self.built.append(service)
        return self.clients[service]


def _glue_entry(name: str, age_hours_ago: bool = False) -> dict[str, Any]:
    """A Glue table entry located under the scratch prefix."""
    created = _OLD if age_hours_ago else datetime.now(UTC)
    return {
        "Name": name,
        "CreateTime": created,
        "StorageDescriptor": {"Location": f"{_WH}testing_repark_acceptance/{name}"},
    }


def _world(old: bool = False) -> _Factory:
    """Two legs of run 12 in Glue and S3 Tables, their objects, and one legacy table."""
    names = ["testing_r12a1_acc_a", "testing_r12a1_u_a", "testing_legacy"]
    glue = _World({"get_tables": [{"TableList": [_glue_entry(n, old) for n in names]}]})
    keys = [
        f"{_WH_ROOT}/testing_r12a1_acc_a/f1",
        f"{_WH_ROOT}/testing_r12a1_u_a/f2",
        f"{_WH_ROOT}/u12s3path_r12a1_acc_zz/p1",
        f"{_WH_ROOT}/u12s3path_r12a1_u_zz/p2",
        f"{_WH_ROOT}/testing_legacy/f3",
    ]
    s3 = _World({}, keys)
    created = _OLD if old else datetime.now(UTC)
    tables = [
        {"name": n, "createdAt": created} for n in ("testing_r12a1_acc_t", "testing_r12a1_u_t")
    ]
    s3t = _World({"list_tables": [{"tables": tables}]})
    return _Factory({"glue": glue, "s3": s3, "s3tables": s3t})


def _env(
    monkeypatch: pytest.MonkeyPatch, arn: str | None = "arn:aws:s3tables:r:1:bucket/b"
) -> None:
    """Point the script at the stub warehouse and, optionally, a table bucket."""
    monkeypatch.setenv("REPARK_ACCEPT_WAREHOUSE", _WH)
    if arn is None:
        monkeypatch.delenv("TABLE_BUCKET_ARN", raising=False)
    else:
        monkeypatch.setenv("TABLE_BUCKET_ARN", arn)


def _all_deletes(factory: _Factory) -> list[tuple[str, str]]:
    """Every delete call recorded on any stub, as (service, operation)."""
    found = []
    for service, client in factory.clients.items():
        for name, _ in client.calls:
            if name in {"batch_delete_table", "delete_table", "delete_objects"}:
                found.append((service, name))
    return found

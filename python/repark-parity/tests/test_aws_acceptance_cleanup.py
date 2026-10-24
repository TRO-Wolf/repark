"""Stubbed-client pins for scripts/aws_acceptance_cleanup.py: nothing here touches AWS.

pins: aws-ephemeral-runs-1/C-004, C-005, C-006, C-007, C-008, C-011
"""

from __future__ import annotations

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


@pytest.mark.parametrize(
    "prefix",
    [
        "testing_r1_",
        "testing_r40079497082_",
        "testing_r0_",
    ],
)
def test_a_well_formed_run_prefix_is_accepted(prefix: str) -> None:
    """The three prefix shapes the workflow and the local default produce validate."""
    assert cleanup.validate_run_prefix(prefix) == prefix


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "testing_",
        "testing_r_",
        "testing_rabc_",
        "testing_r12",
        "xtesting_r12_",
        "testing_r12_x",
        "testing_r12_ ",
        "testing_R12_",
        "*",
        "testing_r12_*",
        "testing_r१२_",
    ],
)
def test_a_prefix_that_is_not_exactly_testing_r_digits_is_refused(prefix: str) -> None:
    """Any other shape, including the bare legacy prefix, raises before a client exists."""
    with pytest.raises(ValueError, match="testing_r<digits>_"):
        cleanup.validate_run_prefix(prefix)


def test_a_prefix_selects_only_its_own_tables_and_not_a_longer_run_id() -> None:
    """Run 12 does not take run 123's tables, the legacy tables or the bench table."""
    tables = [
        _table("testing_r12_a"),
        _table("testing_r12_b"),
        _table("testing_r123_a"),
        _table("testing_old_entity"),
        _table("ice_read_perf_bench"),
        _table("other"),
    ]
    chosen = cleanup.select_by_prefix(tables, "testing_r12_")
    assert [table.name for table in chosen] == ["testing_r12_a", "testing_r12_b"]


def test_the_bench_table_is_never_selected_by_any_mode() -> None:
    """The bench name is excluded by prefix selection, age selection and the deletion guard."""
    bench = _table("ice_read_perf_bench", age_hours=999)
    assert cleanup.select_by_prefix([bench], "testing_r1_") == []
    assert cleanup.select_older_than([bench], 1, _NOW) == []
    with pytest.raises(ValueError, match="not a testing_r<digits>_"):
        cleanup.assert_deletable("ice_read_perf_bench")
    glue, s3 = _Client(), _Client()
    with pytest.raises(ValueError, match="not a testing_r<digits>_"):
        cleanup.delete_glue_tables(glue, "db", [bench])
    with pytest.raises(ValueError, match="not a testing_r<digits>_"):
        cleanup.delete_s3tables(s3, "arn", "ns", [bench])
    assert glue.calls == [] and s3.calls == []


def test_the_age_filter_removes_a_prefix_only_when_its_newest_table_is_old() -> None:
    """A run with one fresh table stays whole; a wholly old run goes; legacy names stay."""
    tables = [
        _table("testing_r1_a", age_hours=30),
        _table("testing_r1_b", age_hours=25),
        _table("testing_r2_a", age_hours=48),
        _table("testing_r2_b", age_hours=2),
        _table("testing_r3_a", age_hours=24.5),
        _table("testing_legacy", age_hours=900),
    ]
    chosen = cleanup.select_older_than(tables, 24, _NOW)
    assert [table.name for table in chosen] == ["testing_r1_a", "testing_r1_b", "testing_r3_a"]


def test_the_age_filter_keeps_a_table_exactly_at_the_cutoff() -> None:
    """The comparison is strict: a table of exactly N hours is not older than N hours."""
    assert cleanup.select_older_than([_table("testing_r1_a", age_hours=24)], 24, _NOW) == []


def test_the_selection_mode_needs_a_positive_age() -> None:
    """A zero or negative age would take a running job's tables, so it raises."""
    for hours in (0, -1):
        with pytest.raises(ValueError, match="positive"):
            cleanup.select_tables([], None, hours, _NOW)
    with pytest.raises(ValueError, match="positive"):
        cleanup.select_tables([], None, None, _NOW)


@pytest.mark.parametrize(
    ("location", "allowed"),
    [
        (f"{_SCRATCH}/testing_r1_a", True),
        (f"{_SCRATCH}/testing_r1_a/", True),
        (f"{_SCRATCH}/testing_r1_a/data/part-0", True),
        (_SCRATCH, False),
        (f"{_SCRATCH}/", False),
        ("s3://example-warehouse/", False),
        ("s3://example-warehouse/other/testing_r1_a", False),
        ("s3://example-warehouse/testing_repark_acceptance_x/testing_r1_a", False),
        ("s3://another-bucket/testing_repark_acceptance/testing_r1_a", False),
        (f"{_SCRATCH}/../prod/testing_r1_a", False),
        ("", False),
        (None, False),
    ],
)
def test_the_location_guard_allows_only_paths_strictly_below_the_scratch_prefix(
    location: str | None, allowed: bool
) -> None:
    """Only a path strictly under the warehouse scratch prefix may have objects deleted."""
    assert cleanup.location_under_scratch(location, _WAREHOUSE) is allowed
    assert cleanup.location_under_scratch(location, _WAREHOUSE.rstrip("/")) is allowed


def test_an_outside_location_lists_and_deletes_no_objects() -> None:
    """A table whose location is outside the scratch prefix costs no S3 call at all."""
    s3 = _Client([{"Contents": [{"Key": "x"}]}])
    assert cleanup.scratch_object_keys(s3, "s3://example-warehouse/prod/t", _WAREHOUSE) == []
    assert (
        cleanup.scratch_object_keys(
            s3, "gs://example-warehouse/testing_repark_acceptance/t", _WAREHOUSE
        )
        == []
    )
    assert s3.calls == []


def test_an_inside_location_lists_with_a_trailing_slash_prefix() -> None:
    """The listing prefix ends in a slash so a sibling table with a longer name is not swept."""
    s3 = _Client([{"Contents": [{"Key": "testing_repark_acceptance/testing_r1_a/m.json"}]}, {}])
    keys = cleanup.scratch_object_keys(s3, f"{_SCRATCH}/testing_r1_a", _WAREHOUSE)
    assert keys == [("example-warehouse", "testing_repark_acceptance/testing_r1_a/m.json")]
    assert _deleted(s3, "paginate") == [
        {"Bucket": "example-warehouse", "Prefix": "testing_repark_acceptance/testing_r1_a/"}
    ]


def test_glue_deletes_go_out_in_batches_of_one_hundred() -> None:
    """250 tables become three batch calls of 100, 100 and 50, each carrying only names."""
    tables = [_table(f"testing_r9_{index:03d}") for index in range(250)]
    glue = _Client()
    assert cleanup.delete_glue_tables(glue, "testing_repark_acceptance", tables) == 250
    calls = _deleted(glue, "batch_delete_table")
    assert [len(call["TablesToDelete"]) for call in calls] == [100, 100, 50]
    assert {call["DatabaseName"] for call in calls} == {"testing_repark_acceptance"}
    assert [name for call in calls for name in call["TablesToDelete"]] == [t.name for t in tables]


def test_a_glue_batch_error_raises_with_the_table_names() -> None:
    """Glue reporting an error for a table makes the run fail instead of passing silently."""
    glue = _Client()
    glue.errors = [{"TableName": "testing_r9_000", "ErrorDetail": {"ErrorCode": "Denied"}}]
    with pytest.raises(RuntimeError, match="testing_r9_000"):
        cleanup.delete_glue_tables(glue, "db", [_table("testing_r9_000")])


def test_object_deletes_go_out_in_batches_of_one_thousand_per_bucket() -> None:
    """2500 keys become batches of 1000, 1000 and 500 with quiet responses."""
    objects = [("example-warehouse", f"testing_repark_acceptance/t/{n}") for n in range(2500)]
    s3 = _Client()
    assert cleanup.delete_scratch_objects(s3, objects) == 2500
    calls = _deleted(s3, "delete_objects")
    assert [len(call["Delete"]["Objects"]) for call in calls] == [1000, 1000, 500]
    assert all(call["Delete"]["Quiet"] is True for call in calls)


def test_an_object_delete_error_raises() -> None:
    """A refused key fails the cleanup so a leftover object is not reported as clean."""
    s3 = _Client()
    s3.errors = [{"Key": "k", "Code": "AccessDenied"}]
    with pytest.raises(RuntimeError, match="1 object deletes"):
        cleanup.delete_scratch_objects(s3, [("b", "k")])


def test_s3_tables_deletes_one_table_at_a_time_in_the_scratch_namespace() -> None:
    """Each S3 Tables table is deleted by bucket ARN, namespace and name."""
    client = _Client()
    tables = [_table("testing_r5_a"), _table("testing_r5_b")]
    assert cleanup.delete_s3tables(client, "arn:x", "testing_repark_acceptance", tables) == 2
    assert _deleted(client, "delete_table") == [
        {"tableBucketARN": "arn:x", "namespace": "testing_repark_acceptance", "name": name}
        for name in ("testing_r5_a", "testing_r5_b")
    ]


def test_listing_reads_every_page_of_both_catalogs() -> None:
    """Names, times and Glue locations survive the page walk; no descriptor gives None."""
    glue = _Client(
        [
            {
                "TableList": [
                    {"Name": "a", "CreateTime": _NOW, "StorageDescriptor": {"Location": "l"}}
                ]
            },
            {"TableList": [{"Name": "b", "CreateTime": _NOW}]},
        ]
    )
    listed = cleanup.list_glue_tables(glue, "db")
    assert [(t.name, t.location) for t in listed] == [("a", "l"), ("b", None)]
    s3t = _Client(
        [
            {"tables": [{"name": "a", "createdAt": _NOW}]},
            {"tables": [{"name": "b", "createdAt": _NOW}]},
        ]
    )
    assert [t.name for t in cleanup.list_s3tables(s3t, "arn", "ns")] == ["a", "b"]


class _Ordered(_Client):
    """A client stub that appends every delete to a shared order list."""

    def __init__(self, order: list[str], pages: list[dict[str, Any]] | None = None) -> None:
        """Share the order list with the other stub."""
        super().__init__(pages)
        self.order = order

    def batch_delete_table(self, **kwargs: Any) -> dict[str, Any]:
        """Record a Glue batch delete in the shared order."""
        self.order.append("tables")
        return super().batch_delete_table(**kwargs)

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        """Record an S3 batch delete in the shared order."""
        self.order.append("objects")
        return super().delete_objects(**kwargs)


_LISTING = [{"Contents": [{"Key": "testing_repark_acceptance/testing_r4_a/f"}]}]


def test_a_dry_run_issues_no_delete_call(capsys: pytest.CaptureFixture[str]) -> None:
    """Without the execute switch the Glue and S3 Tables paths only report counts."""
    tables = [_table("testing_r4_a", location=f"{_SCRATCH}/testing_r4_a")]
    glue, s3, s3t = _Client(), _Client(_LISTING), _Client()
    cleanup.clean_glue(glue, s3, _WAREHOUSE, tables, execute=False)
    cleanup.clean_s3tables(s3t, "arn", tables, execute=False)
    assert _deleted(glue, "batch_delete_table") == []
    assert _deleted(s3t, "delete_table") == []
    assert _deleted(s3, "delete_objects") == []
    assert "glue: 1 tables, 1 objects selected" in capsys.readouterr().out


def test_an_executed_glue_cleanup_deletes_the_tables_then_the_objects() -> None:
    """The order is the table entries first, then the objects below each table's location."""
    tables = [_table("testing_r4_a", location=f"{_SCRATCH}/testing_r4_a")]
    order: list[str] = []
    glue, s3 = _Ordered(order), _Ordered(order, _LISTING)
    cleanup.clean_glue(glue, s3, _WAREHOUSE, tables, execute=True)
    assert order == ["tables", "objects"]


def test_the_command_line_needs_exactly_one_mode() -> None:
    """Neither mode, or both modes, is a usage error."""
    parser = cleanup.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    with pytest.raises(SystemExit):
        parser.parse_args(["--prefix", "testing_r1_", "--older-than-hours", "24"])
    parsed = parser.parse_args(["--older-than-hours", "24", "--execute"])
    assert parsed.execute is True and parsed.prefix is None
    assert parser.parse_args(["--prefix", "testing_r1_"]).execute is False


def test_main_refuses_a_bad_prefix_before_any_client_or_environment_read(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A malformed prefix exits 2 before boto3 is imported or the warehouse is read."""
    monkeypatch.delenv("REPARK_ACCEPT_WAREHOUSE", raising=False)
    monkeypatch.setitem(sys.modules, "boto3", None)
    assert cleanup.main(["--prefix", "testing_"]) == 2
    assert "testing_r<digits>_" in capsys.readouterr().err

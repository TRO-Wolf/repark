"""Stubbed-client pins for scripts/aws_acceptance_cleanup.py: nothing here touches AWS.

pins: aws-ephemeral-runs-1/C-004, C-005, C-006, C-007, C-008, C-011, C-020, C-021, C-022
pins: aws-ephemeral-runs-1/C-023, C-024, C-025, C-026
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
        "testing_r1a1_acc_",
        "testing_r40079497082a1_u_",
        "testing_r0a1_x_",
    ],
)
def test_a_well_formed_run_and_leg_prefix_is_accepted(prefix: str) -> None:
    """The three leg shapes the workflow and the local default produce validate."""
    assert cleanup.validate_run_prefix(prefix) == prefix


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "testing_",
        "testing_r_",
        "testing_r12a1_",
        "testing_rabc_acc_",
        "testing_r12_acc_",
        "testing_r12a_acc_",
        "testing_r12a1b_acc_",
        "testing_r12a1_acc",
        "xtesting_r12a1_acc_",
        "testing_r12a1_acc_x",
        "testing_r12a1_acc_ ",
        "testing_R12_acc_",
        "testing_r12a1_dbt_",
        "testing_r12a1_a_",
        "testing_r12a1_uu_",
        "*",
        "testing_r12a1_acc_*",
        "testing_r१२a1_acc_",
    ],
)
def test_a_prefix_that_is_not_exactly_testing_r_digits_leg_is_refused(prefix: str) -> None:
    """Any other shape, including the bare legacy prefix, raises before a client exists."""
    with pytest.raises(ValueError, match="testing_r<digits>a<digits>_<acc"):
        cleanup.validate_run_prefix(prefix)


def test_a_prefix_selects_only_its_own_tables_and_not_a_longer_run_id() -> None:
    """Run 12 does not take run 123's tables, the legacy tables or the bench table."""
    tables = [
        _table("testing_r12a1_acc_a"),
        _table("testing_r12a1_acc_b"),
        _table("testing_r123a1_acc_a"),
        _table("testing_old_entity"),
        _table("ice_read_perf_bench"),
        _table("other"),
    ]
    chosen = cleanup.select_by_prefix(tables, "testing_r12a1_acc_")
    assert [table.name for table in chosen] == ["testing_r12a1_acc_a", "testing_r12a1_acc_b"]


def test_the_bench_table_is_never_selected_by_any_mode() -> None:
    """The bench name is excluded by prefix selection, age selection and the deletion guard."""
    bench = _table("ice_read_perf_bench", age_hours=999)
    assert cleanup.select_by_prefix([bench], "testing_r1a1_acc_") == []
    assert cleanup.select_older_than([bench], 1, _NOW) == []
    with pytest.raises(ValueError, match="not a testing_r<digits>a<digits>_"):
        cleanup.assert_deletable("ice_read_perf_bench")
    glue, s3 = _Client(), _Client()
    with pytest.raises(ValueError, match="not a testing_r<digits>a<digits>_"):
        cleanup.delete_glue_tables(glue, "db", [bench])
    with pytest.raises(ValueError, match="not a testing_r<digits>a<digits>_"):
        cleanup.delete_s3tables(s3, "arn", "ns", [bench])
    assert glue.calls == [] and s3.calls == []


def test_the_age_filter_removes_a_prefix_only_when_its_newest_table_is_old() -> None:
    """A run with one fresh table stays whole; a wholly old run goes; legacy names stay."""
    tables = [
        _table("testing_r1a1_acc_a", age_hours=30),
        _table("testing_r1a1_acc_b", age_hours=25),
        _table("testing_r2a1_acc_a", age_hours=48),
        _table("testing_r2a1_acc_b", age_hours=2),
        _table("testing_r3a1_acc_a", age_hours=24.5),
        _table("testing_legacy", age_hours=900),
    ]
    chosen = cleanup.select_older_than(tables, 24, _NOW)
    assert [table.name for table in chosen] == [
        "testing_r1a1_acc_a",
        "testing_r1a1_acc_b",
        "testing_r3a1_acc_a",
    ]


def test_the_age_filter_keeps_a_table_exactly_at_the_cutoff() -> None:
    """The comparison is strict: a table of exactly N hours is not older than N hours."""
    assert cleanup.select_older_than([_table("testing_r1a1_acc_a", age_hours=24)], 24, _NOW) == []


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
        (f"{_SCRATCH}/testing_r1a1_acc_a", True),
        (f"{_SCRATCH}/testing_r1a1_acc_a/", True),
        (f"{_SCRATCH}/testing_r1a1_acc_a/data/part-0", True),
        (_SCRATCH, False),
        (f"{_SCRATCH}/", False),
        ("s3://example-warehouse/", False),
        ("s3://example-warehouse/other/testing_r1a1_acc_a", False),
        ("s3://example-warehouse/testing_repark_acceptance_x/testing_r1a1_acc_a", False),
        ("s3://another-bucket/testing_repark_acceptance/testing_r1a1_acc_a", False),
        (f"{_SCRATCH}/../prod/testing_r1a1_acc_a", False),
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
    s3 = _Client(
        [{"Contents": [{"Key": "testing_repark_acceptance/testing_r1a1_acc_a/m.json"}]}, {}]
    )
    keys = cleanup.scratch_object_keys(s3, f"{_SCRATCH}/testing_r1a1_acc_a", _WAREHOUSE)
    assert keys == [("example-warehouse", "testing_repark_acceptance/testing_r1a1_acc_a/m.json")]
    assert _deleted(s3, "paginate") == [
        {"Bucket": "example-warehouse", "Prefix": "testing_repark_acceptance/testing_r1a1_acc_a/"}
    ]


def test_glue_deletes_go_out_in_batches_of_one_hundred() -> None:
    """250 tables become three batch calls of 100, 100 and 50, each carrying only names."""
    tables = [_table(f"testing_r9a1_acc_{index:03d}") for index in range(250)]
    glue = _Client()
    assert cleanup.delete_glue_tables(glue, "testing_repark_acceptance", tables) == []
    calls = _deleted(glue, "batch_delete_table")
    assert [len(call["TablesToDelete"]) for call in calls] == [100, 100, 50]
    assert {call["DatabaseName"] for call in calls} == {"testing_repark_acceptance"}
    assert [name for call in calls for name in call["TablesToDelete"]] == [t.name for t in tables]


def test_a_glue_batch_error_is_returned_with_the_table_names() -> None:
    """Glue reporting an error for a table puts that table in the failed list."""
    glue = _Client()
    glue.errors = [{"TableName": "testing_r9a1_acc_000", "ErrorDetail": {"ErrorCode": "Denied"}}]
    failed = cleanup.delete_glue_tables(glue, "db", [_table("testing_r9a1_acc_000")])
    assert failed == ["testing_r9a1_acc_000"]


def test_object_deletes_go_out_in_batches_of_one_thousand_per_bucket() -> None:
    """2500 keys become batches of 1000, 1000 and 500 with quiet responses."""
    objects = [("example-warehouse", f"testing_repark_acceptance/t/{n}") for n in range(2500)]
    s3 = _Client()
    assert cleanup.delete_scratch_objects(s3, objects) == 0
    calls = _deleted(s3, "delete_objects")
    assert [len(call["Delete"]["Objects"]) for call in calls] == [1000, 1000, 500]
    assert all(call["Delete"]["Quiet"] is True for call in calls)


def test_an_object_delete_error_is_counted() -> None:
    """A refused key is counted so a leftover object is not reported as clean."""
    s3 = _Client()
    s3.errors = [{"Key": "k", "Code": "AccessDenied"}]
    assert cleanup.delete_scratch_objects(s3, [("b", "k")]) == 1


def test_s3_tables_deletes_one_table_at_a_time_in_the_scratch_namespace() -> None:
    """Each S3 Tables table is deleted by bucket ARN, namespace and name."""
    client = _Client()
    tables = [_table("testing_r5a1_acc_a"), _table("testing_r5a1_acc_b")]
    assert cleanup.delete_s3tables(client, "arn:x", "testing_repark_acceptance", tables) == []
    assert _deleted(client, "delete_table") == [
        {"tableBucketARN": "arn:x", "namespace": "testing_repark_acceptance", "name": name}
        for name in ("testing_r5a1_acc_a", "testing_r5a1_acc_b")
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


_LISTING = [{"Contents": [{"Key": "testing_repark_acceptance/testing_r4a1_acc_a/f"}]}]


def test_a_dry_run_issues_no_delete_call(capsys: pytest.CaptureFixture[str]) -> None:
    """Without the execute switch the Glue and S3 Tables paths only report counts."""
    tables = [_table("testing_r4a1_acc_a", location=f"{_SCRATCH}/testing_r4a1_acc_a")]
    glue, s3, s3t = _Client(), _Client(_LISTING), _Client()
    cleanup.clean_glue(glue, s3, _WAREHOUSE, tables, execute=False)
    cleanup.clean_s3tables(s3t, "arn", tables, execute=False)
    assert _deleted(glue, "batch_delete_table") == []
    assert _deleted(s3t, "delete_table") == []
    assert _deleted(s3, "delete_objects") == []
    assert "glue: 1 tables, 1 objects selected" in capsys.readouterr().out


def test_an_executed_glue_cleanup_deletes_the_tables_then_the_objects() -> None:
    """The order is the table entries first, then the objects below each table's location."""
    tables = [_table("testing_r4a1_acc_a", location=f"{_SCRATCH}/testing_r4a1_acc_a")]
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
        parser.parse_args(["--run-prefix", "testing_r1a1_acc_", "--older-than-hours", "24"])
    parsed = parser.parse_args(["--older-than-hours", "24", "--execute"])
    assert parsed.execute is True and parsed.run_prefix is None
    assert parser.parse_args(["--run-prefix", "testing_r1a1_acc_"]).execute is False


def test_main_refuses_a_bad_prefix_before_any_client_or_environment_read(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A malformed prefix exits 2 before boto3 is imported or the warehouse is read."""
    monkeypatch.delenv("REPARK_ACCEPT_WAREHOUSE", raising=False)
    monkeypatch.setitem(sys.modules, "boto3", None)
    assert cleanup.main(["--run-prefix", "testing_"]) == 2
    assert "testing_r<digits>a<digits>_<acc" in capsys.readouterr().err


def test_a_run_and_leg_prefix_selects_only_its_own_leg_of_its_own_run() -> None:
    """Leg acc of run 12 takes neither leg u or x of run 12 nor the same leg of run 123."""
    tables = [
        _table("testing_r12a1_acc_a"),
        _table("testing_r12a1_u_a"),
        _table("testing_r12a1_x_a"),
        _table("testing_r123a1_acc_a"),
        _table("testing_r12a1_acc"),
        _table("testing_r12a1_accounts"),
    ]
    chosen = cleanup.select_by_prefix(tables, "testing_r12a1_acc_")
    assert [table.name for table in chosen] == ["testing_r12a1_acc_a"]


def test_the_age_filter_matches_any_leg_and_groups_by_the_full_run_and_leg_prefix() -> None:
    """Old leg u goes while the fresh leg acc of the same run stays; a bare name groups alone."""
    tables = [
        _table("testing_r5a1_u_a", age_hours=30),
        _table("testing_r5a1_u_b", age_hours=29),
        _table("testing_r5a1_acc_a", age_hours=2),
        _table("testing_r6a1_x_a", age_hours=50),
        _table("testing_r7a1_orphan", age_hours=60),
        _table("testing_r8a1_acc_a", age_hours=1),
    ]
    chosen = cleanup.select_older_than(tables, 24, _NOW)
    assert [table.name for table in chosen] == [
        "testing_r5a1_u_a",
        "testing_r5a1_u_b",
        "testing_r6a1_x_a",
        "testing_r7a1_orphan",
    ]


@pytest.mark.parametrize(
    ("prefix", "stem"),
    [
        ("testing_r12a1_acc_", "u12s3path_r12a1_acc_"),
        ("testing_r40079497082a1_u_", "u12s3path_r40079497082a1_u_"),
        ("testing_r0a1_x_", "u12s3path_r0a1_x_"),
    ],
)
def test_the_object_stem_follows_the_run_and_leg_prefix(prefix: str, stem: str) -> None:
    """The path-write stem is derived from the same validated prefix."""
    assert cleanup.path_object_stem(prefix) == stem


def test_the_object_stem_refuses_an_unvalidated_prefix() -> None:
    """A prefix without a leg yields no stem."""
    with pytest.raises(ValueError, match="testing_r<digits>a<digits>_<acc"):
        cleanup.path_object_stem("testing_r12a1_")


def test_path_objects_are_listed_by_the_leg_stem_below_the_scratch_prefix() -> None:
    """The listing prefix is the warehouse scratch prefix plus the leg stem; strays are dropped."""
    base = "testing_repark_acceptance/u12s3path_r12a1_acc_"
    s3 = _Client(
        [
            {"Contents": [{"Key": f"{base}aa/parquet/part-0"}, {"Key": f"{base}bb/x"}]},
            {"Contents": [{"Key": "testing_repark_acceptance/u12s3path_r12a1_acc"}]},
        ]
    )
    keys = cleanup.path_object_keys(s3, _WAREHOUSE, "testing_r12a1_acc_")
    assert keys == [
        ("example-warehouse", f"{base}aa/parquet/part-0"),
        ("example-warehouse", f"{base}bb/x"),
    ]
    assert _deleted(s3, "paginate") == [{"Bucket": "example-warehouse", "Prefix": base}]


@pytest.mark.parametrize("warehouse", ["", "example-warehouse/", "gs://example-warehouse/"])
def test_path_objects_need_an_s3_warehouse(warehouse: str) -> None:
    """A warehouse that is not an s3 location lists nothing and calls nothing."""
    s3 = _Client([{"Contents": [{"Key": "k"}]}])
    assert cleanup.path_object_keys(s3, warehouse, "testing_r12a1_acc_") == []
    assert s3.calls == []


def test_a_dry_run_lists_path_objects_but_deletes_none(capsys: pytest.CaptureFixture[str]) -> None:
    """Without the execute switch only the count is reported."""
    key = "testing_repark_acceptance/u12s3path_r4a1_u_aa/f"
    s3 = _Client([{"Contents": [{"Key": key}]}])
    cleanup.clean_path_objects(s3, _WAREHOUSE, "testing_r4a1_u_", execute=False)
    assert _deleted(s3, "delete_objects") == []
    assert "s3 path objects: 1 selected" in capsys.readouterr().out


def test_an_executed_cleanup_deletes_the_path_objects_in_batches() -> None:
    """2500 path objects go in three batches."""
    page = {
        "Contents": [
            {"Key": f"testing_repark_acceptance/u12s3path_r4a1_u_a/{n}"} for n in range(2500)
        ]
    }
    s3 = _Client([page])
    cleanup.clean_path_objects(s3, _WAREHOUSE, "testing_r4a1_u_", execute=True)
    sizes = [len(call["Delete"]["Objects"]) for call in _deleted(s3, "delete_objects")]
    assert sizes == [1000, 1000, 500]


@pytest.mark.parametrize(
    "warehouse",
    [
        "",
        "gs://bucket/",
        "bucket/",
        "s3://example-warehouse/",
        "s3://example-warehouse",
        "s3://example-warehouse/sub/dir/",
        "s3://example-warehouse-2/",
    ],
)
def test_main_refuses_an_unset_or_placeholder_warehouse_before_boto3(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], warehouse: str
) -> None:
    """The placeholder bucket is squattable, so no client is built against it."""
    monkeypatch.setenv("REPARK_ACCEPT_WAREHOUSE", warehouse)
    monkeypatch.setitem(sys.modules, "boto3", None)
    assert cleanup.main(["--older-than-hours", "24", "--execute"]) == 2
    assert "REPARK_ACCEPT_WAREHOUSE" in capsys.readouterr().err


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


def test_main_dry_run_lists_everything_and_deletes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without --execute all three services are listed and no delete call is issued."""
    _env(monkeypatch)
    factory = _world()
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_"], factory) == 0
    assert _all_deletes(factory) == []
    assert factory.built == ["glue", "s3", "s3tables"]
    for service, operation in (("glue", "get_tables"), ("s3tables", "list_tables")):
        assert ("get_paginator", {"name": operation}) in factory.clients[service].calls
    assert ("get_paginator", {"name": "list_objects_v2"}) in factory.clients["s3"].calls


def test_main_execute_deletes_exactly_the_selected_leg_everywhere(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Glue tables, their guarded objects, the leg's path objects and its S3 Tables tables go."""
    _env(monkeypatch)
    factory = _world()
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 0
    glue = factory.clients["glue"]
    assert _deleted(glue, "batch_delete_table") == [
        {"DatabaseName": "testing_repark_acceptance", "TablesToDelete": ["testing_r12a1_acc_a"]}
    ]
    removed = {
        entry["Key"]
        for call in _deleted(factory.clients["s3"], "delete_objects")
        for entry in call["Delete"]["Objects"]
    }
    assert removed == {
        f"{_WH_ROOT}/testing_r12a1_acc_a/f1",
        f"{_WH_ROOT}/u12s3path_r12a1_acc_zz/p1",
    }
    assert _deleted(factory.clients["s3tables"], "delete_table") == [
        {
            "tableBucketARN": "arn:aws:s3tables:r:1:bucket/b",
            "namespace": "testing_repark_acceptance",
            "name": "testing_r12a1_acc_t",
        }
    ]


def test_main_builds_no_s3_tables_client_without_a_table_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unset TABLE_BUCKET_ARN skips S3 Tables entirely, even with --execute."""
    _env(monkeypatch, None)
    factory = _world()
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 0
    assert "s3tables" not in factory.built
    assert factory.clients["s3tables"].calls == []
    assert _deleted(factory.clients["glue"], "batch_delete_table")


def test_main_age_mode_deletes_old_tables_of_every_leg_and_never_path_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--older-than-hours removes old tables of both legs; the path-write stems are not listed."""
    _env(monkeypatch)
    factory = _world(old=True)
    assert cleanup.main(["--older-than-hours", "24", "--execute"], factory) == 0
    glue_call = _deleted(factory.clients["glue"], "batch_delete_table")
    assert [call["TablesToDelete"] for call in glue_call] == [
        ["testing_r12a1_acc_a", "testing_r12a1_u_a"]
    ]
    prefixes = [call["Prefix"] for call in _deleted(factory.clients["s3"], "paginate")]
    assert all("u12s3path" not in prefix for prefix in prefixes)
    removed = {
        entry["Key"]
        for call in _deleted(factory.clients["s3"], "delete_objects")
        for entry in call["Delete"]["Objects"]
    }
    assert removed == {f"{_WH_ROOT}/testing_r12a1_acc_a/f1", f"{_WH_ROOT}/testing_r12a1_u_a/f2"}
    names = [call["name"] for call in _deleted(factory.clients["s3tables"], "delete_table")]
    assert names == ["testing_r12a1_acc_t", "testing_r12a1_u_t"]


def test_main_exits_two_on_a_bad_prefix_or_a_placeholder_warehouse_without_a_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both refusals return 2 before the factory is called once."""
    _env(monkeypatch)
    factory = _world()
    assert cleanup.main(["--run-prefix", "testing_r12a1_", "--execute"], factory) == 2
    monkeypatch.setenv("REPARK_ACCEPT_WAREHOUSE", "s3://example-warehouse/")
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 2
    assert factory.built == []


class _FailingGlue(_World):
    """A Glue stub whose first batch delete raises."""

    def batch_delete_table(self, **kwargs: Any) -> dict[str, Any]:
        """Raise on the first call and answer normally afterwards."""
        first = not _deleted(self, "batch_delete_table")
        super().batch_delete_table(**kwargs)
        if first:
            raise RuntimeError("throttled")
        return {"Errors": []}


class _FailingS3Tables(_World):
    """An S3 Tables stub whose deletes raise."""

    def delete_table(self, **kwargs: Any) -> dict[str, Any]:
        """Record the call, then raise."""
        super().delete_table(**kwargs)
        raise RuntimeError("denied")


def test_a_failing_first_glue_batch_does_not_stop_the_remaining_batches(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The first 100 names fail, the other 150 are still sent, and the failures are returned."""
    tables = [_table(f"testing_r9a1_acc_{index:03d}") for index in range(250)]
    glue = _FailingGlue({})
    failed = cleanup.delete_glue_tables(glue, "db", tables)
    assert failed == [t.name for t in tables[:100]]
    assert [len(c["TablesToDelete"]) for c in _deleted(glue, "batch_delete_table")] == [
        100,
        100,
        50,
    ]
    assert "RuntimeError" in capsys.readouterr().err


def test_objects_of_a_table_that_failed_to_delete_are_kept() -> None:
    """Only the objects of tables Glue actually deleted are removed."""
    tables = [
        _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/testing_r9a1_acc_a"),
        _table("testing_r9a1_acc_b", location=f"{_SCRATCH}/testing_r9a1_acc_b"),
    ]
    glue = _Client()
    glue.errors = [{"TableName": "testing_r9a1_acc_a", "ErrorDetail": {}}]
    keys = [
        "testing_repark_acceptance/testing_r9a1_acc_a/f",
        "testing_repark_acceptance/testing_r9a1_acc_b/f",
    ]
    s3 = _World({}, keys)
    s3.keys = keys
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, tables, execute=True) == 1
    removed = {e["Key"] for c in _deleted(s3, "delete_objects") for e in c["Delete"]["Objects"]}
    assert removed == {"testing_repark_acceptance/testing_r9a1_acc_b/f"}


def test_main_continues_past_item_failures_and_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A throwing Glue batch and a throwing S3 Tables table still leave the rest deleted; exit 1."""
    _env(monkeypatch)
    factory = _world()
    factory.clients["glue"] = _FailingGlue(factory.clients["glue"].world)
    factory.clients["s3tables"] = _FailingS3Tables(factory.clients["s3tables"].world)
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 1
    assert len(_deleted(factory.clients["glue"], "batch_delete_table")) == 1
    assert _deleted(factory.clients["s3"], "delete_objects")
    assert "deletes failed" in capsys.readouterr().err


def test_a_table_whose_location_is_another_tables_directory_keeps_its_objects(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The last location segment must be the table's own name; otherwise objects are left."""
    stray = _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/testing_r9a1_acc_b")
    own = _table("testing_r9a1_acc_c", location=f"{_SCRATCH}/testing_r9a1_acc_c/")
    keys = [
        "testing_repark_acceptance/testing_r9a1_acc_b/f",
        "testing_repark_acceptance/testing_r9a1_acc_c/f",
    ]
    s3 = _World({}, keys)
    glue = _Client()
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [stray, own], execute=True) == 0
    removed = {e["Key"] for c in _deleted(s3, "delete_objects") for e in c["Delete"]["Objects"]}
    assert removed == {"testing_repark_acceptance/testing_r9a1_acc_c/f"}
    assert _deleted(glue, "batch_delete_table")[0]["TablesToDelete"] == [stray.name, own.name]
    assert "another directory as location" in capsys.readouterr().out


def test_a_table_without_a_location_has_no_objects_listed() -> None:
    """A Glue entry with no storage descriptor costs no S3 call."""
    s3 = _World({}, ["testing_repark_acceptance/testing_r9a1_acc_a/f"])
    cleanup.clean_glue(_Client(), s3, _WAREHOUSE, [_table("testing_r9a1_acc_a")], execute=True)
    assert _deleted(s3, "paginate") == []
    assert _deleted(s3, "delete_objects") == []

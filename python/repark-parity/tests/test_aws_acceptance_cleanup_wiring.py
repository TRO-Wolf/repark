"""Stubbed-client pins for the wiring of scripts/aws_acceptance_cleanup.py; no AWS.

pins: aws-ephemeral-runs-1/C-022, C-023, C-024, C-025, C-026
"""

from __future__ import annotations

from typing import Any

import pytest
from cleanup_stubs import (
    _NOW,
    _SCRATCH,
    _WAREHOUSE,
    _WH,
    _WH_ROOT,
    _all_deletes,
    _Client,
    _deleted,
    _env,
    _glue_entry,
    _table,
    _World,
    _world,
    cleanup,
)


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


def test_a_table_with_an_anomalous_location_is_kept_with_its_objects_and_counted_as_a_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One good and three bad locations: only the good table and its objects go; failures = 3."""
    good = _table("testing_r9a1_acc_g", location=f"{_SCRATCH}/testing_r9a1_acc_g/")
    other = _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/testing_r9a1_acc_b")
    outside = _table("testing_r9a1_acc_o", location="s3://real-bucket/prod/testing_r9a1_acc_o")
    missing = _table("testing_r9a1_acc_m")
    keys = [
        "testing_repark_acceptance/testing_r9a1_acc_g/f",
        "testing_repark_acceptance/testing_r9a1_acc_b/f",
        "prod/testing_r9a1_acc_o/f",
    ]
    s3 = _World({}, keys)
    glue = _Client()
    tables = [good, other, outside, missing]
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, tables, execute=True) == 3
    removed = {e["Key"] for c in _deleted(s3, "delete_objects") for e in c["Delete"]["Objects"]}
    assert removed == {"testing_repark_acceptance/testing_r9a1_acc_g/f"}
    assert [c["TablesToDelete"] for c in _deleted(glue, "batch_delete_table")] == [[good.name]]
    err = capsys.readouterr().err
    for table in (other, outside, missing):
        assert table.name in err
    assert "testing_r9a1_acc_b" in err and "prod/testing_r9a1_acc_o" in err


def test_a_dry_run_also_counts_an_anomalous_location(capsys: pytest.CaptureFixture[str]) -> None:
    """The anomaly is reported and counted without --execute, and nothing is deleted."""
    bad = _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/testing_r9a1_acc_b")
    glue, s3 = _Client(), _World({}, [])
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [bad], execute=False) == 1
    assert _deleted(glue, "batch_delete_table") == []


def test_main_exits_one_and_keeps_a_bad_location_table_everywhere(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end: the good table goes with its objects, the bad one is untouched, exit 1."""
    _env(monkeypatch, None)
    factory = _world()
    glue = factory.clients["glue"]
    glue.world["get_tables"][0]["TableList"][0]["StorageDescriptor"]["Location"] = (
        f"{_WH}testing_repark_acceptance/testing_r12a1_acc_other"
    )
    names = ["testing_r12a1_acc_a", "testing_r12a1_acc_good"]
    glue.world["get_tables"][0]["TableList"][0]["Name"] = names[0]
    good = _glue_entry(names[1])
    glue.world["get_tables"][0]["TableList"].append(good)
    factory.clients["s3"].keys.append(f"{_WH_ROOT}/{names[1]}/f9")
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 1
    assert [c["TablesToDelete"] for c in _deleted(glue, "batch_delete_table")] == [[names[1]]]
    removed = {
        e["Key"]
        for c in _deleted(factory.clients["s3"], "delete_objects")
        for e in c["Delete"]["Objects"]
    }
    assert f"{_WH_ROOT}/{names[1]}/f9" in removed
    assert not any("other" in key or names[0] in key for key in removed)


def test_an_alias_twin_goes_without_a_second_object_delete(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An _adopted twin points at the original's directory: both entries go, the files once."""
    original = _table("testing_r9a1_acc_v3", location=f"{_SCRATCH}/testing_r9a1_acc_v3")
    twin = _table("testing_r9a1_acc_v3_adopted", location=f"{_SCRATCH}/testing_r9a1_acc_v3/")
    key = "testing_repark_acceptance/testing_r9a1_acc_v3/f"
    s3 = _World({}, [key])
    glue = _Client()
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [twin, original], execute=True) == 0
    assert sorted(_deleted(glue, "batch_delete_table")[0]["TablesToDelete"]) == [
        original.name,
        twin.name,
    ]
    keys = [e["Key"] for c in _deleted(s3, "delete_objects") for e in c["Delete"]["Objects"]]
    assert keys == [key]
    assert capsys.readouterr().err == ""


def test_a_twin_of_a_table_outside_the_selected_set_is_an_anomaly(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A location naming a table that is not selected keeps the entry, reports it, counts it."""
    twin = _table("testing_r9a1_acc_v3_adopted", location=f"{_SCRATCH}/testing_r9a1_acc_v3")
    s3 = _World({}, ["testing_repark_acceptance/testing_r9a1_acc_v3/f"])
    glue = _Client()
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [twin], execute=True) == 1
    assert _deleted(glue, "batch_delete_table") == []
    assert _deleted(s3, "delete_objects") == []
    assert twin.name in capsys.readouterr().err


def test_main_deletes_an_original_and_its_twin_with_exit_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end: the leg's v3 original and its _adopted twin both go, exit 0."""
    _env(monkeypatch, None)
    factory = _world()
    entries = factory.clients["glue"].world["get_tables"][0]["TableList"]
    twin = _glue_entry("testing_r12a1_acc_a_adopted")
    twin["StorageDescriptor"]["Location"] = f"{_WH}testing_repark_acceptance/testing_r12a1_acc_a"
    entries.append(twin)
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 0
    batch = _deleted(factory.clients["glue"], "batch_delete_table")
    assert sorted(batch[0]["TablesToDelete"]) == [
        "testing_r12a1_acc_a",
        "testing_r12a1_acc_a_adopted",
    ]
    keys = [
        e["Key"]
        for c in _deleted(factory.clients["s3"], "delete_objects")
        for e in c["Delete"]["Objects"]
    ]
    assert keys.count(f"{_WH_ROOT}/testing_r12a1_acc_a/f1") == 1


@pytest.mark.parametrize(
    "leaf", ["xtesting_r9a1_acc_a", "testing_r9a1_acc_ab", "TESTING_R9A1_ACC_B"]
)
def test_a_location_leaf_that_only_contains_or_extends_the_name_is_an_anomaly(leaf: str) -> None:
    """Exact equality: neither a longer nor a shorter segment counts as the table's own."""
    table = _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/{leaf}")
    glue, s3 = _Client(), _World({}, [f"testing_repark_acceptance/{leaf}/f"])
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [table], execute=True) == 1
    assert _deleted(glue, "batch_delete_table") == []
    assert _deleted(s3, "delete_objects") == []


def test_a_location_leaf_that_ends_with_the_name_is_an_anomaly() -> None:
    """The mutant form `endswith(name)` would accept `x<name>`; this pins the exact compare."""
    table = _table("a_b", location=f"{_SCRATCH}/xa_b")
    assert cleanup.clean_glue(_Client(), _World({}, []), _WAREHOUSE, [table], True) == 1


def test_an_s3_tables_delete_exception_alone_makes_main_exit_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With Glue and S3 healthy, one raising S3 Tables delete still yields exit 1."""
    _env(monkeypatch)
    factory = _world()
    factory.clients["s3tables"] = _FailingS3Tables(factory.clients["s3tables"].world)
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 1


def test_a_failed_s3_object_delete_is_a_failure_in_the_table_and_path_object_cleanups() -> None:
    """An Errors list from delete_objects counts, for table objects and for path objects."""
    table = _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/testing_r9a1_acc_a")
    keys = [
        "testing_repark_acceptance/testing_r9a1_acc_a/f",
        "testing_repark_acceptance/u12s3path_r9a1_acc_zz/p",
    ]
    s3 = _World({}, keys)
    s3.errors = [{"Key": "k", "Code": "AccessDenied"}]
    assert cleanup.clean_glue(_Client(), s3, _WAREHOUSE, [table], execute=True) == 1
    assert cleanup.clean_path_objects(s3, _WAREHOUSE, "testing_r9a1_acc_", execute=True) == 1


def test_main_exits_one_when_only_the_object_deletes_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tables delete fine; refused object batches alone make the run exit 1."""
    _env(monkeypatch, None)
    factory = _world()
    factory.clients["s3"].errors = [{"Key": "k", "Code": "AccessDenied"}]
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 1


@pytest.mark.parametrize(
    "name", ["testing_rab1a1_acc_x", "testing_r12ax_acc_x", "testing_r1a1b_acc_x"]
)
def test_names_with_letters_in_the_run_or_attempt_digits_are_never_grouped_or_selected(
    name: str,
) -> None:
    """Only ASCII digits form a run id or attempt, in prefix mode and in age mode."""
    old = _table(name, age_hours=999)
    assert cleanup.run_prefix_of(name) is None
    assert cleanup.select_older_than([old], 24, _NOW) == []
    assert cleanup.select_by_prefix([old], "testing_r12a1_acc_") == []


def test_a_name_that_only_contains_the_run_shape_is_never_selected() -> None:
    """The shape is anchored at the start of the name, in prefix mode and in age mode."""
    evil = _table("xtesting_r12a1_acc_evil", age_hours=999)
    assert cleanup.run_prefix_of(evil.name) is None
    assert cleanup.select_by_prefix([evil], "testing_r12a1_acc_") == []
    assert cleanup.select_older_than([evil], 24, _NOW) == []
    with pytest.raises(ValueError, match="not a testing_r<digits>a<digits>_"):
        cleanup.assert_deletable(evil.name)


class _RaisingObjects(_World):
    """An S3 stub whose first delete_objects call raises."""

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        """Raise on the first call and answer normally afterwards."""
        first = not _deleted(self, "delete_objects")
        super().delete_objects(**kwargs)
        if first:
            raise RuntimeError("ClientError: throttled")
        return {"Errors": []}


def test_a_raising_object_batch_is_counted_and_the_other_batches_still_go() -> None:
    """The first of three batches raises: 1000 counted, the other two are still sent."""
    objects = [("example-warehouse", f"testing_repark_acceptance/t/{n}") for n in range(2500)]
    s3 = _RaisingObjects({})
    assert cleanup.delete_scratch_objects(s3, objects) == 1000
    assert [len(c["Delete"]["Objects"]) for c in _deleted(s3, "delete_objects")] == [
        1000,
        1000,
        500,
    ]


def test_main_exits_one_when_an_object_delete_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tables delete fine; a raising object batch alone makes the run exit 1."""
    _env(monkeypatch, None)
    factory = _world()
    factory.clients["s3"] = _RaisingObjects({}, factory.clients["s3"].keys)
    assert cleanup.main(["--run-prefix", "testing_r12a1_acc_", "--execute"], factory) == 1


def test_a_location_leaf_that_differs_from_the_name_only_in_case_is_an_anomaly() -> None:
    """The compare is case-sensitive: `Testing_r9a1_acc_a` is not `testing_r9a1_acc_a`."""
    table = _table("testing_r9a1_acc_a", location=f"{_SCRATCH}/Testing_r9a1_acc_a")
    glue = _Client()
    s3 = _World({}, ["testing_repark_acceptance/Testing_r9a1_acc_a/f"])
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [table], execute=True) == 1
    assert _deleted(glue, "batch_delete_table") == []
    assert _deleted(s3, "delete_objects") == []


def test_age_mode_ignores_the_attempt_less_old_shape() -> None:
    """`testing_r12_acc_…` has no attempt: it is never grouped or selected by age."""
    old = _table("testing_r12_acc_x", age_hours=999)
    assert cleanup.run_prefix_of(old.name) is None
    assert cleanup.select_older_than([old], 24, _NOW) == []


def test_a_twin_of_a_twin_is_deleted_without_a_second_object_delete(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Original, twin A on the original, twin B on twin A: three entries, the files once."""
    original = _table("testing_r9a1_acc_v3", location=f"{_SCRATCH}/testing_r9a1_acc_v3")
    twin_a = _table("testing_r9a1_acc_v3_a", location=f"{_SCRATCH}/testing_r9a1_acc_v3")
    twin_b = _table("testing_r9a1_acc_v3_b", location=f"{_SCRATCH}/testing_r9a1_acc_v3_a")
    key = "testing_repark_acceptance/testing_r9a1_acc_v3/f"
    s3, glue = _World({}, [key]), _Client()
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [twin_b, twin_a, original], True) == 0
    assert sorted(_deleted(glue, "batch_delete_table")[0]["TablesToDelete"]) == sorted(
        [original.name, twin_a.name, twin_b.name]
    )
    keys = [e["Key"] for c in _deleted(s3, "delete_objects") for e in c["Delete"]["Objects"]]
    assert keys == [key]
    assert capsys.readouterr().err == ""


def test_a_nested_location_is_an_anomaly_and_a_direct_child_is_not() -> None:
    """`<scratch>/testing_appointment/<name>` has the right leaf but the wrong parent."""
    nested = _table(
        "testing_r9a1_acc_a", location=f"{_SCRATCH}/testing_appointment/testing_r9a1_acc_a"
    )
    direct = _table("testing_r9a1_acc_b", location=f"{_SCRATCH}/testing_r9a1_acc_b")
    keys = [
        "testing_repark_acceptance/testing_appointment/testing_r9a1_acc_a/f",
        "testing_repark_acceptance/testing_r9a1_acc_b/f",
    ]
    glue, s3 = _Client(), _World({}, keys)
    assert cleanup.clean_glue(glue, s3, _WAREHOUSE, [nested, direct], execute=True) == 1
    assert _deleted(glue, "batch_delete_table")[0]["TablesToDelete"] == [direct.name]
    removed = {e["Key"] for c in _deleted(s3, "delete_objects") for e in c["Delete"]["Objects"]}
    assert removed == {keys[1]}

"""LIVE-v3-M: the documents carry the first live v3 measurement; none of them still says pending.

pins: live-v3-first-measurement/C-001, C-002, C-003
MUTATION: restore ❌ on the north-star row, drop run 33635288918 from the registry row or from
STATUS, or drop a tier2-aws §6 leg row → this REDs.
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_GLUE_LEG = "test_v3_dv_dml_maintenance_against_glue"
_S3T_LEG = "test_v3_dv_dml_maintenance_against_s3tables"
_RUN_ID = "33635288918"
_RUN_LINK = f"https://github.com/TRO-Wolf/repark/actions/runs/{_RUN_ID}"
_REGISTRY_HEADING = "### S3T-V3-1 — FIXED (LIVE-v3-M, 2026-09-02): both live v3 legs are green"
_STALE = (
    "unmeasured",
    "nothing has run against AWS",
    "the first measurement is pending",
    "not yet run",
)


def _read(relative: str) -> str:
    """The whole document at ``relative``."""
    return (_REPO / relative).read_text(encoding="utf-8")


def _normalized(text: str) -> str:
    """``text`` with every run of whitespace collapsed to one space."""
    return " ".join(text.split())


def _northstar_row(label: str) -> str:
    """The north-star §3 matrix row that starts with ``| <label>``."""
    for line in _read("task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md").splitlines():
        if line.startswith(f"| {label}"):
            return line
    raise AssertionError(f"north-star §3 has no row starting with `| {label}`")


def _registry_row(heading: str, next_heading: str) -> str:
    """The whitespace-normalized registry text from ``heading`` up to ``next_heading``."""
    registry = _read("docs/spark-sql-iceberg-parity.md")
    assert heading in registry, heading
    return _normalized(registry[registry.index(heading) : registry.index(next_heading)])


def _status_live_v3_clause() -> str:
    """The whitespace-normalized STATUS sentence that carries the LIVE-v3 state."""
    status = _normalized(_read("STATUS.md"))
    start = status.index("**LIVE-v3 (2026-09-02):**")
    return status[start : status.index("- **Next:**", start)]


def test_registry_row_records_the_measured_run() -> None:
    """S3T-V3-1 is FIXED, names the run and its link, and keeps its local pins."""
    row = _registry_row(_REGISTRY_HEADING, "### V3-COW-1")
    assert _RUN_ID in row and _RUN_LINK in row
    assert "8c4bc55" in row
    assert "6 passed in 122.13s" in row
    assert _GLUE_LEG in row and _S3T_LEG in row
    assert "reproduced the local engine's numbers exactly" in row
    assert "took the decision table's accepted branch" in row
    assert "`exact_commit_counts=False`" in row
    assert "S3T-1" in row and "R126" in row
    assert "**Rationale** — FIXED by measurement" in row
    assert "is no longer BACKLOG" in row
    assert "python/repark/tests/test_v3_acceptance_local.py" in row


def test_northstar_live_row_is_green_and_dated() -> None:
    """The Live row is ✅, names the run, both legs and the registry row, and keeps MW-10."""
    row = _northstar_row("Live: Glue + S3 Tables v3 legs")
    assert "✅" in row
    assert "❌" not in row
    assert _RUN_ID in row and _RUN_LINK in row
    assert _GLUE_LEG in row and _S3T_LEG in row
    assert "S3T-V3-1" in row
    assert "python/repark/tests/test_v3_acceptance_local.py" in row
    assert "MW-10 is format v2" in row
    assert "2026-08-30-mw-10-s3tables-mor-ledger.md" in row
    assert "33333274383" in row


def test_no_document_still_calls_the_legs_unmeasured() -> None:
    """The registry row, the north-star row and the STATUS clause carry no pending wording."""
    scopes = {
        "registry": _registry_row(_REGISTRY_HEADING, "### V3-COW-1"),
        "north star": _northstar_row("Live: Glue + S3 Tables v3 legs"),
        "STATUS": _status_live_v3_clause(),
    }
    for where, text in scopes.items():
        for phrase in _STALE:
            assert phrase not in text, f"{where} still says {phrase!r}"


def test_tier2_runbook_lists_every_leg_and_needs_no_new_iam() -> None:
    """tier2-aws §6 carries one row per leg, answers the v3 questions, and widens nothing."""
    runbook = _read("docs/tier2-aws.md")
    assert "## 6. The legs this workflow runs" in runbook
    section = _normalized(runbook[runbook.index("## 6. The legs this workflow runs") :])
    for leg in (
        "test_process_silver_acceptance_against_glue",
        "test_process_silver_acceptance_against_s3tables",
        "test_mor_merge_compact_expire_against_glue",
        "test_mor_merge_compact_expire_against_s3tables",
        _GLUE_LEG,
        _S3T_LEG,
    ):
        assert f"`{leg}`" in section, leg
    assert "LIVE-v3, answered 2026-09-02: Glue reproduces the local v3 numbers exactly" in section
    assert "answered 2026-09-02: S3 Tables accepts `format-version = 3` at CREATE" in section
    assert "no new IAM action and no new workflow variable" in section
    assert "lives in [../STATUS.md](../STATUS.md) — never here" in section
    assert _RUN_ID not in section


def test_the_leg_tests_exist_where_the_documents_say() -> None:
    """Every cited leg is a real ``def`` in the gated harness, and the local pin exists."""
    harness = _read("python/repark/tests/test_aws_acceptance.py")
    assert f"def {_GLUE_LEG}(" in harness
    assert f"def {_S3T_LEG}(" in harness
    local = _read("python/repark/tests/test_v3_acceptance_local.py")
    assert "def test_v3_acceptance_leg_body_against_the_local_catalog(" in local
    assert "def test_v3_create_refusal_classification_is_the_s3_tables_decision_table(" in local
    twins = _read("python/repark/tests/test_acceptance_v3_helpers.py")
    assert "def test_v3_legs_are_twins_of_the_mor_legs(" in twins
    registry = _read("docs/spark-sql-iceberg-parity.md")
    assert "python/repark/tests/test_acceptance_v3_helpers.py" in registry
    assert Path(_REPO / "python/repark/tests/_acceptance_v3.py").is_file()


def test_status_names_the_measured_legs() -> None:
    """STATUS carries the LIVE-v3 run, the V3-ROWID-3 line, and stays under its ceiling."""
    status = _normalized(_read("STATUS.md"))
    assert "**LIVE-v3 (2026-09-02):**" in status
    assert f"run {_RUN_ID}" in status
    assert "both live v3 legs green" in status
    assert "V3-11" in status
    assert "`V3-ROWID-3` FIXED" in status
    assert (_REPO / "STATUS.md").stat().st_size <= 25_000


def test_v3_rowid_3_row_is_fixed_and_carries_the_decoded_spark_order() -> None:
    """V3-ROWID-3 is FIXED by V3-11, with Spark's decoded file order and the ten-run reading."""
    heading = (
        "### V3-ROWID-3 — FIXED (V3-11, 2026-09-02): the merge-on-read MERGE insert's `_row_id`"
    )
    row = _registry_row(heading, "### BL-9")
    assert "**FIXED 2026-09-02 (V3-11).**" in row
    assert "10 identical runs" in row
    assert "10 of 10" in row
    assert "PySpark 4.1.2" in row and "1.11.0" in row
    assert "test_v3_acceptance_local.py" in row and "assert_v3_lineage" in row
    assert "V3_EXPECTED_INSERTED_ROW_ID = 11" in row
    assert "JavaHashes$StructLikeHash.hash" in row
    assert "163098 + fieldHash" in row
    assert "BACKLOG" not in row


def test_the_partition_file_order_residual_names_the_fork_as_owner() -> None:
    """`F-v3-10-partition-file-order` stays open, re-measured, and asks the fork as F-20."""
    registry = _normalized(_read("docs/spark-sql-iceberg-parity.md"))
    assert "`F-v3-10-partition-file-order` re-measured 2026-09-02 by V3-11" in registry
    assert "**Owner: the fork.**" in registry
    assert "IcebergTableProvider::insert_into" in registry
    assert "fork ask is **F-20**" in registry
    assert "F-20 matches **RePark's** rule, not Spark's" in registry


def test_v3_fileorder_1_declares_the_rule_and_where_spark_parts_company() -> None:
    """V3-FILEORDER-1 carries the decode, the collision caveat and the measured arm table."""
    heading = (
        "### V3-FILEORDER-1 — DECLARED (V3-11, 2026-09-02): same-commit data-file order is "
        "ascending partition value, not Spark's hash-bucket order"
    )
    row = _registry_row(heading, "### V3-UPGRADE-1")
    assert "JavaHashes$StructLikeHash.hash" in row
    assert "163098 + fieldHash" in row
    assert "fall back to **insertion order**" in row
    assert "arrival-**independent** only while no two partitions collide" in row
    for arm in (
        "`{0..4}`",
        "`{a..e}`",
        "two-field",
        "truncate(1, part)",
        "bucket(4, part)",
        "days(d)",
        "{0, NULL, 1}",
    ):
        assert arm in row, arm
    assert "unmaintainable anti-feature" in row
    assert "a_null_partition_slot_is_numbered_first_whatever_order_it_arrives_in" in row


def test_the_maintenance_oracle_note_has_one_home_and_is_true() -> None:
    """The retired `DataSourceV2Relation` note lives once, under MOR-1, and is dated."""
    registry = _read("docs/spark-sql-iceberg-parity.md")
    assert registry.count("this registry carried on six rows applies nowhere") == 1
    assert registry.count("the `DataSourceV2Relation` note this row used to carry is retired") == 5
    assert "the pinned 4.1.2 + 1.11.0 oracle executes all five" in _normalized(registry)


def test_format_v3_track_claims_carry_their_dated_corrections() -> None:
    """The design note's two "not measured" claims each carry a dated correction."""
    track = _normalized(_read("docs/design/format-v3-track.md"))
    assert "It was not exercised against a table with expirable snapshots." in track
    assert "**Corrected 2026-09-02 (LIVE-v3):**" in track
    assert "`14 → 1`" in track
    assert "python/repark/tests/test_v3_acceptance_local.py" in track
    assert "**Nothing was measured on Glue or S3 Tables.**" in track
    assert "**Corrected 2026-09-02 (LIVE-v3-M):** both services are measured now" in track
    assert _RUN_ID in track

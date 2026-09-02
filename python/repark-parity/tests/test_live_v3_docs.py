"""LIVE-v3: the documents say the legs are wired and unmeasured, and nothing claims green.

pins: live-v3-aws-legs/C-004, C-005
MUTATION: mark the north-star row ✅, drop the S3T-V3-1 row, or drop a tier2-aws §6 leg row
→ this REDs.
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_GLUE_LEG = "test_v3_dv_dml_maintenance_against_glue"
_S3T_LEG = "test_v3_dv_dml_maintenance_against_s3tables"


def _read(relative: str) -> str:
    """The whole document at ``relative``."""
    return (_REPO / relative).read_text(encoding="utf-8")


def _northstar_row(label: str) -> str:
    """The north-star §3 matrix row that starts with ``| <label>``."""
    for line in _read("task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md").splitlines():
        if line.startswith(f"| {label}"):
            return line
    raise AssertionError(f"north-star §3 has no row starting with `| {label}`")


def test_registry_row_states_a_pending_measurement() -> None:
    """S3T-V3-1 exists, cites the local pin, and makes no green claim."""
    registry = _read("docs/spark-sql-iceberg-parity.md")
    heading = "### S3T-V3-1 — the live v3 legs are wired (2026-09-02); the first measurement"
    assert heading in registry
    row = " ".join(registry[registry.index(heading) : registry.index("### V3-COW-1")].split())
    assert "nothing has run against AWS yet" in row
    assert "python/repark/tests/test_v3_acceptance_local.py" in row
    assert _GLUE_LEG in row and _S3T_LEG in row
    assert "S3T-1" in row and "R126" in row
    assert "exact_commit_counts=False" in row
    assert "no green claim is made before then" in row


def test_northstar_live_row_stays_unmeasured() -> None:
    """The Live row is still ❌, names both legs, and points at the registry row."""
    row = _northstar_row("Live: Glue + S3 Tables v3 legs")
    assert "❌" in row
    assert "✅" not in row
    assert _GLUE_LEG in row and _S3T_LEG in row
    assert "S3T-V3-1" in row
    assert "python/repark/tests/test_v3_acceptance_local.py" in row
    assert "the v3 live legs themselves stay unmeasured" in row


def test_tier2_runbook_lists_every_leg_and_needs_no_new_iam() -> None:
    """tier2-aws §6 carries one row per leg and states the two v3 legs widen nothing."""
    runbook = _read("docs/tier2-aws.md")
    assert "## 6. The legs this workflow runs" in runbook
    section = " ".join(runbook[runbook.index("## 6. The legs this workflow runs") :].split())
    for leg in (
        "test_process_silver_acceptance_against_glue",
        "test_process_silver_acceptance_against_s3tables",
        "test_mor_merge_compact_expire_against_glue",
        "test_mor_merge_compact_expire_against_s3tables",
        _GLUE_LEG,
        _S3T_LEG,
    ):
        assert f"`{leg}`" in section, leg
    assert "no new IAM action and no new workflow variable" in section
    assert "lives in [../STATUS.md](../STATUS.md) — never here" in section


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


def test_status_names_the_wired_but_unmeasured_legs() -> None:
    """STATUS carries the LIVE-v3 state and the V3-ROWID-3 line, and stays under its ceiling."""
    status = " ".join(_read("STATUS.md").split())
    assert "**LIVE-v3 (2026-09-02):**" in status
    assert "the two live v3 legs are wired" in status
    assert "**unmeasured** until the nightly `aws-acceptance` run (`S3T-V3-1`)" in status
    assert "V3-11" in status
    assert "[V3-ROWID-3](docs/spark-sql-iceberg-parity.md)" in status
    assert (_REPO / "STATUS.md").stat().st_size <= 25_000


def test_v3_rowid_3_row_carries_both_readings_and_names_the_follow_up() -> None:
    """V3-ROWID-3 is a BACKLOG row with repark's and Spark's measured answers and unit V3-11."""
    registry = _read("docs/spark-sql-iceberg-parity.md")
    heading = "### V3-ROWID-3 — the merge-on-read MERGE insert's `_row_id` is nondeterministic"
    assert heading in registry
    row = " ".join(registry[registry.index(heading) : registry.index("### BL-9")].split())
    assert "10 identical runs" in row
    assert "**six** times" in row and "**four** times" in row
    assert "10 of 10" in row
    assert "PySpark 4.1.2" in row and "1.11.0" in row
    assert "test_v3_acceptance_local.py" in row and "assert_v3_lineage" in row
    assert "**V3-11**" in row
    assert "BACKLOG" in row


def test_format_v3_track_expire_claim_is_corrected() -> None:
    """The design note's "not exercised against expirable snapshots" carries the dated fix."""
    track = " ".join(_read("docs/design/format-v3-track.md").split())
    assert "It was not exercised against a table with expirable snapshots." in track
    assert "**Corrected 2026-09-02 (LIVE-v3):**" in track
    assert "`14 → 1`" in track
    assert "python/repark/tests/test_v3_acceptance_local.py" in track

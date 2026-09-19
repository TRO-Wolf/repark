"""ICE-READ-PERF-0 pins over the dispatch-only bench job of ``aws-acceptance.yml``.

pins: ice-read-perf-0/C-018, C-019
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_AWS_YML = _REPO / ".github" / "workflows" / "aws-acceptance.yml"

LIVE_AWS_CONDITION = "github.event_name == 'schedule' || inputs.leg == 'acceptance'"
BENCH_CONDITION = "github.event_name == 'workflow_dispatch' && inputs.leg == 'ice-read-perf-bench'"
UPLOAD_ARTIFACT = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
MODES = ("cold", "warm", "concurrent", "concurrent-cold")
ORDERED_MARKERS = (
    "actions/checkout@",
    "dtolnay/rust-toolchain@",
    "Swatinem/rust-cache@",
    "--bench ice_read_perf --no-run",
    "aws-actions/configure-aws-credentials@",
    "setup --catalog s3tables",
    "put-table-maintenance-configuration",
    "get-table-maintenance-configuration",
    "--phase write",
    "setup --catalog glue",
    'run --mode "${mode}" --catalog s3tables',
    'run --mode "${mode}" --catalog glue',
    "run_io_total.bytes",
    UPLOAD_ARTIFACT,
)


def _text() -> str:
    """Return the workflow text."""
    return _AWS_YML.read_text(encoding="utf-8")


def _job_block(text: str, job: str) -> str:
    """Return the body of ``job`` (everything up to the next two-space key)."""
    match = re.search(rf"(?ms)^  {re.escape(job)}:\s*\n(.*?)(?=^  \S|\Z)", text)
    assert match is not None, f"no job {job}"
    return match.group(1)


def _job_if(block: str) -> str:
    """Return the job-level ``if:`` expression."""
    match = re.search(r"(?m)^    if:\s*(.+?)\s*$", block)
    assert match is not None, "job has no if:"
    return match.group(1)


def _steps(block: str) -> list[str]:
    """Split a job body into its step chunks."""
    return re.split(r"(?m)^      - ", block)[1:]


def _uses_pins(block: str) -> dict[str, str]:
    """Map each action used in ``block`` to its pinned ref."""
    return dict(re.findall(r"uses:\s*([\w./-]+)@([0-9a-f]{40})", block))


def test_the_nightly_job_runs_only_on_the_schedule_or_the_acceptance_leg() -> None:
    """The existing job keeps its nightly and gains only the leg filter."""
    text = _text()
    assert _job_if(_job_block(text, "live-aws")) == LIVE_AWS_CONDITION
    assert 'cron: "43 8 * * *"' in text


def test_the_dispatch_offers_the_two_legs_with_acceptance_by_default() -> None:
    """``leg`` is a choice defaulting to acceptance; ``purpose`` is a free string."""
    text = _text()
    dispatch = text[text.index("  workflow_dispatch:") : text.index("\npermissions:")]
    assert re.search(r"(?m)^      leg:\s*$", dispatch)
    assert "type: choice" in dispatch
    assert re.search(r"(?m)^          - acceptance\s*$", dispatch)
    assert re.search(r"(?m)^          - ice-read-perf-bench\s*$", dispatch)
    assert re.search(r"(?m)^        default: acceptance\s*$", dispatch)
    assert re.search(r"(?m)^      purpose:\s*$", dispatch)


def test_the_bench_job_runs_only_on_its_dispatch_behind_the_same_gate() -> None:
    """Dispatch-only, the aws-acceptance environment, the ref guard, job-scoped OIDC."""
    block = _job_block(_text(), "ice-read-perf-bench")
    assert _job_if(block) == BENCH_CONDITION
    assert re.search(r"(?m)^    environment: aws-acceptance\s*$", block)
    assert re.search(r"(?m)^      id-token: write\s*$", block)
    assert "if: github.ref != 'refs/heads/main'" in block
    assert "persist-credentials: false" in block
    assert "continue-on-error" not in block


def test_the_bench_job_uses_the_same_pinned_actions_as_the_acceptance_job() -> None:
    """Every action the bench shares with live-aws sits at the same SHA."""
    text = _text()
    live = _uses_pins(_job_block(text, "live-aws"))
    bench = _uses_pins(_job_block(text, "ice-read-perf-bench"))
    for action in (
        "actions/checkout",
        "dtolnay/rust-toolchain",
        "Swatinem/rust-cache",
        "aws-actions/configure-aws-credentials",
    ):
        assert bench[action] == live[action], action
    assert f"uses: {UPLOAD_ARTIFACT}" in _job_block(text, "ice-read-perf-bench")


def test_the_bench_builds_before_credentials_and_disables_compaction_before_any_write() -> None:
    """The step order: build, credentials, create, compaction off and read back, write, runs."""
    block = _job_block(_text(), "ice-read-perf-bench")
    positions = [block.index(marker) for marker in ORDERED_MARKERS]
    assert positions == sorted(positions), list(zip(ORDERED_MARKERS, positions, strict=True))
    assert "--type icebergCompaction" in block
    assert '--value \'{"status":"disabled"}\'' in block
    assert "--query 'configuration.icebergCompaction.status'" in block
    assert '!= "disabled"' in block
    assert "s3tables:PutTableMaintenanceConfiguration" in block
    assert "s3tables:GetTableMaintenanceConfiguration" in block


def test_every_mode_runs_on_both_catalogs_at_three_repeats_with_r3_failing_the_job() -> None:
    """Four modes per catalog, ``--repeat 3``, every step under ``set -e`` semantics."""
    block = _job_block(_text(), "ice-read-perf-bench")
    assert re.search(r'(?m)^      BENCH_REPEAT: "3"\s*$', block)
    assert block.count(f"for mode in {' '.join(MODES)}; do") == 2
    assert block.count('--repeat "${BENCH_REPEAT}"') == 2
    assert "BENCH_NAMESPACE: testing_repark_acceptance" in block
    assert "BENCH_TABLE: ice_read_perf_bench" in block
    for step in _steps(block):
        if "run: |" in step and ("cargo bench" in step or "aws s3tables" in step):
            assert "set -euo pipefail" in step, step


def test_no_expression_reaches_a_run_script_and_the_job_carries_no_comment() -> None:
    """Inputs, vars and secrets pass through ``env:``; no ``#`` comment in the new job."""
    block = _job_block(_text(), "ice-read-perf-bench")
    for step in _steps(block):
        if "run:" in step:
            script = step[step.index("run:") :]
            assert "${{" not in script, script
    for line in block.splitlines():
        assert not line.lstrip().startswith("#"), line
        assert not re.search(r"@[0-9a-f]{40}\s+#", line), line

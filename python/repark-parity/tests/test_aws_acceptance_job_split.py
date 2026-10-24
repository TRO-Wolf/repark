"""AWS-EPHEMERAL-RUNS-1 pins over the build, leg and backstop jobs of ``aws-acceptance.yml``.

pins: aws-ephemeral-runs-1/C-012, C-013, C-014, C-027
"""

from __future__ import annotations

import re

import pytest
from test_ice_read_perf_bench_workflow import _job_block, _job_if, _run_script, _steps, _text

LEGS = {"acceptance-dbt": "acc", "integrity-u": "u", "integrity-x": "x"}
BOTO_PIN = re.compile(r"boto3==\d+\.\d+\.\d+(?!\S)")
CREDENTIALS = "aws-actions/configure-aws-credentials@"
ALWAYS_ON_MAIN = "${{ always() && github.ref == 'refs/heads/main' }}"
LIVE_ON_MAIN = "${{ !cancelled() && github.ref == 'refs/heads/main' }}"


def _timeout(block: str) -> int:
    """The job's ``timeout-minutes``."""
    match = re.search(r"(?m)^    timeout-minutes:\s*(\d+)\s*$", block)
    assert match is not None
    return int(match.group(1))


def _needs(block: str) -> str:
    """The job's ``needs:`` value, bracketed list or scalar."""
    match = re.search(r"(?m)^    needs:\s*(.+?)\s*$", block)
    assert match is not None
    return match.group(1)


def _step_named(block: str, prefix: str) -> str:
    """The step whose ``name:`` starts with ``prefix``."""
    return next(step for step in _steps(block) if step.startswith(f"name: {prefix}"))


def test_the_jobs_chain_build_then_three_legs_then_the_backstop() -> None:
    """Legs need only build; the backstop needs all three and runs even when one fails."""
    text = _text()
    assert "needs:" not in _job_block(text, "build")
    for job in LEGS:
        assert _needs(_job_block(text, job)) == "build", job
    backstop = _job_block(text, "backstop")
    assert _needs(backstop) == "[acceptance-dbt, integrity-u, integrity-x]"
    assert _job_if(backstop).startswith("${{ always() && (")
    assert "inputs.leg == 'acceptance'" in _job_if(backstop)


def test_the_timeouts_are_sixty_ninety_ninety_ninety_fifteen() -> None:
    """Build 60, each leg 90, backstop 15."""
    text = _text()
    assert _timeout(_job_block(text, "build")) == 60
    for job in LEGS:
        assert _timeout(_job_block(text, job)) == 90, job
    assert _timeout(_job_block(text, "backstop")) == 15


def test_the_build_job_holds_no_credential_and_uploads_the_pinned_wheel_artifact() -> None:
    """No environment, no OIDC, no secret, no AWS action; wheel and lock go up by full SHA."""
    block = _job_block(_text(), "build")
    assert "id-token" not in block
    assert "environment:" not in block
    assert "secrets." not in block
    assert CREDENTIALS not in block
    assert "uv sync --locked --extra record" in block
    assert "uvx maturin@1.14.1 build --release" in block
    assert "cp uv.lock wheel/uv.lock" in block
    assert "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9" in block
    assert "if-no-files-found: error" in block


@pytest.mark.parametrize("job", LEGS)
def test_a_leg_installs_the_wheel_and_boto3_before_it_mints_and_never_compiles(job: str) -> None:
    """Download, locked sync without the project, wheel, exact boto3 pin, then the first mint."""
    block = _job_block(_text(), job)
    assert "actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333" in block
    assert "uv sync --locked --extra record --no-install-package repark" in block
    assert "uv pip install --python .venv/bin/python --no-deps wheel/repark-*.whl" in block
    assert BOTO_PIN.search(block), job
    assert "maturin" not in block
    assert "rust-toolchain" not in block
    first_mint = block.index(CREDENTIALS)
    for marker in ("download-artifact", "--no-deps wheel/repark-", "boto3=="):
        assert block.index(marker) < first_mint, (job, marker)


@pytest.mark.parametrize("job", LEGS)
def test_a_leg_owns_one_run_and_leg_pair_through_its_environment(job: str) -> None:
    """The job env carries the run id and the leg that name the tables and the cleanup."""
    block = _job_block(_text(), job)
    assert "      REPARK_ACCEPT_RUN_ID: ${{ github.run_id }}\n" in block
    assert "      REPARK_ACCEPT_RUN_ATTEMPT: ${{ github.run_attempt }}\n" in block
    assert f"      REPARK_ACCEPT_LEG: {LEGS[job]}\n" in block
    assert "environment: aws-acceptance" in block
    assert "id-token: write" in block


@pytest.mark.parametrize("job", LEGS)
def test_a_leg_cross_reads_then_cleans_up_always_with_a_fresh_credential(job: str) -> None:
    """Cross-read and cleanup are the last two steps, each behind its own mint."""
    block = _job_block(_text(), job)
    steps = _steps(block)
    cleanup = _step_named(block, "remove this leg's scratch tables")
    cross_read = _step_named(block, "cross-read module")
    assert steps[-1] == cleanup
    assert steps.index(cross_read) < steps.index(cleanup)
    assert f"if: {ALWAYS_ON_MAIN}" in cleanup
    expected = f"testing_r${{{{ github.run_id }}}}a${{{{ github.run_attempt }}}}_{LEGS[job]}_"
    assert f"RUN_PREFIX: {expected}" in cleanup
    assert "REPARK_ACCEPT_WAREHOUSE: ${{ vars.REPARK_ACCEPT_WAREHOUSE }}" in cleanup
    script = _run_script(cleanup) or ""
    command = ".venv/bin/python scripts/aws_acceptance_cleanup.py"
    assert script == f'{command} --run-prefix "${{RUN_PREFIX}}" --execute'
    previous = steps[steps.index(cleanup) - 1]
    assert CREDENTIALS in previous and f"if: {ALWAYS_ON_MAIN}" in previous
    before_cross_read = steps[steps.index(cross_read) - 1]
    assert CREDENTIALS in before_cross_read and f"if: {LIVE_ON_MAIN}" in before_cross_read
    assert f"if: {LIVE_ON_MAIN}" in cross_read
    assert "py-spy dump --pid" in cross_read


@pytest.mark.parametrize("job", LEGS)
def test_the_leg_cleanup_names_only_its_own_leg(job: str) -> None:
    """No leg's cleanup prefix carries another leg's letter."""
    cleanup = _step_named(_job_block(_text(), job), "remove this leg's scratch tables")
    legs = re.findall(r"_(acc|u|x)_\s*$", cleanup.split("RUN_PREFIX:")[1].splitlines()[0])
    assert legs == [LEGS[job]]


def test_dbt_installs_only_in_the_acceptance_job_and_the_previous_release_only_in_leg_x() -> None:
    """dbt-core and dbt-spark, and the hash-pinned previous-release venv, stay in their legs."""
    text = _text()
    for job in LEGS:
        block = _job_block(text, job)
        assert ("dbt-core==" in block) is (job == "acceptance-dbt"), job
        assert (".venv-previous" in block) is (job == "integrity-x"), job
        assert ("REPARK_PREVIOUS_RELEASE_PYTHON" in block) is (job == "integrity-x"), job
    assert "dbt-core" not in _job_block(text, "build")
    assert ".venv-previous" not in _job_block(text, "backstop")


def test_the_backstop_removes_only_by_age_after_installing_boto3_and_minting() -> None:
    """Older-than mode only, no run prefix, boto3 pinned before the mint."""
    block = _job_block(_text(), "backstop")
    script = "\n".join(_run_script(step) or "" for step in _steps(block))
    assert "--older-than-hours 24 --execute" in script
    assert "--run-prefix" not in script
    assert BOTO_PIN.search(block)
    assert block.index("boto3==") < block.index(CREDENTIALS)
    assert "environment: aws-acceptance" in block
    assert "if: github.ref != 'refs/heads/main'" in block


def test_every_action_in_the_workflow_is_pinned_by_a_full_commit_sha() -> None:
    """No tag or branch reference survives the split."""
    text = _text()
    uses = re.findall(r"(?m)^\s*-?\s*uses:\s*(\S+)", text)
    assert uses
    for reference in uses:
        assert re.fullmatch(r"[\w./-]+@[0-9a-f]{40}", reference), reference


def test_all_boto3_pins_are_the_same_exact_release() -> None:
    """The legs and the backstop install one boto3 version."""
    pins = set(re.findall(r"boto3==[0-9.]+", _text()))
    assert len(pins) == 1, pins


def _with(step: str, key: str) -> str:
    """The value of ``key`` under the step's ``with:`` block."""
    match = re.search(rf"(?m)^\s+{re.escape(key)}:\s*(\S+)\s*$", step.split("with:", 1)[1])
    assert match is not None, (key, step)
    return match.group(1)


def _artifact_steps(block: str, action: str) -> list[str]:
    """The steps of ``block`` that use the artifact ``action``."""
    return [step for step in _steps(block) if f"uses: actions/{action}@" in step]


def test_the_upload_and_every_download_agree_on_the_artifact_name_and_path() -> None:
    """One artifact name and one directory run from the build upload to each leg download."""
    text = _text()
    (upload,) = _artifact_steps(_job_block(text, "build"), "upload-artifact")
    name, path = _with(upload, "name"), _with(upload, "path").rstrip("/")
    for job in LEGS:
        (download,) = _artifact_steps(_job_block(text, job), "download-artifact")
        assert _with(download, "name") == name, job
        assert _with(download, "path").rstrip("/") == path, job


def test_each_install_step_reads_the_wheel_from_the_download_path() -> None:
    """The install command points at the directory the download step fills."""
    text = _text()
    for job in LEGS:
        block = _job_block(text, job)
        (download,) = _artifact_steps(block, "download-artifact")
        directory = _with(download, "path").rstrip("/")
        install = _step_named(block, "sync, install the wheel")
        script = _run_script(install) or ""
        assert f"--no-deps {directory}/repark-*.whl" in script, job


def test_the_build_upload_directory_is_the_one_maturin_and_the_lock_copy_write_to() -> None:
    """The build step writes the wheel and the lock into the uploaded directory."""
    block = _job_block(_text(), "build")
    (upload,) = _artifact_steps(block, "upload-artifact")
    directory = _with(upload, "path").rstrip("/")
    script = _run_script(_step_named(block, "sync + build the wheel")) or ""
    assert f"--out ../../{directory}" in script
    assert f"cp uv.lock {directory}/uv.lock" in script


def test_the_wheel_artifact_name_is_not_shared_with_another_upload() -> None:
    """Only one upload step in the whole file carries the wheel artifact's name."""
    text = _text()
    (upload,) = _artifact_steps(_job_block(text, "build"), "upload-artifact")
    name = _with(upload, "name")
    names = []
    for job in (
        "build",
        "acceptance-dbt",
        "integrity-u",
        "integrity-x",
        "backstop",
        "ice-read-perf-bench",
    ):
        names += [
            _with(step, "name")
            for step in _artifact_steps(_job_block(text, job), "upload-artifact")
        ]
    assert names.count(name) == 1, names

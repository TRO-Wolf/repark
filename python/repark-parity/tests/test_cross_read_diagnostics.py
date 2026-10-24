"""Execute the cross-read workflow shell against harmless process fixtures."""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from test_ice_read_perf_bench_workflow import _job_block, _run_script, _steps, _text

_PRELUDE = "import json, os, subprocess, sys, time\nfrom pathlib import Path\n"


def _write_executable(path: Path, body: str) -> None:
    """Write an executable fixture using the test interpreter."""
    path.write_text(f"#!{sys.executable}\n{_PRELUDE}{body}", encoding="utf-8")
    path.chmod(0o755)


def _cross_read_script(text: str) -> str:
    """Extract the workflow shell without replacing its process control."""
    steps = _steps(_job_block(text, "live-aws"))
    step = next(step for step in steps if step.startswith("name: cross-read module"))
    script = _run_script(step)
    assert script is not None
    return script


def _prepare_processes(directory: Path) -> dict[str, str]:
    """Create harmless child commands and return their test environment."""
    executables = directory / ".venv" / "bin"
    executables.mkdir(parents=True)
    (executables / "python").symlink_to(sys.executable)
    (directory / "pytest.py").write_text(
        _PRELUDE
        + "Path('target_pid').write_text(str(os.getpid()))\n"
        + "Path('phase').write_text('first')\n"
        + "time.sleep(float(os.environ['TEST_DURATION']) / 2)\n"
        + "Path('phase').write_text('second')\n"
        + "time.sleep(float(os.environ['TEST_DURATION']) / 2)\n"
        + "sys.exit(int(os.environ['TEST_EXIT']))\n",
        encoding="utf-8",
    )
    _write_executable(
        executables / "sleep",
        "with Path('delay_pids').open('a') as output:\n"
        "    output.write(f'{os.getpid()}\\n')\n"
        "time.sleep(float(os.environ['TEST_DELAY']))\n",
    )
    _write_executable(
        executables / "sudo",
        "arguments = sys.argv[1:]\n"
        "assert arguments[:4] == ['-n', '.venv/bin/py-spy', 'dump', '--pid']\n"
        "assert arguments[5:] == ['--native']\n"
        "target = int(arguments[4])\n"
        "record = [target, int(Path('target_pid').read_text()), Path('phase').read_text()]\n"
        "with Path('captures').open('a') as output:\n"
        "    output.write(json.dumps(record) + '\\n')\n"
        "print('DIAGNOSTIC_NATIVE_FRAME', flush=True)\n"
        "time.sleep(float(os.environ['TEST_PROFILER_DURATION']))\n"
        "sys.exit(int(os.environ['TEST_PROFILER_EXIT']))\n",
    )
    _write_executable(
        executables / "uv",
        "if sys.argv[1:3] == ['pip', 'install']:\n"
        "    sys.exit(0)\n"
        "sys.exit(subprocess.call([sys.executable, '-m', 'pytest']))\n",
    )
    return {
        **os.environ,
        "PATH": f"{executables}{os.pathsep}{os.environ['PATH']}",
        "TEST_DURATION": "0.4",
        "TEST_DELAY": "0.04",
        "TEST_EXIT": "0",
        "TEST_PROFILER_EXIT": "0",
        "TEST_PROFILER_DURATION": "0",
    }


def _assert_children_reaped(directory: Path) -> None:
    """Require the recorded target and delay processes to have exited."""
    paths = [directory / "target_pid", directory / "delay_pids"]
    for path in paths:
        if path.exists():
            for process_id in path.read_text().splitlines():
                assert not Path(f"/proc/{process_id}").exists(), f"child remains: {process_id}"


@pytest.mark.parametrize("test_exit", [0, 1, 17])
@pytest.mark.parametrize("profiler_exit", [0, 3])
def test_sampling_preserves_test_status_and_targets_both_phases(
    tmp_path: Path, test_exit: int, profiler_exit: int
) -> None:
    """Samples target Python in both phases; capture errors cannot hide test failures."""
    environment = _prepare_processes(tmp_path)
    environment.update(TEST_EXIT=str(test_exit), TEST_PROFILER_EXIT=str(profiler_exit))
    result = subprocess.run(
        [
            "bash",
            "--noprofile",
            "--norc",
            "-e",
            "-o",
            "pipefail",
            "-c",
            _cross_read_script(_text()),
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == test_exit, result.stderr
    captures = [json.loads(line) for line in (tmp_path / "captures").read_text().splitlines()]
    assert {capture[2] for capture in captures} == {"first", "second"}
    assert all(capture[0] == capture[1] for capture in captures)
    assert "DIAGNOSTIC_NATIVE_FRAME" in result.stdout
    assert ("Native stack capture failed" in result.stdout) == bool(profiler_exit)
    _assert_children_reaped(tmp_path)


def test_active_dump_finishes_reporting_before_watchdog_stops(tmp_path: Path) -> None:
    """Stopping during a dump preserves its output, failure warning and group closure."""
    environment = _prepare_processes(tmp_path)
    environment.update(TEST_EXIT="17", TEST_PROFILER_EXIT="3", TEST_PROFILER_DURATION="0.8")
    result = subprocess.run(
        ["bash", "-x", "-e", "-o", "pipefail", "-c", _cross_read_script(_text())],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=3,
        check=False,
    )
    assert result.returncode == 17, result.stderr
    assert "DIAGNOSTIC_NATIVE_FRAME" in result.stdout
    assert "Native stack capture failed" in result.stdout
    assert result.stdout.rstrip().endswith("::endgroup::")
    for process_id in (tmp_path / "delay_pids").read_text().splitlines():
        assert f"++ kill {process_id}\n" not in result.stderr
    _assert_children_reaped(tmp_path)


@pytest.mark.parametrize("profiler_exit", [0, 3])
def test_timeout_executes_and_reports_an_unfinished_dump(
    tmp_path: Path, profiler_exit: int
) -> None:
    """The actual timeout command bounds a dump still running after pytest exits."""
    environment = _prepare_processes(tmp_path)
    environment.update(
        TEST_EXIT="17", TEST_PROFILER_EXIT=str(profiler_exit), TEST_PROFILER_DURATION="2"
    )
    _write_executable(
        tmp_path / ".venv" / "bin" / "timeout",
        "assert sys.argv[1:3] == ['--kill-after=5s', '30s']\n"
        "Path('timeout_called').touch()\n"
        "os.execv('/usr/bin/timeout', ['timeout', '--kill-after=0.1s', '0.5s', *sys.argv[3:]])\n",
    )
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", _cross_read_script(_text())],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=3,
        check=False,
    )
    assert result.returncode == 17, result.stderr
    assert (tmp_path / "timeout_called").exists()
    assert "DIAGNOSTIC_NATIVE_FRAME" in result.stdout
    assert "Native stack capture failed" in result.stdout
    assert result.stdout.rstrip().endswith("::endgroup::")
    _assert_children_reaped(tmp_path)


def test_fast_completion_reaps_the_waiting_watchdog(tmp_path: Path) -> None:
    """A quick test does not leave its delayed sampler alive or hold output pipes open."""
    environment = _prepare_processes(tmp_path)
    environment["TEST_DURATION"] = "0.001"
    environment["TEST_DELAY"] = "1.5"
    started = time.monotonic()
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", _cross_read_script(_text())],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        timeout=2,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert time.monotonic() - started < 1.0
    _assert_children_reaped(tmp_path)


def test_profiler_install_precedes_credentials_and_dumps_are_bounded() -> None:
    """The pinned binary installs before credentials; samples cannot wait without a bound."""
    job = _job_block(_text(), "live-aws")
    install = "uv pip install --python .venv/bin/python --only-binary :all: py-spy==0.4.1"
    assert job.index(install) < job.index("aws-actions/configure-aws-credentials@")
    script = _cross_read_script(_text())
    assert "sleep 120 &" in script
    assert "timeout --kill-after=5s 30s sudo -n" in script
    assert "--locals" not in script
    assert "--subprocesses" not in script


@pytest.mark.parametrize("fails", [False, True])
def test_progress_is_visible_before_pytest_finishes(tmp_path: Path, fails: bool) -> None:
    """Real pytest streams progress from the workflow and retains the final test status."""
    executables = tmp_path / ".venv" / "bin"
    executables.mkdir(parents=True)
    _write_executable(
        executables / "python",
        f"os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])\n",
    )
    fixture = tmp_path / "python/repark/tests/parity/test_aws_acceptance_cross_read.py"
    fixture.parent.mkdir(parents=True)
    fixture.write_text(
        "import time\nfrom pathlib import Path\n"
        "def test_progress():\n"
        "    print('CROSS_READ_PROGRESS_VISIBLE', flush=True)\n"
        "    while not Path('release').exists():\n"
        "        time.sleep(0.01)\n"
        f"    assert {not fails}\n",
        encoding="utf-8",
    )
    environment = {**os.environ, "PYTEST_ADDOPTS": "", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"}
    with subprocess.Popen(
        ["bash", "-e", "-o", "pipefail", "-c", _cross_read_script(_text())],
        cwd=tmp_path,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    ) as process:
        try:
            assert process.stdout is not None
            output = b""
            deadline = time.monotonic() + 5
            while b"CROSS_READ_PROGRESS_VISIBLE" not in output:
                remaining = deadline - time.monotonic()
                assert remaining > 0, output
                readable, _, _ = select.select([process.stdout], [], [], remaining)
                assert readable, output
                chunk = os.read(process.stdout.fileno(), 65536)
                assert chunk, output
                output += chunk
            assert process.poll() is None
            (tmp_path / "release").touch()
            remaining_output, _ = process.communicate(timeout=5)
            assert process.returncode == int(fails), output + remaining_output
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
